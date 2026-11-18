# FLD ↔ BIN-SYNC host composition sample

This optional project contains the concrete application-layer adapter between
`Fld1.Database` and BIN-SYNC's durable relay coordinator. It is intentionally
not part of either database runtime and is not added to the default BIN-SYNC
solution: the FLD-DB repository is a separate checkout.

The FLD database runtime remains exactly two assemblies:
`Fld1.Storage.dll` and `Fld1.Database.dll`. The adapter is host integration
code and does not introduce a FLD → BIN-SYNC dependency.

## Build

With BIN-SYNC and FLD-DB checked out as sibling directories:

```sh
dotnet build samples/FldBinSyncHost/FldBinSyncHost.csproj -c Release
```

For non-sibling checkouts, override the paths:

```sh
dotnet build samples/FldBinSyncHost/FldBinSyncHost.csproj -c Release \
  -p:Fld1RepositoryRoot=/absolute/path/to/FLD-DB \
  -p:Fld1DatabaseProject=/absolute/path/to/FLD-DB/src/Fld1.Database/Fld1.Database.csproj \
  -p:Fld1StorageProject=/absolute/path/to/FLD-DB/src/Fld1.Storage/Fld1.Storage.csproj
```

Both `BinSync.Storage` and `Fld1.Database` receive the same
`Fld1StorageProject` property, so the host build resolves one canonical
`Fld1.Storage` project/assembly rather than loading the vendored copy and the
FLD checkout as competing type identities.

## Required host lifecycle

1. Construct one shared BIN-SYNC `StorageDatabase` and bind one
   `StorageMetadataStore`, `StorageRelayProvenanceMap` and
   `StorageMetadataPublicationMode(metadataStore, scopeId, localReplica, namespaceId)`
   to it. The publication-mode wrapper stores the immutable Node→Metadata Store
   cutover marker in a dedicated durable keyspace of that exact Metadata Store.
2. Open one `FldDatabase` for the same namespace. Create exactly one
   `FldDatabaseDataProvider(fld, activeAuthorEpoch, recordIdResolver, fieldIdResolver, publicationMode)`
   and one `FldRemoteBatchApplyStore` for that instance. The optional resolvers map
   application-specific item/field keys to FLD's stable RecordId/FieldId model;
   omit them for direct 16-byte/2-byte key encoding. Construct the coordinator with
   that exact provider, the exact `StorageMetadataStore` and
   `remoteBatchCommitStore: fldApply`; pass the exact provider, Metadata Store and
   apply store to `FldBinSyncHostAdapter`. The adapter rejects split
   provider/database/metadata/apply identities. This prevents field state, Outbox,
   publication cutover and ApplyJournal receipts from landing in separate stores.
   After creating `StorageNamespaceGcStore` over that same BIN-SYNC
   `StorageDatabase`, call `coordinator.AttachNamespaceGcGuard(gcStore, trustStore)`;
   this is required before the FULL_REPLACE-specific metadata barrier can be acquired. Pass these exact `gcStore` and `trustStore` instances to `StorageFullReplaceReceiver`; its typed lease rejects any different store/trust binding.
3. Call the `RecoverAndPublishAllAsync(persistNode, ...)` overload before
   enabling any sender after process startup. It recovers durable PendingApply
   intents and freezes the local provider feed under the composed coordinator →
   Node → FLD → Storage commit barrier. Before cutover it then reconciles FLD
   JournalSeq pages into RPM3. After Metadata Store cutover it skips FLD relay
   scanning entirely and captures the local outbound plus every relay-author
   high watermark directly from Metadata Store. Use `PublishReadyOutboxAsync`
   in the callback; it selects the matching ceiling-aware sender from the
   authority mode captured with the ceilings.
4. Route inbound batches through `ReceiveAndPublishAllAsync(inboundSession, persistNode, ...)`.
   This calls `StorageSyncCoordinator.ReceiveBatchBeforeAckAsync`, so PendingApply,
   Node checkpoint, the injected `FldRemoteBatchApplyStore` and Metadata receipt
   finalization complete before the incoming sender is ACKed. In metadata-owned
   mode that finalization also atomically creates the authoritative relay payload
   in Metadata Store; FLD ApplyJournal retains contextual receipt/recovery data but
   deliberately receives no duplicate `RelayWrites`. Route subsequent outbound
   sends through `RunOutboundAsync(persistNode, ...)`; the local feed freeze runs
   before each gate invocation.
5. After downstream peer ACKs, use the matching retention operation:
   - Metadata Store-owned mode: per-peer ACK/reservation state is already committed
     by `SyncSession`; call `CompactAcknowledgedRelayPayloadsAsync` to compact
     Metadata Store relay payload through the minimum durable peer ACK. Do not
     call FLD `AcknowledgeRelayBatches` or RPM3 in this mode.
   - Legacy Node-owned mode: call `AcknowledgeAllAsync`; it translates safe
     OutboxSeq through RPM3, durably ACKs FLD JournalSeq, then prunes RPM3.
   Never combine the two authorities or duplicate a relay payload journal.
6. Pass `CreateReferenceScanner()` to the BIN-SYNC physical-GC/frontier and
   FULL_REPLACE proof paths. Physical GC uses
   `StorageSyncCoordinator.AcquireApplicationCommitGuard(fld.AcquireCommitGuard)`;
   FULL_REPLACE must use the stronger
   `FldBinSyncHostAdapter.AcquireFullReplaceCommitGuard` factory.

7. Use `FldBinSyncSessionScheduler` for one receive owner per `SyncSession`,
   serialized outbound sends, one shared ceiling set per namespace fanout round,
   and optional periodic outbound cadence. Both outbound entry points freeze the local
   provider feed through the supplied `persistNode` callback before capturing ceilings.
   Run one `RunInboundLoopAsync` per session and one `RunPeriodicOutboundAsync` for
   the namespace. The application
   still creates/reconnects/disposes transports and `SyncSession` instances.

This sample binds the durable contracts, lock order and bounded sender path. The
application still creates/owns its `SyncSession` instances and transport lifecycle.
Before cutover the Node-owned sender uses the RPM3-mapped per-author ceiling. After
cutover, the same helper reads both local outbound and relay author ceilings directly
from Metadata Store; FLD JournalSeq and RPM3 are not consulted for publication or
ACK/compaction. The mode bit is captured under the composed commit barrier, preventing
a cutover race from mixing sequence domains. Do not call a legacy ceiling-free
overload from this FLD relay path. Physical GC remains disabled.


## Explicit implementation boundary

`FldDatabaseDataProvider` now implements the base `IDataProvider` bridge:
generation-pinned paged snapshots, Outbox-backed local changes, stable RecordId /
FieldId resolution, current-state reads, published-origin stamping and a shared
commit guard. It preserves Run v1 identity-zero materialized rows and Run v2
ChangeId/Origin. It intentionally refuses context-free remote APPLY; the exact
`FldRemoteBatchApplyStore` must receive the complete authenticated batch context.

The provider now implements durable Metadata Store publication cutover and author-
epoch closure (the active epoch itself is read from the host's durable identity
authority). FLD-backed FULL_REPLACE receiver staging and atomic namespace generation switch are
now implemented in the FLD engine and host provider adapter. The receiver stages
bounded immutable Run pages outside the active manifest, re-reads them through the
provider adapter to recompute the canonical signed digest, then performs one atomic
manifest switch while retaining old Runs pinned by durable snapshots. Remaining
C-pass work is the generic Storage page/run/WAL physical-format decision and final
public API/source/byte audit. Durable sender-side prepared
FULL_REPLACE source generations are now implemented: use
`StorageFullReplaceSender.PrepareSnapshotAndPersist` before transfer,
`RestorePreparedSnapshot` after process restart, and
`ReleasePreparedSnapshot` after the intended fanout is complete. The FLD provider
implements the durable snapshot-token capability; this path still requires T-pass
restart/crash validation. The optional sample supplies a multi-session scheduler, but
the application still owns transport creation/reconnect/disposal. The adapter is
not yet a complete v3.1 receiver. Physical receipt,
ChangeId and tombstone/Ghost GC remain disabled until the full cross-store T-pass.

## v3.1 control-plane round helper

The optional `V31NamespaceGcHostOrchestrator` is a one-shot host helper in this sample. Construct it with the shared `StorageSyncCoordinator`, the FLD/BIN-SYNC external reference scanner, the composed FLD commit-lease factory and the application's durable per-session frame-sequence allocator:

```csharp
var v31 = new V31NamespaceGcHostOrchestrator(
    coordinator, adapter.CreateReferenceScanner(),
    fld.AcquireCommitGuard,
    NextFrameSequence);

// If this authority is also an Active roster member, persist its own OFR2 first.
// Then perform one authority round and broadcast one identical durable RCP2.
var checkpoint = await v31.RunAuthorityRoundAsync(
    activeMemberSessions, authoritySeed,
    authorityMemberSeed: authorityMemberSeed);

// On a member node:
await v31.RunActiveMemberRoundAsync(authoritySession, memberSeed);
```

The helper sends the accepted roster before receiving reports, binds every received OFR2 identity/epoch to its authenticated session peer, publishes one checkpoint after the supplied sessions have reported, and broadcasts that same checkpoint. If a broadcast fails part-way, retry `BroadcastAcceptedCheckpointAsync` with the same checkpoint; it does not mint another sequence. `ResendAcceptedFrontierAsync` similarly retransmits the already-durable OFR2. Sessions, cancellation, timers/cadence and frame-sequence persistence remain application-owned. A missing Active member contributes frontier zero; this helper never assumes that a connected-session list is the entire roster.


## v3.1 FULL_REPLACE authority fanout

For a one-generation install across every non-retired remote roster member, call
`V31NamespaceGcHostOrchestrator.TransferFullReplaceToMembersAsync`. It captures
one immutable provider snapshot and one signed manifest, then sends identical
watermark/count/digest to each recipient.

For partial fanout failures, prepare once and retain the returned lease while
retrying only recipients that have not installed:

```csharp
using PreparedFullReplaceSnapshot prepared =
    v31.PrepareFullReplaceSnapshot(
        authorityProvider, gcStore, trustStore, authoritySeed, generation);

foreach (SyncSession missing in missingRecipients)
{
    await v31.TransferPreparedFullReplaceToMemberAsync(
        prepared, missing, gcStore, trustStore, authoritySeed, cancellationToken);
}
```

Dispose the prepared object after the intended fanout completes or is abandoned.
To survive process restart, persist the prepared generation with the Metadata Store
before starting network transfer. On restart, reopen the same FLD database and
Metadata Store, call `RestorePreparedSnapshot` with the currently accepted roster
and authority, then retry only recipients that have not installed. Never recreate
the same generation from a newer live provider snapshot. Release permanently only
after fanout completes or the generation is explicitly abandoned.
## Full remote-apply context adapter

`FldRemoteBatchApplyStore` implements the optional Core capability `IProviderRemoteBatchCommitStore`. Create it once for the target `FldDatabase`, pass that exact instance as `remoteBatchCommitStore` to `StorageSyncCoordinator`, and pass it again to `FldBinSyncHostAdapter`. The adapter fails closed if the capability/database identities differ. On the receive-before-ACK path, Core passes the exact authenticated sender/transport epoch, wire issuer/epoch, BatchId, cursor range, previous/current digest, records root and immutable provider mutations. The FLD capability maps those fields to `RemoteBatchCommit` and calls `FldDatabase.ApplyRemoteBatch`.

The adapter maps BIN-SYNC record tombstones to FLD's canonical lifecycle field (`FieldId=0`, `DeadValue`) with the original HLC, author and ChangeId. Field tombstones stay field tombstones. Its `RelayWrites` list is intentionally empty: v3 assigns relay payload and delivery retention to the BIN-SYNC Metadata Store, not a second FLD relay history. Current v3 DELTA_BATCH has no separate provider protocol-state delta, so the adapter passes an empty delta rather than synthesizing protocol state.

This is the lossless FLD ApplyJournal boundary, not yet a complete `IDataProvider`: the full adapter still needs stable generation-pinned paged snapshots, FLD local Outbox→provider feed/author-epoch mapping, record/field key resolver policy and the associated crash/reopen matrix.


### Injecting the FLD contextual remote-apply store

`StorageSyncCoordinator` accepts an optional final `remoteBatchCommitStore` argument. The FLD remote-apply capability is kept separate from the provider's snapshot/feed interface so the host can compose both over the same durable `FldDatabase` without adding BIN-SYNC types to either FLD runtime DLL:

```csharp
var fldApply = new FldRemoteBatchApplyStore(fld);
var coordinator = new StorageSyncCoordinator(
    node,
    fldDataProvider,       // still must target the same FLD database
    namespaceId,
    metadataStore,
    scopeId,
    remoteBatchCommitStore: fldApply);
```

The host must ensure `fldDataProvider` and `fldApply` operate on the same durable FLD state and participate in the same `fld.AcquireCommitGuard()` outer lease. `FldDatabaseDataProvider` is implemented, including the snapshot-pin guard required by FULL_REPLACE commit. For FULL_REPLACE, pass `FldBinSyncHostAdapter.AcquireFullReplaceCommitGuard` as the receiver's commit-guard factory; it holds the Metadata Store transaction gate and fails closed while any target-namespace PendingApply or immutable outbound/relay reservation remains. FLD's ApplyJournal contains committed idempotency receipts rather than a separate pending-provider-intent queue; the FLD commit lease excludes in-flight applies, and the unsealed-WAL barrier is included in the host draft horizon. Cross-repository build and adversarial T-pass remain pending.

## Process-isolated async provider (initial IPC slice)

`FldDatabaseIpcAsyncDataProvider` connects to the separate Database Host and implements the core `IAsyncDataProvider` surface without sync-over-async. Supply a stable per-replica `ClientId`, the expected BIN-SYNC local replica id, and the host's active author-epoch authority:

```csharp
await using var provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
    databaseRoot, stableClientId, timeout, localReplica, activeAuthorEpoch,
    recordIdResolver, fieldIdResolver, cancellationToken);
```

The connection verifies the Host's authoritative LocalAuthor and acquires a fresh Outbox lease. Local append uses operation 0x2111 with OperationId derived from ChangeId; the Host assigns the durable per-client receipt sequence under the same commit lock as the Outbox append. Reconnect with the same ClientId and ChangeId to replay an uncertain append safely. The provider's Outbox ACK uses 0x2112 to retire only the corresponding contiguous provider receipts.

Point reads use 0x210F/0x2110 and revalidate ChangeId before every value chunk. Whole-state reads use generation-pinned snapshot pages and value chunks. Snapshot handles are invalid after Host restart; reconnect and open a new snapshot.

Use `FldDatabaseIpcRemoteBatchApplyStore` for remote apply. It implements `IAsyncProviderRemoteBatchCommitStore` and preserves the full authenticated batch context. The context-free `IAsyncDataProvider.ApplyRemoteBatchAsync` path intentionally throws.

**Boundary:** this is an IPC-backed provider primitive, not yet a complete process-isolated BIN-SYNC coordinator. It implements async published-origin stamping, but does not implement the optional async commit-guard, publication-mode, author-epoch, snapshot-state, FULL_REPLACE, durable-snapshot or GC-scanner capabilities. Origin stamping is not cross-store atomic without the composed async commit barrier. `StorageSyncCoordinator` remains synchronous and Monitor-based. Do not use sync-over-async or hold Monitor across awaits. The async coordinator and composed async-safe commit lease remain the next architectural gate. Physical GC stays disabled.

## v3.1 physical-GC operator procedure (proof-only; production deletion remains off)

The host exposes `GetGcReadinessAsync(gcStore, trustStore)` as a read-only
preflight report and `RunGcMaintenanceAsync(...)` as a bounded maintenance
attempt. The report is split into Receipt, ChangeId and Record/Field Tombstone
domains. Each domain reports its own enabled state and global blockers; a
ready domain is only eligible to enter candidate-level proof checks, not
authorized to delete arbitrary rows.

Operational rules:

1. Keep all three `StorageNamespaceGcFeatureOptions` switches false in
   production until the independent release gates in the V3 roadmap are met.
   Defaults are false. The options are captured by the store instance at
   construction; they are not a live mutable control plane.
2. Treat each domain independently. A disabled domain must remain disabled
   even if its prerequisites are satisfied. Do not enable one domain as a
   workaround for blockers reported by another.
3. Before any controlled test, record the complete readiness report, including
   roster/checkpoint/FULL_REPLACE identity, draft horizon, snapshot-pin state,
   reservation diagnostics, each domain's enabled bit and every blocked reason.
   Re-read readiness immediately before each bounded maintenance round.
4. A non-empty blocker list means skip that domain. In particular, missing or
   unverifiable signed roster/checkpoint, unavailable Host reference scan,
   unpublished drafts, snapshot pins and outstanding peer reservations are
   stop conditions. Tombstone GC additionally requires the exact installed
   FULL_REPLACE baseline for every Active roster member and the provider's
   tombstone-GC capability.
5. Run only bounded pages. Cancellation is observed between domains and by the
   compaction operations. After cancellation, timeout, process restart or an
   uncertain result, leave the domain disabled, complete startup recovery,
   obtain a fresh readiness report and resume only through the durable
   compaction cursors. Never infer deletion from a transport ACK or from a
   previous readiness snapshot.
6. To disable maintenance, stop scheduling calls to `RunGcMaintenanceAsync`.
   For a configuration rollback, stop the host cleanly and restart the entire
   composition (coordinator and GC store included) with the relevant option
   false; do not swap the GC store underneath an attached coordinator.
7. Keep Ghost GC excluded. Do not use TTL, LRU or wall-clock age as a deletion
   authority. Do not treat process-restart tests as Windows NTFS power-loss
   durability evidence.

The durable feature switches are startup configuration; the process-local
emergency stop API below is an additional in-process brake, not a substitute
for durable operator policy. The sample now supplies the durable policy/audit store, stable blocker codes,
generation-bound per-domain leases, operator diagnostics and a durable
all-domains-off action. The embedding application must still expose these
operations through its authenticated operator endpoint and supply its real
identity/authorization provider. Physical GC remains disabled until the
application lifecycle and the remaining V3 release gates are verified.

### Runtime emergency stop and operator API

The low-level `FldBinSyncGcMaintenanceControl` remains process-local and starts
stopped with every domain disabled. It cancels in-flight bounded operations when
stopped, but it is not the production configuration interface. Do not manually
call `Start()`/`SetEnabled()` in the application scheduler: use
`composition.Operator.UpdatePolicyAsync`, which authenticates the actor,
records an audit event, checks the expected generation and applies each domain
independently without overriding immutable startup proof flags.

For first response, call `composition.Operator.EmergencyStop()` in each local
Host process. Then call `EmergencyStopDurablyAsync(actorId, reason)` to persist
the all-domains-off policy and wait for active per-domain leases to drain. If
the durable commit fails, the local latch remains set; do not restart or
re-enable GC until an authorized all-off policy commit is verified. A stop
cannot roll back a page that already committed durably; inspect the operator
diagnostics and readiness again after stopping or restarting.


### Durable operator policy contract and readiness blocker codes

The sample defines `IFldBinSyncGcOperatorPolicyStore`, `IFldBinSyncGcOperatorAuthorizer` and `FldBinSyncGcOperatorPolicyCoordinator`, and includes the concrete `StorageFldBinSyncGcOperatorPolicyStore`. It stores policy and audit in one atomic `StorageWriteBatch` on the same `StorageDatabase` as Metadata, and uses OS file-share locks for per-domain cross-process leases. The application supplies the real `IFldBinSyncGcOperatorAuthorizer` and authenticated endpoint; it does not need to implement another policy store. The store rejects corrupt records and mismatched (scope, local replica, namespace) bindings.

Each committed update carries a strictly increasing policy generation and audit sequence, actor identity, action, reason, affected domains and UTC audit timestamp. The timestamp is audit metadata only; it is never a GC horizon or deletion criterion. The coordinator authenticates/authorizes the actor through the host-supplied authorizer, rejects stale expected generations, commits the policy/audit pair before opening runtime gates, and serializes updates/restores within one coordinator instance. The durable store's atomic compare-and-swap remains mandatory across multiple processes.

Prefer `FldBinSyncAsyncHostComposition.StartAsync` as the application startup barrier. It restores durable policy, completes provider/Node recovery, and only then invokes the callback that starts peer sessions and maintenance scheduling. If recovery fails, the callback is never invoked and the local emergency brake is latched. For more complex application startup state machines, `RecoverBeforeAcceptingSessionsAsync` remains the lower-level barrier. Missing policy leaves the runtime stopped; corrupt, mismatched or unavailable policy stops it and propagates the error. A persisted policy can only restrict immutable startup `StorageNamespaceGcFeatureOptions`; it cannot override a false startup flag. Production code must use `composition.Operator.UpdatePolicyAsync` rather than calling raw runtime `Start/SetEnabled` methods. `EmergencyStop()` remains the immediate process-local brake; `EmergencyStopDurablyAsync(actorId, reason)` commits an audited all-domains-disabled policy for restart-safe shutdown.

`FldBinSyncGcDomainReadiness.Blockers` exposes stable `FldBinSyncGcBlockerCode` identifiers alongside the existing human-readable `BlockedReasons` strings. Unknown future messages map to `Unclassified` rather than being silently treated as a known condition. Consumers should alert on the stable code and retain the message for operator context; neither a blocker code nor a readiness snapshot authorizes candidate deletion.

The sample includes `StorageFldBinSyncGcOperatorPolicyStore`, which stores policy and audit records in one `StorageWriteBatch` on the exact `StorageDatabase` shared with `StorageMetadataStore`, and uses OS file-share locks for cross-process per-domain leases. `FldBinSyncAsyncHostComposition.Create` wires the caller-owned `StorageDatabase`, Metadata adapter, namespace-GC guard, durable policy store/coordinator, shared runtime control and async Host adapter into one checked object graph. `FldBinSyncGcHostOperations` exposes a generation-consistent diagnostics snapshot (policy, bounded audit tail, readiness, startup proof flags and runtime gates), independent per-domain policy updates, one bounded maintenance round and immediate/durable emergency stop. Runtime policy cannot enable a domain whose immutable startup proof switch is false.

Typical startup ordering should use the composition barrier. The callback is not invoked until durable policy restore and provider/Node recovery have completed:

```csharp
using var host = FldBinSyncAsyncHostComposition.Create(
    storage, node, scopeId, metadataScopeId, namespaceId, trustStore,
    provider, remoteApply, persistNode, authorizer,
    gcFeatureOptions: new StorageNamespaceGcFeatureOptions(EnableChangeIdGc: true));

await host.StartAsync(async ct =>
{
    await peerListener.StartAsync(ct);
    await maintenanceScheduler.StartAsync(ct);
}, cancellationToken);

FldBinSyncGcOperatorDiagnosticsSnapshot status =
    await host.Operator.GetDiagnosticsAsync();

await host.Operator.UpdatePolicyAsync(
    expectedGeneration: status.Policy?.Generation ?? 0,
    actorId: authenticatedActorId,
    reason: "approved ChangeId canary",
    maintenanceStarted: true,
    receiptEnabled: false,
    changeIdEnabled: true,
    tombstoneEnabled: false,
    cancellationToken);
```

The application callback must clean up partially opened listeners/schedulers if it throws. Startup failure latches the local emergency brake and does not mark the composition started. For shutdown/rollback, stop scheduling new rounds, call `EmergencyStopDurablyAsync(authenticatedActorId, reason)`, wait for it to return (it drains active domain leases), then close peer sessions and dispose the composition. If durable stop fails, keep the process stopped and do not restart/re-enable GC until an authorized all-off policy commit is verified.

The embedding production application must still provide its real authenticated `IFldBinSyncGcOperatorAuthorizer`, actor identity source, peer listener, scheduler and transport/session lifecycle. The sample tests are not Windows power-loss durability evidence. Until those application-owned pieces are deployed and verified, keep all physical-GC startup flags false and Ghost GC excluded.

The store contract requires `TryAcquireMaintenanceLeaseAsync`. The store atomically verifies the current policy generation and enabled bit; a policy update that disables or changes a domain waits for active leases to drain. A null lease makes the adapter skip that domain's page and disable only that local domain; other independent domains remain eligible. Store read/corruption failures throw and fail closed by stopping the entire local runtime control. `FldBinSyncAsyncHostAdapter.RunGcDomainAsync` now acquires and releases this durable lease internally around every bounded adapter GC operation, including legacy per-domain entry points. Production code must route maintenance through the adapter and must not call lower-level compaction APIs directly. The local runtime gate alone is not a cross-process policy fence.

The `FldBinSyncAsyncHostAdapter` constructor accepts the same policy coordinator and a `requireDurableOperatorPolicy` switch. The adapter requires durable operator policy by default. Pass the coordinator in the production composition; readiness reports `DurableOperatorPolicyUnavailable` and GC entry points fail closed if it is absent. Tests that intentionally exercise the adapter without a policy coordinator must explicitly set `requireDurableOperatorPolicy: false`; this opt-out is not production-safe.


## Executable integration Host

This repository does not contain a production transport/session executable or
application identity provider. To validate the composition as an application
would consume it, use the separate
[FldBinSyncHost.Runtime](../FldBinSyncHost.Runtime/README.md) executable test
Host. It starts the real FLD Database Host as a separate process, performs the
provider-authoritative publication cutover, invokes the composition startup
barrier, and exposes the operator surface through a loopback Kestrel API with
test-only Bearer authentication.

Its peer listener and scheduler are lifecycle probes, not production transport
or physical-GC execution. The process-level Linux/Windows tests validate
startup ordering, authorization, durable policy updates, emergency stop,
process restart and fail-closed startup. They do not constitute production
deployment or authorize enabling GC. A future real application must still
supply its own identity provider, authenticated authorization, SyncSession
transport lifecycle and production scheduler.
