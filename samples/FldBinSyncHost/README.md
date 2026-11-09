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
   FULL_REPLACE proof paths, with the same
   `StorageSyncCoordinator.AcquireApplicationCommitGuard(fld.AcquireCommitGuard)`
   lease factory.

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
authority). The remaining C-pass work is FLD-backed FULL_REPLACE receiver
staging/atomic generation switch, generic Storage page/run/WAL physical-format
decision and final public API/source/byte audit. Durable sender-side prepared
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

The host must ensure `fldDataProvider` and `fldApply` operate on the same durable FLD state and participate in the same `fld.AcquireCommitGuard()` outer lease. The full `fldDataProvider` implementation is still open; do not substitute an unrelated in-memory or second durable database.
