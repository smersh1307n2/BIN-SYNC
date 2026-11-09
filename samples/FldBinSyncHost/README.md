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

1. Construct one shared BIN-SYNC `StorageDatabase`, then bind
   `StorageMetadataStore`, `StorageRelayProvenanceMap`, a conforming
   `IDataProvider` and `StorageSyncCoordinator` to it. **The sample does not yet
   provide the `FldDatabase`→`IDataProvider` adapter**; this remains a C-pass item.
   Do not substitute a whole-database in-memory snapshot or a lossy remote-apply
   mapping.
2. Open one `FldDatabase` for the same namespace. Create exactly one
   `FldRemoteBatchApplyStore` for that instance, inject that same object into the
   `StorageSyncCoordinator(remoteBatchCommitStore: fldApply)` constructor argument,
   and pass it to `FldBinSyncHostAdapter`. The adapter rejects a different FLD
   database instance or a coordinator that does not use that exact capability.
   This prevents field state and ApplyJournal receipts from landing in separate DBs.
3. Call the `RecoverAndPublishAllAsync(persistNode, ...)` overload before
   enabling any sender after process startup. It freezes the provider feed into
   the active outbound authority before startup recovery and publication. Its watermark set contains the local-author ceiling plus every FLD
   relay author's RPM3 ceiling. Before Metadata Store cutover the local ceiling
   comes from Node Outbox; after cutover it comes from Metadata Store. Use
   `PublishReadyOutboxAsync` in the callback; it selects the matching ceiling-aware
   `SyncSession.PushBatchAsync` overload from the mode captured with the ceilings.
4. Route inbound batches through `ReceiveAndPublishAllAsync(inboundSession, persistNode, ...)`.
   This calls `StorageSyncCoordinator.ReceiveBatchBeforeAckAsync`, so PendingApply,
   Node checkpoint, the injected `FldRemoteBatchApplyStore` and Metadata receipt
   finalization complete before the incoming sender is ACKed; only then does the
   relay gate drain and publish downstream. Route subsequent outbound sends through
   `RunOutboundAsync(persistNode, ...)`.
   These overloads freeze newly pending local feed entries before each gate invocation.
5. Route safe ACK through `AcknowledgeAllAsync`. FLD's durable ACK occurs
   before RPM3 pruning.
6. Pass `CreateReferenceScanner()` to the BIN-SYNC physical-GC/frontier and
   FULL_REPLACE proof paths, with the same
   `StorageSyncCoordinator.AcquireApplicationCommitGuard(fld.AcquireCommitGuard)`
   lease factory.

This sample binds the durable contracts, lock order and bounded sender path. The
application still creates/owns its `SyncSession` instances and transport lifecycle;
inside the publication-gate callback, call `PublishReadyOutboxAsync` so the Core
sender filters every author page to the captured ceiling. Before cutover it uses
Node-owned Outbox transport; after cutover it uses Metadata Store-owned transport.
The mode bit is captured under the same commit barrier as the ceilings, preventing
a cutover race from mixing sequence authorities. Do not call a legacy ceiling-free
overload from this FLD relay path. Physical GC remains disabled.


## Explicit implementation boundary

`FldBinSyncHostAdapter` is the relay-publication, ACK-translation and GC-reference adapter, not yet a complete `FldDatabase` data-provider implementation. `FldDatabase` currently exposes relay/outbox/reference operations but no stable paged full-data snapshot API, while BIN-SYNC's base `IDataProvider` apply method omits the full FLD remote-batch receipt context. The contextual `IProviderRemoteBatchCommitStore` half is now implemented independently; the remaining bridge is the generation-pinned snapshot/local-feed/key-resolver provider adapter. Do not bridge by copying the entire database into RAM or synthesizing sender/receipt identity.


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
