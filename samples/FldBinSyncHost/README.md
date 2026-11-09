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
   `StorageMetadataStore`, `StorageRelayProvenanceMap`, provider and
   `StorageSyncCoordinator` to it.
2. Open one `FldDatabase` for the same namespace and construct
   `FldBinSyncHostAdapter` with the stable metadata scope.
3. Call `RecoverAndPublishAllAsync` before enabling any sender after process
   startup. Its watermark set contains the local-author ceiling plus every FLD
   relay author's RPM3 ceiling. Before Metadata Store cutover the local ceiling
   comes from Node Outbox; after cutover it comes from Metadata Store. Use
   `PublishReadyOutboxAsync` in the callback; it selects the matching ceiling-aware
   `SyncSession.PushBatchAsync` overload from the mode captured with the ceilings.
4. Route remote APPLY through `ApplyAndPublishAllAsync`; route subsequent
   outbound sends through `RunOutboundAsync`.
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
