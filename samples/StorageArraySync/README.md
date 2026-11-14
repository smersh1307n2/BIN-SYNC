# Storage-backed array synchronization

This executable demonstrates the public `BinSync.Storage` assembly with two BIN-SYNC nodes and two durable Storage databases. Each replica stores its complete integrity-protected NodePersistence v17 checkpoint, StorageDataProvider state and StorageMetadataStore state in the same caller-owned StorageDatabase.

It covers:
- stable record IDs derived from array item GUIDs and stable field IDs derived from field names;
- multiple fields per array item;
- the explicit Node-Outbox transition path: durable local change feeds are acknowledged only after the Node checkpoint and Metadata Store outbound freeze both succeed;
- exact Node Outbox ChangeIds/fingerprints/payloads frozen into Storage metadata with original per-author sequence numbers;
- reading frozen outbound pages under a durable lease;
- replay-safe publication using the original HLC and local author identity;
- bidirectional BIN-SYNC synchronization;
- metadata PendingApply intent, provider commit and metadata finalization before the sender receives ACK;
- crash/restart after receiver Node checkpoint and provider apply but before ACK, followed by duplicate-batch replay;
- remote materialization does not re-enter the local change feed;
- closing and reopening each replica's single StorageDatabase, restoring the complete Node checkpoint from that database, then applying a second update;

The in-process transport's `beforeAck` hook is the same ordering boundary exposed by `SyncSession.ReceiveBatchAsync`: Node receipt/checkpoint → external provider commit → sender ACK. If provider commit fails, the sender retains the batch for idempotent retry.

Run from the repository root:

```bash
dotnet run --project samples/StorageArraySync/StorageArraySync.csproj -c Release
```

The sample uses the public `IDataProvider`, `IMetadataStore`, `StorageDataProvider`, `StorageMetadataStore` and `StorageSyncCoordinator` APIs. The adapters share one Storage database per replica. This in-process `Transport.SyncRound` helper is intentionally Node-Outbox-based, so the sample exercises the explicit transition path: the coordinator freezes local Node Outbox entries into Metadata Store before provider-feed ACK and applies received immutable field batches through a durable PendingApply transaction before ACK. `RecoverPendingApplies()` handles intents left by a crash. NodePersistence v17 remains the authoritative Node state image, stored in the same StorageDatabase as provider and metadata state. The provider-authoritative, Metadata-Store-owned outbound path is demonstrated by `StorageArrayTcpSync`, which uses the durable `SyncSession.PushBatchAsync` overload rather than the Node Outbox transport helper. Field-level and record-level tombstones are preserved separately; the provider applies BIN-SYNC ghost visibility so a record tombstone hides older fields while newer ghost fields remain visible.
