# Storage-backed array synchronization

This executable demonstrates the public `BinSync.Storage` assembly with two BIN-SYNC nodes and two durable Storage databases. Each replica attaches both StorageDataProvider and StorageMetadataStore to the same caller-owned StorageDatabase.

It covers:
- stable record IDs derived from array item GUIDs and stable field IDs derived from field names;
- multiple fields per array item;
- durable local change feeds and acknowledgement only after the Node checkpoint and Metadata Store outbound freeze both succeed;
- exact Node Outbox ChangeIds/fingerprints/payloads frozen into Storage metadata with original per-author sequence numbers;
- reading frozen outbound pages under a durable lease;
- replay-safe publication using the original HLC and local author identity;
- bidirectional BIN-SYNC synchronization;
- metadata PendingApply intent, provider commit and metadata finalization before the sender receives ACK;
- crash/restart after receiver Node checkpoint and provider apply but before ACK, followed by duplicate-batch replay;
- remote materialization does not re-enter the local change feed;
- closing and reopening both Storage providers and BIN-SYNC node state before a second update.

The in-process transport's `beforeAck` hook is the same ordering boundary exposed by `SyncSession.ReceiveBatchAsync`: Node receipt/checkpoint → external provider commit → sender ACK. If provider commit fails, the sender retains the batch for idempotent retry.

Run from the repository root:

```bash
dotnet run --project samples/StorageArraySync/StorageArraySync.csproj -c Release
```

The sample uses the public `IDataProvider`, `IMetadataStore`, `StorageDataProvider`, `StorageMetadataStore` and `StorageSyncCoordinator` APIs. The adapters share one Storage database per replica. The coordinator freezes local Outbox entries into metadata before provider-feed ACK and applies received immutable field batches through a durable PendingApply transaction before network ACK. `RecoverPendingApplies()` handles intents left by a crash. The Node/Transport path still retains NodePersistence/WAL as its authoritative peer cursor and Outbox/session state; replacing that authority with `IMetadataStore` is the next integration gate. Whole-record tombstones remain explicitly unsupported by the provider coordinator until record-level durable semantics are added.
