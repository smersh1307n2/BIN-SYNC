# Storage-backed array synchronization

This executable demonstrates the public `BinSync.Storage` assembly with two BIN-SYNC nodes and two durable Storage-backed Data Providers.

It covers:
- stable record IDs derived from array item GUIDs and stable field IDs derived from field names;
- multiple fields per array item;
- durable local change feeds and acknowledgement only after the Node checkpoint callback succeeds;
- replay-safe publication using the original HLC and local author identity;
- bidirectional BIN-SYNC synchronization;
- remote materialization into Storage **before the sender receives ACK**;
- crash/restart after receiver Node checkpoint and provider apply but before ACK, followed by duplicate-batch replay;
- remote materialization does not re-enter the local change feed;
- closing and reopening both Storage providers and BIN-SYNC node state before a second update.

The in-process transport's `beforeAck` hook is the same ordering boundary exposed by `SyncSession.ReceiveBatchAsync`: Node receipt/checkpoint → external provider commit → sender ACK. If provider commit fails, the sender retains the batch for idempotent retry.

Run from the repository root:

```bash
dotnet run --project samples/StorageArraySync/StorageArraySync.csproj -c Release
```

The sample uses the public `IDataProvider`, `StorageDataProvider` and `StorageSyncCoordinator` APIs. Durable protocol metadata is provided by `IMetadataStore` / `StorageMetadataStore`; conformance, shared-database, restart and compaction behavior are exercised separately by `BinSync.Storage.Integration`. The Node/Transport path still retains its legacy internal metadata state; direct `IMetadataStore` injection into Node is a separate integration gate.
