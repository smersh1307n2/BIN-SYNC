# Storage-backed array synchronization

This executable demonstrates the production `BinSync.Storage` assembly with two BIN-SYNC nodes and two durable Storage-backed Data Providers.

It covers:
- stable record IDs derived from array item GUIDs and stable field IDs derived from field names;
- multiple fields per array item;
- durable local change feeds and acknowledgement only after the Node checkpoint callback succeeds;
- replay-safe publication using the original HLC and local author identity;
- bidirectional BIN-SYNC synchronization;
- remote materialization into Storage without re-entering the local change feed;
- closing and reopening both Storage providers and BIN-SYNC node state before a second update.

Run from the repository root:

```bash
dotnet run --project samples/StorageArraySync/StorageArraySync.csproj -c Release
```

The sample uses the public `IDataProvider`, `StorageDataProvider` and `StorageSyncCoordinator` APIs. Durable protocol metadata is provided by `IMetadataStore` / `StorageMetadataStore`; its conformance and restart behavior are exercised separately by `BinSync.Storage.Integration`.
