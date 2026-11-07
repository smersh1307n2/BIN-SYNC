# Storage-backed array synchronization

This executable demonstrates an application-owned array persisted by `Fld1.Storage` and synchronized between two BIN-SYNC nodes.

It covers:
- stable record IDs derived from array item GUIDs;
- multiple fields per array item;
- durable local change feed and acknowledgement;
- BIN-SYNC bidirectional synchronization;
- remote state materialization into Storage without re-entering the local feed;
- closing and reopening both Storage partitions and BIN-SYNC node persistence before a second update.

Run from the repository root:

```bash
dotnet run --project samples/StorageArraySync/StorageArraySync.csproj -c Release
```

The sample intentionally uses the current public `Node` write/sync API and a small host-side Storage bridge. It does not claim that the future public Data Provider API has shipped; the durable adapter contract is separately exercised by `BinSync.Storage.Integration`.
