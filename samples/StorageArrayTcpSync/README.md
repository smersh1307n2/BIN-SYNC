# Storage-backed array synchronization over TCP

This executable exercises the public BIN-SYNC Storage adapters over a real loopback TCP transport.

It covers:
- two independent Storage databases, each containing its provider, metadata store and complete Node checkpoint;
- stable array-item RecordIds derived from GUIDs and stable field IDs;
- bidirectional synchronization of multi-field array items over `TcpSyncListener` and `TcpSync.ConnectAsync`;
- receiver-side Node checkpoint, provider apply and Metadata Store PendingApply finalization before the TCP session sends ACK;
- closing and reopening both databases, restoring both Nodes and continuing the same author streams;
- a later field update and a record lifecycle tombstone over TCP.

The sample intentionally opens a fresh TCP session for each exchange. It validates the actual `SyncSession` framing, handshake, batch receive and ACK path rather than the in-process `Transport.SyncRound` helper.

Run from the repository root:

```bash
dotnet run --project samples/StorageArrayTcpSync/StorageArrayTcpSync.csproj -c Release
```
