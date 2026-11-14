# Storage-backed array synchronization over TCP

This executable exercises the public BIN-SYNC Storage adapters over a real loopback TCP transport.

It covers:
- two independent Storage databases, each containing its provider, metadata store and complete Node checkpoint;
- stable array-item RecordIds derived from GUIDs and stable field IDs;
- bidirectional synchronization of multi-field array items over `TcpSyncListener` and `TcpSync.ConnectAsync`;
- receiver-side Metadata Store PendingApply staging and validation before Node mutation, followed by Node checkpoint, provider apply and receipt finalization before the TCP session sends ACK;
- closing and reopening both databases, restoring both Nodes and continuing the same author streams;
- a later field update and a record lifecycle tombstone over TCP.

The sample intentionally opens a fresh TCP session for each exchange. It validates the actual `SyncSession` framing, handshake, Metadata-Store-authoritative batch receive and ACK path rather than the in-process `Transport.SyncRound` helper. Receive is routed through `StorageSyncCoordinator.ReceiveBatchBeforeAckAsync`; the legacy Node-owned receive overload is intentionally not used after provider cutover.

Run from the repository root:

```bash
dotnet run --project samples/StorageArrayTcpSync/StorageArrayTcpSync.csproj -c Release
```


This sample enables the durable Metadata-Store-owned outbound mode before the first local edit. The one-time cutover requires an empty provider local feed and aligned Node/Metadata Store outbound high watermarks; `StorageDataProvider` persists the cutover watermark. After cutover, each stable provider `ChangeId` is frozen directly into Metadata Store, which allocates the author Outbox sequence. The Node Outbox watermark is fenced and does not advance for new provider-backed local edits. The TCP sender uses `SyncSession.PushBatchAsync` with the Metadata Store and scope, so transport reads the immutable durable outbound stream rather than the Node Outbox mirror. Before the first delta, the sample explicitly registers both peers and completes the empty initial-snapshot baseline; later reconnects reuse the durable peer progress. The cutover marker survives the full database close/reopen exercised by the sample.
