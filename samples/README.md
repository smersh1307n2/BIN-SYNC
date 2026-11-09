# Samples — BinSync.Core 3.0.0+

The executable samples use the current public API and target .NET 10.
Run commands from the repository root:

```bash
dotnet run --project samples/TwoNodeLoopback -c Release
dotnet run --project samples/TwoNodeTcp -c Release
dotnet run --project samples/StorageArrayTcpSync -c Release
dotnet run --project samples/RelayProvenanceMap -c Release
dotnet run --project samples/ScenarioHarness -c Release
dotnet run --project samples/ScenarioHarness -c Release -- --soak 10
```

| Project | What it demonstrates |
|---------|----------------------|
| **TwoNodeLoopback** | Transport-independent `SyncSession` over paired `PipeTransport` endpoints, symmetric handshake, concurrent bidirectional batch exchange, ACKs and `BatchApplied` events |
| **TwoNodeTcp** | `TcpSyncListener` + `TcpSync.ConnectAsync`; both endpoints receive the same generic `SyncSession` abstraction and exchange batches in both directions |
| **StorageArrayTcpSync** | Two durable Storage-backed array replicas over real TCP; provider, Metadata Store and Node checkpoints survive reopen, with update and record tombstone propagation |
| **RelayProvenanceMap** | Durable FLD JournalSeq↔BIN-SYNC OutboxSeq mapping with ChangeId kept only in the global ledger; multi-operation source-journal ACK boundary, RPM3 checksum, restart reconstruction and fail-closed unmapped watermark handling |
| **ScenarioHarness** | Ten integration scenarios: bidirectional sync, three-node relay, LWW, tombstones, persistence/resume, sneakernet, idempotent replay, concurrent writers, TCP, and optional soak |

## ScenarioHarness matrix

| Id | Scenario |
|----|----------|
| S1 | Two-node bidirectional sync |
| S2 | Three-node relay |
| S3 | LWW conflict |
| S4 | Tombstone propagation |
| S5 | Persist / resume |
| S6 | Sneakernet export/import |
| S7 | Idempotent replay |
| S8 | Concurrent writers |
| S9 | TCP push/receive through generic `SyncSession` |
| S10 | Soak (`--soak [sec]`, default 15) |

S1–S8 exercise the public in-process `Transport` helpers and core mutation APIs. S9 and the dedicated loopback/TCP samples demonstrate the transport-independent session API used by production transport adapters.


## v3.1 host integration

The authority/member control-plane round, Joining/FULL_REPLACE/Active lifecycle, one-checkpoint-per-round broadcast and application-owned scheduling are documented in [BIN-SYNC-V3.1-HOST-ORCHESTRATION.md](../docs/BIN-SYNC-V3.1-HOST-ORCHESTRATION.md). The synchronized-FLD JournalSeq provenance adapter remains a later integration gate after FLD's neutral semantic and two-DLL extraction work; see [FLD-DB-INTEGRATION-CONTRACT.md](../docs/FLD-DB-INTEGRATION-CONTRACT.md).


`RelayProvenanceMap` is intentionally a low-level RPM3 storage-format/reopen/ACK-boundary sample. It calls `StorageRelayProvenanceMap.AppendPage` with pre-resolved sequence pairs and does not demonstrate the production FLD adapter. Production hosts must use `StorageSyncCoordinator.AppendRelayProvenancePage` under `AcquireApplicationCommitGuard`, after reading whole FLD journal batches; ChangeIds are resolved through the durable Node Outbox ledger before only the `(JournalSeq, OutboxSeq)` pairs are persisted. The host must still perform safe-ACK translation, durable FLD ACK, then map pruning in that order.
