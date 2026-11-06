# Samples — BinSync.Core 3.0.0+

The three executable samples use the current public API and target .NET 10.
Run commands from the repository root:

```bash
dotnet run --project samples/TwoNodeLoopback -c Release
dotnet run --project samples/TwoNodeTcp -c Release
dotnet run --project samples/ScenarioHarness -c Release
dotnet run --project samples/ScenarioHarness -c Release -- --soak 10
```

| Project | What it demonstrates |
|---------|----------------------|
| **TwoNodeLoopback** | Transport-independent `SyncSession` over paired `PipeTransport` endpoints, symmetric handshake, concurrent bidirectional batch exchange, ACKs and `BatchApplied` events |
| **TwoNodeTcp** | `TcpSyncListener` + `TcpSync.ConnectAsync`; both endpoints receive the same generic `SyncSession` abstraction and exchange batches in both directions |
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
