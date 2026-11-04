# Samples — BinSync.Core 2.6+

Public API only (`NodeBuilder`, `RecordId`, `TryGetField`, events). No `InternalsVisibleTo` required for the happy path.

```bash
# from repo root
dotnet run --project samples/TwoNodeLoopback -c Release
dotnet run --project samples/TwoNodeTcp -c Release
dotnet run --project samples/ScenarioHarness -c Release
dotnet run --project samples/ScenarioHarness -c Release -- --soak 10
```

| Project | What it shows |
|---------|----------------|
| **TwoNodeLoopback** | In-process `Transport.SyncRound`, `FieldWritten` / `BatchApplied` events |
| **TwoNodeTcp** | `TcpSyncListener` + `TcpSyncSession` push/receive |
| **ScenarioHarness** | 9 scenarios (+ optional soak): relay, LWW, tomb, persist, sneakernet, TCP, concurrency |

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
| S9 | TCP push/receive |
| S10 | Soak (`--soak [sec]`, default 15) |
