# Samples

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh   # once from repo root
```

## TwoNodeLoopback

In-process `MakeSession` + `SyncRound`.

```bash
dotnet run --project samples/TwoNodeLoopback -c Release
```

## TwoNodeTcp

TCP loopback: `TcpSyncListener` + `ConnectAsync`, push/receive deltas.

```bash
dotnet run --project samples/TwoNodeTcp -c Release
```

## ScenarioHarness

| # | Scenario |
|---|----------|
| S1–S8 | loopback / persist / sneakernet / concurrent |
| S9 | TCP push/receive |
| S10 | Soak (`--soak [sec]`, default 15) |

```bash
dotnet run --project samples/ScenarioHarness -c Release
dotnet run --project samples/ScenarioHarness -c Release -- --soak 10
```
