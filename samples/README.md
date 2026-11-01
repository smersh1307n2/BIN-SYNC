# Samples

## TwoNodeLoopback

Minimal in-process sync of two `Node` instances (`Transport.MakeSession` + `SyncRound`).

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh   # once
dotnet run --project samples/TwoNodeLoopback -c Release
```

## ScenarioHarness

Multi-scenario smoke for realistic flows (logging, exit ≠ 0 on failure):

| # | Scenario |
|---|----------|
| S1 | Two-node bidirectional |
| S2 | Three-node relay (A→B→C) |
| S3 | LWW conflict (later HLC wins) |
| S4 | Field tomb propagates |
| S5 | Persist → load → resume sync |
| S6 | Sneakernet export/import |
| S7 | Idempotent batch replay |
| S8 | Concurrent writers + sync |

```bash
dotnet run --project samples/ScenarioHarness -c Release
```

Expected: all `PASS`, exit code `0`.
