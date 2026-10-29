# BinSync.Bench — performance baseline

Branch: `perf/hotpath-baseline`

BenchmarkDotNet harness for the sync hot path. Goal: **numbers before optimisations**.

## Prerequisites

```bash
# from repo root
bash scripts/vendor-ed25519-from-ztsharp.sh
```

## Run

```bash
cd BinSync.Net

# all benches (Release)
dotnet run -c Release --project bench/BinSync.Bench -- --filter '*'

# by area
dotnet run -c Release --project bench/BinSync.Bench -- --filter '*Codec*'
dotnet run -c Release --project bench/BinSync.Bench -- --filter '*Merge*'
dotnet run -c Release --project bench/BinSync.Bench -- --filter '*Frame*'
dotnet run -c Release --project bench/BinSync.Bench -- --filter '*Batch*'
```

MemoryDiagnoser is on by default (`B/op`, `Gen0`).

## Suites

| Class | What |
|-------|------|
| `CodecBench` | WriteVarint / Writer / Reader / CanonicalB128 |
| `MergeBench` | LWW, MaxU64, MinBytes pair + fold-N |
| `FrameBench` | BuildFrame / ParseFrame / Fragment / Crc32C |
| `BatchBench` | Encode / Decode / idempotent ApplyBatch |
| `BatchApplyColdBench` | cold ApplyBatch (new receiver each op) |

## Interpreting results

1. Save the first run as **baseline** (commit hash + machine).
2. Optimise only rows that dominate wall time or allocs on **Batch** / **Codec**.
3. Re-run the same filter; compare Mean and Allocated.

Do not optimise cold paths (schema, KEY_ROTATION) until ApplyBatch/Codec are clean.
