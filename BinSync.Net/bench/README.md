# BinSync.Bench — performance baseline

Branch: `feature/fld-external-storage` (core and durable metadata-store benchmarks)

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
dotnet run -c Release --project bench/BinSync.Bench -- --filter '*MetadataPeer*'
```

MemoryDiagnoser is on by default (`B/op`, `Gen0`). Metadata peer lifecycle setup is performed once per iteration and excluded from the measured operation; each iteration starts from a fresh durable store with the requested unrelated payload count. These are scaling measurements, not claims of an absolute production latency.

## Suites

| Class | What |
|-------|------|
| `CodecBench` | WriteVarint / Writer / Reader / CanonicalB128 |
| `MergeBench` | LWW, MaxU64, MinBytes pair + fold-N |
| `FrameBench` | BuildFrame / ParseFrame / Fragment / Crc32C |
| `BatchBench` | Encode / Decode / idempotent ApplyBatch |
| `BatchApplyColdBench` | cold ApplyBatch (new receiver each op) |
| `MetadataPeerRegistrationBench` | durable peer registration vs 100/1,000/10,000 unrelated outbound payload rows |
| `MetadataPeerBaselineBench` | durable snapshot-baseline commit vs unrelated payload scale |
| `MetadataPeerRetirementBench` | durable peer-epoch retirement vs unrelated payload scale |

See [METADATA-STORE-BENCHMARKS.md](../../../docs/METADATA-STORE-BENCHMARKS.md) for the first durable peer-lifecycle scaling run and its limitations.

## Interpreting results

1. Save the first run as **baseline** (commit hash + machine).
2. Optimise only rows that dominate wall time or allocations; compare metadata lifecycle results across payload counts to detect accidental whole-partition work.
3. Re-run the same filter; compare Mean and Allocated.

Record runtime, allocated bytes and scaling slope for each metadata transition. Run on the same machine/runtime before and after a change; do not compare numbers from different hosts as a regression.
