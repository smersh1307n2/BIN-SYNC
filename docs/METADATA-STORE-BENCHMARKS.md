# Durable Metadata Store benchmarks

## First peer-lifecycle scaling run

**Run:** GitHub Actions Durable Metadata Bench, commit 534fbb5978be6f6f903ecc01207eb89a955da56a  
**BenchmarkDotNet:** 0.14.0 · **Runtime:** .NET 10.0.14 · **OS:** Ubuntu 26.04.1 LTS · **Architecture:** x64 RyuJIT · **GC:** Server, concurrent  
**Job:** InProcessEmitToolchain, 3 warmup iterations, 10 measurement iterations. Each measured invocation performs 256 independent peer lifecycle operations; the reported mean and allocation are normalized per operation. Database creation and test-data population run in IterationSetup and are excluded from the measured method.

The fixture creates one author stream containing 100, 1,000 or 10,000 unrelated outbound payload rows (plus the corresponding ledger rows). It then measures registration, snapshot-baseline completion or retirement across a 256-peer roster. The lifecycle operation touches one peer and one author stream at a time.

| Operation | Unrelated outbound rows | Mean / operation | Allocated / operation |
|---|---:|---:|---:|
| Complete peer baseline | 100 | 202.1 µs | 6.79 KB |
| Complete peer baseline | 1,000 | 194.9 µs | 6.79 KB |
| Complete peer baseline | 10,000 | 191.3 µs | 6.80 KB |
| Register peer | 100 | 179.9 µs | 9.69 KB |
| Register peer | 1,000 | 167.7 µs | 9.00 KB |
| Register peer | 10,000 | 169.8 µs | 9.00 KB |
| Retire old peer epoch | 100 | 199.3 µs | 10.37 KB |
| Retire old peer epoch | 1,000 | 198.6 µs | 10.37 KB |
| Retire old peer epoch | 10,000 | 200.9 µs | 10.37 KB |

## Interpretation

- Across 100→10,000 unrelated outbound payload rows, none of the three operations shows payload-proportional allocation growth.
- Measured allocation is effectively flat: baseline completion 6.79–6.80 KB, registration 9.00–9.69 KB, retirement 10.37 KB.
- Mean latency is also flat in this run: approximately 0.17–0.20 ms per operation. The small differences are not evidence that larger partitions are faster.
- This is a scaling check for the current delta-write implementation, not a before/after comparison and not a production latency SLA. It does not measure compaction, concurrent writers, relay fan-out, provider I/O, process restart or larger peer counts.
- The first run had an invalid retirement fixture (the replacement epoch used a different NodeId); that run is excluded. The figures above come from the corrected fixture, where both epochs belong to the same peer.

The raw BenchmarkDotNet CSV, Markdown and HTML reports are attached to the successful Durable Metadata Bench workflow run as the durable-metadata-benchmarks artifact. Re-run on the deployment hardware before using these figures for capacity planning.


## 2026-11-08 retry-scan allocation note

The optional `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeFingerprint` lookup returns a `ReadOnlyMemory<byte>` view over the immutable 16-byte ChangeId ledger fingerprint instead of allocating a new 16-byte array for each already-frozen provider-feed row. The coordinator compares the view directly and computes the canonical provider-operation fingerprint only when the ledger contains that ChangeId. The following direct-lookup benchmark measures the lookup itself; it does not measure the complete coordinator retry scan.


## 2026-11-08 frozen ChangeId lookup scaling

**Run:** GitHub Actions Durable Metadata Bench, commit `ed5d38f731c981362fc28b02ba5e5c3d07b7382e`  
**BenchmarkDotNet:** 0.14.0 · **Runtime:** .NET 10.0.14 · **OS:** Ubuntu 26.04.1 LTS · **Architecture:** x64 RyuJIT · **GC:** Server, concurrent  
**Job:** InProcessEmitToolchain, 3 warmup iterations and 10 measurement iterations. Each invocation performs 2,097,152 lookups; setup and durable ledger population are excluded from the measured method.

The fixture freezes 256, 1,024 or 4,096 unique outbound ChangeIds into the durable Metadata Store, then repeatedly looks up their immutable 16-byte fingerprints through the optional identity-store capability.

| Pending feed / ledger rows | Mean / lookup | Allocated / lookup |
|---:|---:|---:|
| 256 | 81.70 ns | 0 B |
| 1,024 | 81.78 ns | 0 B |
| 4,096 | 80.00 ns | 0 B |

**Interpretation:** lookup allocation remained zero at all three ledger sizes. Mean lookup latency remained approximately 80–82 ns in this CI environment. This supports the structural no-copy implementation for the direct lookup. It is not a before/after comparison or a production SLA.


## 2026-11-08 full coordinator retry after freeze / ACK interruption

**Run:** GitHub Actions Durable Metadata Bench, commit `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9`  
**Runtime:** .NET 10.0.14 · **OS:** Ubuntu 26.04.1 LTS · **CPU:** AMD EPYC 7763 (GitHub-hosted runner)  
**Job:** InProcessEmitToolchain, 3 warmup iterations and 10 measurement iterations, one publication retry per invocation.

The fixture uses the real `StorageDataProvider` and `StorageMetadataStore`. Setup first publishes a feed so the immutable Metadata Store freeze succeeds, then injects a provider ACK failure. The measured invocation retries the same still-pending feed, verifies the frozen ChangeId fingerprints, ACKs the provider feed and rehydrates the Node cache from a provider snapshot.

| Pending provider-feed rows | Mean / retry | Allocated / retry |
|---:|---:|---:|
| 256 | 2.055 ms | 1.52 MB |
| 1,024 | 14.324 ms | 6.47 MB |
| 4,096 | 34.651 ms | 33.92 MB |

**Important scope:** this is the complete `PublishLocalChanges` retry path, including provider snapshot rehydration and Node cache replacement. It is not a scan-only benchmark. `RestoreProviderSnapshot` rebuilds the namespace and copies field values, so its allocations are included. The 1,024-row case had high run-to-run variance (24.9% relative standard deviation); treat that mean as directional. The 4,096-row case was more stable (1.02%).

**Next optimization target:** keep the one-time full snapshot for initial cutover and startup reconciliation, but investigate applying the already-read provider feed incrementally to the Node cache after durable freeze. Any incremental path must preserve atomic cache updates, ChangeId duplicate suppression on ACK retry, tombstones, HLC ordering and checkpoint/restart recovery. Benchmark it against this full-path baseline before replacing the snapshot path.

## 2026-11-08 incremental Node-cache retry

**Run:** GitHub Actions Durable Metadata Bench, run `42440960333`, code head `3546461cd14b4130fe4ea3596bf74997d02f617c`  
**Runtime:** .NET 10.0.14 · **OS:** Ubuntu 26.04.1 LTS · **CPU:** AMD EPYC 9V74 (GitHub-hosted runner)  
**Job:** InProcessEmitToolchain, 3 warmup iterations and 10 measurement iterations, one stateful retry per invocation.

The coordinator now applies the exact scanned provider-feed prefix incrementally to the Node data cache after the immutable Metadata Store freeze. It checkpoints Node before acknowledging the provider feed. Startup rehydration retains the full provider snapshot path; ordinary publication retry no longer rebuilds the whole namespace snapshot.

| Pending provider-feed rows | Incremental retry mean | Allocated / retry | Previous full-snapshot retry mean | Previous allocation |
|---:|---:|---:|---:|---:|
| 256 | 1.523 ms | 875.93 KB | 2.055 ms | 1.52 MB |
| 1,024 | 3.829 ms | 3,516.38 KB | 14.324 ms | 6.47 MB |
| 4,096 | 15.869 ms | 14,148.12 KB | 34.651 ms | 33.92 MB |

**Comparison caveat:** the previous full-snapshot baseline ran on an AMD EPYC 7763 runner; this incremental run used an AMD EPYC 9V74 runner. Treat latency differences as directional, not as a controlled before/after claim. The 1,024-row previous baseline also had high variance (24.9% relative standard deviation). Current-run relative standard deviations were approximately 29.3%, 6.45% and 2.93% for 256/1,024/4,096 rows; the 256-row case is noisy. BenchmarkDotNet also warned that these single-invocation stateful iterations are shorter than its recommended 100 ms measurement window.

**Interpretation:** the current retry path reduced measured managed allocation substantially at all sizes, with the largest case falling from 33.92 MB to 14,148 KB. A same-runner baseline comparison is still required before claiming a precise latency speedup. The direct ChangeId lookup remains allocation-free; the remaining incremental-path allocations include feed materialization, metadata operation encoding and changed-record copy-on-write.

**Next:** retain this result as the incremental checkpoint, add a controlled same-runner old-vs-new comparison, and profile allocations in the feed list / operation encoding / changed-record staging before further changes. Do not replace the full snapshot used for initial cutover and startup recovery.

## 2026-11-08 matched-runner full-snapshot vs incremental comparison

**Workflow run:** `42444167368`  
**Baseline code:** `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9` (full provider snapshot rebuild on retry)  
**Incremental code:** `69ed0b5c964357a3c90552b9d697026d86280447` (incremental provider-feed application to Node cache)  
**Environment for both runs:** Ubuntu 26.04.1 LTS · AMD EPYC 7763 · .NET SDK 10.0.403 · .NET runtime 10.0.14 · BenchmarkDotNet 0.14.0. Both revisions ran sequentially in the same GitHub Actions job/runner with the same stateful benchmark configuration (3 warmups, 10 measurement iterations, one retry per invocation).

| Pending feed rows | Full snapshot mean | Incremental mean | Mean latency reduction | Full snapshot allocated | Incremental allocated | Allocation reduction |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 3.796 ms | 2.308 ms | 39.2% | 1.52 MB | 876.14 KB | 43.7% |
| 1,024 | 7.384 ms | 4.746 ms | 35.7% | 6.47 MB | 3,516.05 KB | 46.9% |
| 4,096 | 40.997 ms | 17.522 ms | 57.3% | 33.92 MB | 14,147.82 KB | 59.3% |

**Variance:** full-snapshot relative standard deviations were 36.9%, 16.1% and 2.16% for 256/1,024/4,096 rows; incremental relative standard deviations were 12.3%, 12.8% and 2.13%. The 256-row baseline is noisy; the 4,096-row comparison is stable. BenchmarkDotNet still warns that these single-invocation stateful iterations are below its recommended 100 ms measurement window.

**Decision:** retain incremental provider-feed application. On the matched runner it reduced mean latency at all three sizes and cut measured managed allocation by approximately 44–59%. The strongest stable result is the 4,096-row case: 40.997 → 17.522 ms and 33.92 MB → 14,147.82 KB. This comparison is materially stronger than the earlier cross-runner exploratory result in the previous section. Continue profiling feed materialization, operation encoding and changed-record copy-on-write; do not replace the full snapshot path used for initial cutover/startup recovery.

**Raw artifact:** GitHub Actions artifact `metadata-retry-comparison` from run `42444167368` contains both CSV and Markdown reports.