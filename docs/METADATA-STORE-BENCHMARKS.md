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
| 256 | 57.02 ns | 0 B |
| 1,024 | 57.85 ns | 0 B |
| 4,096 | 59.57 ns | 0 B |

**Interpretation:** lookup allocation remained zero at all three ledger sizes. Mean lookup latency remained approximately 57–60 ns in this CI environment. This supports the structural no-copy implementation for the direct lookup. It is not a before/after comparison, not a production SLA, and not evidence that the complete ACK-retry scan is allocation-free; coordinator-level retry benchmarks and deployment-hardware measurements remain open.
