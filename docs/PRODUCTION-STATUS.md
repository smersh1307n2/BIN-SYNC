# Production status — BinSync.Net vs BIN-SYNC CORE

**Package:** BinSync.Core 3.0.0 · **Target:** net10.0  
**Branch:** feature/fld-external-storage · **PR:** #16 (open, Draft; not merged)  
**CI:** Last fully verified source/test head `b1fb68c26121d4e7d384e983355b0ac2e106bf8c`: BinSync.Net passed Release Build, 643/643 tests, all public samples, coverage threshold (≥50%) and Fuzz + Lab 31/31; FLD Storage Integration passed Release Build, 87/87 tests and both Storage array samples. Both workflows completed successfully on this head.  
**Audit status:** [first-pass normative section/contradiction audit](SPEC-COVERAGE-AUDIT.md) added; stream/fragment binding audited; Metadata Store and Data Provider semantic contracts have volatile reference fixtures and durable Storage-backed adapters. The opt-in transport reads Metadata Store Outbound/Relay pages, persists immutable per-peer reservations and ACK progress, and uses a durable namespace roster with retired-epoch fencing. Durable per-peer cursor/offset/digest state is now passed explicitly into the outbound batch encoder; the Storage-backed send path no longer restores or writes that progress into Node peer dictionaries, and it sends the immutable reservation payload without rematerializing a second Node DTO. New peers are blocked from deltas until a durable snapshot/feed baseline is committed; `AutoReconcileWithMetadataAsync` applies received snapshots to the provider before reconciliation completion, and relay history remains ACK-fenced. Lost-ACK replay is tested across Metadata Store reopen and sender Node recreation. Storage-backed receive now decodes and resolves the author without consulting Node inbox/cursor/digest state, stages the exact payload in PendingApply, and lets Metadata Store validate receipt identity, full canonical wire-header fingerprint, ChangeId equivocation, cursor and digest-chain continuity before Node mutation. Node applies record state through a metadata-authoritative path that does not publish a second InboxAuthors/PeerStates/LastBatchDigest/AcksOut receipt or relay Outbox copy. Node still keeps the HELLO/session epoch fence, alias resolution, GlobalChangeLedger application cache and data/WAL state; the legacy non-Storage transport path remains Node-owned. `FinalizePendingApply`, relay/outbound ACK watermark updates, per-stream payload compaction, `FreezeOutbound`, `AcquireOutboundLease`, `AcceptIncomingBatch`, interested-peer registration, initial-sync baseline completion and peer-epoch retirement now use row-level delta transactions rather than cloning/re-encoding the complete partition on current-format state. `PersistPartition` remains only as a one-time legacy-format migration fallback; current-format mutations no longer clone the full partition. Pending ChangeId validation indexes the pending set once per batch. Durable Inbox receipt keys now match the normative `(sender, epoch, namespace, BatchId)` identity, with legacy author-suffixed key rows normalized on read. New metadata-authoritative receipts persist the 16-byte canonical full-wire-header fingerprint (including fields not covered by BatchDigest) alongside DurableAuthor; completed PendingApply values preserve both, and the coordinator verifies both before ACK. Legacy 52/68-byte receipt rows remain readable but have no full-header proof, so a new metadata-authoritative replay using that old BatchId fails closed. PendingApply v5 persists the optional header fingerprint and authority mode; v4 authority intents without a fingerprint are deliberately recovered through the legacy Node validator. Regressions cover duplicate/conflicting BatchId reuse, header equivocation after reopen, durable-author recovery and Node recreation. Remaining gates: complete the cutover for legacy non-Storage transports and Node WAL/replay mirrors, provider rehydration/restart model, remaining cuts in the deterministic receive crash matrix, execute and archive mutation/compaction benchmarks on deployment hardware, independently proven receipt/dedup GC horizons, and normative §§4–18 audit. No percentage-based spec-parity claim is made.

Legend: **implemented** · **partial** · **helper-only** · **not implemented**

## Spec coverage

| Area | Spec | Current implementation status |
|---|---|---|
| Goals / consistency | §1 | implemented in the core model and apply path |
| Transport | §3 | generic SyncSession, Pipe/Stream/TCP adapters, negotiated per-stream Flow Control, WINDOW_UPDATE, and typed/raw fragment reassembly implemented |
| Identity / alias / epoch | §4 | implemented; b128 API normalization and wire width audited |
| HLC | §5 | implemented in µs; overflow checked before logical increment |
| Codec | §6 | implemented; b128 writer always emits exactly 16 bytes |
| Data model / lifecycle | §7 | implemented; field tombstones are ordered against live values |
| Merge policies | §8 | **partial** — Node apply path supports LWW_FIELD only; other merge helpers are standalone |
| Digests | §9 | record digest v2 distinguishes live field values from tombstones |
| Outbox / inbox | §10 | implemented |
| Membership / GC | §11 | membership and GC_NOTICE frames implemented; destructive tombstone/ghost GC is disabled by E-25 until a safe namespace frontier and stale-peer recovery protocol exist |
| Wire frames | §12 | implemented, including fragments |
| Reconciliation ladder | §13 | implemented; CI lab/fuzz coverage |
| E2EE | §14 | opt-in |
| Security | §15 | TOFU / PreShared / revoke; CA mode is a node-id allow-list, not full X.509 PKI |
| Causal dependencies | §16.19 | opt-in |
| Lab / fuzz | §17 | in CI |
| Schema evolution | §18 | **partial** — opt-in metadata compatibility gate; not a runtime merge-policy dispatcher |
| Public examples | samples | all three build and execute in CI |
| Errata E-1…E-13 | v2.3.7 | incorporated into current specification v2.4.20 FINAL |
| Errata E-24 / E-25 | v2.4.18 / v2.4.19 | normative route-independent ChangeId identity/equivocation and fail-closed tombstone retention |

## Verified correctness fixes in this audit

- HLC logical overflow is rejected before increment; no uint wrap to zero.
- A stale field write cannot resurrect a newer field tombstone; a strictly newer write supersedes it.
- A stale field tombstone cannot replace a newer live value.
- Digest calculation does not materialize the lazy empty FieldTombs dictionary.
- Record digest v2 includes a field-kind byte, so an empty live value and a tombstone with identical metadata produce different digests.
- Long identity input is normalized to exactly 16 bytes before wire encoding.
- Flow Control is negotiated by capability intersection; payload credits are reserved atomically per stream and replenished only after receive dispatch. Oversized logical frames use bounded fragment reassembly.
- Unsupported merge policy codes are rejected by ApplyBatch; legacy policy 0 is treated as LWW_FIELD, not as a schema-gate wildcard.
- Stream binding follows §3.7: control-plane frames use stream 0; namespace frames use `0x0001_0000 | namespaceId`; ACK shares its DELTA_BATCH namespace stream; WINDOW_UPDATE targets the credited stream. Receive paths reject mismatches before applying payloads.
- IBLT response receive now requires the expected namespace. MEMBERSHIP, GC_NOTICE and SCHEMA_MAP use the control stream.
- Fragment reassembly preserves frame type/stream/base sequence/flags, supports interleaved logical messages, rejects cross-stream message-id reuse, trailing fragment bytes and conflicting duplicate parts; raw receive returns the reconstructed `Frame`.

## Release gates still open

- Complete the normative audit of remaining §§4–18 and cross-check every normative scenario against executable tests. No spec-parity percentage is claimed.
- **Snapshot retention:** current snapshot completion is not an Outbox ACK, relay-retention boundary or ChangeId-ledger GC watermark. SnapshotTail/runtime DTO still lacks the per-author cut and completion/fencing proof needed for safe historical retention.
- **E-24 implemented and normative (v2.4.18):** exact global `(namespaceId, ChangeId)` identity with canonical semantic fingerprint and equivocation rejection is implemented. Origin-sequence/range compression remains an unapproved representation alternative; it must prove mesh replay, snapshot/bootstrap and equivocation safety before replacing the exact ledger.
- **E-25 normative (v2.4.19):** tombstone/ghost GC remains disabled until durable namespace membership/safe-frontier, stale-peer fencing and mandatory authoritative full-resync semantics are implemented. TTL/HLC age and GC_NOTICE alone never authorize deletion.
- **Inbox/receipt lifecycle:** Storage-backed receive uses Metadata Store receipt/digest/full-header authority and version-5 PendingApply intents. Legacy PendingApply v2/v3 rows decode with Node-owned recovery semantics; pre-fingerprint v4 authority intents are downgraded to legacy recovery. Legacy 52/68-byte receipt rows remain readable but cannot satisfy a new full-header duplicate proof. NodePersistence v12 adaptive receipts and GlobalChangeLedger remain for the legacy path and as a local apply cache. Receipt expiry/GC and independent dedup retention horizons remain open; do not couple them to relay payload ACK.
- **Peer cache pruning implemented:** `ProcessAckInternal` prunes `PeerState.Ids` only after a validated contiguous ACK covers the full author cursorEnd; `LastStreamDigest` remains as the chain checkpoint. Partial ACK and lost-ACK behavior remain covered by regression tests.
- **ProtocolStateDelta / PendingApply:** durable separate-store provider receipts, restart recovery and metadata finalization are implemented. `FinalizePendingApply` now uses a delta-only transaction; remaining work is deterministic fault injection across each durability boundary and measured scale/latency evidence.
- **FLD APPLY/relay:** APPLY_COMMIT v3 single-encoding, relay-only ApplyJournal, per-author JournalSeq ACK sidecar and post-WAL-seal relay payload compaction are implemented on the FLD branch. The cross-system JournalSeq → ChangeId → OutboxSeq provenance map, outbound publication gate and ACK translation remain open; receipt GC and bounded/rebuildable journal indexing also remain open.
- **Two-DLL FLD boundary:** architecture is decided (`Fld1.Storage.dll` + `Fld1.Database.dll`, one-way Database → Storage); extraction into the final public assemblies and Storage-only physical conformance remain implementation work.
- **Metadata Store / Data Provider:** semantic contracts and volatile reference fixtures are supplemented by durable `StorageMetadataStore` and `StorageDataProvider` adapters. Durable integration covers actual reopen, compaction, PendingApply recovery, feed paging/ACK, snapshot/feed barrier, real TCP array sync, injected failures after Node apply/before provider commit and after provider commit/before metadata finalization, plus a Node-recreation test proving the durable receive cursor/digest is sufficient when Node-local inbox/cursor/digest state is empty. `FinalizePendingApply`, outbound freeze/lease, ACK, compaction, `AcceptIncomingBatch` and peer-roster lifecycle transitions use row-level deltas on current-format partitions. Full-partition clone/diff remains only in the legacy-format migration fallback. A BenchmarkDotNet suite now measures peer registration, initial-sync baseline and peer-epoch retirement against 100/1,000/10,000 unrelated outbound payload rows. The first corrected GitHub-runner scaling results are recorded in [METADATA-STORE-BENCHMARKS.md](METADATA-STORE-BENCHMARKS.md): about 0.17–0.20 ms/op and 6.8–10.4 KB/op, effectively flat across unrelated payload size. These are scaling checks, not a before/after comparison or production SLA; deployment-hardware measurements remain required. The remaining authority gap is not the Storage-backed receive decision: that path now treats Metadata Store as authoritative. Node still owns local Outbox allocation and legacy transport receive state; its WAL/data cache and GlobalChangeLedger remain local replay/application structures until provider rehydration and the final legacy-path cutover are completed. Outbound per-peer progress and replay identity are authoritative in Metadata Store for the opt-in path, while Node supplies alias translation and operation encoding.
- **PendingLocal:** `StorageSyncCoordinator` now coalesces unpublished LWW field changes within each bounded provider-feed page by `(RecordId, FieldId)` using HLC/author ordering; record lifecycle tombstones are handled separately. Every source-page mutation is validated before coalescing, and ACK still follows Node checkpoint + metadata freeze. Cross-page durable PendingLocal staging, crash recovery of the stage/freeze handoff and full source-feed ACK decoupled from Node checkpoint remain open. Never rewrite an unacknowledged/published Outbox entry.
- **Runtime persistence refactor:** Node dictionaries plus NodePersistence/WAL remain authoritative in the current runtime. Moving them behind the neutral Metadata Store boundary is not implemented.
- **Storage-only role:** the generic `Fld1.Storage` engine is implemented in the BIN-SYNC branch and is used directly by durable Storage adapters without fabricating synchronized field records or synthetic HLC/author/ChangeId/Outbox values. Final standalone packaging and the authoritative Node/Transport migration remain open.
- Confirm final documentation/specification parity on the complete branch diff.
- Review PR #16 diff and CI once more; only then request explicit approval to merge into main.

**Merge status:** PR #16 remains Draft and unmerged. No merge is authorized by this status document.


## Current continuation checkpoint

- Last fully verified source/test head: `b7694c4b40f9e09c8a7a6d9cc7f1860dd2c5c716`; current documentation is being updated from this verified checkpoint.
- Added delta-only PendingApply finalization: transaction size is bounded by the incoming batch plus newly required peer-stream rows, not by the number of unrelated records in the partition.
- Added a durable reopen regression that finalizes a second batch after existing relay state, verifies receive cursor/relay high-watermark and payload ordering after reopen, and verifies idempotent completed-token retry.
- Added explicit crash-boundary and retention-horizon requirements to `METADATA-STORE-AUTHORITY-MIGRATION.md`.
- Current verified CI is green on source/test head `b7694c4b`: 642 core tests, 83 durable Storage tests, 31 Fuzz/Lab cases, coverage threshold and all public/Storage samples.


**Continuation checkpoint:** source/test head `b1fb68c26121d4e7d384e983355b0ac2e106bf8c` includes delta-only interested-peer registration, initial-sync baseline completion and peer-epoch retirement, in addition to delta-only FreezeOutbound, AcquireOutboundLease, AcceptIncomingBatch, ACK/compaction and PendingApply finalization. The provider-snapshot rehydration helper now explicitly preserves local ChangeId identities (`SeenChanges`) while removing pending remote IDs from the Node application cache; a regression covers both origins. Peer lifecycle transitions now commit roster, stream, required-stream and retired-epoch rows atomically; idempotent calls do not write. Full-partition clone/diff remains only for legacy-format migration. The Metadata Store reservation is the replay identity authority; the Node-backed legacy transport remains separate. The receive crash suite verifies both provider-not-committed recovery and provider-committed/metadata-not-finalized idempotent recovery. CI is green: 642 core tests, 83 durable Storage tests, 31 Fuzz/Lab cases, coverage threshold and all public/Storage sample executions passed.

- Relay/outbound ACK and payload compaction paths, outbound freeze/lease updates, incoming-batch acceptance and peer roster/baseline/retirement transitions now commit only affected rows on current-format partitions. The new peer-lifecycle BenchmarkDotNet suite builds in CI; controlled allocation/latency results remain outstanding.
