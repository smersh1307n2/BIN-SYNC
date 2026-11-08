# Production status — BinSync.Net vs BIN-SYNC CORE

**Package:** BinSync.Core 3.0.0 · **Target:** net10.0  
**Branch:** feature/fld-external-storage · **PR:** #16 (open, Draft; not merged)  
**CI:** Latest verified source/test head `d3364d310db36cdd84c6e01f379de3ab29f3a112` (2026-11-08): BinSync.Net Release Build and 644/644 tests passed; Fuzz + Lab 31/31 passed; coverage 70.9%; all public samples passed. FLD Storage Integration Release Build and 92/92 tests passed; both Storage array samples, including real TCP, passed. GitHub Actions runs: BinSync.Net #2862 and FLD Storage Integration #962; both successful.
**Audit status:** [first-pass normative section/contradiction audit](SPEC-COVERAGE-AUDIT.md) added; stream/fragment binding audited; Metadata Store and Data Provider semantic contracts have volatile reference fixtures and durable Storage-backed adapters. The opt-in transport reads Metadata Store Outbound/Relay pages, persists immutable per-peer reservations and ACK progress, and uses a durable namespace roster with retired-epoch fencing. Durable per-peer cursor/offset/digest state is now passed explicitly into the outbound batch encoder; the Storage-backed send path no longer restores or writes that progress into Node peer dictionaries, and it sends the immutable reservation payload without rematerializing a second Node DTO. New peers are blocked from deltas until a durable snapshot/feed baseline is committed; `AutoReconcileWithMetadataAsync` applies received snapshots to the provider before reconciliation completion, and relay history remains ACK-fenced. Lost-ACK replay is tested across Metadata Store reopen and sender Node recreation. Storage-backed receive now decodes and resolves the author without consulting Node inbox/cursor/digest state, stages the exact payload in PendingApply, and lets Metadata Store validate receipt identity, full canonical wire-header fingerprint, ChangeId equivocation, cursor and digest-chain continuity before Node mutation. Node applies record state through a metadata-authoritative path that does not publish a second InboxAuthors/PeerStates/LastBatchDigest/AcksOut receipt or relay Outbox copy. Node still keeps the HELLO/session epoch fence, alias resolution, GlobalChangeLedger application cache and data/WAL state; the legacy non-Storage transport path remains Node-owned. `FinalizePendingApply`, relay/outbound ACK watermark updates, per-stream payload compaction, `FreezeOutbound`, `AcquireOutboundLease`, `AcceptIncomingBatch`, interested-peer registration, initial-sync baseline completion and peer-epoch retirement now use row-level delta transactions rather than cloning/re-encoding the complete partition on current-format state. `PersistPartition` remains only as a one-time legacy-format migration fallback; current-format mutations no longer clone the full partition. Pending ChangeId validation indexes the pending set once per batch. Durable Inbox receipt keys now match the normative `(sender, epoch, namespace, BatchId)` identity, with legacy author-suffixed key rows normalized on read. New metadata-authoritative receipts persist the 16-byte canonical full-wire-header fingerprint (including fields not covered by BatchDigest) alongside DurableAuthor; completed PendingApply values preserve both, and the coordinator verifies both before ACK. Legacy 52/68-byte receipt rows remain readable but have no full-header proof, so a new metadata-authoritative replay using that old BatchId fails closed. PendingApply v5 persists the optional header fingerprint and authority mode; v4 authority intents without a fingerprint are deliberately recovered through the legacy Node validator. Regressions cover duplicate/conflicting BatchId reuse, header equivocation after reopen, durable-author recovery and Node recreation. Remaining gates: complete the cutover for legacy non-Storage transports and Node WAL/replay mirrors, complete process-restart orchestration for the explicit provider-authoritative rehydration mode, remaining cuts in the deterministic receive crash matrix, execute and archive mutation/compaction benchmarks on deployment hardware, independently proven receipt/dedup GC horizons, and normative §§4–18 audit. No percentage-based spec-parity claim is made.

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


**Continuation checkpoint:** source/test head `69c91fc245f1f1da849edd96d6fe9443060880bb` includes delta-only interested-peer registration, initial-sync baseline completion and peer-epoch retirement, in addition to delta-only FreezeOutbound, AcquireOutboundLease, AcceptIncomingBatch, ACK/compaction and PendingApply finalization. `Node.RestoreProviderSnapshot` preserves local ChangeId identities (`SeenChanges`) while removing pending remote IDs from the Node application cache; regressions cover both origins and reject duplicate/reserved snapshot rows atomically. `StorageSyncCoordinator.RehydrateNodeFromProviderSnapshot` now exposes an explicit provider-authoritative startup/recovery path: it freezes provider-feed changes, restores a stable provider snapshot, checkpoints the Node base and replays durable PendingApply intents. It must only be used when all local mutations are captured by the provider feed; the default merge-based coordinator path remains unchanged for mixed Node/provider-originated writes. Peer lifecycle transitions now commit roster, stream, required-stream and retired-epoch rows atomically; idempotent calls do not write. Full-partition clone/diff remains only for legacy-format migration. The Metadata Store reservation is the replay identity authority; the Node-backed legacy transport remains separate. The receive crash suite verifies both provider-not-committed recovery and provider-committed/metadata-not-finalized idempotent recovery. CI is green: 642 core tests, 83 durable Storage tests, 31 Fuzz/Lab cases, coverage threshold and all public/Storage sample executions passed.

- Relay/outbound ACK and payload compaction paths, outbound freeze/lease updates, incoming-batch acceptance and peer roster/baseline/retirement transitions now commit only affected rows on current-format partitions. The new peer-lifecycle BenchmarkDotNet suite builds in CI; controlled allocation/latency results remain outstanding.


## Latest continuation checkpoint — 2026-11-08

- PR #16 remains open, Draft and unmerged. Latest branch HEAD: `c089bbed630edbdd8f3ecb0260b7a403239ba371`.
- Added explicit `StorageSyncCoordinator.RehydrateNodeFromProviderSnapshot` for provider-authoritative startup: drain/freeze provider local feed, install a stable provider snapshot into the Node namespace cache, checkpoint, then replay durable PendingApply intents.
- Added integration coverage for stale Node-cache replacement, durable local-feed freeze and PendingApply replay after interruption.
- Verified on this HEAD: BinSync.Net 644/644; Fuzz + Lab 31/31; Storage Integration 88/88; coverage 70.9%; public and Storage samples pass.
- This is a bounded rehydration milestone, not full Node/Metadata Store authority cutover. Node Outbox allocation, NodePersistence/WAL replay mirrors and the legacy non-Storage transport path remain open gates. Provider-authoritative rehydration must not be used when local Node writes bypass the provider feed.


## Latest continuation checkpoint — namespace-scoped recovery (2026-11-08)

- `StorageSyncCoordinator.RecoverPendingApplies` and provider-authoritative rehydration now process only intents for the coordinator's namespace; other namespace intents remain durable for their own coordinators.
- Provider-authoritative rehydration refuses to replace the Node cache while a current-namespace PendingApply has no replayable wire payload. Such a legacy intent must first complete through `RecoverPendingApplies`.
- Added integration coverage for two namespaces sharing one Metadata Store scope, foreign-intent isolation and fail-closed legacy recovery.
- Verified source/test head `c71ec35480245f085c5aa76b337e275deb3e0c81`: 644 core, 89 Storage Integration, 31 Fuzz/Lab, 70.9% coverage; all samples pass.


## Latest continuation checkpoint — durable source identity and incremental freeze (2026-11-08)

- `ProviderChange.ChangeId` is now a stable 128-bit source identity. `StorageDataProvider` embeds it in the existing local-feed row, migrates legacy rows on first read, preserves it across reopen and removes it with the acknowledged feed row.
- `MetadataStore.FreezeOutbound` now supports incremental per-ChangeId replay: existing frozen entries must be an ordered prefix; their sequences remain unchanged, unseen entries are appended in the same transaction, and conflicting/out-of-order reuse fails without advancing the high watermark.
- Added durable reopen coverage for both source ChangeId migration and partial outbound freeze retry.
- Verified source/test head `d3364d310db36cdd84c6e01f379de3ab29f3a112`: 644 core, 92 Storage Integration, 31 Fuzz/Lab, 70.9% coverage; all samples pass.
- Still not complete: `StorageSyncCoordinator.PublishLocalChanges` uses the Node-Outbox mirror path. The next cutover is direct provider-feed-to-Metadata-Store freeze using stable ChangeId, then provider-snapshot cache installation and feed ACK.


## Latest continuation checkpoint — provider authority, Node payload reclamation and Storage writer fencing (2026-11-08)

- Code/test HEAD: `be0d48db38e1c7f56cbd392311204a89120e72c5` (644/644 core, 102/102 Storage Integration, 31/31 Fuzz/Lab, 70.8% coverage; all samples pass). The base `IMetadataStore` contract remains source-compatible: outbound ChangeId lookup is the optional `IMetadataOutboundChangeIdentityStore` capability.
- Added optional `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeFingerprint` backed by the durable global ChangeId ledger. Storage and volatile conformance adapters implement the lookup; Storage keeps the identity available after payload compaction without breaking third-party `IMetadataStore` implementations.
- The lookup now returns a `ReadOnlyMemory<byte>` view over the immutable ledger fingerprint, avoiding an additional 16-byte copy per already-frozen feed row during retry scans; this structural allocation reduction is documented but not yet measured in BenchmarkDotNet.
- Fixed interrupted-publication retry: if Metadata Store freeze commits but provider feed ACK fails, a newer edit may arrive before retry. The coordinator recognizes already-frozen ChangeIds, validates fingerprints, preserves immutable published operations and coalesces only the not-yet-frozen suffix.
- Cutover is explicit and durable: provider feed empty/acknowledged, Node and Metadata Store high-watermarks aligned, provider marker persisted. Metadata Store assigns new outbound sequences; Node's local author sequence is fenced.
- After the marker is durable, duplicate local-author Outbox payload rows are removed from Node. The high-watermark and WAL/ChangeId replay identities remain. Startup reconciliation repeats cleanup if it loads a stale pre-cleanup Node checkpoint.
- `AutoReconcileWithMetadataAsync` detects provider-authoritative mode and rehydrates the Node namespace cache from the provider snapshot before anti-entropy. The legacy merge-based path remains for non-cutover providers.
- Added recovery tests for provider ACK interruption with a later edit, post-ACK checkpoint failure/reopen, stale checkpoint replacement, local Outbox payload cleanup and missing cutover-marker fail-closed behavior.
- Added a process-local shared transaction gate across provider, metadata and Node checkpoint adapters attached to one StorageDatabase. Added a named OS-managed Mutex fence per WAL partition; the fence is acquired before snapshot/WAL recovery and released on database disposal/process termination. The Storage API remains BCL-only.
- Verified GitHub Actions on this code head: BinSync.Net 644/644; Storage Integration 102/102; Fuzz + Lab 31/31; line coverage 70.8%; in-process and real-TCP Storage samples PASS.
- PR #16 remains Draft, open and unmerged. No merge was performed.

**Remaining:** separate-process lock/abandoned-mutex tests on each supported OS, symlink/alias path canonicalization, complete runtime-wide Node/Transport authority removal (legacy Node-backed transport and other-author relay Outbox state remain), physical Storage crash injection/power-loss cuts, controlled BenchmarkDotNet measurements at increasing partition sizes, independent receipt/ChangeId/tombstone GC horizons, and normative cross-implementation conformance. FLD Database and its JournalSeq provenance adapter remain deferred.
