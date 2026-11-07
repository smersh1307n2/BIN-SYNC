# Production status — BinSync.Net vs BIN-SYNC CORE

**Package:** BinSync.Core 3.0.0 · **Target:** net10.0  
**Branch:** feature/fld-external-storage · **PR:** #16 (open, Draft; not merged)  
**CI:** Last fully verified source head `0825a22644f266fd90163eafdfa88aa7c91a2eba`: BinSync.Net passed Release Build, 642/642 tests, all public samples, coverage threshold (≥50%) and Fuzz + Lab 31/31; FLD Storage Integration passed Release Build, 80/80 tests and both Storage array samples. Both workflows completed successfully. The current PR head `50c0d7a9` adds only the corresponding migration-document update.  
**Audit status:** [first-pass normative section/contradiction audit](SPEC-COVERAGE-AUDIT.md) added; stream/fragment binding audited; Metadata Store and Data Provider semantic contracts have volatile reference fixtures and durable Storage-backed adapters. The opt-in transport reads Metadata Store Outbound/Relay pages, persists immutable per-peer reservations and ACK progress, and uses a durable namespace roster with retired-epoch fencing. Durable per-peer cursor/offset/digest state is now passed explicitly into the outbound batch encoder; the Storage-backed send path no longer restores or writes that progress into Node peer dictionaries, and it sends the immutable reservation payload without rematerializing a second Node DTO. New peers are blocked from deltas until a durable snapshot/feed baseline is committed; `AutoReconcileWithMetadataAsync` applies received snapshots to the provider before reconciliation completion, and relay history remains ACK-fenced. Lost-ACK replay is tested across Metadata Store reopen and sender Node recreation. Receive cursor/digest/ChangeId checks run in Metadata Store before Node mutation, while Node still repeats validation and retains its inbox/digest mirror. `FinalizePendingApply`, relay/outbound ACK watermark updates, and per-stream payload compaction now use row-level delta transactions rather than cloning/re-encoding the complete partition for those hot paths; a durable reopen regression covers preservation of existing relay state. Pending ChangeId validation indexes the pending set once per batch. Durable Inbox receipt keys now match the normative `(sender, epoch, namespace, BatchId)` identity, with legacy author-suffixed key rows normalized on read; durable `MetadataBatchReceipt` values retain and return `DurableAuthor` across reopen and completed PendingApply retries, and the coordinator verifies the receipt author before ACK. Regressions cover duplicate/conflicting BatchId reuse and durable-author recovery. Remaining gates: full Node/Transport authority cutover, provider rehydration/restart model, deterministic receive crash matrix, benchmarks and remaining whole-partition mutation paths, independently proven receipt/dedup GC horizons, and normative §§4–18 audit. No percentage-based spec-parity claim is made.

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
- **Inbox/receipt lifecycle:** NodePersistence v12 adaptive receipt encoding and global 16-byte ChangeId ledger are implemented. Receipt expiry/GC and independent dedup retention horizons remain open; do not couple them to relay payload ACK.
- **Peer cache pruning implemented:** `ProcessAckInternal` prunes `PeerState.Ids` only after a validated contiguous ACK covers the full author cursorEnd; `LastStreamDigest` remains as the chain checkpoint. Partial ACK and lost-ACK behavior remain covered by regression tests.
- **ProtocolStateDelta / PendingApply:** durable separate-store provider receipts, restart recovery and metadata finalization are implemented. `FinalizePendingApply` now uses a delta-only transaction; remaining work is deterministic fault injection across each durability boundary and measured scale/latency evidence.
- **FLD APPLY/relay:** APPLY_COMMIT v3 single-encoding, relay-only ApplyJournal, per-author JournalSeq ACK sidecar and post-WAL-seal relay payload compaction are implemented on the FLD branch. The cross-system JournalSeq → ChangeId → OutboxSeq provenance map, outbound publication gate and ACK translation remain open; receipt GC and bounded/rebuildable journal indexing also remain open.
- **Two-DLL FLD boundary:** architecture is decided (`Fld1.Storage.dll` + `Fld1.Database.dll`, one-way Database → Storage); extraction into the final public assemblies and Storage-only physical conformance remain implementation work.
- **Metadata Store / Data Provider:** semantic contracts and volatile reference fixtures are supplemented by durable `StorageMetadataStore` and `StorageDataProvider` adapters. Durable integration covers actual reopen, compaction, PendingApply recovery, feed paging/ACK, snapshot/feed barrier and real TCP array sync. `FinalizePendingApply` is now delta-only, but general `PersistPartition` transitions still materialize/diff whole partitions. The remaining authority gap is not adapter durability: Node/Transport still owns local Outbox allocation and receive inbox/cursor/digest decisions; the Metadata Store is not yet the sole runtime authority. Outbound per-peer progress and replay identity are authoritative in Metadata Store for the opt-in path, while Node supplies alias translation and operation encoding.
- **PendingLocal:** `StorageSyncCoordinator` now coalesces unpublished LWW field changes within each bounded provider-feed page by `(RecordId, FieldId)` using HLC/author ordering; record lifecycle tombstones are handled separately. Every source-page mutation is validated before coalescing, and ACK still follows Node checkpoint + metadata freeze. Cross-page durable PendingLocal staging, crash recovery of the stage/freeze handoff and full source-feed ACK decoupled from Node checkpoint remain open. Never rewrite an unacknowledged/published Outbox entry.
- **Runtime persistence refactor:** Node dictionaries plus NodePersistence/WAL remain authoritative in the current runtime. Moving them behind the neutral Metadata Store boundary is not implemented.
- **Storage-only role:** the generic `Fld1.Storage` engine is implemented in the BIN-SYNC branch and is used directly by durable Storage adapters without fabricating synchronized field records or synthetic HLC/author/ChangeId/Outbox values. Final standalone packaging and the authoritative Node/Transport migration remain open.
- Confirm final documentation/specification parity on the complete branch diff.
- Review PR #16 diff and CI once more; only then request explicit approval to merge into main.

**Merge status:** PR #16 remains Draft and unmerged. No merge is authorized by this status document.


## Current continuation checkpoint

- Last fully verified source head: `0825a22644f266fd90163eafdfa88aa7c91a2eba`; current documentation head: `50c0d7a9c1e034a3caeec364342a7b1b708c085d`.
- Added delta-only PendingApply finalization: transaction size is bounded by the incoming batch plus newly required peer-stream rows, not by the number of unrelated records in the partition.
- Added a durable reopen regression that finalizes a second batch after existing relay state, verifies receive cursor/relay high-watermark and payload ordering after reopen, and verifies idempotent completed-token retry.
- Added explicit crash-boundary and retention-horizon requirements to `METADATA-STORE-AUTHORITY-MIGRATION.md`.
- Current-head CI remains an open verification gate; the previously green 78/78 Storage run predates these latest commits.


**Continuation checkpoint:** source head `0825a22644f266fd90163eafdfa88aa7c91a2eba` additionally removes the Storage-backed outbound ACK/digest mirror into Node and passes `MetadataPeerStreamState` directly to the batch encoder. The reservation in Metadata Store is the replay identity authority; the Node-backed legacy transport remains separate. CI is green: 642 core tests, 80 durable Storage tests, 31 Fuzz/Lab cases, coverage threshold and all public/Storage sample executions passed. The current PR head is `50c0d7a9c1e034a3caeec364342a7b1b708c085d` (documentation-only after that source verification).

- Relay/outbound ACK and payload compaction paths now commit only their affected watermark/payload rows; general `PersistPartition` operations such as freeze and direct shared-transaction incoming acceptance still use whole-partition diffing.
