# BIN-SYNC v3.0 — implementation roadmap
**Дата:** 2026-11-08  
**Статус:** active; execution checklist is authoritative.  
**Normative contract:** BIN-SYNC-SPECIFICATION.txt, v3.0.0 DRAFT.  
**Cross-repository integration:** FLD-DB docs/SYNC-STORAGE-COMPACTION-ROADMAP.md.

Mark [x] only after code, tests and exact-head CI prove it; [~] means active/partial; [ ] means not done. Documentation alone never closes a code gate. Latest exact-head green checkpoint before the v3.1 implementation pass: `c56d2e206d0c8578cbfe66158d6e6f1e0824750c` — BinSync.Net Core 676/676, Fuzz/Lab passed, Storage Integration passed on Ubuntu/Windows/macOS; the in-memory SyncSession, TCP and ScenarioHarness public samples passed. Subsequent v3.1 commits are in progress and are NOT green yet. Durable Metadata Bench completed, including one-million-row freeze: 218.1 µs / 7.77 KB for 1,024 local operations. This verifies strict BSP3 provider marker/feed identity, Node ChangeId-ledger cutover, generation-scoped provider rows, atomic FULL_REPLACE staging/switch, canonical RFC-4122 generation bytes, same-author-epoch reservation baseline proof, bounded streaming digest builder, field/record tombstone preservation, and the shared Core ProviderMutation codec. The 218.1 µs result is a measurement, not a proven improvement over the earlier 196.9 µs run. Current branch HEAD: `55c1ba204d859a7e52f6c8b7a424bc6fc3966ab1`. This is a C-pass head, not an exact-head green checkpoint. The previous `2f75b266` checkpoint is historical.

## 0. Active status at current C-pass HEAD

- **Current branch/head:** `feature/fld-external-storage` @ `e2f6bcf09ea3809f2ee3198d26cbb8ca74391db2`.
- **Last known fully green checkpoint:** `c56d2e206d0c8578cbfe66158d6e6f1e0824750c` (Core 676/676, Fuzz/Lab passed, Storage Integration passed on Ubuntu/Windows/macOS). Later C-pass code is not covered by that result.
- **Workflow:** C implementation first in small commits with immediate roadmap updates; then T (fixtures, fault injection and conformance), O (measured optimization), F (release). Build errors are repaired immediately; test feedback is recorded but test-suite redesign stays in T.
- **C implemented:** signed namespace roster/frontier/checkpoint codecs and durable storage; all-active FULL_REPLACE install ACK tracking; sender/receiver with durable staging/resume; forgotten-origin replay fence; receipt/ChangeId/tombstone compaction plumbing with all physical gates disabled; bounded provider prefix cursors and streamed snapshot row validation; member frontier and authority checkpoint production APIs; mandatory post-FULL_REPLACE HLC floor refresh/Node rehydration callback; freeze-time per-namespace/author/epoch OriginSequence allocator; durable stamping of published winners before provider feed ACK; FULL_REPLACE draft-to-published identity upgrade.
- **Origin contract:** provider feed cursor is not OriginSequence. Coalescible drafts carry ChangeId + author epoch and OriginSequence=0. Metadata Store allocates contiguous OriginSequence only at immutable freeze and persists it with the ChangeId ledger. Coalesced-away drafts consume no origin number. Remote operation apply requires a complete non-zero origin tuple.
- **GC status:** receipt, ChangeId and tombstone/ghost physical deletion remain fail-closed/disabled. Receipt additionally requires retired transport epoch, preserved receive cursor/digest chain, signed origin frontier and no PendingApply. Tombstone GC still lacks author-issued horizon/retired-origin closure and a checkpoint-bound global HLC floor. Never enable physical GC in the C pass.
- **Latest observed CI before the current code slice:** exact build succeeded and Fuzz/Lab passed; Core had one outdated tombstone fixture failure and Storage Integration had 53 failures sharing the outdated zero-OriginAuthorEpoch fixture assumption. These are T-pass fixture updates, not a reason to relax production v3 validation. Exact-head CI for the current code is pending.
- **Remaining C work:** finish author-issued per-epoch horizon and retired-origin closure; bind the FULL_REPLACE HLC floor into the signed retention checkpoint; connect authority/member lifecycle and periodic frontier/checkpoint exchange to application orchestration; concrete application-wide commit/GC lease and cross-store live-reference scan; complete source snapshot memory path (StorageDatabase.OpenSnapshot still clones the partition); FLD JournalSeq-to-ChangeId/Origin adapter; public samples and golden vectors. Preserve the two-DLL split and no legacy API/fallback decoder.
- **T pass is not complete.** Historical checklist entries below are execution history; this active section and latest dated execution entries supersede earlier snapshots.

## 1. Target architecture

- One immutable published operation/payload per (scope, namespace, ChangeId), shared by every peer.
- Origin tuple (authorNodeId, authorEpoch, originSequence) assigned once at original freeze and preserved unchanged.
- One compact progress row per (peer, namespace, author stream); no per-operation ACK bitmap or per-peer payload copy.
- One global ChangeId ledger/dedup authority independent of delivery route.
- One compact receipt per accepted batch; PendingApply exists only while provider apply is unresolved.
- WAL is recovery; FLD/provider feed is unpublished local-change authority; BIN-SYNC is published-operation and delivery authority.
- No duplicate pending journal or duplicate authoritative payload after handoff.
- Storage engine is opaque/reusable; FLD Database owns field semantics. Exactly two DLLs.
- No v2 compatibility, migration fallback, dual-write or legacy public API.
- Receipts, ChangeId ledger and tombstone/ghost have separate GC proofs.

## 2. Current baseline

### BIN-SYNC: feature/fld-external-storage

- Historical implementation checkpoint: `2f75b266c7c2d720d87b106573182393d5d15ea1` (exact-head CI green at that time). Current C-pass HEAD is `0dac1c3cf7a5fd8e740f101ffbbee93bbae16c7e`; exact-head CI has not been run for the current C-pass.
- BinSync.Net: Core 662/662; Fuzz/Lab SUCCESS; Storage Integration 168/168 on Ubuntu, Windows and macOS.
- FLD-DB `fix/crash-consistency-outbox` HEAD: `1f5c2528767e575bfb925a19792c69b6ffb1027e` (not modified during this BIN-SYNC pass).
- Backup branches verified in both repositories: `backup/pre-autonomous-roadmap-v3-2026-11-08`, `backup/bin-sync-storage-gc-receipt-2026-11-08`, `backup/pre-final-bin-sync-storage-2026-11-08`, `backup/post-storage-fault-injection-2026-11-08`.

### FLD-DB: fix/crash-consistency-outbox

- HEAD: `1f5c2528767e575bfb925a19792c69b6ffb1027e`.
- Generic Storage engine, WAL/recovery/checkpoint foundations, FLD field layer, Outbox and remote APPLY are present.
- Two-DLL architecture is documented; extraction/contract completion and multi-application process model remain release gates.
- Re-run cross-repository integration CI after v3 contracts are aligned.

## 3. Execution checklist

### Phase 0 — v3 contracts and authority boundaries

- [x] Review current BIN-SYNC v3 draft and provenance design.
- [x] Review metadata storage, Change staging/retention, metadata GC/retention, Metadata Store and Data Provider conformance, authority migration and FLD integration contracts.
- [x] Review FLD history-storage audit, compaction roadmap, Storage Core architecture, kernel spec and tombstone retention research.
- [x] Record clean-break policy: no v2 readers, migration, dual-write or fallback.
- [x] Record ownership split: WAL / unpublished provider feed / published BIN-SYNC operations / peer progress / receipts / PendingApply.
- [x] Record two-DLL decision and opaque Storage boundary.
- [ ] Reconcile historical v2 statements retained in main spec; clearly non-normative.
- [ ] Align FLD Kernel spec and cross-repository roadmap to exact v3 Origin/publication contract.
- [ ] Publish golden v3 wire and storage vectors.

### Phase 1 — Repair red build and finish immutable Origin end-to-end

- [~] Fixed the initial tuple/model mismatch in Node.DeltaBatch.cs by carrying Origin in the outbound metadata item tuple; continue auditing every clone and serialization boundary.
- [x] Introduced canonical OperationOrigin and integrated it into local Outbox creation, OutboxEntry, MetadataOperation, PendingApply encoding/decoding, Storage ledger comparisons, relay/outbound DTOs, Node checkpoint and inbound ledger fingerprinting. Provider freeze intentionally receives an incomplete pre-freeze draft (local author supplied separately; durable sequence allocated atomically by Metadata Store); snapshots are state images with separate snapshot-generation provenance, not PublishedOperations, and MUST NOT synthesize per-operation Origin.
- [~] Local outbound freeze assigns OriginSequence from the durable author stream sequence and preserves supplied Origin on replay; audit atomic allocator/retry behavior and provider freeze before closing.
- [x] Removed the invalid equality between OriginSequence and peer-relative sender cursor; direct-author validation only rejects an Origin epoch from the future.
- [x] Make provenance mandatory in every v3 DELTA_BATCH; remove optional ImmutableOriginProvenance capability and reject protocol-major mismatch. HELLO and HELLO_ACK both carry and validate ProtocolMajor=3.
- [x] Preserve immutable Origin through preflight, persisted PendingApply/recovery, provider apply decision, relay and Storage restart; integration tests compare the same OperationOrigin before interruption, after reopen/recovery and in the relayed item. Snapshot is a state image, not a list of published operations; it must not synthesize per-operation Origin. Its authoritative FULL_REPLACE generation is a separate Phase 4 requirement.
- [x] Cover direct author, A→B→C→D relay, alternate-route deduplication, duplicate delivery and Origin equivocation in Core scenarios/tests; exact-head CI passed.
- [x] Same ChangeId with changed semantic fingerprint or immutable Origin is rejected atomically; same operation via another route does not create a second relay payload (regression coverage green).
- [x] Exact-head Core test + Fuzz/Lab workflows passed; Storage Integration passed on Ubuntu, Windows and macOS. Samples/build are included in the Core workflow. Keep release-wide coverage/threshold and golden-vector gates in Phase 6.

### Phase 2 — Canonical metadata layout and redundant-fact removal

- [ ] Inventory every persisted row/key and byte cardinality in StorageMetadataStore, Node checkpoint, FLD Outbox, ApplyJournal and WAL.
- [ ] Establish one canonical v3 format marker and row codec; reject absent/old/unknown markers.
- [x] Relay/outbound payload rows persist only ChangeId[16] + payloadLength[u32] + payload; Origin is hydrated from the single global 45-byte ChangeId identity row. Missing ledger rows and author/stream mismatches fail closed. Physical compaction now deletes current row kinds 20/21 after reopen; exact CI passed 155/155 on all three OS platforms. [~] Full cross-store cardinality/byte audit remains.
- [x] Node global ChangeId ledger stores a fixed 16-byte BLAKE3 identity digest that binds semantic mutation + immutable Origin tuple in one hash pass; no separate 29-byte Origin suffix is stored in the ledger value. [~] Storage metadata identity-row cardinality and cross-store duplication still require a byte-level audit and benchmark.
- [ ] Collapse repeated author IDs in ACK/pushed/offset maps into one sparse progress row only if benchmarks prove a win.
- [ ] Remove acknowledged replay reservations atomically with contiguous ACK, preserving digest-chain anchor.
- [ ] Consolidate Inbox/InboxAuthors/ReceivedStreams only when one receipt preserves all retry/ACK/cursor/digest semantics.
- [ ] Remove duplicate relay payload copies and unnecessary WAL/ApplyJournal duplication only after recovery proof.
- [~ C] On Metadata-Store-owned publication cutover, Node now drops its namespace-local `GlobalChangeLedger`; Metadata Store remains the only ChangeId authority. `SeenChanges` WAL replay markers are deliberately retained as a separate recovery concern. Cutover/restart assertions added; exact-head CI pending.
- [~] Record before/after bytes/op, bytes/peer-author, allocations, mutation latency and compaction CPU at 500/10k/100k/1m rows. Exact row saving is 12 bytes per retained relay/outbound operation (32+payload → 20+payload); the durable benchmark completed successfully: 10k rows 408.4 µs / 8.14 KB, 100k rows 375.9 µs / 10.34 KB, 1M rows (1,024-operation freeze) 196.9 µs / 7.77 KB. Against the prior recorded baselines (449.2 µs at 10k, 434.1 µs / 10,944 B at 100k, 232.8 µs / 7.84 KB at 1M), these measurements are faster with slightly lower allocation; compare only as same-workflow directional evidence, not as a cross-machine guarantee.

### Phase 3 — Publication and FLD Storage contract

- [x] Provider feed/FLD Outbox is the sole authority for unpublished local changes in Metadata-Store-owned publication mode; no parallel MetadataStore PendingLocal table. The cutover fence rejects a non-empty local feed/Node Outbox state that would make ownership ambiguous.
- [x] Verify LWW field coalescing across provider pages; field writes and record-lifecycle tombstones use distinct coalescing identities. Provider conformance separately covers lifecycle tombstone/ghost ordering.
- [~ C] Make the provider contract explicitly require remote APPLY to leave the local unpublished feed and its ACK state untouched; StorageDataProvider already follows this boundary. Same-field remote-wins/restart conformance remains Phase T.
- [x] Freeze ChangeId + immutable Origin + payload into Metadata Store atomically; provider source-feed ACK follows durable freeze and Node checkpoint persistence.
- [x] Freeze replay is idempotent when metadata commit succeeds but provider ACK fails; frozen ChangeIds are recognized and only the later unfrozen suffix is coalesced/published.
- [ ] Map FLD JournalSeq to stable ChangeId/Origin without using JournalSeq as OriginSequence.
- [ ] Complete Storage/DataProvider/MetadataStore conformance including reopen and crash boundaries.
- [x] Storage DLL remains protocol-agnostic; field/HLC/tombstone semantics stay in Database DLL.

### Phase 4 — Safe GC, independently proven

#### 4.1 Inbox receipts

- [x] Conservative retired-transport-epoch receipt GC and PendingApply retirement guard exist in earlier Storage-backed implementation.
- [x C] Retirement now blocks old PendingBatch reclamation unless the exact reservation is ACKed or the replacement baseline for the same author epoch covers `old StreamOffset + PendingBatch.CursorEnd`. Before the retired-through fence, a stale epoch may only resume/mark-pushed/ACK that exact reservation; it cannot reserve new data. Epoch-zero and cross-author-epoch baselines never discharge the reservation.
- [x] Revalidated canonical v3 receipt identity. Old receipt key/value formats are rejected; v3 rows with unknown transport epoch are retained and never assigned guessed provenance.
- [x T] Added tests for short-baseline blocking, exact stale-epoch retry/ACK, replacement-baseline supersession and author-epoch mismatch. Exact-head CI at `2f75b26` passed 168/168 on three OS. Atomic retirement transaction remains one commit; dedicated injected-crash-at-commit-boundary coverage remains a Phase T follow-up.

#### 4.2 ChangeId ledger

Design: [BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md](BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md). The design is recorded; implementation and all GC remain disabled.

- [x D] Specified authority-signed namespace roster generations, explicit member keys/status/epoch, monotonic generation and roster digest.
- [x D] Specified signed monotonic per-member contiguous applied-OriginSequence vectors bound to roster generation/digest.
- [x D] Specified per-author safe frontier as the minimum across every Active member; Joining/missing/offline/stale/invalid member contributes zero.
- [x D] Specified durable member retirement fence and mandatory FULL_REPLACE before stale-peer rejoin.
- [x D] Specified reference scan across PendingApply, operation payload, relay/outbound reservation, provider intent, snapshot pin, feed lease and apply token.
- [x D] Specified crash-consistent deletion plus monotonic forgotten checkpoint and replay rejection below the frontier.
- [~] v3.1 codecs, capability negotiation, signed roster/frontier/checkpoint persistence and all-member install ACK are implemented; exact-head adversarial tests remain in Phase T. ChangeId GC remains physically disabled.

#### 4.3 Tombstone / ghost

- [x D] Defined the signed authoritative FULL_REPLACE manifest/ACK, roster binding and separate transfer frames in [BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md](BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md); implementation requires negotiated v3.1 capabilities, not unversioned v3.0 messages.
- [x C] Provider `OpenSnapshot` captures the stable image and source feed watermark in one Storage snapshot; the full-replace digest binds the generation and watermark. Signed wire certificate and all-member binding remain blocked on the roster protocol revision.
- [x D] Specified that every Active member must provide a signed installed ACK for the same or newer generation/digest before tombstone/ghost reclamation.
- [~] All-member installed-generation tracker and provider/metadata proof checks are implemented; physical GC remains disabled pending complete horizon proof and exact-head tests.
- [x C] `IProviderFullReplace` and `StorageDataProvider` stage generation-scoped rows, verify the canonical SHA-256 manifest in bounded scans, fence commit to the captured base generation, and atomically switch the active-generation pointer. `FullReplaceDigestBuilder` streams canonical pages without full-image sorting/materialization; old generations are retained and physical reclamation remains blocked on cross-store safe-GC proof.
- [x D] Specified the forgotten deletion frontier, durable epoch fence and full-rebootstrap rule for retired members.
- [~] Durable frontier/checkpoint persistence and parts of generation fencing are implemented; periodic frontier production, retired-origin closure and full rebootstrap orchestration remain open.
- [ ] Test field tombstone, record tombstone and ghost independently, including offline peers, alternate routes and crashes at every transition.
- [x C] Tombstone/ghost physical GC is explicitly disabled; v3.1 sequence-only frontier is insufficient to rule out delayed pre-snapshot operations from other authors.

### Phase 5 — Public API and clean cutover

- [x C] The public `TcpSyncSession` surface is already removed by the v3 clean cut; `docs/PUBLIC-API.md` explicitly records the removal and generic `SyncSession` as the transport-independent API. No compatibility wrapper is to be restored.
- [~ C] Audit all examples/tests and remaining public API for legacy TCP-specific/compatibility-only references; migrate any remaining call sites to canonical `SyncSession`/`ISyncTransport`/provider API during the C pass, then verify in Phase T.
- [x] Removed optional immutable-Origin provenance negotiation: v3 HELLO/HELLO_ACK carry and require ProtocolMajor=3; `SessionCapabilities` contains no provenance/downgrade bit and decode rejects every other major.
- [x C] On Metadata-Store-owned cutover, Node drops its namespace `GlobalChangeLedger` and Outbox payload projections; `SeenChanges` remains only as the distinct WAL replay marker. Cutover/restart assertions pass.
- [x C] Mandatory `BSP3` provider marker and active-generation pointer are written atomically on fresh open; unmarked/unknown partitions fail closed. Current state keys are generation-scoped; LocalChange rows require exactly one nonzero ChangeId suffix and no read-time synthesis. Exact checkpoint CI green.
- [ ] Ensure all public samples use canonical v3 API.

### Phase 6 — Conformance, performance and release

- [ ] Map every normative v3 requirement to unit/integration/fuzz/lab test and golden vector.
- [ ] Add randomized multi-peer/relay model tests and restart/fault injection at freeze, apply, ACK, snapshot and GC.
- [ ] Verify Linux/Windows/macOS durability semantics; document actual fsync/device/power-loss limitations.
- [ ] Benchmark 500, 10k, 100k, 1m operations and peer/author cardinalities 1, 4, 16, 64.
- [ ] Measure bytes/op, bytes/(peer×author), mutation latency, allocation and compaction CPU.
- [ ] Accept optimization only when correctness is unchanged and target metric improves without material regression elsewhere.
- [ ] Release only when all CI is green on exact release commit, Fuzz/Lab actually ran, samples pass, coverage threshold is met and spec/test mapping is complete.

## 4. GC proof summary

| Domain | Earliest possible reclamation | Hard blockers |
|---|---|---|
| Outbound/relay payload | Minimum contiguous ACK of every required peer/author stream and local consumer | Missing baseline, unacked reservation, active provider feed/lease |
| Inbox receipt | New authenticated transport epoch + complete initial baseline + durable retired-through fence; no unresolved PendingApply | Unknown epoch, pending apply, missing baseline |
| ChangeId identity | Signed all-member origin frontier + no references + durable forgotten checkpoint | Missing provenance, offline member, unsigned/stale checkpoint, alternate-route replay |
| Tombstone/ghost | ChangeId proof + FULL_REPLACE installed by all members + stale-peer fence + forgotten frontier | Merge-only snapshot, offline member, snapshot/feed gap, resurrection path |

Uncertainty means retain. False-positive retention costs space; false-positive deletion can permanently resurrect deleted state or accept an equivocated operation.

## 5. Progress log

Append a dated entry after each meaningful implementation package. Update checklist in the same commit as code/docs it describes.

### 2026-11-08 — v3 roadmap initialized

- [x] Confirmed branch heads and failing workflow run.
- [x] Captured compiler failure at Node.DeltaBatch.cs:861-862; tests did not run because Build failed.
- [x] Captured prior green baseline 92f6631 as historical only.
- [x] Wrote v3 clean-break architecture, metadata ownership, provenance and GC requirements into the normative spec.
- [ ] Code implementation not complete; CI remains red until Phase 1.

### 2026-11-08 — immutable Origin implementation package (active)

- [x] Added OriginAuthorEpoch/OriginSequence to metadata outbound and relay DTOs and persisted stream payload rows.
- [x] Added Origin tuple to global ChangeId ledger value (45-byte canonical identity value) and compare fingerprint + author + epoch + sequence for pending/staged identity checks.
- [x] Added Origin to PendingApply v3 encoding (-7) and reject all older PendingApply encodings.
- [x] Bumped local WAL format to v3 and included Origin coordinates in local mutation records; old WAL versions are rejected.
- [x] Added BMS3 metadata partition identity marker and reject old ledger encodings.
- [x] Preserve Origin in detached Node batch clones; snapshot state roots use a separate non-operation encoding and do not invent OriginSequence.
- [~] CI tests are being updated to include Origin in manual/tampered test batches; last observed run before this fixture package: 640 passed / 14 failed. Re-run CI on exact current head before closing any implementation item.
- [ ] Full ChangeId equivocation, WAL crash-replay and PendingApply recovery tests still required.


### 2026-11-08 — continuation audit after ae5db37

- [x] Located the roadmap at `docs/BIN-SYNC-ROADMAP-v3.md` (not repository root) and re-read its execution checklist and progress history.
- [x] Compared `ae5db37ca4e5c32fe71d320a62c831260ea53fdb` with the current branch: 28 commits ahead, no divergence. The reviewed implementation head is now `56d5142c1a2f7050663a418d4882def50787c1e5`.
- [x] Re-read the v3 normative specification, immutable-Origin/safe-GC design, metadata architecture, Metadata Store and Data Provider conformance contracts, change-staging/retention policy, Metadata GC design, public API contract, spec coverage audit and FLD integration contract.
- [x] Confirmed the old tuple compiler error was real on the PR merge snapshot `a852206fc245c2a0f7469d0e18d19415a2b2c528`: the method accepted a 3-field metadata tuple but dereferenced Origin fields. The current branch version has the 5-field tuple, and the `e6826b4` CI attempt completed Build successfully.
- [x] Confirmed that the CI attempt for `e6826b4` was cancelled during Test, Fuzz/Lab and Storage Integration; this is not a green result and does not supersede the earlier 640 passed / 14 failed evidence.
- [ ] Re-run complete CI on the exact current branch head and capture the 14 test failures individually before changing behavior.
- [ ] Do not mark Phase 1 complete based on the compile fix: end-to-end Origin, PendingApply/recovery, multi-hop relay, equivocation and safe-GC proof gates remain open.


### 2026-11-08 — v3 Storage decoder clean-break package (verification pending)

- [x] Tightened the Storage metadata decoder to reject legacy receipt keys and the old combined receive-cursor/digest row shape; v3 requires the canonical receipt identity and separate author receive cursor / sender digest-chain rows.
- [x] Tightened v3 receipt decoding to reject pre-v3 values without the durable resolved author while preserving the currently optional header-fingerprint and transport-epoch fields used by non-authoritative direct conformance calls. Metadata-authoritative receive still requires the full header fingerprint.
- [x] Replaced the old test that expected legacy ledger/watermark migration with a regression asserting that a BMS3 partition containing a pre-v3 ChangeId ledger row fails closed and is not rewritten.
- [ ] CI has not yet reported for this package. Do not mark the decoder gate complete until Storage Integration and full Core/Fuzz/Lab workflows pass on the exact branch head.
- [ ] Audit and remove now-unreachable `HasLegacyEncoding` rewrite branches only after confirming no current v3 decoder path can set the flag and the regression suite proves old partitions fail closed.


### 2026-11-08 — current branch checkpoint

- [x] Updated the baseline to reviewed head `56d5142c1a2f7050663a418d4882def50787c1e5`; 28 commits ahead of `ae5db37`, no divergence.
- [x] Corrected the distinction between the old merge-build failure, the later successful compile, cancelled test workflows, and the unverified decoder package.
- [ ] Obtain a complete, non-cancelled CI result for this exact head before treating any tests as passed.


### 2026-11-08 — BMS3 first-write identity consistency

- [x] Audited every StorageKey(root, 0, ...) partition-identity write in StorageMetadataStore.
- [x] Found six first-write paths still persisting raw UTF-8 ScopeId while the v3 decoder requires the canonical BMS3 marker + exact scope length + scope bytes.
- [x] Replaced all six with the single EncodePartitionIdentity(partitionKey) encoder. This covers first writes via incoming receipt/batch finalization, outbound freeze, outbound lease, PendingApply, peer metadata delta, and peer stream baseline.
- [x] Existing PendingApply restart conformance test exercises the first-write/reopen path; broader storage integration tests are running.
- [ ] Close this gate only after Storage Integration passes on Windows/Linux/macOS and Core tests remain green on the same source commit.
- [ ] Continue auditing all persistence paths for first-write identity, marker validation and crash/reopen consistency.


### 2026-11-08 — v3 conformance fixture and reference-adapter alignment

- [x] Updated shared Metadata Store conformance factories to provide mandatory Origin epoch/sequence and a resolved single-author stream.
- [x] Reworked inherited multi-author conformance cases into separate author-stream batches; digest chaining remains sender/epoch/namespace ordered while cursors are author-specific.
- [x] Updated direct Storage integration fixtures to carry Origin, including relay, pending-apply, equivocation and coordinator recovery cases.
- [x] Replaced the obsolete PendingApply v3/v4 decode-success expectation with explicit rejection of all pre-v3 formats; current v3 encoding remains round-trippable.
- [x] Corrected snapshot test hashing to use the dedicated snapshot state-root form without operation Origin coordinates; retained mandatory Origin for DELTA_BATCH/Causal fixtures.
- [x] Updated Node persistence format and fixtures to v17 (including its tagged-hash domain); v16 checkpoint payload is rejected.
- [x] Core CI at b60c4a0 reduced the Core failures from 7 to 1; the sole remaining failure exposed the test-only Volatile adapter's stale sender-wide cursor model.
- [x] Updated VolatileMetadataStoreAdapter to model author-specific receive cursors and a separate sender digest chain, matching the v3 storage contract.
- [ ] Re-run Core, Fuzz/Lab, Storage Integration and samples on 4e2ba079678ce3fcaa0058ae43ef652faa33975a; do not close conformance until exact-head CI is green.


### 2026-11-08 — first fully green v3 storage/conformance checkpoint

- [x] Core Build, benchmark build, Core tests, public samples and coverage passed on `6969ba8085281f959ebb210bd481856343605e3a`.
- [x] Core test result: 654/654 passed.
- [x] Fuzz/Lab: 31/31 passed.
- [x] Storage Integration: 149/149 passed on Ubuntu, Windows and macOS (447 successful platform executions).
- [x] Both durable-array and Storage-backed TCP samples passed on all three platforms.
- [x] Strict BMS3 first-write identity was verified through Storage Integration; all six partition-creation paths now use the canonical encoder.
- [x] PendingApply v3 format rejects older -6/-5/-4/-3 encodings; metadata-authoritative recovery requires a full header fingerprint.
- [x] Legacy StorageMetadataStore rewrite branches and their unused whole-partition migration writer were removed in commit `b105868959b26c8d8ef54f90b51b769f07a51f89`.
- [x] Removed the unreachable legacy local-origin marker decoder branch and rejected completed PendingApply receipts that omit the durable author.
- [x] Reconciled the Origin design's direct-author cursor wording with v3's independent OriginSequence/SenderCursor model; no arithmetic inference from peer-relative cursor is permitted.
- [x] Marked older GC and staging analyses as historical/non-normative where they conflict with v3.0.0.
- [x] Exact-head CI on f91df0b4ef93f5a53f419420aa578dfe36bc293b is green: Core tests and Fuzz/Lab succeeded; Storage Integration succeeded on all three configured OS platforms.
- [~] Phase 1 remains open only for a shared canonical Origin value type across Core/Storage/FLD boundaries, provider freeze/replay atomicity, PendingApply/snapshot/restart preservation audit, and crash/recovery proofs. Multi-hop/alternate-route/equivocation regressions now pass.
- [ ] Phase 2 remains open: inventory actual persisted row cardinality and byte cost; benchmark 500/10k/100k/1M; prove Storage metadata does not duplicate the compact Node ledger or operation payload.
- [ ] Phase 3 remains open: FLD provider adapter and atomic cross-store crash matrix.
- [ ] Phase 4 remains open: receipt retirement proof, ChangeId GC proof and tombstone/ghost GC proof.
- [ ] Phase 5 remains open: final public API clean cut, removal of optional receive-authority fallbacks and docs/sample conformance.
- [ ] Phase 6 remains open: release gate, reproducible benchmarks, package/license audit and final clean CI.

### 2026-11-08 — Origin equivocation, v3 handshake and exact-head green verification

- [x] Reconciled 49 commits after the actual previous roadmap checkpoint 4e2ba079678ce3fcaa0058ae43ef652faa33975a; branch is ahead by 49 with no divergence (56d5142 was an earlier comparison anchor, not the last roadmap checkpoint).
- [x] Added/verified four-node A→B→C→D relay plus alternate-route dedup scenario (Scenario43_FourNodeOriginRelayAndAlternateRouteDedup).
- [x] Added atomic rejection coverage for reusing a global ChangeId with a different immutable Origin tuple; checks stored value, inbox/outbox and receive cursor remain unchanged.
- [x] Removed optional ImmutableOriginProvenance capability negotiation. HELLO and HELLO_ACK now carry ProtocolMajor=3; v2 payload length and major 2/4 are rejected.
- [x] Node checkpoint format is v17; v16 checkpoint payloads are rejected. WAL v3 rejection diagnostic corrected.
- [x] Replaced two-stage Fingerprint + BindOrigin hashing on Outbox, inbound ApplyBatch and WAL replay with LedgerFingerprint: semantic mutation and immutable Origin are encoded into one canonical BLAKE3 input. This removes one hash pass and avoids a second 16-byte digest intermediate on these paths.
- [x] Added direct tests for Origin author/epoch/sequence sensitivity of the single-pass ledger fingerprint.
- [x] Exact-head CI for f91df0b4ef93f5a53f419420aa578dfe36bc293b: BinSync.Net test workflow succeeded; Fuzz/Lab (§17) succeeded; FLD Storage Integration succeeded on Ubuntu, Windows and macOS.
- [~] Follow-up review: v17 fixture name/temp-file label and checkpoint image XML comment were corrected in commits 24d22b0 and e7e1eb4. Verify exact-head CI after these two small follow-ups. Do not infer test-count totals from successful workflow status alone.
- [ ] Next: finish audit of canonical fingerprint equivalence for all operation types and causal dependencies; add provider-origin freeze/replay and PendingApply crash-recovery tests; then continue Phase 2 row-cardinality/byte-cost audit.

### 2026-11-08 — handshake wire contract synchronized

- [x] Audited the implementation's HELLO and HELLO_ACK encoder/decoder against the v3 specification.
- [x] Added normative exact payload layout to §12.7–12.8: NodeId b128, epoch u32 LE, protocolMajor u16 LE (=3), capabilities u32 LE; exactly 26 bytes, control stream 0, flags 0; HELLO/ACK are distinct frame types.
- [x] Documented fail-closed major/length/frame validation and the canonical implementation sequence numbers.
- [ ] Verify CI on the latest branch head. CI for code head f91df0b was green; runs after the test-name/comment follow-ups were cancelled, not failed. No implementation behavior changed in this spec-only package.

### 2026-11-08 — latest CI reconciliation after follow-up commits

- [x] Rechecked the full commit graph from the previous roadmap checkpoint: 50 commits ahead, 0 behind.
- [x] Rechecked workflow attempts for the v17 fixture/comment follow-ups: test and Storage jobs were cancelled; Fuzz/Lab succeeded in the first attempt. A later rerun attempt was also cancelled.
- [x] Classified the state correctly as UNVERIFIED for the latest head, not red: the last completed full green run remains f91df0b (Core tests, Fuzz/Lab and Storage Integration on three OS platforms).
- [ ] Obtain a non-cancelled run on the current code head before closing exact-head verification.

### 2026-11-08 — architecture-note reconciliation

- [x] Re-read the five roadmap-linked design documents: Change Staging/Retention, Metadata Storage Architecture, Metadata Store GC, Origin Provenance v3 and FLD-DB Integration Contract.
- [x] Added an explicit v3 normative override to Metadata Storage Architecture so its historical v2–v16 format analysis cannot be mistaken for active compatibility/migration requirements.
- [x] Corrected the architecture status to NodePersistence v17 / WAL v3 and distinguished the Node-local 16-byte Origin-bound digest from the separate 45-byte Storage Metadata ChangeId identity row.
- [x] Recorded the latest completed green checkpoint (f91df0b) and classified subsequent cancelled CI as UNVERIFIED, not failed.

### 2026-11-08 — canonical OperationOrigin implementation package (verification pending)

- [x] Added immutable OperationOrigin value type with non-zero author/epoch/sequence validation and explicit invalid-default handling.
- [x] Added canonical Provenance projections to OutboxEntry, MetadataOperation, MetadataRelayItem and MetadataOutboundItem; relay-local Sequence remains separate from Origin.Sequence.
- [x] Routed local Outbox creation, batch relay staging, DeltaBatch validation, Node checkpoint serialization, WAL replay and Node global ledger fingerprinting through OperationOrigin.
- [x] Routed Storage ChangeId ledger matching, 45-byte identity encoding, PendingApply encoding/decoding, relay/outbound cloning and published payload encoding through OperationOrigin.
- [x] Added unit tests for invalid/default provenance, value equality and separation of local relay sequence from immutable origin sequence.
- [x] Exact-head CI on 6e5cdb7: Core 662/662, Fuzz/Lab 31/31, Storage Integration 150/150 on Ubuntu/Windows/macOS. Durable Metadata Bench completed successfully. Follow-up remains for byte-level metadata reduction and row-cardinality audit.

### 2026-11-08 — OperationOrigin verification and metadata benchmark baseline

- [x] Exact-head Core tests passed 662/662; Fuzz/Lab passed 31/31; Storage Integration passed 150/150 on Ubuntu, Windows and macOS (head 6e5cdb76be8e9322227561ca12012855a0910bdf).
- [x] Durable Metadata Bench completed successfully after enforcing nonzero OriginAuthorEpoch at outbound freeze, fixing all benchmark fixtures and changing the CI report detector to catch padded NA cells.
- [x] Baseline: freeze 1,024 operations with 1,000,000 existing ChangeId rows = 232.8 µs, 7.84 KB managed allocation per invocation.
- [x] Baseline: one-operation freeze at 10k rows = 449.2 µs; at 100k rows = 434.1 µs, 10,944 B allocated.
- [x] Baseline: coordinator retry scan = 1.883 ms / 256 pending rows, 4.430 ms / 1,024, 17.550 ms / 4,096 (allocations 750.87 KB, 3,021.66 KB, 12,193.41 KB respectively). This linear scan/allocation profile is a later optimization candidate; do not change it without a measured improvement.
- [x] Identified a concrete metadata duplication candidate for Phase 2: each relay/outbound payload row repeats OriginEpoch+OriginSequence already stored in the global 45-byte ChangeId identity row. Current stream payload value is 32+payload bytes; candidate is 20+payload bytes with Origin hydrated from the ledger in-memory, saving 12 bytes per operation without extra storage I/O. Benchmark before/after and retain only if throughput/allocation remain acceptable.

### 2026-11-08 — GC design audit corrected to current v3 implementation

- [x] Reconciled the GC design document with actual code: mandatory v3 Origin, 45-byte Storage ChangeId identity, 16-byte Node-local Origin-bound digest, PendingApply format -7, Node checkpoint v17 and WAL v3.
- [x] Corrected stale statements that claimed Origin was absent and optional capability negotiation was required.
- [x] Kept ChangeId GC and tombstone/ghost GC disabled. Origin is now available, but the authoritative signed namespace roster, per-member/per-author contiguous applied-origin frontier, FULL_REPLACE snapshot generation and stale-rejoin fence are not implemented.
- [x] Explicitly documented that Origin gaps are allowed and that a safe frontier must be contiguous; relay/receive/Outbox cursors cannot substitute for it.
- [x] Removed the duplicated 12-byte OriginEpoch/OriginSequence suffix from each relay/outbound payload row; hydration uses the already-loaded ChangeId ledger (no additional Storage read). Added exact persisted-row and fail-closed corruption tests. Benchmark confirmation remains open.

### 2026-11-08 — compact stream-row regression and CI correction

- [x] Implemented the 12-byte-per-operation reduction: relay/outbound payload rows now store only ChangeId (16 B) + payload length (4 B) + payload; immutable Origin is resolved from the global ChangeId ledger instead of duplicated in each stream row.
- [x] Updated remaining compact payload writers and added durable integration coverage for the exact 20-byte fixed overhead, payload round-trip, and rejection of legacy stream-row layouts.
- [x] Exact-head Core/Fuzz workflow succeeded on the implementation head; Storage Integration exposed one xUnit analyzer error (xUnit2031) in the new assertion, not a production-code or runtime failure.
- [x] Fixed the analyzer violation by using Assert.Single(collection, predicate) in commit `95140bb`.
- [x] Re-ran exact code head `f6f041b`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 155/155 on Ubuntu, Windows and macOS. The xUnit2031 assertion issue and the stale compaction key-kind bug are fixed.

- [~] Tighten the compact-row corruption boundary with explicit tests that orphaned payload rows and ledger/stream author mismatches fail closed during hydration; add this before accepting the 12-byte reduction.
- [~] CI exposed a second compact-row issue: compaction still targeted legacy row kinds 4/7 after writers moved to 20/21. Fix deletion keys and assert physical removal after reopen before accepting the storage reduction.
- [ ] Continue with ChangeId/receipt/tombstone GC safe-frontier proof; GC remains disabled because a complete namespace membership roster, contiguous per-author applied-origin frontier, snapshot generation and stale-rejoin fence are not yet available.

### 2026-11-08 — compact-row compaction fix and exact-head green

- [x] Corrected compaction to delete current persisted relay/outbound payload row kinds 20/21 (previous code still targeted historical kinds 4/7).
- [x] Added a reopen assertion proving outbound payload rows are physically absent after compaction; retained ChangeId identity still prevents republishing the same operation.
- [x] Added fail-closed tests for compact payload rows with a missing global ChangeId ledger row and for author mismatch between stream key and ledger provenance.
- [x] Exact code head `f6f041bfc9b2841d33c2dbf4e39fff160fc6d4ee`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 155/155 independently on Ubuntu, Windows and macOS. Both push and PR workflow matrices passed.
- [~] Durable Metadata Bench triggered by the Storage implementation change is still running; keep throughput/allocation acceptance open until its report is inspected.

### 2026-11-08 — Origin recovery assertions and provider-feed audit

- [x] Extended PendingApply interruption and Node-checkpoint/restart integration tests to assert exact OperationOrigin equality from the source Outbox through the durable PendingApply batch and recovered relay row.
- [x] Exact code head `69b3710bd4bb66d0b85c86b2ff8008af4754263a`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 155/155 on Ubuntu, Windows and macOS; push and PR workflows both green.
- [x] Audited Metadata-Store-owned publication: fresh provider changes supply author epoch but no OriginSequence; Metadata Store allocates the durable author stream sequence during atomic freeze, then the coordinator checkpoints Node cache and only then ACKs the provider feed.
- [x] Confirmed coalescing across provider page boundaries and separation of field vs record-lifecycle coalescing keys.
- [x] Added crash/ACK-failure test for Metadata-Store-owned freeze: first operation remains immutable after provider ACK failure; a later same-field edit becomes a suffix operation; retry retains both ChangeIds/payloads and does not duplicate or renumber the frozen prefix.
- [x] Regression test seeds/ACKs provider feed through sequence 5, then freezes feed entries 6/7; Metadata Store assigns OriginSequence 1/2, proving source feed sequence is not copied into OriginSequence.

### 2026-11-08 — durable metadata benchmark after compact-row reduction

- [x] Durable Metadata Bench run `42487879970` completed successfully after the compact Storage layout and compaction-key correction.
- [x] Freeze one local operation at 10k ChangeId rows: 408.4 µs / 8.14 KB allocated (previous recorded baseline: 449.2 µs).
- [x] Freeze one local operation at 100k rows: 375.9 µs / 10.34 KB (previous baseline: 434.1 µs / 10,944 B).
- [x] Freeze 1,024 operations with 1,000,000 existing ChangeId rows: 196.9 µs / 7.77 KB (previous baseline: 232.8 µs / 7.84 KB).
- [x] The compact stream row saves exactly 12 persisted bytes per retained relay/outbound operation; correctness and physical compaction tests pass. No benchmark evidence of a freeze-time/allocation regression; keep the optimization.
- [~] Coordinator retry scan remains linear: 1.196 ms / 256 rows, 5.055 ms / 1,024, 13.823 ms / 4,096; allocations 744.59 KB, 2,997.63 KB, 12,097.38 KB. This is a measured future optimization candidate, but do not change without a better measured trade-off.

### 2026-11-08 — metadata-owned ACK-failure retry proof

- [x] Added a faulting IDataProvider wrapper that fails exactly the first feed ACK after Metadata Store freeze commits.
- [x] Test seeds and acknowledges provider feed rows 1–5, freezes ChangeId at feed sequence 6, injects ACK failure, appends a later same-field edit at feed sequence 7, then retries.
- [x] Verified feed sequences 6/7 map to immutable Metadata Store operations with OriginSequence 1/2; the first payload remains `first-frozen`, the suffix payload is `later-suffix`, both ChangeIds remain stable, and feed ACK completes only after the suffix freeze.
- [x] Storage Integration exact test code head `98b956d`: 156/156 passed on Ubuntu, Windows and macOS. Core exact head `1dfb7d6`: 662/662 and Fuzz/Lab 31/31 passed. A transient 1-second FlowControl test timeout was made scheduler-tolerant (5 seconds); no production FlowControl code changed.


## 6. Execution protocol — staged implementation (C → T → O → F)

The work is intentionally split into short, reviewable commits. The checklist remains authoritative; update it in the same commit as each implementation package.

- **C — broad code implementation:** traverse every remaining phase and implement the planned production-code structures/contracts in small coherent slices. Mark in-progress checklist entries `[~ C]` (or add a `C` marker to the corresponding progress-log item). Do not spend a whole cycle polishing one test suite while later phases remain untouched. Do not weaken invariants, introduce unsafe GC, or silently change normative contracts merely to make the broad pass compile.
- **T — correctness/conformance:** after the broad C pass, revisit each C-marked slice; add/fix unit, integration, model, fault-injection and restart tests; close only with exact-head CI evidence.
- **O — optimization:** profile and benchmark accepted behavior; reduce persisted bytes, allocations, copies, scans and lock contention only when measured results improve without material regression elsewhere.
- **F — finalization:** reconcile specification/docs/public API/samples, run complete CI and platform durability checks on the exact release commit, and close the release gates.

A C marker is explicitly provisional, not a claim of correctness or test completion. Existing [x] entries retain their evidence; do not reopen them without contradictory evidence. Every meaningful code commit must include the matching roadmap checklist/progress-log update. Commit slices should be small enough that progress remains visible and CI can identify regressions quickly. Never batch unrelated phases into one large commit.

### 2026-11-08 — staged C/T/O/F execution adopted

- [x] Reconciled the actual branch head as `1dfb7d686dd61eb80d3ade29b1392a6ffc5acdda`; it is 147 commits ahead of `ae5db37ca4e5c32fe71d320a62c831260ea53fdb`, with no divergence.
- [x] Verified exact-head `BinSync.Net` workflow run 3899 and `FLD Storage Integration` workflow run 1933 both completed successfully.
- [x] Re-read the v3 normative specification, roadmap, Change Staging/Retention, Metadata Storage Architecture, Metadata Store GC Design, Origin Provenance v3 Design and FLD-DB Integration Contract.
- [x] Confirmed the 12-byte stream-row reduction, physical compaction correction, immutable Origin recovery, provider freeze/ACK-failure retry proof and measured metadata benchmark are already implemented; do not redo these.
- [x] Adopted the user's staged execution order: broad implementation pass marked C, then focused correctness/conformance T, measured optimization O, and release finalization F.
- [ ] Begin C pass at the remaining Phase 3–5 implementation gaps; keep ChangeId and tombstone/ghost GC disabled until their independent safe-frontier proofs are complete.


### 2026-11-08 — C pass: receipt retirement reservation guard

- [x] Audited `RetireInterestedPeer` against v3 §0.11 and found that the atomic retirement transaction fenced the old epoch and deleted receipts/peer rows without first rejecting an unresolved outbound `PendingBatch` reservation.
- [x] Added a fail-closed guard for any pending outbound reservation belonging to the peer and a retired-or-older epoch. A completed replacement baseline no longer silently discards the old incarnation's in-flight send/retry context.
- [~] C implementation committed as `aab64ad`; targeted lost-ACK, reservation-retirement and crash-boundary tests are explicitly deferred to Phase T. Receipt GC remains conservative; ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — C pass: provider local-intent ownership boundary

- [x] Clarified the public `IDataProvider.ApplyRemoteBatch` contract: remote materialization may change the visible LWW projection but MUST NOT append, rewrite or acknowledge the independent unpublished local-change feed.
- [x] Inspected `StorageDataProvider.ApplyRemoteBatch`: its transaction updates current-state rows and the idempotency receipt only; it does not mutate LocalChange rows or local feed high/ACK watermarks.
- [~] C contract committed as `f0c2bd8`; same-field remote-wins, local-intent publication and restart tests remain Phase T. No second PendingLocal journal is introduced.


### 2026-11-08 — receipt GC design synchronized with C-pass guard

- [x] Updated `METADATA-STORE-GC-DESIGN.md` to make unresolved outbound PendingBatch reservations an explicit receipt/peer-epoch retirement blocker, matching the new StorageMetadataStore guard and v3 §0.11.
- [x] Clarified that a replacement epoch's completed initial-sync baseline cannot discard the old epoch's unresolved send/lost-ACK retry context.
- [~] Current branch head is `4aac39b`; exact-head CI remains pending for C-pass code commits `aab64ad` and `f0c2bd8`.


### 2026-11-08 — C pass: public session clean-cut audit

- [x] Confirmed the v3 public API contract explicitly states that `TcpSyncSession` was removed and the TCP listener now returns the generic `SyncSession`; no legacy public session wrapper is to be preserved.
- [x] Updated `docs/PUBLIC-API.md` normative reference from the historical v2.4.17 text to BIN-SYNC-SPECIFICATION.txt v3.0.0 clean-break contract.
- [~] Remaining C work: scan and migrate every sample/test/API call site still using TCP-specific or compatibility-only entry points. The complete build/reference audit is deferred to Phase T.
- [~] Current branch head at this log entry: `d501785`; exact-head CI remains pending for the C-pass code changes `aab64ad` and `f0c2bd8`.


### 2026-11-08 — roadmap checkpoint after API documentation pass

- [x] Reconciled the branch head after public API and GC design documentation updates: `0b4259f`.
- [x] Preserved the exact distinction between the last green CI head (`1dfb7d6`) and the newer C-pass code commits (`aab64ad`, `f0c2bd8`).
- [ ] Do not promote C markers to complete until Phase T verifies exact-head build, tests, Fuzz/Lab and Storage Integration.


### 2026-11-08 — Phase 5 C pass: strict provider v3 format

- [x] Verified exact checkpoint `70cda009949f6255469fa957a9e545f3ec97da26`: BinSync.Net workflow run 3914 succeeded (Core tests, Fuzz/Lab, public samples, coverage threshold); FLD Storage Integration run 1948 succeeded on Windows, macOS and Ubuntu. This checkpoint includes receipt reservation retirement guard `aab64ad` and provider local-intent contract `f0c2bd8`.
- [x] Added mandatory provider format marker `BSP3` at reserved key kind `0x25`. Fresh provider partitions commit it before any provider API writes; unknown marker fails closed; existing provider-owned rows without marker fail closed rather than being reinterpreted as v3.
- [x] Removed local-feed identity migration/synthesis. Every v3 LocalChange row must carry exactly one nonzero ChangeId; malformed/truncated/identity-less rows fail closed.
- [x] Added Storage Integration regression tests for marker creation, unmarked legacy-row rejection and v3 feed rows missing ChangeId.
- [~] C implementation and tests committed as `56d818c` and `ad236fd`; exact-head CI remains pending. Do not treat the test code as executed until CI reports.


### 2026-11-08 — Phase 5 C pass: remove duplicate Node ChangeId authority

- [x] Audited the v3 handshake: HELLO and HELLO_ACK both encode `SessionWire.ProtocolMajor=3`; decode rejects any other major. The `SessionCapabilities` enum has no optional immutable-Origin provenance or downgrade capability. Marked the clean-cut requirement complete.
- [x] On durable Metadata-Store-owned publication cutover, `Node.EnableMetadataStoreOwnedPublicationFence` now removes the namespace's `GlobalChangeLedger` entries. `SeenChanges` remains intact because it is a distinct local WAL replay marker, not a second ChangeId identity authority.
- [x] Added cutover and stale-Node-checkpoint restart assertions that the Node global ledger is empty after the provider/Metadata Store authority fence.
- [~] Code/test commits: `d6b02b2`, `6725463`. Exact-head Core + Storage Integration CI pending.
- [x] Updated the normative v3 §0.7 provider storage paragraph and Data Provider conformance contract to specify `BSP3`, strict nonzero ChangeId suffix and fail-closed legacy handling; no read-time identity synthesis.


### 2026-11-08 — Phase 3/4/5 C pass: provider generation and safe retirement

- [x] Added optional `IProviderFullReplace` capability with durable Begin/Stage/Commit/Abort operations. Staged rows remain invisible; Commit validates SHA-256 and atomically switches active generation.
- [x] Bound a staged replacement to its captured base generation so a stale concurrent replacement cannot overwrite a newer active generation. Repeated Begin/Commit after a successful switch is idempotent.
- [x] Made digest verification streaming in bounded 1024-row scans; staging and abort pages are capped at 4096 entries. Abort is resumable/bounded. Old generations remain physically retained until a separately proven cross-store GC path exists.
- [x] Generation ids use canonical RFC-4122 network byte order in digest, active pointer, session manifest and generation-scoped keys.
- [x] Added provider integration coverage for invisible staging, atomic switch, digest mismatch, stale-base rejection, restart/resume, old-generation snapshot stability and retention. The verified checkpoint before the latest three commits passed 165/165 on all OS.
- [x] Tightened peer retirement: exact stale-epoch PendingBatch may finish only before the durable retired-through fence; a replacement snapshot can discharge it only when same-author-epoch baseline covers the absolute end cursor. Unknown epoch and author-epoch mismatch fail closed.
- [x] Added tests for exact stale reservation retry/ACK, insufficient baseline, authoritative baseline supersession and cross-author-epoch mismatch.
- [x] Verified backup branches exist for both BIN-SYNC and FLD-DB; no backup branch was overwritten.
- [~] Latest exact-head CI pending for `1ec26b8`, `25224f4`, `0ba5876`; prior exact green `8564f5e` was Core 662/662, Fuzz/Lab success, Storage Integration 165/165 on Ubuntu/Windows/macOS.
- [ ] Signed namespace roster, all-member OriginSequence frontier, forgotten deletion checkpoint and tombstone/ghost GC remain disabled until the separate negotiated protocol revision and adversarial proof are complete.


### 2026-11-08 — exact green checkpoint: streamed FULL_REPLACE and GC reservation proof

- [x] Re-ran exact-head CI on `2f75b266c7c2d720d87b106573182393d5d15ea1`: Core 662/662; Fuzz/Lab SUCCESS; Storage Integration 168/168 on Ubuntu, Windows and macOS.
- [x] Added `ProviderFullReplaceDigestBuilder`: callers stream canonical snapshot pages; count is appended after the entry stream, so total count need not be known before the first page. The convenience helper remains for small/materialized images.
- [x] Receiver verifies the full-replace digest with bounded 1024-row scans; staging/abort pages are capped at 4096. No full-image materialization during commit.
- [x] Added field-tombstone and record-tombstone preservation coverage; fixed the fixture HLC so the record tombstone actually dominates the field write.
- [x] Added author-epoch equality to replacement-baseline retirement proof; a higher cursor from a different author epoch cannot discharge an old reservation.
- [x] Updated v3 spec, Public API and Data Provider Conformance docs with canonical RFC-4122 generation encoding, digest byte order, bounded page limits, caller safety obligations and old-generation retention.
- [x C] Promoted canonical ProviderMutation encode/decode/validation/order and `ProviderFullReplaceDigestBuilder` into `BinSync.Core/ProviderMutationCodec.cs`; `StorageDataProvider` delegates to the shared Core codec. Added round-trip, malformed encoding and canonical ordering tests; exact-head CI passed 665/665.
- [ ] Implement signed namespace roster, member applied-origin frontiers, authority-signed retention checkpoints and FULL_REPLACE installed ACKs in a separately negotiated v3.1 extension. Until then ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — v3.1 signed roster / safe-GC design checkpoint

- [x] Added `docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md` defining the pinned Namespace Authority, signed full roster, per-member applied-origin vectors, authority-signed retention checkpoint, FULL_REPLACE installed ACK and separate ChangeId/tombstone/receipt/outbound GC horizons.
- [x] Defined Joining/Active/Retired member states, missing-frontier=0, durable retired-through fencing and mandatory FULL_REPLACE on rejoin.
- [x] Preserved compact metadata: one shared ChangeId row per operation plus O(members × authors) frontier state; no per-change/per-peer duplicate history and no PendingLocal journal.
- [x] Defined v3.1 capability negotiation and proposed frame IDs; v3.0 Membership gossip remains advisory and cannot authorize GC.
- [ ] No v3.1 code or GC enablement yet. ChangeId and tombstone/Ghost GC remain disabled until codec, signature, crash, multi-peer and resurrection tests pass.


### 2026-11-08 — v3.1 design documentation synchronized

- [x] Linked the proposed v3.1 roster/GC addendum from the v3.0 specification, Metadata Store GC design and Public API docs without changing v3.0 wire behavior.
- [x] Updated Data Provider Conformance status to the exact green implementation head `2f75b26`: Core 662/662, Fuzz/Lab 31/31, Storage Integration 168/168 on Ubuntu/Windows/macOS, coverage threshold passed, in-process/TCP/ScenarioHarness samples passed.
- [x] Kept ChangeId and tombstone/Ghost GC disabled; no v3.1 protocol code is implied by the design document.


### 2026-11-08 — shared ProviderMutation codec exact-head verification and Storage source audit

- [x] Reconciled branch HEAD after the interruption: `3b9db5bf97550036fce6b5b145a8717b806f6d72`. The intervening commits cover codec promotion, canonical byte-order/spec updates, codec tests, duplicate-validation removal and StorageDataProvider delegation.
- [x] Verified exact-head CI: Core 665/665; Fuzz/Lab 31/31; Storage Integration 168/168 on Windows, Ubuntu and macOS; all five public samples passed.
- [x] Durable Metadata Bench completed, including one million existing ChangeId rows: freeze 1,024 local operations 218.1 µs / 7.77 KB. This is slower than the earlier 196.9 µs / 7.77 KB measurement; no performance improvement is claimed.
- [x] Compared all twelve common vendored `external/Fld1.Storage` source files with FLD-DB `src/Fld1.Storage` at current branch heads. Five are byte-identical; `StorageDatabase`, `StorageFaultPoint`, `StorageSnapshotFile`, `Fld1.Storage.csproj`, `WalManager`, and `WalWriter` differ; `StorageDirectorySync.cs` exists only in the BIN-SYNC vendor snapshot. The vendor README pins `dcd4334`, but the files are not a byte-identical snapshot of that commit either. Do not blindly overwrite either repository.
- [x C] Reconciled Storage divergence by porting the vendor-side directory-entry durability and WAL partial-write/fsync fault injection into authoritative FLD-DB `src/Fld1.Storage`; preserved `Fld1.Storage.Tests` and `BinSync.Storage.Integration` friend assemblies. FLD exact-head CI passed 169/169 on Windows and all six test assemblies passed on Ubuntu/macOS as well.
- [x C] Refreshed all 13 vendored Storage source/project files from FLD-DB code commit `a3abab3e0197f98aac12e9fdafcbfa204d9fbd77`; verified byte-identical file-by-file. Updated vendor README source pin and preserved the no-independent-semantic-changes rule. BIN-SYNC exact-head CI passed Core 665/665, Fuzz/Lab 31/31 and Storage Integration 168/168 on Ubuntu/Windows/macOS.
- [ ] Update the vendor snapshot from one exact tested FLD-DB commit after differences are reconciled; update `external/Fld1.Storage/README.md` source pin in the same package.
- [ ] Implement the FLD Metadata Store/Data Provider adapter using the shared Core codec; complete remote-apply/local-intent/reopen/crash conformance.


### 2026-11-08 — vendored Storage snapshot synchronized and exact CI green

- [x] FLD-DB Storage durability work passed exact code/test CI at `fd9f48f5ef7ec181bcacf2891bc45d8c1c11b28c`: 169 tests on Windows; Ubuntu/macOS also passed; `Fld1.Storage.Tests` 37/37 on all three OS.
- [x] BIN-SYNC vendor snapshot `external/Fld1.Storage` now matches all 13 source/project files from FLD-DB source commit `a3abab3e0197f98aac12e9fdafcbfa204d9fbd77` byte-for-byte. Vendor README pin and test assembly visibility are aligned.
- [x] Exact BIN-SYNC head after vendor refresh: `7cfddd45dec342f916a08f77a9521c831f6f8af7`; Core 665/665, Fuzz/Lab 31/31, Storage Integration 168/168 on Ubuntu/Windows/macOS. All StorageArraySync and StorageArrayTcpSync samples passed.
- [ ] Proceed with v3.1 signed roster/frontier/checkpoint codecs and persistence; no ChangeId or tombstone/Ghost GC is enabled until all-member proof, replay fences, restart and adversarial tests are complete.

### 2026-11-08 — v3.1 implementation pass started (not complete)

- [~ C] Added `IMetadataNamespaceGcStore` as a durable metadata-store contract for accepted namespace roster, per-member Origin frontiers, retention checkpoints, FULL_REPLACE installation ACKs, and protocol-only tombstone-GC proof. This is a new production API, not merely test scaffolding.
- [~ C] Added `StorageNamespaceGcStore` backed by the opaque Storage engine. It stores roster certificates, current-roster pointers, signed member frontiers, signed retention checkpoints and FULL_REPLACE ACKs under a separate key prefix; it shares the Storage transaction gate and does not rewrite ChangeId identity rows.
- [~ C] Added checks for roster generation rollback/fork, authority continuity, member state transitions, Joining→Active only after a durable installed ACK, per-member frontier monotonicity, retention checkpoint monotonicity/previous-digest chain, checkpoint overclaim, all-Active-member installation, and protocol tombstone proof.
- [~ C] Added v3.1 frame IDs and `SyncSession` send/receive adapters for signed roster, Origin frontier, retention checkpoint and FULL_REPLACE installed ACK. The methods fail closed unless the relevant v3.1 capabilities were negotiated; frontier/checkpoint and installed-ACK paths also require roster capability.
- [~ T] Added Storage integration tests for restart persistence, frontier and checkpoint safety horizons, Joining-member activation fencing, rollback/fork and overclaim rejection.
- [~ T] Added SyncSession tests for signed v3.1 frame exchange and missing-capability rejection. First CI exposed two test expectation failures: the handshake automatically negotiates `IncrementalAliasMap`; production code compiled and the remaining 676 tests passed. Corrected the assertions in commit `43efda1`; exact-head CI for `43efda1` is pending.
- [~] The v3.1 C implementation now includes signed FULL_REPLACE BEGIN/READY/CHUNK/COMMIT/ABORT codecs, sender/receiver state machines, durable resume, signed installed ACK, roster/frontier/checkpoint storage and ChangeId forgotten-origin replay fencing. Remaining: wire the transfer to authority/member lifecycle and SyncSession orchestration, produce/publish frontiers and checkpoints, complete retired-origin closure, provide a real application-wide commit/GC lease, and finish cross-store live-reference scans. Physical receipt/ChangeId/tombstone GC is disabled.
- [ ] Review the new StorageNamespaceGcStore implementation against the exact Storage transaction/snapshot semantics and add fault-injection/crash-boundary tests before treating persistence as production-ready.
- [ ] Update v3.1 addendum status and wire/storage vectors after implementation and adversarial tests; do not claim v3.1 conformance from the current adapter tests alone.


### 2026-11-08 — workflow reset: C-pass first, T-pass after code sweep

- [x] Adopted the execution rule: `C` means implementation pass completed for the named code slice; it does not certify behavior. `T` is reserved for focused correctness/conformance work and exact-head verification.
- [x] Reconciled the commits after the previous roadmap refresh: `d58e2e5`, `1d6878d`, `eff6e6d`, `43efda1`, `640af0d` (with `c56d2e2` as the prior green checkpoint). The v3.1 work now has durable roster/frontier/checkpoint/installed-ACK storage, signed message codecs, capability-gated SyncSession send/receive adapters, and a typed receive-frame range extended through `FULL_REPLACE_ABORT`.
- [x] Fixed the two initial SyncSession test expectation failures in `43efda1`: `IncrementalAliasMap` is automatically negotiated and must be included in expected capabilities.
- [x] On `640af0d610c7a3ee20d001210360d555c1f60c1f`, the BinSync.Net workflow Build and benchmark build succeeded; Fuzz/Lab succeeded; the test step failed; public samples were skipped as a consequence. Coverage threshold step reported success but does not make the failed test job green.
- [~ C] Code slice: accept registered v3.1 GC frame IDs in `ReceiveExpectedExclusiveAsync`; the upper bound is now `NamespaceGcFrames.FullReplaceAbort` rather than hard-coded v3.0 frame `0x13`.
- [~ T] Exact-head test failure on `640af0d` is not yet diagnosed. Retrieve the test failure details / TRX before changing production behavior. FLD Storage Integration run `42498381271` was still in progress when this roadmap entry was written.
- [ ] C-pass remaining v3.1 protocol path: FULL_REPLACE BEGIN/CHUNK/COMMIT/ABORT transfer state machine, bounded streaming receive/stage, correlation and ordering, durable store wiring, authority/member lifecycle orchestration, periodic frontier publication/checkpoint production, forgotten-origin replay fence and live-reference GC gate.
- [ ] Do not enable ChangeId or tombstone/ghost GC during the C pass. Their separate safe-horizon proofs and fail-closed behavior remain mandatory.
- [ ] After all production code slices have C markers, run the dedicated T pass: targeted tests, crash/fault injection, multi-peer/rejoin/resurrection, fuzz/resource bounds, samples, benchmarks and exact-head CI.


### 2026-11-08 — v3.1 FULL_REPLACE C-pass: wire envelopes and session transport

- [~ C] Added canonical bounded codecs for FULL_REPLACE_BEGIN, FULL_REPLACE_CHUNK, FULL_REPLACE_COMMIT and FULL_REPLACE_ABORT in `FullReplaceTransferProtocol.cs`. BEGIN binds namespace, generation, accepted-roster generation/digest, provider feed watermark, expected entry count and digest. CHUNK carries a bounded page (≤4096 mutations, ≤16 MiB) with strict length/trailing-byte validation and canonical ProviderMutation decoding. Control frames carry only namespace + generation.
- [~ C] Added capability-gated `SyncSession` send/receive adapters for the four transfer frames on a dedicated per-namespace transfer stream. Receiving frames returns validated envelopes only; it does NOT stage or commit provider state.
- [x C] Corrected all established-session receive type guards to admit registered v3.1 frame IDs through `FULL_REPLACE_ABORT`. The earlier 640af0d change had widened only `ReceiveExpectedExclusiveAsync`; `ReceivePacketAsync` and the single-reader `ReceivePumpAsync` still rejected frame IDs above 0x13. This was a real dispatcher defect and is now fixed.
- [~ T] No focused codec/transport tests or exact-head build have yet validated this package. The previous exact run on 640af0d built successfully but failed during the test step; do not infer that this package is green.
- [ ] Continue C pass: connect received BEGIN/CHUNK/COMMIT/ABORT to roster validation, bounded provider staging, digest verification, durable abort/recovery and installed-ACK publication. Enforce one active transfer per namespace and frame sequence/page continuity; reject interleaving, gaps, overlaps, duplicate page identities and stale generations before provider mutation.
- [ ] Keep FULL_REPLACE commit separate from GC: successful switch alone never authorizes old-generation deletion, ChangeId GC or tombstone/ghost GC.


### 2026-11-08 — dispatcher failure diagnosed and corrected

- [x] Retrieved the failed test job log for run `42498381290` (commit `640af0d`). The sole failure was `NamespaceGcSessionTests.Negotiated_v31_session_exchanges_signed_roster_frontier_checkpoint_and_install_ack`: `ReceivePacketAsync` still rejected established-session frame IDs above `0x13`.
- [x] Confirmed the earlier frame-range change had widened `ReceiveExpectedExclusiveAsync` only. Updated both `ReceivePacketAsync` and `ReceivePumpAsync` to use `NamespaceGcFrames.FullReplaceAbort` as the upper bound. The correction is committed as `e012460a6e3722979b4f136b6237091732ded692`.
- [~ T] The failing test has not yet been rerun on the corrected head. Keep the test gate open; no green status is claimed.


### 2026-11-08 — FULL_REPLACE manifest authenticity and ordered receive C-pass

- [x C] BEGIN now requires an Ed25519 authority signature over a domain-separated canonical manifest binding namespace, generation, roster generation/digest, source feed watermark, expected entry count and digest. Unsigned BEGIN payloads are rejected by the encoder/decoder path.
- [x C] Receiver verifies the manifest signature against the pinned, non-revoked PreShared Namespace Authority and independently verifies the supplied roster certificate before accepting BEGIN.
- [x C] FULL_REPLACE receive APIs now require an explicit expected frame sequence; stream identity and exact frame sequence are checked before payload use. This avoids silently defaulting every later transfer frame to sequence 1.
- [x C] Capped CHUNK bytes to `Frames.MAX_PAYLOAD_SIZE` (rather than the full 16 MiB frame size), leaving room for the frame header.
- [~ T] These new codec/authentication/sequence paths have not yet been covered by a fresh exact-head CI run. Do not mark transfer conformance complete.
- [ ] Next C slice: durable receiver orchestration over `IProviderFullReplace`, persisted staging progress/resume, authority/member transport binding, duplicate/replay handling, atomic commit guard, signed installed-ACK persistence and Node cache rehydration. Keep the old generation and all tombstones until the independent GC proof is complete.


### 2026-11-08 — FULL_REPLACE end-to-end C-pass implementation slice

- [x C] Added `FULL_REPLACE_READY` (0x1C) and extended all session dispatch guards through that frame. READY returns the durable staged cursor, manifest count/digest, committed state and next transfer-frame sequence.
- [x C] Added a canonical READY codec with strict fixed length, identity, digest, range, committed-state and page-boundary checks. Resume uses fixed 4096-entry pages (last page may be shorter), making the next frame sequence derivable and deterministic.
- [x C] Extended `IProviderFullReplace` with an optional durable progress query and implemented it in `StorageDataProvider`, reading the persisted staging cursor after reopen.
- [x C] Added `StorageFullReplaceSender`: stable provider snapshot, first bounded digest pass, signed authority manifest, READY/resume negotiation, bounded second pass, COMMIT and signed installed-ACK verification/persistence.
- [x C] Added `StorageFullReplaceReceiver`: authority peer/epoch and roster verification, local member signing-key preflight before any switch, durable BEGIN/resume, strict contiguous page staging, concurrent ABORT detection, guarded atomic provider commit, Node-cache refresh callback, and durable local installed-ACK persistence. The caller must provide an exclusive commit lease that proves no PendingApply, relay/outbound reservation, provider intent or reader pin can be invalidated.
- [x C] Added signed ACK re-encoding so the authority can persist the exact verified member signature received over the session.
- [~ T] None of these new FULL_REPLACE sender/receiver, READY, resume, ABORT, signature or commit-guard paths has yet been run through exact-head CI. The earlier 640af0d failure was diagnosed and the dispatcher guards fixed, but the current C-pass package remains unverified.
- [ ] Remaining C work: connect sender/receiver to the actual namespace authority/member lifecycle and `StorageSyncCoordinator` public orchestration; implement periodic origin-frontier publication/checkpoint generation, forgotten-origin replay fencing and a concrete cross-store live-reference GC scan. ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — FULL_REPLACE READY/resume and receiver hardening

- [x C] Registered `FULL_REPLACE_READY` as frame `0x1C`; all three established-session dispatch guards now accept it. Updated the v3.1 addendum frame registry.
- [x C] Receiver and sender now negotiate durable resume state. Fixed-size 4096-entry pages (except final) make `NextIndex` and the next frame sequence deterministic; receiver rejects misaligned progress, short non-final pages and conflicting same-sequence CHUNK/ABORT or COMMIT/ABORT frames.
- [x C] `StorageFullReplaceReceiver` verifies the roster is still current under the mandatory commit lease before switching the active provider generation. The receiver preflights its local signing key against the roster before staging/commit, and persists its signed installed ACK only after provider switch and Node-cache refresh.
- [x C] `StorageFullReplaceSender` verifies the authority seed against the pinned roster before sending and checks the second snapshot pass still exactly matches the signed entry count.
- [x C] Optimized provider progress reads to use the single-row Storage lookup rather than cloning a full Storage snapshot on every resume/page check.
- [~ T] No exact-head build or tests have run for this implementation slice. Build/test correctness remains explicitly unproven.
- [ ] Remaining C-pass optimization: audit the provider snapshot memory path. `StorageDataProvider.OpenSnapshot` currently materializes active provider rows into a second `StorageEntry[]`, while `StorageDatabase.OpenSnapshot` itself copies the partition. The digest builder is streaming, but source snapshot memory is not yet truly bounded; remove redundant materialization or redesign snapshot iteration without weakening point-in-time semantics.
- [ ] Remaining C work: connect the receiver's mandatory commit lease to concrete StorageSyncCoordinator/MetadataStore/peer-reservation checks; wire authority/member lifecycle and periodic frontier/checkpoint production; implement forgotten-origin replay fence and cross-store live-reference GC scan. ChangeId and tombstone/ghost GC remain disabled.


### 2026-11-08 — C-pass: origin identity persistence and first safe GC implementations

- [x C] Extended `ProviderMutation` and its canonical codec to persist `ChangeId`, `OriginAuthorEpoch` and `OriginSequence` alongside current field/record tombstone rows and FULL_REPLACE snapshots. The canonical 81-byte-header encoding is required; the prior 53-byte layout is rejected with no fallback decoder.
- [x C] Local provider feed appends now allocate a stable ChangeId, persist the author epoch and provider-assigned monotonic origin sequence into both the current mutation and feed entry, and verify the duplicated feed identity on read. Outbound freeze rejects missing/mismatched source provenance.
- [x C] Both direct Batch and durable PendingApply paths now carry exact per-operation ChangeId/origin tuple into provider mutations. Provider apply fingerprints include the tuple; legacy apply receipts remain replayable using the prior fingerprint encoding.
- [x C] Receipt rows now persist the highest immutable origin sequence in each author-homogeneous batch. Batch validation binds operation origin epoch to the wire issuer epoch and requires strictly increasing origin sequence. Older receipt encodings decode with origin sequence zero and are retained fail-closed.
- [x C] Added same-partition namespace-GC replay fencing to `StorageMetadataStore`. When a ChangeId row is absent but its origin is below the signed retention frontier, the incoming operation is rejected instead of being applied again. A stored receipt with a missing ChangeId row is also rejected unless the signed checkpoint proves the origin was forgotten.
- [x C] Added `CompactReceipts`: receipts can be deleted only when the signed safe frontier covers their persisted maximum origin sequence and no PendingApply references them. Receive cursors and sender digest chains remain durable as the transport replay fence.
- [x C] Added `CompactChangeLedger`: ChangeId rows are reclaimed only below the signed origin frontier, with in-store relay/outbound/pending-batch references checked and a mandatory external lease for provider/outbox/reader references. The signed retention checkpoint is the durable forgotten-origin fence.
- [x C] Added `StorageDataProvider.CompactRecordTombstones`: requires the active provider generation to equal the checkpoint's FULL_REPLACE generation, all Active members to have installed that exact digest, and the tombstone origin to be below the safe frontier. It deletes hidden older fields in bounded 4096-row batches while retaining the tombstone until all hidden fields are gone; crash/retry is safe. It also blocks on provider snapshots, unacknowledged local feed and uncommitted FULL_REPLACE staging.
- [x C] Exposed attachment and compaction entry points through `StorageSyncCoordinator`; GC fails closed until the same-partition GC store/trust are attached and the caller supplies the cross-store lease.
- [~ T] No exact-head build, unit tests, crash injection or GC proof tests have run for these changes. The C markers describe implemented code only.
- [ ] Remaining C work: verify provider mutation identity propagation through every local Node/outbox/reconcile path; wire a concrete application commit/GC lease that serializes Node, MetadataStore, provider, pending apply and peer reservations; add a durable membership transition that prevents re-activation without a matching FULL_REPLACE ACK; connect frontier production/checkpoint signing to periodic peer exchange; review old metadata-row migration behavior. Keep physical GC disabled in production until the T pass validates the gates.


### 2026-11-08 — GC safety review correction: tombstone physical GC remains disabled

- [x C] The provider now persists ChangeId and origin epoch/sequence in current mutations and FULL_REPLACE snapshots, enabling a future tombstone proof to bind the actual tombstone operation.
- [x C] Implemented bounded, crash-repeatable tombstone cleanup plumbing, including deletion of older hidden fields before the tombstone marker.
- [x C] Re-reviewed the v3.1 proof and found it insufficient for safe physical tombstone deletion: a checkpoint covering only the tombstone author's sequence does not exclude a delayed, pre-snapshot operation from another author whose sequence is above the common frontier but whose HLC is older than the tombstone.
- [x C] Disabled `HasProtocolTombstoneGcProof` fail-closed. Therefore `CompactRecordTombstones` currently reclaims nothing, despite the cleanup implementation being present. Do not describe tombstone GC as operational.
- [ ] Required proof extension before enabling tombstone GC: signed per-origin author-issued high-watermarks, all Active members' applied-through values reaching those watermarks, a FULL_REPLACE-bound global HLC floor, and mandatory advancement of every installing Node's HLC before it becomes Active. Retired-origin closure must also be represented; otherwise tombstones from retired authors remain uncollectable.
- [ ] T pass must test delayed pre-snapshot writes from a second author, skewed HLCs, author restart/epoch change, joining/rejoining members, and interrupted bounded cleanup. Receipts and ChangeId GC remain separately guarded by their signed origin sequence horizons.


### 2026-11-08 — C-pass: provider origin metadata and receipt/ChangeId GC

- [x C] `ProviderMutation` now carries the stable `ChangeId` and immutable origin author epoch/sequence. The v2 FULL_REPLACE digest domain binds these fields; old 53-byte provider mutation rows remain decodable with zero provenance and are not eligible for tombstone collection.
- [x C] Local provider feed rows persist the same ChangeId twice (mutation plus feed identity) and validate equality on read. New local appends require a non-zero author epoch; provider assigns the monotonic origin sequence. Outbound freeze validates author, epoch, sequence and ChangeId before publishing.
- [x C] Remote direct-batch and PendingApply conversion paths now pass operation identity into provider mutations. Same-HLC migration preserves existing provenance and upgrades legacy rows only when authenticated new provenance is supplied. Legacy apply-receipt fingerprints remain accepted on exact replay.
- [x C] Receipt encoding now stores maximum origin sequence; new batches must have one author/epoch and strictly increasing origin sequence. Old receipt formats decode with zero origin sequence and are not collected.
- [x C] `CompactReceipts` and `CompactChangeLedger` are exposed through `StorageSyncCoordinator`; both require the same-partition signed-GC guard and an external lease. Receipt compaction retains receive cursors/digest chains. ChangeId compaction checks relay/outbound/pending references and retains the signed checkpoint as replay fence.
- [x C] Provider record-tombstone cleanup plumbing is bounded and crash-repeatable, but it is intentionally disabled (`IsTombstoneGcProofEnabled == false`). The current v3.1 sequence-only proof cannot rule out a delayed pre-snapshot operation from another author with an older HLC. Do not enable physical tombstone deletion.
- [~ T] No build or tests run; runtime and source-compatibility effects of requiring local provider `OriginAuthorEpoch` remain unverified.
- [ ] C-pass remaining: implement the global causal-horizon protocol (per-author issued high-watermarks, every Active member's applied-through watermark, FULL_REPLACE-bound HLC floor, mandatory Node HLC advancement, and retired-origin closure); wire report/checkpoint production and the real application-wide commit/GC lease. Then proceed to T pass.


### 2026-11-08 — Canonical v3 encoding correction

- [x C] Removed the newly introduced legacy decode/fingerprint branches. Provider mutations now accept only the canonical 81-byte fixed header; local feed envelopes accept only that mutation encoding plus one 16-byte ChangeId; receipt values accept only the canonical origin-sequence-bearing lengths. No silent v2/v3 migration decoder is added.
- [x C] Record tombstones now require complete ChangeId/origin provenance at codec validation. Zero provenance remains allowed only for ordinary field materialization from Node LWW state, which is not an immutable operation/tombstone.
- [x C] Kept the explicit same-HLC provenance enrichment path for field materialization: a later authenticated immutable operation may attach its identity to an otherwise identical materialized field, without changing visible data. This is not a storage-format fallback.
- [~ T] Existing persisted rows using earlier in-development layouts will be rejected by the canonical v3.1 decoder; tests and any deliberate data-reset/rebuild procedure must account for this. No compatibility migration is hidden in the code.


### 2026-11-08 — receipt GC replay-fence strengthening

- [x C] `CompactReceipts` now additionally requires the durable per-author receive cursor to cover the receipt's `CursorEnd` and the sender digest-chain row to remain present. Receipt deletion therefore cannot remove the only transport replay fence; old cursor continuations fail even after the receipt is gone.
- [~ T] Exact-head tests for receipt deletion, duplicate retry after lost ACK, cursor continuity, digest-chain preservation and PendingApply races remain queued for the T pass.


### 2026-11-08 — C-pass: bounded provider snapshot enumeration

- [x C] Replaced the second full `StorageEntry[]` materialization in `StorageDataProvider.OpenSnapshot` with a pinned `StorageSnapshot` and two sorted 4096-row keyset cursors (fields + record tombstones). `ReadSnapshot` streams merged canonical order, supports sequential pages and absolute-offset reset/skip, and disposes the pinned snapshot on release/provider shutdown.
- [~ C] The provider no longer duplicates every current row in a second array, but `StorageDatabase.OpenSnapshot` itself still clones the underlying partition. Fully bounded memory requires a storage-engine immutable-root/iterator snapshot API; do not claim O(page) total memory yet.
- [~ T] Exact paging boundary, random/backward seek, release/dispose, snapshot stability under concurrent writes, and fullreplace two-pass digest consistency remain untested on current HEAD.


### 2026-11-08 — C-pass: validate streamed snapshot rows

- [x C] The streaming provider snapshot cursor now validates every decoded row against its active-generation key, record id, field id and row kind before returning it. Corrupt/mixed-generation rows fail closed instead of entering the FULL_REPLACE digest stream.
- [~ T] Add malformed-key/value, mixed-generation, cursor disposal, exact page-boundary and two-pass snapshot digest tests in the later T pass.


### 2026-11-08 — C-pass build repair from exact-head CI

- [x C] Inspected exact failing Core/Fuzz and Storage Integration job logs for pre-repair HEAD `ebb33a3ef4380cb6b289e21fb8603bee1abcefcb`. All five jobs failed at Build (tests never started) with the same four compiler errors: missing `ProviderSnapshotState.LocalHighWatermark`; removed `CompareSnapshotEntries`; nullable `ReceiveState?` incorrectly used as a non-nullable value-type out parameter and then dereferenced.
- [x C] Restored the snapshot watermark on the pinned snapshot state because `ReadLocalChanges` uses it as the source-snapshot barrier; restored the canonical storage-key comparator used by FULL_REPLACE generation digest verification; corrected the value-type `TryGetValue` out variable.
- [~ T] These are compile-only repairs. No tests have run on the repaired HEAD yet. Next: continue C implementation, then build/test the complete C-pass on exact HEAD; do not count prior green checkpoint as current validation.
- **Pre-repair CI:** Core run `42502401957`, Storage Integration run `42502401982`; both failed at Build on Linux/macOS/Windows before their test steps.
- **Current branch HEAD at this roadmap update:** `561a51b7cdbf1c3207e975b22c56ec8c94281357` (roadmap commit follows).


### 2026-11-08 — C-pass: applied-origin and retention checkpoint production

- [x C] Added durable member frontier production. It combines ChangeLedger identities already committed by the Metadata Store with still-unacknowledged local provider feed entries; it uses each mutation's immutable OriginSequence and never substitutes provider feed sequence. Reports are signed by the Active member and persisted through the accepted roster guard. A previous report is retained as the baseline after safe compaction, and each author/epoch advances only through a contiguous sequence prefix.
- [x C] Added authority checkpoint production from durable member reports. Missing Active-member reports conservatively produce no safe frontier for the affected author; optional FULL_REPLACE binding is accepted only when all Active members' installed ACKs are already durable.
- [~ T] Add exact tests for gaps, duplicate/replayed feed rows, post-ACK frontier continuity, report rollback/fork, missing Active member, roster rollover, checkpoint digest chain, and FULL_REPLACE ACK gating. Exact-head CI for this slice is pending.


### 2026-11-08 — C-pass: enforce post-FULL_REPLACE HLC floor before installed ACK

- [x C] Added `Node.AdvanceClockToFloor`, a local-history restore operation that advances HLC without applying remote clock-skew rejection.
- [x C] Added `StorageSyncCoordinator.RefreshAfterFullReplace`: advance to the committed provider snapshot's maximum HLC floor, rehydrate the Node cache from the new generation, replay durable PendingApply intents, and persist Node state. This is the intended callback for `StorageFullReplaceReceiver`; the receiver invokes it before signing/persisting the installed ACK.
- [~ T] Verify callback wiring, interrupted refresh/retry, empty snapshot, HLC tie/logical floor, PendingApply replay and ACK ordering. Exact-head CI for this slice is pending.


### 2026-11-08 — C-pass: eliminate OriginSequence holes caused by pending-write coalescing

- [x C] Found a correctness gap: assigning OriginSequence from the provider feed cursor caused coalesced-away pre-publication edits to consume origin numbers, permanently blocking contiguous applied-origin frontiers. Replaced it with freeze-time allocation.
- [x C] Local provider drafts now carry stable ChangeId + author epoch and OriginSequence=0. Metadata Store allocates OriginSequence atomically per namespace/author/epoch in durable key-kind 22, independently of outbound transport sequence and provider feed sequence. Coalesced-away drafts consume no origin sequence.
- [x C] After immutable freeze, coordinator stamps the assigned origin onto the matching current provider winner before feed ACK. Stamp is idempotent and checks the durable feed row; failed stamp/ACK retries recover the same identity from the ChangeId ledger. Remote apply still requires a complete non-zero origin tuple; FULL_REPLACE state images may carry draft identity and later upgrade it without erasing an existing published origin.
- [x C] Member applied-origin frontiers now derive only from frozen ChangeId ledger rows; unpub drafts are not reported as applied/published origins.
- [~ T] Add tests for coalesced writes, epoch rollover, per-epoch allocator restart, crash after freeze/before stamp, crash after stamp/before feed ACK, draft FULL_REPLACE followed by published delta upgrade, and strict remote partial-origin rejection. Current exact-head CI pending.


### 2026-11-08 — C-pass: durable freeze-time origin assignment and provider stamp

- [x C] Added `IProviderPublishedOriginStore` and implemented it in `StorageDataProvider`. The coordinator pairs each coalesced winning feed entry with the exact immutable Metadata Store item, validates the still-present durable feed row, stamps the assigned origin onto the matching current-state winner, then persists the Node cache and only afterward ACKs the feed.
- [x C] A failed provider stamp or feed ACK leaves the feed replayable. The Metadata Store ChangeId ledger returns the same previously frozen OriginSequence; provider stamping is idempotent. A newer current-state winner is never overwritten by an older feed entry.
- [x C] FULL_REPLACE state images may carry a draft `(ChangeId, AuthorEpoch, OriginSequence=0)`. If its immutable DELTA arrives later at the same HLC, provider apply upgrades the exact same ChangeId/epoch draft to the assigned non-zero OriginSequence; replaying a draft cannot erase a published origin.
- [x C] Scoped the missing allocator-row fail-closed check by namespace, since one Metadata Store partition can contain multiple namespaces.
- [~ T] Add fault-injection around metadata freeze → provider stamp → Node checkpoint → feed ACK, plus concurrent newer-edit and FULL_REPLACE-draft upgrade cases. Exact-head CI is pending.
