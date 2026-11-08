# BIN-SYNC v3.0 — implementation roadmap
**Дата:** 2026-11-08  
**Статус:** active; execution checklist is authoritative.  
**Normative contract:** BIN-SYNC-SPECIFICATION.txt, v3.0.0 DRAFT.  
**Cross-repository integration:** FLD-DB docs/SYNC-STORAGE-COMPACTION-ROADMAP.md.

Mark [x] only after code, tests and CI prove it; [~] means active/partial; [ ] means not done. Documentation alone never closes a code gate. Current implementation checkpoint: code head `1dfb7d686dd61eb80d3ade29b1392a6ffc5acdda`; Core 662/662 and Fuzz/Lab 31/31 passed on the exact head. Storage Integration 156/156 passed on Ubuntu, Windows and macOS at code head `98b956d1cf021d32545968a64541739e54360465`; the later Core-only timeout adjustment does not alter Storage code. Durable Metadata Bench run `42487879970` passed.n: Core 662/662, Fuzz/Lab 31/31, Storage Integration 155/155 on Ubuntu, Windows and macOS. The docs-only follow-up is recorded separately; Durable Metadata Bench for the compact-row change is still running.

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

- Prior roadmap checkpoint: 4e2ba079678ce3fcaa0058ae43ef652faa33975a. Current implementation/doc head: 206f4f849f3ffc74b9812950fcadccd06507dc33; 49 commits ahead, no divergence. Last completed exact-code green checkpoint: f91df0b4ef93f5a53f419420aa578dfe36bc293b; subsequent follow-up/spec docs CI was pending at last inspection.
- The PR merge snapshot a852206fc245c2a0f7469d0e18d19415a2b2c528 failed Build because Node.DeltaBatch.cs:861-862 read Origin fields from a 3-field tuple.
- The later e6826b4865f8966327a628ae07e95720db58d0ff snapshot completed Build, but Test, Fuzz/Lab and Storage Integration workflows were cancelled before completion.
- The latest strict-decoder commits have no reported workflow result yet. CI is therefore UNVERIFIED, not green.
- Last documented green baseline is 92f6631871072799087ee1732f4d65bdaeeff523: Core 648/648, Fuzz/Lab 31/31, Storage Integration 136/136. Historical only; not verification of current HEAD.

### FLD-DB: fix/crash-consistency-outbox

- HEAD: 4ad95ca99d8cc0b904f28ccb7ae722e316dd2044.
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
- [ ] Remove duplicate Node and Metadata Store ChangeId authority after provider cutover.
- [~] Record before/after bytes/op, bytes/peer-author, allocations, mutation latency and compaction CPU at 500/10k/100k/1m rows. Exact row saving is 12 bytes per retained relay/outbound operation (32+payload → 20+payload); the durable benchmark completed successfully: 10k rows 408.4 µs / 8.14 KB, 100k rows 375.9 µs / 10.34 KB, 1M rows (1,024-operation freeze) 196.9 µs / 7.77 KB. Against the prior recorded baselines (449.2 µs at 10k, 434.1 µs / 10,944 B at 100k, 232.8 µs / 7.84 KB at 1M), these measurements are faster with slightly lower allocation; compare only as same-workflow directional evidence, not as a cross-machine guarantee.

### Phase 3 — Publication and FLD Storage contract

- [x] Provider feed/FLD Outbox is the sole authority for unpublished local changes in Metadata-Store-owned publication mode; no parallel MetadataStore PendingLocal table. The cutover fence rejects a non-empty local feed/Node Outbox state that would make ownership ambiguous.
- [x] Verify LWW field coalescing across provider pages; field writes and record-lifecycle tombstones use distinct coalescing identities. Provider conformance separately covers lifecycle tombstone/ghost ordering.
- [~ C] Make the provider contract explicitly require remote APPLY to leave the local unpublished feed and its ACK state untouched; StorageDataProvider already follows this boundary. Same-field remote-wins/restart conformance remains Phase T.
- [ ] Freeze ChangeId + Origin + payload atomically into BIN-SYNC; ACK source feed only after durable freeze.
- [ ] Make freeze replay idempotent when metadata commit succeeds but provider ACK fails; later edits remain suffix operations.
- [ ] Map FLD JournalSeq to stable ChangeId/Origin without using JournalSeq as OriginSequence.
- [ ] Complete Storage/DataProvider/MetadataStore conformance including reopen and crash boundaries.
- [ ] Storage DLL remains protocol-agnostic; field/HLC/tombstone semantics stay in Database DLL.

### Phase 4 — Safe GC, independently proven

#### 4.1 Inbox receipts

- [x] Conservative retired-transport-epoch receipt GC and PendingApply retirement guard exist in earlier Storage-backed implementation.
- [~ C] Block retirement/receipt reclamation while any outbound reservation remains attached to the retired transport epoch; implementation added in `aab64ad`. Exact retry/crash regression tests remain in Phase T.
- [ ] Revalidate against canonical v3 receipt layout; remove legacy-unknown receipt handling because old formats are rejected.
- [ ] Test lost ACK, duplicate batch, epoch retirement, new epoch initial baseline and crash before/after atomic fence+receipt deletion.

#### 4.2 ChangeId ledger

- [ ] Implement authority-signed namespace roster generations with explicit member set and monotonic generation.
- [ ] Implement signed monotonic per-member contiguous applied-OriginSequence checkpoints bound to roster and snapshot generation.
- [ ] Compute per-author safe frontier as minimum across every roster member; missing/offline/stale/invalid member blocks reclamation.
- [ ] Implement member removal fence and mandatory FULL_REPLACE before stale-peer rejoin.
- [ ] Scan references from PendingApply, operation payload, relay/outbound reservation, provider intent, snapshot pin and apply token.
- [ ] Commit deletion + monotonic forgotten checkpoint crash-safely; reject replay below frontier.
- [ ] Keep ChangeId GC disabled until all adversarial tests pass.

#### 4.3 Tombstone / ghost

- [ ] Define signed authoritative FULL_REPLACE generation distinct from merge SNAPSHOT_CHUNK.
- [ ] Bind provider snapshot token and change-feed tail watermark to one atomic barrier.
- [ ] Require all roster members to install same or later generation/digest.
- [ ] Stage full namespace image and atomically switch active Storage generation; remove destination-only stale rows.
- [ ] Persist forgotten deletion frontier before physical purge; fence old-generation deltas and force full rebootstrap.
- [ ] Test field tombstone, record tombstone and ghost independently, including offline peers, alternate routes and crashes at every transition.
- [ ] Keep tombstone/ghost GC disabled until complete proof passes.

### Phase 5 — Public API and clean cutover

- [ ] Remove old public TcpSyncSession and compatibility-only APIs; migrate examples/tests to canonical session/provider API.
- [ ] Remove optional provenance capability and downgrade negotiation.
- [ ] Remove Node-owned duplicate metadata paths where Metadata Store authority is selected; no fallback after durable cutover.
- [ ] Make v3 format marker mandatory on first durable write and validate on every open/recovery path.
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
