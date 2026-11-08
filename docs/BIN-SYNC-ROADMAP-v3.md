# BIN-SYNC v3.0 — implementation roadmap
**Дата:** 2026-11-08  
**Статус:** active; execution checklist is authoritative.  
**Normative contract:** BIN-SYNC-SPECIFICATION.txt, v3.0.0 DRAFT.  
**Cross-repository integration:** FLD-DB docs/SYNC-STORAGE-COMPACTION-ROADMAP.md.

Mark [x] only after code, tests and CI prove it; [~] means active/partial; [ ] means not done. Documentation alone never closes a code gate. Current branch head at this progress update: e6826b4865f8966327a628ae07e95720db58d0ff. CI is still RED on the current implementation work; the last completed test evidence is 640 passed / 14 failed before the latest fixture corrections. Build and Fuzz/Lab did run on that preceding commit.

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

- HEAD: ae5db37ca4e5c32fe71d320a62c831260ea53fdb.
- CI run 42474680383 failed during Build.
- Compiler errors: Node.DeltaBatch.cs:861-862; tuple (Sequence, ChangeId, Payload) is accessed as if it also contained OriginAuthorEpoch and OriginSequence.
- Tests and Fuzz/Lab were skipped because Build failed.
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
- [ ] Define one canonical Origin value type for local writes, Node Outbox, MetadataOperation, decoded inbound operation, PendingApply, relay, checkpoint and snapshot.
- [~] Local outbound freeze assigns OriginSequence from the durable author stream sequence and preserves supplied Origin on replay; audit atomic allocator/retry behavior and provider freeze before closing.
- [x] Removed the invalid equality between OriginSequence and peer-relative sender cursor; direct-author validation only rejects an Origin epoch from the future.
- [ ] Make provenance mandatory in every v3 DELTA_BATCH; remove optional capability/flag and reject protocol major mismatch.
- [ ] Preserve Origin through preflight, PendingApply crash recovery, provider apply, relay, snapshot and restart.
- [ ] Cover direct author, A→B→C→D relay, alternate routes, duplicates and equivocation.
- [ ] Same ChangeId with changed fingerprint or Origin fails before mutation; same operation via another route creates no second relay payload.
- [ ] Build, unit/integration tests, samples, Fuzz/Lab and coverage green on all configured platforms.

### Phase 2 — Canonical metadata layout and redundant-fact removal

- [ ] Inventory every persisted row/key and byte cardinality in StorageMetadataStore, Node checkpoint, FLD Outbox, ApplyJournal and WAL.
- [ ] Establish one canonical v3 format marker and row codec; reject absent/old/unknown markers.
- [~] Stream payload rows now persist Origin epoch/sequence; global ChangeId identity rows now persist fingerprint + author NodeId + Origin epoch/sequence. One-row/no-payload-duplication audit remains.
- [~] Global ChangeId ledger stores fingerprint + Origin tuple in a fixed 45-byte value; verify every dedup/equivocation path and benchmark whether the ledger can be merged with the operation identity row.
- [ ] Collapse repeated author IDs in ACK/pushed/offset maps into one sparse progress row only if benchmarks prove a win.
- [ ] Remove acknowledged replay reservations atomically with contiguous ACK, preserving digest-chain anchor.
- [ ] Consolidate Inbox/InboxAuthors/ReceivedStreams only when one receipt preserves all retry/ACK/cursor/digest semantics.
- [ ] Remove duplicate relay payload copies and unnecessary WAL/ApplyJournal duplication only after recovery proof.
- [ ] Remove duplicate Node and Metadata Store ChangeId authority after provider cutover.
- [ ] Record before/after bytes/op, bytes/peer-author, allocations, mutation latency and compaction CPU at 500/10k/100k/1m rows.

### Phase 3 — Publication and FLD Storage contract

- [ ] Provider feed/FLD Outbox is the sole authority for unpublished local changes; no parallel MetadataStore PendingLocal table.
- [ ] Verify coalescing across provider pages and separate commits for LWW field writes and record lifecycle changes.
- [ ] Preserve pending local intent independently of visible LWW state when remote APPLY wins before publication.
- [ ] Freeze ChangeId + Origin + payload atomically into BIN-SYNC; ACK source feed only after durable freeze.
- [ ] Make freeze replay idempotent when metadata commit succeeds but provider ACK fails; later edits remain suffix operations.
- [ ] Map FLD JournalSeq to stable ChangeId/Origin without using JournalSeq as OriginSequence.
- [ ] Complete Storage/DataProvider/MetadataStore conformance including reopen and crash boundaries.
- [ ] Storage DLL remains protocol-agnostic; field/HLC/tombstone semantics stay in Database DLL.

### Phase 4 — Safe GC, independently proven

#### 4.1 Inbox receipts

- [x] Conservative retired-transport-epoch receipt GC and PendingApply retirement guard exist in earlier Storage-backed implementation.
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
