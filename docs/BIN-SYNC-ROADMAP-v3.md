## 1.15. C-pass: enforce contiguous signed namespace roster history — 2026-11-10

- [x C] Hardened `StorageNamespaceGcStore.AcceptNamespaceRoster`: the first locally accepted NRS2 roster must be generation 1; each later roster must be exactly the next generation and bind the digest of the currently accepted roster. A signed roster can no longer skip an unobserved membership transition or bypass the accepted hash-chain predecessor.
- [x C] Existing same-generation exact replay/fork checks, authority identity pinning and direct membership-transition validation remain in force. Generation overflow fails closed.
- [x C] Production change committed as `ee15822c` (`fix(gc): require contiguous namespace roster chain`) and pushed to `feature/fld-external-storage`.
- [~ T] Add first-generation, exact-next-generation, skipped-generation, wrong-previous-digest, replay/fork, authority-change and `UInt64.MaxValue` adversarial/reopen cases. No tests/build were run in this C slice; physical GC remains disabled.
- [~ C] Continue auditing v3.1 roster/frontier/checkpoint persistence, rollback behavior and cross-store reference proofs.

## 1.14. C-pass: enforce contiguous signed retention checkpoint history — 2026-11-10

- [x C] Hardened `StorageNamespaceGcStore.AcceptRetentionCheckpoint`: after the first accepted checkpoint, every new checkpoint must be exactly the next sequence and its previous digest must match the currently accepted checkpoint. A correctly signed checkpoint that skips one or more sequence numbers is rejected instead of silently truncating an authority decision from the locally observed hash chain.
- [x C] The first locally accepted checkpoint must be sequence 1; same-sequence exact replay remains idempotent, while same-sequence different-digest forks and lower sequences remain rejected. Sequence overflow is explicitly fail-closed.
- [x C] Production change committed as `8de0cb2c` (`fix(gc): require contiguous retention checkpoint chain`) and pushed to `feature/fld-external-storage`.
- [x C] Created paired recovery branches `backup/cpass-2026-11-10-retention-checkpoint-chain` in BIN-SYNC and FLD-DB before this C-pass slice.
- [~ T] Add restart/reopen tests for first checkpoint sequence, exact next sequence, skipped sequence, wrong previous digest, exact replay, same-sequence fork and `UInt64.MaxValue` boundary. No tests/build were run in this C slice; physical GC remains disabled.
- [~ C] Continue the v3/v3.1 source/API/byte audit, especially cross-store reference completeness and the final physical-retention proof boundary.

## 1.13. C-pass: generic Storage blob publication/cleanup source audit — 2026-11-10

- [x C] Created recovery branch `backup/cpass-2026-11-10-issued-origin-monotonicity` from the current feature branch before continuing the remaining C-pass audit.
- [x C] Re-read FLD-DB `StorageBlobStore` end-to-end. Immutable blob publication is ordered as pending catalog row → atomic page-file writes → page-directory fsync → complete catalog marker; readers pin the immutable generation and validate page geometry plus whole-blob SHA-256 when streaming.
- [x C] Confirmed deletion removes the catalog row before page-directory cleanup; interrupted cleanup is recoverable through orphan pruning. Pending catalog generations remain protected by catalog presence, and reader pins block deletion.
- [x C] Confirmed persisted page geometry is authoritative for existing generations; changing the configured default page size affects only new blobs. Metadata decode validates page count without overflow-prone round-up arithmetic.
- [x C] Created recovery branch `backup/cpass-2026-11-10-provider-feed-gc-fix` at the current implementation/documentation head before continuing the C-pass.
- [x C] Fixed a ChangeId-GC provider-feed scan hole: the previous loop stopped on an empty/short `Changes` collection, although FLD can advance `ScannedThroughSequence` over local-only lifecycle rows and later pages can still contain unpublished ChangeIds. GC now captures the provider high-watermark under the composed lease and scans until that cursor is reached, validating forward progress, the captured bound and page capacity. Commit: `5963d352fbae286db37d53b7f5097b58f72ddade`.
- [~ T] Add a regression case with one or more empty/short provider pages followed by a live ChangeId page, plus a concurrent-writer exclusion case. Verify no ledger row is reclaimed while the later feed identity remains pending.
- [~ T] Crash/fault proof remains open for every pending-row/page/temp-rename/directory-fsync/completion-marker boundary, catalog-delete/page-delete interruption, orphan cleanup and concurrent reader pins. Source review is not a durability test.
- [~ C] Continue the remaining cross-store reference/liveness ordering and final public API/source/byte audit. Do not enable physical receipt, ChangeId or tombstone/Ghost GC.

## 1.12. C-pass: preserve issued-origin maxima across signed frontier reports — 2026-11-10

- [x C] Hardened `StorageNamespaceGcStore.AcceptOriginFrontier`: every previously reported author epoch must remain present, and its issued sequence plus maximum HLC (wall/logical tuple) may not regress in a later signed report. The applied-origin vector was already monotonic; the issued-origin vector is independently consumed by tombstone/GC proof construction and must be monotonic as well.
- [x C] Production fix committed as `47be8895046662a06481ac097229c59e2cb2ed47`.
- [~ T] Add adversarial Storage restart/conformance cases for decreasing issued sequence, decreasing HLC wall, decreasing HLC logical at equal wall, dropped epoch, and unchanged/increased values. No tests were run in this C slice; physical GC remains disabled.
- [~ C] Continue the final v3/v3.1 production-source/API/byte audit and close remaining implementation-only gaps before the T-pass.

## 1.11. Recovery point and C-pass continuation reset — 2026-11-09

- [x C] Created `backup/pre-v3-code-sweep-2026-11-09` from `feature/fld-external-storage` before the next broad implementation pass.
- [x C] Reconciled the live branch status against `docs/PRODUCTION-STATUS.md`: latest inspected production code head is `73571b8210bf67ad7e53ba6f29bf6aa4c1056043`; durable sender recovery is in §§1.25 and 1.27, and FLD receiver staging/switch is in §1.26.
- [x C] Base FLD `IDataProvider` bridge, provider author-epoch closure, exact host composition and multi-session send/receive scheduler are now implemented in the optional host sample.
- [x C] Physical-format decision Option B is accepted: generic Storage owns opaque immutable blob pages plus its WAL-backed catalog; Database retains FLD1 Run v1/v2, manifest, field semantics and FLD WAL. The generic blob/page backend and FLD checkpoint/compaction/FULL_REPLACE paths are implemented on the paired branches.
- [~ C] Remaining code gates: finish blob catalog/page failure-ordering and restart-cleanup source audit, then complete the public API/source/byte audit. Durable sender recovery is implemented in §1.25 and FLD receiver staging/atomic switch in §1.26; both await T-pass proof. Transport creation/reconnect/disposal remain application-owned.
- [~ T] Current validation remains red/unverified: latest completed evidence is older than the current source head; stale Core and Storage Integration fixtures/adapters remain a separate T-pass. Historical [~ C] entries below are chronological progress records; the latest status is §§1.17–1.27 and supersedes older open-item summaries. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 0.74. Recovery branches created before the next integration slice — 2026-11-09

- [x C] Created `backup/cpass-2026-11-09-relay-gate-metadata-mode` at `1c7fcd82edbf2656f1e6af1ab0d4ed7eab94517c` before continuing with the cross-store host integration and metadata-backend audit.
- [x C] Retained earlier checkpoints `backup/pre-rpm3-compact-provenance-2026-11-09` and `backup/cpass-2026-11-09-after-relay-guards`.
- [~ T] Backup point contains unvalidated C-pass changes; it is a recovery branch, not a passing release candidate.

# BIN-SYNC v3.0 — implementation roadmap
**Дата:** 2026-11-09  
**Статус:** active; execution checklist is authoritative.  
**Base normative contract:** `BIN-SYNC-SPECIFICATION.txt`, v3.0.0 DRAFT.  
**Mandatory v3.1 extension:** `docs/BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md` (implementation and conformance are required for project completion; wire activation is capability-negotiated and does not rewrite v3.0 frames).  
**Supporting host pattern:** `docs/BIN-SYNC-V3.1-HOST-ORCHESTRATION.md` (non-normative).  
**Required exact payload fixtures:** `docs/BIN-SYNC-v3.1-GOLDEN-VECTORS.md` and executable `samples/V31ControlPlaneVectors` (T-pass/CI gate).  
**Cross-repository integration:** FLD-DB docs/SYNC-STORAGE-COMPACTION-ROADMAP.md.

Mark [x] only after code, tests and exact-head CI prove it; [~] means active/partial; [ ] means not done. Documentation alone never closes a code gate. v3.1 is a mandatory implementation track, not an optional feature: its separate negotiation is a wire-compatibility boundary, not a project-scope choice. Latest exact-head green checkpoint before the v3.1 implementation pass: `c56d2e206d0c8578cbfe66158d6e6f1e0824750c` — BinSync.Net Core 676/676, Fuzz/Lab passed, Storage Integration passed on Ubuntu/Windows/macOS; the in-memory SyncSession, TCP and ScenarioHarness public samples passed. Subsequent v3.1 commits are in progress and are NOT green yet. Durable Metadata Bench completed, including one-million-row freeze: 218.1 µs / 7.77 KB for 1,024 local operations. This verifies strict BSP3 provider marker/feed identity, Node ChangeId-ledger cutover, generation-scoped provider rows, atomic FULL_REPLACE staging/switch, canonical RFC-4122 generation bytes, same-author-epoch reservation baseline proof, bounded streaming digest builder, field/record tombstone preservation, and the shared Core ProviderMutation codec. The 218.1 µs result is a measurement, not a proven improvement over the earlier 196.9 µs run. Current branch HEAD after the latest capability-matrix production change: `7e763f0f5939ce2b134e0d5b7a55e92361e51b3b`; latest production-code commits: `187e56bfb6291305101284aadb5c8367b48b553d` (serialized relay publication gate), `6f0da098c9ecf612dd6eaa918935235766d9fe1d` + `d2f263f41a75db4a63a0d23c544e31a46b5c00ce` (delegate-based host reference scan adapter and constructor ownership fix). Latest CI-tested code baseline: `4382c49bffe1608ebabc4a483771b83242cbc81d`; subsequent production code is not yet CI-validated. latest production-code commit: `963328d9635b641ad832a76878f48418bcecb577` (zero-Outbox/positive-FLD cursor rejection); latest sample commit: `97c9ea4c07f4ef867dc8deae74ee07b46fa4f793`; latest control-plane API commit: `16933bc7ffc33d6a7213d782c376581a6acc57bc9`; RPM3 compact sequence-pair implementation: `0831076f29624b521e2b3a5cd599c4fd72b1d3c0`; FLD Outbox byte audit: `b45bbc1a3462e9b11142dd6656a2eff55200b1c9`; FLD roadmap §7.45 update: `a962371ec293708f61f02453f9bc7c441eafa015`. FLD engine support is at `FLD-DB/fix/crash-consistency-outbox` @ `e6bec094807f660e07b70874254a5925bc8b6deb`. Cross-repository FLD host primitives are on `FLD-DB/fix/crash-consistency-outbox` @ `a52b1c2206e077cf57b04a36c676106dc6d24c93`. This remains a C-pass head, not an exact-head green checkpoint. The last exact-head runs on `1acdfa65979a` were red: Core build failed because `VolatileMetadataStoreAdapter` lacked `TryGetOutboundChangeIdentity(...)`; that test-only adapter compile blocker is now fixed in `bae10f4c2d32` but has not yet been validated by exact-head CI. Storage Integration had 80 failures, predominantly old fixtures assigning OriginSequence to provider drafts (v3.1 requires deferred `OriginSequence=0`), plus old test providers without `IProviderCommitGuard`; these remain T-pass migrations. The Durable Metadata Bench succeeded; Metadata Retry Comparison failed. Do not weaken production invariants. No new exact-head test result is claimed.

## 0.33. FLD recovery checkpoint branch created — 2026-11-09

- [x C] Created `smersh1307n2/FLD-DB:backup/pre-binsync-provenance-map-2026-11-09` at `a962371ec293708f61f02453f9bc7c441eafa015` before further cross-repository work. FLD roadmap §7.46 records the same checkpoint.

## 0.47. C-pass: current production-status pointer refreshed for RPM3 — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` to identify the compact RPM3 implementation/sample commits and the current unvalidated branch head.
- [x C] The historical green v3.0 baseline remains explicitly separate from all current C-pass work.

## 0.53. Recovery checkpoint branch created before further RPM3 work — 2026-11-09

- [x C] Created `backup/pre-rpm3-compact-provenance-2026-11-09` at `93a1cbd82c076ea5002d0e0a7eecccde5f856eed`, preserving the compact RPM3 map, sequence-only rows, empty-tail ACK behavior and current documentation state.

## 0.52. C-pass: production-status pointer refreshed for latest RPM3 behavior — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` with the empty-tail ACK code commit and the expanded RPM3 sample commit; current-head test status remains explicitly unvalidated.

## 0.51. C-pass: empty-tail ACK semantics documented at API surface — 2026-11-09

- [x C] Added XML documentation to `TranslateSafeOutboxSequence` for positive prior ACK + empty scanned tail progression and the zero-watermark prohibition.

## 0.50. C-pass: empty-page progress sample includes full-prune reopen — 2026-11-09

- [x C] Extended the public sample to close/reopen after all mapped rows are pruned, then advance a later empty author page using the already-positive Outbox ACK. It also verifies a fresh author with OutboxSeq=0 cannot advance a positive FLD ACK.
- [~ T] Execute this sample during T to validate the cached and reconstructed paths against real Storage persistence.

## 0.49. C-pass: empty-author scan cursor can progress after positive ACK — 2026-11-09

- [x C] `TranslateSafeOutboxSequence` now advances through newly scanned empty author pages when the supplied safe OutboxSeq equals an already-positive durable ACK and no unacknowledged mappings remain.
- [x C] A zero OutboxSeq still returns the prior FLD ACK only; it can never authorize a positive JournalSeq ACK. If mappings remain, the equal-watermark path cannot skip them.
- [x C] Extended `RelayProvenanceMap` sample for empty-page progression and the zero-watermark invariant; updated the FLD integration contract.
- [~ T] Verify after reopen, with outstanding mappings, with a positive prior ACK, and with a zero Outbox watermark.

## 0.48. C-pass: FLD adapter contract uses sequence-only entries — 2026-11-09

- [x C] Clarified §9.5 that the host constructs `StorageRelayProvenanceEntry(JournalSeq, resolved OutboxSeq)` after calling the global Node ChangeId resolver. ChangeId is not copied into the provenance map.
- [x C] This makes the no-duplicate-metadata rule explicit at the adapter call site, not only in the RPM3 row codec.

## 0.46. C-pass: public sample catalog aligned with compact map — 2026-11-09

- [x C] Updated `samples/README.md` to describe the JournalSeq↔OutboxSeq sequence map and explicitly state that ChangeId remains in the global ledger, not in the provenance rows.
- [~ T] Run the sample and verify its RPM3 row bytes against the v3 golden vector.

## 0.45. C-pass: eliminate duplicated ChangeId from provenance rows (RPM3) — 2026-11-09

- [x C] Reworked `StorageRelayProvenanceMap` to persist only the sequence pair: OutboxSeq in the ordered key and JournalSeq in the value. ChangeId remains solely in the durable global BIN-SYNC ChangeId ledger; the host must resolve every FLD ChangeId through `Node.TryResolveOutboxSequences` before appending the page.
- [x C] Removed the per-mapping ChangeId bytes and the in-memory `HashSet<BatchId>`. This removes 16 persistent bytes per unacknowledged mapping plus the corresponding per-entry managed hash-set overhead; uniqueness remains enforced by the global ledger and strictly increasing resolved OutboxSeq.
- [x C] Bumped cursor marker to RPM3. The 60-byte cursor checksum now covers the compact 76-byte key + 8-byte JournalSeq value rows. Append WAL cost falls from 109 to 93 bytes per mapping Put; compacted snapshot row falls from 108 to 92 bytes.
- [x C] Updated the sample to verify actual RPM3 rows and the checksum, added `RELAY-PROVENANCE-MAP-v3.txt`, and updated byte audit, public API, changelog, production status and FLD integration contract.
- [~ T] Verify resolver-before-append, duplicate ChangeId returning the original OutboxSeq, monotonic rejection after ACK pruning, RPM3 byte vector, and checksum recovery/corruption. Historical RPM2/HashSet entries above are implementation history only and are superseded by this section.

## 0.44. O-pass candidate recorded: Storage batch sort — 2026-11-09

- [x C] The byte audit now records that `StorageDatabase.EncodeBatch` sorts all mutations by StorageKey before WAL append using LINQ `OrderBy`, adding O(N log N) CPU/allocation overhead per write batch.
- [~ O] Benchmark realistic relay-page sizes and mixed metadata batches before changing the FLD Storage serializer; compare LINQ ordering with array sort and pre-sorted mutation collection, preserving deterministic byte order and atomic commit semantics.

## 0.62. C-pass: complete frame registry audited — 2026-11-09

- [x C] Audited `FrameTypeMap`, `NamespaceGcFrames`, `SessionWire` and `SyncSession` parser bounds. IDs 0x01–0x1C are contiguous and unique; the v3.1 extension occupies 0x14–0x1C exactly.
- [x C] Confirmed alias-map 0x03 uses its dedicated path and non-alias session frame parser bounds include 0x04–0x1C.
- [x C] Added the full frame inventory and 25-byte envelope facts to `BIN-SYNC-METADATA-BYTE-AUDIT.md`.
- [~ T] Signed roster/frontier/checkpoint/FULL_REPLACE payload vectors and per-message encode/decode tests remain open.

## 0.61. C-pass: production status refreshed for coordinator map helper — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` with the coordinator resolver/append helper, direct map page bound, current RPM3 sample and unvalidated current head.
- [x C] Historical green v3.0 baseline remains separate from current C-pass validation.

## 0.60. C-pass: direct provenance-map page bound aligned — 2026-11-09

- [x C] `StorageRelayProvenanceMap.AppendPage` independently enforces the same 4,096-row maximum as the coordinator resolver, so callers cannot bypass the allocation bound through the lower-level API.

## 0.83. C-pass: FLD adapter recipe preserves whole JournalSeq groups within RPM3 bound — 2026-11-09

- [x C] Changed the integration recipe to read one `RelayJournalBatch` at a time, so the source map page ends on a complete JournalSeq boundary.
- [x C] Added an explicit 4,096-operation guard before allocating the transient ChangeId array. An oversized single FLD JournalSeq group fails closed; it is not split across two durable map/cursor commits.
- [x C] The 4,096-operation bound is enforced at FLD remote-apply ingress and aggregate relay page reads; whole JournalSeq groups are never split. Boundary/resume tests remain T work.
- [~ T] Test empty batch, one group at 4,096, one group at 4,097 and multi-operation same-JournalSeq ordering.
## 0.82. C-pass: document exact Outbox projection dependency of RPM3 resolver — 2026-11-09

- [x C] Corrected the coordinator XML contract: `AppendRelayProvenancePage` resolves transient ChangeIds against **retained Node Outbox entries** to obtain OutboxSeq; the global ChangeId ledger remains the identity authority but is not itself a sequence index.
- [x C] The helper deliberately fails closed when an Outbox entry was already compacted; it writes no map row or source cursor. The host must acquire the composed application guard before reading the FLD page and must not send until the RPM3 map commit succeeds.
- [~ T] Prove resolver behavior before/after Outbox compaction, including that a missing retained projection never guesses a sequence from current field winners or ledger identity alone.
## 0.81. C-pass: legacy TCP session API search closed — 2026-11-09

- [x C] Searched the current BIN-SYNC repository for `TcpSyncSession`, `TcpSyncClient` and `LegacyTcp`; no remaining source references were returned.
- [x C] Confirmed the current TCP examples are transport adapters around the generic `SyncSession`/`ISyncTransport` surface, not a parallel TCP-specific session API.
- [~ T] Run full solution plus all sample builds and inspect the final public API diff; a search result alone is not a build/conformance proof.
## 0.80. Recovery checkpoints refreshed after persistence and sample C slices — 2026-11-09

- [x C] Created BIN-SYNC `backup/cpass-2026-11-09-0915` at `bf277d14aea91977feb5ea4ba57ce3234a681baa`.
- [x C] Created FLD-DB `backup/cpass-2026-11-09-0915` at `94fd1ebdfa15b95b55184a8eee92a5ebea0a4d5e`.
- [x C] Exact-head BIN-SYNC CI remains unverified: workflows triggered by the earlier code commit were cancelled by subsequent rapid documentation pushes. FLD-DB `.NET CI` is green at code-equivalent HEAD `d49f710c1e2992f753278836d521157813d8b503`; latest FLD changes since then are documentation-only.
## 0.79. C/T/O boundary refreshed after orchestration, vector and byte-audit completion — 2026-11-09

- [x C] Reclassified authority/member orchestration as complete at the protocol/host-recipe boundary; recurring scheduling is intentionally application-owned and must not become a protocol-core timer.
- [x C] Reclassified signed v3.1 payload vectors and executable sample as authored; exact C# execution, decode/trust-store checks and negative cases remain T.
- [x C] Reclassified fixed source-derived byte-layout inventory as complete across NodePersistence, Metadata Store, FLD Outbox/ApplyJournal/WAL, Storage, Manifest and Run; workload measurement and runtime amplification remain O/T.
- [~ C] Generic Storage backend/WAL compatibility and physical-format strategy remain open. The synchronized-FLD publication/ACK adapter and concrete FLD reference-snapshot wrapper were subsequently implemented in the optional host sample (§0.93–0.94); cross-repository sample build and T-pass proof remain.
## 0.78. Recovery checkpoints refreshed before further extraction work — 2026-11-09

- [x C] Created BIN-SYNC backup branch `backup/cpass-2026-11-09-0855` at `e0eb9f17ad40abe92da5155aaa86282404acd16e`.
- [x C] Created FLD-DB backup branch `backup/cpass-2026-11-09-0855` at `d49f710c1e2992f753278836d521157813d8b503`.
- [x C] Earlier pre-RPM3 and pre-integration backup branches remain preserved; these new checkpoints include the current C-pass documentation and signed-vector sample state.
## 0.77. C-pass: executable v3.1 signed-vector sample added — 2026-11-09

- [x C] Added `samples/V31ControlPlaneVectors` to construct the deterministic NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN objects, compare payload and signed bytes against the checked-in vectors, and verify Ed25519 signatures.
- [x C] Added the sample command and description to `samples/README.md`; it uses the public `BinSync.Core` API and the documented test-only seed.
- [~ T] Execute the sample on the exact branch head, add trust-store `DecodeAndVerify` assertions and negative mutation/truncation cases. The sample is authored but not yet run.
## 0.76. C-pass: NodePersistence v17 checkpoint byte layout audited — 2026-11-09

- [x C] Audited the 46-byte Node state envelope (magic/version/body length/tagged BLAKE3) and documented deterministic body section formulas for records/field states, Outbox, grouped authors, adaptive receipts, stream digests, peer progress/reservations, SeenChanges and compact Node ChangeId fingerprints.
- [x C] Confirmed the reader rejects every state version except 17 before digest/body parsing; removed unreachable legacy state-digest tag alternatives without changing accepted input behavior.
- [x C] Clarified that NodeToAlias is derived and not persisted, Outbox author IDs are stored once per namespace, and SeenChanges is distinct from the global ChangeId ledger.
- [~ O/T] Runtime dictionary/object overhead, varint/payload distributions, checkpoint size under realistic cardinalities and save/load allocation/latency remain measurement/conformance work.
## 0.75. C-pass: correct receipt row byte variants and GC interpretation — 2026-11-09

- [x C] Re-read `EncodeReceiptValue` / `DecodePartition`: the 76/80/92/96-byte receipt lengths are combinations of always-present OriginSequence plus optional 16-byte HeaderFingerprint and optional 4-byte TransportPeerEpoch; they are not alternate v2 migration encodings.
- [x C] Re-read `EncodeCompleted` / `DecodeCompleted`: 69/85 bytes are the base completed-apply receipt plus optional 16-byte HeaderFingerprint.
- [x C] Corrected the byte-audit table and documented the GC gate: missing/zero TransportPeerEpoch or OriginSequence prevents receipt reclamation; existing replay cursor/digest state remains the durable fence.
- [~ T] Add round-trip fixtures for all supported field-presence combinations and explicit no-GC cases when TransportPeerEpoch is absent.
## 0.74. C-pass: reject unsafe Inbox/receive-state row consolidation — 2026-11-09

- [x C] Documented why the Inbox receipt (sender/transport epoch/namespace/BatchId), receive cursor (same sender context plus author stream) and digest-chain tail (sender/transport epoch/namespace) remain three separate row kinds.
- [x C] A single per-batch receipt would duplicate shared cursor/digest state; a single stream row would lose per-BatchId duplicate-replay identity. The current layout keeps each fact once at its natural cardinality.
- [~ O] Only revisit a merged encoding if measured real workloads show a net byte/index win and a proof preserves independent retry, contiguous cursor and digest-chain semantics.
## 0.73. C-pass: verify outbound reservation retirement is one atomic peer-stream write — 2026-11-09

- [x C] Source audit confirmed `AcknowledgeOutboundBatch` clears `PendingBatch` in the same `PersistPeerStreamState` commit that advances `AckedCursor` and `LastBatchDigest`, after validating exact pushed reservation identity.
- [x C] The pending immutable reservation is embedded in the single kind-15 peer/author row; it is not a separate durable payload table or per-operation receipt copy.
- [~ T] Add crash injection immediately before/after the Storage commit, reopen/retry, duplicate ACK, wrong digest and wrong peer-epoch cases.
## 0.72. C-pass: v3.1 addendum links signed payload vectors and preserves GC gate — 2026-11-09

- [x C] Updated the v3.1 addendum status to point to the deterministic NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN vectors and distinguish documented vectors from C#-verified vectors.
- [x C] Explicitly retained the rule that golden vectors do not authorize receipt, ChangeId or tombstone/ghost physical GC.
## 0.71. C-pass: remove split ACK/high-watermark persistence fallback rows — 2026-11-09

- [x C] Audited `StorageMetadataStore` row writers and confirmed fresh v3 writes persist relay ACK+high-watermark together as kind 5 / 16-byte value and outbound ACK+high-watermark together as kind 8 / 16-byte value.
- [x C] Removed unreachable switch branches for split-row kinds 6/9 and 8-byte values from kinds 5/8. The earlier `DecodePartition` preflight already rejected these legacy encodings before the switch, so this is dead-code cleanup, not a behavior change; no current writer emitted kinds 6/9.
- [x C] This aligns the parser with the audited fixed row inventory and the clean-break rule. It does not change receipt semantics: the 76/80/92/96-byte receipt values are same-v3 field-presence variants (OriginSequence is always encoded; header fingerprint and transport epoch are optional). Receipt GC still requires non-zero transport epoch and OriginSequence plus all signed retirement/frontier/reference checks.
- [~ T] Add exact fixtures proving canonical kind 5/8 reopen and preflight rejection of kind 6/9 or 8-byte split values; run full Storage Integration only in the T pass.
## 0.70. C-pass: correct stale provenance byte-audit row after RPM3 cutover — 2026-11-09

- [x C] Found and corrected a stale RPM2-era row in `BIN-SYNC-METADATA-BYTE-AUDIT.md`: the table still showed a 24-byte mapping value although the RPM3 implementation stores only the 8-byte JournalSeq value.
- [x C] The audit now consistently reports 76-byte key + 8-byte value = 84-byte retained mapping row; the 60-byte cursor and XOR/SHA-256 mapping-set digest are unchanged. RPM2 is explicitly historical and not a fallback decoder.
- [~ T] Verify the documented 84-byte row against the actual persisted RPM3 sample after the C-pass; no test result is inferred from the documentation correction.
## 0.69. C-pass: deterministic signed v3.1 payload vectors authored — 2026-11-09

- [x C] Added `docs/BIN-SYNC-v3.1-GOLDEN-VECTORS.md` with deterministic NRS2, OFR2, RCP2, FRA2 and FULL_REPLACE_BEGIN payload/signature vectors, one shared roster digest, fixed test-only Ed25519 key and canonical field descriptions.
- [x C] Vectors cover the new signed authority/member control-plane messages and FULL_REPLACE manifest domain; the test seed is explicitly marked public-only and must never be used in production.
- [~ T] Cross-check every byte/signature against the actual C# encoders, `DecodeAndVerify`/`VerifyBegin`, trust-store binding and negative tamper/truncation cases. These are signed payload vectors, not full frame-envelope vectors.
## 0.68. C-pass: two-DLL FLD extraction dependency audit synchronized — 2026-11-09

- [x C] Cross-checked FLD source graph against `FLD-STORAGE-CORE-ARCHITECTURE.md`: `Fld1.Storage` is already a separate opaque byte-key/value engine, but `FldDatabase` still uses `Fld1.Engine` + FLD-specific `Fld1.Run`, `Fld1.Manifest`, `Fld1.Wal` and mixed `Fld1.Core` assemblies.
- [x C] Recorded that there is no `Fld1.Database.csproj` and the current `FldDatabase` does not use `StorageDatabase` as its backing store. The two-DLL extraction therefore needs a physical-format/dependency seam decision, not a cosmetic AssemblyName change.
- [~ C] FLD roadmap §7.50/§7.57 now records the extraction order. Physical `Fld1.Database` consolidation is complete; generic WAL/file-generation reuse, Core primitive classification, public API audit, Storage-only/Database test re-homing and package proof remain. No third Common DLL or linked-source shortcut.
- [x C] The physical `Fld1.Database` boundary now exists. Implement the adapter in a separate host/integration layer; do not introduce direct Engine/Database→BIN-SYNC dependencies.
## 0.67. C-pass: reject stale/duplicate relay scan pages before ChangeId lookup — 2026-11-09

- [x C] `AppendRelayProvenancePage` now reads the RPM3 cursor under the already-held application guard and rejects a regressing scan cursor or non-empty page reusing the committed cursor before allocating resolver arrays or consulting Node.
- [x C] JournalSeq validation now starts from the durable prior scan cursor, so the first source batch must be strictly after the last committed page while multiple operations in the same JournalSeq remain valid.
- [x C] Empty pages retain the explicit no-allocation cursor-advance path, including idempotent same-cursor no-op behavior.
- [~ T] Verify stale page, exact replay, empty same-cursor and empty advancing-cursor behavior under reopen; current code remains unvalidated until the broad T pass.
## 0.66. C-pass: current integration boundary and CI state refreshed — 2026-11-09

- [x C] Updated the active snapshot to distinguish the new coordinator ChangeId resolver from the still-unimplemented FLD Database adapter. The helper is a BIN-SYNC-side primitive; it does not add FLD semantic types or a third runtime DLL.
- [x C] The approved FLD two-DLL source consolidation is now complete: `Fld1.Storage.dll` + `Fld1.Database.dll`, with a one-way Database→Storage reference. The old Core/Engine/Manifest/Run/Wal projects are physically consolidated into `Fld1.Database`; exact-head build/test proof remains in T.
- [x C] FLD's guarded immutable reference-snapshot code and two-DLL source consolidation are present on the current `fix/crash-consistency-outbox` branch. The latest FLD CI attempt failed before runner allocation (`runner_id=0`, no steps), so no current-head build proof exists; this also does not validate the cross-repository sample.
- [~ T] BIN-SYNC prior head `7036e1544d0e8aa3f8b4a15b2c90d0f51fb13f35` failed both Core and Fuzz/Lab at build due the bad map binding check. Fixed at `cf08a22158fc1784fe225ef38efd0e2a165d7eb1`; subsequent workflow attempts were cancelled by later pushes, so the fix remains unverified.
- [x C] The relay/ACK/GC host adapter source now implements FLD page read→resolver→RPM3 append→ceiling-limited send→safe ACK→FLD ACK→RPM3 prune and the reference-snapshot scanner in `samples/FldBinSyncHost` (§§0.93–0.98). [~ C] The `FldDatabase`→`IDataProvider` snapshot/local-feed/full remote-apply bridge is still missing; generic Storage backend/WAL compatibility, final public API/byte audit and transport scheduling remain.
## 0.77. C-pass: bounded author discovery and all-author ACK orchestration — 2026-11-09

- [x C] FLD author discovery now has a process-local cache: first use rebuilds from ApplyJournal, each durable remote APPLY incrementally adds its relay authors, and a durable per-author ACK removes the author only when it covers that author's maximum unacknowledged JournalSeq. No persistent author index was added.
- [x C] BIN-SYNC `StorageRelayProvenanceMap.ReadAuthors(namespaceId)` enumerates persisted RPM3 cursor authors. `StorageRelayPublicationGate` now unions these internally with FLD's `ReadUnacknowledgedRelayAuthors()` callback under the composed host guard; the host cannot accidentally omit a map-only author in the FLD-ACK-before-prune crash window.
- [x C] Added `AcknowledgeAllAsync`: it discovers the same author union and performs per-author safe-sequence translation → durable FLD ACK → RPM3 prune. Partial completion is retryable because successfully pruned authors disappear and the remaining cursors are rediscovered.
- [~ T] Test author-cache rebuild/update/partial-ACK/removal/re-add, automatic FLD+RPM3 union, map-only crash recovery, partial multi-author ACK failure/retry, and zero duplicate author index bytes.
- [~ C] Remaining integration: implement the concrete `FldDatabase`→`IDataProvider` bridge (stable paged snapshots, local ChangeId feed and full remote-apply context) and bind the actual remote APPLY callback. The current sample assumes a preconstructed coordinator/provider and supplies the relay/ACK/GC side only; keep one coordinator/gate per namespace and no direct session sender bypass.
## 0.76. C-pass: make cutover-aware ChangeId resolution page-scalable — 2026-11-09

- [x C] Replaced the Metadata Store resolver's per-page full relay-list scan with a rebuildable in-memory index scoped to one `(namespace, originalAuthor)` stream. The first lookup validates retained sequence ordering/high-watermark and builds the index; later 4,096-operation source pages resolve in O(page size).
- [x C] The index is not persisted and adds zero metadata bytes. It is shared across copy-on-write partition clones, extended only after durable relay append, and invalidated for that author only after durable relay payload compaction.
- [x C] Added collision/conflicting-sequence checks when the in-memory index is extended; persistent ChangeId ledger remains the sole durable identity store.
- [~ T] Add large-cardinality multi-page resolver benchmark (10k/100k/1M relay rows), reopen rebuild equivalence, incremental append, compaction invalidation, and ChangeId duplicate/corruption tests.
- [~ O] Performance evidence remains open; the index is a structural complexity fix, not a measured throughput claim.
## 0.75. C-pass: namespace-wide all-author relay recovery and send barrier — 2026-11-09

- [x C] Added `FldDatabase.ReadUnacknowledgedRelayAuthors()` in FLD-DB. It scans ApplyJournal under the FLD commit lock and returns distinct remote authors with unacknowledged RelayFields; it persists no extra author index.
- [x C] Added `StorageRelayProvenanceMap.ReadAuthors(namespaceId)` to enumerate existing RPM3 cursor authors. Startup must union this with FLD's unacknowledged author set so a crash after FLD ACK but before RPM3 prune is not missed.
- [x C] Replaced the single-author publication API with `ApplyAndPublishAllAsync` / `RecoverAndPublishAllAsync`. Author discovery occurs after APPLY under the composed host guard; the gate drains every author/page to that author's observed source high watermark before running the namespace-wide sender callback.
- [x C] The gate rejects zero/local/duplicate discovered authors, starts closed, remains closed on any author resolver failure, and only opens after all discovered authors reconcile. This closes the multi-author bypass where a sender selecting another author stream could previously observe unmapped relay payload.
- [~ T] Test author discovery after reopen, map-only author recovery after FLD ACK crash, multi-author pages above 4,096 operations, empty pages, resolver failure on the second author, and sender callback suppression until all authors complete.
- [x C] `StorageRelayPublicationGate.DiscoverAuthors` unions `fld.ReadUnacknowledgedRelayAuthors()` with `map.ReadAuthors(namespaceId)` after APPLY; the sample supplies the FLD half and does not cache authors before APPLY.
## 0.73. C-pass: drain every source page before exposing relay Outbox — 2026-11-09

- [x C] Replaced the unsafe one-page send orchestration with `ApplyAndPublishAsync` and `RecoverAndPublishAsync`. Both repeatedly read whole JournalSeq-bounded pages under the composed FLD/Node/Storage guard, resolve all ChangeIds and commit RPM3 mappings until the observed source JournalSeq high watermark is reached; only then can the sender callback run.
- [x C] The page callback receives the current RPM3 cursor and must return `(Changes, ScannedThroughJournalSeq, SourceHighWatermark)` from one FLD-guarded read. A page that fails to advance while source history remains, a cursor beyond source high, or a regressing cursor fails closed.
- [x C] `StorageRelayPublicationGate` now starts in reconciliation-required state. `RunOutboundAsync` and ACK are blocked until `RecoverAndPublishAsync` succeeds; resolver failure or an exception after possibly durable APPLY closes the gate until a full startup rescan succeeds.
- [x C] Updated the integration contract: a single 4,096-operation page is not sufficient authorization to send when later FLD pages remain. The complete source scan cursor must reach the durable ApplyJournal high watermark before any outbound batch is exposed.
- [~ T] Add multi-page >4,096 relay tests, mixed-author empty pages, operation-cap resume, startup-gate default closed, resolver-failure gate closure, callback exception/recovery, and prove no sender observes rows from an unmapped later page.
- [x C] `FldBinSyncHostAdapter.ReadRelayPage` reads complete bounded `ReadRelayBatchPage` groups and the source high watermark; `StorageRelayPublicationGate` invokes it under the composed FLD/Node/Storage guard and reuses one `For(coordinator)` gate.
## 0.72. C-pass: resolve relay identity through the active outbound authority — 2026-11-09

- [x C] Found and corrected a cutover-mode integration gap: after `IProviderPublicationMode` switches outbound sequencing to `StorageMetadataStore`, Node Outbox is intentionally empty. `AppendRelayProvenancePage` now uses `StorageMetadataStore.TryResolveRelaySequences` in metadata-owned mode and retains `Node.TryResolveOutboxSequences` only before cutover.
- [x C] Added `TryResolveRelaySequences` to the durable metadata store. It resolves unique ChangeIds against retained per-author relay rows, returns the assigned metadata relay sequence, and fails closed/clears output if any source row is no longer retained. It stores no duplicate ChangeId index or payload.
- [x C] `AcknowledgeRelayProvenance` now obtains the per-author safe watermark from `GetSafePeerStreamCompactionCursor` after cutover and from Node before cutover. It preserves the already committed RPM3 ACK as a monotonic lower bound during peer retirement/rejoin transitions.
- [x C] Updated the integration contract and RPM3 docs: destination sequence is the active BIN-SYNC per-author outbound coordinate (Node Outbox before cutover, Metadata relay sequence after cutover); ChangeId remains solely in the global ledger.
- [~ T] Add tests for both publication modes, cutover with retained relay rows, missing/compacted ChangeId, safe ACK across peer rollover, and restart/reopen. This code has not been built or tested after the last exact tested baseline `4382c49bffe1608ebabc4a483771b83242cbc81d`.
- [~ C] Continue reviewing metadata-owned peer baseline/relay retention semantics and finish the concrete host composition reference. Physical GC remains disabled.
## 0.71. C-pass: enforce a single publication gate per coordinator — 2026-11-09

- [x C] Made `StorageRelayPublicationGate` construction private and added `For(StorageSyncCoordinator)`, backed by a `ConditionalWeakTable`, so callers using the supported API share one gate for the coordinator's fixed namespace.
- [x C] Updated the integration contract to require the shared factory for every receiver, sender, startup-recovery and ACK path. This removes accidental duplicate gate instances when the host follows the public API.
- [~ C] A host can still bypass the gate by directly calling Node/session APIs; integration must centralize transport scheduling and not expose an alternate sender path. Multiple coordinator instances for the same underlying Node/namespace must not be created by the host.
- [~ T] Verify concurrent `For` calls return the same instance, independent coordinators do not share gates, cancellation releases waiters, and failures always release the semaphore.
## 0.70. C-pass: retry durable frontier reports without minting new history — 2026-11-09

- [x C] Added `SendAcceptedAppliedOriginFrontierAsync`: loads the latest durable local report for the accepted roster and resends that exact report instead of incrementing ReportSequence on transport retry.
- [x C] The resend path verifies the persisted report signature against the accepted roster and checks that the supplied local member seed reproduces the persisted signature before sending. This prevents a retry with the wrong key from silently creating a different signed report.
- [~ T] Add lost-send/retry tests proving the report sequence and payload remain identical across retries, reject wrong member seed, and handle roster rollover with no accepted local report.
- [~ C] Review equivalent resend/retry behavior for other control-plane messages and verify FULL_REPLACE installed-ACK replay semantics before considering control-plane orchestration closed.
## 0.69. C-pass: fail-closed startup cursor reconciliation — 2026-11-09

- [x C] Added `StorageSyncCoordinator.ValidateRelayProvenanceRecovery`: under the composed FLD/Node/Storage guard it verifies RPM3 scope/replica/storage binding and rejects persisted scan or ACK JournalSeq cursors beyond `FldDatabase.GetRelayJournalHighWatermark()`.
- [x C] Added `StorageRelayPublicationGate.RecoverAndPublishPageAsync`: keeps the namespace gate closed while validating the source high watermark, reading/resolving the next durable page, committing RPM3 mapping+cursor, then invoking the sender callback. A stale/replaced source journal fails closed before any send.
- [x C] Updated the integration contract with the concrete FLD high-watermark API and startup ordering. This closes the coordinator-side startup cursor check; host restart policy must still keep all other sender paths disabled until recovery succeeds.
- [~ T] Startup cursor regression, source replacement, map reopen/checksum, crash-before-send and idempotent replay tests remain open. All code since `4382c49bffe1608ebabc4a483771b83242cbc81d` is still unvalidated by CI.
- [x C] Relay/ACK/publication-ceiling/GC host composition source now exists in `samples/FldBinSyncHost`; signed v3.1 vector sources and adapter call shapes are authored. [~ C] The `FldDatabase`→`IDataProvider` snapshot/local-feed/remote-apply bridge is not implemented; generic Storage backend/format compatibility, final metadata byte/cardinality audit and transport scheduler also remain. Execution/conformance belongs to T.
## 0.68. C-pass: gate inbound APPLY and outbound relay exposure atomically — 2026-11-09

- [x C] Extended `StorageRelayPublicationGate` with `ApplyAndPublishPageAsync`, which holds the same async namespace gate across durable remote APPLY → host relay-page read → ChangeId resolution → RPM3 map/cursor commit → sender callback. A crash/failure after APPLY but before map commit cannot expose its relay entries to a sender through this gate.
- [x C] Retained `PublishPageAsync` as the startup/recovery path for already-durable source pages while the host keeps all senders closed; `AcknowledgeAsync` uses the same gate so ACK/pruning cannot race publication.
- [x C] Updated `FLD-DB-INTEGRATION-CONTRACT.md` to require all remote APPLY and outbound send/build paths for one namespace to share one gate instance. The helper is an enforceable host boundary, not a claim that arbitrary bypassing callers are prevented.
- [~ C] The host still must instantiate one gate per namespace, route every receiver and sender through it, compose actual FldDatabase callbacks, and keep startup sending closed until persisted FLD/RPM3/Node state is reconciled.
- [~ T] Commits `187e56b`, `6f0da09`, `d2f263f`, `3805cc3` and `b1e66df` are after the last CI-tested baseline `4382c49`; build/tests must be rerun after C-pass. The baseline's 6 Core and 80 Storage Integration failures remain as enumerated in §0.67.
## 0.67. C-pass: publication scheduler gate and host GC scan adapter — 2026-11-09

- [x C] Added `StorageRelayPublicationGate`: `PublishPageAsync` serializes page read → resolver → atomic RPM3 map/cursor commit → sender callback; `AcknowledgeAsync` serializes safe Outbox ACK → durable host ACK callback → RPM3 pruning; `RunOutboundAsync` is the common gate for other namespace sender/build paths.
- [x C] Added `DelegateApplicationGcReferenceScanner` and `DelegateApplicationGcReferenceScan`. A host can adapt `FldDatabase.OpenReferenceSnapshot()` without a direct FLD reference in BIN-SYNC. The scan owns both the host commit lease and immutable snapshot, disposes the snapshot first, and releases the host lease even if snapshot disposal throws.
- [x C] Constructor validates all delegates and the empty-draft/HLC invariant before taking ownership of either resource, preventing a failed adapter construction from leaking the FLD guard.
- [x C] `FldBinSyncHostAdapter` now composes the actual FLD commit guard, relay-page read, durable relay ACK and immutable reference snapshot; `RecoverAndPublishAllAsync` reconciles persisted FLD/RPM3 cursors before sender enablement. [~ C] The application must still instantiate one adapter/gate per namespace and route every transport sender path through its callbacks.
- [~ T] Exact CI on `4382c49bffe1608ebabc4a483771b83242cbc81d`: BinSync.Net build passed, 6/678 Core tests failed (five OFR1/RCP1 old member-report fixtures missing issued-watermark vectors, one provider mutation codec tombstone fixture missing canonical lifecycle metadata); Fuzz/Lab passed. Storage Integration build passed, 91/171 tests passed and 80 failed on legacy provider-draft OriginSequence / missing IProviderCommitGuard fixture contracts across Ubuntu/Windows/macOS. New gate/scan adapter commits after that baseline have not been built or tested.
- [x C] Startup recovery/publication gating and the concrete FLD host adapter example are implemented; actual FLD API/callback shapes are reconciled in the sample. [~ C] Finish the generic Storage backend/format decision and remaining metadata byte/cardinality audit; physical GC stays disabled.
## 0.65. C-pass: repair exact-head provenance binding compile blocker — 2026-11-09

- [x C] Inspected both failed exact-head workflows for `7036e1544d0e8aa3f8b4a15b2c90d0f51fb13f35`; both stopped at build with CS0103 because `StorageRelayProvenanceMap.IsBoundTo` referenced nonexistent `_scopeId` / `_localReplica` fields.
- [x C] Corrected the binding check to compare the stored canonical root's SHA-256 scope bytes and 16-byte local-replica bytes. This avoids duplicated mutable identity fields and uses the exact same root representation as the persisted map namespace.
- [~ T] Commit `cf08a22158fc1784fe225ef38efd0e2a165d7eb1` is not yet exact-head CI validated. Both previous workflows failed before tests started; no T-pass test result is inferred.
- [x C] FLD host adapter/scanner source and v3.1 control-plane protocol code are implemented. [~ C] Generic Storage backend/format compatibility and final source/byte audit remain before T; physical GC remains disabled.
## 0.64. C-pass: empty relay pages avoid resolver allocations — 2026-11-09

- [x C] `AppendRelayProvenancePage` now fast-paths an empty source page directly to the atomic map cursor update, avoiding zero-length identity/sequence arrays and a no-op Node ledger lookup.
- [x C] The same application guard, map binding and zero-watermark rules remain enforced.

## 0.63. C-pass: source JournalSeq order validated before ledger work — 2026-11-09

- [x C] `AppendRelayProvenancePage` now rejects zero, decreasing or beyond-scanned JournalSeq values before allocating/consulting the Node ledger; same-JournalSeq operations remain allowed as one complete source batch.
- [~ T] Verify malformed page rejection does not touch Node Outbox or Storage cursor.

## 0.59. C-pass: relay provenance resolver page bounded — 2026-11-09

- [x C] `AppendRelayProvenancePage` now rejects more than 4,096 source changes before allocating identity/sequence arrays, matching the coordinator's existing bounded page policy.
- [~ T] Verify boundary values 0, 1, 4,096 and 4,097; ensure rejection occurs before Node resolver or Storage mutation.

## 0.58. C-pass: coordinator helper XML cref made unambiguous — 2026-11-09

- [x C] Replaced an overloaded-method XML `cref` with an unambiguous code reference to avoid documentation compiler warnings.

## 0.57. C-pass: adapter example imports current FLD/BIN-SYNC namespaces — 2026-11-09

- [x C] Added explicit `using` directives to the guarded page-to-RPM3 integration snippet so the referenced FLD and BIN-SYNC types are unambiguous.

## 0.56. C-pass: guarded FLD page-to-RPM3 call sequence documented — 2026-11-09

- [x C] Added a concrete current-API adapter flow to §9.5: acquire `AcquireApplicationCommitGuard(database.AcquireCommitGuard)`, read `ReadRelayBatchPage`, collect `Batch.RelayWrites` ChangeIds with their JournalSeq, then call `AppendRelayProvenancePage`.
- [x C] Clarified that the coordinator helper performs global ChangeId resolution; the host must not duplicate that logic or infer OutboxSeq from field winners.
- [~ T] Compile the adapter after the approved two-DLL extraction and test guard ordering, page retry and missing-ledger identity behavior.

## 0.55. C-pass: coordinator resolves ChangeIds before RPM3 append — 2026-11-09

- [x C] Added `StorageSyncCoordinator.AppendRelayProvenancePage`: accepts transient `(JournalSeq, ChangeId)` source entries, resolves all ChangeIds through `Node.TryResolveOutboxSequences`, converts positive local OutboxSeq values to compact sequence-pair entries and appends mapping+RPM3 cursor atomically.
- [x C] It requires the coordinator application-commit gate, validates the map's scope/local replica/shared StorageDatabase binding, and returns false without writing anything if any ChangeId is no longer retained.
- [x C] The helper stores no ChangeId bytes; global ChangeId ledger remains the sole identity authority. Updated the FLD integration contract, public API guide and changelog.
- [~ T] Test missing ChangeId rollback/no cursor advance, duplicate ChangeId rejection, mismatched map binding, correct lock-order usage, and concurrent source ACK/compaction under the host lease.

## 0.54. C-pass: FLD Manifest and Run structural byte audit — 2026-11-09

- [x C] Audited ManifestCodec: 80-byte fixed base plus 4 bytes/namespace, 64/run, 16/retain, 52+bitmap/result-client and 40/provenance-index entry.
- [x C] Audited RunConstants: 256-byte header, 8-byte footer, 40-byte cluster header, 8-byte field descriptor, optional 24-byte HLC override and 16/32-byte structural slots/segments/refs.
- [x C] Clarified that Run is materialized field state, not a second synchronization history; tombstone is a field-descriptor flag and GhostCount is in the Run header.
- [x C] Added the formulas to `BIN-SYNC-METADATA-BYTE-AUDIT.md`. Full variable payload/PGM/dictionary amplification remains workload-dependent and is not presented as a fixed metadata cost.

## 0.43. C-pass: ApplyJournal and Micro-WAL byte audit — 2026-11-09

- [x C] Audited FLD `ApplyJournal.cs`: 16-byte file header; each record is 20-byte header + APPLY_COMMIT payload + 4-byte CRC32C; per-author relay-ACK sidecar is 12-byte header + 24 bytes/author + CRC; sparse index is 16 bytes/256 records in memory.
- [x C] Audited `ApplyCommitCodec` v3: 139-byte fixed body envelope including the field count; each mutation adds 69 bytes plus value; non-relay-only WAL payload adds relay-index count and varuint indexes; relay-only ApplyJournal records store only relay fields.
- [x C] Audited Micro-WAL v2: 64-byte segment header; each record is 20-byte envelope + payload + 4-byte CRC32C + 0–7 bytes alignment padding.
- [x C] Documented the bounded handoff duplication: full APPLY WAL payload and relay-only ApplyJournal payload coexist until WAL seal/truncation; acknowledged relay payload can be compacted while receipt/protocol metadata remains in ApplyJournal.
- [x C] Documented the FLD reference snapshot's in-memory ChangeId/RecordId/receipt indexes and conservative unsealed-WAL barrier.
- [~ C/O] Manifest/Run, ApplyJournal receipt lifetime/independent compaction, Fld1.Storage page/index/write amplification and full workload cardinalities remain open; do not claim a complete cross-store byte total yet.

## 0.42. FLD source API audit completed — 2026-11-09

- [x C] Located and inspected the current FLD source: `FldDatabase.ReadRelayBatchPage`, `AcknowledgeRelayBatches`, `AcquireCommitGuard`, `ReadUnpublishedLocalDraftHorizon`, `OpenReferenceSnapshot`, and `FldReferenceSnapshot` already exist in `Fld1.Engine.Db`.
- [x C] Confirmed `ReadRelayBatchPage` returns whole ApplyJournal batches for one author and advances `ScannedThroughJournalSeq` across nonmatching-author records; `AcknowledgeRelayBatches` persists the per-author source ACK.
- [x C] Confirmed `FldReferenceSnapshot` provides exactly the four required scanner surfaces: draft horizon, ChangeId, RecordId/tombstone and receipt reference checks. It builds one immutable index under the FLD commit gate and disposes without retaining a file lock.
- [x C] Updated `FLD-DB-INTEGRATION-CONTRACT.md` with exact current signatures and `SYNC-STORAGE-COMPACTION-ROADMAP.md` §7.47 with the source audit.
- [x C] The FLD Database source is now physically consolidated in `Fld1.Database`; its actual APIs are identified. The remaining host adapter must live outside FLD and must not create an Engine→BIN-SYNC dependency or premature package.

## 0.41. C-pass: FLD target API names not misrepresented as current — 2026-11-09

- [x C] Clarified in §9.5 that `ReadRelayBatchPage`, `AcknowledgeRelayBatches` and the ChangeId resolver are target neutral semantic API names, not APIs already exposed by the current pre-extraction FLD branch.
- [x C] The §9.6 sequencing gate remains controlling; the integration contract is a design target, not an implementation claim.

## 0.40. C-pass: source page boundaries made explicit — 2026-11-09

- [x C] The map API and FLD integration contract now require relay pages to end on whole JournalSeq boundaries. A journal transaction with multiple ChangeIds cannot be split across pages; the scanned-through cursor means all source journals up to that boundary were examined.
- [x C] This matches the invariant that a safe relay ACK cannot split a source JournalSeq group.
- [~ T] Add a page-split rejection test and a multi-operation same-JournalSeq page/restart test.

## 0.39. C-pass status head synchronized after test-plan consolidation — 2026-11-09

- [x C] Refreshed `docs/PRODUCTION-STATUS.md` to the current unvalidated head after consolidating the T-pass backlog.
- [x C] The T-pass is now enumerated in one place and remains intentionally unexecuted while C-pass work continues.

## 0.38. T-pass backlog consolidated (execution still deferred) — 2026-11-09

The following is a test plan only; no tests are run in this C pass.

1. **Compile/API:** build Core, Storage, all five samples, XML docs and analyzers on net10.0; resolve any new API/type/nullability/analyzer failures without weakening invariants.
2. **RPM3 persistence:** compare actual rows with `RELAY-PROVENANCE-MAP-v3.txt`; reopen/reconstruct; append empty and multi-entry pages; verify digest stability after append/prune; corrupt cursor marker/length/digest, row key/value, and non-monotonic coordinates. Verify the caller's Node ChangeId resolver prevents duplicate identities before map append.
3. **Relay ACK translation:** no mapping, safe=acked, below-first, exact sequence, within same JournalSeq group, between groups, exact-last, above-last, after partial prune, after full prune; verify zero Outbox ACK never advances positive FLD JournalSeq.
4. **Crash ordering:** crash before map batch, after map+cursor batch, after durable FLD ACK but before map prune, during map prune batch; replay must be idempotent and never over-ACK.
5. **Coordinator broadcast:** publish one checkpoint and send to N sessions; wrong authority seed, stale checkpoint, roster generation mismatch, superseding checkpoint between member sends, and cancellation/retry.
6. **Existing red integration fixtures:** migrate provider-draft OriginSequence to deferred `OriginSequence=0`; update providers for `IProviderCommitGuard`; test freeze-before-stamp and exact-head CI.
7. **GC proof:** adversarial live-reference scanner, lease ordering, membership retirement/rejoin, and independent safe horizons for receipts, ChangeId and tombstone/Ghost. Physical GC stays disabled until all proofs pass.
8. **O-pass:** benchmark metadata row/cardinality footprint, RPM3 cold recovery and hot append/translation, compact mapping-list memory, provider snapshot memory, Storage batch sort, and checkpoint broadcast overhead; revert any regression.

## 0.37. C-pass: production-status head refreshed — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` with the current unvalidated head and the latest coordinator/provenance/sample implementation commits.
- [x C] The historical v3.0 CI baseline remains explicitly historical; no test success is attributed to the C-pass head.
- [~ T] Exact-head build/test matrix remains deferred until the code sweep ends.

## 0.36. C-pass: Unreleased changelog synchronized — 2026-11-09

- [x C] Added the v3.1 C-pass implementation summary to `docs/CHANGELOG.md`, including RPM3 compact provenance mapping, persisted-row sample, byte audit, checkpoint broadcast and host integration sequencing.
- [x C] The changelog explicitly states that current-head validation is pending and physical ChangeId/tombstone/Ghost GC remains disabled.
- [~ T] Promote these notes to a release entry only after exact-head CI, sample execution and v3.1 wire-vector review.

## 0.35. C-pass: production status no longer overstates current CI — 2026-11-09

- [x C] Updated `docs/PRODUCTION-STATUS.md` to distinguish the historical fully verified v3.0 baseline from the current v3.1 C-pass head. It explicitly states that new v3.1 code, RPM3 mapping/checksum, new sample and checkpoint-broadcast changes are not yet built/tested.
- [x C] Updated the public sample status: three pre-v3.1 samples were in the historical baseline; five current executable samples are not yet verified on the current branch.
- [x C] Kept physical ChangeId and tombstone/Ghost GC disabled. No green CI claim is made for the current head.
- [~ T] Exact-head build, full test matrix and sample execution remain deferred until C-pass closure.

## 0.34. C-pass: public API guide aligned with negotiated v3.1 code — 2026-11-09

- [x C] Updated `docs/PUBLIC-API.md`: v3.1 roster/frontier/checkpoint/FULL_REPLACE are no longer described as merely proposed; the guide now distinguishes the v3.1 negotiated extension from the v3.0 wire baseline and keeps physical GC disabled.
- [x C] Documented `StorageRelayProvenanceMap` RPM3 and the host-owned coordinator lifecycle/broadcast surface; linked the orchestration and FLD integration contracts.
- [~ T] Reconcile this guide against the compiled public API XML and exact-head build during T; no API docs are considered release-final before compilation.

## 0.32. Recovery checkpoint branch created — 2026-11-09

- [x C] Created `backup/pre-provenance-checksum-and-control-plane-2026-11-09` at `4026a741b8e19a0d82fe25e4c87fd47cf481852c` before continuing the C pass. Existing earlier backup branches remain intact.

## 0.31. C-pass: authority round serialization documented — 2026-11-09

- [x C] The host orchestration recipe now requires one serialized authority round at a time: publish once, send the same accepted checkpoint to every intended member, then begin the next checkpoint round.
- [x C] Each send revalidates the currently accepted sequence/digest and fails closed if a concurrent round supersedes it; the application host owns the round mutex and cancellation/retry.
- [~ T] Exercise a superseding checkpoint between two member sends and verify the host retries the round rather than silently mixing checkpoint generations.

## 0.30. C-pass: checkpoint broadcast state race closed — 2026-11-09

- [x C] Moved accepted-roster/checkpoint read, authority/generation validation, re-sign and idempotent durable acceptance under the coordinator gate. The verified checkpoint is copied out of the critical section before any awaited transport send.
- [x C] This prevents a concurrent roster/checkpoint transition from racing between validation and re-acceptance while avoiding holding locks across network I/O.
- [~ T] Add a race test with concurrent roster transition/checkpoint broadcast and verify no stale generation is sent.

## 0.29. Cross-repository roadmap synchronized — 2026-11-09

- [x C] Updated FLD-DB `SYNC-STORAGE-COMPACTION-ROADMAP.md` §7.45 to record the independent BIN-SYNC RPM2 provenance-map primitive, its sample and checksum semantics.
- [x C] FLD's roadmap explicitly keeps the full synchronized-database adapter in its later §7.20 implementation order; no premature bridge or database project dependency was introduced.
- [~ C] Cross-store audit remains partial: Outbox fixed sizes are now known, but ApplyJournal/WAL/Manifest/Run and Storage-engine amplification still require the source-level extraction/audit.

## 0.28. C-pass: FLD Outbox byte costs added to cross-store audit — 2026-11-09

- [x C] Audited FLD `OutboxLog.cs` directly: 16-byte file header; each record is 88-byte fixed header + value bytes + 4-byte CRC32C; ACK/state/lease sidecars are 12/20/36 bytes; in-memory sparse index is one 16-byte point per 64 records.
- [x C] Added this to `BIN-SYNC-METADATA-BYTE-AUDIT.md` and explicitly separated FLD Outbox intent from the BIN-SYNC 84-byte mapping row. No payload copy is introduced by the mapping.
- [~ C/O] ApplyJournal, WAL, Manifest/Run and Fld1.Storage page/index/write amplification remain unmeasured; the source paths/layouts must be audited before a complete cross-store total is claimed.

## 0.27. C-pass: reject impossible zero-Outbox positive-FLD cursor — 2026-11-09

- [x C] RPM3 cursor recovery now rejects a positive AckedThroughJournalSeq paired with AckedThroughOutboxSeq=0. The v3.1 adapter contract explicitly states that zero safe OutboxSeq never authorizes a positive FLD relay ACK.
- [~ T] Add malformed-cursor coverage for this impossible watermark pair and the valid partial-group case (positive OutboxSeq with JournalSeq safe-through still zero).

## 0.26. C-pass: public sample index links orchestration guidance — 2026-11-09

- [x C] Updated `samples/README.md` with links to the v3.1 host orchestration recipe and the FLD integration contract, so sample users can find the correct control-plane lifecycle and cross-repository dependency order.
- [~ T] Verify links and examples as part of the final docs/sample CI pass.

## 0.25. C-pass: sample verifies actual RPM2 rows (historical schema, superseded by RPM3) — 2026-11-09

- [x C] Extended `samples/RelayProvenanceMap` to inspect its actual Storage snapshot and compare the three persisted mapping rows, cursor marker/watermarks, endian order, ChangeIds and recomputed SHA-256/XOR mapping-set digest against the deterministic fixture.
- [x C] The sample now checks the durable row representation rather than only the public translation behavior.
- [~ T] Execute the sample and verify its actual bytes match `docs/golden/RELAY-PROVENANCE-MAP-v2.txt`; the code has not been run on this C-pass head.

## 0.24. C-pass: versioned mapping-set checksum and atomic cursor boundary — 2026-11-09

- [x C] Upgraded the provenance cursor to RPM2 (60 bytes; historical, superseded by RPM3): version marker, scanned JournalSeq, ACKed OutboxSeq, ACKed JournalSeq and a 32-byte retained-mapping-set digest.
- [x C] Each mapping row is hashed as SHA-256(key || value); hashes are XOR-aggregated. Append and post-FLD-ACK prune update mapping rows plus cursor/digest in one Storage write batch. Empty pages advance the cursor without changing the digest.
- [x C] Restart reconstructs rows through bounded 1,024-entry pages, recomputes the mapping-set digest and uses constant-time comparison against the durable cursor. Missing/torn/mismatched rows fail closed; no page can be silently skipped.
- [x C] Updated the byte audit, FLD integration contract and deterministic RPM2 golden vector (historical, superseded by RPM3).
- [~ T] Verify exact persisted bytes, append/prune/reopen checksum stability, row corruption, cursor corruption, empty-page digest stability and crash injection around the atomic batch.

## 0.23. C-pass: fail-closed persisted ChangeId validation — 2026-11-09

- [x C] Restart reconstruction now rejects a zero ChangeId in a persisted provenance row, in addition to duplicate IDs, invalid row lengths, out-of-range JournalSeq and non-monotonic destination/source coordinates.
- [~ T] Add corrupt-row tests for zero identity, duplicate identity, malformed cursor marker/length and sequence-order violations.

## 0.22. C-pass: explicit v3.1 host orchestration recipe — 2026-11-09

- [x C] Added `docs/BIN-SYNC-V3.1-HOST-ORCHESTRATION.md` with authority/member round ordering, one checkpoint published once and broadcast to all members, Joining→FULL_REPLACE→installed ACK→Active→new author epoch lifecycle, per-session reader/sequence ownership, and composed host commit-lease rules.
- [x C] The recipe uses the new `SendAcceptedRetentionCheckpointAsync` surface and explicitly keeps timers/scheduling in the application rather than protocol core.
- [x C] Application scheduling is intentionally host-owned: BIN-SYNC exposes explicit roster/frontier/checkpoint/FULL_REPLACE one-message operations and the host orchestration recipe documents round ordering, broadcast, lifecycle and cancellation. No timer/background scheduler is added to protocol core; this is a deliberate architecture boundary, not unfinished runtime work.
- [x C] Deterministic signed-payload vectors and an executable C# vector sample now cover NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN. [~ T] Run the sample, add trust-store decode/verify and negative frame/payload cases; full frame-envelope vectors remain T.

## 0.21. C-pass: one durable checkpoint per authority broadcast round — 2026-11-09

- [x C] Added `SendAcceptedRetentionCheckpointAsync` to the coordinator. It verifies the local authority role, accepted roster generation, currently persisted checkpoint sequence/digest and authority signature before sending.
- [x C] Refactored `PublishAndSendRetentionCheckpointAsync` to publish once and use the same verified send path. Multi-member hosts can now call `PublishRetentionCheckpoint` once and broadcast the same durable signed checkpoint to every Active member without incrementing checkpoint sequence per destination.
- [x C] This keeps checkpoint sequence a round-level authority artifact rather than a per-peer send counter; transport remains host-owned and no timer/background worker was added to protocol core.
- [~ T] Verify same checkpoint can be sent to multiple sessions, stale/forked checkpoint rejection, wrong authority seed rejection and exact accepted-roster generation binding.

## 0.20. C-pass: cross-repository sequencing contract mirrored — 2026-11-09

- [x C] Mirrored the FLD roadmap's required sequencing and two-DLL dependency into `docs/FLD-DB-INTEGRATION-CONTRACT.md` §9.6.
- [x C] The contract now explicitly distinguishes the BIN-SYNC Storage-only Metadata Store adapter from the later synchronized-FLD JournalSeq provenance adapter; neither may be placed inside `Fld1.Storage`.
- [x C] The host adapter is now bound to the public `FldDatabase` API in the optional sample. [~ C] Finish generic Storage backend/API compatibility and exact-head FLD/sample build validation.

## 0.19. Cross-repository adapter dependency revalidated — 2026-11-09

- [x C] Re-read FLD-DB `SYNC-STORAGE-COMPACTION-ROADMAP.md` §§7.19–7.20 and the current FLD branch project graph. The JournalSeq→ChangeId→OutboxSeq adapter is explicitly a later integration gate, after neutral semantic operations/Data Provider conformance, BIN-SYNC persistence refactor, FLD Storage extraction, and the Storage-only Metadata Store adapter.
- [x C] Confirmed FLD still builds the pre-extraction `Fld1.Core`, `Fld1.Engine`, `Fld1.Manifest`, `Fld1.Run` and `Fld1.Wal` projects separately; `Fld1.Storage` is not yet the persistence backend of `FldDatabase`. The approved two-public-DLL boundary is not yet the actual project/package graph.
- [x C] FLD roadmap explicitly forbids a premature direct Node↔FldDatabase bridge or early NuGet adapter. Therefore the current BIN-SYNC Storage map primitive and sample are valid independent C work, but a concrete synchronized-FLD adapter must remain blocked until the upstream architecture gates close. Do not violate the two-DLL boundary just to clear a checkbox.
- [x C] Host adapter/scanner source is implemented and remains separate from the Storage-only Metadata Store adapter. [~ C] Cross-repository build validation and generic Storage backend compatibility remain; Storage-only mode must not instantiate `FldDatabase`.

## 0.18. C-pass: relay provenance storage golden vector — 2026-11-09

- [x C] Added `docs/golden/RELAY-PROVENANCE-MAP-v2.txt` with deterministic root, cursor key/value and OutboxSeq-ordered mapping key/value bytes, including exact lengths and endian rules.
- [~ T] The vector is a source-derived fixture, not yet compared with actual persisted rows. T must run the sample against a temporary StorageDatabase and compare byte-for-byte; do not mark storage/wire golden-vector coverage complete from the document alone.
- [x C] Complete frame registry is audited and signed NRS2/OFR2/RCP2/FRA2/FULL_REPLACE_BEGIN payload vectors plus executable sample are authored. [~ T] Compare against C# encoder/decoder and full frame envelopes.

## 0.17. C-pass: namespace range matches Node API — 2026-11-09

- [x C] The relay provenance map now accepts the full unsigned 16-bit namespace range, including namespace 0, matching `StorageSyncCoordinator` and Node's namespace contract. The stricter positive-only rule belongs to the signed namespace-GC store, not this generic mapping primitive.
- [~ T] Add namespace 0 and 65535 boundary checks to the map conformance cases.

## 0.16. C-pass: sample catalog consistency — 2026-11-09

- [x C] Updated `samples/README.md` to remove the stale claim that there are only three executable samples after adding `RelayProvenanceMap`.
- [x C] The sample catalog now lists the relay mapping sample and its run command alongside the existing canonical v3 examples.
- [~ T] The complete sample set remains unexecuted on the current C-pass head by design.

## 0.15. C-pass: single ChangeId index on recovery — 2026-11-09

- [x C] Restart reconstruction now inserts each decoded ChangeId directly into the shared `HashSet<BatchId>` and uses that same set to detect duplicates. Removed a second temporary identity set that duplicated all ChangeIds during recovery.
- [~ T/O] Verify the shared set and ordered list remain consistent through restart, append and ACK prune; quantify memory/throughput trade-offs in O.

## 0.14. C-pass: compact value-type ChangeId membership — 2026-11-09

- [x C] Replaced the temporary string-hex membership index with `HashSet<BatchId>` and stored the value-type `BatchId` in each in-memory mapping entry. This avoids a separate 32-character string allocation per retained ChangeId while preserving O(1) average duplicate detection.
- [x C] Persisted mapping values remain the canonical 16-byte ChangeId; restart reconstruction builds the same value-type index.
- [~ T/O] Measure the actual managed heap cost of the list + value-type HashSet against the 16-byte-per-entry durable row; compare with a sorted binary-search identity check before accepting the memory trade-off.

## 0.13. C-pass: avoid per-page full-index ChangeId scans — 2026-11-09

- [x C] Added a shared in-memory ChangeId membership set alongside the OutboxSeq-ordered mapping index. AppendPage now checks existing and same-page duplicate identities in O(1) average time per ChangeId rather than rebuilding/scanning a set from every retained mapping on each page.
- [x C] Restart reconstruction populates both indexes from the same bounded cursor pages. Post-FLD-ACK pruning removes ChangeIds from the membership set together with their mapping rows.
- [~ T/O] Validate append/prune/reopen index consistency and measure retained-memory overhead of the extra 32-hex-character membership keys; if allocation/footprint is worse than an alternative compact identity index, revert or replace based on measurements.

## 0.12. C-pass: public relay provenance sample — 2026-11-09

- [x C] Added `samples/RelayProvenanceMap` as a standalone executable sample referencing only the approved BinSync.Core + BinSync.Storage runtime projects.
- [x C] The sample demonstrates multiple ChangeIds sharing one FLD JournalSeq, partial-ACK retention, durable map reopen, safe-through translation, map pruning after FLD-ACK confirmation and rejection of an unmapped watermark.
- [x C] Added the sample to `samples/README.md`. It is a storage mapping sample, not a claim that the concrete FLD host adapter or lifecycle scheduler is complete.
- [~ T] Build/run the sample with the rest of the public samples during the T pass; verify it on all supported CI platforms.

## 0.11. C-pass: source-derived BIN-SYNC metadata byte inventory — 2026-11-09

- [x C] Added `docs/BIN-SYNC-METADATA-BYTE-AUDIT.md` from the actual `StorageMetadataStore` encoders/key builders. It records partition-root framing, every current durable row kind 0–22, fixed key/value sizes, dynamic PendingApply/peer-stream/payload formulas and row cardinality ownership.
- [x C] Added the `StorageRelayProvenanceMap` cursor/mapping row sizes and key/value byte layout. Explicitly separated encoded row bytes from storage-engine page/index/WAL/object overhead.
- [x C] The audit confirms the current non-duplication model: one global 45-byte ChangeId identity/provenance value, one payload envelope per retained relay/outbound operation, stream-level peer progress, no per-peer payload copies and no per-operation ACK bitmap.
- [~ C/O] Cross-store totals remain open: FLD Outbox/ApplyJournal/WAL and Fld1.Storage page/index/write amplification are not included; representative cardinalities and payload distributions are required before claiming a total footprint or GC savings.
- [~ T] Audit formulas should be checked against golden persisted-row fixtures during T. No tests/build were run.

## 0.10. C-pass: fail-closed watermark rule documented — 2026-11-09

- [x C] Updated the FLD integration contract to state that an OutboxSeq above the last durable mapping cannot be translated and must wait for source-page scan/resolution catch-up.
- [x C] Reaffirmed the required order: durable FLD relay ACK first, provenance-map pruning second. A crash between them replays the idempotent FLD ACK.
- [~ T] The new above-last watermark rejection and ACK/prune crash ordering still require dedicated tests; no exact-head test result is claimed.

## 0.9. C-pass: fail closed on an unmapped safe watermark — 2026-11-09

- [x C] `TranslateSafeOutboxSequence` now rejects a safe OutboxSeq above the last durable mapping instead of translating it to the scanned-through JournalSeq. The host must first catch up the FLD page scan and ChangeId→OutboxSeq resolution; an unrepresented destination sequence can never advance the FLD relay ACK.
- [x C] The already-committed watermark remains idempotent, and a safe watermark equal to the latest mapped sequence may advance through the durable source scan cursor.
- [~ T] Add no-mapping, below-first, exact-boundary, between-two-mappings, equal-last, above-last and post-prune replay tests.

## 0.8. C-pass: remove recovery sort from provenance index — 2026-11-09

- [x C] Recovery now consumes the persisted OutboxSeq-ordered prefix directly and validates strict destination sequence plus nondecreasing source JournalSeq as each 1,024-row page arrives. Removed the O(N log N) post-load sort and second validation pass.
- [x C] The storage contract now documents the ordered key/value layout and O(log N) lower-bound translation.
- [~ T/O] Verify storage cursor lexicographic order, duplicate/out-of-order key rejection, restart reconstruction and large-stream cold-start cost in the dedicated T/O pass.

## 0.7. C-pass: order provenance rows by destination sequence — 2026-11-09

- [x C] Changed the durable mapping key order to `OutboxSeq` (big-endian), with `JournalSeq + ChangeId` in the value. Prefix-cursor recovery now naturally yields the exact order needed by the per-author destination stream.
- [x C] Safe-ACK translation now uses lower-bound binary search over the OutboxSeq-ordered index (O(log N)), instead of a linear scan. JournalSeq monotonicity is validated during page append and restart reconstruction.
- [x C] Mapping deletion uses the OutboxSeq primary key while retaining the source JournalSeq threshold semantics; no operation payload or peer-specific copy is introduced.
- [~ T/O] Restart/roundtrip, same-JournalSeq ordering, safe-watermark boundaries and large-cardinality translation benchmarks remain deferred to T/O. This changes the newly introduced mapping row layout before it is consumed by a released adapter; no legacy migration is needed.

## 0.5. C-pass: avoid global-commit-driven mapping reloads — 2026-11-09

- [x C] Replaced cache invalidation on global `StorageDatabase.LastCommitSeq` with a database-scoped shared in-process cache keyed by (scope/local-replica root, namespace, author). Every unrelated Metadata Store/provider commit previously forced a full reload of the unacknowledged mapping index; this no longer happens.
- [x C] Multiple `StorageRelayProvenanceMap` instances over the same `StorageDatabase` share the same state dictionary and the same `StorageDatabaseTransactionGate`, so same-process adapters see each other's committed mapping/ACK updates without duplicate scans.
- [x C] Restart still reconstructs the cache from durable rows using 1,024-entry prefix-cursor pages. The Storage single-writer fence remains the cross-process boundary; no independent process may mutate the same database concurrently.
- [~ T/O] Measure cold reconstruction, hot append/translation, memory per retained mapping and contention with Metadata Store commits. Verify shared-instance visibility and restart reconstruction in T.

## 0.4. C-pass: mapping ownership and restart validation hardening — 2026-11-09

- [x C] `AppendPage` now snapshots caller-owned ChangeId bytes before writing and retains those same owned bytes in the in-memory index; caller mutation cannot desynchronize the cache from the committed key.
- [x C] Restart reconstruction rejects duplicate persisted ChangeIds within one author stream and validates monotonic JournalSeq/OutboxSeq coordinates directly in OutboxSeq key order.
- [x C] Mapping recovery is paged at 1,024 rows, bounding temporary page allocations. The retained in-memory index remains O(unacknowledged mappings); this is explicit and must be included in the byte/cardinality audit.
- [x C] The concrete FLD caller, namespace publication gate and host GC scanner are implemented in the optional sample; the mapping store alone still does not authorize physical GC, which remains disabled pending T proof.
- [~ T] Build, restart, malformed-row and crash-cut tests remain deferred until the C pass is complete.

## 0.3. C-pass: bounded mapping recovery and cross-repository contract — 2026-11-09

- [x C] Mapping recovery now reads durable rows through 1,024-entry `StorageSnapshot.PrefixCursor` pages instead of materializing the complete persisted prefix in one temporary list. The in-memory ordered index remains proportional to unacknowledged mappings, which is required for logarithmic ACK translation; no claim of O(page) total index memory is made.
- [x C] Updated `docs/FLD-DB-INTEGRATION-CONTRACT.md` §9.5 with the exact host call order: page read → ChangeId resolution → atomic map+scan-cursor commit → publication gate/send → safe Outbox ACK translation → durable FLD relay ACK → mapping prune commit.
- [x C] The FLD Database source path and public relay/reference APIs are now identified under `src/Fld1.Database/Fld1.Engine/Db/FldDatabase.cs`. The RPM3 coordinator resolver is ready for a host adapter; no FLD types are added to `Fld1.Storage`.
- [~ T] No tests/build were run for these C commits, by design. Compile/API conformance, same-JournalSeq multi-operation, sparse author pages, crash before/after map commit, FLD-ACK-before-prune replay and restart reconstruction remain T-pass cases.

## 0.2. C-pass: durable JournalSeq provenance mapping primitive — 2026-11-09

- [x C] Added `StorageRelayProvenanceMap` in `BinSync.Storage`. It persists per-(scope, local replica, namespace, original author) `JournalSeq → ChangeId → OutboxSeq` rows and the `ScannedThroughJournalSeq` cursor in one Storage write batch. Empty author pages advance the scan cursor without inventing mappings.
- [x C] The mapping codec keeps source JournalSeq separate from BIN-SYNC OutboxSeq and protocol OriginSequence. OutboxSeq is stored big-endian in the ordered key; JournalSeq and the stable 16-byte ChangeId are stored in the value.
- [x C] Added monotonicity/identity validation, same-journal multi-operation support, duplicate ChangeId rejection, cursor rollback rejection and fail-closed malformed-row checks. Prefix-cursor reload reads OutboxSeq order directly and validates nondecreasing source JournalSeq without a post-load sort.
- [x C] Added safe ACK translation: the first source JournalSeq containing any OutboxSeq above the BIN-SYNC safe watermark is retained in full; the preceding JournalSeq is the FLD safe-through. If no such mapping remains, the durable scanned-through cursor is safe.
- [x C] Added `CommitAfterFldRelayAck`: it validates the caller's translated watermark and atomically persists the acknowledged source/destination cursors while pruning mapping rows. Its contract requires the caller to durably call FLD `AcknowledgeRelayBatches` first; after a crash before map pruning, replaying that FLD ACK is idempotent and mappings remain available.
- [x C] The durable mapping primitive is now wired to actual `ReadRelayBatchPage` groups, the namespace publication gate, per-author ceilings, safe ACK translation, durable FLD relay ACK, RPM3 prune and startup reconciliation by `samples/FldBinSyncHost`. [~ C] The separate `FldDatabase`→`IDataProvider` snapshot/local-feed/remote-apply bridge remains open; T-pass must validate this relay adapter.
- [~ T] Compile/API conformance and crash-boundary tests are deferred to the dedicated T pass, as requested. No test result is claimed for this new code.

## 0.1. Resumption audit — 2026-11-09

- [x C] Re-read the active roadmap, v3.1 roster/GC addendum and FLD integration contract against the current branch files. The durable control-plane send/receive primitives are present in `StorageSyncCoordinator`: roster send/accept, OFR2 publish/send and receive/accept, RCP2 publish/send and receive/accept, and durable FULL_REPLACE installed-ACK exchange. These are callable APIs, not a periodic lifecycle scheduler; do not describe them as an autonomous control-plane loop.
- [x C] Confirmed `AcquireApplicationCommitGuard(hostLeaseFactory)`, host scan snapshot contracts, FULL_REPLACE host draft barrier, and all three mandatory GC scanner entry points are already wired in the coordinator. Physical GC remains disabled.
- [x C] Confirmed the provider snapshot path uses sorted-key lower-bound seeking and bounded 1,024-entry pages. It shares immutable value arrays but still copies an O(key-count) sorted index; therefore only payload duplication is closed, not total snapshot memory bounded to O(page).
- [x C] The synchronized-FLD adapter remains a separate integration boundary from the Storage-only Metadata Store adapter. The host sample now binds the FLD page read, RPM3 map append, publication/send gate, safe-ACK translation, durable FLD ACK and post-ACK map pruning. Do not put FLD semantic types into `Fld1.Storage` or add a third database runtime DLL. [~ T] End-to-end execution and restart proof remain.
- [x C] `FldBinSyncHostAdapter.CreateReferenceScanner` opens one `FldReferenceSnapshot` through `AcquireReferenceSnapshotGuard`, maps the draft horizon and receipt/ChangeId/RecordId indexes, and returns a scan lease that disposes the snapshot before releasing the FLD guard. [~ T] Verify disposal and composed lock ordering.
- [x C] Public coordinator APIs are explicit one-message operations and the host orchestration recipe covers member lifecycle (Joining→FULL_REPLACE→installed ACK→Active; Retired→fenced rejoin), one-checkpoint-per-round broadcast, cancellation, transport ownership and persistence. Periodic scheduling remains deliberately application-owned; no background timer/worker belongs inside protocol core.
- [x C] Public sample and canonical signed-payload vectors are now aligned to the audited 0x01–0x1C frame registry. [~ T] Execute the vector sample and verify encode/decode/trust-store/negative cases; no test pass is claimed yet.
- [x C] Fixed framing and row formulas are documented across NodePersistence v17, StorageMetadataStore, FLD Outbox/ApplyJournal/Micro-WAL, Fld1.Storage, Manifest and Run. [~ O/T] Actual workload cardinalities, adaptive receipt grouping savings, per-author progress compaction, PGM/dictionary amplification, in-memory indexes and write/snapshot amplification still require measurement.
- [ ] Do not mark any of these gates complete from documentation alone. The next C slices are the host integration adapter/mapping and public orchestration/sample surface; then the exact source/byte audit. T/O/F remain separate.

## 0. Active status at current C-pass HEAD

- **Current branch/head at this refreshed status:** `c5347c6ecc89e49d727cce88215529b89ece79e3` (latest roadmap/status commit; latest production-code commit `7f50a0007ccec1f8a2c53b9782b76d5829dbb7bf`; exact-head CI not yet validated). C-pass commits added the durable relay provenance map (`b041f621`), identity/order hardening (`24b6b590`, `9b50bd0`), paged recovery (`2dcfacc`), shared cache (`51d96d1`) and OutboxSeq-ordered key/binary-search translation (`b9fcbf38`).
- **Last known fully green checkpoint:** `c56d2e206d0c8578cbfe66158d6e6f1e0824750c` (Core 676/676, Fuzz/Lab passed, Storage Integration passed on Ubuntu/Windows/macOS). Later C-pass code is not covered by that result.
- **Workflow:** C implementation first in small commits with immediate roadmap updates; then T (fixtures, fault injection and conformance), O (measured optimization), F (release). Build errors are repaired immediately; test feedback is recorded but test-suite redesign stays in T.
- **C implemented:** signed namespace roster/frontier/checkpoint codecs and durable storage; all-active FULL_REPLACE install ACK tracking; sender/receiver with durable staging/resume; forgotten-origin replay fence; receipt/ChangeId/tombstone compaction plumbing with all physical gates disabled; bounded provider prefix cursors and streamed snapshot row validation; member frontier and authority checkpoint production APIs; OFR2 signed issued-origin/max-HLC reports, pending-draft barriers and terminal author closure; NRS2 immutable retired-origin epoch closures; all-active applied-through-final-issued checkpoint derivation; mandatory post-FULL_REPLACE HLC floor refresh/Node rehydration callback; FRA2 signed installed ACK HLC floors and RCP2 authority checkpoint binding requiring identical floors from all Active members; ordered coordinator→Node→shared-Storage application commit barrier with shared-database identity enforcement and deadlock-safe Node checkpoint persistence; local author-epoch terminal closure/rejoin; freeze-time per-namespace/author/epoch OriginSequence allocator; durable stamping of published winners before provider feed ACK; FULL_REPLACE draft-to-published identity upgrade; durable coordinator control-plane exchange for NRS2/OFR2/RCP2/FRA2; immutable old-epoch closure enforcement after rejoin.
- **Origin contract:** provider feed cursor is not OriginSequence. Coalescible drafts carry ChangeId + author epoch and OriginSequence=0. Metadata Store allocates contiguous OriginSequence only at immutable freeze and persists it with the ChangeId ledger. Coalesced-away drafts consume no origin number. Remote operation apply requires a complete non-zero origin tuple.
- **GC status:** receipt, ChangeId and tombstone/ghost physical deletion remain fail-closed/disabled. Receipt additionally requires retired transport epoch, preserved receive cursor/digest chain, signed origin frontier and no PendingApply. Author-issued horizon, retired-origin closure and checkpoint-bound global HLC floor are represented by OFR2/NRS2/FRA2/RCP2. Physical receipt, ChangeId and tombstone/ghost GC remain disabled until host WAL/FLD plus PendingApply/relay/reservation/snapshot live references are covered by the composed lease and the T-pass proves crash/replay safety. Never enable physical GC in the C pass.
- **Latest observed CI before the current code slice:** exact build succeeded and Fuzz/Lab passed; Core had one outdated tombstone fixture failure and Storage Integration had 53 failures sharing the outdated zero-OriginAuthorEpoch fixture assumption. These are T-pass fixture updates, not a reason to relax production v3 validation. Exact-head CI for the current code is pending.
- **Remaining C work:** the only major upstream blocker is the approved two-DLL FLD extraction plus public Storage API conformance; after that, implement the concrete FLD page-read→ChangeId resolver→RPM3 append→publication/send gate→safe-ACK translation→durable FLD ACK→mapping prune adapter and the `IApplicationGcReferenceScanner` wrapper. The BIN-SYNC-side resolver/append primitive, RPM3 checksum/cursor, host control-plane recipe, signed payload vectors and executable sample are present. Fixed source-derived metadata formulas are documented. App scheduling is deliberately host-owned. Preserve no legacy API/fallback decoder.
- **T pass is not complete.** Historical checklist entries below are execution history; this active section and latest dated execution entries supersede earlier snapshots.

### C-pass continuation — 2026-11-09

- [x C] Updated the test-only `VolatileMetadataStoreAdapter` to implement `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeIdentity`. It returns fingerprint and immutable non-zero Origin from the same ChangeId ledger entry, rejects identities owned by incoming routes, and fails closed for incomplete frozen identities. This restores interface completeness without weakening production validation.
- [ ] T remains deferred: migrate provider-draft fixtures to non-zero author epoch with `OriginSequence=0`; update test providers to implement `IProviderCommitGuard`; add freeze-before-stamp crash-window coverage. The exact-head CI must be rerun after the C pass.
- [x C] Authority/member one-round orchestration APIs and host recipe, signed-payload vectors/sample, fixed source-derived metadata byte audit and frame registry inventory are present.
- [~ C] Remaining upstream C gates: generic Storage backend/WAL compatibility, final public API/package conformance and transport-scheduler ownership. Concrete FLD relay publication/ACK and reference-snapshot adapter source are implemented in §0.93–0.94. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled until composed host live-reference proof and adversarial T pass.


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

- [x C] Source-derived fixed framing/row formulas are documented for StorageMetadataStore, NodePersistence v17 checkpoint, FLD Outbox, ApplyJournal, Micro-WAL v2, Fld1.Storage snapshot/WAL, Manifest and Run. [~ O/T] Actual workload cardinalities, variable PGM/dictionary amplification, in-memory indexes, filesystem/page overhead and measured write/snapshot amplification remain open.
- [ ] Establish one canonical v3 format marker and row codec; reject absent/old/unknown markers.
- [x] Relay/outbound payload rows persist only ChangeId[16] + payloadLength[u32] + payload; Origin is hydrated from the single global 45-byte ChangeId identity row. Missing ledger rows and author/stream mismatches fail closed. Physical compaction now deletes current row kinds 20/21 after reopen; exact CI passed 155/155 on all three OS platforms. [~] Full cross-store cardinality/byte audit remains.
- [x] Node global ChangeId ledger stores a fixed 16-byte BLAKE3 identity digest that binds semantic mutation + immutable Origin tuple in one hash pass; no separate 29-byte Origin suffix is stored in the ledger value. [~] Storage metadata identity-row cardinality and cross-store duplication still require a byte-level audit and benchmark.
- [ ] Collapse repeated author IDs in ACK/pushed/offset maps into one sparse progress row only if benchmarks prove a win.
- [x C] `AcknowledgeOutboundBatch` validates the exact pushed reservation (peer/epoch/namespace/author stream, BatchId, cursor end and digest), then persists `AckedCursor`, digest-chain tail and `PendingBatch=null` together in the single peer-stream row/Storage batch. No separate reservation-delete transaction exists. [~ T] Inject crash/reopen around this commit and prove exact retry/duplicate ACK behavior.
- [x C] Audited Inbox receipt, per-author receive cursor and per-sender/epoch/namespace digest-chain tail: their keys/cardinalities and retry/ACK semantics differ (per BatchId vs per author stream vs per sender stream). A single receipt cannot replace these without duplicating stream state per batch or losing digest/cursor boundaries; keep the three compact row kinds separate. [~ O] Revisit only with measured cardinality/byte evidence and a proof preserving all three semantics.
- [ ] Remove duplicate relay payload copies and unnecessary WAL/ApplyJournal duplication only after recovery proof.
- [~ C] On Metadata-Store-owned publication cutover, Node now drops its namespace-local `GlobalChangeLedger`; Metadata Store remains the only ChangeId authority. `SeenChanges` WAL replay markers are deliberately retained as a separate recovery concern. Cutover/restart assertions added; exact-head CI pending.
- [~] Record before/after bytes/op, bytes/peer-author, allocations, mutation latency and compaction CPU at 500/10k/100k/1m rows. Exact row saving is 12 bytes per retained relay/outbound operation (32+payload → 20+payload); the durable benchmark completed successfully: 10k rows 408.4 µs / 8.14 KB, 100k rows 375.9 µs / 10.34 KB, 1M rows (1,024-operation freeze) 196.9 µs / 7.77 KB. Against the prior recorded baselines (449.2 µs at 10k, 434.1 µs / 10,944 B at 100k, 232.8 µs / 7.84 KB at 1M), these measurements are faster with slightly lower allocation; compare only as same-workflow directional evidence, not as a cross-machine guarantee.

### Phase 3 — Publication and FLD Storage contract

- [x] Provider feed/FLD Outbox is the sole authority for unpublished local changes in Metadata-Store-owned publication mode; no parallel MetadataStore PendingLocal table. The cutover fence rejects a non-empty local feed/Node Outbox state that would make ownership ambiguous.
- [x] Verify LWW field coalescing across provider pages; field writes and record-lifecycle tombstones use distinct coalescing identities. Provider conformance separately covers lifecycle tombstone/ghost ordering.
- [x C] The public `IDataProvider.ApplyRemoteBatch` contract explicitly requires remote APPLY to leave the local unpublished feed and its ACK state untouched; `StorageDataProvider` follows this boundary. [~ T] Same-field remote-wins/restart conformance remains Phase T.
- [x] Freeze ChangeId + immutable Origin + payload into Metadata Store atomically; provider source-feed ACK follows durable freeze and Node checkpoint persistence.
- [x] Freeze replay is idempotent when metadata commit succeeds but provider ACK fails; frozen ChangeIds are recognized and only the later unfrozen suffix is coalesced/published.
- [x C] The optional FLD host adapter resolves each source ChangeId to the active BIN-SYNC immutable identity and appends the JournalSeq↔OutboxSeq RPM3 mapping; JournalSeq is never used as OriginSequence. [~ T] Verify end-to-end before/after cutover and restart.
- [ ] Complete Storage/DataProvider/MetadataStore conformance including reopen and crash boundaries.
- [x] Storage DLL remains protocol-agnostic; field/HLC/tombstone semantics stay in Database DLL.

### Phase 4 — Safe GC, independently proven

#### 4.1 Inbox receipts

- [x] Conservative retired-transport-epoch receipt GC and PendingApply retirement guard exist in earlier Storage-backed implementation.
- [x C] Retirement now blocks old PendingBatch reclamation unless the exact reservation is ACKed or the replacement baseline for the same author epoch covers `old StreamOffset + PendingBatch.CursorEnd`. Before the retired-through fence, a stale epoch may only resume/mark-pushed/ACK that exact reservation; it cannot reserve new data. Epoch-zero and cross-author-epoch baselines never discharge the reservation.
- [x] Revalidated canonical v3 receipt identity. Old receipt key/value formats are rejected; v3 rows with unknown transport epoch are retained and never assigned guessed provenance.
- [x T] Added tests for short-baseline blocking, exact stale-epoch retry/ACK, replacement-baseline supersession and author-epoch mismatch. Exact-head CI at `2f75b26` passed 168/168 on three OS. Atomic retirement transaction remains one commit; dedicated injected-crash-at-commit-boundary coverage remains a Phase T follow-up.

#### 4.2 ChangeId ledger

Mandatory extension: [BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md](BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md). The v3.1 implementation is substantially present and remains under C/T validation; physical receipt, ChangeId and tombstone/Ghost GC remain separately disabled until their independent proof gates pass.

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
- [x C] Repository code search for `TcpSyncSession`, `TcpSyncClient` and `LegacyTcp` returns no remaining references; current TCP samples use the generic `SyncSession`/`ISyncTransport` path through transport adapters. [~ T] Run the full solution/sample build and public API surface audit on the exact final head.
- [x] Removed optional immutable-Origin provenance negotiation: v3 HELLO/HELLO_ACK carry and require ProtocolMajor=3; `SessionCapabilities` contains no provenance/downgrade bit and decode rejects every other major.
- [x C] On Metadata-Store-owned cutover, Node drops its namespace `GlobalChangeLedger` and Outbox payload projections; `SeenChanges` remains only as the distinct WAL replay marker. Cutover/restart assertions pass.
- [x C] Mandatory `BSP3` provider marker and active-generation pointer are written atomically on fresh open; unmarked/unknown partitions fail closed. Current state keys are generation-scoped; LocalChange rows require exactly one nonzero ChangeId suffix and no read-time synthesis. Exact checkpoint CI green.
- [ ] Ensure all public samples use canonical v3 API.

### Phase 6 — Conformance, performance and release

- [ ] Map every v3.0 normative requirement and every mandatory v3.1 addendum requirement to unit/integration/fuzz/lab tests and exact golden vectors; include capability-negotiation downgrade/absence behavior.
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

- [x] Tightened the compact-row corruption boundary with explicit fail-closed tests for orphaned payload rows and ledger/stream author mismatches during hydration (see 2026-11-08 compact-row fix below).
- [x] Fixed the compact-row compaction issue: deletion now targets current row kinds 20/21, with reopen assertions proving physical removal while retaining ChangeId identity (see 2026-11-08 compact-row fix below).
- [x C] Namespace membership roster, per-author issued/applied frontiers, FULL_REPLACE-bound checkpoint, retired-origin closure and replay fence are implemented. [~ T] Physical GC remains disabled pending composed host live-reference proof, safe-horizon adversarial tests and storage/recovery validation.

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
- [x C] Legacy TCP-specific session/client entry-point source search is clean; current TCP samples use the canonical generic session/transport API. [~ T] Complete compile/reference/API audit in Phase T.
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

- [x C] Added `IMetadataNamespaceGcStore` as a durable metadata-store contract for accepted namespace roster, per-member Origin frontiers, retention checkpoints, FULL_REPLACE installation ACKs, and protocol-only tombstone-GC proof. This is a new production API, not merely test scaffolding.
- [x C] Added `StorageNamespaceGcStore` backed by the opaque Storage engine. It stores roster certificates, current-roster pointers, signed member frontiers, signed retention checkpoints and FULL_REPLACE ACKs under a separate key prefix; it shares the Storage transaction gate and does not rewrite ChangeId identity rows.
- [x C] Added checks for roster generation rollback/fork, authority continuity, member state transitions, Joining→Active only after a durable installed ACK, per-member frontier monotonicity, retention checkpoint monotonicity/previous-digest chain, checkpoint overclaim, all-Active-member installation, and protocol tombstone proof.
- [x C] Added v3.1 frame IDs and `SyncSession` send/receive adapters for signed roster, Origin frontier, retention checkpoint and FULL_REPLACE installed ACK. The methods fail closed unless the relevant v3.1 capabilities were negotiated; frontier/checkpoint and installed-ACK paths also require roster capability.
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
- [x C] Code slice: accept registered v3.1 GC frame IDs in `ReceiveExpectedExclusiveAsync`; the upper bound is now `NamespaceGcFrames.FullReplaceAbort` rather than hard-coded v3.0 frame `0x13`.
- [~ T] Exact-head test failure on `640af0d` is not yet diagnosed. Retrieve the test failure details / TRX before changing production behavior. FLD Storage Integration run `42498381271` was still in progress when this roadmap entry was written.
- [x C] FULL_REPLACE BEGIN/CHUNK/COMMIT/ABORT transfer state machine is implemented by `StorageFullReplaceReceiver`: signed roster/authority validation, durable bounded staging, contiguous page/index and frame-sequence enforcement, restart progress, ABORT-vs-CHUNK/COMMIT arbitration, durable bounded abort, digest-checked provider switch, composed host commit/reference scan, HLC-floor advancement and signed installed ACK. The receiver keeps the switch separate from GC.
- [x C] Namespace roster acceptance/lifecycle validation, signed OFR2 member reports (including per-epoch issued watermarks), authority retention checkpoints, all-Active-member minimum safe frontier, retired-origin closure, FULL_REPLACE-bound HLC floor and checkpoint replay fence are implemented in `StorageNamespaceGcStore`, `OriginFrontierProtocol` and `StorageSyncCoordinator`.
- [x C] Coordinator APIs publish/send/receive accepted rosters, member frontiers and retention checkpoints under the composed application commit guard. `StorageMetadataStore` consults `IsOriginCoveredByRetentionCheckpoint` at incoming/apply boundaries. Host reference scanner contract is enforced for FULL_REPLACE/frontier publication and physical GC proof.
- [x C] The concrete FLD Database host adapter/scanner is now implemented in the optional `samples/FldBinSyncHost` project; the coordinator/store primitives are bound to actual FLD APIs. [~ T] Build and conformance remain.
- [ ] Do not enable ChangeId or tombstone/ghost GC during the C pass. Their separate safe-horizon proofs and fail-closed behavior remain mandatory.
- [ ] After all production code slices have C markers, run the dedicated T pass: targeted tests, crash/fault injection, multi-peer/rejoin/resurrection, fuzz/resource bounds, samples, benchmarks and exact-head CI.


### 2026-11-08 — v3.1 FULL_REPLACE C-pass: wire envelopes and session transport

- [x C] Added canonical bounded codecs for FULL_REPLACE_BEGIN, FULL_REPLACE_CHUNK, FULL_REPLACE_COMMIT and FULL_REPLACE_ABORT in `FullReplaceTransferProtocol.cs`. BEGIN binds namespace, generation, accepted-roster generation/digest, provider feed watermark, expected entry count and digest. CHUNK carries a bounded page (≤4096 mutations, ≤16 MiB) with strict length/trailing-byte validation and canonical ProviderMutation decoding. Control frames carry only namespace + generation.
- [x C] Added capability-gated `SyncSession` send/receive adapters for the four transfer frames on a dedicated per-namespace transfer stream. Receiving frames returns validated envelopes only; it does NOT stage or commit provider state.
- [x C] Corrected all established-session receive type guards to admit registered v3.1 frame IDs through `FULL_REPLACE_ABORT`. The earlier 640af0d change had widened only `ReceiveExpectedExclusiveAsync`; `ReceivePacketAsync` and the single-reader `ReceivePumpAsync` still rejected frame IDs above 0x13. This was a real dispatcher defect and is now fixed.
- [~ T] No focused codec/transport tests or exact-head build have yet validated this package. The previous exact run on 640af0d built successfully but failed during the test step; do not infer that this package is green.
- [x C] Connected received BEGIN/CHUNK/COMMIT/ABORT to roster/authority validation, bounded durable provider staging, digest verification, restart progress, durable abort/recovery and installed-ACK publication. The receiver rejects wrong generation/namespace, non-contiguous chunk index, non-canonical page size, wrong frame sequence, stale roster and committed-stage mutation before provider switch.
- [~ T] Add adversarial tests for duplicate/interleaved frames, gaps/overlaps, concurrent ABORT/CHUNK, process restart between every frame, digest mismatch and stale generations. Existing exact-head tests are still red on fixture migrations.
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
- [x C] Provider snapshot memory path is now bounded: `StorageDatabase.OpenSnapshot` copies the sorted key index but shares immutable stored value arrays; `StorageSnapshot` prefix cursors seek by lower-bound; `StorageDataProvider.ProviderSnapshotState` merges field/tombstone cursors using bounded 1,024-row pages and decodes one mutation at a time. Snapshot payload memory is no longer duplicated wholesale; the remaining index is O(number of keys). [~ T/O] Measure peak memory/allocations and page throughput on large snapshots; do not claim a benchmark win before measurement.
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
- [x C] Global causal-horizon production code is present: per-author issued high-watermarks, every Active member's applied-through vector, signed retention checkpoint minimum, FULL_REPLACE-bound HLC floor, mandatory Node HLC advancement after install, and final retired-origin closure. Reports/checkpoints are produced and exchanged through `StorageSyncCoordinator` under `AcquireApplicationCommitGuard`.
- [x C] Incoming operation admission checks the signed retention checkpoint replay fence; `StorageMetadataStore` uses `IsOriginCoveredByRetentionCheckpoint` to reject forgotten-origin replay. Namespace GC proof requires every Active member's report and the durable closure of retired origins.
- [~ T] The new protocol invariants have not passed their full test suite: five OFR1/RCP1 tests currently construct stale reports without required issued-origin vectors. Migrate fixtures and add adversarial restart/rejoin/replay tests before treating this protocol as validated.


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
- [x C] `StorageDatabase.OpenSnapshot` no longer clones every value array: it shares immutable values and caches one sorted key/reference index per commit epoch. Repeated snapshots in an unchanged epoch reuse that index in O(1). [~ C] The first snapshot after a write still builds an O(number-of-keys) index; a persistent immutable root/MVCC tree would be required for O(page) total snapshot memory.
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

- [x C] Added durable member frontier production. OFR2 applied entries derive only from immutable ChangeLedger identities already committed by the Metadata Store; unpublished provider feed drafts are reported separately as a count/max-HLC barrier and never treated as applied origins. Reports are signed by the Active member and persisted through the accepted roster guard. A previous report is retained as the baseline after safe compaction, and each author/epoch advances only through a contiguous sequence prefix.
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


### 2026-11-08 — C-pass: signed HLC floor chain from receiver to retention checkpoint

- [x C] Upgraded the installed ACK to FRA2. Each Active member signs the maximum HLC in the committed provider generation after its mandatory Node HLC advance, provider rehydration, PendingApply replay and Node persistence.
- [x C] Upgraded the retention checkpoint to RCP2. The authority requires every Active member's durable ACK for the exact FULL_REPLACE generation/digest, rejects missing members or divergent floors, and binds the shared `(HlcWall,HlcLogical)` floor into the signed checkpoint payload.
- [x C] Added `StorageSyncCoordinator.CreateFullReplaceRefreshCallback(persistNode)` as the intended receiver callback factory. RCP1/FRA1 fallback decoding is not retained.
- [~ T] Add golden vectors and restart/tamper tests for FRA2/RCP2, floor mismatch, missing ACK, callback failure before ACK, and monotonic checkpoint chain. Exact-head CI pending.


### 2026-11-08 — C-pass: author-issued horizon, terminal retirement and commit barrier

- [x C] OFR2 reports the durable per-author-epoch issued sequence and maximum issued HLC, plus pending local draft count/max HLC. Frontiers continue to count only frozen ChangeLedger operations; drafts are a separate checkpoint barrier.
- [x C] Added terminal final-author reports. The coordinator holds its application commit guard, closes all durable local author epochs plus the supplied current epoch in the provider, rejects remaining feed drafts, and signs a terminal report. The store rejects later reports after final closure.
- [x C] NRS2 carries immutable retired-origin epoch closures. An Active member cannot be retired without a final signed OFR2 report; the authority must copy each epoch's final sequence and maximum HLC exactly. Existing closures survive member rejoin.
- [x C] RCP2 safe frontiers now advance only through each author's final issued high-watermark and only when every Active member reports application through that full range. A FULL_REPLACE-bound checkpoint requires zero unpublished drafts on every Active member and an HLC floor covering the maximum issued HLC across all epochs.
- [x C] Added `IProviderCommitGuard` and `StorageSyncCoordinator.AcquireApplicationCommitGuard` (coordinator→provider lock order). FULL_REPLACE commit refuses to switch while local feed drafts remain; the same lease can be supplied to GC entry points.
- [~ T] Exact-head CI is queued for this code slice. Golden vectors, report terminality, closure/rejoin, issued-HLC max, pending-draft race, all-member frontier and commit-guard fault tests remain in T.
- [~ C] Physical GC remains disabled pending the final cross-store live-reference scan, including provider state and host WAL/FLD references under the composed application commit lease.


### 2026-11-08 — C-pass: enforce terminal author identity and safe rejoin

- [x C] Provider now persists a terminal author-identity marker in addition to per-epoch closure markers. Once a final OFR2 report is published, all local appends are rejected, not merely appends using the last known epoch.
- [x C] Added explicit `StorageSyncCoordinator.ActivateLocalAuthorEpoch(newEpoch)`. It requires the local member to be Active in the accepted roster, clears the terminal marker, installs a fresh epoch, and never permits reuse of a previously closed epoch.
- [x C] The final-report path holds the ordered coordinator/provider commit guard while closing known epochs, checking that the feed is empty, closing the author identity and signing the final report.
- [~ T] Verify crash/restart after provider retirement but before report persistence, report persistence before roster retirement, rejoin with a fresh epoch, and rejection of writes on every old epoch.


### 2026-11-09 — C-pass: repair issued-origin state integration and compile blockers

- [x C] Reconciled the key-kind 22 durable allocator representation: the in-memory map now consistently stores `(OriginSequence, MaxHlcWall, MaxHlcLogical)` rather than mixing the new record with the old `ulong` value type. Freeze-time increments preserve all three fields and serialize the 20-byte value.
- [x C] Frontier projection now reads `OriginIssuedState.Sequence` and carries the persisted maximum HLC fields into OFR2.
- [x C] Fixed nullable roster handling in the explicit author-epoch reactivation path.
- [~ T] Exact-head CI must confirm compilation and then expose remaining behavioral/test failures. The last completed CI at `d8db149` failed at compile time on these type mismatches and nullable warnings; the repair commits are `ef75ea3` and `bcece5c`.
- [x C] Audited key-kind 22 call sites, durable 20-byte encoding/decoding, in-memory state, frontier projection and freeze-time retry path. Persistence/restart/fault proof remains in T-pass.


### 2026-11-09 — C-pass: widen application commit barrier to Node mutation gates

- [x C] `StorageSyncCoordinator.AcquireApplicationCommitGuard()` now acquires coordinator → Node `Storage→Outbox→Meta` gates → provider/shared-Storage transaction gate, and releases in reverse order. This closes the direct-Node-write race during FULL_REPLACE, author retirement and local GC when the provider/metadata share the Storage transaction gate.
- [x C] The receiver continues to require an explicit non-null commit lease before generation switch; the caller must still compose the host WAL/FLD transaction gate if it is distinct from the BIN-SYNC Storage database.
- [x C] Updated the Storage integration call sites for the new tombstone-HLC proof contract (compile/API alignment only; no new behavioral assertions were added in this C slice).
- [~ T] CI at `747ea0c` compiled production code and Fuzz/Lab passed, but Core tests had 7 failures in OFR1/RCP1 expectations and record-tombstone mutation validation. Storage Integration failed to compile on the changed tombstone-HLC signature; call sites are now aligned. Re-run exact-head CI and then migrate the obsolete v1 test fixtures to OFR2/RCP2 during T-pass.
- [x C] Concrete lifecycle/control-plane wiring now exists for accepted roster exchange, OFR2 publication/receipt, RCP2 publication/receipt, FRA2 ACK receipt/send and receiver HLC-refresh callbacks. Host peer scheduling/retry remains host-owned; all durable Metadata Store/Data Provider/GC adapters are required to share one StorageDatabase for the internal barrier. Physical GC stays disabled.


### 2026-11-09 — C-pass: shared-storage proof and lock-order correction

- [x C] `AcquireApplicationCommitGuard` now fails closed if the durable `StorageMetadataStore` and `StorageDataProvider` do not share the same `StorageDatabase`; the provider/metadata/GC transaction gate is therefore one concrete lock when these durable implementations are attached.
- [x C] Added internal shared-database identity checks without exposing storage handles through the public provider API.
- [x C] Audited `StorageNodePersistence.Save` against the new lock order and changed it to acquire Node `Storage→Outbox→Meta` gates before the shared Storage transaction gate. This prevents the inverse Storage→Node order from deadlocking against FULL_REPLACE/GC commit guards.
- [~ T] Verify parallel Node writes, Node checkpoint saves, provider appends, metadata commits and FULL_REPLACE commit under contention; add deterministic deadlock/race tests during T-pass.
- [x C] Snapshot values are shared immutably and the sorted index is cached per commit epoch; provider scans use borrowed single-entry/page cursors. [~ C] First snapshot after a write still materializes an O(number-of-keys) index. Physical GC remains disabled until host WAL/FLD and PendingApply/relay/reservation/snapshot references are proven under the composed lease.


### 2026-11-09 — C-pass: repair OFR2 decoder and align canonical documents

- [x C] Fixed OFR2 decoding: the parser now consumes and retains `FinalForAuthor`, `UnpublishedDraftCount`, `MaxUnpublishedDraftHlcWall` and `MaxUnpublishedDraftHlcLogical` before reading the applied-origin vector. Previously the encoder emitted these signed fields but the decoder skipped them, shifting the vector length and causing `OFR2 contains trailing payload bytes` on valid reports.
- [x C] Added canonical validation: final-author reports cannot contain unpublished drafts; an empty draft set must carry a zero maximum HLC.
- [x C] Updated v3.1 addendum wire layouts and domain strings from OFR1/NRS1 to OFR2/NRS2, including issued-origin entries and FRA2/RCP2 HLC-floor fields. Corrected the stale statement that author-issued/retired-origin closure remained open.
- [~ T] The seven Core failures at `747ea0c` included OFR1/RCP1 fixtures, OFR2 decode misalignment, and a record-tombstone fixture rejected by the stricter canonical mutation contract. The OFR2 parser defect is fixed in C; the legacy test fixtures/assertions remain for T-pass migration.
- [x C] Remaining lifecycle wiring was completed in the subsequent coordinator control-plane entries below. [~ C] Storage snapshot-memory audit remains open; retain all physical GC feature flags as false.


### 2026-11-09 — C-pass: durable v3.1 control-plane orchestration

- [x C] Added `StorageSyncCoordinator` orchestration over negotiated `SyncSession` frames: send/receive accepted namespace rosters, publish-and-send OFR2, receive-and-persist member frontiers, publish-and-send RCP2, receive-and-persist checkpoints, and receive-and-persist FULL_REPLACE installed ACKs.
- [x C] Incoming control messages are decoded/verified by the session against the accepted roster, then re-encoded with their original signature and passed through the durable `StorageNamespaceGcStore.Accept*` path. Roster changes during the await fail closed at the store's current-roster check.
- [x C] Outgoing OFR2/RCP2 objects are durably accepted before network send; a lost send can be retried with a newer monotonic report/checkpoint rather than losing the local proof state.
- [~ T] Validate authority/member role separation, capability negotiation, duplicate frames, stale roster during receive, failed send/retry and report/checkpoint monotonicity in the T-pass.


### 2026-11-09 — C-pass: installed-ACK send fence

- [x C] Added `SendDurableFullReplaceInstalledAckAsync`: it requires the ACK to belong to the local node/namespace, finds the exact ACK already durably accepted for that generation, compares transport epoch, roster generation, digest and HLC floor, verifies the member seed against the accepted roster, and only then sends it.
- [~ T] Exercise ACK lost-send/retry, wrong local member, stale roster, mismatched digest/floor and missing durable ACK cases.


### 2026-11-09 — C-pass: immutable retired-origin closure on rejoin

- [x C] OFR2 verification now compares every previously authority-closed epoch for the reporting member against the immutable NRS2 closure: epoch, final issued sequence and maximum HLC must all match, and a rejoined member cannot omit the old epoch from its report.
- [x C] Applied the check in both decode/verify and object verification paths, so both wire receive and local checkpoint construction enforce the same invariant.
- [~ T] Add tamper tests for omitted/changed old epochs after rejoin, unchanged closed epochs alongside a new epoch, and old-epoch attempts to advance issued sequence/HLC.


### 2026-11-09 — C-pass: provider retirement requires closing the active epoch

- [x C] `CloseLocalAuthorIdentity` now validates the persisted active-epoch marker and refuses terminal retirement unless that exact epoch already has a durable closure marker. It also rejects an impossible active marker on an already-retired identity.
- [x C] `ActivateLocalAuthorEpoch` refuses to proceed if the retired identity still carries an active-epoch marker, then enforces a monotonically fresh epoch above all prior closures.
- [~ T] Test wrong `finalAuthorEpoch`, missing closure marker, malformed active marker, crash/retry and valid rejoin.


### 2026-11-09 — C-pass: bound OFR2/RCP2 validation cost

- [x C] Replaced repeated linear lookups in OFR2 own-applied/issued epoch validation with one per-report epoch map; retired-epoch closure checks use an indexed issued-epoch map.
- [x C] RCP2 safe-frontier derivation now builds each member's `(author, epoch) → applied sequence` map once and checks every origin/member pair in O(1) average time instead of rescanning each report vector.
- [x C] This removes the avoidable O(origins × active-members × entries) scan from checkpoint creation while retaining bounded 4096-entry report limits and identical fail-closed conditions.
- [~ T] Add equivalence/property tests and benchmark 1/16/256/4096 entries × 1/2/64/4096 members; confirm allocations and CPU before accepting the optimization.


### 2026-11-09 — C-pass: make every frontier report a consistent application snapshot

- [x C] `PublishAppliedOriginFrontier` now always acquires the coordinator→Node→shared-Storage application commit barrier, not only for final-author reports. The ChangeLedger frontier and provider unpublished-draft count/max-HLC are therefore sampled against one local commit boundary.
- [x C] This closes the race where a local Node/provider write could arrive after a zero-draft scan but before a non-final OFR2 report was signed and later used by a FULL_REPLACE retention checkpoint.
- [~ T] Stress concurrent local writes, remote apply, publication freeze, provider ACK and frontier publication; prove the report is either before or after each operation, never a mixed snapshot.


### 2026-11-09 — C-pass: guard provider rehydration and publication cutover

- [x C] `RefreshAfterFullReplace`, `RehydrateNodeFromProviderSnapshot` and `EnableMetadataStoreOwnedPublication` now acquire the same coordinator→Node→shared-Storage barrier before reading provider snapshots, changing Node cache state or switching publication authority.
- [x C] Nested use from the receiver's already-held commit lease is reentrant on the acquiring thread; Node checkpoint persistence follows the same lock order.
- [~ T] Verify direct and nested invocation, concurrent Node writes/provider appends, failed persistence and retry, and deadlock freedom under contention.


### 2026-11-09 — C-pass: enforce shared storage at FULL_REPLACE receiver construction

- [x C] The durable `StorageFullReplaceReceiver` now rejects a concrete `StorageDataProvider` + `StorageNamespaceGcStore` pair that do not share the same `StorageDatabase`. This aligns receiver generation switching and installed-ACK persistence with the coordinator/metadata transaction gate.
- [~ T] Verify rejection for split databases and successful commit/ACK persistence when all three durable adapters share one database.


### 2026-11-09 — C-pass: reduce snapshot payload duplication

- [x C] `StorageDatabase.OpenSnapshot` now clones the sorted key index but shares the immutable stored value arrays. `Commit` copies caller-owned buffers on publication and later commits replace references rather than mutating arrays; snapshot reads still return owned copies through `TryGet`/bounded cursor pages.
- [x C] Repeated snapshots now share a cached immutable sorted index for the current commit epoch; successful commits invalidate the cache while old snapshots retain their previous index. [~ C] The first snapshot after each commit epoch still allocates O(number-of-keys) index storage; persistent immutable root/MVCC remains a future option, not a current O(page) claim.
- [~ T] Verify snapshot stability across replace/delete/reopen, concurrent commit and cursor paging; benchmark allocations/RSS with large values and 1M rows.


### 2026-11-09 — C-pass: prefix cursor seeks into sorted range

- [x C] `StorageSnapshot.PrefixCursor` now computes the exclusive lexicographic upper bound of a byte prefix and uses `SortedDictionary.GetViewBetween` to seek directly to the matching key range. With `afterKey`, it starts at that cursor position instead of scanning unrelated keys from the partition beginning.
- [x C] Empty/all-0xFF prefixes retain the safe fallback; the existing page/trailing-key checks remain in place.
- [~ T] Verify prefix boundaries (`00`, `FF`, carry across suffix bytes), exact `afterKey`, empty pages, disposal and concurrent commits; benchmark scan cost for 1M rows with sparse prefixes.

- [x C] Follow-up hardening: an `afterKey` at/above the prefix upper bound now retains a valid completed enumerator, so `ReadPage` returns an empty page rather than treating the cursor as disposed.

- [x C] Roster send helper now signs and verifies against the pinned authority trust entry before transmitting, rejecting a mismatched authority seed locally rather than sending a roster peers will reject.


### 2026-11-09 — C-pass: enforce monotonic HLC within an author epoch

- [x C] `StorageMetadataStore.FreezeOutbound` now rejects a new immutable operation whose `(HlcWall,HlcLogical)` is lower than the persisted max for that author epoch. The allocator no longer silently retains a higher max while accepting an older operation that could later resurrect data after tombstone reclamation.
- [x C] Equal HLC is allowed for same-transaction operations; the per-epoch max advances only when the new HLC is greater.
- [~ T] Test HLC wall regression, logical regression at equal wall, equal-HLC multi-field batch, restart persistence and concurrent freeze ordering.

- [x C] Extended the freeze-time HLC monotonicity fence across all epochs of the same namespace/author, not just within one epoch. A rejoined author cannot publish a lower HLC under its fresh epoch than the maximum issued HLC from a previously closed epoch.


### 2026-11-09 — C-pass: tombstone proof binds row HLC to issued epoch maximum

- [x C] `HasProtocolTombstoneGcProof` now rejects a candidate tombstone whose `(HlcWall,HlcLogical)` exceeds the signed maximum HLC for its exact author epoch, in addition to requiring the tombstone HLC and the entire issued-epoch maximum to be covered by the common FULL_REPLACE floor.
- [~ T] Add tamper tests for tombstone HLC above issued max, equal-wall logical overflow, below max, and retired versus active author closure.


### 2026-11-09 — C-pass: replace unsupported range view with binary-seek snapshot index

- [x C] Replaced the unsupported `SortedDictionary.GetViewBetween` attempt with a `SortedList` snapshot index and a bounded lower-bound binary search over its sorted keys. Prefix and `afterKey` cursors now seek in O(log n) and read only matching pages; the cursor does not allocate a view or enumerate unrelated rows.
- [x C] Snapshot payload arrays remain shared immutable references; each `StorageEntry` page returns owned key/value arrays. Empty/all-0xFF prefixes and afterKey beyond the range naturally return empty pages.
- [~ T] Exact-head build and prefix-boundary/stability tests required; the prior CI failure was a compile-time API mismatch, now removed.


### 2026-11-09 — C-pass: make host live-reference scans mandatory for physical GC

- [x C] Added `IApplicationGcReferenceScanner`, a host-owned contract for external ChangeId and record references in WAL/FLD outbox, pending application state and other durable stores. GC calls fail closed if the scanner is absent.
- [x C] `CompactChangeLedger` now checks both BIN-SYNC-owned live references and the host scanner before deleting a ChangeId ledger row.
- [x C] `StorageMetadataStore.HasLiveRecordReference` checks pending apply payloads, relay payloads, outbound payloads and conservatively blocks on any pending immutable peer reservation in the namespace. Malformed retained payloads count as live.
- [x C] `CompactRecordTombstones` now requires the durable Metadata Store and external scanner, verifies all three durable adapters share one StorageDatabase, and checks both internal and host record/ChangeId references under the supplied composed lease.
- [x C] The FLD reference scanner and host lease composition are implemented by the optional sample and `AcquireReferenceSnapshotGuard`. All physical GC feature flags remain false until T-pass proof exists.

- [x C] Threaded the required host scanner through the public `StorageSyncCoordinator` GC entry points; there is no coordinator wrapper that can silently invoke ChangeId or record-tombstone compaction without the scanner.

- [x C] Extended the mandatory host scanner to receipts as well: receipt GC now checks external `(namespace, sender, author epoch, transport epoch, batch id)` references in addition to durable PendingApply, retired transport fence, receive cursor/digest chain and signed origin frontier. All three physical GC entry points require the scanner.

- [x C] External scanner contract now receives `(scopeId, localReplica, namespaceId)` on every callback, so one host adapter can safely serve multiple BIN-SYNC partitions without cross-scope ambiguity.


### 2026-11-09 — C-pass: close the host WAL/FLD draft-horizon gap

- [x C] Added \`ApplicationDraftHorizon\` and extended \`IApplicationGcReferenceScanner\` with \`ReadUnpublishedLocalDraftHorizon(scopeId, localReplica, namespaceId)\`. The host must report all local operations not yet frozen into BIN-SYNC, including WAL commits beyond the Outbox materialization watermark and unacknowledged Outbox entries.
- [x C] OFR2 production now requires both the host scanner and host commit-lease factory. The signed draft barrier combines host WAL/FLD drafts with provider-feed drafts and carries the maximum HLC; source-local CommitSeq/JournalSeq/OutboxSeq are never promoted to OriginSequence.
- [x C] Final author retirement checks the host draft horizon before closing provider epochs or terminally retiring the author identity. A non-zero host draft count fails before any provider closure marker is written.
- [x C] Added \`AcquireApplicationCommitGuard(hostLeaseFactory)\` with lock order coordinator → all Node gates → host WAL/FLD gate → shared BIN-SYNC Storage transaction gate; release is reverse-order. This overload is required for FULL_REPLACE and physical GC when the host has separate durable state.
- [x C] Updated the FLD integration contract and v3.1 addendum with exact draft accounting, partition scoping, live-reference checks and lock composition.
- [x C] The optional host adapter now scans the immutable FLD reference index covering WAL/Outbox/ApplyJournal-derived references and supplies the FLD lease to the BIN-SYNC scan contract. FLD remains protocol-neutral; the adapter lives outside both runtimes.
- [~ T] Previous exact CI at \`870f56b\` was red. Core failures include OFR1/RCP1 fixtures that do not provide OFR2 issued-origin vectors and a record-tombstone mutation fixture rejected by the canonical codec. Storage Integration has broad legacy fixture/API mismatches (local drafts still supply an assigned OriginSequence instead of deferred zero; immutable freeze fixtures contain non-contiguous OriginSequence; additional provenance fixtures need migration). A new run on \`236bfb1\` is in progress. Do not fix by weakening production validation; migrate tests in the dedicated T pass.
- [~ C] Snapshot sorted-index memory remains O(number of keys); physical receipt/ChangeId/tombstone GC flags remain false until the host adapter is built and the cross-store reference horizons are proven in T.


### 2026-11-09 — C-pass: FLD engine host-side scan primitives landed

- [x C] FLD-DB now exposes a same-thread \`FldDatabase.AcquireCommitGuard()\` and a neutral \`FldDraftHorizon\` scan over dirty local edits plus the unacknowledged Outbox suffix.
- [x C] FLD's receipt scanner matches durable ApplyJournal identities across sender peer, transport epoch, issuer author epoch, namespace and batch id. ChangeId/RecordId scanners inspect dirty state, pending Outbox and retained ApplyJournal field/relay payloads.
- [x C] Any unsealed WAL prefix blocks external ChangeId/RecordId/receipt reclamation conservatively until checkpoint/seal; a faulted database throws and cannot authorize GC.
- [x C] Added \`OutboxLog.VisitUnackedBatch\` borrowed-span iteration. FLD horizon/reference scans now inspect IDs and HLC without copying potentially large field-value payloads, using bounded 512-entry pages; ApplyJournal scans use 256-record pages.
- [x C] The optional `samples/FldBinSyncHost` adapter implements `IApplicationGcReferenceScanner`, opens one `FldReferenceSnapshot` under `AcquireReferenceSnapshotGuard` and returns a scan lease that releases snapshot before the FLD guard. The host composes this with `AcquireApplicationCommitGuard(hostLeaseFactory)`; FLD remains protocol-neutral.
- [~ T] FLD exact-head CI is running for code head \`488150e\`; BIN-SYNC Core remains red on the previous exact code head due OFR1/RCP1 fixtures and one obsolete record-tombstone fixture. Fuzz/Lab passed. Do not weaken the production OFR2/ProviderMutation invariants to make legacy fixtures pass.
- [~ O] Zero-copy Outbox scans are expected to reduce temporary allocations, but benchmark acceptance is pending.


### 2026-11-09 — C-pass: make final author retirement epoch-exact

- [x C] \`IProviderAuthorEpochClosureStore.CloseLocalAuthorIdentity\` now requires the declared \`finalAuthorEpoch\`.
- [x C] StorageDataProvider verifies that the supplied epoch exactly matches its persisted active-epoch marker and has a durable per-epoch closure before terminal retirement.
- [x C] The provider persists the exact final retired epoch in a dedicated marker. A retry after crash must name that same epoch; an older closed epoch cannot be substituted to complete retirement.
- [~ T] Add wrong-final-epoch, crash-after-retirement-before-report-persist, retry, and rejoin tests. Exact-head CI for \`cd849db\` is queued.


### 2026-11-09 — C-pass: indexed host scan and tombstone proof snapshots

- [x C] Replaced per-candidate host scanner calls with \`IApplicationGcReferenceScanner.OpenScan(...)\` and one stable \`IApplicationGcReferenceScan\` per frontier/GC operation. This lets the host build indexes once under the composed WAL/FLD lease.
- [x C] \`StorageMetadataStore.CompactChangeLedger\` builds the live ChangeId set once from relay, outbound and PendingApply rows; any pending immutable peer reservation blocks the namespace once, rather than being rechecked for each candidate.
- [x C] Receipt compaction builds the pending-receipt identity set once and indexes roster members once.
- [x C] Record-tombstone compaction builds one fail-closed live RecordId set from pending apply/relay/outbound payloads; malformed retained payloads or pending reservations block the entire pass.
- [x C] Added \`TombstoneGcProofSnapshot\`: signed roster/checkpoint, all-active installed HLC floor, issued/retired author epochs, safe frontiers and every Active member's applied-through vector are loaded/indexed once per tombstone pass. Each tombstone proof check is O(1) average time.
- [x C] \`StorageDataProvider.CompactRecordTombstones\` now reuses the same signed proof snapshot and metadata/host reference snapshots across candidates; no repeated per-tombstone full frontier or payload scans.
- [~ T] Exact-head BIN-SYNC CI is queued for \`02dbe8e\`. The previous exact run at \`ed5d449\` had 6 Core failures (legacy OFR1/RCP1 fixtures and one old record-tombstone fixture) and Storage Integration failures dominated by old provider-draft/OriginSequence fixtures and old non-contiguous freeze inputs. Fuzz/Lab passed. Keep production validation strict; test migration is the T pass.
- [x T] Cross-repository FLD engine host primitives passed exact-head CI on Ubuntu/Windows/macOS at \`0db64f8d8f78f714fb197e0f375a536dcdec47d6\`.
- [~ O] Benchmark host scan snapshot allocations, tombstone proof snapshot build cost and per-candidate query cost before enabling any physical GC.


### 2026-11-09 — C-pass: FULL_REPLACE now observes host WAL/FLD state

- [x C] \`StorageFullReplaceReceiver\` now requires the exact scope and host reference scanner; under the composed coordinator→Node→host WAL/FLD→Storage lease it opens one host scan before generation switch.
- [x C] FULL_REPLACE refuses to commit if the host draft horizon is non-zero, including the conservative unsealed-WAL barrier, or if provider-local feed entries remain. This prevents a full replacement from overwriting unmaterialized local FLD/WAL edits.
- [x C] Integration contract and v3.1 addendum now require the same composed lease and host scan for FULL_REPLACE, OFR2, and all physical GC passes.
- [~ T] Exact-head CI for \`9e730d7\` is in progress; update receiver constructor fixtures during the dedicated T pass.


### 2026-11-09 — C-pass: final host and proof snapshot hardening

- [x C] \`StorageFullReplaceReceiver\` validates the host draft horizon shape (\`Count == 0\` requires zero HLC maximum) and refuses to switch when host drafts or provider feed entries remain.
- [x C] Tombstone proof snapshot now validates checkpoint digest presence/size before comparing; redundant installed-ACK rescans were removed because the common installed-HLC-floor scan already verifies every Active member's installed ACK, epoch, generation and digest.
- [x C] \`StorageMetadataStore\` now uses indexed ChangeId/receipt/RecordId sets and the host scan object; per-candidate protocol proof/frontier scans were removed from tombstone GC.
- [~ T] Exact-head CI is in progress for \`78a7080\`. The last completed BIN-SYNC test runs remain red only on known stale T fixtures: OFR1/RCP1 reports without issued-origin vectors, record-tombstone fixture violating canonical lifecycle metadata, local provider drafts still supplying assigned OriginSequence, and old freeze/remote batch fixtures violating immutable contiguous provenance. No production validation was relaxed.
- [x T] FLD engine reference-snapshot primitives passed exact-head CI on Ubuntu/Windows/macOS at \`0db64f8d8f78f714fb197e0f375a536dcdec47d6\`; later unsealed-WAL barrier changes are on \`e6bec094807f660e07b70874254a5925bc8b6deb\` and have CI pending.


### 2026-11-09 — C-pass: bounded provider snapshot path verified

- [x C] Confirmed the source snapshot implementation now retains a point-in-time Storage snapshot, uses seekable prefix cursors, and merges field/record-tombstone rows through bounded 1,024-entry pages. Immutable value arrays are shared by the Storage snapshot; only the sorted key index is copied. This closes the earlier stale roadmap item claiming a second full `StorageEntry[]` materialization.
- [~ T/O] Large-snapshot peak-memory, allocation and throughput measurements remain open; no performance improvement is claimed without benchmark evidence.

### 2026-11-09 — C-pass continuation: conformance adapter contract

- [x C] `VolatileMetadataStoreAdapter` now implements the combined frozen ChangeId identity lookup. It reads fingerprint and immutable Origin from one ledger entry, verifies local-outbound ownership and rejects incomplete identity; no production behavior or validation was weakened.
- [~ T] The adapter compile blocker reported at `1acdfa65979a` has a code fix committed; exact-head CI has not yet validated it. The 80 Storage Integration failures remain T fixtures/adapters (deferred OriginSequence drafts and `IProviderCommitGuard`).

### 2026-11-09 — C-pass continuation: crash-safe origin stamping and provider-feed GC references

- [x C] Provider publication no longer retains outbound payload objects solely to recover immutable origins. The coordinator resolves the frozen ChangeId identity (fingerprint + OperationOrigin) from one Metadata Store ledger lookup and stamps only the matching current provider winner before ACK.
- [x C] Recovery now handles the freeze-before-provider-stamp crash cut: after restart, a pending provider draft whose ChangeId is already frozen is verified against the durable fingerprint/origin ledger and stamped idempotently; conflicting fingerprints fail closed.
- [x C] Added a combined frozen ChangeId identity lookup and consolidated metadata reads to one ledger lookup per candidate; removed redundant standalone origin/fingerprint lookups.
- [x C] ChangeId GC live-reference scan now includes the local provider feed, so a ChangeId cannot be reclaimed while a local draft still references it. FLD integration contract and v3.1 addendum document the freeze-before-stamp recovery boundary and provider-feed GC fence.
- [x C] FULL_REPLACE now checks the host WAL/FLD draft horizon under the composed application commit barrier before generation switch. Host draft count/HLC shape is validated; provider local feed must also be empty.
- [x C] Host/GC proof work now builds indexed immutable scan snapshots, avoiding repeated full payload/roster/frontier scans per tombstone or ChangeId candidate. The physical GC feature gates remain disabled.
- [~ T] Exact-head `1acdfa65979a4338e9206c5617c3696c3fee4e75` is red. Core does not compile: `VolatileMetadataStoreAdapter` is missing `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeIdentity(...)`. Storage Integration has 80 failures, predominantly fixtures that still append provider drafts with assigned `OriginSequence` rather than zero/deferred origin, plus a few legacy providers that do not implement `IProviderCommitGuard`. Durable Metadata Bench passed; Metadata Retry Comparison failed. These are test adapter/fixture migrations; do not weaken production validation.
- [x C] Concrete BIN-SYNC↔FLD host adapter now supplies the FLD scan snapshot and commit lease through the optional sample. FLD provides neutral draft/reference primitives and remains protocol-neutral.
- [~ O] Measure the combined ledger lookup, indexed host scans and proof-snapshot build cost before accepting optimizations; do not infer improvement from code shape alone.

### 2026-11-09 — C-pass refresh: exact HEAD and concrete red test inventory

- [x C] Current branch HEAD is `29eea904621768175d1021f2de487d144c7d2d50`. Latest completed workflows on this exact HEAD: BinSync.Net and FLD Storage Integration both failed; Fuzz/Lab job passed.
- [~ T] Core test run: 678 total, 672 passed, 6 failed. Five OFR1/RCP1 tests use legacy member reports without the issued-watermark vector required for locally authored applied epochs; one provider mutation codec test builds a record tombstone without canonical lifecycle field/key-epoch-zero/no-value metadata. These are fixture migrations; keep production validation strict.
- [~ T] Storage Integration: 171 total, 91 passed, 80 failed. Failures are concentrated in old provider-draft fixtures supplying assigned OriginSequence instead of deferred zero, and test providers missing `IProviderCommitGuard`. Update fixtures/adapters to current provider contract; do not relax production guards.
- [x C] Fuzz/Lab passed on this exact head. This does not substitute for Core or Storage Integration success.
- [x C] Host integration now binds the coordinator resolver to the FLD relay API, publication gate, ACK and reference scanner. The two-runtime-assembly source extraction is complete. [~ T] Exact compile/test verification remains pending.
- [~ T] Exact-head CI is red; broad test migration remains deferred until C-pass closes. The known failures are now enumerated rather than represented as a generic red status.

### 2026-11-09 — C-pass refresh: FLD two-DLL scaffold

- [x C] FLD-DB now has a consolidated `Fld1.Database.csproj` linking Core/Run/WAL/Manifest/Engine source and referencing the independent generic `Fld1.Storage` project. The solution now exposes only these two runtime source projects; Core/Engine/etc. folders remain code-organization boundaries.
- [~ T] FLD exact-head CI on `a0ea90b9490cc1d93eafd048008327a5bcb4a54f` failed on all three OS jobs, but diagnostic log retrieval returned unavailable blobs. The consolidation has not been accepted as built until actionable errors are inspected and Ubuntu/macOS/Windows pass.
- [x C] The cross-repository host adapter now reads complete JournalSeq pages under the composed FLD commit guard, invokes the BIN-SYNC resolver, sends only immutable OutboxSeq batches under captured ceilings, translates safe ACK to FLD RelayJournalBatch ACK, prunes RPM3 rows afterward, and binds one FLD reference snapshot under the composed guard.
- [~ T] BIN-SYNC exact-head `29eea904621768175d1021f2de487d144c7d2d50` remains red: 6/678 Core tests and 80/171 Storage Integration tests fail for known stale fixture/provider-contract reasons; Fuzz/Lab passes. Do not relax production checks.

### 2026-11-09 — C-pass: reject replayed JournalSeq at resolver boundary

- [x C] `AppendRelayProvenancePage` now rejects a first source JournalSeq equal to or below the already committed scan cursor before allocating resolver arrays or touching the Node ledger. Subsequent entries may share a JournalSeq (one source batch with multiple operations) but may not decrease.
- [x C] This makes the coordinator's early validation match `StorageRelayProvenanceMap.AppendPage`'s strict page-boundary rule; the durable map remains the final atomic invariant check.
- [~ T] Add direct tests for first-entry-equals-cursor, decreasing JournalSeq, same-JournalSeq multi-operation page, and assert rejected pages do not mutate Node Outbox or map cursor.

### 2026-11-09 — C-pass: FLD source now physically consolidated

- [x C] FLD-DB commit `d63a8c49a889c28838305ce15aa4ca5e0b189201` physically moved all 40 Core/Engine/Manifest/Run/WAL source files under `src/Fld1.Database`; removed the five component project files. This replaces the earlier linked-source scaffold and obeys the no-shortcut/two-runtime-DLL decision.
- [x C] The supported solution surface is now `Fld1.Database` + `Fld1.Storage`; tests and benchmarks target Database, Storage-only tests/consumers target Storage.
- [~ T] The physical consolidation has not yet passed exact-head three-OS CI. Continue to treat the FLD project graph as unverified until build/test succeeds.
- [x C] The optional host adapter completes the source composition: complete-JournalSeq relay read, resolver/map append, ceiling-limited downstream publication, safe ACK translation, durable FLD ACK, RPM3 prune and reference-snapshot scanner lease. [~ T] Execution remains pending.

### 2026-11-09 — Recovery point after relay cursor hardening

- [x C] Created `backup/cpass-2026-11-09-after-relay-guards` at `7342dc7f8634d64b8911d0ab4ff93f4750127f97` before the next host-adapter/control-plane changes. Earlier `backup/pre-rpm3-compact-provenance-2026-11-09` remains intact.
- [x C] Relay resolver now rejects first JournalSeq at/below the durable scan cursor before array allocation; same-JournalSeq entries remain valid within a page.
- [~ T] Exact-head CI remains red on the previously observed 6 Core + 80 Storage Integration stale-fixture failures. Continue the C-pass without weakening validation; T-pass remains a separate gate.

### 2026-11-09 — C-pass status file synchronized to exact test evidence

- [x C] `docs/PRODUCTION-STATUS.md` now reflects current branch HEAD `57edd1f0032d24b17e51d265c16c3134472f3d66` and latest production-code commit `ea6968106e697f9d81ef94331cbf6dd986f71b29`.
- [~ T] Latest completed code-equivalent run on `7342dc7f8634d64b8911d0ab4ff93f4750127f97`: Core 672/678 (6 stale OFR1/RCP1 + tombstone fixtures), Storage Integration 91/171 (80 provenance/OriginSequence/commit-guard fixture failures), Fuzz/Lab passed. Exact current HEAD differs only by documentation since the tested code snapshot.
- [x C] FullReplace receiver, causal frontier/checkpoint production, retired-origin replay fence and concrete FLD Database host adapter/reference scanner source are implemented. [~ T] Host restart/race proof remains; physical GC stays disabled.

### 2026-11-09 — C-pass: complete FULL_REPLACE wire-vector sample

- [x C] Expanded `samples/V31ControlPlaneVectors` with exact canonical vectors and encode/decode round-trips for FULL_REPLACE_CHUNK, READY, COMMIT and ABORT in addition to signed BEGIN. The sample now covers all four transfer frame payload forms alongside NRS2/OFR2/RCP2/FRA2.
- [~ T] The sample has not yet been run on the current branch; validate the fixed bytes and signature vectors in the dedicated T pass and include it in CI sample execution.

### 2026-11-09 — exact-head CI triage and RPM3 namespace validator repair

- [x C] Current branch HEAD before this fix was `45160c1fbb07d9ac408c67ff49e857ffa5423015`. The latest completed Core and Storage Integration workflows both stopped at build, not tests.
- [x C] Root cause from both runner logs: `StorageRelayProvenanceMap.ReadAuthors` called missing `ValidateNamespaceId` (CS0103 at line 80). Added the private validator and reused it from `ValidateStream`; code fix `66ae3ac50833db9f4226f33490c780176bac0b14`.
- [~ T] Exact-head CI after `66ae3ac` has not yet completed. No test pass/fail counts are available for `45160c1f` because build failed before test execution.
- [~ T] The older 6/678 Core and 80/171 Storage Integration fixture failures remain historical evidence only; do not carry those counts forward as current-head results.
- [x C] The relay/ACK/GC host adapter contract is composed in `samples/FldBinSyncHost`, including all-author discovery, page drain, resolver, shared publication gate, safe ACK→FLD ACK→RPM3 prune, restart reconciliation and delegate reference scan. [~ C] The sample does not yet implement `IDataProvider` over `FldDatabase` or the full remote-apply bridge; transport/session scheduling and full Storage/API/byte audit remain.
- [~ C] FLD's two-project source boundary is physically consolidated as `Fld1.Database` + `Fld1.Storage`; generic Storage backend reuse and final binary-format/package compatibility audit are still open.
- [~ O] Benchmarks for snapshot/index build and end-to-end relay path remain pending. Physical GC remains disabled until integrated host scan plus proof tests establish safe horizons for receipts, ChangeId and tombstones.

### 2026-11-09 — T inventory refresh after RPM3 build repair

- [x C] The missing `ValidateNamespaceId` helper is fixed; exact-head run on the subsequent roadmap commit built successfully and entered tests.
- [~ T] Core: 678 total, 672 passed, 6 failed. Five failures are stale OFR1/RCP1 fixtures that do not supply the v3.1 issued-origin watermark vector for locally authored applied epochs; the sixth tombstone mutation fixture omits canonical lifecycle metadata (field/key epoch zero/no value).
- [~ T] Storage Integration: 171 total, 91 passed, 80 failed on Ubuntu, Windows and macOS. The common first failure is old provider drafts with assigned `OriginSequence` rather than deferred zero; another repeated failure is legacy test providers without `IProviderCommitGuard`. Update fixtures and adapters to the current contract; do not weaken runtime checks.
- [x T] Fuzz/Lab (§17) passed on the same code snapshot. Coverage was 68.1%, above the 50% gate; this does not offset Core/Storage failures.
- [x C] The two-DLL FLD source consolidation, BIN-SYNC relay gate/author recovery/ACK primitives, concrete host composition/sample and restart reconciliation are implemented. [~ C] Final generic Storage backend/format/API and metadata-cardinality audit remain.
- [~ O] Performance acceptance is still pending for bounded FLD scans, author cache rebuild/update, RPM3 mapping and the full relay publish/ACK path.

### 2026-11-09 — progress estimate and next C-pass boundary

Estimated completion (engineering assessment, not CI pass percentage):

- Protocol/core v3 and durable metadata invariants: **~90%**.
- RPM3 relay mapping, publication gate, all-author recovery and ACK primitives: **~85%**.
- FLD two-project split and durable scan primitives: **~80%** (physical project split done; generic backend integration and compatibility audit remain).
- End-to-end host composition: **~80%** (optional adapter source now binds FLD relay paging, startup reconciliation, publication ceilings, safe ACK and GC reference snapshots; concrete transport scheduler and compile/CI validation remain).
- T-pass/conformance/CI: **~45%** (Core 672/678; Storage Integration 91/171; Fuzz/Lab passed; coverage 68.1% on the last completed code-equivalent run).
- Benchmarks and final acceptance: **~25%**.

Overall estimate: **~78–82%**. Remaining C-pass is not a rewrite of BIN-SYNC v3: it is the last host transport/scheduler binding, generic Storage backend/API and format audit, plus a final public-surface review. Then switch to T-pass, followed by O/F. These are engineering estimates, not test-pass percentages. Physical GC remains disabled until safe horizons for receipts, ChangeId and tombstones are proven end-to-end.

### 2026-11-09 — concrete FLD host composition guide

- [x C] Added `docs/FLD-BINSYNC-HOST-COMPOSITION.md` with the concrete public API binding: shared BIN-SYNC Storage partition, FLD commit lease, all-author discovery, whole-JournalSeq page projection, RPM3 source mapping, startup recovery gate, safe ACK ordering and one immutable FLD reference snapshot.
- [x C] Audited the guide against the actual FLD public API (`ReadRelayBatchPage`, `ReadUnacknowledgedRelayAuthors`, `GetRelayJournalHighWatermark`, `AcknowledgeRelayBatches`, `AcquireCommitGuard`, `OpenReferenceSnapshot`) and BIN-SYNC gate/delegate signatures.
- [x C] Added optional `samples/FldBinSyncHost` relay/ACK/GC host adapter. It binds all-author discovery, whole-JournalSeq page projection, startup recovery, publication ceilings, FLD-safe-ACK-before-RPM3-prune and the immutable FLD reference scanner. It wraps but does not implement the actual `FldDatabase` remote APPLY or `IDataProvider` bridge. It is a host/sample assembly, not a third FLD runtime DLL.
- [x C] Both `BinSync.Storage` and `Fld1.Database` now accept the same `Fld1StorageProject` MSBuild property. The optional sample points both project references at the canonical FLD `Fld1.Storage.csproj`, avoiding duplicate Storage assembly/type identities.
- [x C] The adapter now supplies `PublishReadyOutboxAsync`; Core ceiling-aware sender overloads enforce local and relay sequence limits in both Node-owned and Metadata-Store-owned modes. [~ C] Session creation, multi-peer scheduling and routing every application sender through the gate remain host responsibilities; do not call legacy ceiling-free send overloads.
- [~ T] Compile the optional sample with sibling FLD-DB checkout (or explicit project paths), migrate host callback call sites and add crash-cut/reopen tests for startup reconciliation, multi-author page drain, FLD-ACK-before-RPM3-prune and scanner disposal/lock ordering.


## 0.84. C-pass: allocation-free deterministic relay-author ordering — 2026-11-09

- [x C] `StorageRelayPublicationGate.DiscoverAuthors` now preserves the exact ordinal order of `NodeId.ToString()` by comparing the two 64-bit identity limbs in reversed-endian order. This avoids allocating a 16-byte array and 32-character hex string for each comparison in the O(A log A) author sort.
- [x C] FLD `FldDatabase.ReadUnacknowledgedRelayAuthors` now sorts by its already-materialized canonical hex dictionary key instead of recomputing a hex string per author; code commit `bf72f0d8c4e82303612f079e3aefa790d6794451` and FLD roadmap §7.74.
- [~ T] Verify comparator equivalence against the canonical hex order for zero/high-bit/byte-boundary identities and verify FLD discovery order/identity on reopen.
- [~ O] Measure startup/recovery discovery CPU and allocations across representative author counts; these source-level allocation reductions are not yet benchmark claims.

BIN-SYNC code commit: `9d187f6cbe6c496ec013d1d39c969f8820ed1dab`. Current C-pass remains unvalidated; no CI success is claimed.


## 0.85. C-pass: enforce per-author publication ceilings at every sender callback — 2026-11-09

- [x C] `StorageRelayProvenanceMap.ReadPublicationWatermark` exposes the highest per-author sequence backed by durable RPM3 mappings (or the already-ACKed lower bound when no rows remain).
- [x C] `ApplyAndPublishAllAsync`, `RecoverAndPublishAllAsync` and `RunOutboundAsync` capture both the local Metadata Store outbound high watermark and every FLD relay author's RPM3 ceiling under one composed application commit guard. The local author is included explicitly even though it has no FLD JournalSeq map.
- [x C] Added the ceiling-aware `SyncSession.PushBatchAsync(..., IReadOnlyDictionary<NodeId, ulong> publicationCeilings, ...)` overload. It filters eligible authors, trims local/relay pages before batch construction and rejects any immutable pending reservation whose absolute sequence exceeds its author's captured ceiling. The old overload preserves its original behavior.
- [x C] The optional `FldBinSyncHostAdapter.PublishReadyOutboxAsync` converts the complete watermark set and drains a peer session only through the enforcing Core overload. The callback no longer has to reimplement the sequence-cap logic.
- [x C] Updated `docs/FLD-DB-INTEGRATION-CONTRACT.md` to make the cap mandatory and distinguish it from the adapter async gate.
- [~ T] Migrate host callback implementations and test concurrent local commit between final page commit and send, unknown/new author exclusion, zero ceiling, metadata-owned cutover and ACKed-tail behavior. API migration is intentionally deferred until C-pass closure.
- [~ O] Measure the added per-author map lookup/array cost against the safety benefit; do not remove the ceiling without an equivalent enforceable publication fence.

BIN-SYNC commits: `9d33cddbbcf2af5281fe580d6fdd5885f3efe9b1`, `60489796218cdd39a84f8cbc93ce4f53be90dfd3`, `97609fb3c91e15aac27ff8d2eccf576381993c1c`, contract `87a8b7a0d364f80982696f1b748294bf042424a4`. Exact-head CI is not yet run; do not mark T complete.


## 0.86. Recovery checkpoints after publication-ceiling C slice — 2026-11-09

- [x C] Created `backup/cpass-2026-11-09-publication-ceilings` in BIN-SYNC at `701077ed9a13ce476aaa1e679d9c17f16f0ced5f`.
- [x C] Created the matching FLD-DB backup branch at `01617ecaa06fef4bd4ec16e913f24ffd449158ae`.
- [~ T] These branches preserve the current unvalidated code/API state. They are recovery points, not green baselines.


## 0.87. C-pass: validate map ownership before every relay gate entry — 2026-11-09

- [x C] Added `StorageSyncCoordinator.ValidateRelayProvenanceMap` to fail closed unless the RPM3 map matches the coordinator's metadata scope, local replica and exact durable StorageDatabase.
- [x C] Applied this validation to APPLY/publication, startup recovery, single/all-author ACK and `RunOutboundAsync`. The no-author case can no longer bypass ownership checks and emit a misleading empty/foreign ceiling set.
- [x C] Created recovery branch `backup/cpass-2026-11-09-publication-ceilings-v2` at `0f46e444529b77997ce1ba190838de3c4f4ecb6d`.
- [~ T] Add wrong-scope, wrong-replica, different-StorageDatabase and empty-author-list rejection tests; migrate all callback call sites to the new ceiling-bearing API. No tests have been run for this C-pass.


## 0.88. C-pass: allocation-free RPM3 author enumeration — 2026-11-09

- [x C] Added shared `NodeIdCanonicalComparer`, an allocation-free comparer exactly equivalent to ordinal `NodeId.ToString()` order (little-endian byte identity order).
- [x C] `StorageRelayPublicationGate.DiscoverAuthors` and `StorageRelayProvenanceMap.ReadAuthors` now use the comparer; RPM3 discovery no longer creates a hex string per author or a `SortedDictionary<string,NodeId>`.
- [x C] Created `backup/cpass-2026-11-09-nodeid-ordering` at `2c7007f059c2da1878c24e64a76dec902f9e8c73`.
- [~ T] Compare exact ordering with canonical hex strings for random and edge-case IDs; verify duplicate/corrupt cursor handling and author ordering across reopen.
- [~ O] Measure large-author startup scan allocations/CPU; no benchmark claim yet.


## 0.89. C-pass: bounded Storage snapshot/feed scans and crash-resumable ACK compaction — 2026-11-09

- [x C] Reconciled the authoritative FLD `StorageDatabase.cs` with BIN-SYNC's vendored copy; both now share the same blob `fa31c0e6395ad6bc999bf4a3fee8570b6acef20a`. `OpenSnapshot` shares immutable value arrays and materializes only a sorted key/reference index; `PrefixCursor` supports bounded pages and the borrowed `StorageEntryView`/`VisitPage` API.
- [x C] `StorageMetadataStore.TryGetPartition` now decodes persisted rows through 512-row pages instead of materializing the entire partition in one `ScanPrefix(..., int.MaxValue)` result; persisted-entry indexing is built during decode.
- [x C] `StorageRelayProvenanceMap` uses borrowed spans for author and mapping recovery scans, avoiding copied `StorageEntry` key/value arrays.
- [x C] `StorageDataProvider.ReadLocalChanges` uses borrowed prefix views; `AcknowledgeLocalChanges` deletes at most 4,096 rows per atomic transaction while advancing the durable ACK watermark in the same transaction, so reopen resumes at the last committed page. Tombstone-GC session validation is also paged.
- [x C] Created `backup/cpass-2026-11-09-storage-streaming` at `7b77065bd2904943585e254d3368c99fb01a1b85`.
- [~ T] Add cursor-boundary, visitor-stop, snapshot-lifetime, >100k feed ACK, crash-between-ACK-pages, partition-reopen and exact FLD/BIN byte-parity tests. No tests were run in this C-pass.
- [~ O] Measure snapshot open allocations, metadata partition reopen peak memory, provider ACK throughput and map recovery allocations; the sorted key index remains O(number of keys), not O(page).


## 0.90. C-pass: zero-copy single-entry cursor and provider snapshot merge — 2026-11-09

- [x C] `StorageSnapshot.PrefixCursor.TryReadNext(out StorageEntryView)` exposes one borrowed row without allocating a page, `StorageEntry` or key/value arrays. `VisitPage` is now built on this primitive; FLD authoritative source and BIN vendor remain byte-identical (blob `a28cd58fbb72a1453a3ffb04a1ab1d5ed43ddaa1`).
- [x C] `StorageDataProvider.ProviderSnapshotState` now merges field/tombstone snapshot streams by decoding one borrowed row per cursor, removing two 1,024-row copied page buffers and the per-row `StorageEntry` copies.
- [x C] Added span-based generation-key validation for the borrowed row path.
- [x C] Created `backup/cpass-2026-11-09-provider-snapshot-streaming` at `7c8e160352c08756721a278dac25c63e90e8b541`.
- [~ T] Verify merge ordering, Reset/absolute-offset behavior, cursor exhaustion/disposal, record-tombstone ordering and snapshot lifetime while commits/checkpoints advance.
- [~ O] Benchmark snapshot sender allocations and throughput at 1k/100k/1m rows; compare against prior page-buffer implementation before claiming improvement.


## 0.91. C-pass: preserve FLD1 WAL format while reusing Storage writer fence — 2026-11-09

- [x C] Added public `StorageDatabase.AcquirePartitionLease` / `StoragePartitionLease` to `Fld1.Storage`. It reuses the exact cross-process `WalWriterFence` used by `StorageDatabase.Open` without opening a generic Storage WAL or changing any FLD1 on-disk bytes.
- [x C] FLD `FldDatabase.Open` now acquires one root-wide reserved partition lease before manifest load, WAL recovery, Outbox/ApplyJournal open and run reconciliation; `Dispose` releases it only after all FLD file handles are closed. Multiple namespace handles for one physical root cannot race the shared manifest/runs.
- [x C] FLD Storage and BIN-SYNC vendor remain byte-identical at blob `4582c446ef11f308f9442c59438c5aca691614a8`.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-fl-database-lease` in both repositories (BIN-SYNC `88de4289abaaf9b954f5c54be4d34590733f9f55`, FLD-DB `408399e4a465aecfa21c7dde245c297c85595191`).
- [~ T] Verify same-root second process/second namespace rejection, release after failed open, release after disposal from another thread, abandoned-owner recovery and no generic WAL files/format mutation.
- [~ C] This is the first shared ownership seam, not a completed `FldDatabase` migration to `StorageDatabase`. FLD-specific WAL/Run/Manifest encoding remains authoritative and unchanged pending a separate format/recovery ADR.


## 0.92. C-pass: cache immutable Storage snapshot index per commit epoch — 2026-11-09

- [x C] `StorageDatabase.OpenSnapshot` now caches the immutable `SortedList` index for the current commit epoch. Multiple snapshots with no intervening write share the same index and immutable value arrays in O(1), rather than rebuilding O(number-of-keys) arrays on every snapshot.
- [x C] Every successful commit invalidates the live cache after publishing the new values. Existing snapshots retain their previous sorted index and values; the next snapshot builds the new epoch index once.
- [x C] FLD authoritative source and BIN-SYNC vendor are byte-identical at blob `d5fee8be06b9193a294ce4f7c37ba84b5f37da15`.
- [x C] Matching recovery branches `backup/cpass-2026-11-09-snapshot-index-cache` exist in both repositories (BIN-SYNC `f1232647518ff2801c96a2c7a8af3e1e2e47a08c`, FLD-DB `9869aa81f2cbe964163d21922e1949f470496e42`).
- [~ T] Verify snapshots opened in one epoch remain stable across later commits/checkpoints/deletes and that cache invalidation happens only after durable publication. No test/benchmark claim yet.
- [~ O] Measure repeated snapshot creation in unchanged epochs and first snapshot after commit; O(number-of-keys) index creation remains on a new epoch.


## 0.93. C-pass: concrete FLD host adapter and canonical Storage project injection — 2026-11-09

- [x C] Added optional `samples/FldBinSyncHost/FldBinSyncHostAdapter.cs` implementing the public FLD↔BIN-SYNC composition: all-author discovery, complete JournalSeq page projection, recovery-before-send, APPLY/publication gate, ceiling-bearing outbound callback, durable FLD ACK before RPM3 prune, and one immutable FLD reference snapshot held under the FLD commit guard.
- [x C] Added an optional sample project and build guide. It is not added to the default BIN-SYNC solution because FLD-DB is a separate checkout; it is not a database runtime DLL and creates no FLD→BIN-SYNC dependency.
- [x C] BIN-SYNC `BinSync.Storage` and FLD `Fld1.Database` now both accept `Fld1StorageProject`, allowing the host sample to resolve one canonical FLD Storage project instead of duplicate vendor/authoritative assembly identities.
- [x C] Created matching pre-adapter recovery branches `backup/cpass-2026-11-09-host-adapter-prep` in both repositories (BIN-SYNC at `ef410ca05d8230618273c2907c52b106ba51d671`; FLD-DB at `cf4c454d539f96766ef66febb3d117c9a7c5965b`).
- [~ T] Compile the optional sample against the sibling FLD checkout; verify the shared Storage project resolves once, then add host crash/reopen tests for recovery, publication ceilings, multi-author drain, ACK-before-prune and scanner disposal/lock order. No test/build claim is made for this sample yet.
- [~ C] Transport/session construction and process-level startup scheduler remain application responsibilities; the sample now supplies the safe sender drain helper. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 0.94. C-pass: enforce publication ceilings inside the Core sender — 2026-11-09

- [x C] The publication gate captures the local-author high watermark and all relay-author RPM3 ceilings under one composed application commit guard. Before provider cutover the local ceiling is read from Node Outbox; after cutover it is read from Metadata Store. The captured authority-mode bit accompanies every watermark, preventing a cutover race between ceiling capture and send.
- [x C] Added ceiling-aware `SyncSession.PushBatchAsync` overloads for both Node-owned and Metadata Store-owned transport. The Node path filters Outbox entries by author ceiling; the Metadata Store path filters outbound/relay pages and rejects immutable pending reservations above the captured absolute sequence (`CursorEnd + StreamOffset`). Both exclude absent/zero-ceiling authors; legacy overloads preserve their prior behavior.
- [x C] `FldBinSyncHostAdapter.PublishReadyOutboxAsync` selects the matching sender path from the captured mode, converts the watermark list into the Core ceiling map and drains a peer session only through the enforcing overload.
- [~ T] Verify local and relay streams at zero/exact/above-ceiling boundaries, pending-reservation replay, page truncation and concurrent local freeze between ceiling capture and send. No tests/build have been run for this C-pass.
- [~ C] The host still owns session creation, transport lifecycle, multi-peer scheduling and ensuring every send uses the gated helper. Physical GC remains disabled.


## 0.95. C-pass reconciliation of earlier host-adapter checklist entries — 2026-11-09

- [x C] Reconciled the earlier open items in §§0.79–0.80, 0.84–0.89 and the integration/API checklist: the FLD relay-page reader, RPM3 resolver/append, startup recovery, publication gate, local+relay author ceilings, Core-enforced sender, durable FLD ACK-before-RPM3-prune and reference-snapshot scanner are implemented in the optional host sample and Core/Storage APIs (§§0.93–0.94).
- [~ C] Still open in the broad C-pass: the `FldDatabase`→`IDataProvider` bridge (stable paged snapshots, local feed/ChangeId identity and full remote-apply context), generic Storage page/run/WAL backend compatibility decision and implementation, final public/package/source/byte audit, and host-owned transport/session scheduler. The existing FLD1 field-aware WAL/Run/Manifest format is not to be replaced without the required versioned compatibility decision.
- [~ T] The optional cross-repository sample and new sender overload have not been built or tested. The earlier red BIN-SYNC Core/Storage tests remain T-pass work; no physical receipt, ChangeId or tombstone/Ghost GC is enabled.


## 0.97. C-pass: preserve the active outbound authority across cutover — 2026-11-09

- [x C] The local publication ceiling now comes from Node Outbox before provider-authoritative cutover and from Metadata Store after cutover. Relay ceilings remain RPM3-derived in both modes.
- [x C] The gate captures the authority-mode bit under the composed application commit guard and carries it with the complete watermark set. The sender no longer re-reads mutable cutover state after the barrier is released.
- [x C] Added ceiling-aware Node-owned transport filtering and updated the host sample to choose Node or Metadata Store send path from the captured mode. This preserves the pre-cutover Node Outbox path and the post-cutover Metadata Store path without mixing sequence domains.
- [~ T] Verify both modes before/after cutover, concurrent cutover attempts, local and relay ceiling boundaries, retry/reservation replay and process restart. No tests/build have been run for this C-pass.
- [~ C] Host still owns session lifecycle and multi-peer scheduling; all sends must go through the gate callback. Physical GC remains disabled.


## 0.98. C-pass: expose provider-feed freeze in the concrete host adapter — 2026-11-09

- [x C] `FldBinSyncHostAdapter.PublishLocalChanges(persistNode, limit)` exposes the coordinator's local-feed freeze operation. Convenience overloads for `RecoverAndPublishAllAsync`, `ApplyAndPublishAllAsync` and `RunOutboundAsync` invoke it before entering the gate, draining provider changes into Node Outbox before cutover or Metadata Store after cutover.
- [x C] The integration guide and sample lifecycle require a local-feed freeze before startup recovery/publication and before each later outbound gate invocation. Provider edits not frozen at the captured barrier remain above the ceiling and are deferred safely.
- [~ T] Verify feed drain, idempotent retry, persistence callback ordering and concurrent provider writes around ceiling capture in both publication modes. No tests/build have been run for this C-pass.


## 0.99. C-pass correction: relay/GC adapter is not the full IDataProvider bridge — 2026-11-09

- [x C] The optional sample does implement the FLD relay-page/RPM3/publication/ACK/reference-scan side and both Node-owned/Metadata-Store-owned ceiling-enforced sender paths.
- [x C] The full remote-apply-context half is now implemented separately by `IProviderRemoteBatchCommitStore` + `FldRemoteBatchApplyStore` (§§1.04–1.05); this preserves the FLD `RemoteBatchCommit` header without forcing it into `IDataProvider`.
- [~ C] It does **not** yet adapt `FldDatabase` to BIN-SYNC's `IDataProvider`. `FldDatabase` still lacks a generation-pinned paged full-data snapshot handle, Outbox-backed local feed/author-epoch mapping, record/field key-resolver policy and current-state `TryReadCurrent` semantics. Do not invent a lossy adapter or materialize the entire database into RAM as a shortcut.
- [~ T] After that contract/code is complete, build the optional sample and test snapshot stability, remote-apply idempotency, local feed identity, cutover, crash recovery and GC horizons. Physical GC remains disabled.


## 1.00. Mandatory v3.1 document hierarchy and release gate — 2026-11-09

- [x C] The roadmap distinguishes the v3.0.0 base normative contract, the mandatory v3.1 Roster/GC Addendum, the non-normative host orchestration pattern, and the required deterministic golden-vector fixtures.
- [x C] Clarified the compatibility boundary: v3.1 implementation/support is mandatory for project completion; v3.1-only behavior remains explicitly capability-negotiated and does not modify v3.0-only wire frames. A required member without v3.1 capability contributes no safe GC frontier.
- [x C] Added direct cross-references from the v3.0 specification, Public API, addendum and host guide to the correct companion documents. Corrected the stale host-guide statement that treated the synchronized-FLD relay adapter as wholly future work.
- [~ T] Execute `samples/V31ControlPlaneVectors`, assert exact payload/signature bytes and negative tamper cases in CI, and complete authority/member, joining/rejoin, FULL_REPLACE, crash/restart and safe-horizon conformance. Existing source implementation is not a conformance pass.
- [~ C] The `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/full remote-apply bridge, generic Storage format decision and host transport scheduler remain open. Physical GC remains disabled.


## 1.01. C-pass: execute v3.1 golden vectors as a required CI sample — 2026-11-09

- [x C] Extended `samples/V31ControlPlaneVectors` beyond encode-only checks: NRS2/OFR2/RCP2/FRA2 now decode and verify against trust/roster context and re-sign to byte-identical payloads; FULL_REPLACE_BEGIN now decode/verifies against the accepted roster; chunk/READY/COMMIT/ABORT round-trip coverage remains.
- [x C] Added fail-closed sample cases for truncated signatures, trailing bytes, tampered payloads, wrong pinned authority key, wrong signing domain, wrong roster generation/digest and invalid FULL_REPLACE_READY flags.
- [x C] Added the vector sample to `.github/workflows/dotnet.yml` and included `samples/**` in push/PR path filters so vector-only changes cannot bypass CI.
- [~ T] Exact-head CI for the sample change is running; close only after the executable returns PASS and Core/Fuzz plus Storage Integration results are captured for the same code head.
- [~ T] Add dedicated unit tests for malformed roster/checkpoint/frontier edge cases and the complete FULL_REPLACE sender/receiver crash/rejoin state machine; the sample does not replace the full adversarial test matrix.


## 1.02. C-pass: executable v3.1 authority/member host-round composition — 2026-11-09

- [x C] Added `samples/FldBinSyncHost/V31NamespaceGcHostOrchestrator.cs` as a reusable one-shot host helper over the explicit v3.1 coordinator APIs. It sequences accepted-roster send, per-session OFR2 receive/accept, exactly one durable RCP2 publication and broadcast of the same checkpoint.
- [x C] Added Active-member round composition: receive/accept roster, reject non-Active local members, publish/send OFR2 under the supplied composed host lease, then receive/accept RCP2. Added retries for an already-durable OFR2 and an already-accepted RCP2 without allocating new report/checkpoint sequence identities.
- [x C] The helper validates unique remote sessions and binds each received OFR2's member NodeId and transport epoch to the authenticated session peer. Missing Active roster members remain safe at frontier zero; caller-provided session lists are not treated as authoritative membership.
- [~ C] FULL_REPLACE Joining→installed ACK→Active transition remains a separate explicit receiver/authority operation; the helper intentionally refuses to run an Active-member frontier round for a Joining/Retired member. Actual `FldDatabase`→`IDataProvider` snapshot/local-feed/full remote-apply bridge remains open.
- [~ T] Build the cross-repository sample and test partial authority-round failure, checkpoint rebroadcast idempotency, report resend, duplicate/mismatched session identity, stale transport epoch, missing Active member frontier zero, cancellation and concurrent receive ownership. No build/test pass is claimed for this new helper.


## 1.03. C-pass: cross-repository FLD host build added to CI — 2026-11-09

- [x C] Added independent `fld-host-integration-build` job to `.github/workflows/dotnet.yml`. It checks out the BIN-SYNC branch and `FLD-DB/fix/crash-consistency-outbox`, vendors the required Ed25519 source, and builds the optional host adapter against explicit canonical `Fld1.Database.csproj` and `Fld1.Storage.csproj` paths.
- [x C] The job is independent of the red Core/Storage test jobs, so the cross-repository compile result is visible even while fixture migration remains outstanding.
- [~ T] Exact-head job for workflow commit `6ff484f2730d22aacb224163628ff528417fb3a7` was queued/in progress at this update; no build pass is claimed until the job completes.


## 1.04. C-pass: preserve full remote APPLY context across provider boundary — 2026-11-09

- [x C] Added optional Core capability `IProviderRemoteBatchCommitStore` and immutable `ProviderRemoteBatchCommit`. It retains authenticated SenderPeer/transport epoch separately from wire issuer/epoch, and carries BatchId, cursor range, previous/current digest, RecordsRoot, protocol-state delta and immutable provider mutations.
- [x C] `StorageSyncCoordinator` dispatches to the contextual capability after Node apply and during PendingApply recovery. It verifies the exact full-header fingerprint against the durable PendingApply before calling the provider; non-capability providers retain the existing `IDataProvider.ApplyRemoteBatch(token, mutations)` fallback.
- [x C] Added `samples/FldBinSyncHost/FldRemoteBatchApplyStore`, which maps the full context to FLD `RemoteBatchCommit` and `FldDatabase.ApplyRemoteBatch`. Record tombstones map to FLD lifecycle field 0/DeadValue; field tombstones remain distinct.
- [x C] The FLD adapter deliberately passes an empty `RelayWrites` set because BIN-SYNC Metadata Store is the sole v3 owner of relay payload and delivery retention; it does not create a duplicate FLD relay history. The current v3 Batch DTO has no protocol-state-delta member, so none is synthesized.
- [~ C] Full `IDataProvider` integration is not complete: stable generation-pinned paged snapshots, Outbox-backed local feed/author-epoch mapping, record/field key resolver policy and current-state `TryReadCurrent` semantics remain open.
- [~ T] Build the cross-repository sample using `FLD_DB_READ_TOKEN` (fine-grained Contents: read for FLD-DB) and test lossless header propagation, same-apply-token retry, crash after FLD WAL/ApplyJournal before Metadata finalization, record/field tombstone mapping and duplicate replay. No sample build is claimed until the private checkout job actually executes.


## 1.05. C-pass: inject remote-apply capability separately from IDataProvider — 2026-11-09

- [x C] `StorageSyncCoordinator` accepts an optional `IProviderRemoteBatchCommitStore` dependency separate from `IDataProvider`. Contextual remote APPLY and PendingApply recovery use the explicit capability first, then a provider-implemented capability, then the compatibility fallback.
- [x C] Added `UsesRemoteBatchCommitStore` and `FldRemoteBatchApplyStore.IsBoundTo`; `FldBinSyncHostAdapter` requires and verifies the exact same capability instance and exact same `FldDatabase` instance, failing closed during construction on split-brain wiring.
- [x C] Added `FldBinSyncHostAdapter.ReceiveAndPublishAllAsync`, which routes inbound frames through `ReceiveBatchBeforeAckAsync`: PendingApply → Node checkpoint → contextual FLD `RemoteBatchCommit`/ApplyJournal → Metadata receipt finalization → incoming ACK → all-author relay mapping/publication. Host guides now use this entry point rather than a generic/lossy provider callback.
- [~ C] The full `FldDatabase`→`IDataProvider` snapshot/local-feed/key-resolver bridge remains open; this dependency split closes only the lossless remote-apply half.
- [~ T] Verify recovery calls use the injected adapter after process reopen, wrong-store/wrong-database construction is rejected, duplicate ApplyToken is idempotent, and Node/FLD/Metadata finalize order survives each crash cut. The current cross-repository build job skipped the private FLD checkout because no read credential was available.


## 1.06. C-pass: exact FLD contextual apply-store identity and receive gate — 2026-11-09

- [x C] Added `StorageSyncCoordinator.UsesRemoteBatchCommitStore` and `FldRemoteBatchApplyStore.IsBoundTo`; the host adapter now requires the same capability object to be injected into the coordinator and the same exact `FldDatabase` instance to be held by the adapter. Split-brain/wrong-instance setup fails before any network work.
- [x C] Added `ReceiveAndPublishAllAsync` so the concrete host receives through `StorageSyncCoordinator.ReceiveBatchBeforeAckAsync`, ensuring the authenticated PendingApply context is delivered to `FldRemoteBatchApplyStore` before the source ACK; relay pages are drained only after that receive transaction returns.
- [x C] README and host/integration guides now show construction order and use the lossless receive method; public API docs describe the optional contextual capability.
- [~ C] The full `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/key-resolver bridge, generic Storage ordered page/run backend decision and host-owned transport scheduler remain.
- [~ T] Cross-repository build still needs an authenticated FLD checkout; add wrong-instance, duplicate ApplyToken and crash-cut ordering coverage before T-pass closure. Physical GC remains disabled.


## 1.07. C-pass: serialize v3.1 checkpoint retry and preflight round inputs — 2026-11-09

- [x C] Public `BroadcastAcceptedCheckpointAsync` now acquires the same one-shot `_roundGate` as authority/member rounds. `RunAuthorityRoundAsync` uses a private already-gated core so it does not deadlock; partial broadcast retry remains bound to the exact current durable RCP2 and cannot mint another checkpoint sequence.
- [x C] `PublishAuthorityMemberFrontier` uses a non-blocking gate acquisition and fails closed if another namespace round is active, rather than racing its issued/applied frontier with checkpoint publication.
- [x C] Authority round validates authority/member Ed25519 seed lengths, page bounds, FULL_REPLACE generation/digest pairing and session identities before sending the accepted roster; invalid input no longer partially mutates remote sessions before failing.
- [x C] Created recovery branch `backup/cpass-2026-11-09-contextual-receive-v31-gate` at `3c256b0cd9d7df778874659cf5482519889cd476`.
- [~ T] Verify concurrent authority-round + checkpoint-retry exclusion, partial-broadcast recovery, cancellation at each session boundary, non-member/Joining session rejection, durable frame-sequence monotonicity and same-checkpoint digest identity. Latest CI for `3c256b0` was queued/in progress at this update; no result is claimed.
- [~ C] The `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/key-resolver bridge and generic Storage backend/format strategy remain open. Physical receipt, ChangeId and tombstone/Ghost GC stay disabled.

## 1.08. C-pass: immutable prepared FULL_REPLACE snapshot and shared manifest — 2026-11-09

- [x C] Added `PreparedFullReplaceSnapshot`, which owns one provider snapshot token plus the one signed FULL_REPLACE manifest (roster generation/digest, source change-feed watermark, entry count and digest). Disposal releases the provider snapshot exactly once.
- [x C] Refactored `StorageFullReplaceSender` with `PrepareSnapshot` and `SendPreparedSnapshotAsync`. The digest scan and manifest signing now happen once; each recipient streams the same immutable snapshot and verifies the same signed digest. Existing single-recipient `SendAsync` remains as a convenience wrapper.
- [x C] Added `V31NamespaceGcHostOrchestrator.TransferFullReplaceToMembersAsync`: validates the full non-retired roster/session set, broadcasts the accepted roster, captures one source snapshot and fans the same generation out to all intended peers.
- [~ T] Exact-head compile and partial-fanout/retry tests remain pending. Verify that every recipient receives identical watermark/count/digest and that an already-installed recipient can safely retry the same generation. This is not yet a durable cross-process source snapshot.
- [~ C] The `FldDatabase`→`IDataProvider` source snapshot/local-feed/key-resolver bridge and durable source-snapshot recovery across process restart remain open. Physical GC remains disabled.

## 1.09. C-pass: retain prepared FULL_REPLACE source snapshot for partial fanout retry — 2026-11-09

- [x C] Added `PrepareFullReplaceSnapshot` to return ownership of the immutable prepared snapshot to the host instead of tying its lifetime to one fanout call.
- [x C] Added `TransferPreparedFullReplaceToMemberAsync`. A host can retry only a missing/failed recipient using the exact same source watermark, roster digest, generation and FULL_REPLACE digest; the method revalidates the current accepted roster and target transport epoch before sending.
- [x C] The convenience all-member fanout remains available; the explicit prepare/retry API is the resumable in-process path. The caller owns and must dispose the prepared snapshot only after the intended fanout is complete or abandoned.
- [~ T] Verify partial failure after each recipient, repeat send to already-installed receiver, accepted-roster change during retry, and snapshot disposal after cancellation. No exact-head test result is claimed.
- [~ C] In-process prepared snapshot lifetime does not yet survive process restart; crash-resumable fanout requires a durable provider snapshot generation. The missing FLD provider adapter must supply that without synthesizing ChangeId/origin metadata.

## 1.10. C-pass: fail-fast FULL_REPLACE manifest/roster preflight — 2026-11-09

- [x C] Added `StorageFullReplaceSender.ValidatePreparedSnapshot` to verify the prepared snapshot watermark, accepted roster generation/digest, authority identity and Ed25519 manifest signature before a sender callback can touch the network.
- [x C] `TransferPreparedFullReplaceToMemberAsync` now validates the retained manifest against the current accepted roster before sending a roster or FULL_REPLACE frame. A stale roster or invalid authority key fails locally.
- [x C] The convenience single-recipient and all-member fanout paths now capture and verify the provider snapshot/manifest before broadcasting the roster. This avoids partial remote state changes when the source snapshot or authority signature is invalid.
- [~ T] Validate no network frame is sent on invalid seed, wrong authority, stale roster, disposed snapshot, corrupt page, and wrong target epoch; verify partial retry after a successful preflight. CI for the newest code head is pending.
- [~ C] Prepared source snapshot survives retries only while retained in process memory. Durable FULL_REPLACE source generation recovery after process restart remains open; no physical GC is enabled.

## 1.11. C-pass: bind every FRA2 acknowledgement to the exact source snapshot HLC floor — 2026-11-09

- [x C] `StorageFullReplaceSender.CreateManifest` computes the maximum `(HlcWall,HlcLogical)` while scanning the exact immutable provider snapshot used for the signed manifest/digest. `PreparedFullReplaceSnapshot` retains the pair for multi-recipient fanout and retry.
- [x C] Both `SendPreparedSnapshotAsync` and the compatibility `SendSnapshotAsync` path now pass the source snapshot floor into the common transfer routine. After FRA2 signature/identity/roster/digest checks, the sender also requires the installed ACK's HLC floor to equal the source floor exactly before persisting the ACK into the namespace GC store.
- [x C] This prevents a validly signed but inconsistent receiver acknowledgement from inflating or lowering the common RCP2 FULL_REPLACE HLC floor. All members must install the same immutable source HLC horizon, not merely report mutually equal values.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-fullreplace-hlc-ack` in BIN-SYNC and FLD-DB (BIN-SYNC anchored at `d4ad36fe4fc1a3660c43cc4209a71f400ed224fe`, FLD-DB at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add tests for exact wall/logical equality, wall mismatch, logical mismatch at equal wall, empty snapshot `(0,0)`, source changes after snapshot, signed inconsistent FRA2 from a custom peer and retry after a rejected ACK. No tests were changed or claimed in this C slice.
- [~ C] Durable FULL_REPLACE source snapshot recovery across sender process restart remains open; this code only guarantees process-local immutable snapshot identity. `FldDatabase`→`IDataProvider` generation-pinned snapshot/local-feed/key-resolver bridge also remains open.

## 1.12. C-pass: make prepared FULL_REPLACE snapshot disposal safe during async fanout

- [x C] `PreparedFullReplaceSnapshot` now tracks active transfer leases. `Dispose()` requests release immediately but defers `IDataProvider.ReleaseSnapshot` until the final in-flight transfer exits; release remains exactly-once and is safe after cancellation/failure.
- [x C] `StorageFullReplaceSender.SendPreparedSnapshotAsync` acquires the transfer lease before preflight and holds it across the awaited network transfer. A concurrent dispose can no longer invalidate provider pages after BEGIN/READY or between CHUNK frames. New transfer attempts after disposal are rejected.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-fullreplace-send-lease` in BIN-SYNC (anchored at `545041f4d35a9e9ed5414d1619ffedba6559312d`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Verify dispose-before-send rejection, dispose during READY wait, dispose mid-chunk, cancellation, concurrent transfers and exactly-once ReleaseSnapshot. No exact-head CI result exists for these commits.
- [~ C] Prepared snapshots remain process-local; restart-resumable source generation persistence is still open.

## 1.13. C-pass: Joining promotion requires the exact RCP2 FULL_REPLACE HLC floor

- [x C] `V31NamespaceGcHostOrchestrator.PromoteInstalledJoiningMemberAsync` now checks the Joining member's durable FRA2 `HlcWallFloor` and `HlcLogicalFloor` against the already accepted prior-roster RCP2 before changing the member to Active.
- [x C] Promotion now requires agreement on the exact generation, digest, roster generation, transport epoch, installed state and both HLC floor components. A signed ACK for the same digest but a different HLC horizon cannot activate the member.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-joining-hlc-promotion` in BIN-SYNC (anchored at `10a4e17492641a3911acdbdf8e4aadc865dad371`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add promotion rejection tests for HLC wall mismatch, logical mismatch at equal wall, wrong prior roster generation, stale transport epoch, wrong digest and valid matching ACK. No exact-head CI result is claimed.
- [~ C] Durable prepared FULL_REPLACE snapshot recovery across process restart and the complete FLD provider snapshot/local-feed/key-resolver bridge remain open.

## 1.14. C-pass: enforce v3.1 capability dependencies symmetrically

- [x C] OFR2 send and receive now both require `NamespaceRosterV31 | OriginFrontierV31`; RCP2 send and receive require the same pair because a frontier/checkpoint is meaningful only under the accepted roster context.
- [x C] All FULL_REPLACE transport frames now require `NamespaceRosterV31 | FullReplaceV31`. FRA2 send and receive use the same dependency; a peer cannot advertise FULL_REPLACE support while omitting the roster capability needed to authenticate member identity/epoch.
- [x C] v3.1 capability negotiation remains independent of v3.0 frame behavior. Partial capability sets can establish a session, but dependent v3.1 operations fail closed before sending/receiving frames.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-v31-capability-dependencies` in BIN-SYNC (anchored at `7a8c7f2eea341b879d6f39b747e19d3b3e5c0357`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add asymmetric capability-matrix tests for every v3.1 send/receive API, exact negotiated intersections, partial peers and v3.0-only interoperability. No exact-head CI result is claimed.
- [~ C] Durable FULL_REPLACE source snapshot recovery, complete FLD provider snapshot/local-feed/key-resolver bridge, generic Storage physical format decision and host transport scheduler remain open.

## 1.15. C-pass: validate v3.1 capability dependencies in HELLO/HELLO_ACK

- [x C] `SessionWire.EncodeHello` and `EncodeHelloAck` reject local capability advertisements that include `OriginFrontierV31` or `FullReplaceV31` without `NamespaceRosterV31`.
- [x C] `SessionWire.DecodeHello` and `DecodeHelloAck` reject the same invalid dependency mask from a remote peer with `FRAME_REJECTED`, before constructing a session. Unknown future capability bits remain forward-compatible; valid v3.0 and v3.1 bitmasks retain identical wire bytes.
- [x C] Created matching recovery branches `backup/cpass-2026-11-09-v31-hello-capability-validation` in BIN-SYNC (anchored at `7e763f0f5939ce2b134e0d5b7a55e92361e51b3b`) and FLD-DB (anchored at `d432d0a1f30720a352dacff76bfd7ffee4edd55f`).
- [~ T] Add HELLO/HELLO_ACK encode/decode tests for each partial/valid capability mask, unknown bits, malformed peer advertisements and byte-for-byte valid HELLO compatibility. No exact-head CI result is claimed.
- [~ C] Durable FULL_REPLACE source snapshot recovery, FLD FULL_REPLACE receiver staging and generic Storage physical-format decision remain open; base provider bridge, author closure, publication cutover and host scheduler are implemented in §§1.20–1.24.


## 1.16. C-pass: make Metadata Store the exclusive relay authority after cutover — 2026-11-09

- [x C] Added a direct Metadata Store-owned publication watermark capture: local author from `GetOutboundHighWatermark` and relay authors from `IMetadataPeerStateStore.ReadRelayAuthors` + `GetRelayHighWatermark`, all captured under the composed coordinator → Node → FLD → Storage barrier. No FLD JournalSeq or RPM3 data is consulted after cutover.
- [x C] Added metadata-owned startup recovery, APPLY→send, later outbound and ACK-compaction paths in `StorageRelayPublicationGate`. Startup remains closed until PendingApply recovery succeeds; network send retry does not incorrectly require recovery again.
- [x C] Routed `FldBinSyncHostAdapter` by captured publication mode. After cutover, contextual FLD `RemoteBatchCommit` has an intentionally empty `RelayWrites` set; Metadata Store finalization owns the only relay payload/sequence. Per-peer ACK is committed by `SyncSession`; safe relay payload compaction uses the minimum durable peer cursor and `CompactRelayThrough`. Legacy Node-owned mode retains FLD JournalSeq→RPM3→FLD ACK→RPM3 prune.
- [x C] Added an exact Metadata Store identity check for the sender's `IMetadataPeerStateStore` instance; split Storage partitions fail closed before metadata-owned send.
- [x C] Corrected startup flow so `RecoverPendingApplies` and local-feed freeze run under the composed commit lease, then metadata-owned startup calls `RecoverMetadataOwnedAndPublishAsync` instead of the legacy RPM3 recovery path.
- [x C] Updated the sample README, host composition guide and FLD integration contract to distinguish the two publication authorities and their independent ACK/compaction rules.
- [~ T] Add focused tests for metadata relay-author discovery, local/relay high-watermark capture, exact Metadata Store identity, startup recovery gating, send failure/retry, per-peer minimum ACK, compaction restart and legacy-mode non-regression. The current run at `513188e` is still in progress; Storage Integration on Ubuntu is red on existing provider/origin fixture migrations (91/171), not a test pass for the new relay path.
- [~ C] Full `FldDatabase`→`IDataProvider` bridge remains open: generation-pinned paged snapshot, stable current-state ChangeId/origin mapping, Outbox-backed local feed/author-epoch contract, key resolver and `TryReadCurrent`. Generic Storage page/run/WAL format decision and host transport scheduler also remain. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 1.17. C-pass: add indexed immutable-run access for the FLD provider snapshot — 2026-11-09

- [x C] FLD-DB now exposes `RunReader.TryReadRecordIdAt(uint, Span<byte>)` in `fix/crash-consistency-outbox` (commit `a6e20ce15c7d13ed63579db019d00e52b7b7e63f`). It reads a canonical run slot by index without allocating or scanning earlier clusters.
- [x C] Verified against `RunBuilder`: immutable run cluster slots are emitted in canonical RecordId order, enabling a bounded k-way merge across run generations.
- [~ C] Continue the FLD generation-pinned paged snapshot over captured immutable run buffers and a bounded overlay snapshot; then implement the complete `IDataProvider` current-state, local-feed and key-resolution bridge. No whole-database RAM materialization.
- [~ T] FLD RunReader index-boundary/order tests and cross-repository snapshot/compaction stability tests remain deferred to the dedicated T-pass. No build or test pass is claimed.
- [~ C] Durable FULL_REPLACE source snapshot recovery across sender restart, generic Storage page/run/WAL format decision and host transport/session scheduler remain open. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 1.18. C-pass: implement generation-pinned paged FLD provider snapshot primitive — 2026-11-09

- [x C] FLD-DB `FldDatabase.OpenProviderSnapshot()` now captures an immutable Run/overlay generation and the source Outbox watermark under its commit guard. The handle retains immutable run buffers across concurrent compaction and releases them exactly once on disposal/database close.
- [x C] Snapshot pages use an O(keys) compact sorted RecordId index and bounded page materialization; no whole-database field/value list is built. The implementation is in FLD-DB commit `86088614d06297f5c9a44ffcc9784b33abbb0961` plus follow-up correctness/performance commits `c010333c019001bab35af29b879e5757020309ee`, `8f54b6c09396897bc88ecb5c2da6e65d634b99fe`, `08fcad22169664c2d9f1ab204278b247c2111215` and `8202c6c5b7085071f6df3d2db81389d76e4e4a84`.
- [x C] Current-state resolution preserves Run v2 ChangeId/Origin and keeps Run v1 identity-zero semantics; field tombstones and record lifecycle tombstones remain distinct.
- [~ C] Bind this snapshot to BIN-SYNC `IDataProvider`, including FLD Outbox local feed, durable published-origin stamping, key resolution and current-state lookup. Durable FULL_REPLACE source-generation recovery across sender restart remains open.
- [~ T] Snapshot/compaction concurrency, ordered page retry, tombstone mapping, mixed Run v1/v2, release-on-dispose and cross-repository host build tests remain deferred. No build/test result is claimed.
- [~ C] Generic Storage page/run/WAL physical-format decision and host transport/session scheduler remain open. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.


## 1.19. C-pass: permit identity-zero legacy record tombstones only as snapshot state — 2026-11-09

- [x C] Relaxed `ProviderMutationCodec.Validate` so a canonical lifecycle tombstone with zero ChangeId/Origin can represent a legacy materialized Run v1 snapshot row. This aligns the codec with the FLD snapshot ADR: legacy state is retained without inventing identity.
- [x C] `ValidatePublished` remains strict and still rejects identity-zero record tombstones from outbound freeze or remote APPLY; immutable operations continue to require complete ChangeId/Origin.
- [~ T] Add explicit codec/digest/FULL_REPLACE vectors for legacy record tombstones and negative tests proving `ValidatePublished` rejects them. No test/build result is claimed.
- [~ C] Complete the BIN-SYNC `IDataProvider` host adapter and source-feed/key-resolution bridge; durable FULL_REPLACE sender restart recovery, generic Storage physical-format decision and host session scheduler remain open.


## 1.20. C-pass: add the base FLD Database IDataProvider bridge and enforce exact host composition — 2026-11-09

- [x C] Added optional-sample `FldDatabaseDataProvider`: direct 16-byte RecordId / 2-byte FieldId resolution, fenced FLD Outbox feed, stable ChangeId and deferred origin epoch/sequence mapping, generation-pinned paged snapshots, current-state reads, published-origin WAL stamping and FLD commit guard.
- [x C] Context-free `IDataProvider.ApplyRemoteBatch` deliberately fails closed. The host must inject the exact `FldRemoteBatchApplyStore` so sender/transport epoch/cursor/digest context is not discarded.
- [x C] Added `StorageSyncCoordinator.UsesDataProvider` and constructor checks in `FldBinSyncHostAdapter`: the coordinator must use the exact FLD provider, the provider must use the exact FLD database, and the contextual apply store must target that same database.
- [x C] Updated the host README with construction order and the explicit remaining boundaries. FLD runtime still has exactly two DLLs; the adapter remains optional host code.
- [~ C] Remaining v3/v3.1 gates: FLD FULL_REPLACE receiver staging/atomic generation switch, durable sender snapshot generation recovery across restart, and generic Storage page/run/WAL physical-format decision. Author-epoch closure and host multi-session scheduling are implemented in §§1.21 and 1.23.
- [~ T] Exact cross-repository sample build and provider conformance/crash tests are deferred to the dedicated T-pass. No test/build pass is claimed.
- [~ C] Physical receipt, ChangeId and tombstone/Ghost GC remain disabled until the complete cross-store reference proof and T-pass.


## 1.21. C-pass: close local FLD author epochs only after drafts and Outbox are drained — 2026-11-09

- [x C] `FldDatabaseDataProvider` now implements `IProviderAuthorEpochClosureStore`. An epoch closes only while holding the FLD commit guard and only when `ReadUnpublishedLocalDraftHorizon().Count == 0` and Outbox high watermark equals its acknowledged watermark.
- [x C] Local append and epoch activation use the same FLD→provider lock order as closure, preventing a writer that passed an early check from racing into a just-closed epoch.
- [x C] The adapter obtains the active epoch from the host's durable BIN-SYNC identity authority; it does not duplicate protocol epoch state in FLD. New epochs must be strictly greater than the last locally closed epoch.
- [~ C] FLD FULL_REPLACE receiver staging/atomic generation switch, sender restart snapshot recovery and generic Storage physical-format decision remain open. Publication cutover is implemented in §1.24 and host session scheduling in §1.23.
- [~ T] Verify pending drafts, unacked Outbox, replayable WAL horizon, writer-vs-close race, restart after closure, reactivation and stale-epoch rejection in the T-pass. No test/build result is claimed.


## 1.22. C-pass: preserve pre-freeze ChangeId while binding the active local epoch at the provider edge — 2026-11-09

- [x C] `ProviderMutationCodec.Validate` now accepts a stable ChangeId with an unassigned Origin tuple (epoch/sequence both zero) for a materialized or unpublished provider row. It still rejects half-populated Origin tuples and Origin without ChangeId.
- [x C] `ValidatePublished` remains strict: outbound freeze and remote APPLY still require nonzero ChangeId, OriginAuthorEpoch and OriginSequence.
- [x C] `FldDatabaseDataProvider` binds the host's durable active author epoch to local ChangeId-bearing current-state/snapshot rows whose FLD Run provenance has not yet been stamped. Legacy Run v1 rows with zero ChangeId remain identity-zero and receive no synthetic identity.
- [x C] The local Outbox feed similarly binds the active epoch while keeping OriginSequence deferred until Metadata Store freeze; author-epoch closure prevents a feed from crossing epoch transitions.
- [~ T] Add codec/digest vectors for ChangeId-without-Origin, published-operation rejection, local epoch binding, legacy Run v1 rows, stamp-before/after-supersede and restart after epoch closure. No build/test result is claimed.


## 1.23. C-pass: add host-owned multi-session send/receive scheduling — 2026-11-09

- [x C] Added `FldBinSyncSessionScheduler`: one receive owner per session, serialized sends per session, concurrent fanout across peers and one captured publication-ceiling set shared by every peer in a namespace round.
- [x C] Added one-shot receive/apply/publish, continuous inbound loop and host-cadence periodic outbound loop. Both outbound paths freeze the provider local feed through the supplied Node persistence callback before capturing publication ceilings. A false publication-gate result stops the receive loop so the host can run reconciliation/recovery rather than silently continuing.
- [x C] Scheduler shutdown clears its gate registry without disposing semaphores still held by active duplex callbacks; lock-order inversion during receive-triggered fanout is avoided.
- [~ C] Application still owns transport/session creation, reconnection policy and final `SyncSession` disposal. Generic Storage page/run/WAL physical-format decision and durable FULL_REPLACE sender restart recovery remain open.
- [~ T] Validate one-reader-per-session, concurrent duplex send/receive, multi-peer ceiling consistency, failure retry, cancellation and scheduler shutdown with exact-head sample tests. No build/test result is claimed.


## 1.24. C-pass: persist FLD provider publication cutover in the exact Metadata Store keyspace — 2026-11-09

- [x C] Added `StorageMetadataPublicationMode`, a namespace-scoped `IProviderPublicationMode` view over the exact `StorageMetadataStore` instance used by the coordinator.
- [x C] The immutable 16-byte MCP1/version-1 cutover marker is persisted under a dedicated root (0x71) and namespace key, separate from decoded metadata partition rows so ordinary metadata rewrites cannot erase it. Partition identity, marker magic/version/length, immutable watermark and Storage commit-sequence conflict are validated.
- [x C] `FldDatabaseDataProvider` delegates publication-mode reads/writes to that wrapper and refuses cutover while FLD dirty/replayable drafts or unacknowledged Outbox entries remain.
- [x C] `FldBinSyncHostAdapter` now requires exact provider/FLD database/Metadata Store/scope/local-replica/namespace/remote-apply identities; a provider cutover marker cannot silently point at a different namespace or Metadata Store.
- [~ T] Add reopen/restart, concurrent metadata-write conflict, immutable marker mismatch, missing/corrupt identity row, namespace isolation, exact-composition and cutover crash-boundary tests. No build/test result is claimed.
- [~ C] FLD FULL_REPLACE receiver staging/atomic generation switch, generic Storage physical-format decision and final API/source/byte audit remain open. Durable sender snapshot recovery is implemented in §1.25 and awaits T-pass proof.


## 1.25. C-pass: persist and restore FULL_REPLACE sender source generations across restart — 2026-11-09

- [x C] Added a bounded, versioned durable state record for each prepared source generation: generation ID, provider snapshot token, source-feed watermark, HLC floor and exact signed FULL_REPLACE_BEGIN bytes. The record has a SHA-256 integrity checksum and is indexed by scope/local replica/namespace in a dedicated Storage keyspace.
- [x C] Added idempotent immutable save, indexed lookup/list and conflict-checked deletion. Index/state disagreement, malformed lengths, invalid checksums, duplicate generations and missing indexed rows fail closed.
- [x C] Added `PrepareSnapshotAndPersist`, `RestorePreparedSnapshot`, `ReleasePreparedSnapshot` and `PruneUnreferencedPreparedSnapshots`. Restore checks the durable provider token, snapshot page shape/count, signed manifest, current accepted roster/authority and source watermark before returning a sendable handle.
- [x C] Added `IProviderDurableSnapshotStore`; the FLD host provider delegates token-presence and pruning to FLD's persisted provider-snapshot generations. Prepared handles detach on ordinary Dispose while durable state remains; permanent release requires the exact original provider instance and removes sender progress after provider release.
- [x C] Hardened failure ordering: active transfer leases block permanent release; sender progress is retained if provider release fails; interrupted release is reconciled by pruning; stale provider snapshot tokens are removed without loading full Run payloads; generation restore/validation failures retain the persisted source generation for explicit recovery or cleanup.
- [~ T] Verify process restart/reopen, resumed multi-recipient transfer with byte-identical manifest/digest, missing/corrupt token, index/state mismatch, concurrent metadata write conflict, dispose during transfer, release failure at each durability boundary, stale-token pruning and wrong-provider release. No build/test result is claimed.
- [~ C] FLD FULL_REPLACE receiver staging/atomic generation switch, generic Storage ordered page/run/WAL physical-format decision and final API/source/byte audit remain open. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.26. C-pass: implement FLD-backed v3.1 FULL_REPLACE receiver staging and atomic generation switch — 2026-11-09

- [x C] FLD-DB now exposes protocol-neutral durable replacement staging: ordered immutable Run pages, checksummed stage progress, bounded resumable abort, staged-page readback and atomic per-namespace manifest switch. The active FLD generation remains unchanged until the single manifest commit point.
- [x C] The optional BIN-SYNC `FldDatabaseDataProvider` implements `IProviderFullReplace`. It maps mutations to FLD's neutral snapshot entry model, re-reads staged pages and verifies the canonical signed FULL_REPLACE digest with `ProviderFullReplaceDigestBuilder` before the FLD switch.
- [x C] Preserved namespace isolation, WAL/Outbox/ApplyJournal state and old Run generations pinned by durable provider snapshots. Restart recovery detects a manifest switch that completed before the durable stage commit marker; orphan collection protects uncommitted staged Runs.
- [~ T] Exact cross-repository build, receiver restart, page retry/order, lifecycle/field tombstones, v1/v2 provenance, digest tamper, manifest-switch crash boundary, old-snapshot pins, concurrent writer fencing and abort/reopen coverage remain deferred. No build/test result is claimed.
- [~ C] Generic Storage ordered page/run/WAL physical-format decision and final public API/source/byte audit remain the remaining broad C-pass gates. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.27. C-pass: bind durable FULL_REPLACE sender token to the provider's persisted source watermark — 2026-11-09

- [x C] Extended the optional `IProviderDurableSnapshotStore` capability with a fail-closed token→source-watermark query. Providers that can only say a token exists but cannot prove its barrier do not qualify for restart-durable FULL_REPLACE.
- [x C] `FldDatabase` reads the watermark from its checksummed, namespace-bound provider snapshot manifest, verifies pinned Run presence and overlay integrity, and exposes the binding through the host adapter without introducing a BIN-SYNC dependency into FLD.
- [x C] `PrepareSnapshotAndPersist` now verifies that the provider durably owns the exact token/watermark pair before writing sender progress. `RestorePreparedSnapshot` verifies the same pair again before restoring the snapshot and checking its signed manifest/roster.
- [~ T] Verify absent/default capability, token present with mismatched watermark, wrong namespace, missing Run, corrupt overlay, reopen on a new provider instance and token release between lookup/read. No build/test result is claimed.

## 1.28. C-pass: introduce generic immutable Storage blobs and bind FLD Run generations — 2026-11-09

- [x C] Accepted physical-format Option B in FLD-DB ADR: Storage owns generic opaque immutable page/blob durability; Database retains FLD1 Run v1/v2 encoding, manifest catalog, field/tombstone/provenance semantics and FLD WAL. No third runtime DLL and no BIN-SYNC types in Storage.
- [x C] Added `Fld1.Storage.Engine.StorageBlobStore`: bounded page geometry, stable opaque Guid IDs, checksummed metadata (length/page count/SHA-256), pending→complete publication, idempotent resume, immutable-content conflict rejection, snapshot readers, page/whole-blob verification and reader pins.
- [x C] Blob payload pages are durable immutable files rather than large Storage KV values. The small catalog and publication marker remain in StorageDatabase/WAL, avoiding O(total Run bytes) growth of the generic in-memory KV index. Unix page publication now uses the shared native `StorageDirectorySync` helper and fails closed if directory-entry fsync fails; Windows directory-entry durability remains explicitly unclaimed.
- [x C] FLD checkpoints and compaction publish generic blobs before the manifest and retain exact legacy `runs/{RunId}.fld1` mirrors. Reads prefer complete blobs and fall back to legacy files. FULL_REPLACE stages immutable blobs and materializes the compatibility mirror before advancing its durable stage cursor; the empty replacement marker is also published as a blob.
- [x C] Run IDs are allocated root-wide against both legacy files and the generic blob catalog, with explicit exhaustion rejection. This closes the restart collision where an uncommitted FULL_REPLACE stage had a blob ID not yet represented by the active manifest. Normal checkpoint and FULL_REPLACE now share the same allocator.
- [x C] The legacy Run mirror writer now uses the shared native `StorageDirectorySync` path and fails closed if Unix directory-entry fsync fails. Checkpoint, compaction and FULL_REPLACE cannot publish a manifest/stage reference until both the blob publication and compatibility mirror rename are durable on Linux/macOS; Windows directory-entry durability remains unclaimed.
- [x C] Orphan collection combines the full manifest Run set, durable provider-snapshot pins and incomplete FULL_REPLACE stage Run IDs before pruning catalog blobs and unreferenced page directories. Reader pins prevent deletion while a live reader is open.
- [x C] `StorageBlobReader` serializes page reads, whole-blob streaming/materialization and disposal under one reader gate, so concurrent disposal cannot release the page-directory pin while a read is in flight.
- [~ T] Exact-head FLD build, blob catalog/page crash cuts, pending-write resume, corrupt/missing page, same-ID conflict, reader/delete pin, cross-namespace Run ID, stage restart/collision, compaction pin and directory-fsync fault coverage remain deferred. No build/test result is claimed.
- [~ C] Remaining physical-storage C-pass: audit blob catalog/page failure ordering and restart cleanup end-to-end; verify every Run publication path uses the shared allocator/blob/mirror contract; finish generic Storage API boundary and public API/source/byte audit. WAL itself remains Database-owned in this migration step; do not move it into generic Storage without a separate bounded migration decision.
- [~ C] Physical receipt, ChangeId and tombstone/Ghost GC remain disabled until the independent v3.1 reference-horizon proofs and exact-head T-pass.

## 1.29. C-pass: make FLD provider snapshot generations directory-durable — 2026-11-09

- [x C] `OpenProviderSnapshot` now creates the generation directory through `StorageDirectorySync.CreateDirectoryDurable`, flushing newly created parent entries on Linux/macOS.
- [x C] Overlay Run bytes and temporary snapshot manifest files are flushed to disk and their containing directory entries are synced; the final manifest rename is followed by a directory fsync. A snapshot token is therefore not returned before its durable manifest publication completes.
- [x C] Snapshot release and stale-generation pruning flush the snapshot-root directory after deleting generation directories. A failed Unix directory sync propagates instead of being silently ignored.
- [x C] Exposed the domain-neutral `StorageDirectorySync` helper as a documented public API so the separate `Fld1.Database.dll` can share the exact same Unix durability primitive without duplicating native P/Invoke logic. FLD open-failure cleanup now releases the root writer lease even if disposing the generic Storage database itself throws.
- [x C] `CreateDirectoryDurable` retains parent-entry paths whose fsync failed and retries them on the next call, even though the directory now exists in-process. This prevents a retry from mistaking an incompletely durable mkdir for a durable path.
- [~ T] Verify process-kill/reopen at every boundary: generation mkdir, overlay file, temp manifest, final rename, release and prune; verify Windows remains explicitly outside the directory-entry durability claim. No exact-head build/test result is claimed.
- [~ C] Continue the full Run/blob/snapshot pin audit, generic Storage API boundary and final v3/v3.1 source/byte audit. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.30. C-pass: make persisted provider snapshots consume the generic Run backend — 2026-11-09

- [x C] `RestoreProviderSnapshot` now prefers the generic immutable Run blob and validates the reconstructed FLD1 Run; the legacy `.fld1` mirror remains a fallback only when no completed blob is published.
- [x C] Durable snapshot-token checks now require each pinned Run to have a readable completed blob or a legacy mirror only when no blob catalog row exists. A pending/corrupt blob is never hidden by a compatibility mirror. The watermark-binding path streams each blob to `Stream.Null` to verify every page and the whole-blob SHA-256 without materializing the Run; legacy fallback Runs are fully decoded/validated at this boundary. The cheap presence-only check does not perform a redundant full read.
- [x C] Normal Run loading now attempts one catalog snapshot first and opens a second only on the legacy/pending fallback path, avoiding duplicate Storage snapshot creation on the common blob-backed read path.
- [~ T] Verify snapshot reopen with blob-only, mirror-only and mixed Run v1/v2 generations; missing page, wrong digest, pending catalog row, corrupt mirror fallback and concurrent release while checking the token. No exact-head build/test result is claimed.
- [~ C] Continue the final source/API/byte audit and exact T-pass for blob-backed provider snapshot recovery. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.31. C-pass: harden blob metadata arithmetic and reader lifetime — 2026-11-09

- [x C] Blob metadata page-count validation now uses quotient/remainder rather than `length + pageSize - 1`, preventing unsigned overflow from making a corrupt near-`UInt64.MaxValue` length appear to have zero pages.
- [x C] Whole-blob streaming checks the exact copied byte count in addition to SHA-256; malformed length/page geometry fails closed before a valid reader can be constructed.
- [x C] Reader page access, full streaming/materialization and disposal share a reader gate. Disposal cannot release the page-directory pin while a read operation is in flight; independent readers remain concurrent.
- [x C] The public directory-sync helper validates its path argument and documents the platform boundary.
- [x C] `StorageDatabase.Open` now normalizes the partition directory to an absolute path before opening WAL/catalog files; `DirectoryPath` is stable even if the process working directory later changes.
- [~ T] Add malformed metadata vectors around `UInt64.MaxValue`, page-count boundary, zero-length blob, simultaneous `CopyTo`/`Dispose`, concurrent independent readers and deletion while pinned. No exact-head build/test result is claimed.
- [~ C] Continue final public API/source/byte audit and exact-head T-pass. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.32. C-pass: reclaim provider snapshot generations that never published a manifest — 2026-11-09

- [x C] Audit rejected automatic deletion of GUID-named snapshot directories without a final `snapshot.manifest`: after a crash, absence can mean either an unpublished generation or loss/corruption of a previously durable pin manifest. FLD's pin scan now returns an indeterminate result for this case, and Run/blob orphan GC fails closed.
- [x C] `PruneProviderSnapshotsExcept` is the only bulk cleanup path for directories whose sender liveness must be reconciled; it may run only after the host supplies the complete durable sender-generation set. Explicit `ReleaseProviderSnapshot` may still delete its exact token after the sender's durable release ordering. Both paths flush the snapshot-root directory on Linux/macOS.
- [x C] Valid manifests are parsed, their generation is checked against the containing directory name, and their Run IDs are retained; malformed names, generation mismatches and corrupt/missing manifests never become implicit permission to delete Runs. Windows directory-entry durability remains unclaimed.
- [x C] Created current recovery points before further C-pass work: BIN-SYNC `backup/cpass-2026-11-09-storage-blob-roadmap` at `998a41d51e3b64dbf40735b5bdc3efa4f1c1c1c1` and FLD-DB `backup/cpass-2026-11-09-storage-blob` at `f4f850a6ca35a998a5625482ec83f70946b8a6f2`.
- [~ T] Add crash cuts before/after final manifest rename, restart cleanup of temp-only generations, preservation of valid token generations, corrupt-manifest fail-closed and interaction with sender progress pruning. No exact-head build/test result is claimed.
- [~ C] Continue final source/API/byte audit and exact-head T-pass. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

## 1.33. C-pass: preserve immutable blob geometry across Storage configuration changes — 2026-11-09

- [x C] The configured page size now applies only when creating a new blob. Existing pending/complete catalog rows retain their persisted page size for resume, verification and reads, so a later process can change its default page size without making older blobs unreadable.
- [x C] Public reader API documentation now distinguishes per-page length validation from whole-blob SHA-256 verification and warns that a streaming destination can contain partial bytes if I/O or final integrity validation fails.
- [x C] Created refreshed paired recovery points after page-geometry and durability hardening: BIN-SYNC `backup/cpass-2026-11-09-storage-roadmap-v2` at `d58cd806aeadad4cd40e35720de3a2d938998410` and FLD-DB `backup/cpass-2026-11-09-storage-blob-v2` at `79e384e67f3bcf06024a759e0b4bfd7dc2c62d4f`.
- [x C] Created the latest paired checkpoint for the final Storage API/source audit: both repositories now have `backup/cpass-2026-11-09-storage-final-audit`, anchored at the current FLD-DB and BIN-SYNC branch heads respectively.
- [x C] Immutable identity is the blob length plus SHA-256 content digest; reusing the ID with identical bytes is idempotent even if the caller's current default page size differs. Existing page files are verified using their original durable geometry.
- [x C] `ContainsBlob` and `TryOpenRead` no longer reject valid generations merely because the current writer's preferred page size changed.
- [x C] Blob page file contents are flushed individually, but directory entries are now fsynced once per completed blob generation rather than once per page. The pending catalog row remains authoritative until that single directory flush succeeds, so interrupted writes remain resumable while avoiding thousands of redundant directory fsyncs.
- [~ T] Verify reopen/read/resume with page sizes 4 KiB, 1 MiB and 4 MiB across process versions, same-ID same-content different geometry, same-ID different-content rejection and partial pending-write recovery. No exact-head build/test result is claimed.
- [~ C] Continue final public API/source/byte audit and exact-head T-pass. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.
