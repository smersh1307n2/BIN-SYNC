# Production status — BinSync.Net vs BIN-SYNC CORE

## Current continuation checkpoint — 2026-11-09

- BIN-SYNC `feature/fld-external-storage` latest C-pass source head: `73571b8210bf67ad7e53ba6f29bf6aa4c1056043`; latest roadmap commit: `aa1f10080ce845424697d51cff8be247548a203b`. The optional FLD host provider now implements the full `IProviderFullReplace` receiver capability: durable staged pages, canonical digest re-read/verification and atomic FLD namespace generation switch. Durable sender-side FULL_REPLACE snapshot persistence/restart recovery is also implemented. This is C-pass source, not a validated release.
- FLD-DB `fix/crash-consistency-outbox` latest FLD C-pass branch head: `f4f850a6ca35a998a5625482ec83f70946b8a6f2`; current storage implementation includes the generic immutable blob/page backend and FLD Run/FULL_REPLACE integration. FLD provides namespace-scoped checksummed FULL_REPLACE stage state, write-through/fsync publication, immutable Run pages, bounded resumable abort, pre-switch Run revalidation and an atomic per-namespace manifest switch; restart recovery protects incomplete stages and old provider snapshot pins.
- **C-pass continuation update (2026-11-09):** code was advanced without running the T-pass, per the requested sequencing. The previous exact CI evidence remains red (Core 672/678; Storage Integration 91/171) and does not validate the new commits. The sender snapshot persistence/recovery and FLD receiver staging/switch slices have not been built or tested. No exact-head build/test result is claimed for these changes.
- Latest completed BIN-SYNC CI evidence remains for code head `d4ad36fe4fc1a3660c43cc4209a71f400ed224fe`: Build passed; v3.1 golden vectors passed; Fuzz/Lab passed; Core tests 672/678 with 6 stale OFR1/RCP1/record-tombstone fixtures; Storage Integration 91/171 with 80 stale provider/provenance/guard fixtures across Ubuntu/macOS/Windows. Production validation remains strict.
- Cross-repository FLD host compile was skipped because `FLD_DB_READ_TOKEN` was empty; it is not counted as a build success.
- Code changes after `d4ad36f` (`d0b301b`, `545041f`, `10a4e17`, `b211979`, `9478589`, `7a8c7f2`, `7e763f0`) have not yet had exact-head CI. Earlier results do not validate current HEAD.
- Implemented source includes source-bound FRA2 HLC confirmation, prepared FULL_REPLACE transfer leases, Joining promotion HLC equality and symmetric v3.1 capability dependencies.
- **Remaining C-pass:** generic Storage blob/page failure-ordering and restart-cleanup audit, final public API/source/byte audit and performance comparison before any legacy mirror removal. Physical-format Option B is accepted: Storage owns opaque immutable blob pages plus a WAL-backed catalog; Database retains FLD1 Run v1/v2, manifest, field semantics and FLD WAL. FLD checkpoint/compaction/FULL_REPLACE use blob publication with compatibility mirrors and a root-global collision-safe Run ID allocator. FLD provider bridge, durable sender snapshot recovery and receiver staging/switch are implemented in source and await T-pass proof. The optional sample now schedules per-session send/receive operations; transport creation/reconnect/disposal remain application-owned.
- **Next T-pass:** exact-head cross-repository build; FLD FULL_REPLACE page/digest/restart/atomic-switch/abort crash matrix; HELLO capability matrix and frame capability matrix; FRA2/Joining floor mismatches; concurrent snapshot disposal; fanout/retry/restart; authority/member/rejoin; stale Core/Storage fixture migration; crash cuts and three independent GC horizons. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.

---

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
| Public examples | samples | three pre-v3.1 samples were covered by the historical verified baseline; current branch has five executable samples including `RelayProvenanceMap`, which are not yet built/run on the C-pass head |
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
- **FLD APPLY/relay:** APPLY_COMMIT v3 single-encoding, relay-only ApplyJournal, per-author JournalSeq ACK sidecar and post-WAL-seal relay payload compaction are implemented on the FLD branch. The cross-system JournalSeq → ChangeId → OutboxSeq map, startup publication gate, dual-mode sender ceilings, safe ACK translation, FLD ACK-before-RPM3-prune and immutable reference-snapshot adapter now have concrete host-side source in `samples/FldBinSyncHost`. Exact-head sample build, crash/restart proof, receipt GC and bounded/rebuildable journal indexing remain open.
- **Two-DLL FLD boundary:** architecture is decided and source-consolidated (`Fld1.Storage.dll` + `Fld1.Database.dll`, one-way Database → Storage). The optional host sample can inject the same canonical `Fld1StorageProject` into both projects. Storage-only physical conformance and the generic ordered page/run backend remain open; no FLD1 physical-format migration is authorized without the §7 A/B decision.
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

- Code/test HEAD: `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9` (647/647 core, 107/107 Storage Integration on Ubuntu/Windows/macOS, 31/31 Fuzz/Lab, 70.8% coverage; all samples pass). The base `IMetadataStore` contract remains source-compatible: outbound ChangeId lookup is the optional `IMetadataOutboundChangeIdentityStore` capability.
- Added optional `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeFingerprint` backed by the durable global ChangeId ledger. Storage and volatile conformance adapters implement the lookup; Storage keeps the identity available after payload compaction without breaking third-party `IMetadataStore` implementations.
- The lookup now returns a `ReadOnlyMemory<byte>` view over the immutable ledger fingerprint. The coordinator converts that view to `Digest16` and compares it to a `Digest16` recomputed from the provider mutation, avoiding both 16-byte array materializations on the already-frozen retry path. The direct lookup benchmark measures 0 B allocated per lookup at 256, 1,024 and 4,096 ledger rows. The full coordinator retry benchmark now measures 2.055/14.324/34.651 ms and 1.52/6.47/33.92 MB at 256/1,024/4,096 rows; this includes full provider snapshot rehydration, not just the scan.
- Fixed interrupted-publication retry: if Metadata Store freeze commits but provider feed ACK fails, a newer edit may arrive before retry. The coordinator recognizes already-frozen ChangeIds, validates fingerprints, preserves immutable published operations and coalesces only the not-yet-frozen suffix.
- Cutover is explicit and durable: provider feed empty/acknowledged, Node and Metadata Store high-watermarks aligned, provider marker persisted. Metadata Store assigns new outbound sequences; Node's local author sequence is fenced.
- After the marker is durable, duplicate local-author Outbox payload rows are removed from Node. The high-watermark and WAL/ChangeId replay identities remain. Startup reconciliation repeats cleanup if it loads a stale pre-cleanup Node checkpoint.
- `AutoReconcileWithMetadataAsync` detects provider-authoritative mode and rehydrates the Node namespace cache from the provider snapshot before anti-entropy. The legacy merge-based path remains for non-cutover providers.
- Added recovery tests for provider ACK interruption with a later edit, post-ACK checkpoint failure/reopen, stale checkpoint replacement, local Outbox payload cleanup and missing cutover-marker fail-closed behavior.
- Added a process-local shared transaction gate across provider, metadata and Node checkpoint adapters attached to one StorageDatabase. Each WAL partition now holds a named OS-managed Mutex before snapshot/WAL recovery. A single process-wide owner thread acquires/releases mutexes so StorageDatabase can be disposed from any caller thread; the OS releases/abandons the mutex on process termination and the next opener handles AbandonedMutexException before WAL recovery. Canonical path resolution and process-local name tracking reject same-process, symlink and case aliases. Regressions cover second-process rejection, abrupt owner-process termination, cross-thread disposal and path aliases. The Storage API remains BCL-only.
- Verified GitHub Actions on this code head: BinSync.Net 644/644; Storage Integration 107/107 on Ubuntu, Windows and macOS; Fuzz + Lab 31/31; line coverage 70.8%; in-process and real-TCP Storage samples PASS on all three OSes.
- PR #16 remains Draft, open and unmerged. No merge was performed.

**Remaining:** complete runtime-wide Node/Transport authority removal (legacy Node-backed transport and other-author relay Outbox state remain), deterministic physical Storage crash injection/power-loss cuts, controlled large-partition and end-to-end retry-scan benchmarks, independent receipt/ChangeId/tombstone GC horizons, and normative cross-implementation conformance. FLD Database and its JournalSeq provenance adapter remain deferred. The direct frozen-ChangeId lookup now has a zero-allocation scaling benchmark; this does not yet measure the complete retry scan.


## Latest continuation checkpoint — complete coordinator receive process-kill sequence (2026-11-08)

- Code/test HEAD: 354eab38dc859e2f26838efb332c14310aa17022.
- The experimental file-share WAL lease was reverted after a cross-process lock/test stall. The current single-writer fence is a named OS-managed Mutex, acquired/released on its dedicated process-wide manager thread.
- Cross-process tests target the actual testhost PID and have unconditional child-process cleanup with bounded waits. Storage Integration uses --blame-hang-timeout 3m --blame-hang-dump-type mini to capture diagnostics for any future hang.
- Added actual process-kill/reopen coverage at all four receive durability boundaries: after PendingApply staging before Node apply; after Node checkpoint before provider apply; after provider commit before Metadata Store finalization; and after Metadata Store finalization before network ACK. The final cut verifies the durable receive cursor/receipt remains committed and no PendingApply remains even though ACK was not sent.
- Recovery requires the negotiated alias table and peer epoch in the Node checkpoint. StorageNodePersistence.Save is explicitly documented and demonstrated in both Storage samples immediately after each successful SyncSession handshake and before the first inbound durable batch.
- Windows process-kill recovery retries WAL reopen for up to ten seconds to accommodate delayed OS release of the killed process's file handle.
- Verified: BIN-SYNC Core Release 648/648; Fuzz + Lab 31/31; line coverage 70.4%; Storage Integration 134/134 independently on Ubuntu, Windows and macOS-15; in-memory and real-TCP samples pass on all three platforms.
- PR #16 remains Draft/open/unmerged.

**Remaining:** true physical power-loss/I/O fault injection; retirement of the legacy Node-owned receive/transport path for non-cutover/volatile namespaces and remaining other-author relay state; receipt/dedup/tombstone GC horizons; and FLD JournalSeq provenance/atomic staging integration. The two-DLL decision remains Fld1.Storage.dll plus Fld1.Database.dll.


## Latest continuation checkpoint — Storage durability and retention policy (2026-11-08)

**Code/test head:** `00b4d26e83331495242b475afd1ef76855e3d0c7`. CI passed on this head: BinSync.Net 648/648, Fuzz + Lab 31/31, and Storage Integration 136/136 on each of Ubuntu, Windows and macOS (two successful Storage Integration runs). The Storage directory-sync helper now uses native `open`/`fsync`/`close` on Linux and macOS. WAL segment publication/removal and snapshot/manifest publication/cleanup propagate directory-sync errors; a failed rotation/checkpoint requires reopen and recovery. Windows directory-entry durability remains explicitly unclaimed. The one-million-row ChangeId-ledger benchmark completed: 389.839 µs mean, 73.89 KB allocated, 3 measured iterations; BenchmarkDotNet's short-iteration warning means this is a scaling smoke test, not a latency SLA. See [METADATA-STORE-BENCHMARKS.md](METADATA-STORE-BENCHMARKS.md).

**Retention decision:** pure Node-owned standalone Core/volatile mode remains supported; it is not the migration bridge. The migration bridge exists only before provider cutover. After the durable cutover marker, Metadata Store is exclusive authority and Node write/publication/receive paths remain fenced. Outbound/relay payload compaction remains ACK + peer-baseline/frontier gated; peer epoch retirement removes per-peer rows while retaining the retired-epoch fence. ChangeId ledger, Inbox receipts, receive-chain anchors and tombstones are not GC'd: there is no protocol proof permitting safe deletion. The separate [METADATA-STORE-RETENTION.md](METADATA-STORE-RETENTION.md) records each retention domain, exact current compaction conditions, and prerequisites for future GC.

**Still open for a release claim:** injected partial OS writes and device/fsync failures, Windows directory-entry durability, real power-loss testing, deployment-hardware/end-to-end performance, and normative requirement-ID/cross-implementation mapping. FLD Database, JournalSeq provenance integration and Shared Engine remain deferred until the BIN-SYNC + Storage contract is frozen.


## Latest continuation checkpoint — physical I/O fault boundaries (2026-11-08)

**Code/test head:** `22a89cdd7dd1538e5a4088bda03bebb25060dc09`. CI is green: BinSync.Net 648/648, Fuzz + Lab 31/31, Storage Integration 141/141 on Ubuntu, Windows and macOS; the three-platform workflow also executes both Storage sample applications. WAL writer tests now inject a genuinely torn partial record write before the record is fully handed to the stream, verify the writer/database fail closed, and verify recovery truncates the incomplete tail before sequence reuse. An injected WAL fsync failure faults the writer; reopen resolves the complete record visible to the process-level test. A separate directory-fsync failure after sealed-segment deletion faults the WAL manager and database; reopen resolves from the durable snapshot + active segment. Directory creation now flushes each parent entry on Linux/macOS, and initial WAL segment creation flushes the WAL directory before Open returns. These tests improve deterministic fault coverage but do not emulate loss of volatile device caches or prove physical power-loss behavior.


## Latest continuation checkpoint — author-stream fairness (2026-11-08)

**Code/test head:** `9848227caee54aebbba936afee094eba6e65fc3e`. CI green: BinSync.Net 649/649, Fuzz + Lab 31/31, Storage Integration 142/142 on Ubuntu, Windows and macOS. The Node-owned batch builder and the durable Metadata Store push path both select the smallest next contiguous author sequence across eligible streams; a gap stream is only selected when no contiguous stream is available. The durable transport test seeds a local stream whose next sequence is 5 and a relay stream whose next sequence is 1, then verifies the relay stream is sent and ACKed first. Scenario 23 now explicitly verifies that a first-seen relay operation remains relay-eligible even when it loses LWW on the downstream node. The E-6 and E-8 evidence gaps are closed for these paths.


## 2026-11-09 exact CI refresh

- The prior compile blocker `StorageRelayProvenanceMap.ReadAuthors → ValidateNamespaceId` was fixed in `66ae3ac50833db9f4226f33490c780176bac0b14`.
- Latest completed code-equivalent CI (run `42562536212`): Core 672/678, 6 failed (five legacy OFR1/RCP1 issued-watermark fixtures, one malformed tombstone fixture); Storage Integration 91/171, 80 failed on all three OSes (provider draft fixtures still assign OriginSequence instead of deferred zero, and test providers lack IProviderCommitGuard); Fuzz/Lab passed; coverage 68.1%.
- Latest FLD-DB CI (`42561687647`) had all jobs fail before runner allocation (`runner_id=0`, no steps, no artifacts/logs). It is not evidence of a source build failure; a real runner-backed build/test is still required.
- Physical GC remains disabled. Current source code head after the compile fix is not accepted until all relevant exact-head tests pass.


## 2026-11-09 C-pass refresh — generic Storage blob integration

- FLD-DB accepted physical-format Option B and implemented the first generic immutable blob backend: opaque Guid IDs, compact WAL-backed metadata catalog, durable page files, pending/complete publication, SHA-256 verification, stable readers and process-local reader pins.
- FLD Run checkpoint and compaction publish blob pages before manifest publication; FULL_REPLACE stages the same blob form, writes compatibility mirrors before advancing stage progress, and uses the same root-global Run ID allocator. Legacy `.fld1` mirrors remain intentionally present; this is not yet a completed physical cutover.
- Unix page-file publication now uses `StorageDirectorySync` and fails closed on directory fsync errors. Windows directory-entry durability remains unclaimed.
- Fixed a restart collision hazard: a staged Run ID may exist in the blob catalog without appearing in the active manifest. All Run allocation now checks both legacy paths and blob catalog IDs.
- Roadmap §1.28 records this C-pass. Exact-head builds, fault-injection and conformance tests were intentionally not run during this code-first pass. Previous red CI evidence remains red and does not validate the new source.
- Additional C-pass hardening: FLD legacy Run mirror publication now fails closed on Unix directory fsync errors, and durable provider snapshot generation creation/overlay/manifest publication/release/prune flush directory entries through `StorageDirectorySync`. This protects the v3.1 sender restart token and Run pins. Windows directory-entry durability remains unclaimed.

