> **Normative precedence (2026-11-14):** BIN-SYNC-SPECIFICATION.txt v3.0.0 remains the base wire contract; the mandatory v3.1 signed namespace-roster, retention-checkpoint and FULL_REPLACE addendum governs the v3.1 GC control plane. This file is an implementation/design note, not an alternate wire specification. Historical v2.x layouts, migration readers and TTL-deletion suggestions are non-normative and must not be implemented.

# BIN-SYNC Metadata GC: safe horizons and implementation plan

**Status (2026-11-15):** v3.1 signed roster/frontier/checkpoint, FULL_REPLACE, async Host reference scans, read-only per-domain readiness, bounded async maintenance and shared Metadata writer admission are implemented. The ChangeId Host-reference scan now uses bounded batches (256 IDs per IPC request) rather than one sequential Host IPC roundtrip per candidate. Exact BIN-SYNC HEAD `58060e4feabc85449a425764d2dff58255dbf4ae` passed run `43724973137` (all six jobs; 686/686 tests on Windows and Ubuntu; FLD Host adapter tests 41/41 on both platforms); durable FLD Storage Integration run `43724973227` passed Ubuntu, Windows and macOS. The 50k-candidate integration scan improved from 4.244 s to 0.758 s on Ubuntu and from 3.904 s to 0.786 s on Windows; allocations fell from 213.65 MB to 153.08 MB and from 227.67 MB to 153.09 MB, respectively. This is test-path evidence, not a BenchmarkDotNet result or production authorization. Receipt/ChangeId/field-record Tombstone GC remain behind independent default-off flags; Ghost GC remains disabled. No time-based deletion is authorized. The remaining explicit race proof is a competing Host/provider mutation held behind a real immutable reference-snapshot lease through candidate revalidation and deletion.

## 1. Retention domains must remain independent

| Domain | Current behavior | Safe horizon |
|---|---|---|
| Outbound/relay payload | Implemented compaction | Minimum durable per-peer ACK/baseline cursor, plus local consumer ACK where applicable |
| Per-peer stream rows | Removed on epoch retirement | Durable replacement epoch registration and retired-epoch marker |
| Inbox receipts | Implemented bounded GC; default off | Retired authenticated transport epoch **and** signed origin-retention proof **and** negative Host receipt-reference scan |
| Receive cursor/digest chain | Retained after receipt deletion | Must remain as continuity evidence; never reclaimed with receipt rows |
| ChangeId ledger | Implemented bounded GC; default off | Minimum signed contiguous applied-origin frontier across the committed roster, plus no Metadata/Host live reference |
| Field/record tombstones | Implemented bounded Host GC; default off | Signed origin frontier, installed FULL_REPLACE generation, no Metadata/Host reference or snapshot pin, final Host revalidation |
| Ghost lifecycle | No independent physical GC | Requires its own lifecycle proof; remains disabled |
| PendingApply | Removed on successful finalization | Atomic provider apply/finalize protocol |
| CompletedPendingApply | Retained | Separate apply-token idempotency horizon; not removed by receipt GC |

A single ACK, snapshot, HLC cutoff, disconnected peer, or one generic cursor is not a universal deletion proof. The three implemented GC domains have separate flags and separate proof/reference checks.

## 2. Inbox receipt GC: implemented signed horizon and Host barrier

Receipt identity deliberately carries two different epochs:

- `SenderEpoch` is the immutable origin-author epoch for the operation stream.
- `TransportPeerEpoch` is the authenticated sender/transport incarnation that delivered the batch.

A relay can deliver a third-party author stream, so these values must never be conflated.

A receipt is collectible only when all of the following hold:

1. The sender's old transport incarnation is durably fenced by `RetiredPeerEpochs`; a strictly newer registered transport incarnation exists or the signed roster records the member as retired.
2. The current signed namespace roster and retention checkpoint are valid, and the checkpoint covers the receipt's durable author, origin-author epoch and origin sequence.
3. The durable receive cursor and digest chain cover the receipt's cursor range. These continuity records are retained after the receipt row is deleted.
4. No unresolved PendingApply or other Metadata-owned receipt reference remains.
5. One Host-owned asynchronous reference scan reports no external receipt reference. A failed/disconnected/expired Host scan is not a negative answer.
6. The deletion is committed in a bounded page after the Host scan and final Metadata revalidation.

Retirement and receipt deletion are intentionally separate durable transitions. First, the replacement transport epoch and retired-through fence are committed and old peer-stream rows are retired. The old receipt rows may remain temporarily. A later receipt-GC pass removes only rows whose signed origin horizon and Host reference proof are complete. A crash between these steps leaves extra receipts, never an admissible old transport epoch whose replay receipt was prematurely removed.

**WAL barrier:** FLD's Host receipt-reference snapshot treats an unsealed committed WAL prefix as an uncertainty barrier for every receipt. Receipt GC must return zero until the Host has sealed that committed prefix. The first physical tombstone-GC pass performs that seal; receipt compaction can then advance in bounded pages. This is deliberate fail-closed behavior, not a retry bug.

The active replacement transport epoch's receipt is retained. After old receipt rows are reclaimed, stale old-epoch batches are rejected by the transport-epoch fence and the durable receive cursor/digest chain. If ChangeId GC later removes an operation identity below the signed frontier, a batch-level receipt alone cannot make that operation admissible again.

### Current receipt-GC limits

- Active-epoch receipts are retained; no TTL or age-based pruning is used.
- Legacy receipts without transport-epoch provenance are retained until an exact authenticated duplicate from a registered current epoch re-attributes them. No provenance is inferred.
- CompletedPendingApply tokens retain their separate idempotency role and are not deleted by receipt GC.
- The receive cursor is author-scoped; the sender digest chain is shared across author streams for one sender/epoch/namespace. Both remain durable.

## 3. ChangeId ledger GC: implemented signed frontier, default off

The Storage ChangeId identity retains semantic fingerprint and immutable origin coordinates. The signed v3.1 roster/checkpoint path now supplies the namespace-wide deletion horizon; the old per-peer ACK cursor remains delivery progress only and is never used as an origin frontier.

The asynchronous path is implemented through `StorageMetadataStore.CompactChangeLedgerAsync` and `AsyncStorageSyncCoordinator.CompactChangeLedgerAsync`. It requires:

1. A valid current signed namespace roster and retention checkpoint.
2. The candidate's immutable origin author/epoch/sequence to be covered by the minimum contiguous applied-origin frontier of every active roster member.
3. No PendingApply, local publication intent, outbound/relay payload, immutable reservation, or other Metadata reference to the ChangeId.
4. One Host-owned asynchronous reference scan that reports no FLD-side ChangeId reference.
5. Final Metadata revalidation and bounded deletion while the Host scan/lease and coordinator namespace-admission gate remain active.

Unknown, disconnected, joining, stale or lagging active members contribute zero until a signed roster transition fences them. The Host scan is fail-closed on disconnect, lease expiry, stale generation or incomplete evidence. No Metadata monitor is held across an await.

The code and integration tests cover independent Host-reference vetoes, relay payload release, pushed-reservation/lost-ACK recovery after durable Storage reopen, bounded pages, restart and stale replay below the signed frontier. `EnableChangeIdGc` still defaults to false; test fixtures opt in explicitly. Payload ACK/compaction alone never authorizes ledger deletion.

## 4. Field/record tombstone GC: both candidate kinds are distinct; Ghost remains disabled

The asynchronous tombstone path is implemented through `AsyncStorageSyncCoordinator.CompactTombstonesAsync` and the FLD Host's `IAsyncProviderTombstoneGc` boundary. The coordinator requires a valid signed roster/checkpoint and FULL_REPLACE generation, validates candidate provenance/HLC against the safe origin frontier, and excludes Metadata live-record references and immutable reservations.

The Host performs the final exact-candidate revalidation under its database commit monitor. It seals the committed WAL prefix before opening the final reference snapshot, then checks draft horizon, Outbox/ApplyJournal references, relay references and active/persisted snapshot pins before deleting a candidate. The Host GC lease excludes competing IPC sessions across the scan/commit window; disconnect, expiry, stale Host generation or incomplete reference evidence fails closed.

FLD exposes two distinct candidate kinds: a standalone field tombstone (`IsFieldTombstone`, exact RecordId + FieldId + ChangeId + origin/HLC) and a record lifecycle tombstone (`IsRecordTombstone`). They share the signed origin-frontier and installed FULL_REPLACE safety horizon, but each exact candidate is independently revalidated and reference-scanned. A field tombstone is not authorized merely because a different record tombstone is safe.

If a record-level Dead tombstone is present but not yet reclaimable, FLD retains all field tombstones for that record; removing one could reveal an older value after a later undelete. Once the record tombstone itself is proven and reclaimed, older field versions are not re-exposed, while fields newer than the record deletion remain. FLD's exact-candidate tests cover field tombstone removal without resurrection, record tombstone behavior and the record-delete fence.

The two-independent-Host integration scenario exercises signed retirement/rejoin, fresh FULL_REPLACE, real A→B and B→relay SyncSessions, bounded physical tombstone pages, Host restarts, ChangeId/Receipt horizons and reservation/ACK serialization. Candidate provenance/HLC negative tests and Host snapshot-pin/reference tests remain separate supporting matrices.

Field/record tombstone physical GC is behind `EnableTombstoneGc`, which defaults to false. Ghost reclamation is not a separately proven domain and remains disabled; do not infer Ghost safety from the tombstone tests.

## 5. Signed membership and frontier protocol: v3.1 implemented

The v3.1 addendum defines the authoritative namespace control plane. The implementation includes:

- signed `NRS2` namespace roster certificates with monotonic generation, previous digest, authority signature, member keys/status and transport epochs;
- member-signed `OFR2` applied-origin frontier reports;
- authority-signed `RCP2` retention checkpoints binding the roster generation/digest, FULL_REPLACE generation/digest and per-origin safe frontiers;
- member-signed `FRA2` FULL_REPLACE installation acknowledgements;
- mandatory v3.1 capability negotiation and the FULL_REPLACE transfer protocol;
- durable retired-origin/transport-epoch fences and rejoin at a strictly newer transport epoch.

The safe frontier is the minimum contiguous applied-origin frontier across every active member in the committed roster generation. A new roster generation invalidates old-generation checkpoint evidence until current-generation frontiers and FULL_REPLACE installation acknowledgements are re-established. A v3.0-only or otherwise incapable required member contributes zero and blocks namespace-wide destructive GC.

`InterestedPeers` remains a local transport-delivery roster; it is not a substitute for the signed namespace roster. The administrative authority does not own field writes, choose LWW winners or route data. Authority trust remains provisioned out-of-band. The normative wire/control-plane details are in [BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md](BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md).

## 6. Required acceptance tests before enabling each domain

### Receipt GC

- old transport epoch is durably fenced before its receipt rows are reclaimed;
- signed roster/checkpoint covers the receipt's immutable origin coordinates;
- unsealed Host WAL returns a positive/unknown receipt reference and blocks deletion;
- after WAL seal, bounded receipt pages delete only eligible retired-epoch rows;
- restart between pages preserves receive cursor/digest chain and resumes without an in-memory cursor;
- stale old-epoch replay is rejected after receipt deletion; current-epoch receipts remain independently protected.

### ChangeId GC

- multi-hop relay preserves origin coordinates byte-for-byte;
- same ChangeId with a different origin tuple is rejected;
- frontier is the minimum contiguous frontier across every active signed-roster member;
- application, relay, Outbox, PendingApply, immutable reservation and Host references independently block deletion;
- pushed reservation survives Metadata Storage reopen after lost ACK; retry is idempotent and matching ACK clears it;
- crash during GC checkpoint/page publication is replay-safe;
- replay below the committed frontier is rejected even if a batch-level receipt remains.

### Tombstone/ghost GC

- tombstone is retained while any active peer is below its origin frontier or any Host/Metadata reference remains;
- offline peer blocks GC until durable signed retirement and rejoin requires FULL_REPLACE;
- Host seals WAL and revalidates exact candidates under its commit monitor;
- active/persisted snapshot pins and competing IPC writers block the final Host scan/commit window;
- two independent FLD Hosts reclaim bounded pages, restart between pages and preserve deletion after durable database reopen;
- Ghost GC is out of scope for this implementation and remains disabled until its own lifecycle proof is designed and validated.

### Completed crash/uncertain-completion matrix — 2026-11-14

- Host death while a GC reference scan/lease is active and a second IPC session is blocked; the blocked request fails, the Host restarts on the same FLD database, and a fresh non-owner session plus startup recovery succeed.
- Durable WAL seal followed by database close/reopen before tombstone deletion; the tombstone remains, the unsealed-WAL Receipt barrier is cleared, and a later bounded page can reclaim it.
- Existing Host-death cuts before candidate enumeration and after durable provider deletion remain covered.
- A bounded ChangeId-GC Metadata page commits, its caller-visible result is lost, and reopen resumes from the durable cursor without repeating or resurrecting the page.
- Cancellation after a negative Host reference answer, Host lease expiry/heartbeat, snapshot pins, writer exclusion and restart between bounded pages remain covered.

### Remaining production enablement gate

- close the remaining Metadata writer-bypass boundary: coordinator admission does not fence arbitrary direct public Metadata Store mutations;
- keep the async Host readiness/maintenance API green on every exact PR head. The read-only report exposes signed roster/checkpoint/FULL_REPLACE state, draft horizon, snapshot pins, provider capability and Metadata reservation/payload counts; one bounded sequential round is tested. Readiness never authorizes deletion;
- complete operator-facing diagnostics presentation, independent feature-flag configuration, disable/rollback and retention policy;
- complete provider-specific crash/replay matrices and representative large-namespace performance/retention measurements;
- staged canary/rollout review. Exact-head cross-platform CI must remain green before each individual domain is opted in.

## 7. Non-negotiable rules

- No TTL, LRU, size cap, HLC-age or wall-clock-only deletion for receipts, ChangeId, tombstones or ghosts.
- No deletion based solely on outbound/relay payload compaction.
- No GC of provenance-less legacy records.
- No reclamation while PendingApply or an immutable outbound reservation references the state.
- No claim of bounded metadata growth until all three GC domains have proven horizons and measured storage/CPU costs.

## 8. Implementation audit — 2026-11-08 (provenance implemented; safe-frontier GC still blocked)

The earlier 2026-11-08 audit below is superseded. Immutable Origin is now implemented in the v3 wire/storage path. This closes the provenance-data blocker, but it does **not** prove a safe deletion horizon for ChangeId rows or tombstones. Both GC domains remain disabled.

### 8.1 Current code facts

- A canonical immutable `OperationOrigin` value exists: (author NodeId, author epoch, origin sequence). It is used by Node Outbox entries, MetadataOperation, PendingApply encode/decode, relay/outbound DTOs, Node checkpoint serialization, WAL replay and Storage ledger validation/encoding. The pre-freeze provider DTO is intentionally incomplete: the local author is supplied to `FreezeOutbound`, and the Metadata Store allocates the durable origin sequence atomically at freeze.
- Protocol major 3 is mandatory. HELLO and HELLO_ACK carry ProtocolMajor=3; no optional provenance capability or downgrade path exists. Every DELTA_BATCH operation carries nonzero OriginAuthorEpoch and OriginSequence; the resolved author NodeId is bound by the authenticated author stream/alias mapping.
- `Node.ApplyBatch.ValidateOriginProvenance` checks nonzero epoch/sequence and strict increase within one batch; for direct-author delivery it also rejects an origin epoch greater than the issuer epoch. Gaps inside a batch are explicitly allowed. `StorageMetadataStore.ValidateBatch` enforces one author stream and strictly increasing origin sequence within the batch. Neither path establishes a contiguous applied-origin frontier across batches.
- A relay preserves the original Origin tuple; its own relay/outbound sequence remains a separate delivery coordinate. The base 45-byte Storage ChangeId identity row stores semantic fingerprint[16], origin author[16], epoch[4], sequence[8] and localOutbound[1]. During provider-authoritative cutover the same row may carry an optional legacy Node digest[16] suffix (61 bytes total); it is preserved as a distinct alias and reclaimed with the identity row. The Node-local global ledger stores a 16-byte digest binding semantic mutation and Origin in one BLAKE3 pass.
- Stream payload rows currently store ChangeId[16] + OriginEpoch[4] + OriginSequence[8] + payloadLength[i32] + payload (32+payload bytes). The Origin tuple is already present in the global ChangeId ledger; a candidate reduction to ChangeId[16] + payloadLength[i32] + payload (20+payload bytes) can save 12 bytes per relay/outbound payload row by hydrating Origin from the in-memory ledger at partition load. This optimization is not yet implemented and must be benchmarked before acceptance.
- PendingApply uses the v3 format marker -7 and persists Origin per operation. Node checkpoint is v17; Node WAL is v3. Old checkpoint/WAL/metadata partition formats fail closed rather than migrate.
- Receipt GC has a narrower implemented proof: a retired authenticated transport-peer epoch is durably fenced in the same Storage transaction that removes receipts for that exact transport epoch and its peer-stream rows. Retirement is blocked while matching/epoch-unknown PendingApply remains unresolved. This proves receipt reclamation for retired transport incarnations only; it does not prove ChangeId or tombstone reclamation.
- The transport epoch in receipt GC is not the immutable origin author epoch. Relay delivery cursors and relay ACK watermarks are local to a relay/peer stream and cannot be compared to OriginSequence.
- `InterestedPeers` is a delivery roster, not an authoritative namespace membership roster. MEMBERSHIP remains a hint. There is no durable signed namespace roster generation, no per-member/per-author contiguous applied-origin frontier, and no signed retention checkpoint persisted before ACK.
- Snapshot state is not a list of historical PublishedOperations. It carries its own snapshot-generation provenance and field-author/HLC state; it MUST NOT synthesize ChangeId or OriginSequence for historical operations. Current snapshot merge semantics do not atomically replace a namespace and do not treat absence as deletion.

### 8.2 Required implementation sequence before ChangeId or tombstone GC

1. Implement an authoritative namespace roster certificate: namespace, monotonically increasing generation, previous/current roster digest, sorted member NodeIds and author epochs, rejoin/full-resync status, administrator identity/signature. A locally learned author list or InterestedPeers list is never sufficient.
2. Implement signed durable RetentionCheckpoint per roster member and generation: roster digest, member epoch, snapshot generation/digest, and a sorted per-origin-stream frontier map keyed by (originAuthorNodeId, originAuthorEpoch). Persist before ACK; reject missing members, stale generations, signature failures and regressive frontiers.
3. Define frontier advancement precisely. Since OriginSequence gaps are allowed in relay batches, a member's frontier is the highest **contiguous applied** origin sequence, not the maximum sequence observed. A later sequence cannot jump over a missing/failed operation. A snapshot may advance the frontier only if it is an authoritative FULL_REPLACE image with a verified per-author high-watermark and generation.
4. Persist the applied-origin frontier independently of transport receive cursors, sender cursors, relay ACKs and Outbox/Journal sequences. Those values are not interchangeable.
5. Compute the safe ChangeId horizon for each origin stream as the minimum contiguous frontier across every member of the committed roster generation. Unknown, disconnected or lagging members contribute zero. A member stops constraining the minimum only after durable retirement/fencing and mandatory FULL_REPLACE resync on rejoin.
6. Delete a ChangeId row only when its immutable origin sequence is at or below the safe frontier and no PendingApply, local publication intent, outbound/relay payload, immutable reservation or pinned snapshot references it. Persist the GC checkpoint and deletions atomically.
7. Tombstone/ghost GC additionally requires every active member to have installed a verified FULL_REPLACE generation that contains the deletion (or later state proving it). Implement staged namespace image, digest validation, atomic namespace-generation switch, reader/apply pin tracking and stale-generation fencing before removing any tombstone.
8. Add an independently tested pure safe-frontier calculator, then a crash matrix for checkpoint publication, row deletion, provider namespace switch and stale-peer rejoin. Only after these proofs may physical GC be enabled.

### 8.3 Non-negotiable invariants

- `(namespaceId, ChangeId)` remains the global route-independent dedup identity; Origin is immutable provenance, not a replacement key.
- Same ChangeId with a different semantic fingerprint or different Origin tuple is equivocation and fails closed before mutation.
- A duplicate arriving over another route does not create a second relay payload and does not overwrite the original Origin.
- Relay ACK, outbound ACK, sender cursor, receive cursor, WAL sequence, FLD JournalSeq and OutboxSeq never authorize ChangeId or tombstone deletion by themselves.
- A missing/unknown frontier contributes zero. A roster member cannot disappear from the denominator without a durable epoch fence.
- No TTL, HLC age, wall-clock cutoff, size cap or payload compaction may reclaim receipts, ChangeId rows, tombstones or ghosts without the corresponding proof.
- Physical deletion must not race PendingApply, outbound/relay reservations, provider publication intents or pinned snapshot generations.


The v3.1 roster/frontier/checkpoint and FULL_REPLACE implementation is present and covered by repeated exact-head CI. This supersedes the historical 2026-11-08 audit below wherever it says the signed roster or GC paths do not yet exist. Async readiness and bounded maintenance are implemented and validated on exact PR #16 HEAD `d00501c4a19436c07e74c945ff460576c159a430`; active Host snapshot pins block all three async domains. Multiple independent release gates remain: Metadata writer-bypass closure, provider-specific crash/replay proof, representative scale measurements, operator rollback/retention policy and staged canary. All physical GC feature flags remain false by default.


## 9. Current implementation and composed integration evidence — 2026-11-14

- Receipt, ChangeId and tombstone compaction paths are implemented behind independent default-off flags. Ghost GC remains disabled.
- The two-Host lifecycle composes signed retirement/rejoin, FULL_REPLACE baseline, old/new transport Receipt handling, the unsealed-WAL Receipt barrier, bounded Receipt pages, ChangeId deletion, physical tombstone pages, Host restart and pushed-reservation recovery after Metadata Storage reopen.
- The composed lifecycle injects the retiring-epoch Receipt chain and lost-ACK reservation at the durable Metadata boundary to isolate those state machines. The two FLD Hosts and their A→B / B→relay SyncSession data/tombstone path are real. Separate Host tests cover ApplyJournal/WAL receipt references, active scan/lease writer exclusion, heartbeat drain, cancellation and snapshot pins.
- Exact BIN-SYNC HEAD `93fed52445a5328ef4340916897aa190a90accf7`: BinSync.Net runs `43612231242` and `43612238417` both passed all six jobs. Linux and Windows .NET suites each passed 686/686; `FldBinSyncHost.Tests` passed 35/35 on both Linux and Windows; signed v3.1 vectors and Fuzz + Lab passed; Linux line coverage was 68.7%.
- Exact same HEAD: FLD Storage Integration runs `43612231211` and `43612238406` both passed all three durable-provider jobs on Ubuntu, Windows and macOS 15.
- These results validate this exact code HEAD; they do not authorize production GC.

## 10. Crash matrix closure and release gate — 2026-11-14

- [x T] Active Host scan/lease crash: the Host process is killed while the GC reference scan is open and a second IPC client is blocked. After restart, startup recovery and a new non-owner request succeed; Receipt-GC pages resume.
- [x T] WAL-seal-before-delete crash: the committed tombstone and Outbox acknowledgement are sealed, the database is closed/reopened before deletion, the tombstone remains and the Receipt uncertainty barrier is cleared. A later bounded tombstone-GC page reclaims the row.
- [x T] Metadata uncertain completion: an IOException is injected after a bounded ChangeId-GC page commits. The persisted page cursor and row deletion survive reopen; subsequent compaction resumes safely.
- [x T] Existing process-death cuts before candidate enumeration and after durable provider deletion, plus cancellation, heartbeat, snapshot pin and lease-expiry tests, remain green.
- [!] The WAL test closes/reopens after a completed checkpoint; lower-level torn-WAL/checkpoint recovery is covered by the FLD WAL/Engine crash tests. No wire-format change was made.
- [!] At the 2026-11-14 checkpoint, the remaining gates included Metadata writer-bypass closure; §11 supersedes that item. Remaining after §11: provider-specific crash/replay proof; representative large-namespace performance/retention measurements; operator-facing diagnostics presentation; independent flag configuration; disable/rollback and retention policy; staged canary. Async readiness and bounded maintenance are implemented, but do not authorize deletion. All physical GC flags remain false by default; Ghost GC remains disabled.

## 11. Shared Metadata writer admission closure — 2026-11-15

The direct public Metadata writer-bypass gate is closed at the adapter API boundary:

- `StorageMetadataWriterAdmission.For(StorageDatabase)` returns one shared admission object for every `StorageMetadataStore` and `StorageNamespaceGcStore` attached to the same exact `StorageDatabase` instance.
- Persistent public Metadata mutations enter the gate before the Metadata transaction lock. This includes receive staging/finalization, outbound freeze/ACK/lease state, peer lifecycle, provider cutover, prepared FULL_REPLACE state, baseline application, origin-frontier publication and compaction.
- Signed roster, frontier, checkpoint, FULL_REPLACE installed-ACK and baseline-report writes use the same admission object. A direct namespace-evidence update cannot race a GC proof/reference scan.
- Receipt/ChangeId compactors open the Host reference scan under the Host lease first, then acquire Metadata admission before reading Metadata proof/candidate state; both remain held through final bounded deletion. Tombstone compaction holds Metadata admission across the provider snapshot candidate scan and final provider-delete call. The tombstone commit guard holds synchronous admission plus the Metadata transaction gate while the provider performs the final delete.
- ChangeId and Receipt compaction read the signed Retention Checkpoint once per pass and use an in-memory safe-origin-frontier map for O(1) per-candidate coverage checks. Tombstone compaction reuses one immutable `TombstoneGcProofSnapshot` for all candidates. The final live-reference recheck before deletion remains in place.
- The sync gate is re-entrant on its owning thread; the async lease is thread-independent. Tests verify actual waiters, not merely that a scheduled task has not yet run.

The gate is process-local and keyed by the exact `StorageDatabase` object. FLD's exclusive partition writer fence prevents a second process from opening a competing writer handle; the FLD Host IPC lease still fences external provider/WAL/ApplyJournal mutations. The adapter-owned Metadata keyspaces must not be written through raw `StorageDatabase` batches.

This closes the direct public Metadata API writer-bypass gate. It does not close the remaining provider crash/replay, scale, or operational release gates and does not authorize physical reclamation. All three physical-GC flags remain false by default; Ghost GC remains disabled.

## 12. Composed Host GC scale and durable operator-policy reliability — 2026-11-18

Exact BIN-SYNC HEAD `ec14e0c54e510f5b19d57848fe94439f2a2192f1` passed BinSync.Net run `44206291961` and Composed Host Metadata GC Scale run `44206291946`. The scale artifact reports:

| Tombstone candidates | Allocated bytes | Elapsed |
|---:|---:|---:|
| 10,000 | 10,077,296 | 524.809 ms |
| 50,000 | 47,082,056 | 4,428.153 ms |
| 100,000 | 93,422,976 | 15,826.492 ms |
| 250,000 | 231,933,280 | 103,138.781 ms |

At 250k, the run completed 977 candidate pages/approval batches and one spool commit. Allocation stages were partition-state-load 212,280 B, peer-reservation-check 200 B, payload-count 24 B and Bloom population 1,048,640 B. The Host process peak working set reached 529,346,560 B (from 285,937,664 B before the run); the test process peak working set was 912,646,144 B. These are working-set observations, not incremental retained heap. The 50k allocation result is about 74% below the earlier 180,345,496 B composed baseline; timing is noisy and is not claimed as a stable speedup. The 250k result is a scale test, not production authorization. Keep all physical-GC flags default-off.

### Durable operator policy — remaining reliability proof

The policy and append-only audit are committed in one StorageWriteBatch. A maintenance lease is an OS file-share lock in the Storage database directory; policy updates acquire all three domain locks in stable order before the Metadata writer admission and transaction. This ordering makes policy updates wait for active domain leases and lets the OS release a dead process's lock. The policy store is tied to the exact StorageDatabase instance; FLD's exclusive writer fence prevents a second process from opening another writer handle to the same database. Cross-process lease contention is therefore the supported process boundary; concurrent policy writes are serialized through the one owning database process.

Required tests and operational rules:
- Exercise two independent policy-store instances racing the same expected generation; exactly one commit may win and append an audit event.
- Exercise a child process holding the actual domain lock path; policy update must wait, and killing the child must release the OS lock so update/reacquisition can proceed.
- Corrupt or truncate the persisted policy record; Read/Restore must throw and close the runtime gate. Corrupt audit pages must fail the audit read rather than be silently skipped.
- Offline backup is supported only after closing StorageDatabase cleanly. Restore the complete database directory as one unit; do not copy live WAL/segment files. Lease lock files are not durable lease state: an OS lock exists only while a process owns its open handle, and restored lock files do not represent a live owner.
- A database backup can roll policy generation and audit history backwards. The store has no independent anti-rollback authority. After restoring an older backup, keep physical GC disabled until an operator verifies the restored policy/audit against an external operational record and explicitly authorizes maintenance again. Do not treat a restored `MaintenanceStarted` bit as sufficient authorization for a production canary.

The operator-policy coordinator now treats the process-local runtime brake as an independent prerequisite for lease issuance. It checks the emergency-stop latch and per-domain runtime gate before acquiring the durable OS lock, then rechecks after acquisition and disposes the lease if an emergency stop won the race. `EmergencyStop` therefore prevents new lease issuance even while the durable policy still says enabled; only a new authorized `UpdateAsync` can clear the latch. The offline-backup restore test explicitly latches EmergencyStop before restoring policy, verifies no runtime/lease is reopened, and then performs a new authorized policy generation before reacquiring a lease.

Exact HEAD `f88a26c7aaff48a81b7b566295880fc374873ae6` passed BinSync.Net run `44210904551`; the targeted operator-policy Host adapter set passed 32/32. It covered independent store instances racing one expected generation, child-process lease ownership/death and policy-update unblocking, lease reacquisition, corrupt policy and latest-audit fail-closed restore, and closed-database backup/restore reauthorization. The immediately preceding full Host adapter run `44209593212` passed 67/67 Host tests plus 356/356 FLD Core/Storage/Manifest/Run/WAL/Engine tests. The workflow now skips those 356 FLD tests when the pinned FLD Storage gitlink is unchanged and narrows Host tests to the operator-policy classes when only policy files/tests/docs changed. Full FLD contracts still run on gitlink changes or manual/scheduled milestone runs.

A further process-death test now commits the policy and audit through one `StorageWriteBatch` in a child process, kills the process without disposing the database, then reopens Storage and verifies both records and their matching generation/sequence. Exact BIN-SYNC code HEAD `e2d272178a18b304c8df5443183ac95d19073ed7` passed the 33 targeted operator-policy tests on both Ubuntu run `44211960990` and Windows run `44211961182`. This includes the child-process post-commit recovery case on both operating systems.

FLD-DB branch `fix/crash-consistency-outbox` added an eight-point multi-key StorageWriteBatch crash matrix covering WAL append/flush and B+ tree data/page/manifest publication boundaries. It verifies policy-like and audit-like keys are both recovered after every post-append failure point. FLD commit `9c8dfa852058a057be0d9efab18d6166b41ef5a5` passed the targeted Storage suite 155/155 in run `44212461319`. The tested FLD Storage revision is now being pinned into BIN-SYNC; The FLD Storage crash-consistency fault matrix was also run on Windows: 30/30 `StorageCrashConsistencyTests` passed in run `44213292324` at FLD commit `4bb76f9615f3d64722b7d4a520a443d37e18886f`. The same FLD commit passed routine Ubuntu CI with 155 Storage tests and all 364 FLD Core/Storage/Manifest/Run/WAL/Engine tests green. The BIN-SYNC submodule pin is advanced to this tested FLD revision; the actual Windows power-loss/NTFS durability experiment remains open and is not inferred from process-kill tests.

### ChangeId GC allocation audit — measured reduction, no GC enablement

The composed Host adapter scale test was instrumented by phase. The original 50k ChangeId GC run at BIN-SYNC HEAD `2ebaa9eda22d32085b3ab011c5693db80392e74c` allocated 225,708,800 bytes in the test/client process and took 1,968.280 ms across 13 pages. This is cumulative allocation, not retained memory. Host process peak working set rose by about 6.9 MB; test process peak working set rose by about 30.3 MB. These are high-water marks and must not be interpreted as live heap.

Two measured optimizations were retained:

- FLD Storage commit `adb1dd0fa41d3b0eaefad53451558afc5de71a9c`: for batches above 256 operations, defer exact `Count` maintenance using the existing lazy `_liveCount = -1` behavior; sort mutations once and reuse the sorted array for WAL and B+ tree application; encode the WAL batch into one exact-sized byte array instead of sorting twice and growing a MemoryStream.
- FLD Storage commit `c0084fe72ad5a577644e3af89281aa104820fcb7` plus BIN-SYNC commit `dba0b0aa4286c3a9bc9b3aab88b277592bf3906c`: share one owned key buffer between the StorageWriteBatch dictionary key and mutation value; replace PersistedEntries hex-string keys with binary StorageKey keys and reuse owned immutable key arrays. BIN-SYNC submodule pin is `ed4c054be24b73813424e5217a651c3fd9bcc6f4`.

Exact focused run `44222282767` passed all 3 cases (1k/10k/50k) at BIN-SYNC HEAD `ed4c054be24b73813424e5217a651c3fd9bcc6f4`. At 50k it allocated 107,915,664 bytes and took 670.875 ms: 52.2% fewer cumulative allocations and 65.9% less elapsed time than the original instrumented baseline. Allocation breakdown at 50k: metadata batch staging 13,387,760 B; live-count validation 3,040 B; WAL encoding 3,852,392 B; tree mutation sorting 2,000,952 B; B+ tree COW apply/publish 47,310,512 B; metadata cache publish 648 B; candidate discovery 3,766,424 B; candidate ID materialization 6,270,912 B; Host reference IPC scan 4,325,528 B. Host process peak working set rose about 5.9 MB and test process peak working set about 22.3 MB; both are high-water marks, not retained heap.

The live-count lookup and metadata-cache key costs are now effectively removed from this path. The largest remaining measured component is B+ tree COW apply/publish (47.3 MB), followed by metadata batch staging (13.4 MB). These are the next profiling targets. No physical GC flag is enabled, and these measurements do not authorize Receipt, ChangeId or Tombstone GC.

This section records implementation/test scope only; it does not enable Receipt, ChangeId or Tombstone GC.

## 2026-11-18 — ChangeId GC storage allocation follow-up

The 50k-candidate ChangeId GC measurement now completes in **595.982 ms** with **86,840,072 B** of cumulative test-process allocations (13 bounded storage batches). Against the original instrumented baseline (225,708,800 B / 1,968.280 ms), this is **61.5% fewer allocations** and **69.7% less elapsed time**. Timing is environment-dependent; allocation totals are the more stable comparison.

The B+ tree copy-on-write allocation path was reduced from 47,310,512 B to 33,088,280 B (**30.1% lower**) through four bounded changes: reuse one key-reader delegate per ApplyBatch and skip path construction for candidate leaf lookups; read existing leaf entries through the allocation-free StorageBTreeLeafPageView while validating order during merge; encode leaf pages directly from a list range; and encode internal pages directly from a child-list range without pageChildren/separator staging arrays. The latest 50k COW phase breakdown is:

| Phase | Allocated bytes |
|---|---:|
| Find/group | 7,365,640 |
| Leaf decode/merge | 17,742,136 |
| Leaf page write | 4,062,984 |
| Internal path rewrite | 3,912,064 |
| ApplyBatch total | 33,088,280 |

Metadata batch staging fell from 13,387,760 B to 2,612,920 B (**80.5% lower**) by reserving the known StorageWriteBatch mutation count and staging immutable key/value arrays through internal no-copy methods. Public Put/Delete methods retain their defensive-copy behavior.

Other 50k phases: WAL batch encoding 3,852,416 B; tree mutation sorting 2,000,976 B; live-count validation 3,040 B; root publication 310,808 B; metadata cache publish 816 B; candidate discovery 3,766,592 B; candidate ID materialization 6,267,888 B; Host reference IPC scan 4,326,304 B. The encompassing `metadata-revalidation-and-commit` phase reports 65,275,992 B and **overlaps** the nested metadata-batch-staging, FLD storage commit/COW and metadata-cache-publish phases; do not sum it with those subphases. Subtracting the nested measured phases leaves about 18.34 MB of Host-side revalidation/materialization overhead, which is the next profiling target.

Peak working-set values remain process high-water marks, not retained heap. No physical Receipt, ChangeId, Tombstone or Ghost GC was enabled by these measurements; all physical GC flags remain default-off.

## 2026-11-18 — Host-side ChangeId GC materialization follow-up

The 50k ChangeId GC allocation total is now **69,596,416 B** in the repeated focused run (13 bounded storage batches), versus the original 225,708,800 B baseline: **69.2% fewer cumulative allocations**. The repeated run measured 554.562 ms; the first run on the same commit was 926.324 ms, confirming substantial runner timing noise. Allocation measurements were stable (69.53–69.60 MB); use allocation totals rather than a single elapsed-time sample for comparisons.

Two additional per-candidate allocation sources were removed:

- Candidate IDs are materialized directly into an exact-sized `BatchId[]`. `LedgerKey` is keyed by (namespace, ChangeId), and the candidate scan is restricted to one namespace, so `Distinct()` was redundant. Candidate-ID materialization fell from 6,267,888 B to 800,312 B (**87.2% lower**).
- The ChangeLedger partition root is now computed once per GC batch, not once per candidate. A dedicated encoder writes the fixed namespace/ChangeId suffix directly into the final storage-key array, avoiding a temporary suffix array. Approved-candidate materialization fell from 18,002,056 B to 6,404,376 B (**64.4% lower**).

Latest 50k phase values: candidate discovery 3,766,424 B; candidate-ID materialization 800,312 B; Host reference IPC scan 4,324,384 B; revalidation live-set rebuild 2,080 B; approved-candidate materialization 6,404,376 B; delete-reference array 401,560 B; metadata batch staging 2,612,920 B; B+ tree ApplyBatch 33,078,800 B. The encompassing `metadata-revalidation-total` is 53,669,552 B and includes the nested storage commit phases; it must not be summed with them. The nested `metadata-revalidation-storage-commit` is also inclusive of batch staging, FLD storage commit/COW and metadata cache publish.

All exact-head checks for BIN-SYNC commit `3fb90b1eab19327c8ad4b773b65e0a5b684dde1f` passed: ChangeId GC allocation run `44227221270` (including repeated attempt), composed Host smoke `44227221336`, FLD Storage Integration `44227221265`, and BinSync.Net `44227221397`. FLD .NET and Windows crash/recovery checks passed on the pinned Storage commit `6dfeeb0d326edfcc95af5026de3a53bf0608d77d`.

No physical Receipt/ChangeId/Tombstone/Ghost GC was enabled. Remaining high-value profiling targets are the B+ tree COW leaf merge (17.74 MB), Host reference IPC scan (4.32 MB), and approved-candidate materialization (6.40 MB). Any further change must preserve the signed retention proof, the Host reference scan, the writer-admission boundary and the existing transaction atomicity.
## 2026-11-18 — retained Metadata memory audit

The second memory audit found a larger retained-memory issue than the remaining ChangeId GC allocation phases: the decoded PartitionState retained every encoded Metadata row in PersistedEntries while also materializing the same data into typed dictionaries/lists. This duplicated payload-bearing Relay, Outbound and PendingApply data for the lifetime of each cached partition.

The PersistedEntries cache has been removed. Its four read sites now use the appropriate existing state or a targeted durable read:
- FULL_REPLACE baseline marker checks read only the requested marker from a FLD snapshot.
- Relay/Outbound watermark updates compare against the decoded ACK/high-watermark maps. The ACK map distinguishes an existing durable watermark row from a new stream whose high watermark is present but whose ACK row has not yet been written.
- DecodePartition no longer retains each source StorageEntry key/value array after decoding it into the typed state.

OpenSnapshot() previously cloned every PartitionState dictionary, including ChangeLedger, Receipts, PendingApplies and peer indexes. VolatileSnapshot reads only seven collections: Receives, Relay, RelayAck, RelayHighWatermarks, Outbound, OutboundAck and OutboundHighWatermarks. The snapshot now clones only those seven collections and shallow-copies the Relay/Outbound lists; payload arrays remain immutable and are cloned only when a page crosses the public API boundary.

The decoded partition cache was also unbounded. It is now a four-entry LRU. Evicted partitions are rehydrated from the authoritative FLD rows on the next access. The cache bound is by partition count, not by exact byte size: one exceptionally large active partition can still occupy substantial RAM, so a retained-heap/byte-budget measurement remains necessary before claiming a hard memory ceiling. A focused integration test writes six independent scopes, verifies the four-entry cap, then reads the oldest evicted scope and verifies durable rehydration.

PersistStreamWatermark no longer opens a Storage snapshot just to compare a watermark; it uses the decoded ACK/high-watermark state, preserving the distinction between an absent durable row and a persisted zero ACK.

These changes remove the duplicate encoded row-value retention and unnecessary snapshot collection copies; they do not change the durable FLD row format or enable any physical GC. Cumulative allocation totals are not a proxy for retained heap. Follow-up measurement must use a fresh process, force full GC after loading 50k/100k payload-bearing rows, and report managed retained bytes, LOH, current WorkingSet and peak WorkingSet separately. Continue to profile RelaySequenceIndexes and EncodeIncomingBatch only after this retained-memory baseline exists.
