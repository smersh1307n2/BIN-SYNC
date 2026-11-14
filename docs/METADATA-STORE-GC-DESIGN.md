> **Normative precedence (2026-11-14):** BIN-SYNC-SPECIFICATION.txt v3.0.0 remains the base wire contract; the mandatory v3.1 signed namespace-roster, retention-checkpoint and FULL_REPLACE addendum governs the v3.1 GC control plane. This file is an implementation/design note, not an alternate wire specification. Historical v2.x layouts, migration readers and TTL-deletion suggestions are non-normative and must not be implemented.

# BIN-SYNC Metadata GC: safe horizons and implementation plan

**Status (2026-11-14):** v3.1 signed roster/frontier/checkpoint, FULL_REPLACE and async Host reference-scan paths are implemented. Receipt, ChangeId and tombstone GC are implemented behind independent feature flags that default false; Ghost GC remains disabled. This is implementation/test readiness, not production authorization. No time-based deletion is authorized.

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

## 4. Tombstone/ghost GC: tombstone path implemented, Ghost remains disabled

The asynchronous tombstone path is implemented through `AsyncStorageSyncCoordinator.CompactTombstonesAsync` and the FLD Host's `IAsyncProviderTombstoneGc` boundary. The coordinator requires a valid signed roster/checkpoint and FULL_REPLACE generation, validates candidate provenance/HLC against the safe origin frontier, and excludes Metadata live-record references and immutable reservations.

The Host performs the final exact-candidate revalidation under its database commit monitor. It seals the committed WAL prefix before opening the final reference snapshot, then checks draft horizon, Outbox/ApplyJournal references, relay references and active/persisted snapshot pins before deleting a candidate. The Host GC lease excludes competing IPC sessions across the scan/commit window; disconnect, expiry, stale Host generation or incomplete reference evidence fails closed.

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
- Ghost lifecycle has its own proof and remains disabled until separately validated.

### Remaining crash/enablement gate

- Host death during reference scan, WAL seal, provider deletion and Metadata commit;
- uncertain IPC completion and idempotent retry without inferring success from a broken channel;
- repeated exact-HEAD CI, independent flag review, diagnostics, retention policy and operational disable/rollback path.

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


The v3.1 roster/frontier/checkpoint and FULL_REPLACE implementation is now present and covered by exact-head CI. This supersedes the historical 2026-11-08 audit below wherever it says the signed roster or GC paths do not yet exist. The remaining blockers are the complete Host-death/uncertain-IPC crash matrix and staged enablement review. All physical GC feature flags remain false by default.


## 9. Current implementation and composed integration evidence — 2026-11-14

- Receipt, ChangeId and tombstone compaction paths are implemented behind independent default-off flags. Ghost GC remains disabled.
- The two-Host lifecycle test now composes signed retirement/rejoin, FULL_REPLACE baseline, old/new transport Receipt handling, the unsealed-WAL receipt barrier, bounded receipt pages, ChangeId deletion, physical tombstone pages, Host restart, and pushed-reservation recovery after Metadata Storage reopen.
- The composed test intentionally injects the retiring-epoch receipt chain and the lost-ACK reservation at the Metadata boundary to isolate those durable state machines. The two FLD Hosts and their A→B / B→relay SyncSession data/tombstone path are real. Separate real-Host tests continue to cover ApplyJournal/WAL receipt references, Host scan/lease writer exclusion, heartbeat drain, cancellation and snapshot pins.
- Exact BIN-SYNC code HEAD `7a26828beafcb1e92198d8f997e08256070d05b5`: BinSync.Net workflow run `43609820952` passed all six jobs; Linux/Windows .NET tests each passed 686/686, Linux line coverage was 68.7%, and FLD Host integration passed 34/34 on both Linux and Windows. Signed v3.1 vectors and Fuzz + Lab passed.
- Exact same code HEAD: FLD Storage Integration run `43609820962` passed all three durable-provider jobs on Ubuntu, Windows and macOS 15.
- These results validate this code HEAD; they do not authorize production GC. Complete the remaining Host-death/uncertain-IPC matrix, repeat exact-head CI, and review disable/rollback, diagnostics and retention policy before considering any single GC flag for opt-in.
