# BIN-SYNC Metadata Store retention and GC contract

**Status (2026-11-15):** Receipt, ChangeId and field/record-tombstone compaction paths are implemented with independent proof gates and independent default-off feature flags. The signed v3.1 roster/frontier/checkpoint/FULL_REPLACE control plane, process-isolated FLD IPC reference-scan/GC lease, async per-domain readiness and bounded maintenance round are implemented and exact-head validated on BIN-SYNC PR #16 HEAD `d00501c4a19436c07e74c945ff460576c159a430`. Async Host scans expose active immutable snapshot pins and all three domains fail closed when a pin exists. All physical GC flags remain **false by default**; this document does not authorize production reclamation. Remaining release gates: Metadata writer-bypass closure, provider-specific crash/replay matrices, representative large-namespace performance/retention measurements, operator rollback and staged canary.

Normative detail: [METADATA-STORE-GC-DESIGN.md](METADATA-STORE-GC-DESIGN.md), [BIN-SYNC-ROADMAP-v3.md](BIN-SYNC-ROADMAP-v3.md), and [BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md](BIN-SYNC-v3.1-ROSTER-GC-ADDENDUM.md).

## 1. Retention domains are independent

ACK, snapshot completion, peer disconnection, HLC age or one generic safe cursor is never a universal retention horizon.

| State | Current lifecycle | Required proof / reason |
|---|---|---|
| Outbound payload (local author stream) | Compact only through the minimum safe per-peer stream cursor, with required peer baselines complete; also respect a durable local lease-consumer ACK when present. | Peer ACK does not prove a local transport consumer has finished, and vice versa. |
| Relay payload (original author stream) | Compact only through the minimum safe downstream peer cursor and relay-consumer ACK where present. | Relay retention is tied to downstream delivery, not local outbound ACK. |
| Per-peer stream state | May be removed atomically when a transport epoch is retired; retain the maximum retired epoch as a durable fence. | The fence rejects stale sessions after stream rows are removed. |
| Outbound/relay high-watermarks, ACKs and receive cursors/digest chains | Retain after payload/receipt compaction. | Sequence continuity, retry identity and replay rejection must survive payload removal. |
| Inbox batch receipts | Receipt GC is implemented behind the signed roster/frontier proof, durable transport-epoch retirement fence, receive-cursor/digest-chain checks, no-PendingApply check and Host reference scan. Default off. | A lost ACK may require a receipt for exact retry. A retired transport fence and signed origin horizon must make stale replay rejectable after deletion. |
| ChangeId identity/provenance ledger | ChangeId GC is implemented behind the signed safe-origin frontier and internal + Host reference scans. Default off. | (namespace, ChangeId) remains route-independent dedup/equivocation evidence; the immutable origin tuple and signed replay fence survive row deletion. |
| PendingApply | Delete atomically when provider application is finalized; retain and replay after interrupted provider application. | It is a durable cross-store recovery intent, not a historical receipt archive. |
| CompletedPendingApply | Retain as the idempotency mapping from provider apply token to finalized digest/receipt; no independent GC horizon is authorized here. | It makes repeated finalization with the same token idempotent and rejects token reuse with different input. |
| Field/record tombstones | FLD exposes separate field-tombstone and record-tombstone candidates. Both require signed origin-frontier + installed FULL_REPLACE proof, Host reference scan and final exact-candidate revalidation. Field tombstones are retained while an unproven record deletion could expose an older value. Default off. | A stale or offline peer must not resurrect deleted data or reveal a superseded field value. |
| Ghost lifecycle facts | Retain; Ghost GC is not implemented/enabled. | Ghost reclamation requires its own lifecycle and anti-resurrection proof. |

## 2. Payload compaction rules

CompactOutboundThrough and CompactRelayThrough are delivery-payload compaction entry points, not ChangeId or tombstone GC.

- The requested sequence cannot exceed the durable ACK of an active local consumer.
- It cannot exceed GetSafePeerCursor, the minimum AckedCursor + StreamOffset across all interested peers and required author streams.
- A peer without a committed initial-sync baseline contributes safe cursor zero.
- If there is no local consumer and no peer requirement, a positive compaction request is rejected. No consumer is not proof that delivery is unnecessary.
- Payload deletion is committed in Storage before the in-memory projection changes.
- High-watermarks, ACK state, receive cursors/digest chains and ChangeId identity rows are not deleted by payload compaction.

Retiring a transport peer epoch requires a newer peer epoch and a committed initial-sync baseline. It removes old peer-stream state and eligible transport receipts atomically while persisting the maximum retired epoch. A later connection using that epoch or an older one is rejected. Epoch-unknown legacy receipts are never assigned an origin epoch by guesswork.

## 3. Receipt GC

Receipt reclamation is independent of ChangeId and tombstone reclamation. A receipt is eligible only when all applicable conditions hold:

1. Its authenticated transport-peer epoch is durably retired/fenced.
2. The accepted signed namespace roster and retention checkpoint cover the receipt's immutable origin coordinates.
3. The durable receive cursor covers CursorEnd, and the sender digest-chain anchor remains retained.
4. No matching PendingApply or unresolved provider application intent remains.
5. The FLD Host reference scan reports no live receipt reference; an unsealed committed WAL prefix is a positive/unknown reference and blocks reclamation.
6. The bounded page commit and its cursor/checkpoint are durable before the caller may treat the result as complete.

The receive cursor, digest-chain row and retired-epoch fence remain after receipt deletion. Old-epoch replay is rejected by the transport fence; replay below the signed origin frontier is rejected even if a batch-level receipt has been pruned. Active-epoch receipts and provenance-less legacy rows remain until their own proof exists.

## 4. ChangeId GC

The global ledger remains one shared row per (namespaceId, ChangeId); do not create per-peer duplicate ChangeId history.

A ChangeId row is eligible only when:

- its immutable (originAuthorNodeId, originAuthorEpoch, originSequence) is covered by the signed safe frontier (minimum contiguous applied frontier across every Active member in the accepted roster generation);
- missing, offline, unknown, joining, disconnected or lagging members have contributed zero, unless a signed roster generation and durable epoch fence have retired them;
- no PendingApply, local provider publication intent, relay/outbound payload, immutable pushed reservation, active feed lease, snapshot/apply pin or other Metadata reference remains;
- the Host-owned IApplicationGcReferenceScan reports no external FLD/WAL/Outbox/ApplyJournal reference;
- the bounded deletion and retention checkpoint are persisted consistently, so replay below the committed frontier is rejected after the identity row is gone.

A pushed outbound reservation survives Metadata Storage reopen after a lost ACK. Retry returns the same immutable reservation and only the matching ACK releases it. A transport ACK, sender cursor, relay cursor, WAL sequence, FLD JournalSeq or OutboxSeq alone never authorizes ChangeId deletion.

## 5. Field/record tombstone GC and Ghost retention

Field/record tombstone reclamation additionally requires:

- every Active signed-roster member has installed the exact authoritative FULL_REPLACE generation/digest (or a later proven generation);
- the tombstone's origin is covered by the signed safe frontier;
- no PendingApply, provider intent, relay payload, immutable reservation, snapshot/apply pin or other Metadata reference can require the tombstone;
- the Host has sealed the committed WAL prefix, opened one immutable reference scan, and revalidated exact candidate versions under the FLD commit monitor immediately before deletion;
- bounded deletion is durable and restart-safe.

Field and record tombstones are distinct exact candidate kinds. Each field tombstone must pass its own origin/frontier proof and reference checks; a record tombstone proof does not automatically authorize every field tombstone. If a record-level Dead tombstone is present but not safe to reclaim, FLD retains all field tombstones for that record so a later undelete cannot reveal older values. Once the record deletion is safe, FLD drops older fields and preserves only fields newer than the record deletion.

The Host scanner treats an unsealed WAL prefix conservatively as a non-zero draft/reference horizon. The cross-process lease/heartbeat excludes competing IPC writers during the final scan/commit window. Cancellation, Host death and restart must release process-local leases and preserve durable progress.

Ghost GC remains disabled. It is not covered by the record-tombstone flag and must not be inferred safe from tombstone eligibility.

## 6. Non-negotiable retention rules

- No TTL, LRU, size cap, HLC-age or wall-clock-only deletion for receipts, ChangeId, tombstones or ghosts.
- No coupling of ChangeId deletion to outbound/relay payload ACK.
- No GC of provenance-less legacy records.
- No reclamation while PendingApply or an immutable outbound reservation references the state.
- Missing roster members/checkpoints/frontiers contribute zero.
- A retired member that rejoins must install the current authoritative FULL_REPLACE generation before reactivation.
- Signatures authenticate claims under the pinned-authority/crash-fault model; they do not provide Byzantine quorum safety.
- Do not advertise bounded total metadata size until all GC domains are separately enabled, measured and observed under realistic peer/operation cardinalities.

## 7. Crash and uncertain-completion evidence

The exact validated code HEAD is 93fed52445a5328ef4340916897aa190a90accf7.

- Host A is killed while a GC reference scan/lease is active and a second IPC session is blocked. The blocked request fails; after Host restart, startup recovery and a fresh non-owner request succeed.
- A committed tombstone is WAL-sealed, the database is closed/reopened before deletion, and the tombstone remains while the unsealed-WAL Receipt barrier is cleared. A later bounded page safely reclaims it.
- A bounded ChangeId-GC Metadata page commits, then its caller-visible result is lost. The persisted cursor and deletion survive reopen; compaction resumes without repeating or resurrecting the page.
- Existing Host-death cuts before candidate enumeration and after durable provider deletion remain green, as do cancellation, heartbeat, snapshot-pin, lease-expiry and writer-exclusion tests.
- BinSync.Net runs 43612231242 and 43612238417 passed all six jobs each. Linux/Windows .NET suites passed 686/686; FLD Host tests passed 35/35 on both Linux and Windows; signed v3.1 vectors and Fuzz + Lab passed; Linux line coverage was 68.7%.
- FLD Storage Integration runs 43612231211 and 43612238406 passed all three durable-provider jobs on Ubuntu, Windows and macOS 15.

The WAL-seal test closes/reopens after a completed durable checkpoint and before deletion; lower-level torn-WAL/checkpoint recovery remains covered by FLD WAL/Engine crash tests. This evidence validates the code checkpoint, not production authorization.

## 8. Feature flags, rollback and release gates

The three physical GC domains are independently gated:

- EnableReceiptGc
- EnableChangeIdGc
- EnableTombstoneGc

All default to false. The options are supplied when constructing the namespace GC store/coordinator; there is no implicit global enablement. Ghost GC remains disabled.

For rollback, the embedding Host must stop scheduling new passes, cancel any in-flight pass through its cancellation token, and restart/recreate the GC store/coordinator with all three flags false. A completed deletion is not undone; rollback means stopping further reclamation, not restoring an older Metadata snapshot. The Host application owns scheduling and transport lifecycle.

**Multiple release gates remain before a controlled opt-in:** close the Metadata writer-bypass boundary; complete provider-specific crash/replay and representative scale measurements; finalize independent flag configuration, operator-visible blocked-reason handling, disable/rollback and retention policy; then perform a staged canary. The async readiness report is implemented but is not an authorization to delete. Do not enable any physical GC flag as part of this documentation update.

## 9. Node-owned mode and provider cutover

Pure standalone BIN-SYNC Core operation without durable provider/Metadata Store remains supported, including volatile/in-memory use. It is not the migration bridge and is not a compatibility promise for the removed legacy public TcpSyncSession API.

For a provider being migrated to Metadata Store authority, the pre-cutover Node-Outbox path is a temporary drain/freeze bridge. After the durable provider cutover marker is committed, Metadata Store is the sole outbound/receive metadata authority for that namespace; Node write, publication and legacy receive paths are fenced, and startup reinstalls the fence. There is no fallback from a committed cutover to Node-owned authority.
