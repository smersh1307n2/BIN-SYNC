# BIN-SYNC Metadata Store retention and GC contract

**Status:** fail-closed retention policy for the Storage-backed Metadata Store. This document records what may be reclaimed today, what is retained indefinitely, and the proofs required before adding destructive GC. It does not authorize TTL-based deletion.

## 1. Retention domains are independent

Do not treat ACK, a completed snapshot, peer disconnection, HLC age, or one generic safe cursor as a universal retention horizon. The durable state has distinct identities and purposes:

| State | Current lifecycle | Why it cannot share another domain's horizon |
|---|---|---|
| Outbound payload (local author stream) | Compact only through the minimum safe per-peer stream cursor, with required peer baselines complete; if a local lease consumer exists, also bounded by its durable ACK. | A peer ACK does not prove a local transport consumer has finished, and vice versa. |
| Relay payload (original author stream) | Compact only through the minimum safe peer cursor and relay consumer ACK where present. | Relay retention is tied to downstream delivery, not local outbound ACK. |
| Per-peer stream state | Removed atomically when a peer epoch is retired; the maximum retired epoch remains durable. | The retired-epoch fence prevents stale sessions from reappearing after their stream rows are removed. |
| Outbound/relay high-watermarks and ACKs | Retained after payload compaction. | Sequence continuity and replay cursors must survive payload removal. |
| ChangeId ledger | Retained; no GC is currently authorized. | It is route-independent (namespace, ChangeId) equivocation/dedup evidence. Current ledger values do not retain a provable author-sequence frontier for every route. |
| Inbox receipts (sender, epoch, namespace, BatchId) | Retained; no expiry/GC is currently authorized. | Duplicate/equivocation validation depends on the exact batch identity and canonical header proof. No bilateral receipt-retention checkpoint exists in the protocol. |
| Receive cursors and digest-chain checkpoints | Retained. | They are the contiguous per-author cursor and sender/epoch digest-chain continuity anchors. |
| PendingApply | Deleted atomically when provider application is finalized; retained and replayed after interrupted provider application. | It is the durable cross-store recovery intent, not a historical receipt archive. |
| CompletedPendingApply | Retained as the idempotency mapping from provider apply token to finalized digest/receipt. | It makes repeated finalization with the same token idempotent and rejects token reuse with different input. It is distinct from network duplicate detection. |
| Field/record tombstones and ghost lifecycle facts | Destructive GC remains disabled by E-25. | A stale or offline peer may otherwise resurrect deleted data. Snapshot completion, TTL, HLC age, and GC_NOTICE are not proof of safe namespace-wide deletion. |

## 2. Exact payload compaction conditions

CompactOutboundThrough and CompactRelayThrough are the only current destructive stream-payload compaction entry points.

- The requested sequence cannot exceed the durable ACK of an active local consumer.
- It cannot exceed GetSafePeerCursor, which is the minimum AckedCursor + StreamOffset over all interested peers and required author streams.
- A peer without a committed initial-sync baseline contributes a safe cursor of zero.
- If there is no local consumer and no peer requirement, a positive compaction request is rejected. No consumer is not equivalent to proof that delivery is unnecessary.
- Payload deletion is committed in Storage before the in-memory projection is changed.
- High-watermarks, ACK state, peer state, and ChangeId ledger rows are not deleted by payload compaction.

Retiring a peer epoch removes its roster, baseline and per-author peer-stream rows in one delta transaction, while persisting the maximum retired epoch. A later connection using that epoch or an older one is rejected. This is lifecycle cleanup, not permission to delete receipt or ChangeId evidence.

## 3. Why global ChangeId and Inbox GC are still disabled

The ChangeId ledger is keyed by (namespace, ChangeId) and currently stores a canonical fingerprint plus local/incoming origin. It intentionally survives Outbound/Relay payload compaction. It does not currently encode enough durable origin-stream sequence information to prove that every stale route capable of replaying a ChangeId has crossed a common safe frontier.

Inbox receipts are keyed by (sender, epoch, namespace, BatchId) and preserve cursor range, previous/current digest, durable author and (for new authoritative receipts) the canonical full-wire-header fingerprint. The protocol has no bilateral retention-checkpoint frame proving that both sides have durably advanced beyond a receipt and will reject older epochs/streams. Deleting a receipt based only on a cursor, ACK, peer inactivity or elapsed time could re-admit an equivocated BatchId.

Therefore:
- no TTL, size-based eviction, LRU eviction, or age-based cleanup;
- no coupling of receipt/ChangeId deletion to relay/outbound payload ACK;
- no claim that a snapshot baseline alone makes old dedup evidence obsolete;
- retain legacy receipt rows as read-compatible history; old rows without a full-header fingerprint cannot satisfy a new authoritative duplicate proof and fail closed for that use.

A future safe ChangeId GC design needs durable origin author + contiguous origin sequence association, a namespace-wide minimum safe frontier across active peers, and an explicit stale-peer/epoch retirement rule. A future receipt GC design additionally needs a protocol-level bilateral retention checkpoint (or an equivalent verifiable snapshot/epoch barrier) that binds sender, epoch, namespace, cursor and digest-chain state. These are protocol/storage format changes and must be designed and tested before implementation.

## 4. Tombstone/ghost GC remains disabled

E-25 is normative: tombstone and ghost reclamation is disabled until durable namespace membership, a safe frontier, stale-peer fencing and mandatory authoritative full-resync semantics are established. GC_NOTICE is advisory/control information, not deletion authority. Neither elapsed time nor HLC age can substitute for those proofs.

## 5. Storage cost and release posture

The current policy deliberately trades bounded metadata growth for correctness. Payload bytes can be reclaimed when the per-stream ACK/baseline proof exists; dedup receipts, ChangeId facts, receive-chain anchors and tombstones remain until their independent proof is available. Do not advertise bounded total metadata size or automatic historical GC.

Measure growth using realistic operation and peer cardinalities before deployment. A future GC implementation must include crash-consistent checkpoint publication, restart/replay tests, stale-peer rejoin tests, equivocation tests across the GC boundary, and a measured metadata-size comparison. Do not implement speculative GC as part of routine payload compaction.

## 6. Node-owned mode and provider cutover

Pure standalone BIN-SYNC Core operation without durable provider/Metadata Store remains a supported Node-owned mode, including volatile/in-memory use. It is not the migration bridge and is not a compatibility promise for the removed legacy public TcpSyncSession API.

For a provider being migrated to Metadata Store authority, the pre-cutover Node-Outbox path is a temporary drain/freeze bridge. After the durable provider cutover marker is committed, Metadata Store is the sole outbound/receive metadata authority for that namespace; Node write, publication and legacy receive paths are fenced, and startup reinstalls the fence. There is no fallback from a committed cutover to Node-owned authority.

## 7. Release gates

- Deterministic tests now inject a partial WAL record write, a WAL fsync error and a WAL directory-fsync error after sealed-segment reclamation; they verify fail-closed behavior and reopen recovery. These process-level injections do not reproduce actual device-cache loss, filesystem bugs or power loss.
- Linux/macOS directory-entry updates use native directory fsync and propagate errors. Windows directory-entry durability is not claimed by the current implementation.
- Receipt, ChangeId and tombstone GC remain deliberately disabled until the protocol proofs above exist.
- The section-by-section normative audit still needs requirement-ID to exact-test mapping and cross-implementation coverage for §§4–18.