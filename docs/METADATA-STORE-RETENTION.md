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
| Inbox receipts (sender, author epoch, namespace, BatchId) | Reclaim receipts only when their authenticated transport-peer epoch is durably retired; active-epoch receipts and legacy rows without transport-epoch provenance remain retained. | The retired-through fence rejects the old transport epoch on receive; deletion and fence commit atomically. PendingApply must be resolved first. |
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

Retiring a peer epoch removes its roster, baseline and per-author peer-stream rows in one delta transaction, while persisting the maximum retired epoch. A later connection using that epoch or an older one is rejected. Inbox receipts attributed to that retired transport epoch are also deleted in the same transaction; epoch-unknown legacy receipts and ChangeId evidence are not.

## 3. Why global ChangeId GC and active-epoch Inbox GC are still disabled

The ChangeId ledger is keyed by (namespace, ChangeId) and currently stores a canonical fingerprint plus local/incoming origin. It intentionally survives Outbound/Relay payload compaction. It does not currently encode enough durable origin-stream sequence information to prove that every stale route capable of replaying a ChangeId has crossed a common safe frontier.

Inbox receipts are keyed by (sender, author epoch, namespace, BatchId), and now also record the authenticated transport-peer epoch separately. They preserve cursor range, previous/current digest, durable author and (for new authoritative receipts) the canonical full-wire-header fingerprint. When a newer peer incarnation is registered and its complete initial-sync baseline is durably committed, the old transport epoch is durably fenced. In the same Storage transaction, receipts attributed to that retired transport epoch are deleted. Retirement is blocked while a matching or epoch-unknown PendingApply remains unresolved. Receive entry points reject old epochs and reject epoch-less input after a retirement fence exists. Legacy receipts without transport-epoch provenance remain retained until an exact duplicate is received from a durably registered current epoch; that authenticated replay re-attributes the receipt to the new epoch so it can be reclaimed only when that epoch is later retired.

The active transport epoch's receipts are still retained. The last receipt may be needed to answer a retransmission after an ACK was lost. CompletedPendingApply tokens and receive cursor/digest-chain checkpoints are also retained; they have separate lifecycle rules.

Therefore:
- no TTL, size-based eviction, LRU eviction, or age-based cleanup;
- no coupling of ChangeId deletion to relay/outbound payload ACK;
- no claim that a snapshot baseline alone makes old dedup evidence obsolete;
- retain legacy receipt rows without transport-epoch provenance; do not guess their origin epoch. Re-attribute only after an exact, authenticated duplicate from a durably registered epoch.

A safe ChangeId GC design needs immutable origin author + author epoch + origin sequence preserved unchanged through every relay, an administrator-signed authoritative namespace roster generation, signed monotonic per-member retention checkpoints, the minimum contiguous applied-origin frontier across every member in that roster, and stale-peer/epoch fencing. Missing members/checkpoints contribute zero; legacy rows without origin coordinates are never collected. A safe tombstone GC design additionally needs the same roster/frontier proof and a versioned FULL_REPLACE snapshot that atomically switches the provider's namespace generation; current snapshot merge does not delete rows absent from the snapshot. The exact certificate/checkpoint contract and mandatory implementation order are in [METADATA-STORE-GC-DESIGN.md](METADATA-STORE-GC-DESIGN.md). These features remain disabled until their protocol/storage changes and restart/fault tests are complete.

## 4. Tombstone/ghost GC remains disabled

E-25 is normative: tombstone and ghost reclamation is disabled until durable authoritative namespace membership, a safe per-author frontier, stale-peer fencing and mandatory FULL_REPLACE resync semantics are established. GC_NOTICE is advisory/control information, not deletion authority. Neither elapsed time nor HLC age can substitute for those proofs.

## 5. Storage cost and release posture

The current policy deliberately trades bounded metadata growth for correctness. Payload bytes can be reclaimed when the per-stream ACK/baseline proof exists. Retired-epoch inbox receipts can be reclaimed behind the durable transport fence; active receipts, ChangeId facts, receive-chain anchors and tombstones remain until their independent proof is available. Do not advertise bounded total metadata size or automatic historical GC.

Measure growth using realistic operation and peer cardinalities before deployment. A future GC implementation must include crash-consistent checkpoint publication, restart/replay tests, stale-peer rejoin tests, equivocation tests across the GC boundary, and a measured metadata-size comparison. Do not implement speculative GC as part of routine payload compaction.

## 6. Node-owned mode and provider cutover

Pure standalone BIN-SYNC Core operation without durable provider/Metadata Store remains a supported Node-owned mode, including volatile/in-memory use. It is not the migration bridge and is not a compatibility promise for the removed legacy public TcpSyncSession API.

For a provider being migrated to Metadata Store authority, the pre-cutover Node-Outbox path is a temporary drain/freeze bridge. After the durable provider cutover marker is committed, Metadata Store is the sole outbound/receive metadata authority for that namespace; Node write, publication and legacy receive paths are fenced, and startup reinstalls the fence. There is no fallback from a committed cutover to Node-owned authority.

## 7. Release gates

- Deterministic tests now inject a partial WAL record write, a WAL fsync error and a WAL directory-fsync error after sealed-segment reclamation; they verify fail-closed behavior and reopen recovery. These process-level injections do not reproduce actual device-cache loss, filesystem bugs or power loss.
- Linux/macOS directory-entry updates use native directory fsync and propagate errors. Windows directory-entry durability is not claimed by the current implementation.
- Receipt GC is enabled only for receipts with a recorded transport-peer epoch after that epoch is durably retired; active-epoch and legacy epoch-unknown receipts remain retained. ChangeId and tombstone/ghost GC remain disabled until their independent proofs exist.
- The section-by-section normative audit still needs requirement-ID to exact-test mapping and cross-implementation coverage for §§4–18.