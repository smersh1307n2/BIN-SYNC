# BIN-SYNC Metadata GC: safe horizons and implementation plan

**Status:** partial implementation. Inbox receipt reclamation is enabled only behind a durable retired transport-peer epoch fence. Global ChangeId-ledger GC and tombstone/ghost GC remain disabled. No time-based deletion is authorized.

## 1. Retention domains must remain independent

| Domain | Current behavior | Safe horizon |
|---|---|---|
| Outbound/relay payload | Implemented compaction | Minimum durable per-peer ACK/baseline cursor, plus local consumer ACK where applicable |
| Per-peer stream rows | Removed on epoch retirement | Durable replacement epoch registration and retired-epoch marker |
| Inbox receipts | Partial GC implemented | Retired authenticated transport-peer epoch; never TTL |
| Receive cursor/digest chain | Retained | Must remain as continuity evidence; current layout is not yet fully transport-epoch-scoped |
| ChangeId ledger | No GC | Requires preserved origin author sequence and a namespace-wide applied frontier |
| Field/record tombstones and ghosts | No GC; purge API returns zero | Requires committed namespace membership, all-member frontier, stale-peer fencing, and authoritative replacement snapshot |
| PendingApply | Removed on successful finalization | Atomic provider apply/finalize protocol |
| CompletedPendingApply | Retained | Separate apply-token idempotency horizon; not removed by receipt GC |

A single ACK, snapshot, HLC cutoff, disconnected peer, or one generic cursor is not a universal deletion proof.

## 2. Inbox receipt GC: implemented conservative horizon

A receipt is needed while the original transport incarnation may retry its immutable batch after losing the ACK. Deleting it while that incarnation remains admissible would turn a valid retry into a rejection and could permit BatchId equivocation if other continuity state were also lost.

The implemented proof is deliberately narrower:

1. A durable receive intent records both the existing author-stream epoch and the authenticated transport peer epoch. The latter is distinct: a relay may carry a third-party author stream.
2. Registering a newer peer incarnation durably records the replacement peer epoch and retires the older epoch. The retired-through marker is the persistent fence.
3. All new Metadata-Store receive entry points require a nonzero transport epoch to match a durably registered InterestedPeer row, reject an epoch-less batch once a retired epoch exists, and reject any transport epoch at or below the retired-through marker.
4. In the same Storage transaction that persists the retired-through marker and removes old peer-stream rows, receipts whose sender and recorded transport epoch match the retired incarnation are deleted.
5. Retirement is rejected while a matching (or legacy epoch-unknown) PendingApply intent remains unresolved. Recovery/finalization must complete first.
6. Legacy receipt rows without transport-epoch provenance are retained; an exact duplicate accepted from a durably registered current epoch re-attributes the receipt to that epoch. The implementation never guesses which incarnation created a legacy row.

The receipt deletion and epoch fence are one atomic Storage commit. A crash can therefore expose either the old receipt plus old epoch state, or the retired fence with those receipts gone, never a state in which an old transport epoch is accepted after its retry receipt was deleted.

The receive cursor remains durable. If a new incarnation replays an already accepted old batch after its old receipt was reclaimed, cursor continuity rejects it. If the author epoch itself changes, the ChangeId ledger remains the independent operation deduplication guard.

### Limits of the current receipt GC

- Receipts for the currently active transport epoch are retained; pruning them after a later same-stream batch is accepted is a possible optimization, but it needs an explicit proof that the sender durably committed the previous ACK and a sender/epoch/namespace digest chain, which is shared across the sender's author streams.
- Legacy receipts with transport epoch zero are retained until an exact authenticated duplicate arrives from a registered current epoch; that replay updates the receipt's transport epoch. No provenance is inferred without the duplicate.
- CompletedPendingApply tokens are not deleted by this receipt GC; they have a separate idempotency role and need their own identity/horizon work.
- Receive cursors remain author-scoped; the sender digest chain is intentionally shared across author streams for one sender/epoch/namespace. Both are retained. Do not delete either merely because receipts were removed.

## 3. ChangeId ledger GC: still disabled

The current ChangeId row stores only a canonical fingerprint and a local/incoming-origin flag. It does not store the immutable origin author sequence required to prove that every replica has passed the operation.

A relay-local sequence is not an origin sequence. A relay can assign its own sequence when first accepting an operation; that number is local to that relay and cannot be compared across replicas. Therefore the current per-peer ACK cursor cannot safely authorize deletion of a global (namespace, ChangeId) row.

Required implementation before enabling ledger GC:

1. Extend each operation's immutable provenance with origin author, origin author epoch, and origin sequence. Preserve those fields unchanged across every relay hop; do not infer origin sequence from a relay-local cursor.
2. Include provenance in the canonical operation identity/fingerprint or otherwise reject a ChangeId that is presented with conflicting origin coordinates.
3. Persist origin coordinates in ChangeLedger and relay payload metadata. Legacy rows without provenance remain non-collectable.
4. Track a contiguous applied-origin frontier per peer and author. A snapshot baseline may advance that frontier only when it is an authoritative snapshot of that author at a recorded origin high-watermark.
5. Maintain a durable namespace membership generation/roster. Every active member contributes a frontier; an unknown, disconnected, or lagging member contributes zero. A peer may be excluded only after a durable epoch fence and a mandatory authoritative full resync on rejoin.
6. Compute the safe frontier per origin author as the minimum across all members in the committed roster generation. Delete only ledger rows at or below that frontier, in the same crash-consistent transaction that persists the GC checkpoint.
7. Keep local pending publication, PendingApply, relay payload, and any in-flight reservation constraints independent; no ledger row may be removed while a live payload or pending intent still references it.

Until all seven conditions are met, retain ChangeId rows indefinitely. Payload ACK/compaction does not authorize ledger deletion.

## 4. Tombstone/ghost GC: still disabled

GcNotice.PurgeBelowCutoff currently returns zero. TombstoneCutoffWall in GC_NOTICE is advisory; it is not deletion authority. HLC age and elapsed time cannot prove that an offline replica has observed a deletion.

A tombstone can be reclaimed only when all of the following hold:

- the namespace has a durable, authoritative membership generation;
- every member in that generation has applied the tombstone's origin sequence, or has installed a later authoritative snapshot that proves the same deletion;
- members that do not meet the frontier remain in the roster and block reclamation, or are durably retired/fenced;
- a retired/stale member cannot reconnect and submit pre-GC state;
- rejoining members must install a full authoritative namespace replacement before deltas are accepted.

The last condition is not currently satisfied by ordinary snapshot merge. Existing snapshot application merges incoming rows; absence from a snapshot does not itself delete an old local record. Before tombstones can be removed, implement a versioned full-resync/replace operation with an atomic namespace swap, a durable snapshot generation and a GC frontier. A stale node must not be able to reintroduce a record merely because the authoritative snapshot omitted its old value.

Only after this barrier may StorageDataProvider remove the tombstone row and the corresponding tombstone ChangeId evidence. Field tombstones and record tombstones/ghost lifecycle facts must use the same proven namespace frontier, with record-level lifecycle rules checked separately.

## 5. Membership and frontier protocol still required

The existing MEMBERSHIP message is a roster hint and its membership epoch is not currently a durable, authoritative namespace membership contract. The current InterestedPeers roster is a local transport-delivery roster; it is not proof that every namespace member is represented.

The next protocol/storage work must introduce a durable membership generation and a retention checkpoint that binds at least:

- namespace ID and membership generation;
- authoritative roster digest and the authority/issuer identity;
- per-author contiguous applied frontier (or a verifiable snapshot baseline);
- peer identity and transport epoch;
- snapshot generation and digest;
- the minimum retained frontier and its monotonicity.

The checkpoint must be authenticated, persisted before ACK, monotonic, and fail closed on roster mismatch, epoch regression, missing author frontier, or stale snapshot. A peer removed from the roster must be fenced before its frontier stops constraining GC.

## 5.1. Authoritative namespace roster and retention checkpoint contract

The existing MEMBERSHIP frame is not an authoritative roster: it is built from locally learned durable authors, and its MembershipEpoch is currently a hint rather than a committed namespace generation. InterestedPeers is a transport-delivery roster, not the set of all replicas that may later replay old state. Neither may be used as the denominator for ChangeId or tombstone GC.

Use an administrative namespace roster certificate as the membership authority. This is a control-plane authority only; it does not own field writes, choose LWW winners, or route data. The namespace administrator's public key is provisioned out-of-band in the namespace trust configuration. If no valid certificate is available, membership changes and destructive GC fail closed.

A roster certificate binds:
- namespace ID;
- strictly increasing membership generation and previous roster digest;
- canonical sorted member list (NodeId plus current author epoch and member status);
- the new roster digest;
- whether each added/rejoining member is FULL_RESYNC_REQUIRED;
- the admin-key signature over the complete canonical body.

A node accepts only a valid signature, the exact previous generation/digest it currently trusts, and a strictly increasing generation. A peer cannot promote its locally learned DurableAuthors list into an authoritative roster. A member omitted from a committed roster is fenced for incremental traffic; if it later rejoins, it must install a full authoritative replacement snapshot before sending or accepting deltas for that namespace. A node that has not installed the latest roster certificate cannot advance a retention checkpoint for that generation and therefore cannot participate in GC. This prevents an old roster view from silently authorizing deletion.

Each roster member periodically publishes a signed RetentionCheckpoint bound to (namespace, membershipGeneration, rosterDigest, memberNodeId, memberAuthorEpoch, snapshotGeneration, snapshotDigest), plus a sorted map of contiguous applied origin frontiers keyed by (originAuthorNodeId, originAuthorEpoch). The checkpoint is persisted before it is acknowledged. Regressive frontiers, missing roster members, a roster digest mismatch, or a checkpoint from a retired epoch fail closed. A new roster generation invalidates old-generation checkpoints for GC calculations until members attest the new generation.

For every origin stream, the safe applied frontier is the minimum contiguous frontier reported by every member in the committed roster. Missing/unknown reports contribute zero. ChangeId GC may remove only a ledger row whose immutable origin sequence is at or below that frontier and which is not referenced by any PendingApply, outbound/relay payload, immutable reservation, or provider publication intent. Provenance-less legacy rows remain non-collectable.

Tombstone GC additionally requires that every roster member has installed the same authoritative full-snapshot generation/digest that contains the tombstone's deletion (or a later state proving it). A FULL_REPLACE snapshot is not the current merge snapshot: it must stage a complete namespace image, validate its root and all per-author frontiers, then atomically switch the provider's active namespace generation. Absence from a merge snapshot is never deletion evidence. The old generation is reclaimed only after the switch is durable and no reader/apply is pinned to it. Any rejoining or removed member must complete FULL_REPLACE before incremental deltas are accepted.

This design intentionally separates:
1. origin provenance — immutable (author, authorEpoch, originSequence) carried through every relay;
2. membership authority — the signed committed roster generation;
3. progress evidence — signed per-member contiguous applied frontiers and snapshot generation;
4. physical reclamation — one crash-consistent Storage transaction/checkpoint after all logical proofs exist.

Implementation order is mandatory: (A) add origin provenance to operation envelope, relay payload and ChangeId ledger while treating legacy provenance as unknown; (B) add signed roster certificates and durable membership generations; (C) add signed monotonic retention checkpoints and all-member frontier calculation; (D) enable ChangeId ledger GC behind those proofs; (E) add provider FULL_REPLACE generation switching and rejoin fencing; (F) enable tombstone/ghost GC behind the same frontier plus full-replacement proof. Do not enable C/D/E/F early or use transport InterestedPeers as a substitute.
## 6. Required acceptance tests before enabling each domain

### Receipt GC
- old epoch receipt is deleted atomically with retired-through marker;
- stale old-epoch BeginPendingApply and AcceptIncomingBatch fail after restart;
- epoch-unknown legacy receipts are retained;
- retirement is blocked by unresolved PendingApply;
- crash before/after the retirement transaction yields no state where a deleted receipt is paired with an admissible old epoch;
- same BatchId under a new transport epoch cannot bypass the persisted receive cursor or ChangeId ledger.

### ChangeId GC
- multi-hop relay preserves origin coordinates byte-for-byte;
- same ChangeId with a different origin sequence/author is rejected;
- frontier is the minimum across every roster member;
- offline member blocks GC until fenced;
- stale member rejoin requires authoritative full resync;
- crash during GC checkpoint publication is replay-safe;
- a replay below the committed frontier is rejected, not silently treated as a new operation.

### Tombstone/ghost GC
- tombstone is retained while any active peer is below its origin frontier;
- offline peer blocks GC until durable retirement;
- snapshot replacement removes stale local values atomically;
- stale snapshot/delta from retired generation is rejected;
- tombstone and ChangeId reclamation commit atomically with the GC generation;
- process restart at every publication boundary preserves deletion and prevents resurrection.

## 7. Non-negotiable rules

- No TTL, LRU, size cap, HLC-age or wall-clock-only deletion for receipts, ChangeId, tombstones or ghosts.
- No deletion based solely on outbound/relay payload compaction.
- No GC of provenance-less legacy records.
- No reclamation while PendingApply or an immutable outbound reservation references the state.
- No claim of bounded metadata growth until all three GC domains have proven horizons and measured storage/CPU costs.
