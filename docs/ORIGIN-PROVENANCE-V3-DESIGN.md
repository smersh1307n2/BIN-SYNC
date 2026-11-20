# BIN-SYNC CORE v3 — immutable origin provenance and safe reclamation

**Status:** binding design decision for the next implementation generation. This document supersedes the optional/draft provenance extension in METADATA-STORE-GC-DESIGN.md and the earlier decision to defer OriginSequence.
**Applies to:** BIN-SYNC wire protocol, Core metadata model, StorageMetadataStore persistence, and the FLD adapter boundary.
**Compatibility policy:** clean break. This library is used only by the Storage and FLD-DB work in this development. Do not retain old wire readers, old persistence readers, dual-write paths, format migration, or removed public APIs for compatibility. New stores use one canonical format. An old/unknown metadata format fails closed and must be recreated; it is not auto-migrated.

## 1. Non-negotiable ownership

- WAL is recovery authority for an uncheckpointed physical transaction.
- FLD local pending feed/staging owns only unpublished local changes.
- BIN-SYNC owns ChangeId identity, immutable origin provenance, delivery, ACK, receipts, relay and membership/retention evidence.
- Storage owns opaque bytes, atomic transactions, snapshots, ordered scans, WAL and checkpoint mechanics. It does not interpret protocol identities.
- FLD Database owns field/LWW/tombstone semantics. The two runtime assemblies remain exactly Fld1.Storage.dll and Fld1.Database.dll; dependency is Database → Storage. Shared Engine is a process, not a third DLL.

There is one payload per logical published operation, shared by all peers. Per-peer progress is O(peers × author streams), not O(changes × peers). Do not add per-peer payload copies or per-operation ACK bitmaps.

## 2. Canonical origin identity

Every accepted operation has a mandatory immutable tuple:

**Origin = (OriginAuthorNodeId: b128, OriginAuthorEpoch: u32, OriginSequence: u64)**

- All three values are non-zero/valid; sequence starts at 1.
- OriginAuthorNodeId is the original author resolved from the operation's session alias. It is never the immediate relay.
- OriginAuthorEpoch is the author's incarnation at creation.
- OriginSequence is allocated once by the original author when the operation is frozen for publication. It is monotonic per (namespace, author) and never resets on epoch change.
- The origin tuple and ChangeId never change across retry, snapshot-tail, relay, restart or storage compaction.
- A relay's own sequence/cursor is a separate coordinate. It must never replace OriginSequence.
- A same ChangeId with a different fingerprint or different origin tuple is equivocation and rejects the whole transaction before mutation. A duplicate arriving by another route is a no-op for apply/relay, while the new sender's own receipt and cursor still commit.

The operation fingerprint remains the canonical semantic fingerprint (resolved author, namespace, record, operation kind, canonical payload and causal dependencies). The ledger stores the fingerprint and origin tuple separately so provenance can be compared exactly and queried for GC.

## 3. Mandatory v3 wire record

There is no optional provenance flag and no capability negotiation for this field. Every v3 DELTA_BATCH record has this exact canonical layout:

    recordId             : b128
    changeId             : b128
    originAuthorEpoch    : u32 LE
    originSequence       : u64 LE
    opKind               : u8
    payloadLength        : canonical ULEB128
    payload              : bytes[payloadLength]

The original author NodeId is resolved from the operation's author alias in the authenticated session ALIAS_MAP. The resolved author plus the two fixed-width fields form the mandatory Origin tuple.

- Origin fields are included in the ordered Records array, RecordsRoot, BatchDigest and signature commitment.
- Zero epoch/sequence, malformed length, trailing bytes, unknown author alias, non-monotonic origin sequence within one author batch, and mismatched operation kind are rejected before staging.
- For a direct-author batch (issuerNodeId == originAuthorNodeId), Origin must match the author's durable published-operation identity and the authenticated author incarnation. If the sender cursor is absolute in the author stream, the implementation may verify the exact mapping; if it is peer-relative, validation must use the persisted stream-offset mapping. It must never infer OriginSequence by arithmetic from CursorStart or replace OriginSequence with a sender cursor.
- For a relay batch, the batch cursor is relay-local and may be peer-relative. It is validated against the relay's own stream and is never compared directly with OriginSequence. The origin tuple is copied unchanged. OriginSequence values in a relay batch must be strictly increasing but may contain gaps where the relay had already accepted an operation through another route.
- This rule follows BIN-SYNC-SPECIFICATION.txt v3.0.0 §§0.1, 0.3 and the direct-author fast-path requirements; it supersedes the earlier cursor-equality wording in this document.
- A relay must not infer, regenerate or normalize origin values. A relay row stores both its local relay sequence and the immutable origin tuple.
- No legacy/unflagged record exists in v3. Missing provenance is malformed input, not an unknown/collectable state.

The protocol major version is 3. A v3 endpoint rejects a different major version during HELLO. There is no v2 fallback or downgrade path.

## 4. Canonical durable metadata rows

The physical Storage API remains domain-neutral. The following are logical BIN-SYNC row values owned by StorageMetadataStore; key encodings are implementation details but must be deterministic and versioned by one partition format marker.

### 4.1 Global ChangeId ledger

One row per (namespaceId, ChangeId):

    Fingerprint[16] | OriginAuthorNodeId[16] | OriginAuthorEpoch[u32 LE] |
    OriginSequence[u64 LE] | OriginKind[u8]

- OriginKind distinguishes locally frozen from remotely accepted identity for provider-feed replay; it is not a second dedup key.
- No full payload is stored in the ledger.
- Ledger key remains route-independent. Sender/peer is never part of ChangeId identity.
- A duplicate with exact fingerprint and exact origin tuple is suppressed. Any mismatch is equivocation.

### 4.2 Local outbound and relay payload

A local outbound row contains immutable ChangeId, origin epoch/sequence and the operation envelope/payload. For a local-origin stream, its original-author sequence is the same assigned sequence used by that author's direct stream.

A relay row contains two independent sequence domains:
- local relay sequence: used for this replica's outbound cursor/ACK and local compaction;
- original Origin tuple: used for identity, causal ordering and namespace-wide GC proof.

Compacting relay payload after downstream ACK does not delete the ChangeId ledger row.

### 4.3 Inbox receipt and PendingApply

- Receipt identity is the authenticated issuer (issuerNodeId, issuerEpoch, namespaceId, BatchId). Authenticated transport peer epoch is stored separately for retirement fencing.
- Every new receipt contains the exact full-header fingerprint, cursor range, previous/current digest, resolved author and transport-peer epoch. No unknown/legacy receipt variant exists.
- PendingApply contains the exact v3 wire payload and the canonical per-operation fingerprints plus origin tuples needed to resume the same provider transaction.
- CompletedPendingApply is a provider-token idempotency fact and has a separate retirement condition; it is not an Inbox receipt and is not removed by relay compaction.
- A new storage partition has one explicit format marker. Missing/unknown marker or any old row shape fails closed. There is no compatibility reader or background migration.

## 5. Safe reclamation proofs

Retention domains remain independent. A proof for one domain does not authorize deletion in another.

### 5.1 Inbox receipts and completed apply tokens

Inbox receipt GC is permitted only when all conditions hold:
1. A newer authenticated transport-peer epoch is durably registered.
2. Its complete initial-sync replacement baseline is durably committed.
3. The old epoch is durably fenced by a monotonic retired-through marker.
4. No unresolved PendingApply (including an intent with uncertain transport epoch) references the retiring issuer/peer scope.
5. In one Storage transaction, persist the retired fence and remove only receipts attributed to the retired epoch. A crash must expose either old receipts + admissible old epoch, or the fence + deleted receipts, never a mixed state.

Active-epoch receipts remain. ACK, TTL, HLC age, next batch or snapshot alone are not sufficient.

CompletedPendingApply tokens are removed only in the same retirement transaction when their receipt/issuer/epoch association is known and no PendingApply remains. The provider-side apply-token receipt must either be retired atomically under the same fence or remain durable. Unknown association means retain. A local caller retry after retirement is outside the supported token lifetime and must fail closed.

### 5.2 ChangeId ledger

ChangeId GC remains disabled until all conditions are implemented and tested:

1. Every ledger row has a non-zero immutable Origin tuple.
2. A namespace administrator key is provisioned out of band. It signs a monotonically increasing namespace roster generation, previous-roster digest, canonical member/author-epoch roster, and full-resync requirements.
3. Every member publishes an authenticated, durable, monotonic retention checkpoint bound to namespace, roster generation/digest, member identity/epoch, snapshot generation/digest, and contiguous applied OriginSequence frontier for every origin author.
4. A missing, disconnected, stale, unknown or regressive member/checkpoint contributes frontier zero. It can be excluded only by a committed roster update plus durable epoch fence and mandatory FULL_REPLACE before rejoin.
5. Safe frontier for each origin author is the minimum contiguous applied sequence across every member in the committed roster generation. The sequence coordinate is the original author's monotonic sequence, never a relay sequence, FLD JournalSeq, Storage CommitSeq, HLC or peer-relative cursor.
6. A ledger row is collectable only when its OriginSequence is at or below that frontier and no PendingApply, outbound/relay payload, immutable batch reservation, provider publication intent or snapshot pin references it.
7. The row deletion and monotonic GC checkpoint commit atomically. Replay below the committed frontier is rejected, not treated as a new operation.

No TTL, LRU, storage-pressure trigger, single-peer ACK or payload compaction can substitute for this proof.

### 5.3 Tombstone and ghost GC

Tombstone/ghost GC additionally requires:
- the same authoritative roster and per-author safe frontier;
- every member has installed the same authoritative FULL_REPLACE snapshot generation/digest, or a later generation proving the deletion;
- the snapshot binds a consistent source snapshot and change-feed tail barrier;
- the provider stages the complete namespace image and atomically switches the active generation;
- a stale/fenced member cannot submit old-generation deltas; rejoin requires FULL_REPLACE before incremental traffic;
- field tombstone, record tombstone and ghost/lifecycle rules are validated independently.

Before deleting tombstone payload, persist compact forgotten-frontier knowledge sufficient to reject stale resurrection. The forgotten frontier may conservatively overestimate forgotten history, but must never understate it. Persisting forgotten knowledge and physical tombstone reclamation must be one crash-safe transition or an ordered two-phase transition where the knowledge is durable first.

No wall-clock TTL, HLC cutoff, GC_NOTICE or ordinary snapshot merge authorizes deletion. Absence from a merge snapshot is not deletion evidence.

## 6. Required adversarial acceptance tests

- Direct author batch: exact epoch and cursor/OriginSequence relation; reject mismatch, overflow and zero.
- Multi-hop A→B→C→D: origin tuple is byte-identical at every hop while each relay cursor is independently assigned.
- Same ChangeId via alternate routes: one ledger row and at most one relay payload; exact tuple/fingerprint accepted as duplicate.
- Same ChangeId with changed author, epoch, sequence or fingerprint: whole batch rejected before any state mutation.
- Relay sequence differs from OriginSequence: both survive restart and are used only in their own domains.
- Restart at every PendingApply/provider/finalize boundary preserves the same origin tuple and apply token.
- Receipt retirement: crash before/after atomic fence+receipt/token deletion; unresolved PendingApply blocks retirement.
- ChangeId frontier: every roster member participates; missing/offline/regressive member blocks GC; stale rejoin requires FULL_REPLACE.
- Tombstone frontier: field tombstone, record tombstone and ghost tested independently; no resurrection through alternate route or stale snapshot.
- FULL_REPLACE concurrent with writes: snapshot/feed barrier has no gap; old generation reclaimed only after readers/apply pins release.
- Malformed/old storage partition: fail closed; no implicit migration or fallback.

## 7. Implementation order

1. Make v3 wire records carry mandatory origin epoch/sequence and enforce protocol-major equality.
2. Add immutable Origin to Core operation, pending-intent, frozen-outbound and relay models.
3. Replace ChangeId ledger, outbound, relay, receipt and PendingApply storage row layouts with one v3 format; remove legacy readers/fallback paths.
4. Propagate origin through Node receive, StorageSyncCoordinator, FLD JournalSeq adapter and every relay batch builder.
5. Add restart, multi-hop, equivocation and malformed-format tests.
6. Implement signed roster generations and retention checkpoints.
7. Implement ChangeId GC behind the all-member safe frontier.
8. Implement FULL_REPLACE generation switching, forgotten-frontier persistence and tombstone/ghost GC.

Do not enable ChangeId or tombstone GC before steps 1–8 and their adversarial tests are green. No backward compatibility is a requirement; correctness and deterministic failure are.
