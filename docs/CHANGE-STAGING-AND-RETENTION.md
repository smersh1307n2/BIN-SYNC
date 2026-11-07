# Change Staging, Coalescing, and Metadata Retention

**Status:** Architecture decision draft v0.20. BIN-SYNC core now has the global route-independent ChangeId ledger, compact receipts, author-grouped peer progress, ACK-based Outbox compaction and replay-cache pruning. The durable `StorageMetadataStore` and `StorageDataProvider` adapters are implemented and pass real restart/compaction/provider conformance; Storage Integration is 49/49. `StorageSyncCoordinator` coalesces unpublished LWW updates within each bounded source-feed page before creating immutable Node Outbox entries. The compact Storage ledger folds local-origin into the fingerprint row and combines per-author ACK/high-watermark values into one row (legacy rows remain readable). Remaining BIN-SYNC + Storage gates: authoritative first-time outbound payload construction from Metadata Store pages, peer-set fencing and minimum-ACK retention/compaction, receive-side acceptance authority migration, bounded metadata mutation cost for non-peer mutations, receipt/dedup GC horizons and safe tombstone retention. The durable outbound path now reads Outbound/Relay payload pages from Metadata Store, persists per-peer/per-author progress and exact batch reservations, and commits ACK/digest transitions durably. Registered interested peer streams fence compaction at the minimum ACK; stale peer epochs remain registered until a future explicit retirement protocol, intentionally preferring retention over unsafe GC. The legacy Node-backed path remains available during migration. Cross-page local LWW coalescing is also implemented: the coordinator drains the durable provider feed across pages, keeps only the winning mutation per record/field (and record lifecycle), and freezes only those winners. The provider feed is the durable PendingLocal journal; no duplicate metadata row is written for every intermediate edit, and feed ACK follows Node persistence plus metadata freeze. **FLD Database implementation and its relay-provenance adapter are explicitly deferred to the next project stage.**
**Related architecture:** [Metadata Storage and External Data Provider Architecture](METADATA-STORAGE-ARCHITECTURE.md).  
**Metadata contract:** [Metadata Store Semantic Contract and Conformance Plan](METADATA-STORE-CONFORMANCE.md).  
**Provider contract:** [Data Provider Semantic Contract and Conformance Plan](DATA-PROVIDER-CONFORMANCE.md).  
**PendingLocal ownership/handoff:** [Metadata Storage Architecture §3.3](METADATA-STORAGE-ARCHITECTURE.md).  
**Protocol baseline:** BIN-SYNC CORE v2.4.19 FINAL; route-independent ChangeId identity/equivocation is normative in E-24 and fail-closed tombstone retention is normative in E-25. Independent conformance vectors remain a release gate. The physical representation and any origin-sequence wire extension remain open; do not treat the 32-byte-per-operation ledger as the final optimized design.  
**Primary concern:** retain exactly the history required for convergence and recovery, without storing every superseded local state forever.

## 1. Executive decision

BIN-SYNC storage must distinguish **current replicated state**, **pending local changes that have never been exposed to a peer**, **immutable published changes**, and **remote changes retained for relay**.

For state-based LWW_FIELD synchronization, repeated local writes to the same canonical key \`(SyncScope, NamespaceId, RecordId, FieldId)\` may be coalesced **only while they remain unpublished**. The pending representation retains the latest local operation for that key. Older unpublished local operations can be discarded because no peer has observed them and the latest operation supersedes them under the LWW ordering.

Before a pending operation can be exposed to any peer, it must be atomically frozen into the immutable outbound journal. Freezing assigns its permanent stream sequence and stable ChangeId (unless an externally defined stable operation identity must be preserved). From that point onward, its payload, identity, author, HLC, key epoch and sequence are immutable until protocol retention rules permit deletion.

The following are not interchangeable:

- **Unpublished** means no sender/transport could have exposed the operation to any peer.
- **Unacknowledged** means it may already have been delivered, but the required peer ACK/retention condition has not been met.

An unacknowledged operation is not coalescible. A lost ACK does not prove that the peer did not receive the operation.

This policy is a storage optimization, not a change to the wire protocol or to the semantics of already published changes.

## 2. Why this distinction is necessary

The current architecture correctly requires immutable ChangeIds, author identity, per-author stream sequence, peer-relative pushed/acked watermarks, durable inbox receipts, and relay retention. Those guarantees mean an operation cannot be rewritten merely because a local application has not yet observed a successful synchronization result.

However, preserving every local edit forever is not required for state-based LWW convergence when the edit has never been exposed to another replica. If a field is locally changed from A to B to C, and only C is ever published, every peer receives the same final LWW field state that it would have obtained after receiving all three operations in order, provided no external contract requires the intermediate operations.

The storage model should therefore preserve the **latest unpublished state per coalescible key**, not an append-only event history for every local edit. The append-only history begins at the publication boundary.

This is not event sourcing. Applications that require an audit trail, user-action history, or every intermediate edit must store that history separately or opt the relevant operations out of coalescing.

## 3. Current implementation facts

The following observations were checked against FLD-DB branch fix/crash-consistency-outbox at commit 331206106f7d7d645dabe849957278a3a5a8b008:

1. FldDatabase.Put/PutOwned generates an HLC and a ChangeId for each call, then writes the latest MemFieldValue into _overlay and _dirty, keyed by (RecordId, FieldId).
2. Therefore, several writes to the same field before one Commit() are already collapsed by _dirty: only the last value for that key is serialized in that commit. Earlier generated ChangeIds are discarded and never reach the WAL/Outbox.
3. Each separate Commit() snapshots the current dirty set, appends a WAL group commit, and appends an Outbox entry for every field mutation in that commit.
4. OutboxLog.EntryKey includes CommitSeq, RecordId and FieldId; it prevents duplicate materialization of the same WAL mutation but does not coalesce the same field across different commits.
5. The Outbox is append-only. Its fixed record portion is 88 bytes, followed by the value bytes and a 4-byte record CRC. There is also a 16-byte file header, plus index/ACK/state/lease overhead.
6. RunBuilder stores one LWW winner per (RecordId, FieldId) in a state run. The run is a current-state representation, not a complete local edit history. WAL and Outbox preserve the committed operations needed for recovery and delivery.
7. The remote APPLY path has a separate durable apply/relay journal. Relay-eligible remote operations are retained independently of whether they win the local LWW comparison.
8. The current BIN-SYNC runtime observation is based on code baseline commit f31e043cddaf269df313e5201fc45f75b06c9ec8. SnapshotChunkMessage/BuildSnapshotChunks copies current LWW field and lifecycle state, but the inspected DTO does not carry per-field ChangeId or an Outbox cursor. AutoReconcile uses STATE_DIGEST → NEED_RANGE → SNAPSHOT_CHUNK in two directional rounds; the inspected SendSnapshotCoreAsync path sends chunks without a SnapshotTail/outbox checkpoint. Snapshot transfer tests explicitly assert that peer-relative StreamOffset/StreamAcked/StreamPushed are not advanced or rewritten by the snapshot transfer. The normative specification separately describes an 80-byte SnapshotTail/outbox checkpoint, so the exact spec/runtime baseline and snapshot-to-tail contract require reconciliation before storage code may infer Outbox coverage from a snapshot.

### 3.1. Concrete answer to the repeated-write example

Assume the same field is written four times:

| Situation | Current FLD behavior |
|---|---|
| Four Put calls, then one Commit() | One field mutation reaches that WAL commit and one local Outbox entry is materialized. _dirty has already collapsed the four writes. |
| Four Put + Commit cycles before any peer synchronization | Four WAL mutations and four Outbox entries are retained, even though the current field state/run has only the final winner. |
| Four writes to four different fields, then one Commit() | Four field mutations/Outbox entries; they are different canonical field keys. |
| Four writes to the same field with intermediate commits, followed by checkpoint | The state run contains the final field winner; the unacknowledged Outbound entries remain independently retained. Checkpoint does not mean that peers received them. |

Thus, four edits to one field are not four current records in the state database. In the current implementation they can be four durable outbound operations if each edit was committed separately. The proposed staging model reduces the unpublished outbound representation to one pending operation for that field.

## 4. Logical change lifecycle

The target model has four distinct layers.

### 4.1. Current replicated state

One current winner per canonical LWW key:

SyncScope + NamespaceId + RecordId + FieldId

The state includes the information required by merge and digest semantics:

- operation kind (write or tombstone);
- current value or provider-owned value reference;
- HLC wall/logical;
- original author NodeId;
- key epoch where applicable;
- lifecycle state where applicable.

The current LWW field state does **not** need a full history of every prior ChangeId. ChangeId identifies a change event for deduplication and delivery; it is not part of the v2 RecordDigest field tuple or the LWW comparison key. Keep ChangeId in the durable operation journals and dedup/provenance indexes where required, not automatically in every current-state row.

### 4.2. Pending local changes (coalescible staging)

A durable staging area for local operations that have not been exposed to any peer.

For LWW_FIELD, it holds at most one pending local operation per canonical field key. A later local write to the same key replaces the earlier staged operation only if it is the winner under the canonical BIN-SYNC LWW comparator. Do not use callback arrival order, thread scheduling, dictionary insertion order or last lock acquisition as a substitute for the protocol comparison. It must retain enough information to reproduce that latest local operation even if a later remote APPLY overwrites the visible current field state.

That last condition is important: a pending local write must not be represented only by a pointer to the current visible field value. A remote operation can win LWW after the local write but before publication; the local operation may still need to be delivered/relayed to other peers. The staged payload or a durable immutable provider reference must survive independently of the visible winner.

A staging entry may contain:

- scope and namespace;
- canonical RecordId/FieldId or provider item/field key;
- latest local operation kind;
- local HLC and author;
- key epoch;
- immutable payload, or a provider reference with a documented lifetime and exact-version read guarantee;
- provider/source version or feed token;
- causal dependencies if enabled;
- a local staging generation/version used only for storage concurrency and recovery.

A staging entry has **no BIN-SYNC stream sequence yet**. It is not eligible for DELTA_BATCH construction and is not included in per-peer StreamPushed/StreamAcked calculations.

### 4.3. Frozen local Outbox

A pending operation becomes a frozen Outbox operation in one durable atomic transition before it can be exposed to any peer.

The freeze transition is also a recovery boundary. A caller retrying the exact same freeze after an uncertain commit must be able to recover the original assigned sequences without appending a second copy. The semantic contract therefore permits an exact full-batch retry when every ChangeId, author stream and payload byte matches the already-frozen batch. A conflicting reuse or a retry that partially overlaps an earlier freeze must fail closed; it must not append the new suffix. The caller must retain stable ChangeIds and payload bytes across the retry. This is idempotent publication, not permission to rewrite a frozen operation.

The freeze transition:

1. selects a stable set of pending operations;
2. verifies their coalescing/dependency constraints;
3. assigns permanent per-author/per-namespace sequence numbers;
4. assigns or finalizes stable nonzero ChangeIds;
5. captures immutable payloads or immutable references;
6. appends the operations to the durable Outbox;
7. removes/moves the corresponding pending staging entries;
8. commits all of the above atomically.

The exact internal representation is implementation-specific. The externally observable guarantees are not.

A frozen entry is immutable even if it has not yet been ACKed by any peer. It may already have been sent to one peer, partially sent, or received while the ACK was lost. The sender must replay the same ChangeId, payload and sequence on retry.

New local writes to the same field after freeze create/update a new pending staging entry. They do not rewrite the frozen entry.

### 4.4. Remote relay journal

Remote operations selected for relay remain a separate immutable journal. Preserve original ChangeId, author, HLC, key epoch, operation, payload/reference and provenance.

Do not apply the local pending-coalescing rule to the relay journal by default. A relay node may need to forward operations that lose LWW, and different downstream peers may have different receive/ACK progress. The protocol's first-seen ChangeId and provenance rules are not equivalent to a current-state-only merge.

The relay journal is compacted only after BIN-SYNC's per-peer/per-author safe-retention rules permit it. It is not merged into the local-origin Outbox or the local pending staging table.

## 5. Coalescing rules

Coalescing is permitted only when all of the following are true:

- the operation is local-origin;
- it has not been frozen or exposed to any sender/peer;
- the merge policy defines a safe supersession rule;
- the new operation has the same scope, namespace and canonical key;
- no retained operation depends on the superseded ChangeId;
- the application/provider has not requested event/audit preservation for these operations.

### 5.1. Policy matrix

| Operation/model | Default coalescing before publication | Rule |
|---|---|---|
| LWW_FIELD absolute field write | Yes | Keep latest local operation for the exact field key by the protocol's total LWW order. |
| LWW_FIELD field tombstone | Yes | A later local write/tombstone for the same field may replace the earlier staged operation; preserve the final operation kind. |
| Lifecycle field 0x0000 | Yes, same key only | Keep latest lifecycle state. Do not discard or rewrite independent field operations; preserve ghost/tombstone semantics. |
| LWW_RECORD absolute record state/tombstone | Only if provider policy explicitly defines it | Same unpublished-supersession rule, subject to record-level semantics. |
| COUNTER_G, COUNTER_PN | No generic coalescing | Only a merge-policy-specific accumulator/compaction with a proof of equivalent merge semantics is allowed. |
| SET_OR, SEQ_CRDT | No generic coalescing | Element identities, adds/removes and causal retention cannot be replaced by a single latest payload. |
| BLOB_REF | No generic coalescing | Blob descriptors are immutable identities. A higher-level LWW field may replace an unpublished descriptor only if that is the declared data model. |
| MANUAL / conflict history | No by default | Preserve alternatives or require an explicit application resolution policy. |
| Audit/event-sourced operations | No | Intermediate events are the data, not redundant states. |
| Remote relay operations | No by default | Preserve provenance and ChangeId until safe relay retention/GC. |
| Frozen/published local operations | Never | Identity, payload and sequence are immutable until safe deletion. |

### 5.2. Causal dependencies

If optional causalDeps are enabled, a staged operation must not be discarded while any retained operation references its ChangeId. The implementation may:

- keep the referenced predecessor as a separate staged operation;
- or use a formally defined dependency-preserving compaction rule.

It must not silently remove a referenced ChangeId and leave a dangling dependency.

The simplest public contract is to make causal dependencies refer only to stable, frozen operations. If an application requires a dependency on an unpublished operation, that operation must be frozen first. This keeps the common LWW staging path compact and avoids exposing provisional identities.

### 5.3. Ordering at freeze

Surviving staged operations must be frozen in a deterministic order that preserves the originating author's causal order. For ordinary local HLC-generated LWW writes, order by their retained HLC tuple, with a deterministic canonical-key tie-break only as a defensive fallback. Do not use dictionary iteration order.

Sequence numbers are assigned only during freeze and then never changed. Peer-relative StreamOffset remains BIN-SYNC-owned and does not renumber the shared Outbox.

### 5.4. The publication boundary

The implementation must make the transition from pending to frozen durable before a transport can send the operation. It is acceptable to freeze conservatively before the first send attempt. It is not acceptable to send first and record the freeze afterward. The durable self-echo check required by BIN-SYNC E-13 must remain correct across both storage stages: it must not become blind to a locally authored winner merely because that operation is still in pending staging rather than the frozen Outbox. The implementation should expose one semantic query over the local pending winner plus frozen/unconfirmed Outbox state, without merging their retention lifetimes.

If a process crashes after freeze but before the first byte is sent, the operation remains frozen and will be sent on retry. This may retain one extra superseded operation, but it cannot cause divergence.

### 5.5. E-13 self-echo lookup across storage stages

BIN-SYNC E-13 requires durable self-echo suppression. The cross-peer dedup mechanism must cover every published operation even after its Outbox payload is compacted. An exact ChangeId ledger is the safe fallback; an origin-sequence/checkpoint representation is under protocol-level evaluation. Pending operations have no published ChangeId yet and remain independently visible to the local self-echo lookup only if an external provider supplies a stable operation identity.

For the exact canonical field key, the runtime needs an efficient lookup over:
- the latest pending local operation, if it has an externally stable source operation identity;
- durable cross-peer dedup state for frozen local-origin operations (exact ChangeId fingerprints as the safe fallback; no full payload copy);
- the relevant frozen/unconfirmed local Outbox payload where byte comparison is required.

The ledger comparison uses ChangeId plus the canonical operation fingerprint. Where the protocol self-echo rule is based on an operation tuple rather than a ChangeId, compare operation kind, HLC, resolved original author and exact value bytes; a hash may be a prefilter but not proof of equality. The query must not use the current visible field winner alone: a remote APPLY may have replaced that winner while a local pending operation still exists.

This lookup is a derived view over pending staging and frozen Outbox, not a third authoritative history. It may be implemented as an index keyed by canonical field key and referencing the owning staged/frozen payload. The index must be rebuilt or transactionally maintained and must not duplicate full payload bytes. It must preserve E-13's distinction between a true local echo and a first-seen remote operation that merely loses LWW.

For a future snapshot-plus-tail transfer, the source snapshot and change-feed watermark must share one barrier, but that alone does not prove that every frozen operation is covered by the snapshot. The current snapshot payload is a current-state LWW slice, while Outbox/relay journals may contain operations (including LWW losers) that are not represented as current field winners. The implementation must explicitly define, per target peer, which frozen operations are semantically subsumed by the snapshot baseline and which provenance/relay operations still require delivery. Until that rule is specified and tested, a snapshot MUST NOT implicitly advance Outbox ACK/GC, discard pending staging, or suppress relay entries. Never take an arbitrary current-state snapshot and independently sample an unrelated feed cursor; that can omit a change or send it on both sides of the baseline.

## 6. Metadata minimization: what is stored where

The Metadata Store should avoid storing the same information in several forms without a clear recovery reason.

| Information | Canonical owner | Retention |
|---|---|---|
| Current field value/state and LWW metadata | Data Provider when it owns user data; FLD when FLD is the data DB | Until superseded and safe state compaction permits replacement |
| Current field HLC/author/kind/key epoch | Same current-state owner | One winner per key; not one copy per historical edit |
| Pending local operation | Metadata Store or provider durable feed/staging mechanism | Coalesced until freeze |
| Frozen local outbound payload/identity/sequence | One authoritative local Outbox | Until every required peer/retention rule permits deletion |
| Incoming batch receipt/cursor/digest checkpoint | Metadata Store | Until duplicate retry and protocol recovery horizons permit GC |
| Seen ChangeId/provenance | Metadata Store or durable provider journal | Until protocol-safe dedup/relay GC horizon |
| Remote relay operation | Separate relay journal | Until all relevant downstream retention conditions are met |
| Peer pushed/acked offsets and incarnation state | BIN-SYNC metadata | As required to validate ACK and retention |
| Transport/session aliases and temporary buffers | Runtime memory | Ephemeral; reconstruct after reconnect |

The same logical fact may be encoded in one compact record or transaction. The contract should not force one physical row/object per scalar. Use compact binary layouts and batched operations, but do not combine state with different lifetimes into a blob that must be rewritten on every small update.

### 6.1. ChangeId placement

ChangeId is required for a published change and for protocol deduplication/provenance. It is not automatically required in the current LWW field-state row.

Recommended behavior:

- assign ChangeId when the operation is frozen, or earlier only if a provider/API requires an externally stable operation identity;
- if a staged local operation is replaced before freeze, its unexposed provisional identity may be discarded;
- after freeze, preserve ChangeId byte-for-byte across retries, snapshot/tail handling where applicable, relay and recovery;
- never generate a new ChangeId while replaying an already frozen operation.

If the application needs an ID for each user edit, that is an application/source event ID and must be distinct from a provisional BIN-SYNC ChangeId.

### 6.2. Payload ownership and avoiding duplicate values

For FLD as the synchronized database, a staged local operation must retain its own payload because a later remote APPLY may replace the visible field winner before the staged local operation is frozen. It may be stored inline, in an immutable value blob, or as a stable reference into a retained WAL/journal generation. The reference option is valid only if checkpoint/compaction cannot reclaim the referenced bytes before freeze.

For an external Data Provider, the Metadata Store should not copy user data unnecessarily when the provider exposes an immutable durable change-feed payload or an exact-version reference whose lifetime is guaranteed. Otherwise the metadata side must retain the payload needed to publish the operation; a cursor alone is insufficient.

The contract must define who owns the payload and when the provider may reclaim it. Do not let both provider and Metadata Store believe the other side retains the only copy.

### 6.3. Recommended logical cardinality

The logical layout should scale with the number of current keys and retained protocol operations, not with the total number of local edits ever performed:

| Category | Recommended cardinality | Avoid |
|---|---|---|
| Scope/replica configuration | One compact state per scope/replica | Repeating NodeId/configuration in every field row where a scope key already supplies it |
| Current LWW field metadata | One winner per (scope, namespace, recordId, fieldId) | One metadata row per historical local edit |
| Pending LWW operation | At most one per exact canonical field key | Duplicating every superseded write in a pending journal |
| Frozen local Outbox | One immutable entry per published operation | Rewriting a frozen entry or maintaining a second outbound copy |
| Inbox receipt | One receipt per accepted batch idempotency key | One full batch receipt per field mutation |
| Seen ChangeId/provenance | One compact dedup/provenance fact per protocol-defined identity and retention scope | Storing full payloads again in the dedup index |
| Peer progress | One state per (scope, peer, namespace, author stream) | One scalar ACK cursor per peer or namespace |
| Relay journal | One retained immutable entry per relay-eligible operation | Merging relay operations into local pending staging |

A record does not need a separate metadata row merely because it exists. In the current BIN-SYNC field model, an empty document is not the same as a deleted record; lifecycle state is represented by its reserved field operation. Store record-level metadata only when a real protocol/provider feature requires it.

For a raw logical LWW field-state tuple, RecordId (16), FieldId (2), operation kind (1), KeyEpoch (2), HLC (12) and AuthorNodeId (16) total 49 bytes before scope/namespace keying, indexes, value storage and physical encoding. ChangeId is intentionally excluded from this current-state tuple; it belongs to operation/dedup history. FLD may compress author/HLC representation in its run format. This is a logical accounting aid, not a frozen binary layout.

Small peer-state values or receipts may be packed into compact per-peer/per-stream records to reduce object and index overhead, provided updates remain atomic and their independent retention lifetimes remain explicit. Do not pack all metadata for a scope into one monolithic blob that must be rewritten for every cursor or peer ACK.

### 6.4. FLD Outbox record encoding — concrete size-reduction candidates

The current FLD `OutboxLog` uses an 88-byte fixed record plus a 4-byte CRC (92 bytes before value bytes). The stored fields include `OutboxSeq`, `CommitSeq`, `ChangeId`, `RecordId`, `FieldId`, `OpKind`, `AuthorNodeId`, HLC, `KeyEpoch` and a 32-bit value length. The current database also persists one stable `LocalAuthor` identity for the whole database, while each Outbox file is already namespace-scoped. These facts create safe compression opportunities, subject to a versioned log-format change:

- **AuthorNodeId:** do not repeat the same 16-byte database author in every local Outbox record. Resolve it from the durable LocalAuthor file or store it once in the Outbox header/segment. Reopen must fail closed if the header identity conflicts with LocalAuthor.
- **OutboxSeq:** records are appended in monotonic sequence order and the log maintains a sparse offset index. If compaction preserves a `firstSeq` plus record count and records are never removed/reordered except by prefix compaction, sequence can be reconstructed from record ordinal instead of storing 8 bytes per record. Prove this against torn-tail recovery, AppendIfMissing, ACK compaction and crash reopen before adopting.
- **CommitSeq:** multiple field entries commonly share one WAL CommitSeq. Store a delta/varint from the previous CommitSeq or encode commit groups with one CommitSeq per group. Never infer commit materialization boundaries from OutboxSeq; they are separate clocks.
- **Value length:** replace the fixed 32-bit length with a bounded varint if benchmarks show a net win and decoding remains allocation-free.
- **HLC:** consider block-relative wall-time plus logical deltas only if real workloads show a meaningful reduction. Preserve exact HLC ordering and avoid per-entry branches/allocations on the append/read hot path.

At the current fixed 92-byte baseline, removing repeated author (16 B) and explicit sequence (8 B), and delta-encoding CommitSeq (typically 1 byte rather than 8) could reduce fixed metadata by roughly 30 bytes per entry before any HLC/value-length optimization. This is a candidate estimate, not a committed layout. Keep ChangeId until its identity semantics are deliberately changed; do not remove it merely because a local cursor exists. OutboxSeq is currently a local namespace cursor and is not yet a stable wire-level origin sequence.

Any new layout requires dual-version reader or explicit migration, crash-injection coverage at every record boundary, and benchmark comparison of append throughput, scan throughput, recovery time and bytes per operation. No in-place rewrite of a published Outbox entry is permitted.

## 7. FLD-specific target and migration

### 7.1. Current behavior is safe but retains more unpublished history

Today FLD commits each dirty field mutation into WAL and Outbox. This is safe and simple, but four separate commits to one field produce four Outbox entries even if no peer has ever seen the first three.

The current behavior must not be changed by mutating or deleting arbitrary Outbox records. The append-only Outbox and immutable sequence rules protect peers that may already have received an operation.

### 7.2. Preferred future design: separate staging from frozen Outbox

Introduce an explicit durable pending-local-change staging area (name is not fixed). Local commit atomically updates:

- current FLD field state;
- the coalesced pending local operation for the same key;
- the storage WAL/recovery state.

The pending operation retains its payload independently from the current visible field state, because a later remote APPLY may win LWW.

A separate freeze transaction moves a stable pending set into the existing immutable Outbox, assigns OutboxSeq/ChangeId and flushes it durably before exposure to BIN-SYNC. The existing Outbox remains append-only and keeps its fenced lease/ACK behavior.

This preserves the existing Outbox contract and optimizes only the unpublished staging area.

### 7.3. Alternative not selected: rewrite an unacknowledged Outbox tail

Do not implement coalescing by replacing an existing Outbox entry in place merely because it has not been ACKed. It may have been sent to one peer, while another peer has not received it. Rewriting the shared entry would make the same sequence refer to different payloads at different peers and would violate deterministic replay and batch-chain expectations.

A per-peer replacement scheme would require independent per-peer immutable streams and is a substantially different protocol/storage architecture; it is not justified for the current shared-Outbox model.

### 7.4. WAL and checkpoint interaction

Pending staging data must be recoverable after crash and must not be lost when WAL segments are checkpointed/truncated. A checkpoint may materialize current field state and pending staging state into durable storage before retiring the corresponding WAL prefix.

The pending staging payload must remain available even if the visible current field value has been superseded by remote APPLY. A checkpoint that stores only the visible winner is insufficient to recover a pending local operation that lost LWW.

### 7.5. Storage-only FLD mode

The same separation applies when FLD is used only as a Metadata Store. Storage-only writes must not create synthetic protocol HLCs, authors, ChangeIds or local Outbox entries.

The generic metadata store can use storage-native commit/version IDs for its own WAL and snapshot ordering. BIN-SYNC HLC, ChangeId, author sequence and peer cursor remain ordinary metadata values owned by BIN-SYNC, not automatically generated by FLD's physical engine.

## 8. External provider contract implications

A Data Provider must provide one of these mechanisms:

1. a durable change feed with stable feed tokens and immutable payloads/references;
2. a transactional local-change hook that writes source state and pending-change metadata atomically;
3. a snapshot/diff mechanism with a stable snapshot token and a change-feed watermark or write barrier that closes the snapshot-to-tail gap.

When the runtime consumes a durable feed:

1. read a bounded feed batch;
2. map source operations to canonical keys and merge-policy semantics;
3. upsert coalescible pending LWW operations (or append non-coalescible operations);
4. persist the provider feed checkpoint in the same Metadata Store transaction;
5. only then acknowledge/advance the provider feed.

A replayed feed batch must be idempotent. The provider cursor must never advance before the corresponding pending operations are durable.

If the provider only exposes current state and no change feed, a snapshot comparison can produce final-state LWW changes, but it cannot preserve intermediate events or local operations that were overwritten before observation. That mode is valid only when the application explicitly accepts state-based synchronization rather than event-history synchronization.

Remote apply must remain idempotent and distinguishable from local source changes. A remote operation must not be recaptured as a new local-origin operation merely because the provider emitted a generic row-changed notification.

## 9. Retention and garbage collection

The following lifetimes are independent:

- **Pending staging:** retained until frozen or safely superseded by another local operation under the coalescing rules.
- **Frozen local Outbox:** retained until BIN-SYNC proves the safe contiguous prefix across all interested peers and any additional protocol retention conditions are satisfied.
- **Relay journal:** retained until all relevant downstream peers have safely advanced; local-origin ACK does not imply relay ACK.
- **Per-peer batch replay cache:** retain the cached BatchId/previous-digest tuple for an author-stream cursor range while it may need deterministic retry; once that peer's per-author StreamAcked reaches/passes the range end, the cache entry is no longer needed for retry and can be pruned transactionally with ACK state.
- **Inbox receipts:** retained for the protocol's duplicate-batch/replay horizon.
- **Seen ChangeId/provenance:** retained for the protocol's deduplication and relay/GC horizon.
- **Current field tombstones/ghosts:** retained according to the protocol's causal/GC watermark; not deleted merely because their payload is empty or a newer local operation exists.
- **Provider feed entries:** retained until the Metadata Store has durably staged them or another contractually safe transfer boundary is reached.

No generic DeleteOldMetadata operation is sufficient. Each category needs its own safe-retention condition.

### 9.1. Cross-peer deduplication and local WAL replay markers

The receiver recognizes the same original operation across direct peers and relay paths. Immediate sender identity is not part of operation identity. The runtime implementation uses a per-replica dictionary keyed by `(namespaceId, ChangeId)`; the replica identity is implicit in the owning Node.

**Implemented exact ChangeId ledger:** each key retains a 16-byte BLAKE3 fingerprint (32 logical bytes per operation including the 16-byte ChangeId, before dictionary/page overhead). The fingerprint covers namespace, RecordId, OpKind, canonical FULL_AUTHOR_ID operation bytes (resolved permanent author, HLC, field/key epoch, value or tombstone), and a sorted unique causal-dependency set. Same ID/same fingerprint is suppressed across routes and after origin Outbox compaction; same ID/different known fingerprint rejects the complete batch before WAL/state publication. The ledger stores no operation payload and no duplicate author field.

Local WAL replay markers remain separately represented by `SeenChanges`, but this set now contains only local-node markers. Remote peer-scoped SeenChanges rows were redundant after the global ledger became authoritative and are no longer written or probed. v3-v5 migration folds all legacy remote identities into unknown global-ledger markers before dropping those peer-scoped rows; v6-v7 migration retains the already-persisted global ledger and filters SeenChanges to local markers. `MaxSeenChanges` therefore caps only local WAL replay markers; `MaxGlobalChangeLedgerEntries` caps global operation identities.

**Candidate optimization — content-derived ChangeId:** ChangeId could be defined as BLAKE3-128 over a canonical semantic operation (namespace, record, op kind, permanent original author, HLC/key epoch and canonical value/tombstone bytes). The receiver would recompute it before apply. This would make conflicting payload reuse fail ID validation and reduce the exact dedup index from 32 logical bytes (ID + fingerprint) to a key-only 16-byte set. It still requires exact retention to suppress old relay loops; it does not by itself create a safe GC horizon. Audit provider-supplied IDs, API compatibility, E2EE/canonicalization, alias resolution, causal metadata and legacy wire compatibility before selecting it.

**Candidate optimization — origin sequence:** The protocol specification describes immutable `u64 Seq` per `(authorNodeId, namespaceId)`, but the wire protocol currently carries peer-relative batch cursors rather than a stable per-operation origin sequence. Do not assume FLD's current `OutboxSeq` already provides this: `OutboxLog` currently describes it as a namespace cursor, and relay persistence must be audited separately. Investigate carrying an explicit origin sequence per operation and maintaining per-origin contiguous high-watermarks plus compact sparse gaps/ranges. This can reduce retained state substantially when sequences are received contiguously. It is NOT safe to implement a watermark-only design until all of the following are specified and tested:
1. sequence assignment is durable, monotonic, never reused, and preserved unchanged by every relay;
2. the receiver can distinguish origin sequence from peer-relative transport cursor and bind it to the resolved permanent author and namespace;
3. snapshot/bootstrap establishes an authenticated, two-sided checkpoint for every covered author stream and cannot hide relay-relevant operations (including LWW losers);
4. gaps remain represented until filled or explicitly fenced; no watermark may cross an unknown missing operation;
5. the protocol defines how same-sequence/different-payload equivocation is detected. A high-watermark alone suppresses replay but cannot compare an old payload fingerprint. If this conflict check remains mandatory, retain a compact commitment or provide authenticated origin hash-chain proofs;
6. a node cannot accept an older sequence from a newly discovered route after it has safely compacted that sequence's dedup evidence.

**Current retention rule:** until the origin-sequence and checkpoint contract is proven, exact ChangeId fingerprints remain the safe fallback and have no TTL/ACK-only GC rule in the current protocol. Do not reclaim dedup facts using a peer cursor, local relay ACK, Inbox expiry, tombstone GC or arbitrary TTL. A Bloom filter may accelerate lookup but cannot be the only source of truth.

**Legacy migration (implemented in NodePersistence v6):** v3-v5 SeenChanges entries do not contain a canonical payload fingerprint. Preserve each distinct `(namespaceId, ChangeId)` as an explicit unknown global marker; do not use a zero digest as a sentinel. For retained Outbox payloads, reconstruct the fingerprint only when the complete operation value is present and validate that repeated legacy payloads for one ChangeId agree. If a legacy FieldWrite payload has a deferred/truncated value, keep its marker unknown rather than guessing from the current field winner. The first matching new arrival upgrades an unknown marker while suppressing relay; historical equivocation cannot be checked until that fingerprint is known.

### 9.2. Inbox receipt compaction

Inbox receipts have a separate lifecycle. The idempotency key is `(issuerNodeId, namespaceId, issuerEpoch, batchId)`. The current compact v8 receipt stores:
- the resolved durable author stream used for the original ACK;
- the original `cursorEnd` used to construct duplicate ACKs (the replay-supplied cursor is not trusted);
- a 16-byte header fingerprint;
- `appliedAt` (HLC wall, Unix microseconds);
- `expiresAt` (zero means no expiry is scheduled).

The fingerprint commits to namespace, issuer, epoch, BatchId, cursorStart/cursorEnd, previous batch digest, records root, batch digest, schema id/version, merge policy and flags (excluding the transport-only causal-trailer-present bit). Thus cursorStart and the digest/chain fields are committed without storing each separately. The receipt does not retain the full Fields payload, individual ChangeIds or causal dependencies.

**Duplicate semantics are intentionally header/idempotency based, not payload revalidation.** If the BatchId key exists and the known header fingerprint matches, the receiver ACKs using the stored cursorEnd and author without decoding or applying replay-supplied entries/ChangeIds/CausalDeps; this must bypass the causal gate. A payload altered while keeping the same BatchId and committed header is ignored as a duplicate, not applied. A changed committed header (for example cursor range, schema/mode fields or BatchDigest) is rejected before ACK. This preserves the existing hardening contract tested by `Duplicate_batch_bypasses_causal_gate_and_does_not_trust_replay_change_ids`.

NodePersistence v8 stores the unified receipt map. v3-v7 migration validates the old set/map parity where applicable and creates explicit legacy-unknown receipts without cursorEnd/fingerprint; those receipts retain the old duplicate-ACK behavior until their original data can no longer be recovered. No speculative upgrade from a replay payload is allowed because it cannot prove the original receipt.

The normative Inbox GC condition remains: `expiresAt` has elapsed AND both sides have confirmed the relevant log segment. New receipts currently set `expiresAt=0`; no receipt expiry/GC is implemented. Relay ACK, WAL truncation, ChangeId-ledger retention and tombstone GC are not substitutes for this condition. The receipt does not yet carry a separate WAL-generation reference or materialization state; WAL/receipt recovery remains covered by the accepted-batch WAL frame and node-state snapshot.
### 9.3. Per-peer batch replay cache has a finite ACK-based lifetime

The current Node.DeltaBatch signing path stores PeerState.Ids keyed by (authorStream, cursorStart, cursorEnd), with the stable BatchId and previous digest needed to reproduce the exact same wire batch after a lost ACK. This cache is necessary while a peer may retry an unacknowledged range. After a validated contiguous ACK covers the same peer/namespace/author range end, ProcessAckInternal now prunes that mapping before Outbox compaction. It deliberately preserves LastStreamDigest, which is the current digest-chain checkpoint and is not a historical replay mapping.

The pruning condition is author-specific and uses the validated StreamAcked watermark. StreamPushed alone, a partial ACK, another author's ACK or another peer's ACK cannot release the mapping. The regression test verifies removal after ACK, preservation of LastStreamDigest, persistence/restart behavior and idempotent repeated ACK. This bounded replay-cache reclamation is implemented.

### 9.5. Tombstone GC safety is an open protocol gate

The current `GcNotice.Apply` does not persist or enforce the received `TombstoneCutoffWall`. `WatermarkGate` exists but is not wired into DELTA_BATCH, reconcile or snapshot acceptance. The former `PurgeBelowCutoff` implementation removed ordinary `Record.State` values by age without checking tombstone/ghost semantics, and did not reclaim `FieldTombs` or record lifecycle tombstones. That destructive behavior has now been disabled: the method returns zero until a safe watermark/resync gate is implemented, and tests assert preservation of live values and tombstones.

Do not rely on this path for tombstone reclamation. Before enabling it, implement a durable monotonic cutoff per namespace, force peers behind the cutoff through full resync/snapshot, and reclaim only tombstone/ghost state proven safe. Never delete a live field solely because its HLC is old. Inbox receipt expiry, global ChangeId ledger retention and relay payload ACK remain independent lifetimes. The normative requirements are summarized in Metadata Storage Architecture §8.3 and FLD-DB roadmap §7.8.

### 9.4. FLD APPLY single-encoding and relay-only materialization (implemented)

The FLD remote APPLY path has been changed:
- ApplyCommitCodec v3 encodes the incoming Fields array once and stores relay selection as canonical varint indexes. Decode reconstructs RelayFields as references to those same decoded operation objects. v2 payloads remain readable.
- FldDatabase.ApplyRemoteBatch flushes the full APPLY_COMMIT to WAL, then ApplyJournal.AppendIfMissing writes a compact journal representation containing batch receipt metadata and only RelayFields payloads.
- The journal's relay-only form stores the selected payloads once, with no duplicate full Fields array or relay-index list. Non-relay operation payloads are not copied into ApplyJournal.
- The WAL is durable before the journal materialization; ApplyJournal is flushed before ApplyRemoteBatch returns. Thus a subsequent checkpoint/WAL truncation cannot remove the only durable copy of relay-eligible operations.
- Journal promotion is idempotent by batch identity and compares canonical receipt metadata plus relay payload. WAL recovery can rebuild the compact journal from both old v2 and new v3 APPLY_COMMIT records.
- Relay-eligible LWW losers remain in the relay-only journal.

Until WAL truncation, non-relay operations exist only in WAL and relay operations exist in WAL plus the relay-only journal. After WAL truncation, only relay-selected payloads remain in the journal. The metadata receipt and relay payload are still co-located in one append-only journal record/file, so their physical retention lifetimes are not yet independent.

ReadBatches now uses a sparse JournalSeq anchor every 256 records and binary-searches the nearest anchor, scanning at most one bounded stride before the requested page. ReadRelayBatchPage returns ScannedThroughJournalSeq across empty author ranges; the adapter must persist this scan cursor atomically with returned relay operations, and it is not a safe ACK. The full IndexEntry List has been removed. The BatchKey dictionary now stores only a 64-bit record offset, but it still has one entry per receipt and the on-disk history remains unbounded.

FLD now exposes `AcknowledgeRelayBatches(author, safeThroughJournalSeq)`: a CRC-protected sidecar persists the per-author safe watermark using temp-write, file flush, atomic replace and best-effort directory flush. Reads suppress acknowledged payload immediately. After the corresponding APPLY WAL prefix is sealed/truncated, checkpoint/reopen compaction rewrites the journal and removes only acknowledged authors' relay fields, preserving the receipt and all other authors' payload. WAL recovery skips re-materializing a sealed batch whose receipt is already present, so it cannot resurrect the compacted payload; if the receipt is missing, the full WAL record rebuilds it. A regression test proves restart persistence, author isolation, duplicate receipt behavior and actual journal-size reduction. The BIN-SYNC adapter still needs to durably map source JournalSeq→ChangeId→local OutboxSeq and compute the minimum contiguous ACK across all interested peers before calling this API. Receipt GC and disk-backed/rebuildable batch indexes remain open; compaction currently rewrites the full journal.

### 9.5. Preferred remote-APPLY storage decomposition

The logical lifetimes must be separated:

1. **WAL apply transaction:** retain the complete incoming operation set exactly once for crash recovery/application. Store relay selection as compact indexes/bitset over that operation array, not a second copy of each selected payload. This is an internal WAL format change/version, not a wire-format change.
2. **Durable inbox receipt:** retain only the metadata required to recognize a duplicate batch and reproduce the same ACK/result: sender/issuer identity and epochs, namespace, BatchId, cursor range, previous/current digest, RecordsRoot, CommitSeq, AppliedCount, and the protocol-defined expiry/confirmation state. ProtocolStateDelta (up to the current 4 MiB limit) is a separate recovery payload: retain it only until BIN-SYNC has durably materialized/checkpointed the corresponding protocol metadata state, or retain a stable reference to that materialized state. If the delta remains the only recovery source, it must remain until that state is checkpointed. Do not keep a full raw delta for the entire Inbox duplicate-retry lifetime after its effects are already durable and queryable.
3. **Relay journal:** retain only the exact relay-eligible operations, with original ChangeId, author, HLC, key epoch, canonical key, operation payload and the batch/provenance metadata needed to reconstruct the outbound relay. Do not retain non-relay operations here.
4. **Current field state:** retain only the current LWW winner per field key in runs/overlay; this is not a substitute for relay payloads.
5. **BIN-SYNC ChangeId ledger and peer state:** remain protocol-owned metadata and are not silently folded into FLD's relay payload log.

The WAL APPLY record is the authoritative atomic commit. Receipt and relay sidecar records may be materialized idempotently from that WAL record using CommitSeq as their recovery identity. The WAL prefix must not be truncated until the required receipt and relay materialization is durable. A crash between WAL flush and sidecar flush must rebuild the missing sidecar state on open without duplicating entries.

The receipt and relay journals need independent retention:

- A receipt may be removed only under the normative Inbox GC condition (expiry elapsed and both sides have confirmed the corresponding log segment).
- A relay payload may be removed only after BIN-SYNC's safe downstream retention/ACK calculation says no interested peer still needs it.
- Receipt expiry does not imply relay completion; relay completion does not necessarily imply that duplicate-batch receipt expiry has elapsed.

The public FLD API should therefore expose separate semantic read/ack/retention operations for relay payloads and batch receipts, or one transactional metadata API with independent receipt and relay watermarks. A single ApplyJournal sequence must not be treated as both lifetimes unless the physical format supports independent segment reclamation.

### 9.6. Payload-sharing alternatives

There are two viable physical strategies for relay payload lifetime:

- **Copy relay subset once into a dedicated RelayJournal:** simplest compaction and WAL truncation. It duplicates relay payload once relative to the WAL apply record, but avoids retaining non-relay operations and allows independent relay ACK/GC.
- **Reference immutable WAL payload ranges:** avoids copying relay payload, but pins the WAL segment until all referenced relay entries are safely retired. This can retain unrelated local/remote commits in the same segment and complicate checkpoint/truncation.

The preferred default is a dedicated compact RelayJournal unless measurements show that WAL segment pinning is materially cheaper for the target workload. Never keep both a full ApplyJournal copy and a separate full RelayJournal copy. Its read path should seek from a retained sequence/index boundary (e.g. lower-bound over a sorted page index or segment index), not rescan all historical entries for every page. Compaction must reclaim both file bytes and in-memory batch/index entries.

A receipt should not retain the full incoming field array merely to answer a duplicate batch. A compact receipt record is sufficient once the original apply effects are committed and the relay subset has its own durable owner. ProtocolStateDelta is a bounded per-batch recovery envelope, not a permanent log. Materialize it into normal indexed protocol metadata during finalization; for separate stores retain it only in the pending-apply intent until provider and metadata finalization complete, then delete it. Duplicate BatchId returns only the compact receipt/ACK result, never the full delta. The delta must contain only mutations for this batch, not a serialized Node/PeerStates/ChangeIdLedger image. The current 4 MiB maximum is a safety ceiling, not a target payload size; ordinary batch metadata updates should remain small and should not rewrite unrelated peers or historical dedup entries.

## 10. Storage cost example

Let one field value have payload size V. Ignore filesystem allocation, index growth and checkpoint overhead for a simple comparison.

Current FLD Outbox record overhead is approximately 92 + V bytes per field mutation (88-byte fixed record plus 4-byte CRC, plus payload). Four separate commits to the same field therefore retain roughly 4 × (92 + V) bytes in Outbox until safe ACK/compaction, in addition to WAL history and one current state winner.

With pending coalescing, four local updates to the same LWW field before freeze retain one staged latest operation, roughly one metadata entry plus one payload/reference. Freezing then produces one immutable Outbox operation, approximately 92 + V bytes. The four source commits may still exist temporarily in WAL until checkpoint, but they do not become four published protocol changes.

If the first operation is frozen before the other three writes, all four operations remain distinct. If the four writes affect four different fields, there are four pending keys and four eventual frozen operations. Coalescing is per canonical key, not per record as a whole.

This policy reduces redundant outbound history; it does not promise that every physical copy of a value disappears. WAL, current-state storage, pending staging and frozen Outbox have different recovery responsibilities.

## 11. Required tests before implementation is accepted

1. Four writes to the same LWW field before one commit produce one pending/frozen operation.
2. Four separate commits to the same field before any freeze coalesce to one pending operation.
3. Freeze after the first commit, then three more commits: the first frozen entry remains byte-identical and three later writes coalesce to one new pending entry.
4. Crash before freeze: recovery restores only the latest staged operation for the key.
5. Crash during freeze: recovery sees either the complete pre-freeze staging state or the complete frozen Outbox transition, never a missing/duplicated operation.
6. Remote APPLY wins after a local staged write: the staged local payload remains available and is still publishable.
7. A newer local write replaces the staged local operation even if a remote operation arrived between the two local writes; final HLC/order and relay semantics remain correct.
8. A lost ACK after send does not permit rewriting the frozen entry; retry uses the same ChangeId, sequence and payload.
9. Two peers with different ACK progress see the same immutable Outbox operation at the same local sequence.
10. A stale Outbox lease cannot freeze, read or ACK data after a newer lease takes ownership.
11. A causal dependency on a staged ChangeId prevents unsafe coalescing or forces dependency-preserving freeze.
12. COUNTER_PN, SET_OR, SEQ_FRACTIONAL, BLOB_REF and MANUAL do not accidentally use LWW coalescing.
13. Relay LWW losers remain retained and relayable with original ChangeId/provenance.
14. Local echo suppression continues to obey BIN-SYNC E-13; staging must not create a new echo loop.
15. Snapshot baseline and tail are bound to one stable source state/feed watermark, and the protocol explicitly proves which frozen operations are covered by the snapshot versus still requiring tail/relay delivery.
16. Provider feed replay after crash does not duplicate staged operations or advance its cursor prematurely.
17. Storage-only FLD writes create no synthetic protocol author/HLC/ChangeId/Outbox entries.
18. Checkpoint/compaction cannot reclaim payload bytes still referenced by pending or frozen operations.
19. Concurrent same-key local writes that reach the staging lock out of HLC order retain the canonical LWW winner, not merely the last callback to acquire the lock.
20. A snapshot transfer does not silently ACK, GC or discard frozen/relay operations whose provenance is not represented by the snapshot payload.
21. Cross-peer dedup survives Outbox/RelayJournal payload compaction. Until an origin-sequence/checkpoint protocol is proven, exact ChangeId fingerprints are not pruned by TTL or peer ACK; disk-backed lookup remains bounded in RAM.
22. Inbox receipt expiry/GC requires the protocol's elapsed-expiry plus confirmed-log-segment condition and survives restart.
23. E-13 self-echo suppression sees the correct durable local winner across pending staging and frozen/unconfirmed Outbox without treating staged entries as published stream records.
24. Per-peer PeerState.Ids replay-cache entries survive lost ACK/restart and are pruned only after a valid ACK covers their author-stream cursorEnd.
25. APPLY_COMMIT encodes every incoming operation payload once; relay selection is represented without duplicating selected payload bytes.
26. ApplyJournal/receipt storage does not retain non-relay operation payloads after the WAL recovery boundary has passed.
27. Duplicate-batch receipt remains available after WAL checkpoint/truncation without retaining the full original field array.
28. Relay payload survives WAL truncation and is reclaimed only after safe downstream ACK; receipt expiry and relay ACK are tested independently.
29. Crash after WAL flush but before receipt/relay sidecar materialization reconstructs both sidecars exactly once from the WAL commit.
30. Relay journal paging starts at the requested retained sequence without scanning all previously acknowledged history; compaction releases both disk bytes and in-memory indexes.
31. Local WAL replay markers are retained while their WAL records can replay and are reclaimed only after durable truncation/checkpoint; the global ChangeId ledger follows the namespace/scope epoch lifetime described in §9.1.
32. Deferring ChangeId allocation until freeze does not weaken local staging/WAL replay idempotency; a storage-native commit/staging generation handles that boundary.
33. Reuse of an Inbox idempotency key with conflicting cursor/digest metadata is rejected using a compact receipt, without retaining the full incoming field payload.
34. A ChangeId received from a second peer after its prior Outbox/RelayJournal payload was compacted is suppressed by durable cross-peer dedup; conflicting operation identity reuse is rejected. Test both the exact-ledger fallback and any future origin-sequence representation.
35. A same BatchId with changed cursorStart/cursorEnd, author, BatchDigest, RecordsRoot or PreviousBatchDigest is rejected using the compact Inbox receipt.
36. PeerState.Ids entries are pruned atomically only when a valid ACK covers their complete per-author cursorEnd; partial ACK and lost-ACK restart preserve the entries.
37. Freezing a local operation inserts its ChangeId fingerprint atomically with the immutable Outbox entry; crash/reopen cannot leave one without the other.
38. First remote receipt inserts the global ledger entry and relay payload atomically; a matching duplicate from another peer advances that peer's receipt/cursor without reapply or relay append.
39. Legacy NodePersistence v5 SeenChanges entries migrate into the partitioned global ledger without fabricated fingerprints or invented expiry.

## 12. Implementation sequence

1. Freeze the semantic lifecycle and coalescing matrix; do not start by changing the current Outbox file format.
2. Add tests for current behavior and the target staging/freeze invariants.
3. Design a compact pending staging representation and its WAL/checkpoint recovery rules.
4. Define the neutral provider change-feed and pending-stage operations in the Metadata Store contract.
5. Implement staging/freeze in FLD synchronized-database mode while retaining the current immutable Outbox API.
6. Validate remote-APPLY-over-pending-local behavior and multi-peer lost-ACK scenarios.
7. Reuse the same semantic contract for external providers and FLD Storage-only mode.
8. Measure bytes per pending/frozen field, WAL amplification, allocation, commit latency and freeze throughput before choosing physical record layout.
9. Only after conformance tests pass, consider changing defaults or freezing the public API.

**Core rule:** coalesce only operations that have never been externally observable; preserve every operation once it may have been observed. Current state, unpublished staging, frozen outbound history, relay payloads, Inbox receipts and cross-peer dedup state has separate ownership and retention rules.

## 9. Cross-repository code audit addendum (2026-11-07)

This addendum supersedes older implementation baselines cited in §3 for current-code claims. Revalidated source-code baselines (2026-11-07): FLD-DB `fix/crash-consistency-outbox` at `eacff36f1d3ff513508100d27f9d572bf3cfff66`; BIN-SYNC `feature/fld-external-storage` at `d2e0313c7de685066d663d5e14901b3a67abd1b7`. The FLD baseline includes the per-author relay ACK/compaction implementation; the BIN-SYNC baseline includes the current ChangeId/Outbox/ACK primitives and the latest GcNotice source change. Comparing current branch heads with these baselines shows only documentation and test/conformance-fixture changes after them, with no production runtime source changes in this review.

The full code-backed audit, target data model, size accounting, risks and tests are in [FLD-DB: Sync History Storage Audit](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-HISTORY-STORAGE-AUDIT.md).

### 9.1. One operation, many peers is already the BIN-SYNC model

The current BIN-SYNC Outbox stores each logical operation once. Per-peer PeerState holds per-author StreamPushed/StreamAcked/StreamOffsets; batch construction reads the same Outbox entry for each peer. Do not introduce per-peer payload copies or per-operation ACK bitmaps. The one-author-per-DELTA_BATCH rule is a stream-ordering boundary, not a requirement to duplicate payloads.

Current implementation gaps found during the audit:

- The initial audit identified that ProcessAckInternal did not prune PeerState.Ids after a validated ACK. This was implemented later: acknowledged mappings are pruned atomically when the validated per-author ACK covers cursorEnd, while LastStreamDigest is retained. See §9.4 item 2 and the cited implementation/test commits.
- StreamAcked, StreamPushed and StreamOffsets are serialized as three maps and repeat author IDs. A per-author tuple with presence flags can reduce metadata without changing semantics.
- SeenChanges is currently keyed by immediate sender peer, not globally by ChangeId. The live Outbox queue suppresses an already queued ChangeId, but that is not durable after Outbox compaction. Implement the exact global ChangeId+fingerprint ledger described in the metadata architecture; keep local WAL replay markers separate.
- Inbox, InboxAuthors and ReceivedStreams contain overlapping batch receipt/cursor facts. Consolidation is a candidate only after all duplicate replay and ACK paths are mapped.

### 9.2. FLD remote APPLY has confirmed payload amplification

Current RemoteBatchCommit validates RelayWrites as an exact subset of Writes, but ApplyCommitCodec v2 serializes Fields and RelayFields as two full arrays. The same full APPLY_COMMIT payload is persisted in WAL and ApplyJournal. Therefore, at the operation-payload level, a non-relay operation is represented twice (WAL + ApplyJournal), while a relay-selected operation is represented four times (Fields + RelayFields in each file), before counting the current-state run/overlay.

ApplyJournal also retains an index entry in both a List and a BatchKey Dictionary for every historical batch. It has no ACK/compaction/truncation boundary, and ReadBatches scans the list from its beginning for each cursor page. This is an unbounded disk/RAM/paging cost, not merely a theoretical layout concern.

Preferred internal redesign: encode each incoming operation once in WAL and represent relay selection with a compact bitmap or operation indexes; keep a compact metadata-only batch receipt; materialize only relay-selected payloads into a relay-only journal before truncating the authoritative WAL segment. Receipt GC, relay-payload GC and ChangeId-ledger GC must remain independent. This does not require a DELTA_BATCH wire change.

### 9.3. FLD local Outbox and WAL

FLD local Commit currently flushes the WAL group commit and then appends the field payloads to Outbox. Thus local operation bytes exist in both files until WAL checkpoint/truncation. A WAL-backed Outbox reference with promotion of unacknowledged payloads before WAL truncation is a possible later optimization, but it is more invasive than removing the APPLY duplication.

The current FLD Outbox fixed record is 88 bytes plus a 4-byte CRC before value bytes. The local AuthorNodeId is constant for the database and is repeated in every namespace Outbox row; this can be removed from each row in a versioned format. OutboxSeq can potentially be derived from a segment base plus ordinal, and CommitSeq can be group-encoded/delta-encoded. Keep ChangeId until its identity contract is deliberately changed. A rough fixed-record target is 60–65 bytes before group framing/CRC overhead; this is not a benchmark.

### 9.4. Priority and current implementation status

1. Global cross-peer ChangeId dedup, including same-ID equivocation rejection — implemented; retain the exact ledger until a safe GC horizon exists.
2. Prune acknowledged `PeerState.Ids` mappings — implemented; preserve the independent stream digest checkpoint.
3. Consolidate Inbox/InboxAuthors into one header-bound receipt map — implemented; receipt expiry/GC remains open.
4. Remove redundant remote `SeenChanges` rows and compact the remaining local WAL marker key — implemented; NodePersistence v11 migration is covered.
5. APPLY single-encoding, relay-only materialization, sparse journal paging and FLD per-author relay ACK/physical payload compaction — implemented in FLD-DB.
6. BIN-SYNC source `JournalSeq → ChangeId → OutboxSeq` provenance, durable scan cursor and per-author minimum downstream ACK aggregation — still open as a later FLD synchronized-database integration gate. It is not the immediate implementation task: first establish the neutral Data Provider/Metadata Store contracts, refactor runtime persistence, and extract the two-DLL Storage Core.
7. Inbox receipt expiry/GC, bounded/rebuildable receipt indexing, and Storage physical format — still open; no unsafe time-only deletion is authorized.
8. PendingLocal cross-commit coalescing — design contract and baseline tests exist; implementation belongs to FLD-DB and is not yet available.
9. Only after the above, evaluate compact Outbox encoding, content-derived ChangeId, OriginSeq and WAL-backed Outbox references.

No wire-format or ChangeId generation change is authorized by this addendum.

## 8. Compact history decisions and implementation gates (2026-11-07)

See the cross-repository [Sync Storage Compaction Roadmap](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-STORAGE-COMPACTION-ROADMAP.md) and code-backed [Sync History Storage Audit](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-HISTORY-STORAGE-AUDIT.md).

The target lifecycle decisions are:
1. Current field state stores the current winner and required lifecycle/tombstone data; it is not a full ChangeId history.
2. Pending local LWW staging stores at most one latest unpublished operation per canonical field key. Its payload is independent of the current visible winner.
3. Freeze/publication is atomic and immutable. Once an operation could have been exposed to any peer, it cannot be coalesced or rewritten, regardless of ACK state.
4. Remote relay payload is separate from local pending staging. Preserve relay-eligible operations, including LWW losers; do not apply local coalescing to relay history.
5. One payload serves all peers. Peer progress is stored separately per peer/namespace/author stream.
6. Batch receipts, global ChangeId dedup facts, relay payload and outgoing replay mappings are separate logical records with independent lifetimes. Physical co-location is allowed only if atomicity and reclamation remain independent.

The first implementation priorities are now complete: route-independent ChangeId dedup, ACK-covered replay-cache pruning, compact receipts, APPLY single-encoding/relay-only materialization and FLD per-author relay payload reclamation. The remaining integration gap is the durable BIN-SYNC source-provenance mapping and per-author minimum downstream ACK calculation. Inbox receipt GC and bounded/rebuildable receipt indexing remain separate gates. Durable PendingLocal coalescing follows in FLD-DB; physical compact encodings come only after logical transitions are tested.

No ChangeId generation change, OriginSeq extension, wire-format change or destructive GC policy is approved here. Detailed exit criteria and crash tests are in the roadmap.

### 9.6. NodePersistence v10 grouped receipt and dedup encoding

Accepted Inbox receipts are now serialized grouped by `(issuer, namespace, epoch)`; each group writes its shared issuer/session key once, then the BatchId and receipt values. Global ChangeId fingerprints are grouped by namespace. This reduces repeated scope-key bytes in snapshots while preserving the same logical dictionary keys and retention behavior. For each additional receipt in a group, the raw saving is approximately 16 bytes plus namespace/epoch varint widths; for each additional ledger entry in a namespace, it is the namespace varint width. Group count varints offset a small amount, and singleton groups may be slightly larger; these are format-derived estimates, not measured benchmarks.

NodePersistence v10 reads the old flat v3-v9 layouts and writes the grouped layout. Tests cover v9 migration, v10 round-trip, unknown legacy markers, expiry fields and invalid cursor overflow. No wire-format change.

### 9.7. Compact local WAL replay marker identity (NodePersistence v11)

Once remote provenance was moved to the global ChangeId ledger, `SeenChanges` contained only markers belonging to the local node. The NodeId in each key was therefore constant and redundant. v11 stores the marker as `(namespace, ChangeId)` in memory and on disk; older v3-v10 snapshots are migrated by retaining only legacy entries whose peer equals the loaded NodeId. The global ledger remains distinct because it has a different semantic and retention contract. Raw persistence savings are 16 bytes per local marker plus reduced in-memory tuple/hash-entry overhead; this is a format-derived estimate, not a measured benchmark. WAL replay atomicity, rollback and restart tests pass.

### 9.8. PendingLocal implementation contract (design gate)

The implementation will add `CommitPending()` and `FreezePending()` without silently changing the existing immediate-publication semantics of `Commit()`. Multiple `CommitPending()` calls may replace one another only for the same local LWW field key and only while unpublished. `FreezePending()` assigns the final ChangeId/OutboxSeq and makes the payload immutable. Existing `Commit()` remains immediate; when staged entries exist it merges the staged set with current dirty writes into one freeze. Outbox read/lease/high-watermark entry points freeze first, so transport cannot miss committed pending operations. Remote relay and non-LWW operations are never coalesced by this path.

The durable protocol requires distinct WAL records for pending mutation, pending freeze and pending-snapshot commit. The pending snapshot is immutable and generation-specific. Checkpoint writes and fsyncs the snapshot, rotates to a fresh WAL segment, appends a durable marker containing snapshot generation/hash/through-sequence, then truncates only the older sealed prefix. The marker segment remains retained. Recovery requires the referenced snapshot, loads it as the pending base, and replays later pending commits/freezes; a missing/mismatched snapshot fails closed. This avoids both an unbounded pinned WAL prefix and a window in which checkpoint can silently discard pending local payload.

The full crash matrix and required tests are in FLD-DB roadmap §7.15. This is a design contract only; no PendingLocal WAL kind, snapshot or API is implemented yet. Do not report cross-commit coalescing as available until the crash/reopen tests pass. The compatibility baseline is now tested: four ordinary `Commit()` calls produce four immutable Outbox operations, while four Put calls inside one `Commit()` already produce one final operation. GitHub Actions run [42350334573](https://github.com/smersh1307n2/FLD-DB/actions/runs/42350334573) passed the baseline tests. The target remains that four `CommitPending()` calls to one key followed by one freeze produce exactly one final operation.

### 9.9. NodePersistence count and length hardening

The v11 snapshot reader now validates collection counts against both `Int32` range and the minimum encoded bytes available in the remaining body before allocating collections or entering loops. This is applied to namespaces/records, Outbox, peer state and author progress, receipts, aliases, digests, replay mappings, local WAL markers, global ChangeId ledger, record fields/tombstones and causal dependencies. Outbox payload lengths are similarly checked against the remaining body before allocation/copy. Grouped v10 receipt/ledger counts use the same bounded-count validation and still reject empty/duplicate groups. Persisted integer varints are also rejected before narrowing when they exceed `Int32`, and namespace IDs are checked against the `UInt16` domain rather than silently wrapping.

A malformed authenticated snapshot whose namespace count is `UInt64.MaxValue` is covered by a hardening test; its digest is recomputed so the test exercises structural count validation rather than the outer integrity check. This is a local persistence robustness change; no wire format or state version change. GitHub Actions run [42352040484](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352040484) passed .NET 10 build/tests, public samples, coverage and fuzz/lab hardening at commit `add61134362dd0ddf2151f8d75cbadc4b7d119aa`. The follow-up test also recomputes the snapshot digest after injecting an overflowing node epoch and an out-of-range namespace ID.

### 9.10. NodePersistence v12 adaptive receipt grouping

NodePersistence v12 keeps the v10 session grouping key `(issuer, namespace, epoch)` and adaptively chooses one of two encodings inside each session group:

- **Flat:** one receipt count followed by each `BatchId + DurableAuthor + receipt value`. This remains the smaller representation when most authors occur only once in that session group.
- **Author-grouped:** one author-group count, then each `DurableAuthor + receipt count`, followed by `BatchId + receipt value`. This removes repeated 16-byte author IDs when one author has multiple receipts.

The writer chooses author grouping only when the exact raw saving is positive after accounting for the extra subgroup-count varints and the layout byte. For N receipts and K distinct authors, the repeated author bytes potentially removed are `16 × (N-K)`; the writer subtracts the actual varint-count overhead and one layout byte. Singleton-heavy groups therefore remain flat rather than growing from unnecessary grouping. The in-memory `InboxAuthors` key and receipt semantics do not change.

The v12 reader supports both layouts and retains v3-v11 compatibility. Tests cover an author-grouped session, a flat singleton-author session, known/unknown fingerprints, expiry/cursor fields and an explicit v11 migration fixture. GitHub Actions run [42352397093](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352397093) passed build, tests, public samples, coverage and fuzz/lab hardening at commit `8dc03fc79db4dd0bfa43fc449f02441faa309d9f`. No network wire-format change.

### 9.11. Outbox compaction minimum ACK by author

`CompactOutbox` previously evaluated every Outbox entry against every interested peer, giving O(C×P) work for C entries and P interested peers. It now first collects the distinct authors present in the Outbox, computes the minimum safe local OutboxSeq for each author across all interested peers, then performs one linear entry pass. Complexity is O(C + A×P), where A is the number of distinct authors in the namespace.

The calculation preserves the previous retention rules exactly: peers are the union of connected peers, alias/session peers and persisted PeerState peers; no interested peers means keep the log; any interested peer without a PeerState blocks all reclamation; each peer's relative ACK is translated by its per-author StreamOffset; and the minimum across peers is authoritative. Tests cover two peers with unequal ACK progress, reclamation after both ACK, a known peer missing PeerState, and the no-peer keep-log case. Existing snapshot-offset coverage remains in place.

GitHub Actions run [42352564256](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352564256) passed .NET 10 build/tests, public samples, coverage and fuzz/lab hardening at commit `abc14552f115275c31e45f01c5073fb8de956d11`. No wire or persistence format change.

### 9.12. Public per-author safe Outbox ACK query

BIN-SYNC now exposes `Node.GetSafeOutboxAckedThrough(namespaceId, authorNodeId)`. It returns the minimum local OutboxSeq acknowledged by every interested peer for that author stream, translating each peer-relative ACK through its persisted `StreamOffset`. It returns zero when no interested peers exist or when any known interested peer has no durable `PeerState` for the namespace. The query remains valid after Outbox payload entries have been compacted because the per-peer watermarks and offsets are durable Node state.

`CompactOutbox` uses the same aggregation helper, so the query and physical retention decision share one definition of safe progress. This supplies the BIN-SYNC half of the FLD relay ACK contract; the adapter must still persist source `JournalSeq → ChangeId → OutboxSeq` provenance and use that mapping to translate the safe local Outbox watermark into a source JournalSeq. CI run [42352727766](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352727766) passed build, tests, public samples, coverage and fuzz/lab hardening at commit `ce4bc54d658f36bca231ecb8b5205639960d015f`. No wire or persistence format change.

### 9.13. Resolve retained OutboxSeq values for relay provenance

BIN-SYNC now exposes `Node.TryResolveOutboxSequences(namespaceId, identities, sequences)` and the `OutboxChangeIdentity(authorNodeId, changeId)` value type. The adapter can pass the relay operations returned by FLD's `ReadRelayBatchPage` and obtain the local OutboxSeq for each `(author, ChangeId)` without copying or exposing Outbox payload buffers. ChangeIds must be unique within the namespace; a conflicting author for an existing ChangeId is treated as corrupt state. If any requested entry is no longer retained, the method returns false and clears the output; the adapter must not advance `ScannedThroughJournalSeq` or infer a sequence from current record winners.

The adapter's next transaction is to persist the returned `(JournalSeq, ChangeId, author, OutboxSeq)` mapping together with `ScannedThroughJournalSeq` before any downstream send/ACK can cause Outbox compaction. This is not merely an eventual bookkeeping step: the adapter must be the sole owner of transport scheduling for the Node/namespace and keep an outbound publication gate closed until each page's mappings and scan cursor are durably committed, including after restart. Page resolution failure must leave the cursor unchanged. Mapping rows are stored in source JournalSeq order; for one author, OutboxSeq must be monotonic in that order. To translate a safe Outbox watermark, stop before the first source JournalSeq containing any OutboxSeq above it; if no such row exists, the persisted scanned-through cursor is safe. Persist FLD's relay ACK before pruning mapping rows; a crash between those operations leaves only harmless stale rows. Do not hold FLD or BIN-SYNC internal locks across disk/network I/O; use an adapter-level per-namespace gate. This helper plus `GetSafeOutboxAckedThrough` supplies the BIN-SYNC primitives needed to translate downstream peer ACKs back to FLD's per-author JournalSeq. Durable mapping storage and the adapter transaction itself remain open. GitHub Actions run [42353097852](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42353097852) passed build, tests, public samples, coverage and fuzz/lab hardening at commit `1086308da7553ed180daf014d8fe7762505c25ce`. No wire or persistence format change.

### 9.14. Adaptive in-memory Inbox receipt index

`InboxAuthors` is no longer a flat dictionary repeating `(issuer, namespace, epoch)` in every receipt key. `InboxReceiptStore` keeps one-receipt streams in a singleton map and promotes a stream to a BatchId-keyed inner map when a second receipt arrives; when deletion reduces a group back to one receipt, it demotes it again. Thus singleton-heavy workloads do not pay for an inner dictionary per stream, while long-lived sessions avoid repeating the 24-byte issuer/namespace/epoch scope in every key. For a stream with N receipts, raw key material changes from `40×N` bytes to `24 + 16×N`, a format/layout-derived saving of `24×(N-1)` bytes before dictionary/object overhead. This is not yet a measured total-heap saving; benchmark across realistic group cardinalities remains required.

The store preserves the dictionary-like operations used by batch apply, duplicate detection, persistence and tests. WAL replay builds a detached clone before publication and swaps the prebuilt maps into the live node, so no collection growth is introduced during the state publication phase. Tests assert singleton→group promotion, group→singleton demotion, lookup/removal correctness and v12 persistence round-trip. The existing `MaxInboxReceipts` admission cap remains opt-in through `NodeOptions.Limits`; receipts are still not expired or GC'd without a protocol-safe retry horizon.

GitHub Actions run [42353496249](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42353496249) passed build, tests, public samples, coverage and fuzz/lab hardening at commit `c612ce1552a0c7ac444f14661fb7e57fa77c22a2`. No wire or persistence format change.

### 9.15. Compact global ChangeId ledger values

`ChangeLedgerValue` no longer stores a separate Boolean beside its 16-byte fingerprint. Zero is now an explicit reserved marker for an unknown legacy fingerprint; every known fingerprint must be non-zero. The value therefore occupies 16 bytes rather than the previous 24-byte padded layout, saving 8 raw bytes per global ChangeId ledger entry (about 8 MiB at one million entries, excluding dictionary overhead).

The invariant is enforced at both boundaries: `ChangeIdentity.Fingerprint` fails closed if BLAKE3-128 ever returns the reserved all-zero value, and NodePersistence rejects a known entry carrying zero. Unknown legacy markers continue to serialize with the existing known/unknown flag, so the on-disk v12 encoding and state version do not change. This is an in-memory representation optimization, not a network fingerprint or wire-format change.

GitHub Actions run [42353818669](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42353818669) passed build, tests, public samples, coverage and fuzz/lab hardening at commit `6f1ad14c6c90783561f54fe20303385f03de3324`.

### 9.16. Normative route-independent ChangeId semantics (E-24)

BIN-SYNC CORE v2.4.18 E-24 now specifies the implemented global ledger contract: replica-local key `(namespaceId, ChangeId)`; alias/route-independent semantic fingerprint after resolving original author; duplicate suppression without re-apply or re-relay while still committing the new peer's batch receipt/cursor; whole-batch rejection for same-ID/different-fingerprint equivocation; conservative unknown-but-seen migration; and no TTL/ACK-based ledger deletion before a protocol-safe horizon. Local operations register their identity atomically with field state, Outbox and WAL. The current implementation uses BLAKE3-128 as a local fingerprint and reserves zero for unknown legacy markers; the fingerprint is not transmitted and does not change DELTA_BATCH bytes.

E-24 supersedes route-scoped ChangeId dedup as the global authority while retaining the E-13 byte-identical local-state echo guard as an additional filter. The existing cross-route, local self-echo, equivocation, legacy migration, persistence and WAL atomicity tests cover the implementation; independent multi-implementation conformance vectors are still required before claiming interoperability certification. The focused fingerprint canonicalization tests cover alias independence plus author, namespace, RecordId, KeyEpoch, value bytes, causal dependency set, FieldTomb and RecordTombstone semantics. GitHub Actions run [42354862144](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42354862144) passed 609 .NET 10 tests, public samples, coverage threshold and fuzz/lab hardening at commit `b3d65223629080b7488ff431343061ae6fe21e28`.

### 9.17. Fail-closed tombstone retention (E-25)

The protocol specification now explicitly removes any implication that `TOMBSTONE_MIN_TTL` or scalar `GC_NOTICE.TombstoneCutoffWall` authorizes deletion. Safe reclamation requires a durable namespace safe frontier covering all relevant author streams, confirmation by every peer in the current baseline, stale-peer fencing plus mandatory full resync, and an atomic snapshot/feed barrier. A wall cutoff or LastSeen timestamp alone is insufficient. Until those mechanisms are implemented and wired into every incremental/snapshot acceptance path, tombstone/ghost GC remains disabled; `PurgeBelowCutoff` is a no-op. E-25 changes no wire layout.


## 12. BIN-SYNC + Storage implementation update (2026-11-07)

Verified at BIN-SYNC commit `d264d4a9c58998a6ff3baea9458b5f9fb61ab202`:

- `StorageMetadataStore` stores one global ChangeId ledger row containing the 16-byte canonical fingerprint plus one origin byte. The redundant per-ChangeId local-outbound marker key is no longer written. The reader still accepts the legacy marker and folds it into the new row on the next metadata transaction.
- Relay ACK + high-watermark are now one 16-byte per-(namespace, original author) value; local Outbound ACK + high-watermark are one 16-byte per-(namespace, local author) value. The decoder accepts the previous separate 8-byte rows and the next metadata write migrates them to the combined representation.
- `StorageSyncCoordinator` coalesces only unpublished LWW field state within one bounded provider-feed page, selecting the HLC/author winner. It validates every source mutation before coalescing. The source feed is ACKed only after the Node checkpoint and Metadata Store freeze succeed.
- Durable Storage Integration passes 49/49 tests, including legacy ledger/watermark format migration, restart, compaction, PendingApply recovery, and real TCP array synchronization. BIN-SYNC CI passes 642/642 tests, 72.1% line coverage, public samples, Fuzz and Lab.

The logical key/value reduction is 67 bytes per local ChangeId ledger entry (one 68-byte key removed, one value byte added) and 60 bytes per stream watermark pair (one 68-byte key removed, with the value growing from 8 to 16 bytes). These are logical Storage key/value bytes, not a claim about exact on-disk page/WAL savings.

**Still not complete:** the transport still reads Node.Outbox and Node.PeerStates; Metadata Store outbound payloads are still a durable mirror. Per-peer/per-author progress and immutable batch reservations are not yet the transport authority. The bounded source-page coalescing is not a replacement for durable cross-page PendingLocal staging. Do not claim full BIN-SYNC + Storage completion until those authority and recovery gates pass.

**Scope control:** do not start FLD Database implementation or the FLD JournalSeq → BIN-SYNC OutboxSeq provenance adapter in this stage.