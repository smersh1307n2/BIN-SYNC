# Change Staging, Coalescing, and Metadata Retention

**Status:** Architecture decision draft v0.1; proposed storage policy for review before API freeze.  
**Related architecture:** [Metadata Storage and External Data Provider Architecture](METADATA-STORAGE-ARCHITECTURE.md).  
**Protocol baseline:** BIN-SYNC CORE v2.4.17 FINAL. No wire-format change is proposed.  
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

The implementation must make the transition from pending to frozen durable before a transport can send the operation. It is acceptable to freeze conservatively before the first send attempt. It is not acceptable to send first and record the freeze afterward.

If a process crashes after freeze but before the first byte is sent, the operation remains frozen and will be sent on retry. This may retain one extra superseded operation, but it cannot cause divergence.

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
- **Inbox receipts:** retained for the protocol's duplicate-batch/replay horizon.
- **Seen ChangeId/provenance:** retained for the protocol's deduplication and relay/GC horizon.
- **Current field tombstones/ghosts:** retained according to the protocol's causal/GC watermark; not deleted merely because their payload is empty or a newer local operation exists.
- **Provider feed entries:** retained until the Metadata Store has durably staged them or another contractually safe transfer boundary is reached.

No generic DeleteOldMetadata operation is sufficient. Each category needs its own safe-retention condition.

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

**Core rule:** coalesce only operations that have never been externally observable; preserve every operation once it may have been observed. Current state, unpublished staging, frozen outbound history and relay history must have separate ownership and retention rules.