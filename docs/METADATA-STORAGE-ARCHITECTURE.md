# BIN-SYNC Metadata Storage and External Data Provider Architecture

**Status:** Architecture contract draft v0.4 — includes the two-assembly FLD Storage boundary; ChangeId retention representation is under renewed size/correctness review.  
**Scope:** separation of synchronization runtime, external data providers, and durable metadata storage.  
**Wire format:** unchanged. This draft defines target durable metadata and cross-peer dedup semantics; the latter must be promoted into a normative protocol erratum and conformance tests before release.  
**Implementation status:** proposed architecture; the current Node API is not yet an implementation of this contract.

## 1. Purpose

BIN-SYNC is a synchronization protocol and runtime, not a database. It does not, by itself, provide durable persistence. Any state that must survive process restart must be stored by a durable storage implementation supplied by the host application or by an adapter.

The storage boundary must not assume that synchronized user data is stored in FLD-DB, or even in a database. Valid data sources include FLD-DB, SQL/NoSQL databases, files, in-memory collections with an application-owned persistence mechanism, arrays, domain objects, and custom stores.

The purpose of this architecture is to define a stable separation between:

1. **BIN-SYNC Runtime** — protocol, synchronization decisions, validation, sequencing, deduplication semantics, peer state transitions, and coordination.
2. **Data Provider** — maps a concrete user data source to the canonical BIN-SYNC data model, observes local changes, reads source state, and applies accepted remote changes.
3. **Metadata Store** — durably stores the synchronization metadata and journals required to resume, deduplicate, relay, acknowledge, and recover synchronization.
4. **Storage/Data Adapter** — connects a concrete provider or metadata engine to the neutral contracts without making BIN-SYNC depend on a particular database.

FLD-DB is a candidate first-party implementation of the Metadata Store contract. It may also act as the Data Provider when the user's actual data is stored in FLD-DB. These are distinct roles and must remain distinct in the architecture and API.

## 2. Non-negotiable principles

1. **BIN-SYNC is not a database.** Its in-memory collections and caches are runtime state, not durable storage.
2. **No implicit persistence.** Restart-surviving state is persisted through an explicit contract. A host may choose volatile storage, but then restart recovery guarantees are correspondingly unavailable.
3. **No FLD dependency in BIN-SYNC Core.** Core defines neutral contracts and protocol semantics. FLD-specific implementations live in an adapter/package that depends on both sides.
4. **Data and metadata are separate concerns.** The physical location and representation of user data must not dictate the metadata storage implementation.
5. **Atomic protocol acceptance.** A batch must not be acknowledged until its required data effects and protocol receipt/state are durably recoverable.
6. **No false cross-store atomicity.** Two independent stores do not become one transaction merely because BIN-SYNC coordinates them. External providers must supply a supported atomic or recoverable apply strategy.
7. **No recursive synchronization by default.** A metadata store used by BIN-SYNC is infrastructure. Its internal writes must not automatically become user changes in the same BIN-SYNC scope.
8. **Preserve original authorship and change identity.** Relay, replay, retries, and storage migrations must not regenerate the original ChangeId, author, HLC, or other protocol identity.
9. **Keep the hot path lean.** Storage operations should support bounded batches, efficient scans/visitors, stable snapshots, and low-allocation representations. Events and diagnostics must not be required for correctness.
10. **Contracts define guarantees, not a particular physical layout.** A custom implementation may use a relational database, append log, embedded engine, or another durable mechanism.

## 3. Logical architecture

The intended dependency direction is:

    Application
       |
       +---- Data Provider ----------------------+
       |       |                                 |
       |       +-- source reads/writes           |
       |       +-- local change feed              |
       |       +-- canonical record mapping      |
       |                                         |
       +---- BIN-SYNC Runtime -------------------+
               |                                 |
               +-- protocol semantics            |
               +-- peer/session coordination    |
               +-- validation and sequencing    |
               +-- durable state commands       |
                                                 |
                                      Metadata Store contract
                                                 |
                                  +--------------+--------------+
                                  |              |              |
                              FLD adapter   Custom adapter   Other store

The runtime coordinates the provider and metadata store. The provider does not own wire-protocol semantics. The metadata store does not interpret wire frames or independently decide synchronization policy.

The provider and metadata store may be implemented by the same physical engine. For example, FLD-DB may atomically store both user field state and synchronization metadata. Physical co-location does not merge their logical responsibilities.

### 3.1. Data Provider contract

A Data Provider is responsible for adapting a user source to the BIN-SYNC canonical model. It must define:

- a stable mapping from source entities/items to canonical RecordId values;
- a stable mapping from source properties/fields to canonical FieldId values;
- canonical byte representation for field values and tombstones;
- how local source changes are discovered through a durable feed or equivalent change-capture mechanism;
- how source snapshots are read consistently, including a stable snapshot token/version and the corresponding change-feed watermark needed to continue with an exact tail;
- how accepted remote changes are applied;
- how provider-specific validation, schema mapping, and source constraints are enforced;
- how local and remote operations are distinguished so remote apply does not reappear as a new local change.

The current BIN-SYNC wire/data model is not an arbitrary object serializer. A provider must map its source to the canonical BIN-SYNC record/field model. For example, an array can be represented as records with stable element identifiers and fields; a domain object can be represented by a stable record identity and field identifiers. The provider chooses and documents that mapping.

The provider must not use transient memory addresses, array indexes that can shift, or process-local hash codes as persistent item identities unless the application guarantees their stability.

A provider may expose a native durable change feed. If it does not, the integration must arrange durable change capture before the source can discard or overwrite the corresponding change.

### 3.2. Metadata Store contract

The Metadata Store is responsible for durable synchronization metadata, not for interpreting user data semantics. It stores the minimum state needed by BIN-SYNC to continue safely after restart.

The store must expose semantic atomic operations or transactions. A generic key/value CRUD interface alone is insufficient because correctness depends on atomic transitions across several metadata categories.

A Metadata Store may also retain canonical change payloads when required for durable outbound delivery or relay. It must not be forced to store the entire current user dataset.

A low-level embedded engine and the BIN-SYNC Metadata Store are separate layers. The engine provides atomic byte-key/value batches, durable logs, recovery, snapshots and ordered scans. The Metadata Store adapter implements BIN-SYNC-specific receipts, ChangeId ledger, peer cursors, Outbox/relay retention and semantic transactions on top of that engine. The adapter must not be confused with the physical storage engine.

### 3.2.1. FLD Storage Core extraction boundary

FLD-DB's reusable Storage Core is a domain-neutral byte engine, not the FldDatabase facade and not a BIN-SYNC-specific store. It ships as the independent `Fld1.Storage.dll`; FLD field semantics ship as `Fld1.Database.dll`, which references Storage. Storage has no reverse dependency. A third Common assembly is not part of the current design.

- Storage Core owns WAL, recovery, atomic batches, immutable runs/pages, manifest/checkpoint publication, snapshots, ordered scans, storage-native commit IDs, checksums and safe generation reclamation.
- FLD Database owns RecordId/FieldId, HLC, LWW/lifecycle, local pending staging, frozen Outbox, remote APPLY and field APIs.
- BIN-SYNC owns ChangeId/BatchId semantics, peer/author cursors, inbox/relay/dedup policy and protocol GC.
- The BIN-SYNC↔FLD adapter depends on both the neutral Storage API and BIN-SYNC contracts; FLD Storage Core does not depend on BIN-SYNC or FldDatabase.

The Storage API uses opaque byte keys/values and storage-native identifiers. It must not expose field, author, HLC, ChangeId, BatchId, peer or protocol DTO types. The same physical WAL/run/manifest engine should serve FldDatabase and Storage-only consumers; Storage-only mode must not create synthetic field records, LocalAuthor, HLC or Outbox entries. See the FLD-DB [Storage Core architecture](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/FLD-STORAGE-CORE-ARCHITECTURE.md).

### 3.3. BIN-SYNC Runtime

The runtime owns:

- wire protocol and canonical encoding/decoding;
- peer/session state machines and peer-specific cursor semantics;
- batch identity, digest-chain, inbox and duplicate rules;
- ChangeId deduplication and provenance policy;
- author stream sequencing and relay policy;
- ACK validation and safe-prefix calculation;
- reconciliation, digest, IBLT, snapshot and GC protocol decisions;
- deciding which changes are eligible for outbound delivery or relay;
- producing protocol-state deltas/commands for durable commit.

The runtime may cache loaded metadata in memory. Such caches are disposable and must be reconstructible from the Metadata Store. A cache must never become a second, independently authoritative durable copy.

## 4. Identity and scope model

The architecture must not conflate the following identities:

| Identity | Meaning | Owner |
|---|---|---|
| Metadata store instance | Physical/logical storage service or database | Store implementation |
| Sync scope | One logically independent synchronized dataset/configuration | Host/application |
| Replica identity | BIN-SYNC NodeId for one participating replica | BIN-SYNC / host configuration |
| Protocol namespace | Existing 16-bit BIN-SYNC namespace within a scope | BIN-SYNC protocol |
| Source item identity | Stable identity of a provider item mapped to RecordId | Data Provider |
| Field identity | Canonical 16-bit FieldId | Data Provider/schema |
| Change identity | Stable 128-bit ChangeId | Originating change mechanism |

A single Metadata Store instance may host multiple independent sync scopes. Each scope must be isolated from all others, including inbox keys, peer state, Outbox cursors, relay retention, deduplication and GC state.

The store's tenancy/scope key is not the BIN-SYNC 16-bit NamespaceId. NamespaceId remains the protocol field defined by the wire specification. A metadata implementation must include sufficient scope and replica context in its durable keys so that two applications can safely share one physical store.

A scope may contain multiple protocol namespaces. The same NodeId may participate in several scopes, but state must never leak between them.

Source item identity must be stable across process restarts and provider re-open. The provider maps that identity to the canonical RecordId. FLD RecordId/FieldId are not universal Metadata Store keys and must not leak into the neutral contract as mandatory storage primitives.

## 5. What must be persisted

Persisted state is divided by purpose. This classification is logical; an implementation may encode several categories in one compact journal or transaction.

### 5.1. Replica and scope metadata

Persist the durable identity/configuration required to reopen the replica consistently, including NodeId binding, node incarnation/epoch, and the local HLC clock state (LastWall/LastLogical) or an equivalent monotonic-clock recovery value. A restart must not silently roll back the local HLC behind a previously committed local operation. Persist local author-stream sequence allocators separately from physical WAL/commit sequence numbers.

Do not persist transient transport objects, sockets, session handles, callbacks, or live locks.

### 5.2. Local change tracking and outbound journal

For each locally authored change that has crossed the publication boundary and become a frozen Outbox entry, retain enough information to reproduce the exact canonical operation after restart:

- stable nonzero ChangeId;
- original author identity;
- canonical source identity (RecordId/FieldId after provider mapping);
- operation kind and immutable value/tombstone payload, or a durable provider reference whose lifetime is guaranteed;
- HLC and key epoch where applicable;
- immutable author-stream sequence/cursor;
- causal/dependency metadata when enabled;
- source/provider version or receipt needed to avoid losing or duplicating a source change.

Before publication, a separate pending-staging representation retains the latest eligible local operation per coalescible key. It has no stream sequence yet and may not need a protocol ChangeId until freeze. Its payload/reference must remain recoverable independently of the visible current-state winner. See [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md).

A cursor without a recoverable payload is not a durable outbound journal. If the provider supplies a native durable change feed, the runtime may consume it under a lease and need not duplicate the full payload in the Metadata Store, provided the feed payload/reference remains valid until the pending operation is durably staged or frozen.

The implementation must choose one authoritative outbound journal for each change stream. It must not maintain two independently advancing journals with ambiguous ownership.

The local change lifecycle has two durable stages before acknowledgement: (a) coalescible pending local changes that have never been exposed to any peer, and (b) immutable frozen Outbox entries that may have been sent and therefore must never be rewritten. For LWW_FIELD, pending changes may be coalesced per exact canonical field key; frozen or potentially published entries may not. Sequence numbers are assigned at freeze, not while an operation remains coalescible. The detailed rules, policy matrix, current FLD behavior, payload ownership and test requirements are defined in [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md).

### 5.3. Incoming batch acceptance and deduplication

Persist the state required to recognize a previously accepted batch and safely answer a retry after a lost ACK. This includes the relevant sender/issuer identity, namespace, epoch, BatchId, accepted cursor range, BatchDigest, RecordsRoot, PreviousBatchDigest/chain checkpoint, author-bound duplicate ACK result, and protocol-defined receipt expiry/confirmation state. A compact receipt must be able to reject reuse of the same idempotency key with conflicting cursor/digest metadata without retaining the original full field payload.

Cross-peer deduplication must be independent of the immediate sender: relay does not create a new operation identity, and a duplicate arriving through another route must not be applied or relayed again. The previously proposed exact ledger keyed by `(scope, localReplicaId, namespaceId, ChangeId)` with a 128-bit canonical-operation fingerprint (32 logical bytes per operation before indexes/page overhead) is a correctness baseline, NOT an approved final storage layout. Its semantics remain the reference behavior: same identity/same operation is a duplicate; same identity/different operation is a conflict; a new sender's batch receipt and receive cursor are still committed atomically. The operation payload must not be copied into the dedup index.

The current Node SeenChanges key is peer-scoped and also contains local WAL replay markers. This is a legacy implementation detail, not the target key model. During v5 migration, entries whose peer key equals the persisted local NodeId remain local WAL replay markers; other peer-scoped entries collapse into one `(namespaceId, ChangeId)` legacy marker within the owning `(scopeId, localReplicaId)` partition, with an unknown fingerprint. If the persisted local NodeId is unavailable, retain the old representation until identity is resolved rather than guessing. Keep local WAL replay markers separate and reclaim them only after the corresponding WAL record is irreversibly truncated or a durable replay watermark makes reapplication impossible.

The current protocol has no proven global snapshot barrier or immutable per-operation origin sequence on the wire. The 80-byte SnapshotTail names digests but does not define an explicit per-author checkpoint vector or a snapshot-completion ACK contract; the current runtime DTO also omits SnapshotTail. It cannot be treated as a cursor/retention proof. Consequently, the exact-ledger baseline has no safe TTL or ACK-only GC rule in this protocol revision. Do not reclaim dedup facts using a peer cursor, local relay ACK, Inbox expiry, tombstone GC or arbitrary TTL; a Bloom filter cannot be the sole source of truth.

**Additional candidate — content-derived ChangeId:** the current specification defines ChangeId as a 128-bit idempotency key but does not require it to be random. Evaluate deriving it as `BLAKE3-128(domainTag || canonicalOperation)`, where canonicalOperation includes namespace, record, operation kind, permanent original author identity, HLC/key epoch and canonical value/tombstone bytes; transport aliases must be resolved before hashing, and any semantic causal metadata must either be included or explicitly excluded by contract. The receiver recomputes and validates the ID before mutation. If the hash is the identity commitment, a same-ID/different-payload reuse fails validation (apart from a cryptographic collision), so the dedup index may be a key-only 16-byte exact set instead of a 16-byte ID plus 16-byte fingerprint. This halves the logical per-operation index payload, but does NOT solve unbounded retention. It also changes ID semantics and must be checked against provider-supplied IDs, API guarantees, encrypted payloads, snapshots (which currently synthesize fresh IDs and do not preserve original per-field ChangeId), causal trailers and legacy peers. Snapshot baseline records may need a distinct identity namespace or must stop pretending to be original published operations. Do not silently apply it to existing ChangeIds.

**Optimization investigation (open; do not implement either representation yet):** The protocol specification describes an immutable `u64 Seq` per `(authorNodeId, namespaceId)`, but that sequence is not carried as an explicit per-operation origin identity in DELTA_BATCH. Do not assume the current FLD `OutboxSeq` is already this identity: its implementation describes a namespace cursor, and relay persistence must be audited separately. The batch cursor is peer-relative (`StreamOffset`), so it cannot be reused as a global operation ID. Evaluate a protocol extension that carries a stable origin sequence per operation and maintains receiver-side contiguous high-watermarks plus compact sparse gaps/ranges. This can replace per-operation retention only if the protocol proves that a sequence prefix is complete and that no older operation can later arrive through another route, including after snapshot/bootstrap and relay. Also decide explicitly whether old-ID equivocation must remain detectable: a watermark alone can suppress an old sequence but cannot compare its original payload fingerprint. If conflict detection is mandatory, the design needs an authenticated origin commitment/hash-chain or retained per-operation fingerprint; do not silently weaken the current same-ID/different-payload rejection rule. A snapshot checkpoint vector is useful only with a two-sided completion/fencing contract and explicit coverage of relay-relevant operations, including LWW losers. Until those proofs exist, exact retention is the safe fallback.

See [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md) for the implementation and migration details.

### 5.4. Peer and stream state

Persist peer-specific state that affects future protocol decisions, including LastPushed/LastReceived/LastAcked where used, per-author StreamPushed/StreamAcked/StreamOffsets, received-stream and batch replay mappings, InitialSyncComplete, peer incarnation/epoch, membership epoch, and durable membership/retention facts. Preserve author-stream identity; one scalar cursor per peer or namespace is insufficient.

A peer's cursor mapping or ACK must not alter another peer's stream view. Unknown or newly introduced peers must not silently inherit an ACK that allows deletion of data they have not received.

### 5.5. Relay journal

Remote changes that are accepted for relay must remain recoverable until the relevant downstream peers have safely acknowledged them.

The relay journal must preserve original change identity, author, HLC, key epoch, operation and payload/reference. It must include operations selected for relay even when they lose the local LWW comparison, where required by BIN-SYNC relay semantics.

The relay journal is distinct from the local-origin outbound journal. Their cursors, ownership and retention rules must not be conflated. The exact ChangeId ledger remains the correctness fallback for duplicate suppression after Outbox/relay payload compaction; the relay journal retains payload only until safe downstream ACK. Whether the fallback requires a permanent per-operation fingerprint or can be replaced by a proven origin-sequence/checkpoint representation remains open. ACK may reclaim payload but cannot by itself reclaim dedup identity.

The durable batch receipt and relay payload journal also have independent lifetimes. A duplicate-batch receipt may expire under the Inbox GC rule while relay payload remains needed by downstream peers; relay ACK may complete while the receipt's duplicate-retry horizon has not elapsed. Do not store them in one indivisible record/file unless the physical layout supports independent reclamation. FLD now uses ApplyCommitCodec v3: each incoming operation is encoded once with compact relay-selection indexes, and ApplyJournal stores receipt metadata plus only relay-selected payloads. The full WAL commit is flushed before the relay-only journal materialization, so the latter is durable before WAL truncation. Receipt and relay payload are still co-located in one append-only journal and need independent physical GC/index lifetimes. See [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md).

### 5.6. Optional protocol state

Optional features may require additional durable state, such as causal dependency tracking, schema evolution state, trust/key epochs, or GC/membership checkpoints. Each feature must declare:

- whether its state is durable or reconstructible;
- its scope and identity keys;
- the atomic operation that updates it;
- retention and migration rules;
- what happens when the feature is enabled after data already exists.

Secrets and private keys should use an appropriate secret/key store. The Metadata Store may retain non-secret key identifiers, epochs, fingerprints, and protocol references, but must not become an implicit secret-management system.

### 5.7. Existing NodePersistence inventory to account for

The current NodePersistence v5 is a useful completeness checklist, not a schema that the new Metadata Store must copy. The new boundary must account for each semantic category below and explicitly classify it as durable, reconstructible, or intentionally ephemeral:

- node identity/incarnation and HLC clock state;
- current replicated field state, lifecycle and tombstones;
- local Outbox entries and per-(author, namespace) sequence allocators;
- Inbox idempotency keys and InboxAuthors needed to reproduce the correct author-bound duplicate ACK; the wire spec also defines cursorStart/cursorEnd/appliedAt/expiresAt, while current NodePersistence v5 serializes the key set and related stream checkpoints rather than the complete InboxEntry tuple, so the durable expiry/range lifecycle must be explicitly preserved or safely derived;
- LastBatchDigest checkpoints and per-stream digest-chain checkpoints;
- peer state: received-stream mappings, StreamPushed, StreamAcked, StreamOffsets, batch range → BatchId/previous-digest replay mappings, initial-sync status, membership epoch and applicable last-seen/retention data. The replay mapping is needed only while its range may be retried and can be pruned after a valid per-author ACK covers that range end; the current inspected ProcessAckInternal path does not appear to prune PeerState.Ids, so this is a concrete metadata-growth item;
- cross-peer dedup semantics are required; exact ChangeId + canonical fingerprint is the safe fallback, while origin-sequence/range compaction is under protocol-level investigation;
- LastStreamAuthor mappings;
- durable author registry and known author epochs;
- peer-wide session/incarnation epochs, distinct from namespace-scoped epoch watermarks;
- quarantine and forbidden-namespace policy state;
- schema-version state;
- peer identity registry keys required to reconnect/rebuild live peer objects.

The current NodePersistence v5 also serializes AliasToNode and NodeToAlias tables per peer. These are session-negotiated mappings, but durable author resolution and relay must survive restart. Before dropping the serialized tables, the implementation must prove that the required mapping can always be reconstructed from the durable author registry and a completed handshake before any operation needing that mapping is replayed. Otherwise persist the semantic mapping/registry state, not necessarily the exact in-memory dictionaries.

This inventory is deliberately exhaustive: a future storage adapter must not accidentally omit a less visible checkpoint merely because it is not part of the main data Outbox. Conversely, it must not persist an entire runtime object graph when a smaller semantic fact is sufficient.

### 5.8. State that should normally remain ephemeral

Do not persist live transport objects, sockets, session handles, callbacks, temporary encode buffers, diagnostics, transient locks, active tasks or rebuildable caches merely because they exist in the current Node implementation.

If replay requires information currently held only in an ephemeral structure, normalize and persist the required semantic result at the durable acceptance boundary rather than blindly persisting the entire session object.

## 6. Required atomic operations and guarantees

The contract is defined by atomic semantic transitions, not by a prescribed file format or SQL schema.

### 6.1. Accept incoming batch

One accepted incoming batch must transition all relevant durable state together:

1. record the batch receipt/idempotency key;
2. advance the appropriate sender/namespace/epoch receive cursor and digest-chain checkpoint;
3. apply or durably stage the provider data changes;
4. update ChangeId deduplication/provenance state;
5. append all relay-eligible operations;
6. persist any related protocol-state delta;
7. make the committed result recoverable before the runtime sends ACK.

A successful durable acceptance must mean that a restart can recover the same logical outcome and answer a duplicate retry correctly. Partial publication is forbidden.

The exact data-provider coordination strategy is defined in §7. The Metadata Store must not claim atomicity over an external source that it cannot control.

### 6.2. Freeze and read outbound changes

The store/provider must expose an atomic freeze transition that converts a stable set of eligible pending local changes into immutable Outbox entries, assigns permanent per-author/per-namespace sequence numbers and stable ChangeIds, and durably commits the transition before any transport can expose those entries to a peer. Pending LWW changes may be coalesced before this transition according to the separate change-retention contract.

The runtime then reads a bounded, stable batch of **frozen** outbound entries with immutable identities and payloads/references. Enumeration must not require holding a global writer lock while network I/O is performed. A frozen entry remains byte-identical across retries, even when no ACK has yet been received.

If multiple consumers may read the same stream, the store/provider must support fencing or an equivalent ownership mechanism so that a stale consumer cannot ACK after a newer consumer takes over.

### 6.3. Acknowledge outbound changes

ACK processing is a protocol decision owned by BIN-SYNC. The Metadata Store receives only a validated safe contiguous prefix of frozen entries or equivalent commit instruction. ACK does not authorize rewriting an entry; it authorizes retention/compaction only after all applicable peer and protocol conditions are met.

The store must reject out-of-range, non-contiguous, stale-lease or stale-generation ACKs. It must never infer a safe prefix by looking at one peer when the protocol requires aggregation across all interested peers.

### 6.4. Read and acknowledge relay changes

Relay enumeration and acknowledgement must be independent of the local-origin outbound stream. Relay data is retained until the runtime's safe-retention calculation permits deletion. Stale relay consumers must be fenced.

### 6.5. Snapshot and recovery

The store must provide a consistent read view for any operation that compares or exports metadata. A snapshot must remain valid for its documented lifetime while concurrent commits/checkpoints/compaction occur. Independently, the Data Provider must expose a source snapshot tied to a stable version/change-feed watermark. The runtime must be able to establish a baseline at that watermark and then consume every later source change without a gap. A provider that cannot provide this boundary must use an equivalent write barrier or durable journal protocol; taking an arbitrary source snapshot and separately reading an unrelated metadata cursor is not sufficient.

A state snapshot is not an acknowledgement of every frozen Outbox or relay operation that predates its source watermark. The current 80-byte SnapshotTail contains three digests but does not define a per-author checkpoint vector or a snapshot-completion ACK/fencing contract; the current runtime DTO omits SnapshotTail. It is therefore an integrity descriptor only for now, not a stream cursor or retention proof. Snapshot completion MUST NOT advance Outbox/relay safe-ACK, ChangeId-ledger GC or peer stream offsets, and MUST NOT discard pending/frozen changes. A future snapshot-baseline protocol must explicitly bind the snapshot state to per-author stream cuts and prove completion before any such compaction.

The current BIN-SYNC SnapshotChunkMessage/BuildSnapshotChunks path is a current-state LWW snapshot and does not expose a per-field ChangeId or Outbox cursor in the inspected DTO. Until the protocol adds a verifiable snapshot-baseline barrier, a new peer must not be treated as having received old operation history merely because it installed current field state; the sender must retain/reconcile the required author streams or require a full rebootstrap path.

Recovery must be deterministic. A store must not expose a state where an inbox cursor is advanced but the corresponding accepted data/relay state is missing, or vice versa.

## 7. Data and metadata transaction boundary

This is the most important integration rule.

### 7.1. Preferred: one atomic transaction domain

When the Data Provider and Metadata Store share one transactional engine, the provider data mutation and all required metadata updates should commit in one transaction. This is the strongest and simplest model.

FLD-DB used as the synchronized data database is intended to follow this model: field state, local Outbox, incoming receipt/inbox state, deduplication and relay journal must be committed under the FLD durability boundary according to the specialized FLD integration contract.

### 7.2. Separate stores: recoverable apply protocol

When user data and metadata live in different stores, a global atomic transaction must not be assumed. A safe integration requires a durable idempotency/recovery protocol.

At minimum:

1. BIN-SYNC persists a pending apply intent with a stable apply token and enough information to retry.
2. The Data Provider applies the remote operation idempotently and durably records that token/receipt atomically with the user data change.
3. BIN-SYNC finalizes the metadata transaction: incoming receipt/cursor, deduplication, relay journal and protocol delta.
4. After restart, an unfinished intent is queried/retried with the same token. The provider must return the prior durable result rather than applying the operation twice.

The provider's idempotency receipt must be committed atomically with the source data effect. Merely keeping an in-memory set of processed tokens is insufficient.

A provider may instead expose a transaction coordinator or a durable transactional inbox/outbox that gives equivalent guarantees. The exact mechanism is provider-specific.

### 7.2.1. ProtocolStateDelta lifecycle

`ProtocolStateDelta` is a bounded, versioned recovery envelope for the metadata mutations associated with one incoming batch. It is not a second long-term database, a serialized Node snapshot, or a permanent duplicate of Inbox/SeenChanges/peer state.

The target lifecycle is:

1. BIN-SYNC constructs a small delta containing only the metadata mutations for this batch.
2. In a shared provider/metadata transaction, the runtime materializes those mutations as normal indexed metadata records atomically with the provider data effect and Inbox receipt. The raw delta is not retained after commit.
3. With separate stores, the raw delta is retained only inside the durable pending-apply intent while the provider idempotently applies the batch and the Metadata Store finalizes its metadata transaction.
4. Once both sides are durably finalized, remove the raw delta/intent payload. Keep the materialized metadata records and compact Inbox receipt.
5. A duplicate BatchId returns the compact original receipt/ACK result; it does not return or reconstruct the full ProtocolStateDelta.

The delta must not contain a full Node/PeerStates/ChangeIdLedger image. A normal batch updates only bounded records affected by that batch. If a store cannot materialize and query the resulting metadata after restart, its Metadata Store contract is incomplete; retaining an ever-growing raw delta log is not an acceptable substitute.

### 7.3. Unsupported weak mode

If the provider cannot offer a shared transaction, durable idempotent apply, or an equivalent recoverable protocol, the integration cannot promise crash-safe exactly-once effects. It may still offer weaker best-effort or at-least-once behavior, but that mode must be explicit, must not send an ACK that implies stronger durability, and must document possible duplicate application or reconciliation requirements.

### 7.4. Local source change capture

The reverse direction has the same issue. If the provider reports a local change, it must not discard that change until the canonical operation is durably represented in pending staging or a durable provider-owned feed under an agreed transfer boundary. Pending LWW changes may be coalesced only before freeze; non-coalescible operations remain distinct. A provider-native change feed with durable retention and consumer fencing is acceptable. A transient event callback alone is not a durable change feed. See [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md).

## 8. FLD-DB as a Metadata Store

FLD-DB should be considered a first-party candidate for a reusable Metadata Store implementation, including when the user's actual synchronized data is stored elsewhere.

In this mode, FLD-DB is used **only as storage infrastructure**. It does not own the external user's data and does not become a Data Provider for that data.

### 8.1. Required Storage-only role

The Storage-only role must be an explicit, first-class API/engine role. It must not be implemented as a collection of loosely related flags such as disabling Outbox or suppressing HLC in an ordinary synchronized database.

The role must reuse suitable FLD physical-storage capabilities where practical:

- WAL durability and crash recovery;
- group commit;
- immutable runs and compaction/checkpoint;
- stable snapshots;
- bounded batch writes and scans;
- concurrency and writer ownership guarantees.

But it must not implicitly perform FLD's synchronized-data semantics:

- no automatic BIN-SYNC NodeId/LocalAuthor binding;
- no protocol-level HLC generation for metadata writes;
- no automatic ChangeId generation;
- no local synchronization Outbox;
- no FLD-native automatic relay-journal behavior; BIN-SYNC relay records may still be stored as ordinary Metadata Store payloads through the neutral contract;
- no automatic BIN-SYNC event/change publication;
- no recursive inclusion of metadata writes in the same synchronized dataset.

FLD may use internal physical commit/version numbers for WAL, recovery, snapshots and compaction. Those are storage-engine mechanics and must not be exposed or confused with BIN-SYNC HLC, ChangeId, author sequence or peer cursor.

### 8.2. Reuse without semantic leakage

The preferred direction is to separate FLD's reusable physical storage primitives from the synchronized field-store behavior. A dedicated Storage-only facade/engine profile may reuse the same WAL/run/manifest implementation, but it must expose only the semantics that are valid for metadata storage.

Do not force the ordinary FldDatabase API to represent metadata-only writes by fabricating authors, HLCs, ChangeIds or local Outbox entries. Do not create a second physical engine if the existing core can safely serve both roles; equally, do not compromise the synchronized database semantics merely to share one public class.

A specific FLD design constraint must be resolved before implementation: the current FLD1 run model stores HLC and AuthorNodeId alongside field values because it is designed for synchronized field state. Storage-only metadata writes must not acquire protocol meaning from those fields. If the physical format still needs an ordering/version token, it must be explicitly storage-native and isolated from BIN-SYNC identity/comparison semantics; otherwise the storage-only path needs a representation that does not require synthetic protocol authors or HLC values. Reusing the current Put/Commit path with fabricated identities is not an acceptable shortcut.

Whether the implementation should be a dedicated FldStorage API, a separate FldMetadataStore adapter, or an internal storage-core facade is an implementation decision to validate against FLD's current architecture. The semantic separation is mandatory; the exact public type name is not yet fixed.

### 8.3. Metadata layout

The neutral contract must not require FLD's RecordId/FieldId layout. The FLD adapter may map scope, replica, peer, item and journal keys onto FLD's native key/value representation.

Prefer compact, bounded binary records and batched updates over many tiny allocations or one physical record per scalar. Keep independently updated state independently addressable where that avoids rewriting large blobs. Group related state when atomicity or scan locality requires it.

No concrete record layout is approved by this document. It must be designed after the operation model, access patterns, retention and transaction boundaries are settled. The current FLD run format is namespace-scoped, while a generic Metadata Store may host many independent sync scopes and namespaces. The FLD adapter must define an explicit, collision-free mapping or physical partitioning strategy; it must not treat the wire NamespaceId as the complete store tenancy key.

### 8.4. Two FLD usage modes

| Mode | User data | Synchronization metadata | FLD role |
|---|---|---|---|
| FLD synchronized database | Stored in FLD | Stored atomically with field state | Data Provider + Metadata Store implementation |
| External source + FLD metadata | Stored in SQL/files/arrays/custom provider | Stored in FLD Storage-only mode | Metadata Store only |

The two modes may share physical storage primitives, but they must not share accidental semantics. In particular, the second mode must not create an FLD local author or an FLD Outbox for its metadata records.

## 9. Custom Metadata Store implementations

A custom Metadata Store must be implementable without referencing FLD types, file formats, run concepts, WAL internals or FLD identifiers.

The contract must specify observable guarantees:

- durable commit boundary and what successful completion means;
- atomicity of each semantic operation;
- isolation/concurrency behavior;
- bounded enumeration and stable snapshot semantics;
- lease/fencing behavior where multiple consumers are possible;
- cursor monotonicity and non-reuse;
- retention and safe deletion conditions;
- recovery behavior after interruption;
- format/schema versioning and migration behavior;
- maximum supported payload/batch sizes and failure behavior.

A custom store may internally use a relational transaction, an append-only log, a document store, an embedded database or a purpose-built engine. It is not required to mimic FLD's physical layout.

A volatile in-memory implementation may be provided for tests and ephemeral use, but it must advertise that it provides no restart durability.

## 10. Package and dependency boundaries

The intended package direction is:

- **BinSync.Core** — protocol/runtime and neutral provider/metadata contracts.
- **Provider adapters** — map concrete user sources to the canonical model.
- **Metadata adapters** — implement the durable metadata contract.
- **FLD adapter** — references BinSync.Core contracts and FLD engine APIs; neither core project references the other.
- **Host application** — selects and composes a provider and a metadata store.

A host should be able to use a custom Data Provider with FLD as Metadata Store, FLD as Data Provider with a compatible metadata implementation, or custom implementations for both.

Do not put FLD-specific DTOs, RecordId types, WAL concepts, or package references into BinSync.Core.

## 11. API design constraints

This document intentionally does not freeze public C# interface signatures. The following constraints apply when the API is designed:

1. Prefer semantic operations over exposing generic CRUD as the correctness boundary.
2. Avoid one enormous interface with many optional parameters and FLD-specific members.
3. Avoid a proliferation of tiny interfaces unless they correspond to real optional capabilities with a concrete consumer.
4. Transactions or atomic commands must cover the complete logical transition, not just individual key writes.
5. Use typed stable identifiers and explicit scope/replica context.
6. Use bounded batch APIs and streaming/visitor patterns where they materially reduce allocations and copying.
7. Use ValueTask or synchronous fast paths only where supported by actual hot-path measurements; do not add async abstraction overhead by default.
8. Caller-owned buffers, borrowed memory and snapshot lifetimes must have explicit ownership rules.
9. Store implementations must not invoke user callbacks while holding internal locks.
10. Events are notifications after committed transitions, never the durability mechanism or a required step in the hot path.

A possible API shape is one core Metadata Store contract plus an explicit transaction/atomic-operation surface. Optional capabilities may be added only when a real implementation or runtime feature requires them. Exact names and signatures remain open.

## 12. Explicitly prohibited designs

- BIN-SYNC Core directly referencing FLD-DB.
- Treating BIN-SYNC's current Node dictionaries as durable state.
- Connecting the current Node to FldDatabase through a sequence of non-atomic calls and assuming the result is crash-safe.
- Storing only cursors while the corresponding outbound/relay payload can disappear.
- Treating local Outbox and relay journal as the same queue.
- Generating new ChangeIds, HLCs or authors during replay, retry, remote apply or relay.
- Treating FLD CommitSeq/WAL offsets as BIN-SYNC author-stream cursors.
- Using a single scalar cursor for multiple independent author streams or peers.
- Persisting session aliases as a substitute for resolved original author identities.
- Making FLD Storage-only writes generate FLD local Outbox entries.
- Assuming a transaction across independent provider and metadata stores.
- Acknowledging remote data before the provider effect and protocol receipt are durably recoverable.
- Making a metadata store synchronize itself by default.

## 13. Acceptance scenarios

The architecture and eventual contract implementation must be validated against at least these scenarios:

1. **FLD as synchronized database:** local write, restart, outbound delivery, remote apply, lost ACK retry, relay, checkpoint and compaction preserve all existing BIN-SYNC invariants.
2. **SQL provider + FLD metadata:** user data remains in SQL; FLD stores only metadata; restart resumes cursors and dedup state; no FLD local Outbox is generated.
3. **Array/file provider + FLD metadata:** stable item identity mapping, durable local change capture, remote apply and restart recovery work without database-specific assumptions.
4. **Custom metadata store:** a non-FLD implementation passes the same semantic contract tests.
5. **Multiple scopes:** two applications/datasets in one physical Metadata Store never share peer state, cursors, dedup entries or relay retention.
6. **Multiple namespaces:** namespace cursors remain independent while sharing the appropriate replica identity.
7. **Lost ACK:** the same incoming batch returns the same durable duplicate outcome without repeating source effects or relay append.
8. **Cross-peer duplicate ChangeId:** duplicate content is not applied/relayed twice, while each sender's receive cursor is advanced correctly.
9. **LWW loser:** a mutation that does not replace local visible state remains relay-eligible when protocol policy requires it.
10. **Crash between source and metadata commits:** the pending apply token is recovered and provider idempotency prevents a duplicate source effect.
11. **Crash during local change capture:** a source change is neither lost nor assigned a second ChangeId after recovery.
12. **Stale consumer:** a prior lease holder cannot read/ack after a newer lease is issued.
13. **Concurrent snapshot/checkpoint:** digest/export reads one stable view without holding a writer lock over network I/O.
14. **Compaction/retention:** no required inbox, dedup, Outbox or relay information is deleted before the protocol's safe-retention condition.
15. **Storage-only isolation:** FLD metadata writes do not generate HLC/ChangeId/local Outbox or appear as synchronized user changes.
16. **Migration/reopen:** metadata format upgrades preserve scope isolation, cursor continuity and duplicate recognition.
17. **Unpublished local coalescing:** multiple LWW writes to one field before freeze produce one pending operation; a frozen operation is never rewritten.
18. **Pending local vs remote winner:** a remote APPLY that wins LWW does not destroy the payload of a still-pending local operation.
19. **Non-LWW operations:** CRDT, BLOB_REF, MANUAL and audit/event operations do not use generic LWW coalescing.

## 14. Relationship to existing documents

- **BIN-SYNC-SPECIFICATION.txt** remains the normative wire/protocol specification. This architecture document does not change wire layouts or protocol invariants.
- **docs/FLD-DB-INTEGRATION-CONTRACT.md** remains the specialized contract for FLD-DB when it stores synchronized field data. It must conform to this architecture but may define stronger FLD-specific guarantees such as atomic field-data + metadata commit, local Outbox, FLD snapshots and FLD WAL behavior.
- **docs/CHANGE-STAGING-AND-RETENTION.md** defines the detailed lifecycle and safe coalescing/retention rules for local staged changes, frozen Outbox entries and remote relay operations.
- FLD's kernel specification owns FLD physical format, WAL, run, manifest and Storage-only implementation details. It must not redefine BIN-SYNC protocol semantics.

If a specialized contract conflicts with this document, the conflict must be resolved explicitly. A FLD-specific optimization must not weaken a general durability or isolation guarantee.

## 15. Decisions and remaining implementation questions

The following are not decided by v0.2 and must be resolved before freezing public API:

1. Exact public names and signatures for provider and metadata interfaces.
2. Whether the runtime exposes a unified transaction object, semantic atomic commands, or both.
3. Exact scope/replica identifier representation and how it is supplied at construction.
4. Which protocol state is mandatory in the first implementation versus capability-gated.
5. **Decided:** no built-in in-memory Metadata Store in the first stable package. A volatile implementation belongs in test utilities; if later exposed publicly, it must advertise that restart recovery/durable ACK guarantees are unavailable.
6. **Decided:** FLD uses two independently buildable assemblies: `Fld1.Storage.dll` and `Fld1.Database.dll`; Database references Storage only. Storage-only consumers do not load Database. No third Common assembly.
7. **Decided:** storage-native ordering/version uses Storage Core CommitSeq and byte-key ordering; it is never fabricated as HLC/author. Split generic page/run mechanics from any field-specific codec that prevents opaque metadata storage.
8. **Decided:** each local replica uses an owning `(SyncScopeId, LocalReplicaNodeId)` partition; protocol NamespaceId is a key/prefix dimension inside that partition, not a physical FLD namespace. Storage Core exposes opaque logical partition IDs and can host multiple partitions. The current field-specific namespace/run codec must be separated from the generic partition catalog.
9. Exact metadata record layout, key encoding, compaction and retention strategy in FLD.
10. How current NodePersistence/WAL state migrates to the new provider/store boundary.
11. **Decided:** the first stable contract supports both strong modes: shared atomic provider+metadata transaction, or separate-store pending-apply with a durable provider idempotency receipt. A provider that supports neither may use an explicitly weaker best-effort mode only; BIN-SYNC must not emit an ACK that claims durable exactly-once acceptance.
12. **Decided:** durable Metadata Store, crash recovery, idempotent retry and atomic metadata transitions are required for the strong profile. Volatile/best-effort profiles are opt-in and must explicitly weaken restart/delivery guarantees; they are never silently selected.
13. **Conservative rule decided:** current snapshot completion never advances stream offsets or Outbox/relay/ChangeId GC. A future snapshot-baseline protocol still needs a per-author cut vector and explicit completion/fencing semantics.
14. **Correctness fallback, representation open:** cross-peer dedup must be exact and survive payload compaction. ChangeId + canonical fingerprint is the safe fallback; evaluate content-derived ChangeId and origin-sequence/checkpoint compaction before freezing physical retention. Local WAL replay markers remain separate and reclaimable only after their WAL replay horizon closes.
15. **Decided:** compact Inbox receipt retains cursor range, resolved author, BatchDigest, RecordsRoot, PreviousBatchDigest, ACK result, appliedAt/expiresAt and storage commit identity; duplicate keys must match all identity/chain fields.
16. **Decided:** prune PeerState.Ids only in the same durable ACK transaction when the matching peer/namespace/author StreamAcked covers the entire cursorEnd; implementation remains pending.
17. **Decided:** ProtocolStateDelta is a bounded per-batch recovery envelope, materialized into normal indexed metadata records during finalization. Retain the raw delta only in a pending-apply intent across separate-store recovery; delete it after provider and metadata finalization. Duplicate batches return only the compact receipt/ACK result, never the full delta.
18. Implement and test the decided Inbox duplicate validation consistently in BIN-SYNC and FLD; conflicting reuse must fail closed without retaining the full original payload.
19. **Correctness baseline, representation open:** cross-peer at-most-once relay suppression must survive payload compaction. Exact ChangeId + fingerprint remains the safe fallback; do not finalize permanent per-operation storage until origin-sequence/range and snapshot-fencing alternatives have been proved against mesh replay, equivocation and bootstrap scenarios.

These open points must not be resolved by adding speculative flags or prematurely publishing an unstable API. They require a focused design pass against the current Node, FLD-DB engine and representative external providers.

## 16. Recommended implementation sequence

1. Review and approve the architecture and resolve the open decisions that affect the contract.
2. Approve the change staging/coalescing lifecycle and its crash/relay conformance tests.
3. Define the neutral Metadata Store semantic operations and conformance tests.
4. Define the Data Provider change-capture/apply contract and its atomicity modes.
5. Refactor BIN-SYNC persistence behind the neutral boundary without changing wire behavior.
6. Implement FLD Storage-only semantics using reusable physical storage primitives.
7. Implement the FLD Metadata Store adapter.
8. Adapt the existing FLD synchronized-database integration to the same durable protocol semantics while preserving its stronger atomic transaction path.
9. Validate SQL, array/file and custom-store scenarios.
10. Only then freeze public API and update package/version documentation.

**Design rule:** do not begin by replacing Node dictionaries with a thin collection of callbacks. First establish the complete atomic state transitions and ownership of every durable datum.

## 8. Code audit addendum — shared operations and peer delivery (2026-11-07)

The current runtime has been checked against branch feature/fld-external-storage at HEAD dd2dd969d70b6fd46371679903855a69ae9399c3. The detailed cross-repository audit is maintained in [FLD-DB: Sync History Storage Audit](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-HISTORY-STORAGE-AUDIT.md).

### 8.1. Do not create per-peer operation copies

BIN-SYNC already keeps one shared Outbox entry per logical operation. PeerState is keyed by peer/namespace and stores one combined author-progress entry (ACK, pushed and offset watermarks) per author, plus per-session received cursors. Batch construction reads the same operation for each target peer; the wire batch is one author stream at a time. This is the desired separation: one immutable payload, independent per-peer cursors. Do not add a change×peer delivery table or per-operation ACK bitmap.

CompactOutbox currently checks every interested peer for every entry. A later CPU optimization may precompute the minimum safe local ACK per author and then scan entries once. A peer with no PeerState must still block reclamation; no peers must preserve the existing keep-the-log behavior.

### 8.2. Replay cache and compact peer-state encoding

PeerState.Ids stores the deterministic batch replay mapping only while a peer may retry an unacknowledged cursor range. A validated contiguous ACK now prunes fully covered mappings while preserving LastStreamDigest as the independent digest-chain checkpoint.

The v9 in-memory peer state consolidates StreamAcked, StreamPushed and StreamOffsets into one per-author value entry with presence flags. This removes repeated author keys and separate dictionary entry sets for authors that have multiple watermarks. ReceivedStreams no longer repeats NamespaceId in each key because the containing PeerState is already scoped by (peer, namespace); its key is now (senderPeer, issuerEpoch, author). NodePersistence v9 writes these compact structures once per author/stream.

NodePersistence v3-v8 remains readable. The v3-v8 reader validates the legacy ReceivedStreams namespace against its containing PeerState and migrates the three independent watermark dictionaries into the combined v9 author-progress map. The v9 reader validates flags, nonnegative values, ACK≤pushed and overflow-safe offset arithmetic. No wire format changes: this is a local persistence/in-memory representation change.

The byte reduction depends on how many of the three watermarks exist per author and on dictionary implementation overhead. The structural reduction is deterministic (one author key and one dictionary value instead of up to three repeated author keys/entries); measured disk/RAM results should be published separately rather than claiming a fixed percentage.

### 8.4. Inbox receipt growth is bounded by admission control

Because receipt expiry/GC has no proven protocol horizon, accepted-batch receipts are never evicted by age or by unrelated ACK progress. To prevent unbounded growth from becoming an uncontrolled memory/disk failure, `ResourceLimits.MaxInboxReceipts` now provides an explicit admission cap (default 1,000,000; zero means unlimited). The cap is checked during whole-batch preflight before WAL append or state publication. When full, the new batch is rejected atomically, including a new BatchId that contains a globally deduplicated ChangeId: the batch still requires its own durable receipt and cursor transition.

This is backpressure, not reclamation. It does not make receipt GC safe, and it does not reduce already persisted receipt bytes. Hosts must monitor the cap and provision/recover capacity deliberately. A future safe-GC design must retain the independent duplicate-replay horizon and log-segment confirmation condition.

### 8.3. Tombstone GC is not yet safe to enable

The current GC path does not satisfy the protocol's tombstone-retention invariant and must not be treated as a completed tombstone-GC implementation:

- `GcNotice.Apply` merges quarantine/author-epoch state but does not persist or enforce the received `TombstoneCutoffWall`.
- `WatermarkGate` exists as a helper, but the inspected DELTA_BATCH apply/reconcile/snapshot paths do not call it. The required stale-peer `OUTDATED_WATERMARK` / full-resync gate is therefore not connected to receipt of GC_NOTICE.
- The former `GcNotice.PurgeBelowCutoff` implementation removed entries from `Record.State` solely because their field HLC wall was below the cutoff. It did not check whether the field was a tombstone, whether it was dominated by a record lifecycle tombstone, or whether a safe peer watermark had been established. It did not reclaim `Record.FieldTombs` or lifecycle tombstone metadata.
- This age-only deletion has now been disabled: `PurgeBelowCutoff` returns zero until a safe cutoff/resync gate exists. Its test now asserts that old live values, field tombstones and record lifecycle tombstones all remain intact.

Required design before enabling deletion: persist a monotonic safe tombstone cutoff per namespace; bind it to a protocol-visible peer watermark or a mandatory full-snapshot/resync transition; reject incremental synchronization from peers behind that cutoff; preserve ordinary live field values; reclaim only tombstone/ghost state whose deletion is proven safe. Field tombstones, record lifecycle tombstones, relay payload retention, Inbox receipts and global ChangeId dedup have separate lifetimes. A tombstone's age alone is never sufficient.

Until those conditions and crash/restart tests exist, the implementation fails closed: `PurgeBelowCutoff` is a no-op and `TombstoneCutoffWall` is not treated as authorization to delete state. Do not add TTL-based Inbox receipt or ChangeId-ledger cleanup as a substitute.

### 8.3. Global ChangeId ledger — implemented

The runtime now has a route-independent ledger keyed by (namespace, ChangeId), local to the receiving replica. Each value is a BLAKE3-128 fingerprint over a domain-separated canonical semantic operation: namespace, envelope RecordId, OpKind, FULL_AUTHOR_ID operation encoding (resolved author, HLC, field/key epoch, value or tombstone semantics), plus the normalized sorted set of causal dependencies. Compact aliases, issuer route and relay-local Seq are excluded.

- Same ChangeId + same known fingerprint: do not re-apply and do not enqueue another relay copy, even after the original Outbox payload has been ACK-compacted. This also prevents an old operation from resurrecting state after tombstone/state reclamation.
- Same ChangeId + different known fingerprint: reject the complete batch with BATCH_TRANSACTION_FAILED before WAL append or state publication.
- Local operations insert their ledger fact in the same gated commit as Storage/Outbox and the local WAL frame. Remote facts publish only after the accepted remote-batch WAL frame is durable. WAL replay reconstructs the same facts.
- SeenChanges now retains only local WAL replay markers. Remote peer-scoped entries are migrated into the global ledger (unknown when old payload fingerprints are unavailable) and then discarded; new remote batches do not write per-peer ChangeId markers. Batch Inbox receipts remain issuer-specific.
- Causal dependencies participate in the fingerprint and are preserved through relay, causal trailer encoding and remote WAL replay.

NodePersistence is now v6. Loading v3-v5 creates explicit unknown-fingerprint markers for legacy SeenChanges. A retained legacy Outbox payload upgrades its marker when its complete semantic value is available; a truncated/deferred legacy field value remains unknown. An unknown marker suppresses relay as an already-seen identity and is upgraded on its first matching arrival, but cannot validate historical equivocation until then.

The global ledger has an independent MaxGlobalChangeLedgerEntries cap (default 1,000,000; zero means unlimited). No TTL or destructive GC is implemented: a protocol-safe dedup retention horizon is still unproven.

### 8.4. ChangeId and OriginSeq remain open gates

Current relay nodes allocate their own local author-stream sequence for relayed entries. That Seq is not a stable original-operation identity and cannot replace ChangeId. A true OriginSeq must be assigned by the original author and preserved through relay, with incarnation, backup/clone and equivocation rules.

Content-derived BLAKE3-128 ChangeId remains a candidate for eliminating the separate fingerprint from the exact ledger, but changes ID-generation semantics and needs compatibility and canonicalization tests. No ChangeId or wire-format change is approved by this addendum.

### 8.5. Independent retention lifetimes

Keep these separate even if a compact physical encoding stores them in one transactional batch: (1) batch receipt / duplicate-ACK horizon, (2) global ChangeId dedup fact, (3) relay payload retention, (4) outgoing batch replay cache, and (5) peer/author cursor state. A relay ACK does not automatically expire a batch receipt; receipt expiry does not authorize deletion of relay payload or dedup identity. Current-state snapshot completion is not a GC proof until the per-author snapshot cut and completion/fencing contract exists.

## 17. Compact metadata target and implementation order (2026-11-07)

The cross-repository sequence is maintained in [FLD-DB Sync Storage Compaction Roadmap](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-STORAGE-COMPACTION-ROADMAP.md); code-backed findings remain in [Sync History Storage Audit](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/SYNC-HISTORY-STORAGE-AUDIT.md).

Target logical records:
- one immutable payload per published/relay-eligible ChangeId, shared by every destination peer;
- one route-independent ChangeId/fingerprint fact per replica/scope/namespace;
- one issuer-specific metadata-only receipt per accepted batch;
- one progress tuple per peer/namespace/author stream;
- replay mappings only for unacknowledged ranges, pruned atomically with validated contiguous ACK while preserving digest-chain checkpoint;
- relay selection as indexes into accepted batch operations, not a second payload array;
- independent retention rules for payload, receipt, dedup, replay cache and peer progress.

Do not add a per-change/per-peer delivery table or ACK bitmap. Do not merge receipt and global dedup: identities and retention horizons differ. Inbox/InboxAuthors/ReceivedStreams may be consolidated only after every retry/ACK path is covered by tests.

The global ChangeId ledger detects same-ID/same-content duplicates across routes and same-ID/different-content conflicts. Keep 128-bit ChangeId and canonical fingerprint for now; content-derived identity is not approved. Until a protocol-safe dedup GC horizon is defined, retain conservatively.

Order: baseline; global dedup, ACK-covered replay pruning, Inbox/InboxAuthors key consolidation and remote SeenChanges removal (implemented); compact header-bound InboxReceipt v8 (implemented; expiry/GC still open); APPLY single-encoding and relay-only materialization (implemented; independent receipt/relay GC remains open); durable pre-publication local coalescing; versioned compact encodings; safe GC/snapshot semantics; optional WAL-backed Outbox. No wire-format change is implied.


## 18. Replay-cache pruning implementation status (2026-11-07)

Implemented: `Node.ProcessAckInternal` prunes `PeerState.Ids` entries only when the validated contiguous ACK for that author covers `cursorEnd`. `LastStreamDigest` remains intact as the independent digest-chain checkpoint. Restart and duplicate-ACK coverage was added in `ChangeIdWireTests`. Implementation commit `dc9a1304de00e75bacb0d2bdd864e0f48e687361`; test commit `402b801e4b2d2781e048f3785766c0abf0425016`; GitHub Actions run 42342495367 passed.

Global ChangeId dedup is now implemented as described in §8.3 and §19. Receipt consolidation remains a separate work item.

## 19. Global ChangeId dedup implementation status (2026-11-07)

Implemented on `feature/fld-external-storage`:
- canonical route-independent BLAKE3-128 semantic fingerprint and per-replica (namespace, ChangeId) ledger;
- duplicate suppression after Outbox compaction and local-origin echo suppression;
- same-ID/different-fingerprint batch rejection before durable commit;
- local commit, remote WAL commit/replay, persistence v6, v3-v5 migration and legacy unknown-fingerprint handling;
- causal dependency inclusion in fingerprints and preservation through relay and WAL replay;
- a configurable global-ledger resource cap.

Focused tests cover second-route duplicates after origin Outbox compaction, returned local echoes, equivocation rejection without partial mutation, v5 unknown-marker migration, v6 persistence, causal trailer relay and WAL replay. GitHub Actions run [42344100307](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42344100307) passed .NET 10 build/tests, public samples, coverage threshold and fuzz/lab hardening jobs.

Not included: a safe dedup GC horizon, receipt consolidation, APPLY payload de-duplication, or a ChangeId/wire-format change. The backup branch remains untouched.

## 20. Inbox receipt consolidation and v8 header commitment (2026-11-07)

Implemented on `feature/fld-external-storage`:
- replaced the separate Inbox HashSet plus InboxAuthors Dictionary with one key-to-InboxReceipt Dictionary;
- duplicate ACK resolution uses the stored durable author and original cursorEnd, never LastStreamAuthor or replay-supplied cursorEnd;
- each new receipt stores a 16-byte header fingerprint, appliedAt (HLC wall Unix microseconds) and expiresAt (currently zero/no scheduled expiry);
- the fingerprint commits to issuer/namespace/epoch/BatchId, cursor range, previous/records/batch digests, schema id/version, merge policy and flags (excluding causal-trailer framing);
- duplicate replay compares only the committed header; it deliberately does not inspect entry payloads or causal dependencies, preserving the hardening contract that duplicate BatchId bypasses the causal gate and ignores replay-supplied ChangeIds;
- NodePersistence v8 stores the receipt; v3-v7 migrations retain explicit legacy-unknown receipts when cursor/fingerprint data did not exist;
- WAL replay and atomic state publication carry the one receipt dictionary.

CursorEnd and timestamps are varint encoded; the fingerprint is 16 bytes. This preserves the key consolidation while adding the data needed for exact duplicate ACK cursor reconstruction and header conflict detection. It is not a claim of net disk reduction versus the older v6 set+map layout: the new receipt metadata consumes some of the bytes saved by key de-duplication. The raw saving relative to v6 depends on cursor/appliedAt varint lengths; the larger benefit is one authoritative receipt object and reduced in-memory collection overhead.

GitHub Actions run [42345734949](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42345734949) passed .NET 10 build/tests, public samples, coverage and fuzz/lab hardening. This run tested the v8 header-commitment implementation before the subsequent cursorEnd varint-only serialization refinement; that final refinement is covered by the next CI run.

Legacy v3-v7 receipts have no header fingerprint or original cursorEnd and retain old duplicate behavior. New receipts use `expiresAt=0`; no receipt expiry/GC is implemented. A WAL-generation/materialization field is also not yet stored. No wire-format change.

## 21. Remove redundant remote SeenChanges provenance (2026-11-07)

After the global ledger became the remote dedup authority, the per-peer `SeenChanges(senderPeer, namespace, ChangeId)` rows became redundant. Remote ApplyBatch no longer inserts or probes those keys. A new accepted remote operation is recorded once in `GlobalChangeLedger(namespace, ChangeId)`; same-ID retries from any peer are resolved there. `SeenChanges` now holds only the distinct local WAL replay markers, preserving their separate replay lifetime.

NodePersistence remains v7: no layout change was needed. When loading v3-v5, all old SeenChanges identities are first folded into explicit unknown global-ledger markers, then only entries whose peer is the local NodeId remain in SeenChanges. v6-v7 already carry the global ledger; their remote SeenChanges keys are dropped while local WAL markers are retained. Thus old route-dedup facts are not lost.

The removed remote row repeated peer NodeId (16 B), namespace varint and ChangeId (16 B) per received operation—about 33 raw bytes per remote marker for common namespace values—plus HashSet entry/bucket overhead. The global ledger still stores the identity and fingerprint once. This is a format-derived estimate, not a benchmark. `MaxSeenChanges` now bounds local WAL replay markers; `MaxGlobalChangeLedgerEntries` bounds global remote/local dedup facts.

GitHub Actions run [42345011358](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42345011358) passed .NET 10 build/tests, public samples, coverage and fuzz/lab hardening. No wire-format or NodePersistence-version change.