# BIN-SYNC Metadata Storage and External Data Provider Architecture

> **v3 normative override (2026-11-08):** `BIN-SYNC-SPECIFICATION.txt` v3.0.0 and `docs/BIN-SYNC-ROADMAP-v3.md` are authoritative. Detailed version-by-version NodePersistence/WAL layouts and migration experiments below are historical analysis only; they MUST NOT be read as permission to accept old formats, migrate, dual-write, or preserve compatibility paths. The only accepted Node checkpoint is v17; the only accepted WAL is v3; the only accepted wire protocol major is 3. The Storage Metadata partition uses its separate canonical BMS3 marker and 45-byte base ChangeId identity value, with the optional 16-byte legacy Node digest suffix (61 bytes total) only for provider-authoritative cutover migration, as specified in the normative contract.

**Status (2026-11-10):** This document retains historical design decisions; the old `f91df0b4` green checkpoint is not evidence for the current source head. Current inspected heads at the last source audit are BIN-SYNC `1e36e09f` (`feature/fld-external-storage`) and FLD-DB `fc11a16` (`fix/crash-consistency-outbox`); both working trees matched their origins. No build or test run has been performed against these exact heads in this C-pass. NodePersistence is v17 and WAL is v3. Immutable Origin is persisted through Node Outbox/WAL, PendingApply, Metadata Store identity/payload rows and relay records. The Node-local global ChangeId ledger uses one 16-byte BLAKE3 digest binding semantic mutation and immutable Origin in one pass; the separate Storage Metadata ChangeId identity row uses the 45-byte base format, with a cutover-only optional 16-byte legacy Node digest suffix (61 bytes total), as defined by the normative spec. Provider-authoritative publication freezes stable provider ChangeIds into Metadata Store; the Metadata Store assigns the local author's durable OriginSequence at freeze (never from provider OutboxSeq or JournalSeq), incrementally updates the Node cache and ACKs only the captured provider-feed prefix after checkpoint. Metadata Store owns published operation identity, receipts, delivery progress and relay payload. The two-DLL boundary is `Fld1.Database.dll → Fld1.Storage.dll`; Storage remains domain-neutral and Database owns field/LWW/tombstone semantics. The remaining release gates are exact-head T-pass (including PendingApply/WAL/snapshot/restart crash matrix), adversarial signed-frontier/GC horizon tests, cross-store cardinality/benchmark evidence and final public API/source/byte audit. Physical receipt, ChangeId and tombstone/Ghost GC remain disabled.
**Scope:** separation of synchronization runtime, external data providers, and durable metadata storage.
**Protocol baseline:** BIN-SYNC CORE v3.0.0 DRAFT (breaking clean cut; no v2.x compatibility readers or migrations). **Wire format:** immutable Origin is mandatory on every published operation and included in RecordsRoot/BatchDigest; sender cursor and OriginSequence are independent. The v3 normative rules live in `BIN-SYNC-SPECIFICATION.txt`; this architecture note defines ownership and adapter boundaries.
**Implementation status:** Core/Storage conformance currently has a green 654/654 Core and 149/149 Storage baseline on all three platforms. Storage v3 partition identity uses the canonical BMS3 marker; legacy rewrite paths are removed. Metadata Store owns route-independent ChangeId/Origin dedup, PendingApply receipts, per-author receive cursors, sender digest chains, outbound/relay payload and per-peer delivery progress. The provider feed owns unpublished local changes only. Legacy Node-owned send/receive paths remain transitional and are a Phase 5 clean-cut removal gate. FLD provider adapter, source JournalSeq provenance mapping, safe receipt/ChangeId/tombstone GC and million-row metadata benchmarks remain open.

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

The store must expose semantic atomic operations or transactions. A generic key/value CRUD interface alone is insufficient because correctness depends on atomic transitions across several metadata categories. The operation semantics and provider-independent acceptance matrix are maintained in [Metadata Store Semantic Contract and Conformance Plan](METADATA-STORE-CONFORMANCE.md); that document deliberately does not freeze public C# type or method names.

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
- peer state: received-stream mappings, StreamPushed, StreamAcked, StreamOffsets, batch range → BatchId/previous-digest replay mappings, initial-sync status, membership epoch and applicable last-seen/retention data. The replay mapping is needed only while its range may be retried and is pruned after a validated contiguous per-author ACK covers that range end; ProcessAckInternal preserves LastStreamDigest as the independent digest-chain checkpoint;
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
- **docs/DATA-PROVIDER-CONFORMANCE.md** defines the provider-side source mapping, durable change-feed, snapshot/feed barrier, idempotent remote-apply and local/remote-origin guarantees, with a test-only conformance plan.
- FLD's kernel specification owns FLD physical format, WAL, run, manifest and Storage-only implementation details. It must not redefine BIN-SYNC protocol semantics.

If a specialized contract conflicts with this document, the conflict must be resolved explicitly. A FLD-specific optimization must not weaken a general durability or isolation guarantee.

## 15. Decisions and remaining implementation questions

The following remain open in architecture contract v0.6 and must be resolved before freezing public API:

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
16. **Implemented:** ProcessAckInternal prunes PeerState.Ids only in the same validated ACK persistence transition when the matching peer/namespace/author StreamAcked covers the entire cursorEnd; LastStreamDigest remains intact. Implementation commit dc9a1304de00e75bacb0d2bdd864e0f48e687361; restart/idempotent-ACK tests commit 402b801e4b2d2781e048f3785766c0abf0425016.
17. **Decided:** ProtocolStateDelta is a bounded per-batch recovery envelope, materialized into normal indexed metadata records during finalization. Retain the raw delta only in a pending-apply intent across separate-store recovery; delete it after provider and metadata finalization. Duplicate batches return only the compact receipt/ACK result, never the full delta.
18. Implement and test the decided Inbox duplicate validation consistently in BIN-SYNC and FLD; conflicting reuse must fail closed without retaining the full original payload.
19. **Correctness baseline, representation open:** cross-peer at-most-once relay suppression must survive payload compaction. Exact ChangeId + fingerprint remains the safe fallback; do not finalize permanent per-operation storage until origin-sequence/range and snapshot-fencing alternatives have been proved against mesh replay, equivocation and bootstrap scenarios.
20. **Added in v0.6:** the provider-side semantic contract is documented in `DATA-PROVIDER-CONFORMANCE.md`; the test-only volatile suite covers mapping, feed paging/ACK, snapshot/feed watermark, idempotent remote apply and origin separation. Public signatures and durable provider implementation remain open.

These open points must not be resolved by adding speculative flags or prematurely publishing an unstable API. They require a focused design pass against the current Node, FLD-DB engine and representative external providers.

## 16. Recommended implementation sequence

1. Review and approve the architecture and resolve the open decisions that affect the contract.
2. Approve the change staging/coalescing lifecycle and its crash/relay conformance tests.
3. Define the neutral Metadata Store semantic operations and conformance tests.
4. **Semantic contract and initial volatile tests implemented:** define/validate Data Provider change capture, snapshot/feed barrier and shared-transaction/separate-store idempotent apply modes. Durable provider and end-to-end feed-to-metadata transaction tests remain open.
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

The v9 in-memory peer state consolidates StreamAcked, StreamPushed and StreamOffsets into one per-author value entry with presence flags. This removes repeated author keys and separate dictionary entry sets for authors that have multiple watermarks. ReceivedStreams no longer repeats NamespaceId in each key because the containing PeerState is already scoped by (peer, namespace); its key is now (senderPeer, issuerEpoch, author). NodePersistence v9 introduced these compact structures, and v10 retains them while grouping repeated receipt and ledger scope keys.

NodePersistence v3-v10 remains readable. The v3-v8 reader validates the legacy ReceivedStreams namespace against its containing PeerState and migrates the three independent watermark dictionaries into the combined v9 author-progress map. The v9-v11 readers validate flags, nonnegative values, ACK≤pushed and overflow-safe offset arithmetic. v10 groups receipt/ledger scope keys; v11 removes the repeated local NodeId from WAL replay markers. These are local persistence changes; no wire format changes.

The byte reduction depends on how many of the three watermarks exist per author and on dictionary implementation overhead. The structural reduction is deterministic (one author key and one dictionary value instead of up to three repeated author keys/entries); measured disk/RAM results should be published separately rather than claiming a fixed percentage.

### 8.4. Inbox receipt growth is bounded by admission control

Because receipt expiry/GC has no proven protocol horizon, accepted-batch receipts are never evicted by age or by unrelated ACK progress. To prevent unbounded growth from becoming an uncontrolled memory/disk failure, `ResourceLimits.MaxInboxReceipts` now provides an explicit admission cap (default 1,000,000; zero means unlimited). The cap is checked during whole-batch preflight before WAL append or state publication when the host supplies this ResourceLimits instance through NodeOptions.Limits; Node remains unbounded if Limits is null. When full, the new batch is rejected atomically, including a new BatchId that contains a globally deduplicated ChangeId: the batch still requires its own durable receipt and cursor transition.

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

The global ledger has an independent MaxGlobalChangeLedgerEntries cap (default 1,000,000; zero means unlimited) when a ResourceLimits instance is attached through NodeOptions.Limits; otherwise the core remains unbounded. No TTL or destructive GC is implemented: a protocol-safe dedup retention horizon is still unproven.

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

The Node global ChangeId ledger detects same-ID/same-content duplicates across routes and same-ID/different-content conflicts in the legacy Node-owned receive path. Storage-backed Metadata-Store-authoritative receive uses the Metadata Store's global ledger and per-operation apply mask instead. Keep 128-bit ChangeId and canonical fingerprint as specified by E-24; content-derived identity is not approved. Until a protocol-safe dedup GC horizon is defined, retain conservatively.

Order: baseline; global dedup, ACK-covered replay pruning, Inbox/InboxAuthors key consolidation and remote SeenChanges removal (implemented); compact header-bound InboxReceipt v8 (implemented; expiry/GC still open); APPLY single-encoding, relay-only materialization and FLD per-author relay ACK/compaction (implemented; BIN-SYNC now exposes the safe per-author ACK query and retained ChangeId→OutboxSeq resolver; durable source JournalSeq provenance and atomic adapter scan/ACK integration remain open); durable pre-publication local coalescing; versioned compact encodings; safe GC/snapshot semantics; optional WAL-backed Outbox. No wire-format change is implied.


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

In the legacy Node-owned receive path, after the Node global ledger became the remote dedup authority, the per-peer `SeenChanges(senderPeer, namespace, ChangeId)` rows became redundant. Remote ApplyBatch no longer inserts or probes those keys. A new accepted remote operation is recorded once in Node `GlobalChangeLedger(namespace, ChangeId)`; same-ID retries from any peer are resolved there. In Metadata-Store-authoritative receive, the metadata ledger supplies this decision and Node does not touch its global ledger. `SeenChanges` now holds only the distinct local WAL replay markers, preserving their separate replay lifetime.

NodePersistence remains v7: no layout change was needed. When loading v3-v5, all old SeenChanges identities are first folded into explicit unknown global-ledger markers, then only entries whose peer is the local NodeId remain in SeenChanges. v6-v7 already carry the global ledger; their remote SeenChanges keys are dropped while local WAL markers are retained. Thus old route-dedup facts are not lost.

The removed remote row repeated peer NodeId (16 B), namespace varint and ChangeId (16 B) per received operation—about 33 raw bytes per remote marker for common namespace values—plus HashSet entry/bucket overhead. In the legacy Node-owned path, the Node global ledger still stores the identity and fingerprint once; Metadata-Store-authoritative receive uses the separate Metadata Store ledger and does not add a Node copy. This is a format-derived estimate, not a benchmark. `MaxSeenChanges` now bounds local WAL replay markers; `MaxGlobalChangeLedgerEntries` bounds the legacy Node global ledger.

GitHub Actions run [42345011358](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42345011358) passed .NET 10 build/tests, public samples, coverage and fuzz/lab hardening. No wire-format or NodePersistence-version change.

## 22. NodePersistence v10 grouped scope encoding (2026-11-07)

NodePersistence v10 groups accepted Inbox receipts by their shared `(issuer, namespace, epoch)` key and writes that key once per group, followed by the group's BatchId/receipt values. The global ChangeId ledger is grouped by namespace and writes the namespace once per group. This removes repeated scope identifiers without changing either in-memory dictionary key, receipt semantics, fingerprint semantics or the network protocol.

For a group containing N receipts, the raw receipt-key saving is approximately `(N-1) × (16 + varint(namespace) + varint(epoch))` bytes, offset by one additional per-group count varint. For N ledger entries in one namespace, the saving is approximately `(N-1) × varint(namespace)`, also offset by the group-count encoding. These are format-derived estimates, not benchmark results; singleton groups can be slightly larger. The benefit scales with batches per issuer/session and changes per namespace.

The v10 reader validates group keys, non-empty group counts, receipt flags, cursor range and expiry ordering. It reads v3-v9 snapshots using their original flat layouts; v9 remains separately covered by a compatibility fixture. The v10 round-trip test covers multiple receipts in shared and distinct groups, known/unknown ChangeId markers, and receipt expiry fields. No DELTA_BATCH wire format change.

## 23. NodePersistence v11 compact local WAL replay markers (2026-11-07)

After remote peer-scoped `SeenChanges` rows were removed, the set retained only local WAL replay markers. Its key still repeated the owning NodeId for every row, even though a Node's identity is immutable and already stored once in the snapshot header. v11 changes the in-memory and persisted key from `(NodeId, namespace, ChangeId)` to `(namespace, ChangeId)`. The namespace remains part of the key; the global ChangeId ledger remains separate because its retention lifetime and semantic fingerprint differ from the local WAL replay horizon.

NodePersistence v11 writes only namespace + ChangeId for local markers. It reads v3-v10 layouts and keeps only legacy rows whose peer equals the loaded NodeId; v3-v5 migration still folds every legacy route marker into an explicit unknown global-ledger fact before filtering the local replay set. WAL replay preflight, rollback, checkpoint clone/publish and restart tests use the compact key. This removes 16 raw bytes per local marker, plus the corresponding tuple/hash-entry overhead. The estimate is format-derived, not a benchmark. CI run [42349333368](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42349333368) passed .NET 10 build, 601 tests, public samples, coverage and fuzz/lab hardening at commit `2b6d3f0eed`, including follow-up migration/test hardening.

## 24. FLD per-author relay ACK and payload compaction (2026-11-07)

FLD now exposes `FldDatabase.AcknowledgeRelayBatches(author, safeThroughJournalSeq)`. The method validates the JournalSeq watermark, persists it in an atomic CRC-protected sidecar, and causes `ReadRelayBatchPage` to suppress acknowledged payload immediately. Physical removal is deferred until checkpoint seals and truncates the corresponding APPLY WAL commits; then ApplyJournal is atomically rewritten, removing only the acknowledged author's relay fields while preserving the batch receipt and other authors' payloads. WAL recovery checks existing sealed receipts before comparing full WAL payloads, so compacted relay fields are not resurrected or treated as a conflict.

The FLD mechanism is implemented and CI-tested. BIN-SYNC must still durably map each source (author, JournalSeq, ChangeId) to its republished local OutboxSeq and compute the minimum contiguous ACK across every interested downstream peer before calling the FLD API. Read/scan progress is not a safe ACK. Receipt GC remains independent and open.

## 25. NodePersistence count/length validation (2026-11-07)

The NodePersistence v11 reader rejects collection counts that exceed either `Int32` or the minimum number of encoded bytes available in the remaining snapshot body. The check precedes capacity allocation for large collections and applies to nested records, Outbox entries, receipts, peer progress, replay mappings, ledgers, fields/tombstones and causal dependencies. Length-prefixed Outbox payloads are checked against the remaining body before copying. v10 grouped receipt and ledger sections use the same validation for group and member counts. All persisted integer varints are checked before narrowing to `Int32`, and namespace IDs are checked against `UInt16`; malformed values cannot silently wrap. Recomputed-digest malformed snapshot tests cover impossible counts, an overflowing node epoch and an out-of-range namespace ID independently of the outer BLAKE3 integrity check. GitHub Actions run [42352040484](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352040484) passed at commit `add61134362dd0ddf2151f8d75cbadc4b7d119aa`. No state-version or network-wire change.

## 26. NodePersistence v12 adaptive receipt encoding (2026-11-07)

Accepted batch receipts remain logically keyed by `(issuer, namespace, epoch, BatchId)`, but v12 avoids repeating the 16-byte `DurableAuthor` where several receipts in one session group belong to the same author stream. Each `(issuer, namespace, epoch)` group selects a flat or author-grouped encoding based on exact byte cost. Flat mode is retained for singleton-heavy groups; grouped mode is used only when the saved author IDs exceed the added layout and count-varint bytes. This avoids making sparse groups larger just to enforce a single physical layout.

The v12 reader validates the layout tag, rejects duplicate session/author groups and duplicate receipt keys, and reads the previous v3-v11 formats. Round-trip coverage exercises both v12 layouts; an explicit v11 fixture verifies backward compatibility. The writer changes only local persistence; no receipt identity, duplicate ACK behavior, in-memory key, or network wire format changes. CI run [42352397093](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352397093) passed at commit `8dc03fc79db4dd0bfa43fc449f02441faa309d9f`.

## 27. Outbox compaction watermark aggregation (2026-11-07)

Outbox compaction now aggregates the minimum safe local ACK once per author stream. It collects interested peers using the existing membership/session/PeerState union, computes each author's minimum `AckedFor(author) + StreamOffset(author)`, and then scans the Outbox once to retain entries above that watermark. The cost changes from O(C×P) to O(C + A×P), with C entries, P interested peers and A distinct authors in the namespace. Semantics remain unchanged: no peers means no reclamation; a known peer without PeerState blocks reclamation; and peer-relative ACKs are translated through the persisted offset before comparison. CI run [42352564256](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352564256) passed at commit `abc14552f115275c31e45f01c5073fb8de956d11`. No wire or persistence change.

## 28. Public safe Outbox ACK watermark

`Node.GetSafeOutboxAckedThrough(namespaceId, authorNodeId)` exposes the minimum local OutboxSeq acknowledged by all interested peers for an author stream. It translates peer-relative ACKs using each `PeerState.StreamOffset`, and fails closed with zero when no peers are known or any known peer lacks a namespace PeerState. The method remains useful after Outbox payload compaction because peer progress is retained independently. `CompactOutbox` and the public query share the same computation, avoiding drift between the reclamation policy and the value consumed by an external adapter. This is the BIN-SYNC safe-ACK half of the FLD relay retention contract; source JournalSeq → ChangeId → OutboxSeq provenance is still required before the adapter can advance FLD's per-author relay watermark. CI run [42352727766](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42352727766) passed at commit `ce4bc54d658f36bca231ecb8b5205639960d015f`.

## 29. Resolve retained Outbox sequence by ChangeId

`OutboxChangeIdentity` and `Node.TryResolveOutboxSequences(namespaceId, identities, sequences)` provide a batch resolver for the FLD relay adapter. The adapter supplies each relay operation's original author and ChangeId from `ReadRelayBatchPage`; BIN-SYNC resolves those identities against the retained Outbox and returns local per-author OutboxSeq values in input order. The method rejects duplicate ChangeIds within a namespace and author conflicts, performs no payload copies, and returns false with a cleared output if any entry has already been reclaimed or is missing. A false result is fail-closed: do not advance `ScannedThroughJournalSeq` and do not infer sequence values from current field winners.

The caller must durably store the resulting `JournalSeq → ChangeId → OutboxSeq` provenance and scan cursor before allowing downstream send/ACK. Combined with `GetSafeOutboxAckedThrough`, this gives the adapter the identity resolution and safe-ACK primitives; the durable mapping store/transaction is still an integration task. CI run [42353097852](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42353097852) passed at commit `1086308da7553ed180daf014d8fe7762505c25ce`.

## 30. Adaptive in-memory Inbox receipt index

The in-memory receipt index now uses an adaptive store rather than a flat dictionary keyed by `(issuer, namespace, epoch, BatchId)`. A session with one receipt is kept in the singleton map; the second receipt promotes it to a BatchId-keyed inner map; removal back to one receipt demotes it. This avoids one nested dictionary allocation for every singleton session while allowing repeated 24-byte session scope material to be shared across larger groups. Raw key bytes per stream change from `40N` to `24 + 16N` for N≥2, saving `24(N-1)` bytes before dictionary/object overhead. Total managed-heap savings are not yet benchmarked and must not be inferred from this raw-key estimate.

WAL replay clones the staged receipt store before mutating live state, then swaps the prebuilt maps during publication. Persistence enumeration remains logically flat and NodePersistence v12 chooses its own adaptive disk encoding. Tests verify promotion/demotion, key lookup/removal and persistence round-trip. The existing receipt admission cap is enforced only when the host supplies `NodeOptions.Limits`; receipt expiry/GC remains blocked on a protocol-safe duplicate-retry horizon. CI run [42353496249](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42353496249) passed at commit `c612ce1552a0c7ac444f14661fb7e57fa77c22a2`.

## 31. Compact global ChangeId ledger value

`ChangeLedgerValue` now stores only the 16-byte `Digest16`; the separate `IsKnown` Boolean was removed. All-zero fingerprint is reserved for an unknown legacy marker, and known fingerprints are required to be non-zero. This reduces the managed value layout from 24 bytes to 16 bytes, saving 8 bytes per ledger entry before dictionary overhead (approximately 8 MiB at one million entries). The fingerprint computation fails closed if it ever produces zero, and persistence rejects a known-zero entry.

The v12 snapshot encoding remains unchanged: the known/unknown flag is still written, and fingerprint bytes are emitted only for known entries. Unknown legacy markers still round-trip as zero in memory. No state-version, network-wire or ChangeId algorithm change; only the local value representation and its validation invariant changed. CI run [42353818669](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42353818669) passed at commit `6f1ad14c6c90783561f54fe20303385f03de3324`.

## 32. Normative ChangeId identity and equivocation (E-24, 2026-11-07)

The protocol specification is now BIN-SYNC CORE v2.4.18 FINAL. E-24 formalizes the implemented replica-local `(namespaceId, ChangeId)` ledger, alias-independent semantic fingerprint after author resolution, duplicate/no-relay behavior across routes, whole-batch rejection on fingerprint conflict, unknown-but-seen legacy migration and conservative no-GC policy until a safe dedup horizon exists. Local writes register the identity atomically with field state, Outbox and WAL. The current BLAKE3-128 fingerprint is local metadata and is not added to DELTA_BATCH; wire bytes are unchanged.

The focused tests verify that alias changes do not change a fingerprint while author, namespace, RecordId, KeyEpoch, value bytes, causal dependency set, FieldTomb and RecordTombstone semantics do. The existing mesh tests cover alternate routes, local echo after Outbox compaction, equivocation atomicity, persistence migration and WAL replay. GitHub Actions run [42354862144](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42354862144) passed 609 tests, public samples, coverage and fuzz/lab hardening at commit `b3d65223629080b7488ff431343061ae6fe21e28`.

## 33. Tombstone GC remains disabled by E-25

A tombstone is part of replicated database semantics, not disposable Storage-layer metadata. E-25 clarifies that `TOMBSTONE_MIN_TTL` is only an age threshold and `GC_NOTICE.TombstoneCutoffWall` is advisory; neither authorizes deletion. A future safe GC needs a durable namespace frontier covering all relevant author streams, confirmed baseline by every current peer, fencing and full resync for stale peers, and one atomic snapshot/feed barrier. Until that protocol is fully implemented and enforced before every incremental and snapshot apply path, field tombstones, record lifecycle tombstones and ghost state must remain durable. The current BIN-SYNC helper is intentionally a no-op. E-25 and the fail-closed helper passed GitHub Actions run [42355154761](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42355154761), including 609 .NET 10 tests and fuzz/lab hardening.


## 10. Tombstone retention research and E-25

The cross-project research note is maintained in FLD-DB: [Tombstone Retention and Safe Garbage Collection](https://github.com/smersh1307n2/FLD-DB/blob/fix/crash-consistency-outbox/docs/TOMBSTONE-RETENTION-RESEARCH.md).

The note compares Microsoft Sync Framework ForgottenKnowledge, WinFS P2P knowledge summaries, CRDT causal stability/commitment, Cassandra tombstone grace periods, CouchDB purge checkpoints and Couchbase Sync Gateway retention. Its design recommendation preserves E-25:

- TTL/HLC age is never sufficient to delete a tombstone.
- Tombstone payload, relay payload, global ChangeId ledger, Inbox receipts and forgotten-frontier/GC-epoch metadata have independent retention conditions.
- Safe tombstone reclamation requires a durable namespace membership baseline, explicit stale-peer fencing, mandatory full resync/bootstrap, contiguous processed ACK plus exact provenance, and an atomic snapshot/feed barrier.
- A compact forgotten frontier must survive individual tombstone removal; local OutboxSeq/JournalSeq/CommitSeq values must not be treated as globally comparable origin sequence numbers.
- Until all receive, reconcile and snapshot paths enforce these conditions, destructive tombstone GC remains disabled.

This is a research-backed design proposal, not an implementation or wire-format change. No OriginSeq extension or ChangeId-generation change is approved by this note.


## 34. NodePersistence v13: persist one alias direction

The in-memory alias maps remain bidirectional for constant-time lookup:
- `AliasToNode`: `alias → (NodeId, epoch)`;
- `NodeToAlias`: `(NodeId, epoch) → alias`.

They are reverse indexes over the same session bindings, not independent protocol facts. NodePersistence v13 now writes only `AliasToNode`; the reader rebuilds `NodeToAlias` and rejects zero peers, invalid aliases/epochs, or duplicate reverse bindings. The v3-v12 readers retain their original two-map layout, so existing snapshots remain readable. The in-memory representation and alias negotiation/session behavior are unchanged.

The raw saving per binding is one removed copy of `NodeId(16) + varint(epoch) + varint(alias)`, plus the repeated peer-group header and count for the removed reverse map. The exact total depends on aliases per peer. This is a persistence-only change; no wire bytes or protocol identity changed.

## 35. NodePersistence v14: shared Outbox metadata

The old Outbox snapshot encoding repeated `NamespaceId`, `AuthorNodeId` and `OpKind` for every entry, and stored `Seq` as a fixed 64-bit integer. v14 keeps each namespace's Outbox list in its original order, but writes:
- the namespace once in the outer group;
- a per-namespace author table, with each entry referring to an author by varint index (so interleaved authors do not reorder the Outbox);
- `Seq` as a non-negative varint;
- `OpKind` derived from the first canonical payload byte, with the writer verifying that it matches the in-memory field;
- `RecordId` only for field operations. A record-tombstone payload already contains its RecordId, so v14 reconstructs it from the payload and verifies equality before writing.

ChangeId, payload bytes, CreatedAt and causal dependencies retain their values. CreatedAt remains fixed-width in v14; its later compression is a separate candidate. The v3-v13 readers retain the legacy per-entry layout. No Outbox ordering, per-author sequence, relay identity, operation payload or network wire encoding changes.

For N entries and K distinct authors in one namespace, the author table costs approximately `16K + varint(K)` bytes. Per entry, v14 removes the repeated namespace varint and OpKind byte, replaces the 16-byte author with a varint index, and replaces the fixed 8-byte sequence with a varint. Record-tombstone entries also omit the duplicate 16-byte RecordId. The approximate raw saving is therefore:

`Σ[namespaceVarint + (8 − sequenceVarint) + 1 + (16 − authorIndexVarint) + (16 if RecordTombstone)] − (16K + varint(K))`.

For the focused three-entry test (two authors, small sequence values, one record tombstone), the format-derived saving is 55 bytes before outer-file effects. This is a byte-layout calculation, not a benchmark. The new test covers mixed/interleaved authors, sequence values, payload/OpKind reconstruction and RecordId reconstruction. CI run [42363786112](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42363786112) passed 638 tests, public samples, coverage and fuzz/lab hardening at commit `e9657d29124c20582c5de24bbd909b75acccf75a`.

## 36. Remaining metadata compaction candidates

The following are audit findings, not approved changes:

1. **Outbox run grouping / shared BaseSeq.** v14 shares AuthorNodeId through a per-namespace author table; v15 also varint-encodes each sequence. A further candidate is grouping consecutive Outbox entries with the same author and contiguous sequence into an ordered run: one `AuthorNodeId` and `BaseSeq` per run, with entry offsets implicit from run order. This is the persistence-level analogue of the earlier `CommitGroup` idea. It must preserve the original Outbox order, split on author changes or sequence gaps, and use a flat/single-entry form when grouping costs more. It is an encoding group only; it must not imply that separate local API calls were one atomic transaction. Measure real retained Outbox distributions (including ACK-created gaps and relayed authors) before adopting it.

2. **PeerState compatibility mirrors.** v15 keeps `LastReceivedSeq` as a varint for exact round-trip/diagnostic compatibility, although inspected production paths do not read it. `LastAckedSeq` is no longer persisted; it is reconstructed from `MinStreamAcked`/per-author progress on load. `LastPushedSeq` remains because legacy aggregate ACK validation reads it, and the transport path can advance it before per-author stream progress is published. Removing `LastReceivedSeq` from memory/API is a separate compatibility decision, not part of the v15 byte-format change.

3. **Global ChangeId ledger.** The logical key and 16-byte fingerprint remain necessary for E-24 equivocation detection. A content-derived ChangeId could potentially make the fingerprint redundant, but this changes identity semantics and remains unapproved. Do not replace exact deduplication with a Bloom filter or reclaim entries using TTL/ordinary ACK.

4. **Do not merge distinct protocol facts.** `LastBatchDigest` and `LastStreamDigest` have different chain scopes. `ReceivedStreams` includes the original stream issuer, which may differ from the direct relay peer owning the PeerState. `PeerState.Ids` stores replay mappings for outstanding ranges; its ACK-pruning behavior is already implemented. These are not duplicate fields merely because they contain related identifiers or digests.

The next measurement target is a representative Node snapshot with many Outbox entries, several authors and ACK-created sequence gaps. Record exact serialized byte counts by section and run-length distribution before deciding whether BaseSeq grouping justifies another format version. Receipt/ledger GC and E-24/E-25 semantics remain separate correctness problems, not size optimizations.


## 37. NodePersistence v15: compact peer progress and receipt expiry

v15 applies varint encoding to non-negative synchronization counters that were still stored as fixed U64 values:
- `OutboxSeqRef` counters;
- `PeerState.LastPushedSeq` and `LastReceivedSeq`;
- `PeerState.LastSeen`;
- `ReceivedStreams` cursors;
- per-author ACK, pushed and stream-offset watermarks;
- outstanding replay-range start/end values;
- Outbox `CreatedAt`.

The in-memory values and protocol semantics are unchanged. Values are range-checked on read and negative values are rejected on write. The count-before-allocation bounds were lowered only to the exact minimum sizes of the new v15 encodings; the v3-v14 readers retain their old fixed-width layouts.

`LastAckedSeq` was a persisted compatibility mirror of `MinStreamAcked`. v15 no longer writes it and reconstructs it from the per-author ACK progress on load. `LastReceivedSeq` remains persisted as a varint to preserve exact state round-trips and diagnostics, despite no read in the inspected production paths. `LastPushedSeq` remains authoritative for the legacy aggregate ACK guard.

For receipts, v15 uses the previously unused bit 2 of the existing receipt-flags byte to indicate a nonzero `ExpiresAt`. When expiry is zero, the expiry varint is omitted; nonzero expiry values are still written and restored. `AppliedAt`, cursorEnd and fingerprint behavior are unchanged. This is not a receipt-GC policy and does not introduce TTL-based deletion.

The v15 tests cover mixed Outbox authors, sequence and timestamp round-trip, RecordTombstone RecordId reconstruction, peer progress round-trip, adaptive receipt layouts, and both zero/nonzero expiry. GitHub Actions run [42364523691](https://github.com/smersh1307n2/BIN-SYNC/actions/runs/42364523691) passed Release build, 638 tests, public samples, coverage threshold and fuzz/lab hardening at commit `ad78fb8f68906944a3c8aac4588dda6b8d29a3a7`. The v3-v14 snapshot readers remain supported. No network wire-format change.


## Implementation status: FLD Storage integration (2026-11-07)

The first cross-repository integration slice is now executable.

- `Fld1.Storage` is built as an independent assembly with no reference to FLD Database, FLD Engine or BIN-SYNC.
- Its byte-key/value API provides atomic write batches, read-your-writes, optimistic conflict detection, stable snapshots, ordered prefix scans, durable WAL recovery and independently rooted snapshots.
- `BinSync.Storage.Integration` compiles against a pinned source snapshot of the authoritative FLD-DB `Fld1.Storage` project. The source snapshot is vendored because the FLD-DB repository is private and cannot be fetched by the public BIN-SYNC GitHub Actions runner. Its provenance is recorded in `external/Fld1.Storage/README.md`.
- The durable Storage-backed Data Provider adapter passes the provider conformance suite (8/8), including restart, snapshot/feed barrier, bounded cursor paging, ACK cleanup, remote idempotency and equivocation rejection.
- `samples/StorageArraySync` demonstrates two BIN-SYNC nodes exchanging a structured array backed by independent Storage partitions, including node and Storage reopen followed by a second update.

This is deliberately a **Data Provider integration milestone**, not completion of the Metadata Store implementation or a claim that the public Data Provider API has shipped. The current adapter implements the test-only conformance contract. The remaining integration gate is a durable Storage-backed Metadata Store adapter covering receipt/ChangeId ledgers, relay and outbound streams, lease fencing, ACK/compaction, snapshots and Pending Apply recovery. Its implementation must preserve the one-payload/many-peers model and persist only changed logical metadata entries; a whole-partition rewrite per operation is not an acceptable final design.


### Durable Metadata Store conformance slice

A second adapter now maps the test-only Metadata Store contract onto `Fld1.Storage`. It stores receipts, receive cursors, global ChangeId fingerprints, relay entries, outbound entries, ACK/high-watermarks, lease generations, pending-apply intents and completed-apply receipts as separate logical Storage keys.

The adapter computes a logical-entry diff and commits only changed keys in one Storage batch; it does not serialize and rewrite the entire partition for every operation. Payload ownership follows the contract: relay/outbound payload entries are separate from compact receipt and ChangeId facts; ACK/compaction removes eligible payload entries without erasing deduplication facts.

The cross-repository integration suite now runs **30 tests successfully**: the full Data Provider conformance suite, the full Metadata Store conformance suite, and restart/compaction assertions. The restart test reopens the store with receive receipts, relay/outbound state, a live lease and Pending Apply intent; it finalizes the provider receipt exactly once, compacts acknowledged payloads, reopens again, and verifies that high-watermarks and ChangeId/receipt dedup facts remain.

This remains an integration/conformance adapter rather than a shipped production Metadata Store API. The next gate is to promote the contracts into production APIs, define the Node integration boundary, and add crash/fault-injection tests around every storage/metadata transaction boundary.


## Public runtime API promotion (2026-11-07)

The conformance contracts have now been promoted into `BinSync.Core` as public `IDataProvider` and `IMetadataStore` interfaces with their shared value models. The implementations live in the optional `BinSync.Storage.dll` assembly, which depends on BIN-SYNC Core and the neutral `Fld1.Storage.dll`, not the reverse.

Public runtime components:
- `StorageDataProvider` — durable source mapping, field state, local feed, snapshot barrier, ACK and idempotent remote apply;
- `StorageMetadataStore` — per-key durable metadata records and diff-based atomic updates;
- `StorageSyncCoordinator` — explicit host-side bridge from provider feed to Node writes and from immutable received field operations to durable provider and metadata commits.

The coordinator's local publication order is provider feed → immutable Metadata Store freeze → incremental Node cache application → durable Node checkpoint → provider ACK. A failed checkpoint leaves the feed unacknowledged; replaying the same HLC/author is idempotent. Remote apply consumes immutable batch operations and derives the provider receipt token from (issuer/namespace/BatchId), so replay after later updates remains bound to the original payload. When an IMetadataStore is attached, a PendingApply intent is durable before Node mutation; the Metadata Store returns per-operation dedup decisions, Node does not mirror those identities or append a duplicate RemoteBatch WAL record, and metadata receipt/relay publication is finalized only after the provider receipt. Pending intents and their decision masks can be reconstructed after restart.

The public array sample now uses these runtime APIs. Each replica attaches StorageDataProvider and StorageMetadataStore to the same caller-owned StorageDatabase. The receive callback checkpoints Node state, begins/finalizes metadata PendingApply around provider commit, and only then allows ACK. The integration workflow exercises the two-replica array sample across close/reopen and subsequent updates.

**Remaining runtime integration gate:** `StorageMetadataStore` is implemented and publicly exposed, but the existing `Node`/Transport path still uses its legacy in-memory/WAL metadata state. Wiring `IMetadataStore` into batch freeze, receive/relay, ACK/lease and Pending Apply must be done as a separate compatibility-preserving migration. The current coordinator is explicit; automatic NodeBuilder/Transport injection is not yet claimed.


## 7.24 Storage-backed provider commit ordering and global ChangeId ledger (2026-11-07)

The runtime Storage milestone now includes the following verified path:

- `StorageDataProvider` and `StorageMetadataStore` are public implementations in `BinSync.Storage.dll`; both can attach to one caller-owned `StorageDatabase` instance and neither disposes it when attached.
- Provider read-modify-write operations use Storage write batches for their reads and writes. Read-watermark and ACK operations bind the snapshot generation to the optimistic batch base, so a concurrent commit cannot silently publish stale sequence state.
- Local feed append preserves every distinct event, updates current state only when the incoming HLC/author wins, treats an identical same-version replay as idempotent, and rejects same-version equivocation.
- Remote provider apply atomically stores its idempotency receipt with field-state changes, preserves the LWW winner, and rejects same-version equivocation even when a different apply token is supplied.
- `MetadataFrozenOperation` now carries the same canonical 16-byte fingerprint used by incoming `MetadataOperation`. Local outbound freeze records that fingerprint in the global ChangeId ledger. An incoming copy of the same operation from another route advances that sender's receive cursor but does not create another relay entry; conflicting fingerprints fail the whole batch.
- `Transport.DeliverBatch/SyncRound` and `SyncSession.ReceiveBatchAsync` expose a pre-ACK callback. The receiver first checkpoints its Node state/receipt, then commits external provider materialization, and only then returns ACK. Callback failure leaves the sender's batch pending for duplicate replay.
- The Storage array example now persists the receiver Node checkpoint, commits the provider, then ACKs. Its integration test simulates a restart after provider commit but before ACK; retry is recognized as a duplicate and provider apply returns its original receipt.

CI at commit `37d4e3f3e028`: BIN-SYNC core 642/642, fuzz/lab 31/31, Storage integration 38/38; the array sample reports PASS. These counts are the last fully completed test run before the subsequent documentation-only commits.

### Remaining integration and performance gates

1. **Node/Transport Metadata Store authority:** the public `IMetadataStore` implementation is not yet injected into Node's internal Outbox, receive/relay, per-peer ACK/lease and Pending Apply paths. The current NodePersistence/WAL metadata remains authoritative there. Migrate in compatibility-preserving slices with crash injection at every commit boundary; do not dual-write without a single recovery authority.
2. **Metadata mutation complexity:** Storage persists only changed logical keys, but the current adapter still clones collection structures and derives the key diff from partition state on each mutation. Replace this with explicit per-operation dirty-key deltas/copy-on-write before claiming bounded per-operation CPU/allocation cost for large partitions.
3. **Multi-writer policy:** adapters sharing one StorageDatabase refresh stale partition generations and fail closed on conflicting optimistic writes. Automatic retry/merge and multi-process fencing are not yet implemented.
4. **Retention:** outbound/relay payload compaction must continue preserving global ChangeId fingerprints, receipts and source JournalSeq→ChangeId→OutboxSeq provenance until a proven GC horizon exists.


## 7.25 Immutable batch-to-Storage integration and author-scoped receive progress (2026-11-07)

The latest integration slice closes two correctness gaps found by running the real array sample through the Metadata Store:

1. **Provider apply is based on the immutable received batch, not the receiver's current LWW view.** \`StorageSyncCoordinator.ApplyRemoteBatch(Batch)\` decodes each FieldWrite/FieldTomb and passes the exact record, field, HLC, key epoch, durable author and value to the provider. The provider receipt token is derived from \`(namespace, issuer, BatchId)\`. A retry after a later write therefore resolves the original receipt rather than fingerprinting a newer current winner. Multiple immutable operations for one field in one batch remain in the batch fingerprint and are folded by the provider's LWW comparator.
2. **Receive cursor and digest-chain scopes are different.** BIN-SYNC sequence numbers are per \`(peer, epoch, author stream)\`, while the batch digest chain is per \`(peer, epoch, namespace)\`. \`MetadataIncomingBatch.AuthorStreamNodeId\` now carries the author stream; \`StorageMetadataStore\` persists author-scoped receive cursors separately from sender-level digest checkpoints. Receipt identity is also bound to the author stream. Legacy callers that omit this field retain the legacy single-stream cursor behavior; old combined receive records are decoded and rewritten to the split key representation on the next metadata mutation.
3. **Metadata PendingApply is now used by the coordinator when a Metadata Store is attached.** It persists the immutable operation payload envelope (16-byte RecordId + canonical op payload), applies the provider batch with the same token, and finalizes the receive receipt/relay metadata only after provider durability. \`RecoverPendingApplies()\` replays pending envelopes after restart and finalizes them idempotently.
4. **Local Outbox is frozen into durable metadata before source-feed ACK.** After the caller's Node checkpoint succeeds, `StorageSyncCoordinator` reads owned local Outbox pages, verifies contiguous original sequence numbers, computes canonical ChangeId fingerprints, and calls `FreezeOutbound`. A gap fails closed; metadata sequences must exactly match Node Outbox sequences. This ensures a source feed is not acknowledged until both Node's Outbox and the durable metadata publication copy are committed.
5. **The array example uses one StorageDatabase per replica** shared by its StorageDataProvider and StorageMetadataStore adapters. It asserts local outbound high-watermarks, uses metadata-backed incoming PendingApply, and reopens Storage and Node state before the next update. The metadata store is now a durable companion for outbound frozen payloads and incoming receipt/relay/PendingApply, not yet the authoritative Node Outbox/session state.

Current unsupported case: \`OpRecordTombstone\` is explicitly rejected by the provider coordinator. A record-level tombstone cannot be faithfully represented as a set of field tombstones because a later-arriving older field could otherwise resurrect the record. The provider contract needs a durable record tombstone key and a defined comparison/visibility rule before this operation can be accepted.

### Remaining integration gates

- **Node/Transport authority migration:** route Node outbound freeze/read/lease/ACK and inbound receipt/relay through \`IMetadataStore\` as the single recovery authority. Do not keep two independent sources of truth or dual-write without a crash-safe reconciliation contract.
- **Full operation coverage:** record lifecycle and field tombstones are supported; define durable semantics for non-LWW/event operations and explicit undelete workflows.
- **Metadata mutation complexity:** the adapter persists changed logical keys, but still clones collection structures and derives a key diff from partition state on each mutation. Replace this with explicit dirty-key deltas/copy-on-write before claiming bounded per-operation CPU/allocation cost.
- **Concurrency and retention:** multi-process fencing/automatic retry are not implemented; preserve receipts, global ChangeId fingerprints and source JournalSeq→ChangeId→OutboxSeq provenance until a proven GC horizon exists.


## 7.26 Record lifecycle persistence and array deletion (2026-11-07)

\`StorageDataProvider\` now persists \`ProviderMutation.IsRecordTombstone\` under a separate record-level Storage key. It does not fan a record delete out into field tombstones. \`TryReadCurrent\` compares the field's HLC/author with the record lifecycle HLC/author and applies the same ghost visibility rule as \`Model.IsGhost\`: fields at or before the dead lifecycle are hidden; newer ghost fields remain visible. Snapshot pages include the record tombstone and all retained field states so a consumer can reconstruct the lifecycle and ghost state.

The local feed path maps record tombstones to \`Node.DeleteRecord\`; the received-batch and PendingApply recovery paths decode \`OpRecordTombstone\` and preserve its HLC and durable author. The array sample now deletes one array item on replica B, synchronizes the lifecycle tombstone through BIN-SYNC, and verifies that the item is hidden on both Storage replicas.

**Verified CI at commit \`58cc41d8e8d1\`:** Storage integration 44/44 and array sample PASS (including record deletion); BIN-SYNC core 642/642; fuzz/lab workflow success.


## 7.27 Complete Node checkpoint stored in the same Storage database (2026-11-07)

\`NodePersistence.SerializeToBytes(Node)\` now exposes the exact integrity-protected v15 image previously written only through the atomic file API. \`StorageNodePersistence\` stores that image under a stable NodeId key using one Storage write batch and restores it through \`NodePersistence.LoadFromBytes\`, retaining all existing version/digest validation. It does not own the database.

The array sample now uses exactly one StorageDatabase per replica for:
- the complete Node checkpoint (identity, HLC, records, Outbox, receipts, receive streams, aliases, peer progress and ChangeId ledger);
- StorageDataProvider state and local source feed;
- StorageMetadataStore outbound frozen payloads, inbound relay/receipts and PendingApply state.

Thus the Node checkpoint, provider data and metadata survive a single database close/reopen. The second update and record deletion happen after reopening these same databases; no separate Node state files are used by the sample. Each component still commits its own atomic Storage batch, so the coordinator's recovery order remains Node checkpoint → metadata PendingApply → provider receipt → metadata finalization → network ACK.

The latest integration tests also verify that a conflicting global ChangeId fingerprint makes outbound metadata freeze fail closed and leaves the source provider feed unacknowledged.




## 7.28 Provider-authoritative outbound publication cutover (2026-11-08)

The Storage-backed outbound source now supports a durable, explicit authority transition:

- Before cutover, the coordinator drains the provider feed through the legacy Node-Outbox transition path. Cutover is rejected until the provider feed is empty/acknowledged and Node/Metadata Store author high-watermarks match.
- The provider persists an immutable cutover watermark. Afterward, the coordinator rejects Node Outbox advancement and fails closed if the marker is absent while Metadata Store is ahead.
- New local operations are frozen directly from stable provider ChangeIds into Metadata Store. Metadata Store assigns the outbound author sequence; the provider feed is acknowledged only after the freeze transaction commits.
- Node is rehydrated from a stable provider snapshot and remains an application-data cache. After cutover, duplicate local-author Outbox payload rows are removed from the Node checkpoint, but the monotonic sequence watermark and WAL/ChangeId replay identities remain. Legacy relay payload rows are removed from the Node cache at provider cutover; relay history already committed to Metadata Store remains available for downstream peers.
- The coordinator checks the durable global ChangeId fingerprint ledger when replaying an unacknowledged provider feed. It retains already-frozen operations and coalesces only the later unfrozen suffix. This handles the crash/interruption window where freeze commits, provider ACK fails, and a newer edit arrives before retry.
- `StorageArrayTcpSync` enables the cutover, completes an explicit empty initial peer baseline, sends through the Metadata Store durable transport overload and verifies a post-reopen update without advancing Node Outbox.
- `AutoReconcileWithMetadataAsync` detects the persisted cutover marker and runs provider-authoritative rehydration before anti-entropy, so a stale Node checkpoint cannot reintroduce provider-external rows.

Verified at BIN-SYNC code/test head `be0d48db38e1c7f56cbd392311204a89120e72c5`: 644 core tests, 102 Storage Integration tests, 31 Fuzz/Lab cases, 70.8% line coverage and both Storage array samples pass.

**Boundary:** this is the provider-owned outbound publication slice, not complete Node/Transport replacement. Node session epoch fencing, alias resolution, runtime caches and the legacy path remain. Physical crash-cut injection, multi-process concurrency/fencing, large-partition benchmarks and independent retention/GC proofs remain open.


## 7.29 Storage transaction serialization and single-writer fencing (2026-11-08)

The Storage adapter now has two complementary serialization layers:

- `StorageDatabaseTransactionGate` is a process-local shared gate keyed by the actual `StorageDatabase` instance. `StorageDataProvider`, `StorageMetadataStore` and `StorageNodePersistence.Save` use the same gate when attached to that instance, preventing stale-base WAL commit conflicts between adapters with previously independent locks.
- `WalWriterFence` acquires one named OS-managed Mutex per normalized physical WAL directory and partition. A process-wide manager thread owns/releases the Mutex so a `StorageDatabase` may be disposed from any caller thread. The OS abandons/releases the Mutex if the owning process terminates; the next opener handles abandoned acquisition before reading the snapshot or replaying/truncating WAL.
- The fence rejects a second open of the same partition in one process and across processes. The design is BCL-only; no P/Invoke or native library dependency was added.
- This is deliberately **single-writer per partition**, not multi-writer. Parallel adapter operations sharing one in-process `StorageDatabase` are serialized; separate processes cannot write the same partition concurrently.

Verified at BIN-SYNC `8e6833e5aa5fe7086dbd72b18cf71e45f30169c9`: 647/647 core tests, 107/107 Storage Integration tests on Ubuntu, Windows and macOS, 31/31 Fuzz/Lab cases, 70.8% line coverage; both Storage array samples and the real-TCP restart/tombstone flow pass on all three OSes. Integration regressions cover concurrent provider+metadata commits through one shared database, same-process and cross-process second-writer rejection, successful reopen after dispose, recovery after owner-process termination and abandoned-thread recovery.

**Still to validate:** deterministic physical power-loss cuts during WAL append/rotation/checkpoint replacement and controlled end-to-end retry-scan allocation/latency benchmarks. The current cross-process tests validate lock release/recovery after process termination; they do not simulate torn device writes or storage-controller cache loss.

## 12. Current Storage adapter representation

At BIN-SYNC `feature/fld-external-storage` commit `d264d4a9c58998a6ff3baea9458b5f9fb61ab202`, the Storage adapter's durable representation is:

- ChangeId ledger: one 18-byte logical suffix (namespace + ChangeId) and a 17-byte value (16-byte fingerprint + one origin flag). The old additional kind-11 outbound-origin marker is accepted by the decoder and folded into the value on the next transaction.
- Relay stream watermark: one key per (namespace, original author), with a 16-byte value containing ACK and high-watermark. Legacy separate ACK/high-watermark keys are accepted and rewritten together on the next transaction.
- Local outbound stream watermark: one key per (namespace, local author), with a 16-byte value containing ACK and high-watermark. Legacy separate rows are accepted and rewritten together.
- Provider local feed: durable append-only sequence until the coordinator successfully checkpoints/finalizes publication and acknowledges the scanned prefix. The coordinator drains the complete currently-pending feed across pages and coalesces LWW winners before Node writes. The provider feed itself is the durable PendingLocal journal; it is ACKed only after Node persistence and Metadata Store freeze.

The current Storage provider feed carries stable ChangeIds. Before cutover, the transition path mirrors selected Node.Outbox winners into Metadata Store. After cutover, Metadata Store owns local outbound payloads and sequence allocation; the duplicate local-author payload rows are removed from Node while the high-watermark and WAL/ChangeId replay markers remain. The Storage-backed transport overload reads the durable Metadata Store stream. Legacy Node transport and legacy relay Outbox payload rows are removed from Node at provider cutover; Metadata Store retains the authoritative relay stream. Verified code/test head `0399e8f0dfda39b5e860eca50364406323c7b7fa`: 644/644 core, 100/100 Storage Integration, 31/31 Fuzz/Lab, 70.8% line coverage; real TCP array sync passes.

**Deferred:** FLD Database's JournalSeq provenance mapping and its dedicated transaction adapter are not part of the current BIN-SYNC + Storage completion gate.


## Latest continuation checkpoint — complete coordinator receive process-kill sequence (2026-11-08)

- Code/test HEAD: 354eab38dc859e2f26838efb332c14310aa17022.
- The experimental file-share WAL lease was reverted after a cross-process lock/test stall. The current single-writer fence is a named OS-managed Mutex, acquired/released on its dedicated process-wide manager thread.
- Cross-process tests target the actual testhost PID and have unconditional child-process cleanup with bounded waits. Storage Integration uses --blame-hang-timeout 3m --blame-hang-dump-type mini to capture diagnostics for any future hang.
- Added actual process-kill/reopen coverage at all four receive durability boundaries: after PendingApply staging before Node apply; after Node checkpoint before provider apply; after provider commit before Metadata Store finalization; and after Metadata Store finalization before network ACK. The final cut verifies the durable receive cursor/receipt remains committed and no PendingApply remains even though ACK was not sent.
- Recovery requires the negotiated alias table and peer epoch in the Node checkpoint. StorageNodePersistence.Save is explicitly documented and demonstrated in both Storage samples immediately after each successful SyncSession handshake and before the first inbound durable batch.
- Windows process-kill recovery retries WAL reopen for up to ten seconds to accommodate delayed OS release of the killed process's file handle.
- Verified: BIN-SYNC Core Release 648/648; Fuzz + Lab 31/31; line coverage 70.4%; Storage Integration 127/127 independently on Ubuntu, Windows and macOS-15; in-memory and real-TCP samples pass on all three platforms.
- PR #16 remains Draft/open/unmerged.

**Remaining:** true physical power-loss/I/O fault injection; retirement of legacy Node-owned receive/transport and other-author relay state; receipt/dedup/tombstone GC horizons; and FLD JournalSeq provenance/atomic staging integration. The two-DLL decision remains Fld1.Storage.dll plus Fld1.Database.dll.
