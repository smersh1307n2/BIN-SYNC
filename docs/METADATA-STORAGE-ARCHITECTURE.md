# BIN-SYNC Metadata Storage and External Data Provider Architecture

**Status:** Architecture contract draft v0.2 — design baseline for subsequent API design and implementation.  
**Scope:** separation of synchronization runtime, external data providers, and durable metadata storage.  
**Wire protocol:** unchanged; this document does not revise BIN-SYNC CORE v2.4.17.  
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

Persist the state required to recognize a previously accepted batch and safely answer a retry after a lost ACK. This includes the relevant sender/issuer identity, namespace, epoch, BatchId, accepted cursor range, digest-chain checkpoint, and durable receipt/result needed by the protocol.

Persist ChangeId deduplication/provenance state with the protocol-defined sender/channel and namespace dimensions; a global ChangeId set without the required context is not a valid substitute. Important current gap: E-8/E-13 define first-seen relay eligibility and the durable SeenChanges key, but the current normative specification does not define a safe SeenChanges GC horizon. Inbox GC and tombstone GC are separate and cannot be reused as an assumed rule. Until a protocol-safe deduplication watermark/expiry is defined, exact SeenChanges entries must be retained; an arbitrary TTL or Bloom-filter-only replacement can re-enable relay loops or suppress legitimate first-seen changes. See the detailed retention analysis in [Change Staging, Coalescing, and Metadata Retention](CHANGE-STAGING-AND-RETENTION.md).

### 5.4. Peer and stream state

Persist peer-specific state that affects future protocol decisions, including LastPushed/LastReceived/LastAcked where used, per-author StreamPushed/StreamAcked/StreamOffsets, received-stream and batch replay mappings, InitialSyncComplete, peer incarnation/epoch, membership epoch, and durable membership/retention facts. Preserve author-stream identity; one scalar cursor per peer or namespace is insufficient.

A peer's cursor mapping or ACK must not alter another peer's stream view. Unknown or newly introduced peers must not silently inherit an ACK that allows deletion of data they have not received.

### 5.5. Relay journal

Remote changes that are accepted for relay must remain recoverable until the relevant downstream peers have safely acknowledged them.

The relay journal must preserve original change identity, author, HLC, key epoch, operation and payload/reference. It must include operations selected for relay even when they lose the local LWW comparison, where required by BIN-SYNC relay semantics.

The relay journal is distinct from the local-origin outbound journal. Their cursors, ownership and retention rules must not be conflated.

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
- Inbox idempotency keys and InboxAuthors needed to reproduce the correct author-bound duplicate ACK;
- LastBatchDigest checkpoints and per-stream digest-chain checkpoints;
- peer state: received-stream mappings, StreamPushed, StreamAcked, StreamOffsets, batch range → BatchId/previous-digest mappings, initial-sync status, membership epoch and applicable last-seen/retention data;
- SeenChanges keyed with the protocol-defined peer/provenance and namespace dimensions;
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

A state snapshot is not automatically an acknowledgement of every frozen Outbox or relay operation that predates its source watermark. The runtime and provider contract must explicitly define which operations are semantically covered by the snapshot baseline and which provenance/relay operations still require delivery. This is especially important for LWW losers, which are not present in the current-state snapshot but may remain relay-eligible. Until that coverage rule is specified and tested, snapshot completion must not by itself advance Outbox/relay safe-ACK or GC boundaries, or discard pending/frozen changes.

The current BIN-SYNC SnapshotChunkMessage/BuildSnapshotChunks path is a current-state LWW snapshot and does not expose a per-field ChangeId or Outbox cursor in the inspected DTO. The normative specification separately describes SnapshotTail/outbox checkpoint information; reconcile that spec/runtime boundary before designing storage operations that rely on snapshot-tail coverage.

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

## 15. Decisions intentionally left open

The following are not decided by v0.2 and must be resolved before freezing public API:

1. Exact public names and signatures for provider and metadata interfaces.
2. Whether the runtime exposes a unified transaction object, semantic atomic commands, or both.
3. Exact scope/replica identifier representation and how it is supplied at construction.
4. Which protocol state is mandatory in the first implementation versus capability-gated.
5. Whether a built-in in-memory store ships in the core package or only in test utilities.
6. Whether FLD Storage-only is a dedicated public engine/facade or an adapter over a separated internal storage core.
7. How to represent storage-native ordering/version information without fabricating protocol HLC/author fields required by the current FLD1 run format.
8. How a multi-scope/multi-namespace Metadata Store maps onto FLD's currently namespace-scoped physical runs and database options.
9. Exact metadata record layout, key encoding, compaction and retention strategy in FLD.
10. How current NodePersistence/WAL state migrates to the new provider/store boundary.
11. Whether external providers use shared transactions or the pending-apply/idempotency protocol in the first release.
12. Which guarantees are required for the first stable public contract and which weaker modes, if any, may be explicitly supported.
13. Exact snapshot-to-tail coverage semantics: whether a state snapshot can subsume frozen local LWW operations, how relay-eligible LWW losers are retained/delivered, and how the normative SnapshotTail/outbox checkpoint maps to the current runtime.
14. A finite, protocol-safe SeenChanges/provenance dedup GC rule. Current NodePersistence stores the exact peer/namespace/ChangeId set, but the normative spec has no corresponding expiry or safe watermark; production storage cannot safely compact it by an invented TTL.

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