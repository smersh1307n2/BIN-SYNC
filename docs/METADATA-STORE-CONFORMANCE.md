# BIN-SYNC Metadata Store Semantic Contract and Conformance Plan

**Status:** design baseline for implementation phase 3; semantic contract only.  
**Related architecture:** [Metadata Storage and External Data Provider Architecture](METADATA-STORAGE-ARCHITECTURE.md).  
**Protocol baseline:** BIN-SYNC CORE v2.4.19 FINAL; this document does not change wire format.  
**Purpose:** define the observable operations and provider-independent tests required before moving Node persistence behind a durable Metadata Store.

## 1. Scope and non-goals

This document specifies the durable behavior that a Metadata Store must provide to BIN-SYNC Runtime. It does not freeze public C# interface names, generic type parameters, key encodings, file formats, or a concrete FLD adapter API. Those choices follow only after the semantic contract and test fixture are reviewed against the current Node state inventory and external-provider transaction modes.

The Metadata Store is not the Data Provider and is not the physical Storage engine:

- **Data Provider** owns user data, stable item/field mapping, source change capture, source snapshots and durable idempotent application.
- **Metadata Store** owns durable BIN-SYNC metadata: replica/scope state, inbox receipts, ChangeId ledger, peer/author progress, frozen outbound changes, relay retention and pending-apply recovery intents.
- **Physical Storage** owns generic atomic byte transactions, durability, recovery, snapshots, ordered scans and storage-native commit identifiers.
- **BIN-SYNC Runtime** owns protocol interpretation, validation, ACK aggregation, sequencing, relay decisions and generation of semantic state changes.

A physical implementation may co-locate these roles, but its transaction guarantees must be explicit. FLD Storage-only mode is a Metadata Store substrate, not FldDatabase with protocol features switched off.

## 2. Partition and identity rules

Every durable key must be scoped by the owning synchronization scope and local replica identity. Protocol NamespaceId is an additional dimension inside that partition; it is not a complete tenancy key.

The durable identity dimensions are:

- SyncScopeId: identifies one independent synchronized dataset/configuration.
- LocalReplicaNodeId and incarnation/epoch: identifies the replica whose metadata is stored.
- NamespaceId: existing protocol namespace, scoped under the owning replica.
- PeerNodeId and peer epoch/session incarnation where relevant.
- Original AuthorNodeId for author streams and relay retention.
- BatchId for incoming batch receipt identity.
- ChangeId for route-independent mutation identity.

No sequence coordinate may be substituted for another. In particular, physical CommitSeq, peer-relative cursor, author OutboxSeq, source relay JournalSeq, HLC wall time and snapshot generation are distinct domains.

Two scopes or replicas sharing one physical store must never observe each other's inbox, peer state, dedup ledger, Outbox, relay data, leases or retention watermarks.

## 3. Required semantic operations

Names below are descriptive labels, not proposed public API identifiers.

### 3.1 Open and recover replica state

Opening a replica must recover one internally consistent state for its scope and replica identity:

- stable local NodeId and incarnation/epoch;
- HLC recovery state, if the runtime uses a local HLC;
- per-peer/per-namespace receive and author-stream progress;
- inbox receipts and their compact duplicate-response data;
- exact ChangeId dedup facts and semantic fingerprints;
- local frozen Outbox and its per-author sequence allocators;
- relay journal and per-author retention watermarks;
- any unfinished pending-apply intents;
- lease/fencing generations required to reject stale consumers.

A corrupt, unsupported or incomplete durable state must fail closed. Recovery must not silently reset an identity, sequence, ACK watermark, receipt or dedup fact.

### 3.2 Accept an incoming batch

One successful strong-profile acceptance must atomically or recoverably establish all metadata required to answer a retry and safely ACK the sender:

1. validate the incoming identity and chain position in Runtime;
2. persist a compact Inbox receipt keyed by the protocol duplicate key, including enough cursor/digest/author/result fields to validate exact duplicate reuse;
3. advance only the matching sender/namespace/epoch receive cursor and digest-chain state;
4. update the global route-independent ChangeId ledger using canonical semantic fingerprints;
5. append relay-eligible operations with original author and ChangeId intact, including eligible LWW losers;
6. persist the bounded ProtocolStateDelta or materialize its semantic mutations;
7. coordinate the Data Provider effect using the selected atomicity mode;
8. make the committed result recoverable before the Runtime emits ACK.

Same batch identity with the same canonical content returns the original duplicate result and does not reapply or relay. Conflicting cursor/digest/author metadata for the same receipt key is rejected. Same ChangeId and same fingerprint arriving by another route is not reapplied or relayed, but the new sender's own receipt/cursor is still committed. Same ChangeId with a different fingerprint rejects the whole batch before any visible mutation.

**Atomicity modes:**

- Shared transaction: provider data effect and all required metadata mutations commit in one transaction.
- Separate stores: persist a pending-apply intent; provider atomically applies the stable token with its data effect and durable idempotency receipt; Metadata Store then finalizes receipt/cursor/dedup/relay/protocol state. Recovery queries or retries the same token and finalizes exactly once logically.
- Best-effort: explicitly weaker, opt-in only. It must never claim durable exactly-once acceptance or emit an ACK that implies it.

A generic callback or event is not an atomicity mechanism.

### 3.3 Capture, freeze and read local outbound changes

The provider/runtime must not lose a local source change before it is represented durably.

Before publication, eligible pending LWW operations may be coalesced according to the approved change-staging contract. Freeze is the immutable publication boundary:

- assign permanent author-stream sequence and stable ChangeId;
- retain canonical operation payload or a durable reference sufficient to reproduce it;
- commit freeze durably before any sender can enumerate or transmit the operation;
- never rewrite or coalesce an already frozen entry;
- preserve original author, HLC, KeyEpoch, operation kind and payload across retry/restart.

Reads are bounded and stable. A reader must not hold a global writer lock during network I/O. Multiple consumers require lease or generation fencing. A stale consumer must not ACK after a newer consumer takes ownership.

### 3.4 Acknowledge local outbound changes

Runtime validates peer ACKs and calculates the safe contiguous prefix across every interested peer. Metadata Store receives only that safe prefix plus the relevant lease/generation context.

The store must reject:

- ACK beyond the durable high watermark;
- backward ACK;
- a prefix containing a gap;
- stale lease or stale generation;
- ACK for another scope, replica, namespace or author.

ACK authorizes retention/compaction of the frozen Outbox prefix only. It does not authorize ChangeId ledger GC, Inbox receipt deletion, tombstone deletion or forgotten-frontier GC. Those have independent lifetimes and proof requirements.

No interested peer, a peer without durable progress, or an unknown membership baseline is fail-closed and cannot be interpreted as ACK of the full stream.

### 3.5 Read and acknowledge relay changes

Relay is independent from the local-origin Outbox stream. Each relay operation retains its original author and ChangeId; relaying does not create a new operation identity.

Relay reads are bounded, author-filterable and resumable. The returned scan cursor must distinguish read progress from safe deletion authority. The store must preserve all relay-eligible operations until Runtime proves a safe contiguous per-author retention prefix across all interested peers.

Relay ACK is per original author. Mixed-author source batches may be reclaimed independently by author only when all operations for that author in the covered prefix are safe. Acknowledging relay payload must not delete the Inbox receipt or global ChangeId fact.

Where a provider has its own JournalSeq (such as FLD ApplyJournal), translation between that coordinate and BIN-SYNC OutboxSeq is adapter-owned provenance. The mapping and scan cursor must commit atomically; it is not part of the physical Storage API and must not be guessed from current field winners.

### 3.6 Snapshot and recovery view

Metadata Store snapshots must provide a consistent read view across concurrent commits/checkpoints/compaction for their documented lifetime. A snapshot remains valid until its lease is released or expires under an explicit fencing rule.

Data Provider snapshots are separate: they must bind source state to a stable source version and change-feed watermark, or provide an equivalent write barrier. An arbitrary provider snapshot and an unrelated metadata cursor do not form a baseline.

Installing a current-state snapshot is not proof that historical Outbox/relay operations or dedup facts can be deleted. Until the protocol defines per-author cuts and explicit completion/fencing, snapshot completion must not advance stream offsets or retention watermarks.

### 3.7 Pending apply intent lifecycle

For separate provider/metadata stores, pending intent is a recoverable protocol state, not an unbounded duplicate log:

- intent has a stable token, scope/replica/namespace and enough canonical input or a durable payload reference to retry;
- provider records the token/receipt atomically with its data effect;
- metadata finalization materializes ordinary indexed metadata and compact receipt;
- after both sides are durable, raw intent payload is removed;
- duplicate retry returns the compact receipt, not the old full payload;
- restart discovers unfinished intents and resumes them before normal publication.

## 4. Physical Storage requirements (below Metadata Store)

Physical Storage exposes generic operations only:

- atomic bounded write batch / transaction;
- durable commit result and storage-native CommitSeq;
- point reads and bounded ordered scans;
- consistent snapshots with explicit lifetime;
- recovery after torn tail / interrupted commit;
- concurrency control and writer fencing appropriate to the supported process model;
- checksums, format versioning and deterministic corruption behavior;
- safe checkpoint/compaction while snapshots remain valid.

It must not expose NodeId, ChangeId, BatchId, HLC, peer, author, Outbox, relay or wire DTO semantics. Storage-only writes must not fabricate synchronized field records, local authors, HLC values or Outbox entries.

## 5. Provider-independent conformance matrix

A candidate Metadata Store implementation is conformant only if the same semantic test suite passes without relying on its physical layout.

| Area | Required test / invariant | Failure that must be prevented |
|---|---|---|
| Partitioning | Two scopes × multiple namespaces × two local replicas; interleaved writes/reopen | Cross-scope or cross-replica state leakage |
| Recovery | Reopen after every successful semantic commit | Lost cursor, identity, receipt, ACK or dedup fact |
| Incoming batch | Commit, restart, retry exact batch | Reapply or duplicate relay |
| Receipt conflict | Reuse receipt key with altered cursor/digest/author | Accept equivocation or return wrong ACK |
| ChangeId | Same ID/same fingerprint through different peers | Duplicate mutation/relay |
| ChangeId conflict | Same ID/different fingerprint in one batch | Partial batch publication |
| LWW loser | Accepted losing mutation remains relay eligible | Losing operation silently disappears |
| Separate stores | Crash before provider apply, after provider commit, before metadata finalize, after finalize | Duplicate provider effect or ACK before durable completion |
| Local staging | Several same-field pending LWW writes coalesce before freeze | Unnecessary unpublished history |
| Freeze | Crash around sequence/ChangeId assignment and durable commit | Sequence reuse or mutable published payload |
| Outbound read | Page limits, resume, concurrent append and restart | Gap, duplicate, unstable payload |
| Lease | Old reader resumes after a new lease is acquired | Stale reader sends or ACKs |
| Outbound ACK | Unequal peer ACKs, unknown peer, missing peer state, gap and out-of-range | Unsafe prefix reclamation |
| Relay | Multiple authors in one source batch; author-specific reads and ACKs | One author's ACK deletes another author's payload |
| Relay recovery | Crash before/after relay ACK and during compaction | Lost relay operation or receipt |
| Snapshot | Snapshot concurrent with commit/checkpoint/compaction | Mixed-generation or use-after-reclaim view |
| Snapshot retention | Complete current-state snapshot without explicit stream-cut proof | Incorrect Outbox/relay/dedup GC |
| Corruption | Bad checksum, unsupported version, truncated record, duplicate key | Silent reset or partial state exposure |
| Storage-only | Metadata writes/reopen/scan | Synthetic HLC/author/ChangeId/local Outbox |
| Boundedness | Maximum record, batch, page, count and scan limits | Unbounded allocation or integer overflow |

Crash tests must use deterministic fault injection at semantic durability boundaries, not sleeps or probabilistic process termination. For each injected failure, assert both the recovered state and whether publication/ACK is permitted.

## 6. Existing BIN-SYNC test coverage versus this contract

The current suite already provides valuable protocol/runtime evidence:

- `WalTests` covers Node WAL append/replay, local mutation identity, torn-tail handling, attached-WAL commit ordering and checkpoint interaction.
- `RemoteBatchWalAtomicityTests` covers failure before publication, semantic WAL replay failure, lifecycle preflight and idempotent replay.
- `Hardening.cs` covers full NodePersistence round-trip, sequence continuation after restart, stream-offset recovery, minimum-peer-ACK/fail-closed behavior, retained ChangeId→OutboxSeq resolution, corrupt/truncated snapshot rejection, count/varint bounds, atomic snapshot replacement and concurrent save/write.
- `ChangeIdWireTests.cs` covers route-independent ChangeId fingerprints, alternate-route duplicate suppression, equivocation rejection, global-ledger persistence and v5–v12 receipt/ledger migration compatibility.
- `SnapshotTests` covers snapshot encoding/application, tombstones, chunk bounds and concurrent transfers.
- `InvariantsAndAdvanced.cs` covers author-stream ordering, ACK/re-delivery and multi-node relay behavior.
- `SessionStreamBindingTests` covers namespace/stream binding at the transport boundary.
- E-25 tests preserve fail-closed tombstone retention.

These tests provide substantial coverage of the current NodePersistence/WAL implementation and should not be repeated as new test cases unless a refactor changes their execution path. They do **not** yet prove conformance of an arbitrary Metadata Store or FLD Storage-only implementation. The following remain unimplemented at the abstraction boundary:

1. A provider-independent semantic store contract and reusable conformance fixture.
2. A store implementation whose durable state is not a serialized Node object graph.
3. Shared-transaction and separate-store pending-apply conformance across restart.
4. Scope/replica partition isolation in one physical store.
5. Stable metadata snapshot lifetime under compaction.
6. Generic storage-only semantics with no fabricated HLC/author/Outbox.
7. End-to-end crash tests for the FLD Storage-only adapter.
8. Durable FLD ApplyJournal JournalSeq → BIN-SYNC ChangeId/OutboxSeq provenance transaction and publication gate.

Do not count existing NodePersistence roundtrip tests as proof of items 1–7. They are necessary regression tests during refactoring, not substitutes for the new conformance suite.

## 7. Implementation sequence and gates

Follow the architecture sequence:

1. Review and approve this semantic contract; resolve remaining decisions affecting transaction and recovery behavior.
2. Finalize change staging/coalescing lifecycle and its crash/relay tests.
3. Implement a provider-independent conformance fixture and a test-only volatile store. The volatile store is for semantic tests only and must advertise no restart durability.
4. Define the neutral Data Provider contract and its shared-transaction / separate-store idempotent apply modes.
5. Refactor BIN-SYNC persistence behind the semantic boundary without wire changes; keep existing Node/WAL tests as regression coverage.
6. Extract and implement FLD's two-assembly physical Storage Core and Storage-only role.
7. Implement the BIN-SYNC Metadata Store adapter over the neutral Storage API.
8. Separately complete the FLD synchronized-database integration, including the durable source JournalSeq provenance mapping, publication gate and ACK translation.
9. Validate external SQL, file, array and custom-provider scenarios.
10. Freeze public API and package/version documentation only after the conformance suite passes.

**Do not start by adding a direct `Node ↔ FldDatabase` callback bridge.** The current Node and FldDatabase APIs do not create a shared transaction domain. The durable relay provenance adapter remains required, but it is a later synchronized-database integration gate, not a replacement for the Metadata Store boundary or Storage-only implementation.

## 8. Change control

Any implementation that changes these observable guarantees must update this document and the matching conformance tests. Physical key/value layout, record grouping, checksums and compaction strategy may evolve without changing the semantic contract, provided all conformance tests continue to pass.
