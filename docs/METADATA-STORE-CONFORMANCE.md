# BIN-SYNC Metadata Store Semantic Contract and Conformance Plan

**Status:** semantic contract plus executable conformance. Twenty provider-independent semantic tests run against both a volatile reference adapter and the durable `StorageMetadataStore` adapter. The Storage adapter passes restart, compaction, snapshot, equivocation and pending-apply recovery integration. The optional durable peer capability now owns per-peer/per-author send cursors, immutable batch replay reservations, interested-peer roster and ACK-based payload retention for the opt-in transport path. Full Node/Transport authority cutover is still incomplete: receive validation and the Node inbox/digest cache remain transitional authorities, and the legacy transport path remains Node-backed.  
**Related architecture:** [Metadata Storage and External Data Provider Architecture](METADATA-STORAGE-ARCHITECTURE.md).  
**Related provider contract:** [Data Provider Semantic Contract and Conformance Plan](DATA-PROVIDER-CONFORMANCE.md).  
**Protocol baseline:** BIN-SYNC CORE v2.4.20 FINAL; this document does not change wire format.  
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

Same batch identity with the same canonical content returns the original duplicate result and does not reapply or relay. The receipt key is exactly `(sender, senderEpoch, namespaceId, BatchId)`; resolved author-stream identity belongs to receive progress and cannot create a second receipt for the same protocol batch. Conflicting cursor/digest/author metadata for the same receipt key is rejected. Same ChangeId and same fingerprint arriving by another route is not reapplied or relayed, but the new sender's own receipt/cursor is still committed. Same ChangeId with a different fingerprint rejects the whole batch before any visible mutation.

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

For a separate Data Provider feed, the Metadata Store/runtime handoff must also persist the canonical pending operations, a replay/equivocation receipt for the source page and the provider's staged-through cursor atomically. The provider feed prefix is acknowledged only after this stage transaction is durable. If the process stops before provider ACK, replay of the same source page must be idempotent. The ownership and ordering rules are detailed in [Metadata Storage Architecture §3.3](METADATA-STORAGE-ARCHITECTURE.md) and [Data Provider Conformance](DATA-PROVIDER-CONFORMANCE.md).

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
- `BeginPendingApply` distinguishes a token that was already finalized with `MetadataPendingApplyAlreadyFinalizedException`; unrelated `InvalidOperationException` state conflicts must propagate and must never be reinterpreted as duplicate delivery;
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
| Source feed handoff | Replay the same provider page after stage commit but before provider ACK; reject source-token equivocation; ACK only after durable stage | Lost or duplicated local source changes |
| Local staging | Out-of-order same-field LWW writes choose the canonical HLC/author winner; non-LWW operations remain distinct; remote apply does not overwrite pending local payload | Unnecessary unpublished history or loss of local intent |
| Freeze | Crash around sequence/ChangeId assignment, freeze receipt and pending removal; retry the same freeze token | Sequence reuse, duplicate Outbox entries or mutable published payload |
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

These tests provide substantial coverage of the current NodePersistence/WAL implementation and should not be repeated unless a refactor changes their execution path. The new abstraction-level status is:

1. **Metadata Store semantic contract and reusable fixture: implemented.** Twenty provider-independent semantic tests run against a volatile reference model; the fixture explicitly does not claim restart durability.
2. **Physical durable Metadata Store implementation: implemented for the current `IMetadataStore` contract.** `StorageMetadataStore` persists receipts, ChangeId ledger, relay/outbound payloads, leases and PendingApply state through `Fld1.Storage`; the Storage integration suite exercises close/reopen and compaction. NodePersistence still independently owns Node/Transport peer state, Outbox, replay mappings and the runtime's active receive state.
3. **Shared-transaction/separate-store apply: semantic retry behavior covered; real provider/store crash/reopen integration remains open.**
4. **Scope/replica isolation: covered by volatile semantic tests; one durable physical store/reopen proof remains open.**
5. **Snapshot lifetime: logical view stability across append/ACK/compaction and disposal are covered by the volatile model; physical generation pinning and concurrent durable checkpoint/compaction remain open.**
6. **Generic Storage-backed Metadata Store semantics: implemented for the current contract and covered by durable adapter conformance.** Full Node/Transport authority migration and end-to-end FLD Storage-only packaging remain separate gates.
7. **FLD ApplyJournal JournalSeq → BIN-SYNC ChangeId/OutboxSeq provenance transaction and publication gate: open.**
8. **PendingLocal/source-feed staging and coalescing: open.** The current Metadata Store suite tests frozen Outbox operations directly; it does not yet test idempotent provider-page staging, HLC winner coalescing, remote-update preservation of pending local payload or freeze-retry cleanup.

The separate [Data Provider Conformance](DATA-PROVIDER-CONFORMANCE.md) suite now has six semantic tests plus a volatile-profile assertion for stable mapping, feed paging/ACK fencing, snapshot/feed watermark, idempotent remote apply and origin separation. Durable provider reopen/fault-injection and end-to-end feed ACK only after durable Metadata Store staging remain open.

Do not count existing NodePersistence roundtrip tests or volatile reference tests as proof of durable external-store conformance. They are regression/semantic evidence, not substitutes for real adapter crash/reopen tests.

## 7. Implementation sequence and gates

Follow the architecture sequence:

1. Review and approve this semantic contract; resolve remaining decisions affecting transaction and recovery behavior.
2. **Open next gate:** finalize PendingLocal/source-feed staging and coalescing semantics plus deterministic crash/relay tests. The 39-case change-staging matrix is design evidence, not an executable provider-independent staging suite yet.
3. **Initial slice complete:** provider-independent Metadata Store contract and volatile fixture are present (20 semantic tests); durable store/reopen implementation remains open.
4. **Initial slice complete:** neutral Data Provider semantic contract and volatile fixture are present (six semantic tests); durable provider and end-to-end source-feed-to-metadata staging remain open.
5. Refactor BIN-SYNC persistence behind the semantic boundary without wire changes; keep existing Node/WAL tests as regression coverage.
6. Extract and implement FLD's two-assembly physical Storage Core and Storage-only role.
7. Implement the BIN-SYNC Metadata Store adapter over the neutral Storage API.
8. Separately complete the FLD synchronized-database integration, including the durable source JournalSeq provenance mapping, publication gate and ACK translation.
9. Validate external SQL, file, array and custom-provider scenarios.
10. Freeze public API and package/version documentation only after the conformance suite passes.

**Do not start by adding a direct `Node ↔ FldDatabase` callback bridge.** The current Node and FldDatabase APIs do not create a shared transaction domain. The durable relay provenance adapter remains required, but it is a later synchronized-database integration gate, not a replacement for the Metadata Store boundary or Storage-only implementation.

## 8. Change control

Any implementation that changes these observable guarantees must update this document and the matching conformance tests. Physical key/value layout, record grouping, checksums and compaction strategy may evolve without changing the semantic contract, provided all conformance tests continue to pass.


## 9. Current Node source refactor surface

A branch-level source audit identified the concrete places that must be separated when Data Provider and Metadata Store become independent. This is an impact inventory, not approval to change these files in one mechanical pass.

| Current source | Current responsibility / coupling | Required direction |
|---|---|---|
| `Node.cs` | Owns the `_storage` namespace→record→field graph and exposes record/field reads/counts; owns Outbox and peer metadata alongside it | Keep protocol identity and coordination in Runtime; delegate authoritative user-data reads to Data Provider; do not let Metadata Store become a second data store |
| `Production/Node.LocalWrites.cs` | Local writes mutate `Storage` and create Outbox operations under Storage→Outbox ordering | Provider captures local changes durably; freeze and metadata Outbox publication must share the provider transaction or a durable feed/intent boundary |
| `Production/Node.ApplyBatch.cs` | Incoming batch validation, field-state application, ChangeId dedup, peer cursor and relay Outbox updates currently share Node locks/state | Split provider data effects from protocol metadata; preserve atomic shared-transaction or explicit pending-apply recovery semantics |
| `Production/Snapshot.cs`, `Production/Reconcile.cs` | Snapshot build/apply and reconciliation enumerate or mutate Node.Storage | Read/apply through Data Provider; bind provider snapshot to a source version/feed watermark; current snapshot completion is not retention proof |
| `Production/Node.Republish.cs` | Re-publishes current Node.Storage fields as fresh Outbox operations | Treat as provider rescan/repair operation with explicit identity semantics; never use it to reconstruct missing historical ChangeId or relay provenance |
| `Production/Node.Helpers.cs` | Computes record digests and record maps from Node.Storage | Move data enumeration/digest input to provider snapshot; keep digest algorithm/protocol decisions in Runtime |
| `Production/Node.Causal.cs` | Documents and coordinates callback execution around the Storage→Outbox→Meta lock order | Preserve a documented lock order; no external I/O or callbacks while engine locks are held |
| `Production/NodePersistence.cs` | Serializes both user field Storage and protocol metadata (Outbox, peer state, receipts, ledgers, etc.) into one Node snapshot | Split provider-owned data persistence from Metadata Store state; NodePersistence cannot remain the Metadata Store implementation for an external provider |
| `Production/Node.Wal.cs` + `Production/Wal.cs` | WAL replay restores local mutations and accepted remote batches, including data payload effects | Separate provider-owned durable data/change feed from metadata recovery intent; retain idempotent replay and exact ChangeId identity |
| `Production/Node.DeltaBatch.cs` | Builds per-author outbound batches from retained Outbox and computes safe per-author ACK positions | Remains Runtime/Metadata Store behavior; Outbox must be loaded from the Metadata Store, not reconstructed from current provider winners |

The current global lock order is documented as **Storage → Outbox → Meta**. The external provider/Metadata Store split must not hold provider or Node locks across network waits, and must not perform disk I/O while holding the other engine's internal locks. A shared transaction coordinator may order durable operations, but it does not replace each engine's local lock discipline.

The refactor must preserve existing tests for local writes, remote apply, WAL replay, snapshots/reconcile, digests, author-stream batching and ChangeId dedup. The initial Data Provider suite now covers semantic source mapping, bounded feed paging, snapshot/feed continuity, idempotent remote apply and no recursive capture of remote writes. Remaining tests must exercise actual durable feed/reopen behavior, concurrent snapshot barriers, provider-side receipt recovery and end-to-end staging/ACK ordering.


## 10. Initial executable harness status

The first executable slice is present in the BIN-SYNC branch:

- `BinSync.Net/tests/BinSync.MetadataStore.Conformance` contains the test-only semantic adapter contract and inherited conformance suite; it is referenced by `BinSync.Tests` but intentionally not a standalone solution test target.
- `BinSync.Net/tests/BinSync.Tests/Production/VolatileMetadataStoreAdapter.cs` is a reference model only; it clones partition state before acceptance so a validation failure cannot publish a partial receipt/cursor/dedup/relay transition.
- `VolatileMetadataStoreConformanceTests` runs the shared suite against that model and separately asserts that the reference adapter does not claim restart durability.
- The twenty semantic tests cover exact batch retry, route-independent ChangeId suppression, atomic equivocation rejection, per-author relay ACK, bounded/resumable relay reads, scope/replica isolation, receive cursor/digest-chain gaps and conflicting receipt reuse, preservation of receipts/ChangeId dedup facts after relay compaction, immutable/bounded outbound pages, lease-generation fencing, safe outbound ACK bounds, atomic outbound freeze identity assignment, namespace/author stream isolation, cursor high-water validation, sequence/ChangeId preservation after Outbox compaction, stable metadata snapshots across concurrent logical commits and compaction, snapshot disposal, pending-apply recovery after provider commit, provider/apply-token equivocation rejection, and pending-intent payload ownership/idempotent begin.

GitHub Actions run `42360397431` passed the Release solution build, all 637 tests, public samples, coverage threshold and fuzz/lab hardening at commit `504a09092302f137d222421f6929533b95188318`. Line coverage was 72.5%; this combined run includes the Metadata Store and Data Provider conformance suites. An earlier solution layout briefly launched the helper class library as a standalone test target; it was removed from `BinSync.slnx` and remains a project reference of `BinSync.Tests`, where the inherited suite is discovered and runs in the normal test host. The corrected build/test/fuzz/lab run is green.

**Now covered semantically by the volatile reference model:** frozen local Outbox operations, consumer lease fencing, validated outbound safe-prefix ACK, stable metadata snapshot view under logical append/ACK/compaction, pending-apply token/receipt behavior and idempotent provider retry. These tests do not prove physical snapshot-generation pinning, process restart, torn-write recovery or disk durability. **Still open:** deterministic crash/reopen tests against a durable Metadata Store; PendingLocal/source-page staging and LWW coalescing conformance; durable Data Provider feed/reopen tests; end-to-end proof that provider feed ACK follows durable staging; shared-transaction and separate-store integration against real providers; generic FLD Storage-only semantics and end-to-end FLD adapters. The volatile model makes no restart-durability claim.


## 11. Durable Storage adapter status (supersedes earlier volatile-only status)

As of BIN-SYNC branch `feature/fld-external-storage`, commit `a714dca0014beac19063753ed13a8da0f6cd7fbb`, the current semantic suite is exercised by the real `StorageMetadataStore` implementation backed by `Fld1.Storage`. The FLD Storage Integration workflow includes the inherited Metadata Store conformance suite and additional restart/compaction coverage. The volatile adapter remains useful as a fast reference model; it is no longer the only implementation tested.

The durable adapter currently persists:
- incoming batch receipts and per-sender/per-author receive cursors/digest-chain checkpoints;
- one global `(NamespaceId, ChangeId) → fingerprint + origin` ledger, independent of delivery route;
- relay and local outbound payloads with independent author sequence domains;
- outbound lease generations, ACK/high-water marks;
- PendingApply intents and compact completed-apply receipts;
- consistent logical snapshots across adapter operations.

**Important remaining authority gap:** Node/Transport still owns and uses its own Outbox, `PeerStates`, pushed/ACKed/offset watermarks, `LastStreamDigest` and batch replay identity mappings. The Storage adapter's outbound stream is currently populated from Node's Outbox by `StorageSyncCoordinator`; it is a durable mirror, not yet the transport's source of truth. The next migration gate is to add peer/author progress and immutable per-peer outbound batch reservations to the Metadata Store contract, then move the transport read/ACK path onto those durable records.

The current Storage integration test run at this commit is being used to validate legacy ChangeId-origin-marker migration into the compact ledger representation; do not mark that gate complete until its CI result is green.
