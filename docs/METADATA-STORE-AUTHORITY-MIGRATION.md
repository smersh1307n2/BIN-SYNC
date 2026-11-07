# Metadata Store authority migration

**Status:** in progress, verified against BIN-SYNC `feature/fld-external-storage` at `3a430d35b5ce` (2026-11-07). The optional `IMetadataPeerStateStore` now persists a namespace peer roster, per-peer/per-author cursors, exact immutable batch reservations and ACK/digest progress. The opt-in transport path reads Outbound/Relay pages from Metadata Store; peer ACKs fence shared-payload compaction, including offline interested peers. Receive now persists the exact PendingApply wire intent before Node mutation and commits provider state plus metadata receipt before ACK; Node still performs the first receive validation and remains a transitional protocol-state cache. Cross-page PendingLocal coalescing is implemented. Peer-epoch retirement, fully authoritative receive validation/recovery, bounded non-peer metadata mutation cost, GC horizons and complete authority cutover remain open. This document does not change the wire protocol.

## 1. Current boundary (verified in code)

The Storage integration is durable and usable, including over real TCP, but `IMetadataStore` is not yet the authoritative protocol-state source.

The current path is:

1. `StorageDataProvider` exposes a durable local change feed.
2. `StorageSyncCoordinator.PublishLocalChanges` imports feed entries into `Node.WriteField/WriteTombField/WriteRecordTombstone`.
3. `Node` allocates ChangeId/author sequence and owns the actual Outbox entries.
4. `FreezeLocalOutboxToMetadata` copies those entries to `IMetadataStore.FreezeOutbound`; it validates sequence continuity but does not make the metadata copy the source used by transport.
5. The legacy `SyncSession.PushBatchAsync` still reads Node's Outbox and peer state. The overload accepting `IMetadataPeerStateStore` restores per-peer progress from Metadata Store, reads local Outbound and Relay pages at the peer's durable cursor, materializes a temporary encoder view from those rows without replacing or mutating the live Node Outbox, resumes any immutable reserved payload, persists a new reservation before send, marks it pushed after transport handoff, and commits the matching ACK/digest durably. Node remains responsible for alias translation and wire encoding; its peer state is only a runtime mirror in this path.
6. On receive, `SyncSession.ReceiveBatchAsync` decodes and validates the packet through Node's side-effect-free preparation step, invokes the coordinator's `beforeApply` callback to persist the exact wire payload as a PendingApply intent, then applies the batch to Node. Before ACK, the coordinator checkpoints Node, applies immutable provider mutations idempotently, and finalizes Metadata Store receipt/dedup/relay state in one metadata transaction. Recovery replays an unfinished PendingApply wire payload into Node and provider. The ordering is durable, but Node still supplies the first receive cursor/digest validation and its inbox remains a transitional duplicate authority.

The TCP array sample proves this sequence across independent Storage databases, close/reopen, a subsequent update and a record tombstone. It does not change the ownership described above.

## 2. State ownership gap

| State | Current authoritative owner | Required owner in Storage-backed mode |
|---|---|---|
| User field values and lifecycle | Provider plus Node's in-memory copy | Data Provider; Node view is reconstructible/cache |
| Local source feed | StorageDataProvider | Data Provider |
| PendingLocal before freeze | Provider feed / coordinator | Provider or Metadata Store, one explicit owner |
| Frozen outbound payload and sequence | Node Outbox; Metadata Store mirror | Metadata Store |
| Global ChangeId fingerprint ledger | Node and Metadata Store copies | Metadata Store |
| Per-peer, per-author ACK/pushed/offset | Node `PeerStates` | Metadata Store |
| Outbound BatchId/digest replay mapping | Node `PeerStates.Ids` and `LastStreamDigest` | Metadata Store |
| Inbound batch receipts and receive digest chain | Node `InboxAuthors`/`LastBatchDigest` plus Metadata Store receipts | Metadata Store |
| Relay payload and relay progress | Node and Metadata Store | Metadata Store |
| Pending provider APPLY | Metadata Store intent plus provider receipt | Metadata Store + provider receipt |
| Alias map for a live session | Node/session memory | Runtime cache; not durable |
| Durable author/epoch knowledge | Node `DurableAuthors`, `KnownEpochs`, `PeerSessionEpochs` | Explicit durable control metadata or a documented reconstructible source |
| Diagnostic queues/events | Node memory | Runtime only |

A critical implementation detail: Node's per-peer progress is not a single ACK counter. It includes per-author ACKed and pushed watermarks, stream offsets, replay mappings for cursor ranges, initial-sync state and received-stream cursors. The sender digest chain is also peer-specific. These values are part of correctness, not optional diagnostics.

## 3. Existing contract is not yet sufficient

The current `IMetadataStore` exposes durable incoming receipts, PendingApply, outbound/relay pages, local consumer leases, receive cursors and high-watermarks. The optional `IMetadataPeerStateStore` adds a durable interested-peer roster, peer/epoch/namespace/author progress and immutable outbound reservations without forcing volatile or third-party `IMetadataStore` implementations to change. The durable outbound slice is now functional, including minimum-ACK compaction fencing. This remains a staged capability rather than the complete authority boundary.

In particular:

- `AcquireOutboundLease` / `AcknowledgeOutbound` currently model a consumer lease over a local author stream. They do not identify a remote peer and cannot by themselves represent independent ACK progress for multiple peers.
- `MetadataOutboundItem.Sequence` is the local durable sequence. BIN-SYNC wire cursors are peer/author-stream-relative and may include a stream offset. The mapping must be durable per peer and author.
- Node caches the BatchId and previous digest for an outstanding `(author, cursorStart, cursorEnd)` range. A lost ACK must reproduce the same batch identity and digest, not mint a new batch.
- `MetadataFrozenOperation` currently stores ChangeId, fingerprint and opaque payload. `OutboxEntry` also carries causal dependency metadata; the coordinator's current freeze envelope does not persist that metadata. Before generalizing the authoritative path beyond the current LWW/key-epoch-zero subset, payload parity must be made explicit and tested.
- `Node.ApplyBatchPayloadWithBatch` mutates Node's inbox, digest chain and record cache before the `beforeAck` callback. A storage-backed authoritative receive path must instead make Metadata Store acceptance the durable protocol decision and must not rely on a second Node receipt as the acceptance authority.

Do not solve these gaps by merely adding more mirror calls around existing Node methods. That would preserve two authorities.

## 4. Required semantic records

The public contract should express semantic transitions, not expose FLD or Node implementation dictionaries.

### 4.1 Peer author progress — implemented first slice

Key: `(scopeId, localReplica, peer, namespaceId, authorNodeId)`.

Required durable values:

- acknowledged wire cursor;
- greatest wire cursor handed to the peer;
- local-sequence-to-wire-cursor offset;
- initial-sync/snapshot barrier state;
- last accepted outbound digest-chain checkpoint for this peer;
- monotonic version/fencing token for concurrent senders.

The store must distinguish an absent watermark from an explicit zero where protocol rules require that distinction.

### 4.2 Outbound replay reservation — implemented first slice

Key: `(scopeId, localReplica, peer, namespaceId, authorNodeId, cursorStart, cursorEnd)`.

Persist enough immutable data to reproduce an in-flight batch exactly:

- stable BatchId;
- PreviousBatchDigest and BatchDigest;
- RecordsRoot and exact cursor range;
- local outbound sequence range;
- immutable operation payloads and any causal metadata required by the selected operation family.

Reservation must be idempotent for the same range and must reject conflicting reuse. The digest-chain checkpoint advances only after the frame has been handed to the transport, matching the existing `AdvanceStream` boundary.

Current implementation detail: `StorageMetadataStore` persists one compact peer-stream row (keyed by peer, peer epoch, namespace and author) containing ACK/pushed cursor, snapshot offset, issuer epoch, digest checkpoint and at most one exact encoded pending batch payload. The reservation is committed before send; `MarkOutboundBatchPushed` follows a successful send; ACK validation and reservation release are one metadata transaction. The first registration/reservation can bootstrap cursor, previous digest and snapshot offset from a proven baseline or the existing Node checkpoint. The new `SyncSession.PushBatchAsync(..., IMetadataPeerStateStore, scopeId)` path restores the Node mirror from durable progress, reads the correct Outbound/Relay page, and builds the first batch from those persisted rows. `ReadOutboundForPeer`/`ReadRelayForPeer` enforce page continuity and fail closed on missing payload below the high-watermark.

### 4.3 Inbound acceptance

The existing `AcceptIncomingBatch` semantics remain the model: sender/epoch/namespace/author-stream cursors, exact digest-chain continuity, route-independent ChangeId dedup and immutable receipts must be committed atomically. A duplicate through another peer advances that peer's receipt/cursor but must not create a second relay operation.

### 4.4 Safe reclamation

Outbound payload reclamation is based on the minimum durable peer ACK across all interested peers for the author stream, combined with relay retention/provenance requirements. A single local-consumer ACK must never authorize deletion while another peer still needs the payload.

## 5. Migration order

1. **Freeze the semantic contract.** Per-peer/per-author progress, namespace interested-peer registration and immutable reservation are implemented and covered by Storage integration tests, including restart, offset bootstrap, epoch digest-chain rollover, peer isolation, offline peer retention, ACK fencing and a three-replica relay fan-out. Still add lost-ACK replay across a real process restart, explicit stale-sender fencing and safe retirement of obsolete peer epochs.
2. **Extend neutral API and codec.** The optional `IMetadataPeerStateStore` capability and Storage row codec are implemented. Keep the base `IMetadataStore` source-compatible; later decide whether the optional capability should become mandatory after every adapter implements it.
3. **Make outbound reads authoritative.** Implemented for the opt-in Storage-backed send overload: first-build payloads come from immutable `MetadataOutboundPage` / `MetadataRelayPage` rows; Node is used only as a temporary encoding/alias context and the live Outbox is not swapped. The legacy Node-backed overload remains for volatile/compatibility mode. A complete mode switch and removal of duplicate Node protocol state remain open.
4. **Move send transitions and retention.** Implemented first slice: the durable path persists pushed state, exact reservations and ACK/digest transitions in Metadata Store. A durable namespace peer roster enrolls existing and future outbound/relay author streams. The minimum ACK across all interested peers fences Outbound and Relay compaction; a legacy local consumer lease/ACK is additionally honored when present. The three-replica integration test proves one shared relay payload remains while an offline downstream peer has not ACKed, then becomes compactable after both downstream ACKs. Explicit peer-epoch retirement, fencing of concurrent external senders and transactional replay-cache cleanup remain open; stale registrations deliberately retain payloads.
5. **Move receive authority.** Partially implemented: the exact incoming wire payload and semantic operations are durably staged as PendingApply before Node mutation; after Node checkpoint and idempotent provider apply, Metadata Store atomically commits receipt/dedup/relay progress and removes the intent; ACK is sent last. Recovery replays the stored wire payload. Still open: move initial sequence/digest-chain validation and duplicate decision from Node into Metadata Store, then reduce Node inbox/digest maps to disposable caches.
6. **Recover without a second authority.** Reopen runtime state from Metadata Store and provider snapshots/feeds. Any Node checkpoint retained for performance must be explicitly a cache/checkpoint, never a competing source of protocol truth.
7. **Prove multi-peer behavior.** A real three-replica relay test now covers A→B→C, with D registered but offline: C's ACK cannot compact the shared relay payload; after D connects and ACKs, the minimum peer cursor advances and compaction succeeds. Still add process restart at each send/receive boundary, lost-ACK replay, alternate-route duplicate delivery, multiple author streams under relay, and concurrent sender/fencing tests.
8. **Remove transitional mirroring.** Once conformance and recovery pass, remove the coordinator's Node-Outbox-to-Metadata mirror path and old duplicate durable protocol maps from the Storage-backed runtime. Keep volatile mode separately if it remains a supported configuration.

## 6. Acceptance criteria

The migration is complete only when all of the following are true:

- Storage-backed transport obtains peer progress, Outbound/Relay payload pages and replay reservations from Metadata Store; Node only performs protocol alias/encoding work and mirrors progress. The legacy path remains explicitly separate.
- Node does not independently commit a second durable inbox/receipt/digest authority.
- Every ACK is preceded by durable metadata acceptance and recoverable provider effects.
- Repeated sends after lost ACK reproduce the exact BatchId, digest and cursor range.
- A peer that is offline does not lose payloads due to another peer's ACK.
- Compaction is safe across all peers and relay routes.
- Process restart reconstructs the same next outbound batch and receive decision.
- Multi-peer TCP integration, crash matrix, conformance and existing volatile-mode suites all pass.
- Metadata write/clone cost remains bounded by changed logical keys rather than total partition size.

## 7. Current verified milestone

Historical adapter milestone (earlier baseline: BIN-SYNC `aa5c52e2`):

- BIN-SYNC CI and fuzz/lab workflows pass.
- FLD Storage Integration passes 47/47 tests.
- Existing in-process StorageArraySync passes.
- New StorageArrayTcpSync passes bidirectional multi-field array sync, database reopen, subsequent update and record tombstone propagation over real TCP.

This closes the real-TCP adapter/coordinator validation gap only. Sections 2–6 remain the next architectural work; do not mark full Storage authority migration complete.


## 8. Verified implementation update (2026-11-07)

The current Storage-backed adapter baseline has advanced beyond the initial real-TCP sample:

- `StorageMetadataStore` and `StorageDataProvider` pass 49/49 durable Storage Integration tests.
- The global ChangeId ledger now stores a 16-byte fingerprint plus a one-byte local-origin flag. The previous separate local-outbound marker key is read for compatibility and migrated on the next transaction.
- Relay and local Outbound ACK/high-watermark pairs are each encoded in one 16-byte per-author row. Legacy separate 8-byte rows are still decoded and are rewritten in the compact form on the next metadata transaction.
- Unpublished local LWW updates are coalesced across the complete currently-pending provider feed, including page boundaries, before the Node Outbox is appended. The provider feed itself is the durable PendingLocal journal; it is ACKed only after Node persistence and Metadata Store freeze succeed, so no duplicate per-edit metadata table is introduced.
- BIN-SYNC CI at code baseline `d264d4a9` passes 642/642 tests, 72.1% line coverage, public samples, Fuzz and Lab; Storage Integration passes 49/49, including legacy-format migration and real TCP samples.
- Documentation-only commits through `e0333aa3` were revalidated by GitHub Actions: `BinSync.Net` run `42379342614` and `FLD Storage Integration` run `42379342571` both completed successfully.

The main remaining authority gap is receive-side validation: Node still validates sequence/digest continuity and mutates its inbox before Metadata Store finalizes the receipt. The opt-in durable push overload reads per-peer Outbound/Relay pages, restores per-peer progress, reserves exact payloads before send and commits ACKs durably; the live Node Outbox is not swapped during encoding. The durable namespace peer roster and minimum-ACK horizon retain one shared payload until every interested downstream peer acknowledges it. Cross-page local feed changes are coalesced before Node writes and only final LWW winners are frozen; the provider feed remains unacknowledged until Node persistence and metadata freeze succeed. Remaining gates are explicit peer-epoch retirement/fencing, fully authoritative receive validation, restart/lost-ACK and alternate-route fault matrix, bounded metadata mutation cost for non-peer operations, receipt/dedup GC horizons and safe tombstone retention.

**Scope:** this is the BIN-SYNC + generic Storage stage only. FLD Database and its JournalSeq provenance adapter are deferred.
