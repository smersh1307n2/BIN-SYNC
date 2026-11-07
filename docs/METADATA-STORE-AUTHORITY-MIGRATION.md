# Metadata Store authority migration

**Status:** implementation plan updated against BIN-SYNC `feature/fld-external-storage` at `d264d4a9c58998a6ff3baea9458b5f9fb61ab202` (2026-11-07). Durable Storage adapters and compact metadata rows are implemented; full Node/Transport authority migration remains open. This document does not change the wire protocol.

## 1. Current boundary (verified in code)

The Storage integration is durable and usable, including over real TCP, but `IMetadataStore` is not yet the authoritative protocol-state source.

The current path is:

1. `StorageDataProvider` exposes a durable local change feed.
2. `StorageSyncCoordinator.PublishLocalChanges` imports feed entries into `Node.WriteField/WriteTombField/WriteRecordTombstone`.
3. `Node` allocates ChangeId/author sequence and owns the actual Outbox entries.
4. `FreezeLocalOutboxToMetadata` copies those entries to `IMetadataStore.FreezeOutbound`; it validates sequence continuity but does not make the metadata copy the source used by transport.
5. `SyncSession.PushBatchAsync` calls Node transport batch creation. The transport reads Node's Outbox and Node's peer progress/replay state.
6. On receive, `SyncSession.ReceiveBatchAsync` first applies the batch to Node, then invokes `beforeAck`. The coordinator checkpoints Node, applies the immutable batch to the provider, finalizes Metadata Store PendingApply and returns; only then does SyncSession send ACK.

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

The current `IMetadataStore` already exposes durable incoming receipts, PendingApply, outbound/relay pages, local consumer leases, receive cursors and high-watermarks. Those methods are necessary but do not replace Node's peer-specific delivery state.

In particular:

- `AcquireOutboundLease` / `AcknowledgeOutbound` currently model a consumer lease over a local author stream. They do not identify a remote peer and cannot by themselves represent independent ACK progress for multiple peers.
- `MetadataOutboundItem.Sequence` is the local durable sequence. BIN-SYNC wire cursors are peer/author-stream-relative and may include a stream offset. The mapping must be durable per peer and author.
- Node caches the BatchId and previous digest for an outstanding `(author, cursorStart, cursorEnd)` range. A lost ACK must reproduce the same batch identity and digest, not mint a new batch.
- `MetadataFrozenOperation` currently stores ChangeId, fingerprint and opaque payload. `OutboxEntry` also carries causal dependency metadata; the coordinator's current freeze envelope does not persist that metadata. Before generalizing the authoritative path beyond the current LWW/key-epoch-zero subset, payload parity must be made explicit and tested.
- `Node.ApplyBatchPayloadWithBatch` mutates Node's inbox, digest chain and record cache before the `beforeAck` callback. A storage-backed authoritative receive path must instead make Metadata Store acceptance the durable protocol decision and must not rely on a second Node receipt as the acceptance authority.

Do not solve these gaps by merely adding more mirror calls around existing Node methods. That would preserve two authorities.

## 4. Required semantic records

The public contract should express semantic transitions, not expose FLD or Node implementation dictionaries.

### 4.1 Peer author progress

Key: `(scopeId, localReplica, peer, namespaceId, authorNodeId)`.

Required durable values:

- acknowledged wire cursor;
- greatest wire cursor handed to the peer;
- local-sequence-to-wire-cursor offset;
- initial-sync/snapshot barrier state;
- last accepted outbound digest-chain checkpoint for this peer;
- monotonic version/fencing token for concurrent senders.

The store must distinguish an absent watermark from an explicit zero where protocol rules require that distinction.

### 4.2 Outbound replay reservation

Key: `(scopeId, localReplica, peer, namespaceId, authorNodeId, cursorStart, cursorEnd)`.

Persist enough immutable data to reproduce an in-flight batch exactly:

- stable BatchId;
- PreviousBatchDigest and BatchDigest;
- RecordsRoot and exact cursor range;
- local outbound sequence range;
- immutable operation payloads and any causal metadata required by the selected operation family.

Reservation must be idempotent for the same range and must reject conflicting reuse. The digest-chain checkpoint advances only after the frame has been handed to the transport, matching the existing `AdvanceStream` boundary.

### 4.3 Inbound acceptance

The existing `AcceptIncomingBatch` semantics remain the model: sender/epoch/namespace/author-stream cursors, exact digest-chain continuity, route-independent ChangeId dedup and immutable receipts must be committed atomically. A duplicate through another peer advances that peer's receipt/cursor but must not create a second relay operation.

### 4.4 Safe reclamation

Outbound payload reclamation is based on the minimum durable peer ACK across all interested peers for the author stream, combined with relay retention/provenance requirements. A single local-consumer ACK must never authorize deletion while another peer still needs the payload.

## 5. Migration order

1. **Freeze the semantic contract.** Add conformance cases for per-peer/per-author progress, explicit-zero vs absent state, stream offsets, batch replay reservation, lost ACK, stale sender fencing and multi-peer minimum ACK.
2. **Extend neutral API and codec.** Add the peer-progress and outbound-replay semantic operations to `IMetadataStore`; implement them in `StorageMetadataStore`. Keep the storage engine byte-oriented and BIN-SYNC-independent.
3. **Make outbound reads authoritative.** Add a storage-backed Node/runtime path that reads immutable outbound pages from Metadata Store, performs the same alias/origin translation and batch encoding, and uses durable replay reservations. The old in-memory path remains for volatile mode and compatibility tests.
4. **Move send transitions.** Route pushed cursor/digest and ACK validation into Metadata Store transactions. Compute safe retention from all interested peers; only then compact outbound payloads and replay mappings.
5. **Move receive authority.** Validate and durably accept incoming batches in Metadata Store before treating them as accepted in the runtime. Apply provider state under PendingApply; finalize the durable receipt; send ACK last. Node's in-memory receipt/digest maps become disposable caches.
6. **Recover without a second authority.** Reopen runtime state from Metadata Store and provider snapshots/feeds. Any Node checkpoint retained for performance must be explicitly a cache/checkpoint, never a competing source of protocol truth.
7. **Prove multi-peer behavior.** Run at least three replicas with one offline peer, relay through a second peer, restart at each boundary, lost ACK, duplicate delivery by alternate route, peer-specific compaction, multiple author streams, and concurrent senders using fencing.
8. **Remove transitional mirroring.** Once conformance and recovery pass, remove the coordinator's Node-Outbox-to-Metadata mirror path and old duplicate durable protocol maps from the Storage-backed runtime. Keep volatile mode separately if it remains a supported configuration.

## 6. Acceptance criteria

The migration is complete only when all of the following are true:

- Storage-backed transport obtains outbound payloads and peer progress from Metadata Store, not Node.Outbox/PeerStates.
- Node does not independently commit a second durable inbox/receipt/digest authority.
- Every ACK is preceded by durable metadata acceptance and recoverable provider effects.
- Repeated sends after lost ACK reproduce the exact BatchId, digest and cursor range.
- A peer that is offline does not lose payloads due to another peer's ACK.
- Compaction is safe across all peers and relay routes.
- Process restart reconstructs the same next outbound batch and receive decision.
- Multi-peer TCP integration, crash matrix, conformance and existing volatile-mode suites all pass.
- Metadata write/clone cost remains bounded by changed logical keys rather than total partition size.

## 7. Current verified milestone

At BIN-SYNC `aa5c52e2`:

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
- Unpublished local LWW updates are coalesced within each bounded source-feed page before the Node Outbox is appended. This is not yet cross-page durable PendingLocal staging.
- BIN-SYNC CI at this baseline passes 642/642 tests, 72.1% line coverage, public samples, Fuzz and Lab; Storage Integration passes 49/49, including legacy-format migration and real TCP samples.

The main authority gap is unchanged: transport reads Node.Outbox and Node.PeerStates; the Storage outbound stream is still populated as a durable mirror. The next implementation gate remains the peer/author progress and outbound batch reservation contract, followed by transport read/ACK migration and receive-side authority migration.

**Scope:** this is the BIN-SYNC + generic Storage stage only. FLD Database and its JournalSeq provenance adapter are deferred.
