# Metadata Store authority migration

**Status:** in progress, verified against BIN-SYNC `feature/fld-external-storage` through the current authority-migration work. The optional `IMetadataPeerStateStore` persists the namespace peer roster, per-peer/per-author cursors, immutable batch reservations, ACK/digest progress, retired epoch watermarks and explicit initial snapshot/feed-baseline completion. Durable receive staging validates cursor/digest continuity and ChangeId equivocation before Node mutation, then records PendingApply and finalizes provider/metadata receipts before ACK. The receive/transport path is still transitional: Node retains a second inbox, receive cursor/digest cache, local Outbox allocation and runtime mirror; the Metadata Store is not yet the sole protocol authority. The latest hardening changes make PendingApply finalization a delta-only metadata transaction (no whole-partition clone/re-encode) and index pending ChangeIds once per validation pass. This improves mutation cost but does not complete Node/Transport cutover. Remaining gates: authoritative receive/transport state migration, provider rehydration/restart model, deterministic crash matrix, measured metadata mutation/compaction costs, receipt/dedup GC horizons, and normative §§4–18 audit. The durable Storage Inbox receipt key now follows the protocol identity `(senderNodeId, senderEpoch, namespaceId, batchId)` and excludes the separately tracked resolved author stream. Legacy author-suffixed physical keys are accepted during decode and normalized to the protocol key on subsequent state rewrite. This closes a duplicate-key divergence. Durable receipt values now also retain and return the resolved `durableAuthor` (including completed PendingApply retries); the complete protocol `headerFingerprint` is still owned by Node's transitional inbox path and remains an authority-cutover gap.

This document does not change the wire protocol.
## 1. Current boundary (verified in code)

The Storage integration is durable and usable, including over real TCP, but `IMetadataStore` is not yet the authoritative protocol-state source.

The current path is:

1. `StorageDataProvider` exposes a durable local change feed.
2. `StorageSyncCoordinator.PublishLocalChanges` imports feed entries into `Node.WriteField/WriteTombField/WriteRecordTombstone`.
3. `Node` allocates ChangeId/author sequence and owns the actual Outbox entries.
4. `FreezeLocalOutboxToMetadata` copies those entries to `IMetadataStore.FreezeOutbound`; it validates sequence continuity. The Node Outbox remains the local mutation/sequence staging source, but the durable transport overload now reads the immutable frozen payload from Metadata Store.
5. The legacy `SyncSession.PushBatchAsync` still reads Node's Outbox and peer state. The overload accepting `IMetadataPeerStateStore` restores per-peer progress from Metadata Store, reads local Outbound and Relay pages at the peer's durable cursor, materializes a temporary encoder view from those rows without replacing or mutating the live Node Outbox, resumes any immutable reserved payload, persists a new reservation before send, marks it pushed after transport handoff, and commits the matching ACK/digest durably. Node remains responsible for alias translation and wire encoding; durable peer progress is passed directly into the batch encoder as an explicit input. The durable overload does not restore or write ACK/digest cursors into Node peer dictionaries.
6. On receive, `SyncSession.ReceiveBatchAsync` decodes and validates the packet through Node's side-effect-free preparation step, invokes the coordinator's `beforeApply` callback to persist the exact wire payload as a PendingApply intent, then applies the batch to Node. Before ACK, the coordinator checkpoints Node, applies immutable provider mutations idempotently, and finalizes Metadata Store receipt/dedup/relay state in one metadata transaction. Recovery replays an unfinished PendingApply wire payload into Node and provider. The ordering is durable, but Node still supplies the first receive cursor/digest validation and its inbox remains a transitional duplicate authority.

The TCP array sample proves this sequence across independent Storage databases, close/reopen, a subsequent update and a record tombstone. It does not change the ownership described above.

## 2.1 Initial snapshot/feed baseline

A new peer incarnation must not begin incremental delivery merely because the first retained author sequence happens to be one. The Storage path now persists a peer/epoch/namespace baseline-completion marker. `ReadOutboundForPeer`, `ReadRelayForPeer` and new batch reservations fail with `SNAPSHOT_REQUIRED` until that marker exists.

`StorageSyncCoordinator.AutoReconcileWithMetadataAsync` is the durable entry point:

1. Recover pending provider applies and publish the local provider feed.
2. Capture local outbound author high-watermarks before anti-entropy. These become outbound `StreamOffset` values; relay author streams always use offset zero.
3. Run both anti-entropy rounds. Every received snapshot is applied to Node, checkpointed, then applied idempotently to the Data Provider before the peer can send the reconciliation terminator.
4. Only after both rounds report completion, persist the peer baseline marker and per-author offsets.

The outbound safe-compaction coordinate is local author sequence: `AckedCursor + StreamOffset` after the baseline marker is complete. A snapshot is therefore not an ACK by itself; only the completed snapshot/feed barrier authorizes the captured outbound prefix. Relay history is never skipped by this baseline and still requires per-peer ACK. If a relay page starts after sequence one, the new peer remains fail-closed with `SNAPSHOT_REQUIRED`.
## 2. State ownership gap

| State | Current authoritative owner | Required owner in Storage-backed mode |
|---|---|---|
| User field values and lifecycle | Provider plus Node's in-memory copy | Data Provider; Node view is reconstructible/cache |
| Local source feed | StorageDataProvider | Data Provider |
| PendingLocal before freeze | Provider feed / coordinator | Provider or Metadata Store, one explicit owner |
| Frozen outbound payload and sequence | Node allocates/stages locally; Metadata Store owns the immutable transport/replay copy after freeze | Metadata Store |
| Global ChangeId fingerprint ledger | Node and Metadata Store copies | Metadata Store |
| Per-peer, per-author ACK/pushed/offset | Metadata Store in durable transport mode; Node is a runtime mirror | Metadata Store |
| Outbound BatchId/digest replay mapping | Metadata Store reservation; Node retains a runtime mirror | Metadata Store |
| Inbound batch receipts and receive digest chain | Metadata Store receipt/chain is committed after provider apply; Node still performs duplicate validation as a transitional mirror | Metadata Store |
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
- The legacy Node-backed path caches the BatchId and previous digest for an outstanding `(author, cursorStart, cursorEnd)` range. In Storage-backed mode, the immutable Metadata Store reservation is the replay identity authority. A lost ACK must reproduce the same batch identity and digest, not mint a new batch.
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

The store must distinguish an absent watermark from an explicit zero where protocol rules require that distinction. It must also persist a peer/epoch/namespace snapshot-baseline completion marker; a zero cursor alone must never authorize incremental DELTA_BATCH for a new peer.

### 4.2 Outbound replay reservation — implemented first slice

Key: `(scopeId, localReplica, peer, namespaceId, authorNodeId, cursorStart, cursorEnd)`.

Persist enough immutable data to reproduce an in-flight batch exactly:

- stable BatchId;
- PreviousBatchDigest and BatchDigest;
- RecordsRoot and exact cursor range;
- local outbound sequence range;
- immutable operation payloads and any causal metadata required by the selected operation family.

Reservation must be idempotent for the same range and must reject conflicting reuse. The digest-chain checkpoint advances only after the frame has been handed to the transport, matching the existing `AdvanceStream` boundary.

Current implementation detail: `StorageMetadataStore` persists one compact peer-stream row (keyed by peer, peer epoch, namespace and author) containing ACK/pushed cursor, snapshot offset, issuer epoch, digest checkpoint and at most one exact encoded pending batch payload. The reservation is committed before send; `MarkOutboundBatchPushed` follows a successful send; ACK validation and reservation release are one metadata transaction. The first registration can bootstrap cursor, previous digest and snapshot offset from a proven baseline or an existing legacy Node checkpoint. The new `SyncSession.PushBatchAsync(..., IMetadataPeerStateStore, scopeId)` path passes the saved `MetadataPeerStreamState` directly to the batch encoder, reads the correct Outbound/Relay page, and builds the first batch from those persisted rows. It does not hydrate Node peer cursors/digests; the durable reservation owns replay identity and the exact encoded payload. The path sends the immutable reservation payload directly rather than rematerializing a second Node outbound DTO. `ReadOutboundForPeer`/`ReadRelayForPeer` enforce page continuity and fail closed on missing payload below the high-watermark.

### 4.3 Inbound acceptance

The existing `AcceptIncomingBatch` semantics remain the model: sender/epoch/namespace/author-stream cursors, exact digest-chain continuity, route-independent ChangeId dedup and immutable receipts must be committed atomically. A duplicate through another peer advances that peer's receipt/cursor but must not create a second relay operation.

### 4.4 Safe reclamation

Outbound payload reclamation is based on the minimum durable peer ACK across all interested peers for the author stream, combined with relay retention/provenance requirements. A single local-consumer ACK must never authorize deletion while another peer still needs the payload.

## 5. Migration order

1. **Freeze the semantic contract.** Per-peer/per-author progress, namespace interested-peer registration and immutable reservation are implemented and covered by Storage integration tests, including restart, offset bootstrap, epoch digest-chain rollover, peer isolation, offline peer retention, ACK fencing and a three-replica relay fan-out. Verified since the earlier baseline: exact lost-ACK reservation replay across Metadata Store reopen and sender Node recreation; durable retired-epoch watermark; stale epoch read/write fencing; old peer-stream/roster cleanup after replacement epoch registration. The remaining gap is to bind the initial snapshot/feed barrier to durable per-peer progress and require it before any new-peer incremental read.
2. **Extend neutral API and codec.** The optional `IMetadataPeerStateStore` capability and Storage row codec are implemented. Keep the base `IMetadataStore` source-compatible; later decide whether the optional capability should become mandatory after every adapter implements it.
3. **Make outbound reads authoritative.** Implemented for the opt-in Storage-backed send overload: first-build payloads come from immutable `MetadataOutboundPage` / `MetadataRelayPage` rows; Node is used only as a temporary encoding/alias context and the live Outbox is not swapped. The legacy Node-backed overload remains for volatile/compatibility mode. Durable ACK mirroring is now explicitly non-authoritative and does not require a retained Node Outbox range. A complete mode switch and removal of duplicate Node protocol state remain open.
4. **Move send transitions and retention.** Implemented first slice: the durable path persists pushed state, exact reservations and ACK/digest transitions in Metadata Store; committed ACK/digest progress is no longer written back into Node. A durable namespace peer roster enrolls existing and future outbound/relay author streams. The minimum ACK across all interested peers fences Outbound and Relay compaction; a legacy local consumer lease/ACK is additionally honored when present. The three-replica integration test proves one shared relay payload remains while an offline downstream peer has not ACKed, then becomes compactable after both downstream ACKs. Peer-epoch retirement now registers the replacement epoch first, durably advances the retired watermark, fences old sessions and removes old peer progress/required-stream rows so safe compaction can proceed. Fencing concurrent external senders that race within the same active epoch and broader transactional replay-cache cleanup remain open.
5. **Move receive authority.** Partially implemented: the exact incoming wire payload and semantic operations are durably staged as PendingApply before Node mutation; Metadata Store now validates per-author cursor continuity, sender-session digest-chain continuity, and global ChangeId fingerprint conflicts before that mutation. A pending intent serializes a sender/epoch/namespace digest chain until provider apply and receipt finalization complete. After Node checkpoint and idempotent provider apply, Metadata Store atomically commits receipt/dedup/relay progress and removes the intent; ACK is sent last. Recovery replays the stored wire payload. Still open: make Metadata Store the sole acceptance/duplicate authority, remove the duplicate Node inbox/digest decisions, and prove the complete crash/retry matrix.
6. **Recover without a second authority.** Reopen runtime state from Metadata Store and provider snapshots/feeds. Any Node checkpoint retained for performance must be explicitly a cache/checkpoint, never a competing source of protocol truth.
7. **Prove multi-peer behavior.** A real three-replica relay test now covers A→B→C, with D registered but offline: C's ACK cannot compact the shared relay payload; after D connects and ACKs, the minimum peer cursor advances and compaction succeeds. Lost-ACK replay across Metadata Store reopen and sender recreation is now covered. Still add process restart at every receive boundary, alternate-route duplicate delivery with equivocation, multiple author streams under relay, and concurrent same-peer sender/fencing tests.
8. **Remove transitional mirroring.** Once conformance and recovery pass, remove the coordinator's Node-Outbox-to-Metadata mirror path and old duplicate durable protocol maps from the Storage-backed runtime. Keep volatile mode separately if it remains a supported configuration.

## 6. Acceptance criteria

The migration is complete only when all of the following are true:

- Storage-backed transport obtains peer progress, Outbound/Relay payload pages and replay reservations from Metadata Store; Node performs protocol alias/encoding work using explicit durable progress inputs and does not mirror durable ACK/digest state. The legacy path remains explicitly separate.
- Node does not independently commit a second durable inbox/receipt/digest authority.
- Every ACK is preceded by durable metadata acceptance and recoverable provider effects.
- Repeated sends after lost ACK reproduce the exact BatchId, digest and cursor range.
- A peer that is offline does not lose payloads due to another peer's ACK.
- Compaction is safe across all peers and relay routes.
- Process restart reconstructs the same next outbound batch and receive decision (sender-side lost-ACK replay is covered; full receive-boundary matrix remains open).
- Multi-peer TCP integration, crash matrix, conformance and existing volatile-mode suites all pass.
- Metadata write/clone cost remains bounded by changed logical keys rather than total partition size; peer progress and PendingApply insertion now use targeted row writes, while `FinalizePendingApply` and general partition mutations still require optimization.

## 7. Current verified milestone

Historical adapter milestone (earlier baseline: BIN-SYNC `aa5c52e2`):

- BIN-SYNC CI and fuzz/lab workflows pass.
- FLD Storage Integration passes 47/47 tests.
- Existing in-process StorageArraySync passes.
- New StorageArrayTcpSync passes bidirectional multi-field array sync, database reopen, subsequent update and record tombstone propagation over real TCP.

This closes the real-TCP adapter/coordinator validation gap only. Sections 2–6 remain the next architectural work; do not mark full Storage authority migration complete.


## 8. Verified implementation update (2026-11-07)

The current Storage-backed adapter baseline has advanced beyond the initial real-TCP sample:

- `StorageMetadataStore` and `StorageDataProvider` pass 73/73 durable Storage Integration tests at the latest completed Storage CI run.
- The global ChangeId ledger now stores a 16-byte fingerprint plus a one-byte local-origin flag. The previous separate local-outbound marker key is read for compatibility and migrated on the next transaction.
- Relay and local Outbound ACK/high-watermark pairs are each encoded in one 16-byte per-author row. Legacy separate 8-byte rows are still decoded and are rewritten in the compact form on the next metadata transaction.
- Unpublished local LWW updates are coalesced across the complete currently-pending provider feed, including page boundaries, before the Node Outbox is appended. The provider feed itself is the durable PendingLocal journal; it is ACKed only after Node persistence and Metadata Store freeze succeed, so no duplicate per-edit metadata table is introduced.
- BIN-SYNC core CI at `6569dc44` passes 642/642 tests, 71.0% line coverage, public samples, Fuzz and Lab (31/31); Storage Integration passes 73/73 at `568f8e3a` and subsequent targeted PendingApply row-write changes are being revalidated.
- Documentation-only commits through `e0333aa3` were revalidated by GitHub Actions: `BinSync.Net` run `42379342614` and `FLD Storage Integration` run `42379342571` both completed successfully.

- Pending receive staging now indexes already-pending ChangeIds once per validation pass, using the normative `(namespaceId, ChangeId)` identity. This removes the prior nested pending-batch scan and prevents equal ChangeId bytes in different namespaces from being treated as equivocation.

The main remaining authority gaps are the durable initial snapshot/feed barrier for new peers and final receive authority cutover: Metadata Store now validates cursor/digest/ChangeId before Node mutation, but Node still repeats the validation and maintains an inbox/digest mirror. The opt-in durable push overload reads per-peer Outbound/Relay pages, restores per-peer progress, reserves exact payloads before send and commits ACKs durably; the live Node Outbox is not swapped during encoding. The durable namespace peer roster and minimum-ACK horizon retain one shared payload until every interested downstream peer acknowledges it. Cross-page local feed changes are coalesced before Node writes and only final LWW winners are frozen; the provider feed remains unacknowledged until Node persistence and metadata freeze succeed. Remaining gates are explicit peer-epoch retirement/fencing, fully authoritative receive validation, restart/lost-ACK and alternate-route fault matrix, bounded metadata mutation cost for non-peer operations, receipt/dedup GC horizons and safe tombstone retention.

**Scope:** this is the BIN-SYNC + generic Storage stage only. FLD Database and its JournalSeq provenance adapter are deferred.


## 8. Delta finalization and mutation-cost boundary

`StorageMetadataStore.FinalizePendingApply` now commits only the records affected by one incoming batch. The transaction deletes the raw PendingApply intent, writes the compact completed-token receipt, and—when the incoming receipt is new—writes the inbox receipt, author receive cursor, sender digest checkpoint, new ChangeId ledger rows, relay payload rows/high-watermarks and newly required peer-stream rows. In-memory indexes are published only after the Storage transaction commits.

The previous implementation cloned every dictionary in the partition and rebuilt `StateEntries` for a single finalize. The new path is proportional to the incoming operation count plus the affected peer-stream registrations; it does not enumerate unrelated historical receipts, ChangeIds, relay entries or peer state. Pending ChangeId validation is separately O(pending operations + incoming operations), rather than rescanning all pending batches for every incoming operation.

This is a structural complexity improvement, not a benchmark claim. Release evidence still requires measurements at increasing partition sizes (e.g. 10k / 100k / 1m ledger rows), reporting bytes written, allocations, p50/p95 latency and reopen time. The generic `PersistPartition` path remains in use for other state transitions and still performs whole-partition materialization/diffing; it is a separate optimization gate. Relay/outbound ACK watermark updates and per-stream payload compaction now also use direct row-level write/delete batches instead of cloning and diffing the whole partition. These paths retain the same fail-closed safe-prefix checks and update the in-memory projection only after the Storage transaction commits. `AcquireOutboundLease` now updates only its lease row for current-format partitions; when legacy marker, ledger or watermark encodings are detected, it deliberately falls back to one full rewrite so migration and lease generation remain atomic.

## 9. Deterministic receive crash matrix

The separate-provider receive sequence is:

1. **Before PendingApply commit:** no metadata acceptance is durable; sender must not receive ACK.
2. **After PendingApply commit, before provider apply:** recovery discovers the intent and retries the exact token/payload; no ACK.
3. **During provider apply before its atomic receipt:** provider must expose either no effect or a committed effect with the token; metadata must not guess.
4. **After provider effect + provider token receipt, before metadata finalize:** recovery queries/retries the same token and finalizes metadata exactly once logically; no ACK until finalize.
5. **After metadata finalize, before transport ACK:** replay resolves to the compact inbox/completed receipt; provider effect and relay append are not repeated.
6. **After ACK:** exact replay remains a duplicate; conflicting receipt-key or ChangeId reuse is rejected.
7. **During subsequent compaction/reopen:** inbox receipt, ChangeId identity, receive cursor, digest chain and relay retention must remain mutually consistent.

Current durable tests cover PendingApply persistence/replay, provider-token idempotency, finalize/reopen, completed-token duplicate behavior, cursor/digest rejection, ChangeId equivocation and relay page persistence. The new delta-finalize regression verifies that pre-existing relay state survives a second batch finalization and reopen. The remaining gap is deterministic fault injection at every provider and physical-Storage commit boundary, including torn commit/recovery, and end-to-end assertions that ACK is impossible at each pre-finalize cut. A passing ordinary integration test is not a substitute for those crash cuts.

## 10. Retention and garbage-collection horizons

No time-based TTL, HLC age, last-seen timestamp or local Outbox ACK is sufficient proof to delete protocol identity. Retention domains remain independent:

| State | Earliest safe deletion condition | Current policy |
|---|---|---|
| PendingApply payload | Provider effect is durably acknowledged by token and metadata finalization commits | Delete in the same transaction as completed receipt |
| Completed apply token | Provider and metadata agree on a durable dedup horizon, and the token can never be retried/reused within that horizon | Retain; no GC API until provider token-retention contract exists |
| Inbox batch receipt | Sender epoch is fenced/retired and the protocol retry/replay horizon proves no valid duplicate can arrive; replacement state retains the exact duplicate/equivocation decision | Retain; no TTL |
| ChangeId fingerprint ledger | Every route/peer capable of replay is fenced beyond a proven dedup horizon, and a durable compact checkpoint preserves equivocation detection | Retain; no GC |
| Relay payload | All interested downstream peers have a completed baseline and ACK the contiguous per-author prefix | Existing per-author ACK/compaction rules |
| Local frozen Outbox payload | Every interested peer ACKs the contiguous author prefix, including a completed snapshot/feed offset proof where applicable | Existing minimum-peer-ACK compaction rules |
| Peer stream reservations | Matching ACK commits, or the peer incarnation is explicitly retired with fencing proof | One immutable in-flight reservation per stream |
| Retired peer epoch | A stronger durable namespace/replica epoch checkpoint subsumes the fence | Retain; no TTL |
| Tombstones / ghosts | Normative safe namespace frontier plus stale-peer fencing and mandatory authoritative full-resync proof | GC remains disabled under E-25 |

A future GC implementation must be horizon-based and monotonic. It must persist the proof/checkpoint that authorizes deletion in the same transaction as deletion, be idempotent across restart, and fail closed when any interested peer or provider receipt is missing. Receipt GC and ChangeId GC must not be coupled to relay payload compaction.

## 11. Authority cutover gates (do not mark complete prematurely)

The migration is complete only when all of the following are true:

- Node's inbox receipt lookup, receive cursor and digest-chain checkpoint are caches/rebuildable projections, not independent acceptance authorities.
- Metadata Store validates and commits the incoming batch identity/cursor/chain and dedup decision before the provider effect becomes publishable.
- Node applies user-visible state from the provider or a rebuildable projection; Node's local Outbox/peer dictionaries do not allocate or own durable protocol sequence state in Storage-backed mode.
- The transport reads immutable outbound/relay reservations and ACKs only through Metadata Store. The legacy Node-backed transport path is either explicitly volatile-only or removed from the Storage-backed API surface.
- Recovery reconstructs the runtime from provider + Metadata Store without relying on stale NodePersistence inbox/Outbox state.
- Shared-transaction and separate-store profiles have distinct conformance tests and documented ACK guarantees.
- A two-/three-node test proves restart, lost ACK, alternate-route dedup, equivocation, peer retirement and compaction using only durable Metadata Store authority.

Until these gates pass, the durable outbound capability is a useful first slice, not a completed source-of-truth migration.
