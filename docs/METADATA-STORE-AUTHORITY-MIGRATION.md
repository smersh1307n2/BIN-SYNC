# Metadata Store authority migration

**Status:** in progress, verified against BIN-SYNC `feature/fld-external-storage` through the provider-authoritative outbound publication slice. The optional `IMetadataPeerStateStore` persists the namespace peer roster, per-peer/per-author cursors, immutable batch reservations, ACK/digest progress, retired epoch watermarks and explicit initial snapshot/feed-baseline completion. In Storage-backed receive mode, SyncSession decodes and resolves authors without consulting Node inbox/cursor/digest state; PendingApply is staged first, and Metadata Store validates receipt identity, the canonical full-wire-header fingerprint, ChangeId equivocation, cursor and digest-chain continuity before Node mutation. New receipt rows persist the 16-byte full-header fingerprint with DurableAuthor; the coordinator verifies both before ACK. Node then applies record state without publishing a second InboxAuthors/PeerStates/LastBatchDigest/AcksOut receipt or relay Outbox copy. Version-5 PendingApply records persist the authority mode and optional header fingerprint; legacy v2/v3 intents retain Node-owned recovery semantics, while pre-fingerprint v4 authority intents are downgraded to the legacy Node validator. In Storage-backed provider-authoritative mode, a durable provider cutover marker fences the Node Outbox sequence; stable provider ChangeIds are frozen directly into Metadata Store, which allocates the outbound sequence, and the provider feed is ACKed only after that freeze commits. Node is rebuilt from the provider snapshot as an application-data cache. The legacy non-Storage path and pre-cutover transition still use Node Outbox. Node continues to own session epoch fencing, alias resolution, its application-side ChangeId cache, data/WAL checkpoint and the legacy local Outbox path. Current-format metadata mutations use row-level delta transactions for PendingApply finalization, outbound freeze/lease, ACK and compaction, incoming-batch acceptance, interested-peer registration, initial-sync baseline completion and peer-epoch retirement. Peer roster, stream, required-stream and retired-epoch rows are committed atomically; idempotent registration/baseline/retirement calls do not write. `PersistPartition` is retained only for one-time legacy-format migration on those transitions. Pending ChangeIds are indexed once per validation pass. Remaining gates: separate-process/abandoned-mutex verification for the new single-writer WAL fence on each supported OS, physical Storage crash matrix, complete legacy Node/Transport transition, controlled BenchmarkDotNet measurements for peer lifecycle mutations and metadata mutation/compaction cost, receipt/dedup GC horizons, and normative §§4–18 audit. The durable Storage Inbox receipt key follows protocol identity `(senderNodeId, senderEpoch, namespaceId, batchId)` and excludes the separately tracked resolved author stream. Legacy author-suffixed physical keys are accepted during decode and normalized on subsequent state rewrite. Durable receipt values retain and return the resolved `durableAuthor` (including completed PendingApply retries); new Storage-backed receipts persist the complete 16-byte protocol `headerFingerprint`. Legacy receipt rows predating this field remain readable but cannot prove full-header identity in the metadata-authoritative replay path; such duplicates fail closed.

This document does not change the wire protocol.
## 1. Current boundary (verified in code)

The Storage integration is durable and usable, including over real TCP. The Metadata Store is authoritative for Storage-backed outbound payload sequencing after an explicit provider cutover and for Storage-backed receive receipts/peer transport progress. This is a staged authority migration, not a replacement of every Node-owned state path.

The current Storage-backed provider path is:

1. `StorageDataProvider` exposes a durable local change feed whose entries carry stable 128-bit `ChangeId` values.
2. Before cutover, `StorageSyncCoordinator.PublishLocalChanges` uses the legacy transition path: coalesce pending feed entries, import winners into Node, checkpoint Node, freeze its Outbox into Metadata Store and then ACK the provider feed.
3. `EnableMetadataStoreOwnedPublication` permits cutover only with an empty provider feed and exactly aligned Node/Metadata outbound high-watermarks. It installs a stable provider snapshot into Node, persists the Node checkpoint and writes an immutable cutover watermark to the provider.
4. After cutover, `PublishLocalChanges` no longer allocates new Node Outbox rows. It computes canonical fingerprints for any feed ChangeIds already present in the Metadata Store ledger, preserves those immutable operations, coalesces only the unfrozen suffix, calls `FreezeOutbound` directly for new winners, ACKs the provider feed after the metadata commit and rebuilds Node from the provider snapshot as a cache.
5. A retry after metadata freeze but before provider-feed ACK is idempotent even if newer provider edits arrive meanwhile: the global ChangeId ledger identifies the already-frozen prefix, its original sequences remain immutable, and only the later suffix receives new outbound sequences. If a provider cutover marker is missing while Metadata Store has advanced beyond Node's fenced sequence, the coordinator fails closed instead of falling back to the Node mirror path.
6. The legacy `SyncSession.PushBatchAsync` still reads Node's Outbox and peer state. The overload accepting `IMetadataPeerStateStore` restores per-peer progress from Metadata Store, reads local Outbound and Relay pages at the peer's durable cursor, materializes a temporary encoder view from those rows without replacing or mutating the live Node Outbox, resumes any immutable reserved payload, persists a new reservation before send, marks it pushed after transport handoff, and commits the matching ACK/digest durably. Node remains responsible for alias translation and wire encoding; durable peer progress is passed directly into the batch encoder as an explicit input. The durable overload does not restore or write ACK/digest cursors into Node peer dictionaries.
7. On the Storage-backed receive path, `SyncSession.ReceiveBatchAsync` uses `DecodeBatchPayloadForMetadataAuthority`: Node checks wire integrity, schema, session epoch and alias resolution, but does not consult Node-local inbox/cursor/digest state. The coordinator's `beforeApply` callback stages the exact payload as a version-5 PendingApply intent, including the canonical 16-byte `InboxReceiptIdentity` full-header fingerprint; Metadata Store validates receipt identity, full header, ChangeId equivocation, cursor and digest chain. Node then applies record state through `ApplyBatchPayloadWithMetadataAuthority`, which does not publish Node InboxAuthors, PeerStates, LastBatchDigest, AcksOut or a relay Outbox mirror. Before ACK, the coordinator checkpoints Node, applies immutable provider mutations idempotently, and finalizes Metadata Store receipt/dedup/relay state in one metadata transaction. The final receipt and completed PendingApply retain both DurableAuthor and HeaderFingerprint, which the coordinator verifies before ACK. Recovery honors the persisted authority mode: v5 intents replay through the Metadata Store-authoritative path; v4 intents from the pre-fingerprint transition are downgraded to the legacy Node validator; legacy v2/v3 intents continue through the old Node-owned path. The legacy non-Storage SyncSession path remains Node-owned.

The TCP array sample proves this sequence across independent Storage databases, close/reopen, a subsequent update and a record tombstone. It does not change the ownership described above.

`Node.RestoreProviderSnapshot` provides a staged namespace-cache replacement primitive: it builds the replacement off to the side, rejects duplicate field/lifecycle rows and reserved lifecycle-field misuse before publication, preserves local outbound ChangeId markers from `SeenChanges`, and removes only pending remote IDs from the Node application cache so PendingApply replay can proceed. Regression tests cover local-vs-remote identity preservation and atomic rejection of malformed snapshots. `StorageSyncCoordinator.RehydrateNodeFromProviderSnapshot` now wires this primitive into an explicit provider-authoritative startup/recovery operation: it first drains and freezes the durable provider local feed, captures a stable provider snapshot, collects ChangeIds from durable PendingApply intents, replaces the Node namespace data cache, checkpoints that base, and only then replays/finalizes the pending intents against Node and provider. The operation is deliberately explicit rather than automatically invoked by `AutoReconcileWithMetadataAsync`: the existing coordinator mode still supports Node-originated writes and uses a merge-based snapshot path. Callers may use provider-authoritative rehydration only when all local mutations for the namespace are captured by the provider feed; Node-originated writes that bypass the provider are intentionally not preserved by the replacement. This closes the helper-to-startup-path gap for that declared ownership mode, but does not remove NodePersistence/WAL or complete the legacy transport cutover.

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
| User field values and lifecycle | Data Provider; Node namespace view is a rebuildable cache in provider-authoritative mode | Data Provider |
| Local source feed | StorageDataProvider | Data Provider |
| PendingLocal before freeze | Provider feed / coordinator; no second per-edit metadata journal | One explicit provider-owned staging source until freeze |
| Frozen outbound payload and sequence | Metadata Store after provider cutover; Node Outbox only in pre-cutover/legacy mode | Metadata Store |
| Global ChangeId fingerprint ledger | Metadata Store is authoritative in Storage-backed publication/receive mode; Node retains an application replay cache | Metadata Store |
| Per-peer, per-author ACK/pushed/offset | Metadata Store in durable transport mode; Node dictionaries remain for legacy mode | Metadata Store |
| Outbound BatchId/digest replay mapping | Metadata Store immutable reservation in durable transport mode; Node retains legacy state | Metadata Store |
| Inbound batch receipts and receive digest chain | Metadata Store in Storage-backed receive mode; Node validation remains for legacy transport | Metadata Store |
| Relay payload and relay progress | Metadata Store in durable Storage-backed mode; Node remains legacy owner otherwise | Metadata Store |
| Pending provider APPLY | Metadata Store intent plus provider receipt | Metadata Store + provider receipt |
| Alias map for a live session | Node/session memory | Runtime cache; not durable |
| Durable author/epoch knowledge | Node `DurableAuthors`, `KnownEpochs`, `PeerSessionEpochs` | Explicit durable control metadata or a documented reconstructible source |
| Diagnostic queues/events | Node memory | Runtime only |

The Storage-backed receive path no longer uses Node receive cursors/inbox/digest as acceptance authority, but Node's legacy transport path still does. The Node WAL can replay remote batches into its local data cache; it is not consulted for Storage-backed receipt decisions. In provider-authoritative outbound mode, the Node Outbox is fenced at cutover and is not the source of new local outbound sequences. Node's GlobalChangeLedger remains an application-side replay cache, while Metadata Store's ChangeId ledger is authoritative for Storage-backed publication and relay/equivocation decisions.

A critical implementation detail: Node's per-peer progress is not a single ACK counter. It includes per-author ACKed and pushed watermarks, stream offsets, replay mappings for cursor ranges, initial-sync state and received-stream cursors. The sender digest chain is also peer-specific. These values are part of correctness, not optional diagnostics.

## 3. Existing contract is not yet sufficient

The current `IMetadataStore` exposes durable incoming receipts, PendingApply, outbound/relay pages, local consumer leases, receive cursors and high-watermarks. The optional `IMetadataPeerStateStore` adds a durable interested-peer roster, peer/epoch/namespace/author progress and immutable outbound reservations without forcing volatile or third-party `IMetadataStore` implementations to change. The durable outbound slice is now functional, including minimum-ACK compaction fencing. This remains a staged capability rather than the complete authority boundary.

In particular:

- `AcquireOutboundLease` / `AcknowledgeOutbound` currently model a consumer lease over a local author stream. They do not identify a remote peer and cannot by themselves represent independent ACK progress for multiple peers.
- `MetadataOutboundItem.Sequence` is the local durable sequence. BIN-SYNC wire cursors are peer/author-stream-relative and may include a stream offset. The mapping must be durable per peer and author.
- The legacy Node-backed path caches the BatchId and previous digest for an outstanding `(author, cursorStart, cursorEnd)` range. In Storage-backed mode, the immutable Metadata Store reservation is the replay identity authority. A lost ACK must reproduce the same batch identity and digest, not mint a new batch.
- `MetadataFrozenOperation` currently stores ChangeId, fingerprint and opaque payload. `OutboxEntry` also carries causal dependency metadata; the coordinator's current freeze envelope does not persist that metadata. Before generalizing the authoritative path beyond the current LWW/key-epoch-zero subset, payload parity must be made explicit and tested.
- `Node.ApplyBatchPayloadWithBatch` remains the legacy Node-owned receive path. Storage-backed transport now uses `ApplyBatchPayloadWithMetadataAuthority`, which keeps record application and the session epoch fence but does not publish a duplicate Node inbox/cursor/digest/ACK/relay state. This distinction is persisted on PendingApply intents so a restart does not guess which authority accepted the batch.

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
6. **Recover without a second authority.** Partial: `StorageSyncCoordinator.RehydrateNodeFromProviderSnapshot` now installs a stable provider snapshot into the Node namespace cache and replays durable PendingApply intents after the base checkpoint. It is explicitly opt-in and requires every local mutation to be captured by the provider feed; the existing merge path remains for mixed-origin workloads. Recovery is namespace-scoped: a coordinator processes only its own namespace's PendingApply intents and leaves other namespaces for their coordinators. Provider-authoritative replacement fails closed if one of its own pending intents has no replayable wire payload; the caller must first complete that legacy intent through `RecoverPendingApplies`, then rehydrate from the now-committed provider snapshot. This does not yet reconstruct the complete runtime solely from Metadata Store + provider: NodePersistence/WAL still supplies Outbox allocation, local replay identity and legacy receive/session state. The remaining cutover must rebuild protocol state from Metadata Store and prove recovery without relying on stale Node protocol mirrors.
7. **Prove multi-peer behavior.** A real three-replica relay test now covers A→B→C, with D registered but offline: C's ACK cannot compact the shared relay payload; after D connects and ACKs, the minimum peer cursor advances and compaction succeeds. Lost-ACK replay across Metadata Store reopen and sender recreation is now covered. Still add process restart at every receive boundary, alternate-route duplicate delivery with equivocation, multiple author streams under relay, and concurrent same-peer sender/fencing tests.
8. **Remove transitional mirroring.** Once conformance and recovery pass, remove the coordinator's Node-Outbox-to-Metadata mirror path and old duplicate durable protocol maps from the Storage-backed runtime. Keep volatile mode separately if it remains a supported configuration.

### Local outbound identity groundwork (2026-11-08)

`ProviderChange.ChangeId` is now an optional stable 128-bit source identity. `StorageDataProvider` persists it as a suffix in the existing local-feed row (not a second key/table), returns the same identity after reopen, removes it with the feed row on ACK, and upgrades legacy feed rows on first read by atomically appending an identity. A zero or malformed persisted identity fails closed.

`FreezeOutbound` now supports per-ChangeId incremental idempotency: a replay containing previously frozen feed entries plus newly appended entries preserves existing sequences and allocates only the new ones, while conflicting reuse still fails atomically. Previously frozen entries must be an ordered prefix of the supplied operations; this prevents a retry from assigning a later sequence to an earlier source entry. This is required because a provider feed can grow between a durable freeze and a failed provider ACK.

This is groundwork only: `StorageSyncCoordinator.PublishLocalChanges` still materializes provider changes through Node Outbox and then freezes those entries into Metadata Store. The next cutover slice must:

1. require non-zero `ProviderChange.ChangeId` for the provider-authoritative publication mode;
2. encode the exact provider mutation into `MetadataFrozenOperation` using that ChangeId and its semantic fingerprint;
3. let Metadata Store allocate the per-author Outbox sequence and own the immutable payload directly;
4. install/checkpoint the provider snapshot in Node only as an application-data cache, then ACK the provider feed after durable freeze/recovery;
5. keep the Node-Outbox mirror only as an explicit transition path until all pre-cutover rows are frozen and the legacy transport path is fenced.

The provider feed remains the sole unpublished-local-change staging source; no additional Pending journal is introduced. The ChangeId suffix is transient with that feed row and does not become a second long-term history record.

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

This is a structural complexity improvement, not a benchmark claim. Release evidence still requires measurements at increasing partition sizes (e.g. 10k / 100k / 1m ledger rows), reporting bytes written, allocations, p50/p95 latency and reopen time. The generic `PersistPartition` path remains in use for other state transitions and still performs whole-partition materialization/diffing; it is a separate optimization gate. Relay/outbound ACK watermark updates and per-stream payload compaction now also use direct row-level write/delete batches instead of cloning and diffing the whole partition. These paths retain the same fail-closed safe-prefix checks and update the in-memory projection only after the Storage transaction commits. `AcquireOutboundLease` now updates only its lease row for current-format partitions; when legacy marker, ledger or watermark encodings are detected, the decoder records a partition-level legacy flag and the operation deliberately falls back to one full rewrite so migration and lease generation remain atomic; current-format partitions do not scan all persisted rows to make this decision. `FreezeOutbound` now writes only the new outbound payload rows, ChangeId ledger rows, one stream watermark and any newly required peer-stream rows; it uses the same legacy-format fallback before taking the delta path. `AcceptIncomingBatch` now commits the receipt, receive cursor/chain, new ChangeId ledger rows, relay payloads/watermarks and newly required peer-stream rows in one delta transaction; legacy partitions still use the established full migration rewrite.

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


## 12. Provider-authoritative outbound publication (2026-11-08)

The Storage-backed provider path now has its first explicit outbound authority cutover. The implementation is deliberately opt-in and one-way for a provider instance:

- `IProviderPublicationMode` exposes the durable cutover watermark; `StorageDataProvider` persists it in one Storage key and rejects a different subsequent watermark.
- Cutover requires the provider feed to be fully acknowledged and the Node Outbox high-watermark to equal the Metadata Store outbound high-watermark. Node-only writes must be migrated before enabling the mode.
- After cutover, the Node Outbox high-watermark is an immutable fence. Direct Node Outbox writes cause the coordinator to fail closed.
- `StorageDataProvider` stable ChangeIds are frozen directly to `IMetadataStore`; Metadata Store allocates outbound author sequences. The provider feed is acknowledged only after the immutable freeze transaction commits. Node's current namespace data is then rebuilt from a stable provider snapshot and persisted as a cache.
- The optional `IMetadataOutboundChangeIdentityStore.TryGetOutboundChangeFingerprint` capability returns a `ReadOnlyMemory<byte>` view of the durable global ChangeId ledger fingerprint, including after outbound payload compaction, without adding a mandatory member to the base `IMetadataStore` contract or allocating a 16-byte copy per replayed frozen row. On replay, the coordinator retains already-frozen identities and coalesces only the not-yet-frozen source-feed suffix. This is essential when a metadata freeze commits, provider ACK fails, and a newer local edit arrives before retry: the old immutable operation must remain in the stream and the newer operation receives the next sequence.
- The TCP sample now enables this mode, establishes an explicit empty initial peer baseline, pushes through the durable Metadata Store transport overload and verifies that a post-reopen update advances Metadata Store without advancing Node Outbox. `AutoReconcileWithMetadataAsync` detects the persisted provider cutover and automatically rehydrates the Node cache from the provider snapshot before anti-entropy; the legacy merge-based snapshot path remains for providers without the marker. Once the marker is durable, duplicate local-author Outbox payload rows are removed from the Node checkpoint while the Node sequence fence and WAL/ChangeId replay identities remain. Rehydration repeats the cleanup if a stale pre-cleanup checkpoint is loaded after a crash.

**Verified at code/test head `0399e8f0dfda39b5e860eca50364406323c7b7fa`:** BinSync.Net 644/644, Storage Integration 100/100, Fuzz + Lab 31/31, line coverage 70.8%; in-process and real-TCP Storage samples pass. Regressions cover metadata-freeze/provider-ACK interruption with a later edit, post-ACK Node checkpoint failure/reopen, automatic rehydration of a stale Node checkpoint during durable reconcile, local Node Outbox payload cleanup with watermark preservation, and missing cutover-marker fail-closed behavior.

**Still open:** this is not a full removal of Node protocol state. The legacy Node-backed transport and pre-cutover migration path remain; NodePersistence/WAL still supports the legacy Node Outbox and receive/application caches; after cutover the coordinator removes duplicate local-author Outbox payload rows, while other-author relay rows remain subject to the separate receive/relay migration gate. The receive authority and durable peer transport slice are Storage-backed opt-in paths, not a universal runtime mode. Deterministic physical-Storage crash injection, multi-process concurrency/fencing, controlled large-partition benchmarks, receipt/ChangeId/tombstone GC horizons and normative cross-implementation conformance remain release gates.
