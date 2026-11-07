# BinSync.Storage

Runtime adapters that connect BIN-SYNC's public `IDataProvider` and `IMetadataStore` contracts to the neutral `Fld1.Storage` byte-key/value engine.

## Components

- `StorageDataProvider`: stable source identity mapping, durable current-field and record-lifecycle state, local change feed, contiguous scan watermark, source snapshot/feed barrier, ACK cleanup, idempotent remote apply receipts and restart recovery. Local append is idempotent for an identical HLC/author mutation, preserves every distinct source event in the feed, keeps current state at the LWW winner and rejects same-version equivocation.
- `StorageMetadataStore`: durable receive cursors and batch receipts, global ChangeId fingerprints across inbound and outbound paths, relay/outbound payload streams, leases, ACK/high-watermarks, compaction and Pending Apply intents/completions. Each logical metadata entry is a separate Storage key; each operation writes only the changed key diff in one Storage batch.
- `StorageNodePersistence`: stores the complete integrity-protected NodePersistence v15 image under a stable NodeId key in the same caller-owned StorageDatabase. Save is one atomic Storage write batch; load uses the existing digest/version validation.
- `StorageSyncCoordinator`: replays local provider changes into a Node with their original HLC/author, invokes a caller-supplied durable Node checkpoint, freezes newly appended local Outbox entries into Metadata Store using their original per-author sequence numbers, and only then acknowledges the provider feed. For received field batches it applies the exact immutable operations (not reconstructed current winners), optionally wraps the provider commit in `IMetadataStore` PendingApply/Finalize transactions, and can recover pending provider commits after restart.
- `StorageDataProvider(StorageDatabase)` and `StorageMetadataStore(StorageDatabase)`: allow both adapters to share one caller-owned Storage database/partition without either adapter disposing it.

## Required publication ordering

```text
provider local feed
    -> Node.WriteField / Node.TombField (original HLC and author)
    -> StorageNodePersistence saves Node state + Outbox in the same StorageDatabase
    -> MetadataStore.FreezeOutbound (exact ChangeId/fingerprint/payload, original OutboxSeq)
    -> provider ACK
```

If the checkpoint callback fails, the feed remains unacknowledged. Replaying the same HLC and author against the restored Node is idempotent and does not create a second Outbox operation.

## Required receive ordering

```text
receive and durably checkpoint Node batch/receipt
    -> MetadataStore.BeginPendingApply (when attached)
    -> StorageDataProvider.ApplyRemoteBatch (batch-id receipt + exact field mutations)
    -> MetadataStore.FinalizePendingApply
    -> sender ACK
```

Both the in-process `Transport.DeliverBatch/SyncRound` and `SyncSession.ReceiveBatchAsync` expose a `beforeAck` callback carrying the resolved immutable `Batch`. The coordinator applies each FieldWrite/FieldTomb from that batch, preserving its HLC, key epoch, durable author and value; multiple operations for one field in one batch are retained in the receipt and folded by Storage's LWW comparison. The provider apply token is derived from the immutable issuer/namespace/BatchId, so retries still match after later updates. When a Metadata Store is attached, a durable PendingApply intent precedes provider commit and metadata finalization follows it. `RecoverPendingApplies()` replays those intents after restart. The executable array sample attaches both adapters to the same StorageDatabase for each replica.

Metadata receive cursors are tracked per `(sender, sender epoch, namespace, original author stream)`, while the previous-batch digest chain is tracked per `(sender, sender epoch, namespace)`. This mirrors Node's per-author sequence cursors and sender-level digest chain. Legacy metadata batches that omit `AuthorStreamNodeId` retain the prior single-stream cursor behavior. Metadata relay payloads use an opaque envelope: 16-byte RecordId followed by the canonical operation payload, allowing a pending apply to reconstruct provider mutations after restart.

## Current boundaries

- Storage adapters are public runtime classes; the conformance suites remain test-only.
- `MetadataFrozenOperation` carries the canonical 16-byte operation fingerprint. The Metadata Store records locally frozen fingerprints in the same global ChangeId ledger used for incoming operations, so an operation already seen on another route is not relayed a second time and equivocation is rejected.
- The current Node field-write API supports key epoch zero for local provider publication. Received remote field operations preserve their encoded key epoch in Storage. Record lifecycle tombstones are stored under a separate record key; `TryReadCurrent` applies BIN-SYNC ghost visibility, hiding fields ordered at or before the lifecycle tombstone while allowing newer ghost fields.
- The coordinator is an explicit host-side integration layer. The existing Node/Transport path has not yet been migrated to use `IMetadataStore` as its authoritative Outbox, receive/relay, ACK/lease and Pending Apply state.
- The Metadata Store adapter serializes operations within one adapter instance and uses Storage's optimistic batch sequence to reject stale state. Shared-instance interleaving refreshes cached partitions; concurrent conflicting writes fail closed with `StorageConflictException`. A multi-process writer/lease model is not claimed.
- The adapter currently clones collection structures and computes a logical key diff from the in-memory partition state on mutation. Persisted writes are key-level diffs, but CPU work still scales with partition size; this remains a performance optimization gate before large-scale metadata workloads.
- The vendored `external/Fld1.Storage` source is pinned to the authoritative private FLD-DB commit recorded in its README. It must be refreshed from a tested FLD-DB commit, not modified independently.

The executable array example under `samples/StorageArraySync` exercises the public provider and coordinator APIs.
