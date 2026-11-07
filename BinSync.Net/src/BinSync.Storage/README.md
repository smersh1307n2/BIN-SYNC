# BinSync.Storage

Runtime adapters that connect BIN-SYNC's public `IDataProvider` and `IMetadataStore` contracts to the neutral `Fld1.Storage` byte-key/value engine.

## Components

- `StorageDataProvider`: stable source identity mapping, durable current-field state, local change feed, contiguous scan watermark, source snapshot/feed barrier, ACK cleanup, idempotent remote apply receipts and restart recovery. Local append is idempotent for an identical HLC/author mutation, preserves every distinct source event in the feed, keeps current state at the LWW winner and rejects same-version equivocation.
- `StorageMetadataStore`: durable receive cursors and batch receipts, global ChangeId fingerprints across inbound and outbound paths, relay/outbound payload streams, leases, ACK/high-watermarks, compaction and Pending Apply intents/completions. Each logical metadata entry is a separate Storage key; each operation writes only the changed key diff in one Storage batch.
- `StorageSyncCoordinator`: replays local provider changes into a Node with their original HLC/author, invokes a caller-supplied durable Node checkpoint, and only then acknowledges the provider feed. It can also materialize Node LWW winners/tombstones into the provider without creating new local feed entries.
- `StorageDataProvider(StorageDatabase)` and `StorageMetadataStore(StorageDatabase)`: allow both adapters to share one caller-owned Storage database/partition without either adapter disposing it.

## Required publication ordering

```text
provider local feed
    -> Node.WriteField / Node.TombField (original HLC and author)
    -> caller persists Node state + Outbox
    -> provider ACK
```

If the checkpoint callback fails, the feed remains unacknowledged. Replaying the same HLC and author against the restored Node is idempotent and does not create a second Outbox operation.

## Required receive ordering

```text
receive and durably checkpoint Node batch/receipt
    -> StorageSyncCoordinator.ApplyRemoteFields
    -> StorageDataProvider.ApplyRemoteBatch (durable receipt + field state)
    -> sender ACK
```

Both the in-process `Transport.DeliverBatch/SyncRound` and `SyncSession.ReceiveBatchAsync` expose a `beforeAck` callback. A callback failure leaves the sender unacknowledged; replay of the same batch is handled as a Node duplicate and the provider's deterministic apply token makes the provider commit idempotent. The executable array sample exercises this path, including a simulated receiver restart between provider commit and ACK.

Remote materialization uses a deterministic apply token derived from the canonical Node field states. The provider records the receipt atomically with the field updates, so a retry after restart cannot apply a conflicting payload under the same token.

## Current boundaries

- Storage adapters are public runtime classes; the conformance suites remain test-only.
- `MetadataFrozenOperation` carries the canonical 16-byte operation fingerprint. The Metadata Store records locally frozen fingerprints in the same global ChangeId ledger used for incoming operations, so an operation already seen on another route is not relayed a second time and equivocation is rejected.
- The current Node field-write API supports key epoch zero. The coordinator rejects other epochs instead of silently dropping them.
- The coordinator is an explicit host-side integration layer. The existing Node/Transport path has not yet been migrated to use `IMetadataStore` as its authoritative Outbox, receive/relay, ACK/lease and Pending Apply state.
- The Metadata Store adapter serializes operations within one adapter instance and uses Storage's optimistic batch sequence to reject stale state. Shared-instance interleaving refreshes cached partitions; concurrent conflicting writes fail closed with `StorageConflictException`. A multi-process writer/lease model is not claimed.
- The adapter currently clones collection structures and computes a logical key diff from the in-memory partition state on mutation. Persisted writes are key-level diffs, but CPU work still scales with partition size; this remains a performance optimization gate before large-scale metadata workloads.
- The vendored `external/Fld1.Storage` source is pinned to the authoritative private FLD-DB commit recorded in its README. It must be refreshed from a tested FLD-DB commit, not modified independently.

The executable array example under `samples/StorageArraySync` exercises the public provider and coordinator APIs.
