# BinSync.Storage

Runtime adapters that connect BIN-SYNC's public `IDataProvider` and `IMetadataStore` contracts to the neutral `Fld1.Storage` byte-key/value engine.

## Components

- `StorageDataProvider`: stable source identity mapping, durable current-field state, local change feed, contiguous scan watermark, source snapshot/feed barrier, ACK cleanup, idempotent remote apply receipts and restart recovery.
- `StorageMetadataStore`: durable receive cursors and batch receipts, global ChangeId fingerprints, relay/outbound payload streams, leases, ACK/high-watermarks, compaction and Pending Apply intents/completions. Each logical metadata entry is a separate Storage key; each operation writes only the changed key diff in one Storage batch.
- `StorageSyncCoordinator`: replays local provider changes into a Node with their original HLC/author, invokes a caller-supplied durable Node checkpoint, and only then acknowledges the provider feed. It can also materialize Node LWW winners/tombstones into the provider without creating new local feed entries.

## Required publication ordering

```text
provider local feed
    -> Node.WriteField / Node.TombField (original HLC and author)
    -> caller persists Node state + Outbox
    -> provider ACK
```

If the checkpoint callback fails, the feed remains unacknowledged. Replaying the same HLC and author against the restored Node is idempotent and does not create a second Outbox operation.

Remote materialization uses a deterministic apply token derived from the canonical Node field states. The provider records the receipt atomically with the field updates, so a retry after restart cannot apply a conflicting payload under the same token.

## Current boundaries

- Storage adapters are public runtime classes; the conformance suites remain test-only.
- The current Node API supports field key epoch zero. The coordinator rejects other epochs instead of silently dropping them.
- The coordinator is an explicit host-side integration layer. Automatic NodeBuilder/Transport injection of external providers and metadata stores is a separate migration gate.
- The Metadata Store adapter serializes operations within one adapter instance and uses Storage's optimistic batch sequence to reject stale state. A multi-process writer/lease model is not claimed.
- The vendored `external/Fld1.Storage` source is pinned to the authoritative private FLD-DB commit recorded in its README. It must be refreshed from a tested FLD-DB commit, not modified independently.

The executable array example under `samples/StorageArraySync` exercises the public provider and coordinator APIs.