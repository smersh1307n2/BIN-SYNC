using System.IO;
using BinSync;
using Xunit;

namespace BinSync.MetadataStore.Conformance;

/// <summary>
/// Reusable semantic tests for Metadata Store adapters. The suite intentionally
/// tests observable behavior rather than a concrete key/value or file layout.
/// </summary>
public abstract class MetadataStoreConformanceSuite
{
    protected abstract IMetadataStoreConformanceAdapter CreateStore();

    [Fact]
    public void Exact_batch_retry_returns_original_receipt_without_duplicate_relay()
    {
        using var store = CreateStore();
        var batch = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));

        var first = store.AcceptIncomingBatch(batch);
        var duplicate = store.AcceptIncomingBatch(batch);

        Assert.False(first.IsDuplicate);
        Assert.True(duplicate.IsDuplicate);
        Assert.Equal(first.CursorEnd, duplicate.CursorEnd);
        Assert.Equal(first.BatchDigest, duplicate.BatchDigest);
        Assert.Equal(1UL, store.GetReceiveCursor("scope-a", Node(1), Node(2), 1, 7));
        Assert.Single(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);
        Assert.Equal(1UL, store.GetRelayHighWatermark("scope-a", Node(1), 7, Node(20)));
    }

    [Fact]
    public void Same_change_from_another_peer_advances_its_receipt_but_does_not_relay_twice()
    {
        using var store = CreateStore();
        var operation = Operation(20, 30, 40, 50);
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10, operation));

        var secondRoute = store.AcceptIncomingBatch(
            CreateBatch("scope-a", 1, 3, 1, 7, 11, 1, 1, 0, 11, operation));

        Assert.False(secondRoute.IsDuplicate);
        Assert.Equal(1UL, store.GetReceiveCursor("scope-a", Node(1), Node(3), 1, 7));
        Assert.Single(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);
        Assert.Equal(1UL, store.GetRelayHighWatermark("scope-a", Node(1), 7, Node(20)));
    }

    [Fact]
    public void Change_id_equivocation_rejects_the_whole_batch_without_partial_publication()
    {
        using var store = CreateStore();
        var first = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));
        store.AcceptIncomingBatch(first);

        var conflicting = CreateBatch("scope-a", 1, 2, 1, 7, 11, 2, 2, 10, 11,
            Operation(20, 31, 41, 51),
            Operation(20, 30, 99, 52));

        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(conflicting));
        Assert.Equal(1UL, store.GetReceiveCursor("scope-a", Node(1), Node(2), 1, 7));
        Assert.Equal(1UL, store.GetRelayHighWatermark("scope-a", Node(1), 7, Node(20)));
        Assert.Empty(store.ReadRelay("scope-a", Node(1), 7, Node(20), 1, 10).Items);
    }


    [Fact]
    public void Locally_frozen_change_is_in_global_ledger_and_not_relayed_again_on_another_route()
    {
        using var store = CreateStore();
        var frozen = FrozenOperation(30, 50, fingerprint: 40);
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { frozen });

        var alternateRoute = store.AcceptIncomingBatch(
            CreateBatch("scope-a", 1, 3, 1, 7, 11, 1, 1, 0, 11,
                Operation(20, 30, 40, 51)));

        Assert.False(alternateRoute.IsDuplicate);
        Assert.Equal(1UL, store.GetReceiveCursor("scope-a", Node(1), Node(3), 1, 7));
        Assert.Equal(0UL, store.GetRelayHighWatermark("scope-a", Node(1), 7, Node(20)));
        Assert.Empty(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);

        var equivocation = CreateBatch("scope-a", 1, 4, 1, 7, 12, 1, 1, 0, 12,
            Operation(20, 30, 41, 52));
        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(equivocation));
        Assert.Equal(0UL, store.GetReceiveCursor("scope-a", Node(1), Node(4), 1, 7));
    }

    [Fact]
    public void Incoming_change_id_cannot_be_republished_as_a_new_local_outbound_change()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50)));

        Assert.Throws<InvalidDataException>(() =>
            store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
                new[] { FrozenOperation(30, 51, fingerprint: 40) }));
        Assert.Equal(0UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));
    }

    [Fact]
    public void Relay_ack_is_per_author_and_cannot_exceed_that_author_high_watermark()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50),
            Operation(21, 31, 41, 51)));

        store.AcknowledgeRelay("scope-a", Node(1), 7, Node(20), 1);
        store.CompactRelayThrough("scope-a", Node(1), 7, Node(20), 1);

        Assert.Empty(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);
        Assert.Single(store.ReadRelay("scope-a", Node(1), 7, Node(21), 0, 10).Items);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            store.AcknowledgeRelay("scope-a", Node(1), 7, Node(21), 2));
    }

    [Fact]
    public void Relay_pages_are_bounded_and_resume_without_gaps_or_duplicates()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 3, 0, 10,
            Operation(20, 30, 40, 50),
            Operation(20, 31, 41, 51),
            Operation(20, 32, 42, 52)));

        var first = store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 2);
        var second = store.ReadRelay("scope-a", Node(1), 7, Node(20), first.ScannedThroughSequence, 2);

        Assert.Equal(new ulong[] { 1, 2 }, first.Items.Select(x => x.Sequence));
        Assert.Equal(2UL, first.ScannedThroughSequence);
        Assert.Equal(new ulong[] { 3 }, second.Items.Select(x => x.Sequence));
        Assert.Equal(3UL, second.ScannedThroughSequence);
    }

    [Fact]
    public void Scope_and_local_replica_partitions_do_not_leak_relay_or_receipt_state()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50)));

        Assert.Single(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);
        Assert.Empty(store.ReadRelay("scope-b", Node(1), 7, Node(20), 0, 10).Items);
        Assert.Empty(store.ReadRelay("scope-a", Node(9), 7, Node(20), 0, 10).Items);
        Assert.Equal(0UL, store.GetReceiveCursor("scope-b", Node(1), Node(2), 1, 7));
        Assert.Equal(0UL, store.GetReceiveCursor("scope-a", Node(9), Node(2), 1, 7));
    }

    [Fact]
    public void Receive_cursor_gaps_and_conflicting_receipt_reuse_fail_closed()
    {
        using var store = CreateStore();
        var first = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));
        store.AcceptIncomingBatch(first);

        var gap = CreateBatch("scope-a", 1, 2, 1, 7, 11, 3, 3, 10, 11,
            Operation(20, 31, 41, 51));
        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(gap));

        var conflictingReceipt = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 99,
            Operation(20, 30, 40, 50));
        Assert.Throws<InvalidDataException>(() => store.AcceptIncomingBatch(conflictingReceipt));
        Assert.Equal(1UL, store.GetReceiveCursor("scope-a", Node(1), Node(2), 1, 7));
        Assert.Equal(1UL, store.GetRelayHighWatermark("scope-a", Node(1), 7, Node(20)));
    }

    [Fact]
    public void Relay_ack_does_not_collect_receipts_or_change_id_dedup_facts()
    {
        using var store = CreateStore();
        var operation = Operation(20, 30, 40, 50);
        var original = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10, operation);
        store.AcceptIncomingBatch(original);
        store.AcknowledgeRelay("scope-a", Node(1), 7, Node(20), 1);

        Assert.Empty(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            store.AcknowledgeRelay("scope-a", Node(1), 7, Node(20), 0));

        var duplicateReceipt = store.AcceptIncomingBatch(original);
        Assert.True(duplicateReceipt.IsDuplicate);

        var alternateRoute = store.AcceptIncomingBatch(
            CreateBatch("scope-a", 1, 3, 1, 7, 11, 1, 1, 0, 11, operation));
        Assert.False(alternateRoute.IsDuplicate);
        Assert.Equal(1UL, store.GetReceiveCursor("scope-a", Node(1), Node(3), 1, 7));
        Assert.Empty(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);
        Assert.Equal(1UL, store.GetRelayHighWatermark("scope-a", Node(1), 7, Node(20)));
    }

    [Fact]
    public void Frozen_outbound_sequences_are_stable_bounded_and_payloads_are_owned()
    {
        using var store = CreateStore();
        var frozen = store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
            new[]
            {
                FrozenOperation(30, 50),
                FrozenOperation(31, 51),
                FrozenOperation(32, 52)
            });
        ulong lease = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000001"));

        Assert.Equal(new ulong[] { 1, 2, 3 }, frozen.Select(x => x.Sequence));
        var first = store.ReadOutbound("scope-a", Node(1), 7, Node(20), lease, 0, 2);
        Assert.Equal(new ulong[] { 1, 2 }, first.Items.Select(x => x.Sequence));
        Assert.Equal(2UL, first.ScannedThroughSequence);

        first.Items[0].Payload[0] = 99;
        var reread = store.ReadOutbound("scope-a", Node(1), 7, Node(20), lease, 0, 2);
        Assert.Equal(50, reread.Items[0].Payload[0]);

        var second = store.ReadOutbound("scope-a", Node(1), 7, Node(20), lease, first.ScannedThroughSequence, 2);
        Assert.Equal(new ulong[] { 3 }, second.Items.Select(x => x.Sequence));
        Assert.Equal(3UL, second.ScannedThroughSequence);
    }

    [Fact]
    public void Outbound_lease_generation_fences_stale_readers_and_acknowledgers()
    {
        using var store = CreateStore();
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(30, 50) });

        ulong staleLease = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000001"));
        ulong currentLease = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000002"));

        Assert.Throws<InvalidOperationException>(() =>
            store.ReadOutbound("scope-a", Node(1), 7, Node(20), staleLease, 0, 10));
        Assert.Throws<InvalidOperationException>(() =>
            store.AcknowledgeOutbound("scope-a", Node(1), 7, Node(20), staleLease, 1));

        Assert.Single(store.ReadOutbound("scope-a", Node(1), 7, Node(20), currentLease, 0, 10).Items);
        store.AcknowledgeOutbound("scope-a", Node(1), 7, Node(20), currentLease, 1);
        Assert.Empty(store.ReadOutbound("scope-a", Node(1), 7, Node(20), currentLease, 0, 10).Items);
    }

    [Fact]
    public void Outbound_ack_rejects_backward_and_out_of_range_prefixes()
    {
        using var store = CreateStore();
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
            new[] { FrozenOperation(30, 50), FrozenOperation(31, 51) });
        ulong lease = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000001"));

        store.AcknowledgeOutbound("scope-a", Node(1), 7, Node(20), lease, 2);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            store.AcknowledgeOutbound("scope-a", Node(1), 7, Node(20), lease, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            store.AcknowledgeOutbound("scope-a", Node(1), 7, Node(20), lease, 3));
    }

    [Fact]
    public void Outbound_freeze_rejects_reused_change_ids_without_partial_sequence_assignment()
    {
        using var store = CreateStore();
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(30, 50) });

        Assert.Throws<InvalidDataException>(() =>
            store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
                new[] { FrozenOperation(31, 51), FrozenOperation(30, 52) }));
        Assert.Equal(1UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));

        var next = store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(31, 51) });
        Assert.Equal(2UL, Assert.Single(next).Sequence);
    }

    [Fact]
    public void Outbound_freeze_exact_and_partial_retries_preserve_existing_sequences()
    {
        using var store = CreateStore();
        var batch = new[] { FrozenOperation(30, 50), FrozenOperation(31, 51) };

        var first = store.FreezeOutbound("scope-a", Node(1), 7, Node(20), batch);
        var retry = store.FreezeOutbound("scope-a", Node(1), 7, Node(20), batch);

        Assert.Equal(new ulong[] { 1, 2 }, first.Select(item => item.Sequence));
        Assert.Equal(first.Select(item => item.Sequence), retry.Select(item => item.Sequence));
        Assert.Equal(first.Select(item => item.ChangeId), retry.Select(item => item.ChangeId));
        Assert.Equal(2UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));

        // A replay can include an already-frozen feed entry and a newer entry
        // appended before the source feed ACK completed. Keep the old sequence.
        var incremental = store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
            new[] { FrozenOperation(30, 50), FrozenOperation(32, 52) });
        Assert.Equal(new ulong[] { 1, 3 }, incremental.Select(item => item.Sequence));
        Assert.Equal(3UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));

        var incrementalRetry = store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
            new[] { FrozenOperation(30, 50), FrozenOperation(32, 52) });
        Assert.Equal(new ulong[] { 1, 3 }, incrementalRetry.Select(item => item.Sequence));
        Assert.Equal(3UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));

        Assert.Throws<InvalidDataException>(() =>
            store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
                new[] { FrozenOperation(30, 50), FrozenOperation(32, 53) }));
        Assert.Throws<InvalidDataException>(() =>
            store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
                new[] { FrozenOperation(33, 53), FrozenOperation(30, 50) }));
        Assert.Equal(3UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));
    }

    [Fact]
    public void Outbound_streams_are_partitioned_by_namespace_and_author()
    {
        using var store = CreateStore();
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(30, 50) });
        store.FreezeOutbound("scope-a", Node(1), 7, Node(21), new[] { FrozenOperation(31, 51) });
        store.FreezeOutbound("scope-a", Node(1), 8, Node(20), new[] { FrozenOperation(32, 52) });

        ulong leaseA = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000001"));
        ulong leaseB = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(21), Guid.Parse("00000000-0000-0000-0000-000000000002"));
        ulong leaseC = store.AcquireOutboundLease(
            "scope-a", Node(1), 8, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000003"));

        Assert.Equal(Batch(30), Assert.Single(store.ReadOutbound("scope-a", Node(1), 7, Node(20), leaseA, 0, 10).Items).ChangeId);
        Assert.Equal(Batch(31), Assert.Single(store.ReadOutbound("scope-a", Node(1), 7, Node(21), leaseB, 0, 10).Items).ChangeId);
        Assert.Equal(Batch(32), Assert.Single(store.ReadOutbound("scope-a", Node(1), 8, Node(20), leaseC, 0, 10).Items).ChangeId);
    }

    [Fact]
    public void Relay_and_outbound_pages_reject_cursors_beyond_durable_highwater()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50)));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            store.ReadRelay("scope-a", Node(1), 7, Node(20), 2, 0));

        store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(31, 51) });
        ulong lease = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000001"));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            store.ReadOutbound("scope-a", Node(1), 7, Node(20), lease, 2, 0));
    }

    [Fact]
    public void Outbound_compaction_preserves_highwater_and_prevents_change_id_reuse()
    {
        using var store = CreateStore();
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20),
            new[] { FrozenOperation(30, 50), FrozenOperation(31, 51) });
        ulong lease = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000001"));
        store.AcknowledgeOutbound("scope-a", Node(1), 7, Node(20), lease, 2);
        store.CompactOutboundThrough("scope-a", Node(1), 7, Node(20), 2);

        Assert.Empty(store.ReadOutbound("scope-a", Node(1), 7, Node(20), lease, 0, 10).Items);
        Assert.Equal(2UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));
        Assert.Throws<InvalidDataException>(() =>
            store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(30, 52) }));

        var next = store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(32, 53) });
        Assert.Equal(3UL, Assert.Single(next).Sequence);
    }


    [Fact]
    public void Snapshot_remains_consistent_while_new_commits_and_compaction_advance_live_state()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50)));
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(31, 51) });

        using var snapshot = store.OpenSnapshot("scope-a", Node(1));

        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 11, 2, 2, 10, 11,
            Operation(20, 32, 42, 52)));
        store.FreezeOutbound("scope-a", Node(1), 7, Node(20), new[] { FrozenOperation(33, 53) });

        ulong lease = store.AcquireOutboundLease(
            "scope-a", Node(1), 7, Node(20), Guid.Parse("00000000-0000-0000-0000-000000000001"));
        store.AcknowledgeOutbound("scope-a", Node(1), 7, Node(20), lease, 1);
        store.CompactOutboundThrough("scope-a", Node(1), 7, Node(20), 1);
        store.AcknowledgeRelay("scope-a", Node(1), 7, Node(20), 1);
        store.CompactRelayThrough("scope-a", Node(1), 7, Node(20), 1);

        Assert.Equal(1UL, snapshot.GetReceiveCursor(Node(2), 1, 7));
        Assert.Equal(1UL, snapshot.GetRelayHighWatermark(7, Node(20)));
        Assert.Equal(1UL, snapshot.GetOutboundHighWatermark(7, Node(20)));
        Assert.Single(snapshot.ReadRelay(7, Node(20), 0, 10).Items);
        Assert.Single(snapshot.ReadOutbound(7, Node(20), 0, 10).Items);

        Assert.Equal(2UL, store.GetReceiveCursor("scope-a", Node(1), Node(2), 1, 7));
        Assert.Equal(2UL, store.GetRelayHighWatermark("scope-a", Node(1), 7, Node(20)));
        Assert.Equal(2UL, store.GetOutboundHighWatermark("scope-a", Node(1), 7, Node(20)));
        Assert.Single(store.ReadRelay("scope-a", Node(1), 7, Node(20), 1, 10).Items);
        Assert.Single(store.ReadOutbound("scope-a", Node(1), 7, Node(20), lease, 1, 10).Items);
    }

    [Fact]
    public void Disposed_snapshot_releases_its_read_view_and_rejects_further_access()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50)));
        var snapshot = store.OpenSnapshot("scope-a", Node(1));

        Assert.Single(snapshot.ReadRelay(7, Node(20), 0, 10).Items);
        snapshot.Dispose();

        Assert.Throws<ObjectDisposedException>(() => snapshot.GetReceiveCursor(Node(2), 1, 7));
    }

    [Fact]
    public void Pending_apply_recovers_provider_commit_and_finalizes_metadata_exactly_once()
    {
        using var store = CreateStore();
        var provider = new IdempotentTestProvider();
        var batch = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));
        var token = Guid.Parse("00000000-0000-0000-0000-000000000123");

        store.BeginPendingApply(batch, token);
        var firstIntent = Assert.Single(store.ReadPendingApplyIntents("scope-a", Node(1)));
        Assert.Equal(token, firstIntent.ApplyToken);

        // Simulate a crash after the provider committed but before metadata finalization.
        provider.ApplyOnce(token, firstIntent.Batch);
        var recoveredIntent = Assert.Single(store.ReadPendingApplyIntents("scope-a", Node(1)));
        var providerReceipt = provider.ApplyOnce(token, recoveredIntent.Batch);

        Assert.Equal(1, provider.ApplyCount);
        var finalized = store.FinalizePendingApply("scope-a", Node(1), providerReceipt);
        Assert.False(finalized.IsDuplicate);
        Assert.Empty(store.ReadPendingApplyIntents("scope-a", Node(1)));

        var retry = store.FinalizePendingApply("scope-a", Node(1), providerReceipt);
        Assert.True(retry.IsDuplicate);
        Assert.Equal(finalized.CursorEnd, retry.CursorEnd);
        Assert.Equal(finalized.BatchDigest, retry.BatchDigest);
        Assert.Equal(1UL, store.GetReceiveCursor("scope-a", Node(1), Node(2), 1, 7));
        Assert.Single(store.ReadRelay("scope-a", Node(1), 7, Node(20), 0, 10).Items);
    }

    [Fact]
    public void Pending_apply_returns_global_ChangeId_decisions_and_reserves_unfinalized_ids()
    {
        using var store = CreateStore();
        var accepted = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));
        store.AcceptIncomingBatch(accepted);

        var mixed = CreateBatch("scope-a", 1, 3, 1, 7, 11, 1, 2, 0, 11,
            Operation(20, 30, 40, 50),
            Operation(21, 31, 41, 51));
        var token = Guid.Parse("00000000-0000-0000-0000-000000000126");
        var intent = store.BeginPendingApply(mixed, token);

        Assert.Equal(new[] { false, true }, intent.ApplyOperations);
        var recovered = Assert.Single(store.ReadPendingApplyIntents("scope-a", Node(1)));
        Assert.Equal(new[] { false, true }, recovered.ApplyOperations);

        var competing = CreateBatch("scope-a", 1, 4, 1, 7, 12, 1, 1, 0, 12,
            Operation(21, 31, 41, 51));
        Assert.Throws<InvalidOperationException>(() => store.BeginPendingApply(
            competing, Guid.Parse("00000000-0000-0000-0000-000000000127")));

        store.FinalizePendingApply("scope-a", Node(1),
            new MetadataProviderApplyReceipt(token, mixed.BatchDigest.ToArray()));

        var afterFinalize = CreateBatch("scope-a", 1, 4, 1, 7, 13, 1, 3, 0, 13,
            Operation(20, 30, 40, 50),
            Operation(21, 31, 41, 51),
            Operation(22, 32, 42, 52));
        var nextIntent = store.BeginPendingApply(afterFinalize,
            Guid.Parse("00000000-0000-0000-0000-000000000128"));
        Assert.Equal(new[] { false, false, true }, nextIntent.ApplyOperations);
    }

    [Fact]
    public void Pending_apply_rejects_token_or_provider_receipt_equivocation_without_losing_intent()
    {
        using var store = CreateStore();
        var batch = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));
        var token = Guid.Parse("00000000-0000-0000-0000-000000000124");
        store.BeginPendingApply(batch, token);

        var conflictingBatch = CreateBatch("scope-a", 1, 2, 1, 7, 11, 1, 1, 0, 11,
            Operation(20, 31, 41, 51));
        Assert.Throws<InvalidDataException>(() => store.BeginPendingApply(conflictingBatch, token));

        var wrongProviderReceipt = new MetadataProviderApplyReceipt(token, Bytes(99));
        Assert.Throws<InvalidDataException>(() =>
            store.FinalizePendingApply("scope-a", Node(1), wrongProviderReceipt));
        var retained = Assert.Single(store.ReadPendingApplyIntents("scope-a", Node(1)));
        Assert.Equal(batch.BatchId, retained.Batch.BatchId);
        Assert.Equal(0UL, store.GetReceiveCursor("scope-a", Node(1), Node(2), 1, 7));
    }

    [Fact]
    public void Pending_apply_intent_owns_input_payload_and_repeated_begin_is_idempotent()
    {
        using var store = CreateStore();
        var batch = CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50));
        var token = Guid.Parse("00000000-0000-0000-0000-000000000125");

        store.BeginPendingApply(batch, token);
        batch.Operations[0].Payload[0] = 99;
        store.BeginPendingApply(batch, token);

        var intent = Assert.Single(store.ReadPendingApplyIntents("scope-a", Node(1)));
        Assert.Equal(50, intent.Batch.Operations[0].Payload[0]);
        Assert.Equal(0UL, store.GetReceiveCursor("scope-a", Node(1), Node(2), 1, 7));
    }

    protected static MetadataIncomingBatch CreateBatch(
        string scope,
        byte replica,
        byte sender,
        uint senderEpoch,
        int namespaceId,
        byte batchId,
        ulong cursorStart,
        ulong cursorEnd,
        byte previousDigest,
        byte digest,
        params MetadataOperation[] operations)
        => new()
        {
            ScopeId = scope,
            LocalReplica = Node(replica),
            SenderPeer = Node(sender),
            SenderEpoch = senderEpoch,
            NamespaceId = namespaceId,
            BatchId = Batch(batchId),
            CursorStart = cursorStart,
            CursorEnd = cursorEnd,
            PreviousBatchDigest = Bytes(previousDigest),
            BatchDigest = Bytes(digest),
            Operations = operations
        };

    protected static MetadataFrozenOperation FrozenOperation(byte change, byte payload, byte? fingerprint = null)
        => new()
        {
            ChangeId = Batch(change),
            Fingerprint = Bytes(fingerprint ?? payload),
            Payload = new[] { payload }
        };

    protected static MetadataOperation Operation(byte author, byte change, byte fingerprint, byte payload)
        => new()
        {
            AuthorNodeId = Node(author),
            ChangeId = Batch(change),
            Fingerprint = Bytes(fingerprint),
            Payload = new[] { payload },
            RelayEligible = true
        };

    protected static NodeId Node(byte seed) => NodeId.From(Bytes(seed));
    protected static BatchId Batch(byte seed) => BatchId.From(Bytes(seed));

    private sealed class IdempotentTestProvider
    {
        private readonly Dictionary<Guid, byte[]> _receipts = new();

        public int ApplyCount { get; private set; }

        public MetadataProviderApplyReceipt ApplyOnce(Guid token, MetadataIncomingBatch batch)
        {
            if (_receipts.TryGetValue(token, out var priorDigest))
            {
                if (!priorDigest.AsSpan().SequenceEqual(batch.BatchDigest))
                    throw new InvalidDataException("Provider apply token was reused for different input.");
                return new MetadataProviderApplyReceipt(token, priorDigest.ToArray());
            }

            _receipts.Add(token, batch.BatchDigest.ToArray());
            ApplyCount++;
            return new MetadataProviderApplyReceipt(token, batch.BatchDigest.ToArray());
        }
    }

    private static byte[] Bytes(byte seed)
    {
        var bytes = new byte[16];
        bytes[15] = seed;
        return bytes;
    }
}
