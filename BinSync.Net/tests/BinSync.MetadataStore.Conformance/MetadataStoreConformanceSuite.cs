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
    public void Adapter_must_declare_its_durability_profile()
    {
        using var store = CreateStore();
        Assert.False(store.SupportsRestartDurability);
    }

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
    public void Relay_ack_is_per_author_and_cannot_exceed_that_author_high_watermark()
    {
        using var store = CreateStore();
        store.AcceptIncomingBatch(CreateBatch("scope-a", 1, 2, 1, 7, 10, 1, 1, 0, 10,
            Operation(20, 30, 40, 50),
            Operation(21, 31, 41, 51)));

        store.AcknowledgeRelay("scope-a", Node(1), 7, Node(20), 1);

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

    private static byte[] Bytes(byte seed)
    {
        var bytes = new byte[16];
        bytes[15] = seed;
        return bytes;
    }
}
