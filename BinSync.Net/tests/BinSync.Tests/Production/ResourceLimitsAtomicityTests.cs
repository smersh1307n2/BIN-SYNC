using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class ResourceLimitsAtomicityTests
{
    private static RecordId Rid(byte value)
    {
        Span<byte> id = stackalloc byte[16];
        id.Clear();
        id[0] = value;
        return RecordId.From(id);
    }

    [Fact]
    public void Full_outbox_rejects_write_without_mutating_record()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxOutboxEntriesPerNamespace = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));
        Assert.Throws<SyncException>(() => node.WriteField(3, Rid(1), 0x101, [2]));

        Assert.False(node.TryGetField(3, Rid(1), 0x101, out _));
        Assert.Equal(1, node.GetOutboxCount(3));
        Assert.Equal(1, node.GetRecordCount(3));
    }

    [Fact]
    public void Rejected_direct_outbox_push_does_not_consume_author_sequence()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxOutboxEntriesPerNamespace = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));

        var op = new OpFieldWrite
        {
            FieldId = 0x101,
            KeyEpoch = 0,
            Wall = 2,
            Logical = 0,
            Origin = 1,
            AuthorNodeId = node.NodeId.ToArray(),
            Value = [2]
        };

        Assert.Throws<SyncException>(() =>
            node.PushOutbox(3, Rid(1), op, node.NodeId));

        // The next allocation must be exactly the next sequence after the
        // existing queued entry; the rejected push must not leave a hole.
        Assert.Equal(2, node.NextOutboxSeqInternal(node.NodeId, 3));
        Assert.Equal(1, node.GetOutboxCount(3));
    }

    [Fact]
    public void Max_records_does_not_block_updates_to_existing_record()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxRecords = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));
        Assert.True(node.WriteField(3, Rid(1), 0x101, [2]));

        Assert.True(node.TryGetField(3, Rid(1), 0x101, out var field));
        Assert.Equal(new byte[] { 2 }, field!.Value);
        Assert.Equal(1, node.GetRecordCount(3));
    }

    [Fact]
    public void Max_records_rejects_new_record_without_creating_empty_record()
    {
        using var node = new Node(limits: new ResourceLimits
        {
            MaxRecords = 1
        });

        Assert.True(node.WriteField(3, Rid(1), 0x100, [1]));
        Assert.Throws<SyncException>(() => node.WriteField(3, Rid(2), 0x100, [2]));

        Assert.False(node.TryGetRecord(3, Rid(2), out _));
        Assert.Equal(1, node.GetRecordCount(3));
    }
    [Fact]
    public void Apply_batch_rejects_full_relay_outbox_before_any_commit()
    {
        var sender = Fix.MakeNode(41);
        var receiver = new Node(Fix.Nid(42), epoch: 1, nowFn: () => 42_000_000,
            limits: new ResourceLimits { MaxOutboxEntriesPerNamespace = 1 });
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x101, [2]));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;

        Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, sender.NodeId));

        Assert.False(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(1)), out _));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Empty(receiver.Inbox);
        Assert.Empty(receiver.SeenChanges);
        Assert.Empty(receiver.PeerStates);
        Assert.Empty(receiver.LastStreamAuthor);

        // The exact same batch/cursor/digest remains applicable after capacity is freed.
        receiver.Limits = null;
        var applied = receiver.ApplyBatch(batch, sender.NodeId);
        Assert.Equal(2, applied.Applied);
        Assert.True(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(1)), out _));
        Assert.Equal(2, receiver.GetOutboxCount(3));
    }

    [Fact]
    public void Apply_batch_rejects_seen_change_overflow_before_any_commit()
    {
        var sender = Fix.MakeNode(43);
        var receiver = new Node(Fix.Nid(44), epoch: 1, nowFn: () => 44_000_000,
            limits: new ResourceLimits { MaxSeenChanges = 1 });
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        Assert.True(sender.WriteField(3, Fix.Rid(2), 0x100, [2]));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;

        Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, sender.NodeId));

        Assert.False(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(1)), out _));
        Assert.False(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(2)), out _));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Empty(receiver.Inbox);
        Assert.Empty(receiver.SeenChanges);
        Assert.Empty(receiver.PeerStates);
    }

    [Fact]
    public void Apply_batch_rejects_record_limit_for_whole_batch_atomically()
    {
        var sender = Fix.MakeNode(45);
        var receiver = new Node(Fix.Nid(46), epoch: 1, nowFn: () => 46_000_000,
            limits: new ResourceLimits { MaxRecords = 1 });
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        Assert.True(sender.WriteField(3, Fix.Rid(2), 0x100, [2]));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;

        Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, sender.NodeId));

        Assert.Equal(0, receiver.GetRecordCount(3));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Empty(receiver.Inbox);
        Assert.Empty(receiver.SeenChanges);
        Assert.Empty(receiver.PeerStates);
    }


    [Fact]
    public void Apply_batch_rejects_duplicate_nonzero_change_ids_before_commit()
    {
        var sender = Fix.MakeNode(51);
        using var receiver = new Node(Fix.Nid(52), epoch: 1, nowFn: () => 52_000_000);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        Assert.True(sender.WriteField(3, Fix.Rid(2), 0x100, [2]));
        var duplicateId = BatchId.From(Fix.Nid(53));
        sender.Outbox[3][^2].ChangeId = duplicateId;
        sender.Outbox[3][^1].ChangeId = duplicateId;
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;

        Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, sender.NodeId));
        Assert.Equal(0, receiver.GetRecordCount(3));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Empty(receiver.Inbox);
        Assert.Empty(receiver.SeenChanges);
        Assert.Empty(receiver.PeerStates);
    }

    [Fact]
    public void Apply_batch_does_not_count_already_seen_change_ids_against_capacity()
    {
        var sender = Fix.MakeNode(54);
        using var receiver = new Node(Fix.Nid(55), epoch: 1, nowFn: () => 55_000_000,
            limits: new ResourceLimits { MaxSeenChanges = 1 });
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        var changeId = BatchId.From(Fix.Nid(56));
        sender.Outbox[3][^1].ChangeId = changeId;
        receiver.SeenChanges.Add((sender.NodeId, 3, changeId));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;

        var result = receiver.ApplyBatch(batch, sender.NodeId);
        Assert.Equal(1, result.Applied);
        Assert.Equal(1, receiver.GetRecordCount(3));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Single(receiver.SeenChanges);
    }

    [Fact]
    public void Apply_batch_rejects_outbox_sequence_exhaustion_before_commit()
    {
        var sender = Fix.MakeNode(57);
        using var receiver = new Node(Fix.Nid(58), epoch: 1, nowFn: () => 58_000_000);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;
        receiver._outboxSeq[(sender.NodeId, 3)] = long.MaxValue;

        Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, sender.NodeId));

        Assert.Equal(0, receiver.GetRecordCount(3));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Empty(receiver.Inbox);
        Assert.Empty(receiver.SeenChanges);
        Assert.Empty(receiver.PeerStates);
        Assert.Equal(long.MaxValue, receiver._outboxSeq[(sender.NodeId, 3)]);
    }
    [Fact]
    public void Resource_limits_reject_negative_caps_but_allow_zero_as_unlimited()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new ResourceLimits { MaxOutboxEntriesPerNamespace = -1 }; });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new ResourceLimits { MaxSeenChanges = -1 }; });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new ResourceLimits { MaxRecords = -1 }; });

        var unlimited = new ResourceLimits
        {
            MaxOutboxEntriesPerNamespace = 0,
            MaxSeenChanges = 0,
            MaxRecords = 0,
        };
        Assert.Equal(0, unlimited.MaxOutboxEntriesPerNamespace);
        Assert.Equal(0, unlimited.MaxSeenChanges);
        Assert.Equal(0, unlimited.MaxRecords);
    }

    [Fact]
    public void BoundedNode_enforces_seen_change_limit()
    {
        using var node = new Node(Fix.Nid(171));
        var limits = new ResourceLimits { MaxSeenChanges = 1 };

        Assert.True(BoundedNode.WriteField(node, limits, 3, Rid(1), 0x100, [1]));
        Assert.Throws<SyncException>(() =>
            BoundedNode.WriteField(node, limits, 3, Rid(1), 0x101, [2]));
        Assert.Equal(1, node.GetOutboxCount(3));
        Assert.Single(node.SeenChanges);
    }

    [Fact]
    public void BoundedNode_preflight_is_atomic_with_the_write()
    {
        using var node = new Node(Fix.Nid(172));
        var limits = new ResourceLimits
        {
            MaxRecords = 1,
            MaxOutboxEntriesPerNamespace = 0,
        };
        var gates = node.EnterAllGates();
        Exception? writeError = null;
        var worker = new Thread(() =>
        {
            try { BoundedNode.WriteField(node, limits, 3, Rid(1), 0x100, [1]); }
            catch (Exception ex) { writeError = ex; }
        }) { IsBackground = true };

        worker.Start();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while ((worker.ThreadState & ThreadState.WaitSleepJoin) == 0 &&
               stopwatch.Elapsed < TimeSpan.FromSeconds(3))
            Thread.Sleep(1);
        Assert.True((worker.ThreadState & ThreadState.WaitSleepJoin) != 0,
            "Bounded write did not block on the node gate.");

        // This write wins while the bounded writer is waiting. The bounded preflight
        // must run after the gate is acquired, so it must observe this record.
        Assert.True(node.WriteField(3, Rid(2), 0x100, [2]));
        gates.Dispose();

        Assert.True(worker.Join(TimeSpan.FromSeconds(3)));
        Assert.IsType<SyncException>(writeError);
        Assert.False(node.TryGetRecord(3, Rid(1), out _));
        Assert.Equal(1, node.GetRecordCount(3));
    }

}
