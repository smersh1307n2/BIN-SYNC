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
            node.PushOutbox(3, Rid(1), op));

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
    public void Apply_batch_rejects_mixed_author_stream_before_any_commit()
    {
        var sender = Fix.MakeNode(39);
        using var receiver = new Node(Fix.Nid(40), epoch: 1, nowFn: () => 40_000_000);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        Assert.True(sender.WriteField(3, Fix.Rid(2), 0x100, [2]));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;
        Assert.Equal(2, batch.Entries.Count);

        // Create a genuinely mixed-author wire batch. CreateDeltaBatch intentionally
        // normalizes operations to compact aliases, so mutate only after it has signed
        // the normal batch, then recompute both content-bound digests.
        var second = Ops.DecodeOp(batch.Entries[1].Payload).Op;
        switch (second)
        {
            case OpFieldWrite write:
                write.AuthorNodeId = Fix.Nid(99);
                break;
            case OpFieldTomb tomb:
                tomb.AuthorNodeId = Fix.Nid(99);
                break;
            case OpRecordTombstone recordTomb:
                recordTomb.AuthorNodeId = Fix.Nid(99);
                break;
            default:
                throw new InvalidOperationException("Unexpected test operation type.");
        }
        batch.Entries[1].Payload = Ops.EncodeOpFull(second);
        batch.RecordsRoot = Node.ComputeRecordsRoot(batch.Entries);
        batch.BatchDigest = Node.ComputeBatchDigest(batch.BatchId, batch.NamespaceId,
            batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
            batch.RecordsRoot, batch.PrevBatchDigest);

        var wireSecond = Assert.IsType<OpFieldWrite>(Ops.DecodeOp(batch.Entries[1].Payload).Op);
        Assert.True(wireSecond.FullAuthorMode);
        Assert.Equal(Fix.Nid(99), wireSecond.AuthorNodeId);
        Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, sender.NodeId));

        Assert.False(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(1)), out _));
        Assert.False(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(2)), out _));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Empty(receiver.Inbox);
        Assert.Empty(receiver.SeenChanges);
        Assert.Empty(receiver.PeerStates);
        Assert.Empty(receiver.LastStreamAuthor);
    }

    [Fact]
    public void Apply_batch_rejects_global_change_ledger_overflow_before_any_commit()
    {
        var sender = Fix.MakeNode(43);
        var receiver = new Node(Fix.Nid(44), epoch: 1, nowFn: () => 44_000_000,
            limits: new ResourceLimits { MaxGlobalChangeLedgerEntries = 1 });
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
    public void Apply_batch_does_not_count_global_duplicate_against_ledger_capacity()
    {
        var sender = Fix.MakeNode(54);
        using var receiver = new Node(Fix.Nid(55), epoch: 1, nowFn: () => 55_000_000);
        Transport.MakeSession(sender, receiver);

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        var first = sender.CreateDeltaBatch(receiver.NodeId, 3)!;
        Assert.Equal(1, receiver.ApplyBatch(first, sender.NodeId).Applied);
        var changeId = first.Entries[0].ChangeId;
        Assert.True(receiver.GlobalChangeLedger.ContainsKey((3, changeId)));
        receiver.Limits = new ResourceLimits { MaxGlobalChangeLedgerEntries = 1 };

        // Same ChangeId in the next contiguous author-stream batch, but a new
        // BatchId: this is a global operation duplicate, not an Inbox replay.
        var secondId = BatchId.From(Fix.Nid(56));
        var secondEntry = new OutboxEntry
        {
            NamespaceId = first.NamespaceId,
            Seq = 2,
            ChangeId = changeId,
            RecordId = first.Entries[0].RecordId,
            OpKind = first.Entries[0].OpKind,
            Payload = first.Entries[0].Payload.ToArray(),
            AuthorNodeId = first.Entries[0].AuthorNodeId,
            CreatedAt = first.Entries[0].CreatedAt
        };
        var root = Node.ComputeRecordsRoot(new List<OutboxEntry> { secondEntry });
        var digest = Node.ComputeBatchDigest(secondId, first.NamespaceId, first.IssuerNodeId,
            first.IssuerEpoch, 2, 2, root, first.BatchDigest);
        var second = new Batch(first.NamespaceId, first.SchemaId, first.SchemaVersion,
            first.MergePolicy, first.Flags, first.IssuerNodeId, first.IssuerEpoch,
            2, 2, secondId, first.BatchDigest, root, digest,
            new List<OutboxEntry> { secondEntry });

        var result = receiver.ApplyBatch(second, sender.NodeId);
        Assert.Equal(0, result.Applied);
        Assert.Equal(1, receiver.GetRecordCount(3));
        Assert.Equal(1, receiver.GetOutboxCount(3)); // first arrival was relayed exactly once
        Assert.Single(receiver.Outbox[3]);
        Assert.Single(receiver.GlobalChangeLedger);
        Assert.Empty(receiver.SeenChanges);
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
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new ResourceLimits { MaxGlobalChangeLedgerEntries = -1 }; });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new ResourceLimits { MaxRecords = -1 }; });

        var unlimited = new ResourceLimits
        {
            MaxOutboxEntriesPerNamespace = 0,
            MaxSeenChanges = 0,
            MaxGlobalChangeLedgerEntries = 0,
            MaxRecords = 0,
        };
        Assert.Equal(0, unlimited.MaxOutboxEntriesPerNamespace);
        Assert.Equal(0, unlimited.MaxSeenChanges);
        Assert.Equal(0, unlimited.MaxGlobalChangeLedgerEntries);
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
    public void BoundedNode_allows_update_when_record_limit_is_reached()
    {
        using var node = new Node(Fix.Nid(174));
        var limits = new ResourceLimits { MaxRecords = 1 };

        Assert.True(BoundedNode.WriteField(node, limits, 3, Rid(1), 0x100, [1]));
        Assert.True(BoundedNode.WriteField(node, limits, 3, Rid(1), 0x101, [2]));

        Assert.True(node.TryGetField(3, Rid(1), 0x101, out var field));
        Assert.Equal(new byte[] { 2 }, field!.Value);
        Assert.Equal(1, node.GetRecordCount(3));
    }

    [Fact]
    public void Resource_limit_preflight_helpers_are_not_public_api()
    {
        Assert.Null(typeof(ResourceLimits).GetMethod(nameof(ResourceLimits.EnsureCanEnqueueOutbox)));
        Assert.Null(typeof(ResourceLimits).GetMethod(nameof(ResourceLimits.EnsureSeenChangesCapacity)));
        Assert.Null(typeof(ResourceLimits).GetMethod(nameof(ResourceLimits.EnsureStorageCapacity)));
    }

    [Fact]
    public void BoundedNode_raises_field_written_after_releasing_node_gates()
    {
        using var node = new Node(Fix.Nid(173));
        var limits = new ResourceLimits();
        int events = 0;

        node.FieldWritten += (_, _, _) =>
        {
            if (Interlocked.Increment(ref events) == 1)
            {
                var writer = Task.Run(() =>
                    node.WriteField(3, Rid(3), 0x101, new byte[] { 2 }));
                Assert.True(writer.Wait(TimeSpan.FromSeconds(3)));
            }
        };

        Assert.True(BoundedNode.WriteField(node, limits, 3, Rid(3), 0x100, new byte[] { 1 }));
        Assert.Equal(2, events);
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


    [Fact]
    public void Apply_batch_does_not_surface_disposed_relay_peer_after_commit()
    {
        var sender = Fix.MakeNode(61);
        var receiver = Fix.MakeNode(62);
        var stalePeer = Fix.MakeNode(63);
        Transport.MakeSession(sender, receiver);
        Transport.MakeSession(stalePeer, receiver);

        // A peer handle can become stale independently of this receiver's session map.
        stalePeer.Dispose();

        Assert.True(sender.WriteField(3, Fix.Rid(1), 0x100, [1]));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;

        // Relay alias propagation is opportunistic control-plane work. It must not
        // turn an already accepted batch into an apparent failure.
        var result = receiver.ApplyBatch(batch, sender.NodeId);

        Assert.False(result.Duplicate);
        Assert.Equal(1, result.Applied);
        Assert.True(receiver.TryGetRecord(3, RecordId.From(Fix.Rid(1)), out _));
        Assert.Single(receiver.Inbox);
        Assert.Equal(1, receiver.GetOutboxCount(3));
    }

}
