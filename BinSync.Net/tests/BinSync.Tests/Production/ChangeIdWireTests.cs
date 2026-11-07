using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ChangeIdWireTests
{
    private static byte[] Id(byte value)
    {
        var bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    [Fact]
    public void Batch_payload_roundtrip_preserves_change_id_and_binds_it_to_records_root()
    {
        var node = new Node(Id(1));
        var changeId = BatchId.From(Id(0xA1));
        var entry = new OutboxEntry
        {
            NamespaceId = 7,
            Seq = 1,
            ChangeId = changeId,
            RecordId = RecordId.From(Id(0x22)),
            OpKind = 0x01,
            Payload = new byte[] { 0x10, 0x20, 0x30 },
            AuthorNodeId = NodeId.From(Id(1)),
        };
        var entries = new List<OutboxEntry> { entry };
        var root = Node.ComputeRecordsRoot(entries);
        var batchId = BatchId.From(Id(0xB1));
        var issuer = NodeId.From(Id(1));
        var digest = Node.ComputeBatchDigest(batchId, 7, issuer, 1, 1, 1, root, default);
        var batch = new Batch(7, 0, 1, 0x02, 0, issuer, 1, 1, 1,
            batchId, default, root, digest, entries);

        var encoded = node.EncodeBatchPayload(batch);
        var decoded = node.DecodeBatchPayload(encoded);

        Assert.Equal(changeId, decoded.Entries[0].ChangeId);
        Assert.Equal(root, Node.ComputeRecordsRoot(decoded.Entries));
        Assert.Equal(digest, decoded.BatchDigest);

        var changedIdEntry = new OutboxEntry
        {
            NamespaceId = entry.NamespaceId,
            ChangeId = BatchId.From(Id(0xA2)),
            RecordId = entry.RecordId,
            OpKind = entry.OpKind,
            Payload = entry.Payload,
            AuthorNodeId = entry.AuthorNodeId,
        };
        var changedRoot = Node.ComputeRecordsRoot(new List<OutboxEntry> { changedIdEntry });
        Assert.NotEqual(root, changedRoot);

        var (seed, publicKey) = BatchSignature.GenerateKeyPair();
        var signature = BatchSignature.SignBatch(seed, batch);
        Assert.True(BatchSignature.VerifyBatch(publicKey, batch, signature));
        var changedDigest = Node.ComputeBatchDigest(batch.BatchId, batch.NamespaceId,
            batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
            changedRoot, batch.PrevBatchDigest);
        var changedBatch = new Batch(batch.NamespaceId, batch.SchemaId, batch.SchemaVersion,
            batch.MergePolicy, batch.Flags, batch.IssuerNodeId, batch.IssuerEpoch,
            batch.CursorStart, batch.CursorEnd, batch.BatchId, batch.PrevBatchDigest,
            changedRoot, changedDigest, new List<OutboxEntry> { changedIdEntry });
        Assert.False(BatchSignature.VerifyBatch(publicKey, changedBatch, signature));
    }

    [Fact]
    public void Records_array_roundtrip_includes_change_id_in_wire_layout()
    {
        var entry = new OutboxEntry
        {
            ChangeId = BatchId.From(Id(0xCC)),
            RecordId = RecordId.From(Id(0x11)),
            OpKind = 3,
            Payload = new byte[] { 4, 5 },
        };

        var bytes = Node.EncodeRecordsArray(new List<OutboxEntry> { entry });
        var decoded = Node.DecodeRecordsArray(bytes);

        Assert.Single(decoded);
        Assert.Equal(entry.RecordId, decoded[0].Rid);
        Assert.Equal(entry.ChangeId, decoded[0].ChangeId);
        Assert.Equal(entry.OpKind, decoded[0].Kind);
        Assert.Equal(entry.Payload.ToArray(), decoded[0].Payload);
        Assert.Throws<InvalidDataException>(() => Node.DecodeRecordsArray(bytes.Append((byte)0).ToArray()));
    }
    [Fact]
    public void Change_id_survives_A_to_B_to_C_to_A_and_duplicate_arrival_from_second_peer()
    {
        var a = Fix.MakeNode(41);
        var b = Fix.MakeNode(42);
        var c = Fix.MakeNode(43);

        // Establish all three sessions before the write so A retains its outbox
        // entry until both B and C have acknowledged it.
        Transport.MakeSession(a, b);
        Transport.MakeSession(b, c);
        Transport.MakeSession(a, c);

        Assert.True(a.WriteField(1, Fix.Rid(0x41), 0x100, new byte[] { 0xCA, 0xFE }));
        var originalChangeId = Assert.Single(a.Outbox[1]).ChangeId;
        Assert.False(originalChangeId.IsZero);

        // A -> B: first network hop.
        Assert.NotNull(Transport.DeliverBatch(a, b, 1));
        var bRelay = Assert.Single(b.Outbox[1]);
        Assert.Equal(originalChangeId, bRelay.ChangeId);

        // B -> C: second hop. The issuer changes to B, but ChangeId must not.
        Assert.NotNull(Transport.DeliverBatch(b, c, 1));
        var cRelay = Assert.Single(c.Outbox[1]);
        Assert.Equal(originalChangeId, cRelay.ChangeId);

        // A -> C: the same logical mutation reaches C through a second peer.
        // It must be applied idempotently and must not be queued for relay twice.
        Assert.NotNull(Transport.DeliverBatch(a, c, 1));
        Assert.Single(c.Outbox[1]);
        Assert.Equal(originalChangeId, c.Outbox[1][0].ChangeId);
        Assert.Contains((a.NodeId, 1, originalChangeId), c.SeenChanges);

        // C -> A closes the mesh loop. A already owns this ChangeId, so it must
        // not create a second outbox item or invent a replacement identity.
        Assert.NotNull(Transport.DeliverBatch(c, a, 1));
        Assert.False(a.Outbox.TryGetValue(1, out var returnedEntries) &&
                     returnedEntries.Any(e => e.ChangeId == originalChangeId));
        Assert.Contains((c.NodeId, 1, originalChangeId), a.SeenChanges);

        Assert.True(a.Storage[1].ContainsKey(Fix.Rid(0x41)));
        Assert.True(b.Storage[1].ContainsKey(Fix.Rid(0x41)));
        Assert.True(c.Storage[1].ContainsKey(Fix.Rid(0x41)));
    }

    [Fact]
    public void Snapshot_apply_rejects_zero_or_duplicate_change_ids_before_mutating_storage()
    {
        var source = Fix.MakeNode(51);
        var target = Fix.MakeNode(52);
        Assert.True(source.WriteField(1, Fix.Rid(0x51), 0x100, new byte[] { 7 }));
        Assert.True(source.WriteField(1, Fix.Rid(0x51), 0x101, new byte[] { 8 }));
        var snapshot = Assert.Single(source.BuildSnapshot(1));

        var zeroIdBatch = new Batch(snapshot.NamespaceId, snapshot.SchemaId, snapshot.SchemaVersion,
            snapshot.MergePolicy, snapshot.Flags, snapshot.IssuerNodeId, snapshot.IssuerEpoch,
            snapshot.CursorStart, snapshot.CursorEnd, snapshot.BatchId, snapshot.PrevBatchDigest,
            snapshot.RecordsRoot, snapshot.BatchDigest,
            snapshot.Entries.Select(e => new OutboxEntry
            {
                NamespaceId = e.NamespaceId, Seq = e.Seq, ChangeId = default,
                RecordId = e.RecordId, OpKind = e.OpKind, Payload = e.Payload,
                AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt
            }).ToList());

        Assert.Throws<SyncException>(() => target.ApplySnapshot(zeroIdBatch));
        Assert.Empty(target.Storage);

        var tamperedEntries = snapshot.Entries.Select((e, i) => new OutboxEntry
        {
            NamespaceId = e.NamespaceId, Seq = e.Seq,
            ChangeId = i == 0 ? BatchId.From(Id(0x77)) : e.ChangeId,
            RecordId = e.RecordId, OpKind = e.OpKind, Payload = e.Payload,
            AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt
        }).ToList();
        var tamperedBatch = new Batch(snapshot.NamespaceId, snapshot.SchemaId, snapshot.SchemaVersion,
            snapshot.MergePolicy, snapshot.Flags, snapshot.IssuerNodeId, snapshot.IssuerEpoch,
            snapshot.CursorStart, snapshot.CursorEnd, snapshot.BatchId, snapshot.PrevBatchDigest,
            snapshot.RecordsRoot, snapshot.BatchDigest, tamperedEntries);
        Assert.Throws<SyncException>(() => target.ApplySnapshot(tamperedBatch));
        Assert.Empty(target.Storage);

        var duplicateEntries = snapshot.Entries.Select(e => new OutboxEntry
        {
            NamespaceId = e.NamespaceId, Seq = e.Seq, ChangeId = snapshot.Entries[0].ChangeId,
            RecordId = e.RecordId, OpKind = e.OpKind, Payload = e.Payload,
            AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt
        }).ToList();
        var duplicateBatch = new Batch(snapshot.NamespaceId, snapshot.SchemaId, snapshot.SchemaVersion,
            snapshot.MergePolicy, snapshot.Flags, snapshot.IssuerNodeId, snapshot.IssuerEpoch,
            snapshot.CursorStart, snapshot.CursorEnd, snapshot.BatchId, snapshot.PrevBatchDigest,
            snapshot.RecordsRoot, snapshot.BatchDigest, duplicateEntries);

        Assert.Throws<SyncException>(() => target.ApplySnapshot(duplicateBatch));
        Assert.Empty(target.Storage);
    }

    private static void Rehash(Batch batch)
    {
        batch.RecordsRoot = Node.ComputeRecordsRoot(batch.Entries);
        batch.BatchDigest = Node.ComputeBatchDigest(batch.BatchId, batch.NamespaceId,
            batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
            batch.RecordsRoot, batch.PrevBatchDigest);
    }

    [Fact]
    public void ApplySnapshot_rejects_op_kind_that_disagrees_with_payload()
    {
        using var source = Fix.MakeNode(101);
        using var target = Fix.MakeNode(102);
        Assert.True(source.WriteField(1, Fix.Rid(0x65), 0x100, new byte[] { 1 }));
        var batch = Assert.Single(source.BuildSnapshot(1));
        batch.Entries[0].OpKind = OpsConst.TagFieldTomb;
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplySnapshot(batch));
        Assert.Empty(target.Storage);
    }

    [Fact]
    public void ApplySnapshot_rejects_record_tombstone_with_different_envelope_record_id()
    {
        using var source = Fix.MakeNode(103);
        using var target = Fix.MakeNode(104);
        var recordId = Fix.Rid(0x66);
        Assert.True(source.WriteField(1, recordId, 0x100, new byte[] { 1 }));
        Assert.True(source.DeleteRecord(1, recordId));
        var batch = Assert.Single(source.BuildSnapshot(1));
        var tombstone = Assert.Single(batch.Entries, e =>
            Ops.DecodeOp(e.Payload).Op is OpRecordTombstone);
        tombstone.RecordId = RecordId.From(Id(0xFE));
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplySnapshot(batch));
        Assert.Empty(target.Storage);
    }

    [Fact]
    public void ApplyBatch_rejects_op_kind_that_disagrees_with_payload_before_mutation()
    {
        using var source = Fix.MakeNode(105);
        using var target = Fix.MakeNode(106);
        Transport.MakeSession(source, target);
        Assert.True(source.WriteField(1, Fix.Rid(0x67), 0x100, new byte[] { 1 }));
        var batch = source.CreateDeltaBatch(target.NodeId, 1)!;
        batch.Entries[0].OpKind = OpsConst.TagFieldTomb;
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplyBatch(batch, source.NodeId));
        Assert.Empty(target.Storage);
        Assert.Equal(0, target.GetRecordCount(1));
    }

    [Fact]
    public void ApplyBatch_rejects_record_tombstone_with_different_envelope_record_id_before_mutation()
    {
        using var source = Fix.MakeNode(107);
        using var target = Fix.MakeNode(108);
        Transport.MakeSession(source, target);
        var recordId = Fix.Rid(0x68);
        Assert.True(source.WriteField(1, recordId, 0x100, new byte[] { 1 }));
        Assert.True(source.DeleteRecord(1, recordId));
        var batch = source.CreateDeltaBatch(target.NodeId, 1)!;
        var tombstone = Assert.Single(batch.Entries, e =>
            Ops.DecodeOp(e.Payload).Op is OpRecordTombstone);
        tombstone.RecordId = RecordId.From(Id(0xFD));
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplyBatch(batch, source.NodeId));
        Assert.Empty(target.Storage);
        Assert.Equal(0, target.GetRecordCount(1));
    }

    [Fact]
    public void Ack_cannot_advance_beyond_sent_stream_or_accept_another_author()
    {
        var a = Fix.MakeNode(61);
        var b = Fix.MakeNode(62);
        Transport.MakeSession(a, b);
        Assert.True(a.WriteField(1, Fix.Rid(0x61), 0x100, new byte[] { 1 }));

        var batch = a.CreateDeltaBatch(b.NodeId, 1)!;
        a.SignBatchFor(batch, b.NodeId);
        a.AdvanceStream(b.NodeId, 1, batch);

        Assert.Throws<SyncException>(() =>
            a.ProcessAck(b.NodeId, 1, batch.CursorEnd + 100, batch.DurableAuthor));
        Assert.Throws<SyncException>(() =>
            a.ProcessAck(b.NodeId, 1, batch.CursorEnd, NodeId.From(Id(0xEE))));
        Assert.Equal(1, a.OutboxSize(1));

        a.ProcessAck(b.NodeId, 1, batch.CursorEnd, batch.DurableAuthor);
        Assert.Equal(0, a.OutboxSize(1));
    }

    [Fact]
    public void Ack_watermark_and_pushed_stream_survive_restart_and_duplicate_ack_is_idempotent()
    {
        var a = Fix.MakeNode(63);
        var b = Fix.MakeNode(64);
        Transport.MakeSession(a, b);
        Assert.True(a.WriteField(1, Fix.Rid(0x63), 0x100, new byte[] { 2 }));

        var batch = a.CreateDeltaBatch(b.NodeId, 1)!;
        a.SignBatchFor(batch, b.NodeId);
        a.AdvanceStream(b.NodeId, 1, batch);
        var peerState = a.PeerStates[(b.NodeId, 1)];
        Assert.Single(peerState.Ids);
        var streamCheckpointKey = ((a.NodeId, 1, batch.IssuerEpoch), b.NodeId);
        var sentDigestCheckpoint = a.LastStreamDigest[streamCheckpointKey];

        a.ProcessAck(b.NodeId, 1, batch.CursorEnd, batch.DurableAuthor);

        // The ACK is contiguous and covers the cached range: release its replay
        // mapping, but retain the independent digest-chain checkpoint.
        Assert.Empty(peerState.Ids);
        Assert.Equal(sentDigestCheckpoint, a.LastStreamDigest[streamCheckpointKey]);

        var path = Path.Combine(Path.GetTempPath(), $"binsync-ack-{Guid.NewGuid():N}.bin");
        try
        {
            NodePersistence.Save(a, path);
            var restored = NodePersistence.Load(path);
            Assert.Equal(batch.CursorEnd, restored.PeerStates[(b.NodeId, 1)].StreamPushed[batch.DurableAuthor]);
            Assert.Equal(batch.CursorEnd, restored.PeerStates[(b.NodeId, 1)].AckedFor(batch.DurableAuthor));
            Assert.Empty(restored.PeerStates[(b.NodeId, 1)].Ids);
            restored.ProcessAck(b.NodeId, 1, batch.CursorEnd, batch.DurableAuthor);
            Assert.Empty(restored.PeerStates[(b.NodeId, 1)].Ids);
            Assert.Equal(0, restored.OutboxSize(1));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Duplicate_batch_ack_uses_original_author_after_another_author_stream_arrives()
    {
        var author = Fix.MakeNode(65);
        var relay = Fix.MakeNode(66);
        var receiver = Fix.MakeNode(67);
        Transport.MakeSession(author, relay);
        Assert.True(author.WriteField(1, Fix.Rid(0x65), 0x100, new byte[] { 1 }));
        Transport.DeliverBatch(author, relay, 1);
        Transport.MakeSession(relay, receiver);

        var firstBatch = relay.CreateDeltaBatch(receiver.NodeId, 1,
            filterAuthorNodeId: author.NodeId.ToArray())!;
        relay.SignBatchFor(firstBatch, receiver.NodeId);
        var firstPayload = relay.EncodeBatchPayload(firstBatch);
        var firstAccepted = receiver.ApplyBatchPayload(firstPayload, relay.NodeId);
        relay.AdvanceStream(receiver.NodeId, 1, firstBatch);
        Assert.Equal(author.NodeId, firstAccepted.DurableAuthor);

        Assert.True(relay.WriteField(1, Fix.Rid(0x66), 0x100, new byte[] { 2 }));
        var secondBatch = relay.CreateDeltaBatch(receiver.NodeId, 1,
            filterAuthorNodeId: relay.NodeId.ToArray())!;
        relay.SignBatchFor(secondBatch, receiver.NodeId);
        var secondAccepted = receiver.ApplyBatchPayload(relay.EncodeBatchPayload(secondBatch), relay.NodeId);
        relay.AdvanceStream(receiver.NodeId, 1, secondBatch);
        relay.ProcessAck(receiver.NodeId, 1, secondAccepted.CursorEnd, secondAccepted.DurableAuthor);
        Assert.Equal(relay.NodeId, receiver.LastStreamAuthor[(relay.NodeId, 1)]);

        var replay = receiver.ApplyBatchPayload(firstPayload, relay.NodeId);
        Assert.True(replay.Duplicate);
        Assert.Equal(author.NodeId, replay.DurableAuthor);
        relay.ProcessAck(receiver.NodeId, 1, replay.CursorEnd, replay.DurableAuthor);
        Assert.Equal(firstBatch.CursorEnd, relay.PeerStates[(receiver.NodeId, 1)].AckedFor(author.NodeId));
    }

}
