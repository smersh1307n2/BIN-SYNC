using System.Buffers.Binary;
using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class RemoteBatchWalAtomicityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Remote_batch_wal_failure_does_not_publish_any_effects(bool usePayloadApi)
    {
        using var sender = Fix.MakeNode(961);
        using var receiver = Fix.MakeNode(962);
        Transport.MakeSession(sender, receiver);

        var recordId = Fix.Rid(961);
        Assert.True(sender.WriteField(3, recordId, 0x100, [0xA5]));
        var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;
        var payload = usePayloadApi ? sender.EncodeBatchPayload(batch) : null;

        using var wal = Wal.Open(Path.Combine(Path.GetTempPath(),
            "binsync-remote-wal-failure-" + Guid.NewGuid().ToString("N") + ".wal"));
        receiver.AttachWal(wal);
        wal.Dispose();

        if (usePayloadApi)
            Assert.Throws<ObjectDisposedException>(() => receiver.ApplyBatchPayload(payload!, sender.NodeId));
        else
            Assert.Throws<ObjectDisposedException>(() => receiver.ApplyBatch(batch, sender.NodeId));

        Assert.False(receiver.TryGetRecord(3, recordId, out _));
        Assert.Equal(0, receiver.GetRecordCount(3));
        Assert.Equal(0, receiver.GetOutboxCount(3));
        Assert.Empty(receiver.Inbox);
        Assert.Empty(receiver.InboxAuthors);
        Assert.Empty(receiver.SeenChanges);
        Assert.Empty(receiver.PeerStates);
        Assert.Empty(receiver.LastStreamAuthor);
        Assert.Empty(receiver.LastBatchDigest);
    }

    [Fact]
    public void Wal_replay_semantic_failure_does_not_publish_earlier_local_mutations()
    {
        using var writer = Fix.MakeNode(963);
        using var sender = Fix.MakeNode(964);
        using var target = new Node(writer.NodeId, writer.Epoch);
        Transport.MakeSession(sender, target);

        var walPath = Path.Combine(Path.GetTempPath(),
            "binsync-wal-replay-atomic-" + Guid.NewGuid().ToString("N") + ".wal");
        using var wal = Wal.Open(walPath);
        writer.AttachWal(wal);

        var localRecordId = Fix.Rid(963);
        Assert.True(writer.WriteField(3, localRecordId, 0x100, [0x51]));

        var remoteRecordId = Fix.Rid(964);
        Assert.True(sender.WriteField(3, remoteRecordId, 0x100, [0xA4]));
        var batch = sender.CreateDeltaBatch(target.NodeId, 3)!;
        var invalidPayload = sender.EncodeBatchPayload(batch);
        // Batch is immutable; alter only the wire cursor while keeping the frame
        // structurally valid so semantic replay rejects it with SEQ_GAP.
        BinaryPrimitives.WriteUInt64BigEndian(invalidPayload.AsSpan(28, 8), 2);
        BinaryPrimitives.WriteUInt64BigEndian(invalidPayload.AsSpan(36, 8), 2);
        wal.AppendRemoteBatch(sender.NodeId, sender.Epoch,
            Array.Empty<AliasMapEntry>(), invalidPayload);

        var clockBefore = target.ClockState;
        Assert.Throws<SyncException>(() => wal.Replay(target));

        Assert.False(target.TryGetRecord(3, localRecordId, out _));
        Assert.False(target.TryGetRecord(3, remoteRecordId, out _));
        Assert.Equal(0, target.GetRecordCount(3));
        Assert.Equal(0, target.GetOutboxCount(3));
        Assert.Empty(target.Inbox);
        Assert.Empty(target.SeenChanges);
        Assert.Empty(target.PeerStates);
        Assert.Equal(clockBefore, target.ClockState);
        Assert.Equal(0, target.GetRecordCount(3));
    }

    [Fact]
    public void ApplyBatch_rejects_noncanonical_lifecycle_operations_before_publication()
    {
        object[] invalidOperations =
        {
            new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = 100, Logical = 0, Origin = 1, Value = Array.Empty<byte>() },
            new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = 100, Logical = 0, Origin = 1, Value = new byte[] { Model.Alive, 0xFF } },
            new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = 100, Logical = 0, Origin = 1, Value = new byte[] { 0x02 } },
            new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 1, Wall = 100, Logical = 0, Origin = 1, Value = new byte[] { Model.Alive } },
            new OpFieldTomb { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = 100, Logical = 0, Origin = 1 },
        };

        for (int i = 0; i < invalidOperations.Length; i++)
        {
            using var sender = Fix.MakeNode(980 + i);
            using var receiver = Fix.MakeNode(990 + i);
            Transport.MakeSession(sender, receiver);
            var recordId = Fix.Rid(980 + i);
            Assert.True(sender.WriteField(3, recordId, 0x0100, new byte[] { 0xAA }));
            var batch = sender.CreateDeltaBatch(receiver.NodeId, 3)!;
            var operation = invalidOperations[i];
            batch.Entries[0].Payload = Ops.EncodeOp(operation);
            batch.Entries[0].OpKind = operation is OpFieldTomb
                ? OpsConst.TagFieldTomb
                : OpsConst.TagFieldWrite;

            var error = Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, sender.NodeId));
            Assert.Equal("BATCH_TRANSACTION_FAILED", error.CodeName);
            Assert.False(receiver.TryGetRecord(3, recordId, out _));
            Assert.Empty(receiver.Inbox);
        }
    }

    [Fact]
    public void Wal_replay_success_is_idempotent_and_preserves_runtime_peer_handles()
    {
        using var writer = Fix.MakeNode(965);
        using var peer = Fix.MakeNode(966);
        using var target = new Node(writer.NodeId, writer.Epoch);
        target.RegisterLoopbackPeer(peer);

        var walPath = Path.Combine(Path.GetTempPath(),
            "binsync-wal-replay-success-" + Guid.NewGuid().ToString("N") + ".wal");
        using var wal = Wal.Open(walPath);
        writer.AttachWal(wal);

        var recordId = Fix.Rid(965);
        Assert.True(writer.WriteField(3, recordId, 0x100, new byte[] { 0xB5 }));

        Assert.Equal(1, wal.Replay(target));
        Assert.True(target.TryGetField(3, recordId, 0x100, out var field));
        Assert.Equal(new byte[] { 0xB5 }, field!.Value);
        Assert.Single(target.Outbox[3]);
        Assert.True(target.PeersByNid!.TryGetValue(peer.NodeId, out var restoredPeer));
        Assert.Same(peer, restoredPeer);

        Assert.Equal(0, wal.Replay(target));
        Assert.Equal(1, target.GetOutboxCount(3));
        Assert.True(target.TryGetField(3, recordId, 0x100, out field));
        Assert.Equal(new byte[] { 0xB5 }, field!.Value);
        Assert.True(target.PeersByNid.TryGetValue(peer.NodeId, out restoredPeer));
        Assert.Same(peer, restoredPeer);
    }

    [Fact]
    public void Wal_replay_accounts_for_remote_relay_sequences_before_later_local_mutations()
    {
        using var origin = Fix.MakeNode(967);
        using var relay = Fix.MakeNode(968);
        using var receiver = new Node(origin.NodeId, origin.Epoch);

        Transport.MakeSession(origin, relay);
        var relayedRecordId = Fix.Rid(967);
        Assert.True(origin.WriteField(3, relayedRecordId, 0x100, new byte[] { 0xC1 }));
        var originBatch = origin.CreateDeltaBatch(relay.NodeId, 3)!;
        Assert.Equal(1, relay.ApplyBatch(originBatch, origin.NodeId).Applied);

        Transport.MakeSession(relay, receiver);
        var forwardedBatch = relay.CreateDeltaBatch(receiver.NodeId, 3, origin.NodeId)!;
        var walPath = Path.Combine(Path.GetTempPath(),
            "binsync-wal-relay-sequence-" + Guid.NewGuid().ToString("N") + ".wal");
        using var wal = Wal.Open(walPath);
        receiver.AttachWal(wal);

        Assert.Equal(1, receiver.ApplyBatch(forwardedBatch, relay.NodeId).Applied);
        Assert.Single(receiver.Outbox[3]);
        Assert.Equal(1, receiver.Outbox[3][0].Seq);

        var localRecordId = Fix.Rid(969);
        Assert.True(receiver.WriteField(3, localRecordId, 0x101, new byte[] { 0xC2 }));
        Assert.Equal(2, receiver.Outbox[3][1].Seq);

        using var recovered = new Node(receiver.NodeId, receiver.Epoch);
        Assert.Equal(2, wal.Replay(recovered));
        Assert.Equal(new long[] { 1, 2 }, recovered.Outbox[3].Select(entry => entry.Seq).ToArray());
        Assert.True(recovered.TryGetRecord(3, relayedRecordId, out _));
        Assert.True(recovered.TryGetRecord(3, localRecordId, out _));
        Assert.True(recovered.PeersByNid!.TryGetValue(relay.NodeId, out var peerHandle));
        Assert.Null(peerHandle);
        Assert.True(recovered.PeerStates.ContainsKey((relay.NodeId, 3)));

        // A reconnect must recognize the replay-created placeholder as a resumed
        // peer; otherwise MakeSession would reset the receive cursor and digest chain.
        Transport.MakeSession(relay, recovered);
        Assert.True(recovered.PeerStates.ContainsKey((relay.NodeId, 3)));
        Assert.NotEmpty(recovered.LastBatchDigest);
    }

}
