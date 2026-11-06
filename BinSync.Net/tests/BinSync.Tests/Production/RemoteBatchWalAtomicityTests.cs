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

}
