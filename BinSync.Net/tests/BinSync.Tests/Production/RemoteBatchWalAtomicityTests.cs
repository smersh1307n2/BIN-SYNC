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
}
