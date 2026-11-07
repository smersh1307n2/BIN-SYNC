using System.Text;
using BinSync;
using BinSync.Storage;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageSyncCoordinatorTests
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-coordinator-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void Failed_node_checkpoint_does_not_ack_feed_and_replay_does_not_duplicate_outbox()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 401, deleteOnDispose: true);
        Node node = NewNode(4);
        RecordId recordId = provider.ResolveRecordId(Guid.Parse("40000000-0000-0000-0000-000000000004").ToByteArray());
        ushort fieldId = provider.ResolveFieldId("value"u8.ToArray());
        provider.AppendLocalChange(new ProviderMutation
        {
            RecordId = recordId,
            FieldId = fieldId,
            AuthorNodeId = node.NodeId,
            HlcWall = 1000,
            HlcLogical = 2,
            KeyEpoch = 0,
            Value = Encoding.UTF8.GetBytes("durable")
        });
        var coordinator = new StorageSyncCoordinator(node, provider, 1);

        Assert.Throws<IOException>(() => coordinator.PublishLocalChanges(_ => throw new IOException("simulated checkpoint failure")));
        Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        Assert.Equal(1, node.GetOutboxCount(1));

        int published = coordinator.PublishLocalChanges(_ => { });
        Assert.Equal(1, published);
        Assert.Equal(1, node.GetOutboxCount(1));
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }

    [Fact]
    public void Remote_node_materialization_is_durable_but_does_not_create_local_feed_entries()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 402, deleteOnDispose: true);
        Node remoteNode = NewNode(5);
        RecordId recordId = RecordId.From(Guid.Parse("50000000-0000-0000-0000-000000000005").ToByteArray());
        const ushort fieldId = 0x010C;
        remoteNode.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("remote"), new Hlc(2000, 1));

        var coordinator = new StorageSyncCoordinator(remoteNode, provider, 1);
        var receipt = coordinator.ApplyRemoteFields(new[] { (recordId, fieldId) });

        Assert.NotNull(receipt);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("remote", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(remoteNode.NodeId, current.AuthorNodeId);
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
        Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
    }


    [Fact]
    public void Provider_commit_before_ack_survives_receiver_restart_and_duplicate_batch_retry()
    {
        string nodePath = Path.Combine(_root, "receiver.node");
        using var provider = new StorageDataProvider(Path.Combine(_root, "provider"), 403, deleteOnDispose: true);
        Node sender = NewNode(6);
        Node receiver = NewNode(7);
        RecordId recordId = RecordId.From(Guid.Parse("60000000-0000-0000-0000-000000000006").ToByteArray());
        const ushort fieldId = 0x0101;
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("committed"), new Hlc(3000, 1));

        Transport.MakeSession(sender, receiver);
        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);

        Assert.Throws<IOException>(() => Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, _) =>
            {
                // Receiver protocol receipt is durable before external provider apply.
                NodePersistence.Save(target, nodePath);
                coordinator.ApplyRemoteBatch(batch);
                // Simulate process loss after provider commit but before sender ACK.
                throw new IOException("simulated crash before ACK");
            }));

        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var committed));
        Assert.Equal("committed", Encoding.UTF8.GetString(committed!.Value));

        // Advance provider state after the original batch committed. Replaying the
        // old immutable batch must hit its original receipt and must not fingerprint
        // the provider's newer current winner as if it were the old batch payload.
        provider.ApplyRemoteBatch(Guid.Parse("00000000-0000-0000-0000-000000000403"),
            new[]
            {
                new ProviderMutation
                {
                    RecordId = recordId,
                    FieldId = fieldId,
                    HlcWall = 4000,
                    HlcLogical = 0,
                    AuthorNodeId = sender.NodeId,
                    Value = Encoding.UTF8.GetBytes("newer")
                }
            });

        receiver = NodePersistence.Load(nodePath);
        Transport.MakeSession(sender, receiver);
        coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var retry = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (target, batch, result) =>
            {
                Assert.True(result.Duplicate);
                var receipt = coordinator.ApplyRemoteBatch(batch);
                Assert.True(receipt!.IsDuplicate);
                NodePersistence.Save(target, nodePath);
            });

        Assert.NotNull(retry);
        Assert.True(retry!.Value.Duplicate);
        Assert.Null(Transport.DeliverBatch(sender, receiver, 1));
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var latest));
        Assert.Equal("newer", Encoding.UTF8.GetString(latest!.Value));
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
    }


    [Fact]
    public void Received_batch_materializes_multiple_immutable_updates_to_same_field()
    {
        using var provider = new StorageDataProvider(Path.Combine(_root, "multi-update"), 404, deleteOnDispose: true);
        Node sender = NewNode(8);
        Node receiver = NewNode(9);
        RecordId recordId = RecordId.From(Guid.Parse("80000000-0000-0000-0000-000000000008").ToByteArray());
        const ushort fieldId = 0x0201;

        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("one"), new Hlc(1000, 0));
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("two"), new Hlc(2000, 0));
        sender.WriteField(1, recordId, fieldId, Encoding.UTF8.GetBytes("three"), new Hlc(3000, 0));

        var coordinator = new StorageSyncCoordinator(receiver, provider, 1);
        var delivered = Transport.DeliverBatch(sender, receiver, 1,
            beforeAck: (_, batch, _) => coordinator.ApplyRemoteBatch(batch));

        Assert.NotNull(delivered);
        Assert.True(provider.TryReadCurrent(recordId, fieldId, out var current));
        Assert.Equal("three", Encoding.UTF8.GetString(current!.Value));
        Assert.Equal(0UL, provider.LocalChangeHighWatermark);
    }

    private static Node NewNode(byte seed)
    {
        byte[] id = new byte[16];
        id[0] = seed;
        id[15] = (byte)(seed ^ 0xA5);
        return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
    }
}
