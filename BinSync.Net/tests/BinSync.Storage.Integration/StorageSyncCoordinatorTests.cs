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

    private static Node NewNode(byte seed)
    {
        byte[] id = new byte[16];
        id[0] = seed;
        id[15] = (byte)(seed ^ 0xA5);
        return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
    }
}
