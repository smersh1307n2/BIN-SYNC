using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageNodePersistenceTests
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-node-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void Complete_node_checkpoint_round_trips_inside_shared_storage_database()
    {
        string directory = Path.Combine(_root, "node-checkpoint");
        Node node = NewNode(21);
        RecordId recordId = RecordId.From(Guid.Parse("e1000000-0000-0000-0000-000000000001").ToByteArray());

        using (var storage = StorageDatabase.Open(directory, 410))
        {
            Assert.False(StorageNodePersistence.TryLoad(storage, node.NodeId, out _));
            Assert.True(node.WriteField(1, recordId, 0x0101, "stored-in-storage"u8.ToArray(), new Hlc(9000, 2)));
            StorageNodePersistence.Save(storage, node);
            Assert.True(StorageNodePersistence.TryLoad(storage, node.NodeId, out var inProcess));
            Assert.Equal(node.NodeId, inProcess!.NodeId);
        }

        using (var reopenedStorage = StorageDatabase.Open(directory, 410))
        {
            Node restored = StorageNodePersistence.Load(reopenedStorage, node.NodeId);
            Assert.Equal(node.NodeId, restored.NodeId);
            Assert.Equal(node.Epoch, restored.Epoch);
            Assert.Equal(1L, restored.GetOutboxHighWatermark(1, node.NodeId));
            Assert.Single(restored.ReadOutboxEntries(1, node.NodeId, 0, 10));
            Assert.True(restored.TryGetRecord(1, recordId, out var record));
            Assert.NotNull(record);
            Assert.Equal("stored-in-storage", System.Text.Encoding.UTF8.GetString(record!.State[0x0101].Value));
        }
    }

    [Fact]
    public void Serialize_to_bytes_uses_the_same_integrity_checked_checkpoint_format()
    {
        Node node = NewNode(22);
        byte[] image = NodePersistence.SerializeToBytes(node);
        Node restored = NodePersistence.LoadFromBytes(image);
        Assert.Equal(node.NodeId, restored.NodeId);

        image[^1] ^= 0x80;
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(image));
    }

    private static Node NewNode(byte seed)
    {
        byte[] id = new byte[16];
        id[0] = seed;
        id[15] = (byte)(seed ^ 0xA5);
        return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
    }
}
