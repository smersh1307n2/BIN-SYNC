using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class SharedStorageDatabaseTests
{
    [Fact]
    public void Data_provider_and_metadata_store_share_one_database_without_stale_partition_state()
    {
        string root = Path.Combine(Path.GetTempPath(), "binsync-shared-storage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        string path = Path.Combine(root, "db");
        using (var database = StorageDatabase.Open(path, 501))
        {
        NodeId replica = NodeId.From(Identity(1));
        NodeId author = NodeId.From(Identity(2));
        RecordId record = RecordId.From(Identity(3));

        using (var provider = new StorageDataProvider(database))
        using (var metadata = new StorageMetadataStore(database))
        {
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0101,
                AuthorNodeId = author,
                HlcWall = 100,
                Value = "one"u8.ToArray()
            });

            BatchId changeId = BatchId.From(Identity(4));
            var frozen = metadata.FreezeOutbound("array-demo", replica, 1, author,
                new[] { new MetadataFrozenOperation { ChangeId = changeId, Fingerprint = Enumerable.Repeat((byte)9, 16).ToArray(), Payload = "op-1"u8.ToArray() } });
            Assert.Single(frozen);

            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0102,
                AuthorNodeId = author,
                HlcWall = 101,
                Value = "two"u8.ToArray()
            });

            Assert.Equal(2UL, provider.LocalChangeHighWatermark);
            var feed = provider.ReadLocalChanges(0, 10);
            Assert.Equal(2, feed.Changes.Count);

            Guid consumer = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
            ulong lease = metadata.AcquireOutboundLease("array-demo", replica, 1, author, consumer);
            var outbound = metadata.ReadOutbound("array-demo", replica, 1, author, lease, 0, 10);
            Assert.Single(outbound.Items);
            Assert.Equal(changeId, outbound.Items[0].ChangeId);

            // A provider commit invalidates metadata cache generations and vice versa.
            // Re-reading both sides must refresh, not reuse stale in-memory partitions.
            Assert.Equal(1UL, metadata.GetOutboundHighWatermark("array-demo", replica, 1, author));
            Assert.True(provider.TryReadCurrent(record, 0x0102, out var current));
            Assert.Equal("two", System.Text.Encoding.UTF8.GetString(current!.Value));
        }

        // Adapters attached to a caller-owned StorageDatabase must not close it.
        Assert.True(database.LastCommitSeq >= 3);
        using (var reopenedProvider = new StorageDataProvider(database))
        using (var reopenedMetadata = new StorageMetadataStore(database))
        {
            Assert.Equal(2UL, reopenedProvider.LocalChangeHighWatermark);
            Assert.Equal(1UL, reopenedMetadata.GetOutboundHighWatermark("array-demo", replica, 1, author));
        }

        }

        Directory.Delete(root, recursive: true);
    }

    private static byte[] Identity(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        id[15] = (byte)(value ^ 0xA5);
        return id;
    }
}
