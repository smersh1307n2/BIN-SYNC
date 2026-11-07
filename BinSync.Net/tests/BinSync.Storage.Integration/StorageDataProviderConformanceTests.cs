using BinSync;
using BinSync.DataProvider.Conformance;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageDataProviderConformanceTests : DataProviderConformanceSuite
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-provider-" + Guid.NewGuid().ToString("N"));

    protected override IDataProviderConformanceAdapter CreateProvider()
        => new StorageDataProviderConformanceAdapter(
            Path.Combine(_root, Guid.NewGuid().ToString("N")),
            partitionId: 77);

    [Fact]
    public void Local_feed_mappings_remote_receipts_and_current_state_survive_restart()
    {
        string directory = Path.Combine(_root, "restart");
        Directory.CreateDirectory(directory);
        var itemKey = new byte[] { 0xA1, 0xB2, 0xC3 };
        var fieldKey = new byte[] { 0x10, 0x20 };
        RecordId recordId;
        ushort fieldId;
        var token = Guid.Parse("00000000-0000-0000-0000-000000000301");
        var remote = Mutation(7, 3, 90, 2);

        try
        {
            using (var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 78, deleteOnDispose: false))
            {
                recordId = provider.ResolveRecordId(itemKey);
                fieldId = provider.ResolveFieldId(fieldKey);
                provider.AppendLocalChange(new ProviderMutation
                {
                    RecordId = recordId,
                    FieldId = fieldId,
                    AuthorNodeId = Node(1),
                    HlcWall = 100,
                    HlcLogical = 0,
                    KeyEpoch = 0,
                    Value = new byte[] { 42 }
                });
                provider.ApplyRemoteBatch(token, new[] { remote });
            }

            using var reopened = new StorageDataProviderConformanceAdapter(directory, partitionId: 78, deleteOnDispose: false);
            Assert.Equal(recordId, reopened.ResolveRecordId(itemKey));
            Assert.Equal(fieldId, reopened.ResolveFieldId(fieldKey));
            Assert.Single(reopened.ReadLocalChanges(0, 10).Changes);
            Assert.True(reopened.TryReadCurrent(remote.RecordId, remote.FieldId, out var current));
            Assert.Equal(remote.Value, current!.Value);
            Assert.True(reopened.ApplyRemoteBatch(token, new[] { remote }).IsDuplicate);
            Assert.Equal(1UL, reopened.LocalChangeHighWatermark);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Acknowledged_feed_cleanup_does_not_remove_current_array_state()
    {
        string directory = Path.Combine(_root, "ack");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 79, deleteOnDispose: false);
            var mutation = Mutation(8, 4, 55, 1);
            provider.AppendLocalChange(mutation);
            _ = provider.ReadLocalChanges(0, 10);
            provider.AcknowledgeLocalChanges(1);
            Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
            Assert.True(provider.TryReadCurrent(mutation.RecordId, mutation.FieldId, out var current));
            Assert.Equal(mutation.Value, current!.Value);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}