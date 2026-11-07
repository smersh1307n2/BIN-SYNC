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
                Assert.True(fieldId >= 0x0100);
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

    [Fact]
    public void Source_feed_retry_is_idempotent_and_same_version_equivocation_fails_closed()
    {
        string directory = Path.Combine(_root, "source-retry");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 81, deleteOnDispose: false);
            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(new byte[] { 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }),
                FieldId = 0x0101,
                AuthorNodeId = NodeId.From(new byte[] { 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }),
                HlcWall = 500,
                HlcLogical = 2,
                Value = "stable"u8.ToArray()
            };

            provider.AppendLocalChange(mutation);
            provider.AppendLocalChange(mutation);
            Assert.Equal(1UL, provider.LocalChangeHighWatermark);
            Assert.Single(provider.ReadLocalChanges(0, 10).Changes);

            var equivocation = new ProviderMutation
            {
                RecordId = mutation.RecordId,
                FieldId = mutation.FieldId,
                AuthorNodeId = mutation.AuthorNodeId,
                HlcWall = mutation.HlcWall,
                HlcLogical = mutation.HlcLogical,
                Value = "conflict"u8.ToArray()
            };
            Assert.Throws<InvalidDataException>(() => provider.AppendLocalChange(equivocation));
            Assert.Equal(1UL, provider.LocalChangeHighWatermark);
            Assert.True(provider.TryReadCurrent(mutation.RecordId, mutation.FieldId, out var current));
            Assert.Equal("stable", System.Text.Encoding.UTF8.GetString(current!.Value));

            Guid firstToken = Guid.Parse("00000000-0000-0000-0000-000000000501");
            Guid conflictingToken = Guid.Parse("00000000-0000-0000-0000-000000000502");
            provider.ApplyRemoteBatch(firstToken, new[] { mutation });
            Assert.Throws<InvalidDataException>(() => provider.ApplyRemoteBatch(conflictingToken, new[] { equivocation }));
            Assert.False(provider.ApplyRemoteBatch(conflictingToken, new[] { mutation }).IsDuplicate);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Out_of_order_local_feed_keeps_current_value_at_the_lww_winner()
    {
        string directory = Path.Combine(_root, "lww-order");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 80, deleteOnDispose: false);
            RecordId record = RecordId.From(new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
            NodeId author = NodeId.From(new byte[] { 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record, FieldId = 0x0101, AuthorNodeId = author,
                HlcWall = 200, Value = "newer"u8.ToArray()
            });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record, FieldId = 0x0101, AuthorNodeId = author,
                HlcWall = 100, Value = "older"u8.ToArray()
            });

            Assert.True(provider.TryReadCurrent(record, 0x0101, out var current));
            Assert.Equal("newer", System.Text.Encoding.UTF8.GetString(current!.Value));
            Assert.Equal(2, provider.ReadLocalChanges(0, 10).Changes.Count);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Record_tombstone_hides_older_fields_but_newer_ghost_field_survives_restart_and_snapshot()
    {
        string directory = Path.Combine(_root, "record-tombstone");
        Directory.CreateDirectory(directory);
        RecordId record = RecordId.From(Guid.Parse("f1000000-0000-0000-0000-000000000001").ToByteArray());
        NodeId author = NodeId.From(Guid.Parse("f2000000-0000-0000-0000-000000000002").ToByteArray());

        using (var provider = new StorageDataProvider(directory, 82))
        {
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0101,
                AuthorNodeId = author,
                HlcWall = 1000,
                Value = "before-delete"u8.ToArray()
            });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = Model.LifecycleFieldId,
                IsRecordTombstone = true,
                AuthorNodeId = author,
                HlcWall = 2000,
                Value = Array.Empty<byte>()
            });

            Assert.False(provider.TryReadCurrent(record, 0x0101, out _));

            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0101,
                AuthorNodeId = author,
                HlcWall = 3000,
                Value = "newer-ghost"u8.ToArray()
            });
            Assert.True(provider.TryReadCurrent(record, 0x0101, out var visible));
            Assert.Equal("newer-ghost", System.Text.Encoding.UTF8.GetString(visible!.Value));

            var snapshot = provider.OpenSnapshot();
            try
            {
                var page = provider.ReadSnapshot(snapshot.Token, 0, 10);
                Assert.True(page.IsComplete);
                Assert.Contains(page.Entries, entry => entry.IsRecordTombstone);
            }
            finally
            {
                provider.ReleaseSnapshot(snapshot.Token);
            }
        }

        using (var reopened = new StorageDataProvider(directory, 82))
        {
            Assert.True(reopened.TryReadCurrent(record, 0x0101, out var visible));
            Assert.Equal("newer-ghost", System.Text.Encoding.UTF8.GetString(visible!.Value));
            Assert.Equal(3, reopened.ReadLocalChanges(0, 10).Changes.Count);
        }

        try { Directory.Delete(directory, recursive: true); } catch { }
    }


}