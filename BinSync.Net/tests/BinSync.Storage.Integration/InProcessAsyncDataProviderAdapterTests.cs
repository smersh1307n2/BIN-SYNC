using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration.Tests;

public sealed class InProcessAsyncDataProviderAdapterTests
{
    [Fact]
    public async Task Async_adapter_preserves_local_feed_snapshot_remote_apply_and_cancellation_contracts()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-async-provider-adapter-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 813);
            using var provider = new StorageDataProvider(storage);
            await using var adapter = new InProcessAsyncDataProviderAdapter(provider);

            byte[] sourceItemKey = Id(0x81);
            byte[] sourceFieldKey = Id(0x82);
            RecordId localRecord = await adapter.ResolveRecordIdAsync(sourceItemKey);
            ushort localField = await adapter.ResolveFieldIdAsync(sourceFieldKey);
            byte[] localValue = "async-adapter-local"u8.ToArray();
            byte[] localChangeId = Id(0x83);

            await adapter.AppendLocalChangeAsync(new ProviderMutation
            {
                RecordId = localRecord,
                FieldId = localField,
                HlcWall = 1001,
                HlcLogical = 3,
                AuthorNodeId = NodeId.From(Id(0x84)),
                KeyEpoch = 0,
                Value = localValue,
                ChangeId = BatchId.From(localChangeId),
                OriginAuthorEpoch = 5
            });

            Assert.Equal(1ul, await adapter.GetLocalChangeHighWatermarkAsync());
            ProviderChange localChange = Assert.Single(
                (await adapter.ReadLocalChangesAsync(0, 16)).Changes);
            Assert.Equal(BatchId.From(localChangeId), localChange.ChangeId);
            Assert.Equal(localValue, localChange.Mutation.Value);

            ProviderSnapshotHandle snapshot = await adapter.OpenSnapshotAsync();
            try
            {
                ProviderSnapshotPage page = await adapter.ReadSnapshotAsync(snapshot.Token, 0, 16);
                Assert.True(page.IsComplete);
                Assert.Contains(page.Entries, entry =>
                    entry.RecordId == localRecord && entry.FieldId == localField);
            }
            finally
            {
                await adapter.ReleaseSnapshotAsync(snapshot.Token);
            }

            ProviderMutation? current = await adapter.ReadCurrentAsync(localRecord, localField);
            Assert.NotNull(current);
            Assert.Equal(localValue, current!.Value);

            await adapter.AcknowledgeLocalChangesAsync(1);
            Assert.Empty((await adapter.ReadLocalChangesAsync(0, 16)).Changes);

            RecordId remoteRecord = RecordId.From(Id(0x85));
            byte[] remoteValue = "async-adapter-remote"u8.ToArray();
            ProviderApplyReceipt remoteReceipt = await adapter.ApplyRemoteBatchAsync(
                Guid.NewGuid(),
                [
                    new ProviderMutation
                    {
                        RecordId = remoteRecord,
                        FieldId = 0x0881,
                        HlcWall = 2001,
                        HlcLogical = 1,
                        AuthorNodeId = NodeId.From(Id(0x86)),
                        KeyEpoch = 0,
                        Value = remoteValue,
                        ChangeId = BatchId.From(Id(0x87)),
                        OriginAuthorEpoch = 6,
                        OriginSequence = 1
                    }
                ]);
            Assert.False(remoteReceipt.IsDuplicate);
            Assert.Equal(1ul, await adapter.GetLocalChangeHighWatermarkAsync());
            ProviderMutation? remoteCurrent = await adapter.ReadCurrentAsync(remoteRecord, 0x0881);
            Assert.NotNull(remoteCurrent);
            Assert.Equal(remoteValue, remoteCurrent!.Value);

            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await adapter.GetLocalChangeHighWatermarkAsync(cancelled.Token));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[15] = value;
        return id;
    }
}
