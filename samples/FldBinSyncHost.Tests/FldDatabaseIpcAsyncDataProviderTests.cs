using BinSync;
using BinSync.FldHost;
using Fld1.Engine.Db;
using Fld1.Storage.Ipc;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldDatabaseIpcAsyncDataProviderTests
{
    [Fact]
    public async Task Async_ipc_provider_appends_reads_snapshots_and_acknowledges_without_sync_bridge()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-async-provider-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x534;
        byte[] authorBytes = Id(0x41);
        NodeId author = NodeId.From(authorBytes);
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();
        byte[] recordBytes = FldDatabase.NewRecordId();
        byte[] changeBytes = Id(0x72);
        byte[] value = "async-provider-value"u8.ToArray();

        using var database = FldDatabase.Open(root, namespaceId, authorBytes);
        using var dispatcher = new FldDatabaseIpcDispatcher(database);
        using var shutdown = new CancellationTokenSource();
        var server = new StorageIpcServer(
            StorageIpcEndpoint.ComputePipeName(root, 0), dispatcher.HandleAsync);
        Task serverTask = server.RunAsync(shutdown.Token);

        try
        {
            await server.Ready.WaitAsync(TimeSpan.FromSeconds(10));
            StorageIpcEndpoint.Publish(root, StorageIpcEndpoint.Create(
                root, 0, ownerId, Environment.ProcessId));

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                    root, Guid.NewGuid(), TimeSpan.FromSeconds(10), NodeId.From(Id(0x42)), () => 17));

            await using var provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                root, clientId, TimeSpan.FromSeconds(10), author, () => 17);

            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(recordBytes),
                FieldId = 7,
                HlcWall = 123456,
                HlcLogical = 9,
                AuthorNodeId = author,
                KeyEpoch = 3,
                Value = value,
                ChangeId = BatchId.From(changeBytes),
                OriginAuthorEpoch = 17,
                OriginSequence = 0
            };
            await provider.AppendLocalChangeAsync(mutation);

            ProviderChangePage changes = await provider.ReadLocalChangesAsync(0, 16);
            ProviderChange change = Assert.Single(changes.Changes);
            Assert.Equal(1ul, change.Sequence);
            Assert.Equal(BatchId.From(changeBytes), change.ChangeId);
            Assert.Equal(value, change.Mutation.Value);
            Assert.Equal(1ul, changes.ScannedThroughSequence);

            ProviderMutation? current = await provider.ReadCurrentAsync(RecordId.From(recordBytes), 7);
            Assert.NotNull(current);
            Assert.Equal(changeBytes, current.ChangeId.ToArray());
            Assert.Equal(value, current.Value);
            Assert.Equal(17u, current.OriginAuthorEpoch);

            await provider.CommitPublishedOriginsAsync([
                new ProviderPublishedOrigin(change, new OperationOrigin(author, 17, 44))
            ]);
            current = await provider.ReadCurrentAsync(RecordId.From(recordBytes), 7);
            Assert.NotNull(current);
            Assert.Equal(44ul, current.OriginSequence);

            ProviderSnapshotHandle snapshot = await provider.OpenSnapshotAsync();
            Assert.Equal(1ul, snapshot.ChangeFeedWatermark);
            ProviderSnapshotPage snapshotPage = await provider.ReadSnapshotAsync(snapshot.Token, 0, 16);
            ProviderMutation snapshotEntry = Assert.Single(snapshotPage.Entries);
            Assert.True(snapshotPage.IsComplete);
            Assert.Equal(value, snapshotEntry.Value);
            Assert.Equal(BatchId.From(changeBytes), snapshotEntry.ChangeId);
            Assert.Equal(44ul, snapshotEntry.OriginSequence);

            await provider.AcknowledgeLocalChangesAsync(1);
            await provider.ReleaseSnapshotAsync(snapshot.Token);
            Assert.Equal(1ul, await provider.GetLocalChangeHighWatermarkAsync());

            byte[] clientBytes = new byte[16];
            clientId.TryWriteBytes(clientBytes, bigEndian: true, out _);
            Guid operationGuid = new(changeBytes, bigEndian: true);
            byte[] operationBytes = new byte[16];
            operationGuid.TryWriteBytes(operationBytes, bigEndian: true, out _);
            var localMutation = new FldLocalProviderMutation(
                recordBytes, 7, value, changeBytes, 17, 123456, 9, 3);
            byte[] request = FldDatabaseIpcLocalProviderProtocol.EncodeAutoSequenceRequest([localMutation]);
            byte[] fingerprint = FldDatabaseIpcProtocol.ComputeFingerprint(
                FldDatabaseIpcOperationCodes.AppendLocalProviderBatchAutoSequence, request);
            Assert.Throws<InvalidOperationException>(() =>
                database.TryGetLocalOperationReceipt(clientBytes, operationBytes, 1, fingerprint, out _));
        }
        finally
        {
            shutdown.Cancel();
            try { await serverTask.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
            StorageIpcEndpoint.DeleteIfOwned(root, 0, ownerId);
            dispatcher.Dispose();
            database.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static byte[] Id(byte value)
    {
        byte[] id = new byte[16];
        id[0] = value;
        return id;
    }
}
