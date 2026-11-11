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

    [Fact]
    public async Task Async_ipc_provider_roundtrips_record_tombstone_as_empty_provider_value()
    {
        string root = Path.Combine(
            Path.GetTempPath(), "binsync-fld-async-provider-tombstone-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x534;
        byte[] authorBytes = Id(0x43);
        NodeId author = NodeId.From(authorBytes);
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();
        byte[] recordBytes = FldDatabase.NewRecordId();
        byte[] changeBytes = Id(0x74);

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

            await using var provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                root, clientId, TimeSpan.FromSeconds(10), author, () => 17);
            var tombstone = new ProviderMutation
            {
                RecordId = RecordId.From(recordBytes),
                FieldId = FldLifecycle.FieldId,
                IsRecordTombstone = true,
                HlcWall = 123457,
                HlcLogical = 1,
                AuthorNodeId = author,
                KeyEpoch = 0,
                Value = Array.Empty<byte>(),
                ChangeId = BatchId.From(changeBytes),
                OriginAuthorEpoch = 17,
                OriginSequence = 0
            };

            await provider.AppendLocalChangeAsync(tombstone);

            ProviderChangePage changes = await provider.ReadLocalChangesAsync(0, 8);
            ProviderMutation local = Assert.Single(changes.Changes).Mutation;
            Assert.True(local.IsRecordTombstone);
            Assert.Empty(local.Value);

            ProviderMutation? current = await provider.ReadCurrentAsync(
                RecordId.From(recordBytes), FldLifecycle.FieldId);
            Assert.NotNull(current);
            Assert.True(current.IsRecordTombstone);
            Assert.Empty(current.Value);

            ProviderSnapshotHandle snapshot = await provider.OpenSnapshotAsync();
            ProviderSnapshotPage page = await provider.ReadSnapshotAsync(snapshot.Token, 0, 8);
            ProviderMutation snapshotEntry = Assert.Single(page.Entries);
            Assert.True(snapshotEntry.IsRecordTombstone);
            Assert.Empty(snapshotEntry.Value);

            await provider.AcknowledgeLocalChangesAsync(1);
            await provider.ReleaseSnapshotAsync(snapshot.Token);
            Assert.Equal(1ul, database.GetOutboxWatermark());
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

    [Fact]
    public async Task Async_remote_batch_store_preserves_context_and_deduplicates_apply()
    {
        string root = Path.Combine(Path.GetTempPath(), "fld-async-remote-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x535;
        byte[] localAuthor = Id(0x51);
        byte[] sender = Id(0x52);
        byte[] issuer = Id(0x53);
        byte[] record = Id(0x54);
        byte[] change = Id(0x55);
        byte[] batchId = Id(0x56);
        byte[] batchDigest = Id(0x57);
        byte[] recordsRoot = Id(0x58);
        byte[] value = "async-contextual-apply"u8.ToArray();
        Guid ownerId = Guid.NewGuid();
        Guid clientId = Guid.NewGuid();

        using var database = FldDatabase.Open(root, namespaceId, localAuthor);
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
            await using var client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
                root, clientId, TimeSpan.FromSeconds(10));
            var store = new FldDatabaseIpcRemoteBatchApplyStore(client, checked((int)namespaceId));
            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(record),
                FieldId = 0x0102,
                HlcWall = 123456,
                HlcLogical = 3,
                AuthorNodeId = NodeId.From(issuer),
                KeyEpoch = 2,
                Value = value,
                ChangeId = BatchId.From(change),
                OriginAuthorEpoch = 4,
                OriginSequence = 9
            };
            var batch = new ProviderRemoteBatchCommit(
                Guid.NewGuid(),
                NodeId.From(sender),
                senderTransportEpoch: 5,
                namespaceId: checked((int)namespaceId),
                NodeId.From(issuer),
                issuerEpoch: 4,
                BatchId.From(batchId),
                cursorStart: 1,
                cursorEnd: 1,
                new byte[16],
                batchDigest,
                recordsRoot,
                protocolStateDelta: new byte[] { 0xA1, 0xB2 },
                mutations: [mutation]);

            ProviderApplyReceipt first = await store.ApplyRemoteBatchAsync(batch);
            Assert.False(first.IsDuplicate);
            Assert.Equal(batchDigest, first.BatchFingerprint);
            Assert.True(database.TryGetBytes(record, 0x0102, out byte[]? stored));
            Assert.Equal(value, stored);
            Assert.Equal(0ul, database.GetOutboxHighWatermark());
            Assert.Equal(1ul, database.GetRelayJournalHighWatermark());

            ProviderApplyReceipt duplicate = await store.ApplyRemoteBatchAsync(batch);
            Assert.True(duplicate.IsDuplicate);
            Assert.Equal(first.BatchFingerprint, duplicate.BatchFingerprint);
            Assert.Equal(1ul, database.GetRelayJournalHighWatermark());
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
