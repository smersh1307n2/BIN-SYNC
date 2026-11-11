using BinSync;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Engine.Db;
using Fld1.Storage;
using Fld1.Storage.Engine;
using Fld1.Storage.Ipc;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldDatabaseIpcAsyncDataProviderTests
{
    [Fact]
    public async Task Async_storage_coordinator_publishes_through_real_database_host_and_survives_host_restart()
    {
        string hostRoot = Path.Combine(Path.GetTempPath(), "fld-binsync-host-process-" + Guid.NewGuid().ToString("N"));
        string nodeRoot = Path.Combine(Path.GetTempPath(), "binsync-host-process-node-" + Guid.NewGuid().ToString("N"));
        const uint namespaceId = 0x535;
        byte[] authorBytes = Id(0x55);
        NodeId author = NodeId.From(authorBytes);
        Guid clientId = Guid.NewGuid();
        Guid firstOwner = Guid.Empty;
        Process? host = null;
        FldDatabaseIpcAsyncDataProvider? provider = null;
        try
        {
            using (var nodeStorage = StorageDatabase.Open(nodeRoot, 701))
            using (var metadata = new StorageMetadataStore(nodeStorage))
            using (var nodeWal = Wal.Open(Path.Combine(nodeRoot, "node.wal")))
            {
                Node node = new NodeBuilder().WithNodeId(author).WithEpoch(17).Build();
                node.AttachWal(nodeWal);
                try
                {
                    RecordId record = RecordId.From(Id(0x56));
                    BatchId changeId = BatchId.From(Id(0x57));
                    byte[] value = "real-host-async-publication"u8.ToArray();
                    const string scope = "real-host-async-publication";

                    host = StartDatabaseHost(hostRoot, namespaceId, authorBytes);
                    StorageIpcEndpointInfo firstEndpoint = await WaitForHostEndpointAsync(
                        hostRoot, host, TimeSpan.FromSeconds(45));
                    firstOwner = firstEndpoint.OwnerId;

                    provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                        hostRoot, clientId, TimeSpan.FromSeconds(15), author, () => 17);
                    await provider.EnableMetadataStoreOwnedPublicationAsync(0);
                    ulong clientSequence = await provider.AppendLocalChangeWithReceiptAsync(new ProviderMutation
                    {
                        RecordId = record,
                        FieldId = 0x0551,
                        AuthorNodeId = author,
                        OriginAuthorEpoch = 17,
                        OriginSequence = 0,
                        HlcWall = 55_700,
                        HlcLogical = 3,
                        Value = value,
                        ChangeId = changeId
                    });
                    Assert.Equal(1UL, clientSequence);

                    // Simulate Host restart after the FLD WAL commit but before
                    // BIN-SYNC has recorded ClientSequence in its Node WAL. Reconnect
                    // with the same ClientId; coordinator retry must recover the
                    // original receipt instead of appending a second Outbox operation.
                    await provider.DisposeAsync();
                    provider = null;
                    await StopDatabaseHostAsync(host);
                    host = null;
                    await WaitForDatabaseWriterReleaseAsync(
                        hostRoot, namespaceId, authorBytes, TimeSpan.FromSeconds(15));
                    host = StartDatabaseHost(hostRoot, namespaceId, authorBytes);
                    StorageIpcEndpointInfo receiptRecoveryEndpoint = await WaitForHostEndpointAsync(
                        hostRoot, host, TimeSpan.FromSeconds(45), firstOwner);
                    Assert.NotEqual(firstOwner, receiptRecoveryEndpoint.OwnerId);
                    provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                        hostRoot, clientId, TimeSpan.FromSeconds(15), author, () => 17);

                    var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, checked((int)namespaceId));
                    int published = await coordinator.PublishLocalChangesAsync(
                        provider, current => StorageNodePersistence.Save(nodeStorage, current));

                    Assert.Equal(1, published);
                    Assert.Equal(1UL, metadata.GetOutboundHighWatermark(
                        scope, node.NodeId, checked((int)namespaceId), node.NodeId));
                    Assert.True(node.TryGetField(checked((int)namespaceId), record, 0x0551, out var cached));
                    Assert.Equal(value, cached!.Value);

                    ProviderMutation? current = await provider.ReadCurrentAsync(record, 0x0551);
                    Assert.NotNull(current);
                    Assert.Equal(changeId, current!.ChangeId);
                    Assert.Equal(1UL, current.OriginSequence);
                    Assert.Empty((await provider.ReadLocalChangesAsync(0, 16)).Changes);
                    Assert.Equal(1UL, await provider.GetLocalChangeHighWatermarkAsync());

                    await provider.DisposeAsync();
                    provider = null;
                    await StopDatabaseHostAsync(host);
                    host = null;
                    await WaitForDatabaseWriterReleaseAsync(
                        hostRoot, namespaceId, authorBytes, TimeSpan.FromSeconds(15));

                    host = StartDatabaseHost(hostRoot, namespaceId, authorBytes);
                    StorageIpcEndpointInfo secondEndpoint = await WaitForHostEndpointAsync(
                        hostRoot, host, TimeSpan.FromSeconds(45), firstOwner);
                    Assert.NotEqual(firstOwner, secondEndpoint.OwnerId);

                    provider = await FldDatabaseIpcAsyncDataProvider.ConnectAsync(
                        hostRoot, clientId, TimeSpan.FromSeconds(15), author, () => 17);
                    current = await provider.ReadCurrentAsync(record, 0x0551);
                    Assert.NotNull(current);
                    Assert.Equal(value, current!.Value);
                    Assert.Equal(changeId, current.ChangeId);
                    Assert.Equal(1UL, current.OriginSequence);
                    Assert.Empty((await provider.ReadLocalChangesAsync(0, 16)).Changes);
                    Assert.Equal(1UL, await provider.GetLocalChangeHighWatermarkAsync());
                    await provider.AcknowledgeOperationReceiptsAsync(clientSequence);
                }
                finally
                {
                    node.Dispose();
                }
            }
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            if (host is not null)
                await StopDatabaseHostAsync(host);
            try { Directory.Delete(hostRoot, recursive: true); } catch { }
            try { Directory.Delete(nodeRoot, recursive: true); } catch { }
        }
    }

    private static Process StartDatabaseHost(string root, uint namespaceId, byte[] localAuthor)
    {
        AssemblyMetadataAttribute repositoryMetadata = typeof(FldDatabaseIpcAsyncDataProviderTests)
            .Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(attribute => attribute.Key == "Fld1RepositoryRoot");
        string repositoryRoot = repositoryMetadata.Value
            ?? throw new InvalidOperationException("FLD repository root assembly metadata is empty.");
        string project = Path.Combine(repositoryRoot, "src", "Fld1.Database.Host", "Fld1.Database.Host.csproj");
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.ArgumentList.Add("run");
        start.ArgumentList.Add("--project");
        start.ArgumentList.Add(project);
        start.ArgumentList.Add("--configuration");
#if DEBUG
        start.ArgumentList.Add("Debug");
#else
        start.ArgumentList.Add("Release");
#endif
        start.ArgumentList.Add("--no-build");
        start.ArgumentList.Add("--no-launch-profile");
        start.ArgumentList.Add("--");
        start.ArgumentList.Add(root);
        start.ArgumentList.Add(namespaceId.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add(Convert.ToHexString(localAuthor));
        return Process.Start(start)
            ?? throw new InvalidOperationException("Could not start the real FLD Database Host process.");
    }

    private static async Task<StorageIpcEndpointInfo> WaitForHostEndpointAsync(
        string root, Process host, TimeSpan timeout, Guid? previousOwner = null)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (host.HasExited)
            {
                string stdout = await host.StandardOutput.ReadToEndAsync();
                string stderr = await host.StandardError.ReadToEndAsync();
                throw new InvalidOperationException(
                    $"FLD Database Host exited with {host.ExitCode}. stdout={stdout} stderr={stderr}");
            }
            if (File.Exists(StorageIpcEndpoint.GetEndpointFilePath(root, 0)))
            {
                try
                {
                    StorageIpcEndpointInfo endpoint = StorageIpcEndpoint.Read(root, 0);
                    if (previousOwner is null || endpoint.OwnerId != previousOwner.Value)
                        return endpoint;
                }
                catch (IOException) { }
                catch (InvalidDataException) { }
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("Real FLD Database Host did not publish a fresh IPC endpoint.");
    }

    private static async Task WaitForDatabaseWriterReleaseAsync(
        string root, uint namespaceId, byte[] localAuthor, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        IOException? lastFailure = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var probe = FldDatabase.Open(root, namespaceId, localAuthor);
                return;
            }
            catch (IOException exception)
            {
                lastFailure = exception;
                await Task.Delay(50);
            }
        }
        throw new TimeoutException("FLD Database Host process exited but its writer fence was not released.", lastFailure);
    }

    private static async Task StopDatabaseHostAsync(Process host)
    {
        try
        {
            if (!host.HasExited)
                host.Kill(entireProcessTree: true);
            await host.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
        }
        catch (InvalidOperationException) { }
        catch (System.ComponentModel.Win32Exception) { }
        finally
        {
            host.Dispose();
        }
    }

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
            ulong receiptSequence = await provider.AppendLocalChangeWithReceiptAsync(mutation);
            Assert.Equal(1ul, receiptSequence);

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
            Assert.True(database.TryGetLocalOperationReceipt(
                clientBytes, operationBytes, receiptSequence, fingerprint, out _));

            await provider.AcknowledgeOperationReceiptsAsync(receiptSequence);
            await provider.AcknowledgeOperationReceiptsAsync(receiptSequence);
            Assert.Throws<InvalidOperationException>(() =>
                database.TryGetLocalOperationReceipt(clientBytes, operationBytes, receiptSequence, fingerprint, out _));

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
