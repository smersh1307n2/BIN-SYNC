using BinSync;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Fld1.Engine.Db;
using Fld1.Engine.Storage;
using Xunit;

namespace BinSync.FldHost.Tests;

public sealed class FldRemoteBatchApplyStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(
        Path.GetTempPath(), "binsync-fld-host-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Fact]
    public void Host_adapter_rejects_coordinator_metadata_scope_mismatch()
    {
        const uint namespaceId = 71;
        byte[] localAuthor = Id(0xD1);
        const string scope = "fld-host-scope-binding";

        using var database = FldDatabase.Open(
            Path.Combine(_directory, "fld-scope-binding"), namespaceId, localAuthor);
        using var storage = StorageDatabase.Open(
            Path.Combine(_directory, "binsync-scope-binding"), 703);
        using var metadata = new StorageMetadataStore(storage);
        using var node = new NodeBuilder()
            .WithNodeId(NodeId.From(localAuthor))
            .WithEpoch(1)
            .Build();
        var publicationMode = new StorageMetadataPublicationMode(
            metadata, scope, NodeId.From(localAuthor), checked((int)namespaceId));
        using var provider = new FldDatabaseDataProvider(
            database, () => 1, publicationMode: publicationMode);
        var remoteApply = new FldRemoteBatchApplyStore(database);
        var coordinator = new StorageSyncCoordinator(
            node, provider, checked((int)namespaceId), metadata, scope + "-wrong",
            remoteBatchCommitStore: remoteApply);
        var map = new StorageRelayProvenanceMap(storage, scope, node.NodeId);

        Assert.Throws<ArgumentException>(() => new FldBinSyncHostAdapter(
            database, provider, metadata, coordinator, map, scope, remoteApply));
    }

    [Fact]
    public async Task Provider_full_replace_staging_writes_wait_for_database_commit_gate()
    {
        const uint namespaceId = 72;
        byte[] localAuthor = Id(0xD2);
        using var database = FldDatabase.Open(
            Path.Combine(_directory, "fld-full-replace-commit-gate"), namespaceId, localAuthor);
        using var provider = new FldDatabaseDataProvider(database, () => 1);
        Guid generation = Guid.Parse("72000000-0000-0000-0000-000000000001");
        byte[] digest = new byte[32];
        digest[0] = 0x72;

        using var writerStarted = new ManualResetEventSlim();
        using var commitGuard = database.AcquireCommitGuard();
        Task writer = Task.Run(() =>
        {
            writerStarted.Set();
            provider.BeginFullReplace(generation, 0, 1, digest);
            provider.StageFullReplacePage(generation, 0, new[]
            {
                new ProviderMutation
                {
                    RecordId = RecordId.From(Id(0xD3)),
                    FieldId = 0x0123,
                    AuthorNodeId = NodeId.From(localAuthor),
                    OriginAuthorEpoch = 1,
                    OriginSequence = 1,
                    HlcWall = 7200,
                    ChangeId = BatchId.From(Id(0xD4)),
                    Value = "staged-under-commit-gate"u8.ToArray()
                }
            });
        });

        Assert.True(writerStarted.Wait(TimeSpan.FromSeconds(5)));
        Assert.False(writer.Wait(TimeSpan.FromMilliseconds(150)));
        commitGuard.Dispose();
        await writer.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(provider.TryGetFullReplaceProgress(generation, out ProviderFullReplaceProgress? progress));
        Assert.NotNull(progress);
        Assert.Equal(1, progress.NextIndex);
        Assert.False(progress.IsCommitted);
    }

    [Fact]
    public void Contextual_remote_apply_maps_to_fld_wal_and_duplicate_survives_reopen()
    {
        const uint namespaceId = 7;
        byte[] localAuthor = Id(0x11);
        byte[] sender = Id(0x22);
        byte[] issuer = Id(0x33);
        byte[] recordBytes = Id(0x44);
        byte[] changeBytes = Id(0x55);
        byte[] batchBytes = Id(0x66);
        byte[] previousDigest = new byte[16];
        byte[] batchDigest = Id(0x77);
        byte[] recordsRoot = Id(0x88);
        byte[] value = "fld-host-integration"u8.ToArray();
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(recordBytes),
            FieldId = 0x0102,
            HlcWall = 123456,
            HlcLogical = 3,
            AuthorNodeId = NodeId.From(issuer),
            KeyEpoch = 2,
            Value = value,
            ChangeId = BatchId.From(changeBytes),
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
            BatchId.From(batchBytes),
            cursorStart: 1,
            cursorEnd: 1,
            previousDigest,
            batchDigest,
            recordsRoot,
            protocolStateDelta: new byte[] { 0xA1, 0xB2 },
            mutations: new[] { mutation });

        ulong firstRelayWatermark;
        using (var database = FldDatabase.Open(_directory, namespaceId, localAuthor))
        {
            var adapter = new FldRemoteBatchApplyStore(database);
            var first = adapter.ApplyRemoteBatch(batch);

            Assert.False(first.IsDuplicate);
            Assert.Equal(batch.BatchDigest, first.BatchFingerprint);
            Assert.True(database.TryGetBytes(recordBytes, 0x0102, out byte[]? stored));
            Assert.Equal(value, stored);
            Assert.True(database.TryResolve(recordBytes, 0x0102, out FieldResolver.Resolved resolved));
            Assert.Equal(changeBytes, resolved.ChangeId);
            Assert.Equal(4U, resolved.OriginAuthorEpoch);
            Assert.Equal(9UL, resolved.OriginSequence);
            Assert.Equal(0UL, database.GetOutboxHighWatermark());
            Assert.Empty(database.ReadRelayBatches(issuer, afterJournalSeq: 0, limit: 10));
            firstRelayWatermark = database.GetRelayJournalHighWatermark();
            Assert.Equal(1UL, firstRelayWatermark);

            var duplicate = adapter.ApplyRemoteBatch(batch);
            Assert.True(duplicate.IsDuplicate);
            Assert.Equal(first.BatchFingerprint, duplicate.BatchFingerprint);
            Assert.Equal(firstRelayWatermark, database.GetRelayJournalHighWatermark());
        }

        using (var reopened = FldDatabase.Open(_directory, namespaceId, localAuthor))
        {
            var adapter = new FldRemoteBatchApplyStore(reopened);
            Assert.True(reopened.TryGetBytes(recordBytes, 0x0102, out byte[]? recovered));
            Assert.Equal(value, recovered);
            Assert.True(reopened.TryResolve(recordBytes, 0x0102, out FieldResolver.Resolved recoveredMetadata));
            Assert.Equal(changeBytes, recoveredMetadata.ChangeId);
            Assert.Equal(4U, recoveredMetadata.OriginAuthorEpoch);
            Assert.Equal(9UL, recoveredMetadata.OriginSequence);
            Assert.Equal(0UL, reopened.GetOutboxHighWatermark());
            Assert.Empty(reopened.ReadRelayBatches(issuer, afterJournalSeq: 0, limit: 10));

            var duplicateAfterRestart = adapter.ApplyRemoteBatch(batch);
            Assert.True(duplicateAfterRestart.IsDuplicate);
            Assert.Equal(firstRelayWatermark, reopened.GetRelayJournalHighWatermark());
        }
    }

    [Fact]
    public void Contextual_remote_record_tombstone_preserves_origin_metadata_in_fld_lifecycle_row()
    {
        const uint namespaceId = 9;
        byte[] localAuthor = Id(0x91);
        byte[] issuer = Id(0x92);
        byte[] sender = Id(0x93);
        byte[] recordBytes = Id(0x94);
        byte[] changeBytes = Id(0x95);
        byte[] batchBytes = Id(0x96);
        byte[] batchDigest = Id(0x97);
        byte[] recordsRoot = Id(0x98);
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(recordBytes),
            FieldId = FldLifecycle.FieldId,
            IsRecordTombstone = true,
            HlcWall = 223456,
            HlcLogical = 4,
            AuthorNodeId = NodeId.From(issuer),
            KeyEpoch = 0,
            Value = Array.Empty<byte>(),
            ChangeId = BatchId.From(changeBytes),
            OriginAuthorEpoch = 7,
            OriginSequence = 12
        };
        var batch = new ProviderRemoteBatchCommit(
            Guid.NewGuid(),
            NodeId.From(sender),
            senderTransportEpoch: 3,
            namespaceId: checked((int)namespaceId),
            NodeId.From(issuer),
            issuerEpoch: 7,
            BatchId.From(batchBytes),
            cursorStart: 12,
            cursorEnd: 12,
            new byte[16],
            batchDigest,
            recordsRoot,
            protocolStateDelta: Array.Empty<byte>(),
            mutations: new[] { mutation });

        using var database = FldDatabase.Open(_directory, namespaceId, localAuthor);
        var adapter = new FldRemoteBatchApplyStore(database);
        ProviderApplyReceipt receipt = adapter.ApplyRemoteBatch(batch);

        Assert.False(receipt.IsDuplicate);
        Assert.True(database.TryResolve(recordBytes, FldLifecycle.FieldId, out FieldResolver.Resolved resolved));
        Assert.Equal(FldLifecycle.DeadValue.ToArray(), resolved.Value);
        Assert.Equal(changeBytes, resolved.ChangeId);
        Assert.Equal(7U, resolved.OriginAuthorEpoch);
        Assert.Equal(12UL, resolved.OriginSequence);
        Assert.Equal(0UL, database.GetOutboxHighWatermark());
    }

    [Fact]
    public void Local_fld_outbox_feed_preserves_stable_change_identity_across_reopen()
    {
        const uint namespaceId = 8;
        byte[] localAuthor = Id(0x19);
        byte[] recordBytes = Id(0x29);
        byte[] changeBytes = Id(0x39);
        byte[] value = "local-outbox-feed"u8.ToArray();
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(recordBytes),
            FieldId = 0x0203,
            HlcWall = 987654,
            HlcLogical = 2,
            AuthorNodeId = NodeId.From(localAuthor),
            KeyEpoch = 1,
            Value = value,
            ChangeId = BatchId.From(changeBytes),
            OriginAuthorEpoch = 6,
            OriginSequence = 0
        };

        using (var database = FldDatabase.Open(_directory, namespaceId, localAuthor))
        using (var provider = new FldDatabaseDataProvider(database, () => 6))
        {
            provider.AppendLocalChange(mutation);
            var page = provider.ReadLocalChanges(afterSequence: 0, limit: 16);
            var change = Assert.Single(page.Changes);
            Assert.Equal(1UL, page.ScannedThroughSequence);
            Assert.Equal(BatchId.From(changeBytes), change.ChangeId);
            Assert.Equal(BatchId.From(changeBytes), change.Mutation.ChangeId);
            Assert.Equal(NodeId.From(localAuthor), change.Mutation.AuthorNodeId);
            Assert.Equal(6U, change.Mutation.OriginAuthorEpoch);
            Assert.Equal(0UL, change.Mutation.OriginSequence);
            Assert.Equal(value, change.Mutation.Value);
        }

        using (var reopened = FldDatabase.Open(_directory, namespaceId, localAuthor))
        using (var provider = new FldDatabaseDataProvider(reopened, () => 6))
        {
            var recovered = Assert.Single(provider.ReadLocalChanges(afterSequence: 0, limit: 16).Changes);
            Assert.Equal(BatchId.From(changeBytes), recovered.ChangeId);
            Assert.Equal(BatchId.From(changeBytes), recovered.Mutation.ChangeId);
            Assert.Equal(value, recovered.Mutation.Value);
            Assert.Equal(1UL, recovered.Sequence);
        }
    }

    [Fact]
    public async Task Two_fld_databases_resume_bidirectional_sync_after_full_process_restart()
    {
        // First exchange creates and checkpoints both peers. The helper closes every
        // FLD, BIN-SYNC Storage, Node and SyncSession handle before returning.
        await ExchangeRoundAsync(restoreNodes: false, recordASeed: 0xA2, recordBSeed: 0xB2,
            scope: "fld-host-two-node-roundtrip");
        // Reopen every durable component from disk, including the complete Node image
        // and metadata stores, then reconnect and exchange a second pair of changes.
        await ExchangeRoundAsync(restoreNodes: true, recordASeed: 0xC2, recordBSeed: 0xD2,
            scope: "fld-host-two-node-roundtrip");
    }

    private async Task ExchangeRoundAsync(bool restoreNodes, byte recordASeed, byte recordBSeed, string scope)
    {
        const int namespaceId = 7;
        byte[] authorA = Id(0xA1);
        byte[] authorB = Id(0xB1);
        byte[] recordA = Id(recordASeed);
        byte[] recordB = Id(recordBSeed);
        byte[] valueA = new[] { recordASeed, (byte)0x51, (byte)0x61 };
        byte[] valueB = new[] { recordBSeed, (byte)0x52, (byte)0x62 };

        using var fldA = FldDatabase.Open(Path.Combine(_directory, "fld-a"), (uint)namespaceId, authorA);
        using var fldB = FldDatabase.Open(Path.Combine(_directory, "fld-b"), (uint)namespaceId, authorB);
        using var providerA = new FldDatabaseDataProvider(fldA, () => 1);
        using var providerB = new FldDatabaseDataProvider(fldB, () => 1);
        using var storageA = StorageDatabase.Open(Path.Combine(_directory, "binsync-a"), 701);
        using var storageB = StorageDatabase.Open(Path.Combine(_directory, "binsync-b"), 702);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var nodeA = restoreNodes
            ? StorageNodePersistence.Load(storageA, NodeId.From(authorA))
            : new NodeBuilder().WithNodeId(NodeId.From(authorA)).WithEpoch(1).Build();
        using var nodeB = restoreNodes
            ? StorageNodePersistence.Load(storageB, NodeId.From(authorB))
            : new NodeBuilder().WithNodeId(NodeId.From(authorB)).WithEpoch(1).Build();

        var coordinatorA = new StorageSyncCoordinator(
            nodeA, providerA, namespaceId, metadataA, scope,
            remoteBatchCommitStore: new FldRemoteBatchApplyStore(fldA));
        var coordinatorB = new StorageSyncCoordinator(
            nodeB, providerB, namespaceId, metadataB, scope,
            remoteBatchCommitStore: new FldRemoteBatchApplyStore(fldB));

        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = RecordId.From(recordA),
            FieldId = 0x0301,
            HlcWall = 1001UL + recordASeed,
            AuthorNodeId = NodeId.From(authorA),
            KeyEpoch = 0,
            Value = valueA,
            ChangeId = BatchId.From(Id((byte)(recordASeed + 1))),
            OriginAuthorEpoch = 1
        });
        providerB.AppendLocalChange(new ProviderMutation
        {
            RecordId = RecordId.From(recordB),
            FieldId = 0x0301,
            HlcWall = 1002UL + recordBSeed,
            AuthorNodeId = NodeId.From(authorB),
            KeyEpoch = 0,
            Value = valueB,
            ChangeId = BatchId.From(Id((byte)(recordBSeed + 1))),
            OriginAuthorEpoch = 1
        });

        Assert.Equal(1, coordinatorA.PublishLocalChanges(node => StorageNodePersistence.Save(storageA, node)));
        Assert.Equal(1, coordinatorB.PublishLocalChanges(node => StorageNodePersistence.Save(storageB, node)));

        var (toA, toB) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, toB);
        var connectTask = SyncSession.ConnectAsync(nodeA, toA);
        await using var sessionB = await acceptTask.WaitAsync(TimeSpan.FromSeconds(20));
        await using var sessionA = await connectTask.WaitAsync(TimeSpan.FromSeconds(20));
        // Persist negotiated peer aliases before the durable receive-before-ACK path.
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);

        Task<BatchResult> receiveAtB = coordinatorB.ReceiveBatchBeforeAckAsync(
            sessionB, node => StorageNodePersistence.Save(storageB, node));
        Assert.True(await sessionA.PushBatchAsync(nodeA, namespaceId).WaitAsync(TimeSpan.FromSeconds(20)));
        BatchResult resultAtB = await receiveAtB.WaitAsync(TimeSpan.FromSeconds(20));
        // The sender's ACK/progress is part of its durable Node checkpoint too.
        StorageNodePersistence.Save(storageA, nodeA);
        Assert.Equal(1, resultAtB.Applied);
        Assert.True(fldB.TryGetBytes(recordA, 0x0301, out byte[]? receivedAtB));
        Assert.Equal(valueA, receivedAtB);

        Task<BatchResult> receiveAtA = coordinatorA.ReceiveBatchBeforeAckAsync(
            sessionA, node => StorageNodePersistence.Save(storageA, node));
        Assert.True(await sessionB.PushBatchAsync(nodeB, namespaceId).WaitAsync(TimeSpan.FromSeconds(20)));
        BatchResult resultAtA = await receiveAtA.WaitAsync(TimeSpan.FromSeconds(20));
        StorageNodePersistence.Save(storageB, nodeB);
        Assert.Equal(1, resultAtA.Applied);
        Assert.True(fldA.TryGetBytes(recordB, 0x0301, out byte[]? receivedAtA));
        Assert.Equal(valueB, receivedAtA);

        Assert.True(providerA.TryReadCurrent(RecordId.From(recordB), 0x0301, out ProviderMutation? currentA));
        Assert.Equal(valueB, currentA!.Value);
        Assert.True(providerB.TryReadCurrent(RecordId.From(recordA), 0x0301, out ProviderMutation? currentB));
        Assert.Equal(valueA, currentB!.Value);
    }

    [Fact]
    public async Task Pending_apply_recovers_when_fld_commit_precedes_metadata_receipt_and_ack()
    {
        const int namespaceId = 10;
        const string scope = "fld-crash-boundary";
        byte[] authorA = Id(0xE1);
        byte[] authorB = Id(0xE2);
        byte[] recordId = Id(0xE3);
        byte[] value = "durable-before-ack"u8.ToArray();
        string fldAPath = Path.Combine(_directory, "crash-fld-a");
        string fldBPath = Path.Combine(_directory, "crash-fld-b");
        string storageAPath = Path.Combine(_directory, "crash-storage-a");
        string storageBPath = Path.Combine(_directory, "crash-storage-b");

        // Simulate a process dying after FLD has durably committed the remote
        // mutation but before the Metadata PendingApply receipt can be finalized
        // and before the sender can receive its ACK.
        using (var fldA = FldDatabase.Open(fldAPath, (uint)namespaceId, authorA))
        using (var fldB = FldDatabase.Open(fldBPath, (uint)namespaceId, authorB))
        using (var providerA = new FldDatabaseDataProvider(fldA, () => 1))
        using (var providerB = new FldDatabaseDataProvider(fldB, () => 1))
        using (var storageA = StorageDatabase.Open(storageAPath, 711))
        using (var storageB = StorageDatabase.Open(storageBPath, 712))
        using (var metadataA = new StorageMetadataStore(storageA))
        using (var metadataB = new StorageMetadataStore(storageB))
        using (var nodeA = new NodeBuilder().WithNodeId(NodeId.From(authorA)).WithEpoch(1).Build())
        using (var nodeB = new NodeBuilder().WithNodeId(NodeId.From(authorB)).WithEpoch(1).Build())
        {
            var coordinatorA = new StorageSyncCoordinator(
                nodeA, providerA, namespaceId, metadataA, scope);
            var crashAfterCommit = new ApplyThenThrowOnceStore(new FldRemoteBatchApplyStore(fldB));
            var coordinatorB = new StorageSyncCoordinator(
                nodeB, providerB, namespaceId, metadataB, scope,
                remoteBatchCommitStore: crashAfterCommit);

            providerA.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(recordId),
                FieldId = 0x0501,
                HlcWall = 5001,
                AuthorNodeId = NodeId.From(authorA),
                KeyEpoch = 0,
                Value = value,
                ChangeId = BatchId.From(Id(0xE4)),
                OriginAuthorEpoch = 1
            });
            Assert.Equal(1, coordinatorA.PublishLocalChanges(
                node => StorageNodePersistence.Save(storageA, node)));

            var (toA, toB) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(nodeB, toB);
            var connectTask = SyncSession.ConnectAsync(nodeA, toA);
            await using var sessionB = await acceptTask.WaitAsync(TimeSpan.FromSeconds(20));
            await using var sessionA = await connectTask.WaitAsync(TimeSpan.FromSeconds(20));
            StorageNodePersistence.Save(storageA, nodeA);
            StorageNodePersistence.Save(storageB, nodeB);

            using var sendCancellation = new CancellationTokenSource();
            Task<BatchResult> receiveTask = coordinatorB.ReceiveBatchBeforeAckAsync(
                sessionB, node => StorageNodePersistence.Save(storageB, node));
            Task<bool> sendTask = sessionA.PushBatchAsync(
                nodeA, namespaceId, ct: sendCancellation.Token);

            await Assert.ThrowsAsync<SimulatedProcessCrashException>(
                async () => await receiveTask.WaitAsync(TimeSpan.FromSeconds(20)));
            sendCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await sendTask.WaitAsync(TimeSpan.FromSeconds(5)));

            // FLD has committed, Node is checkpointed, but the durable Metadata
            // intent is deliberately still pending and the sender has no ACK.
            Assert.True(fldB.TryGetBytes(recordId, 0x0501, out byte[]? committedBeforeRestart));
            Assert.Equal(value, committedBeforeRestart);
            Assert.Equal(0UL, fldB.GetOutboxHighWatermark());
            Assert.Single(metadataB.ReadPendingApplyIntents(scope, nodeB.NodeId));
            StorageNodePersistence.Save(storageA, nodeA);
        }

        // Full process restart: reopen both FLD stores, both BIN-SYNC Storage
        // databases and the exact persisted Node images. Recovery replays the
        // PendingApply intent; FLD recognizes the already-committed ChangeId as
        // a duplicate, after which Metadata can safely finalize the receipt.
        using (var fldA = FldDatabase.Open(fldAPath, namespaceId, authorA))
        using (var fldB = FldDatabase.Open(fldBPath, namespaceId, authorB))
        using (var providerB = new FldDatabaseDataProvider(fldB, () => 1))
        using (var storageA = StorageDatabase.Open(storageAPath, 711))
        using (var storageB = StorageDatabase.Open(storageBPath, 712))
        using (var metadataB = new StorageMetadataStore(storageB))
        using (var nodeA = StorageNodePersistence.Load(storageA, NodeId.From(authorA)))
        using (var nodeB = StorageNodePersistence.Load(storageB, NodeId.From(authorB)))
        {
            var coordinatorB = new StorageSyncCoordinator(
                nodeB, providerB, namespaceId, metadataB, scope,
                remoteBatchCommitStore: new FldRemoteBatchApplyStore(fldB));

            Assert.Equal(1, coordinatorB.RecoverPendingApplies(
                node => StorageNodePersistence.Save(storageB, node)));
            Assert.Empty(metadataB.ReadPendingApplyIntents(scope, nodeB.NodeId));
            Assert.Equal(0, coordinatorB.RecoverPendingApplies(
                node => StorageNodePersistence.Save(storageB, node)));
            Assert.True(fldB.TryGetBytes(recordId, 0x0501, out byte[]? recovered));
            Assert.Equal(value, recovered);
            Assert.Equal(0UL, fldB.GetOutboxHighWatermark());

            // The sender still owns the unacknowledged immutable batch. Reconnect
            // and deliver it again: the receiver recognizes the finalized receipt,
            // returns a duplicate ACK, and does not echo or reapply the mutation.
            var (toA, toB) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(nodeB, toB);
            var connectTask = SyncSession.ConnectAsync(nodeA, toA);
            await using var sessionB = await acceptTask.WaitAsync(TimeSpan.FromSeconds(20));
            await using var sessionA = await connectTask.WaitAsync(TimeSpan.FromSeconds(20));
            StorageNodePersistence.Save(storageA, nodeA);
            StorageNodePersistence.Save(storageB, nodeB);

            Task<BatchResult> replayReceive = coordinatorB.ReceiveBatchBeforeAckAsync(
                sessionB, node => StorageNodePersistence.Save(storageB, node));
            Assert.True(await sessionA.PushBatchAsync(
                nodeA, namespaceId).WaitAsync(TimeSpan.FromSeconds(20)));
            BatchResult replayResult = await replayReceive.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.True(replayResult.Duplicate);
            StorageNodePersistence.Save(storageA, nodeA);
            Assert.True(fldB.TryGetBytes(recordId, 0x0501, out byte[]? afterReplay));
            Assert.Equal(value, afterReplay);
            Assert.Equal(0UL, fldB.GetOutboxHighWatermark());
        }
    }

    [Fact]
    public async Task Process_kill_after_fld_commit_before_metadata_finalize_recovers_and_duplicate_acks()
    {
        const uint namespaceId = 11;
        const string scope = "fld-process-kill-after-commit";
        byte[] authorA = Id(0xF1);
        byte[] authorB = Id(0xF2);
        byte[] recordId = Id(0xF3);
        string directory = Path.Combine(_directory, "fld-process-kill");
        string readyPath = Path.Combine(directory, "child-ready");
        string ownerPidPath = Path.Combine(directory, "child-owner-pid");
        string? projectDirectory = AppContext.BaseDirectory;
        while (projectDirectory is not null &&
               !File.Exists(Path.Combine(projectDirectory, "FldBinSyncHost.Tests.csproj")))
            projectDirectory = Directory.GetParent(projectDirectory)?.FullName;
        Assert.NotNull(projectDirectory);

        string configuration = Directory.GetParent(AppContext.BaseDirectory.TrimEnd(
            Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))?.Name ?? "Release";
        var start = new System.Diagnostics.ProcessStartInfo
        {
            FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet",
            WorkingDirectory = projectDirectory!,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        start.ArgumentList.Add("test");
        start.ArgumentList.Add(Path.Combine(projectDirectory!, "FldBinSyncHost.Tests.csproj"));
        start.ArgumentList.Add("--no-build");
        start.ArgumentList.Add("--no-restore");
        start.ArgumentList.Add("--configuration");
        start.ArgumentList.Add(configuration);
        start.ArgumentList.Add("--filter");
        start.ArgumentList.Add("FullyQualifiedName~Fld_process_kill_child_probe");
        start.ArgumentList.Add("--results-directory");
        start.ArgumentList.Add(Path.Combine(directory, "child-test-results"));
        start.Environment["BIN_SYNC_FLD_CRASH_DIR"] = directory;
        start.Environment["BIN_SYNC_FLD_CRASH_READY"] = readyPath;
        start.Environment["BIN_SYNC_FLD_CRASH_PID"] = ownerPidPath;

        System.Diagnostics.Process? child = null;
        try
        {
            child = System.Diagnostics.Process.Start(start);
            Assert.NotNull(child);
            Task<string> stdout = child!.StandardOutput.ReadToEndAsync();
            Task<string> stderr = child.StandardError.ReadToEndAsync();

            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!File.Exists(readyPath) && !child.HasExited && DateTime.UtcNow < deadline)
                await Task.Delay(25);
            if (!File.Exists(readyPath))
            {
                if (!child.HasExited)
                    child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                Assert.True(File.Exists(readyPath),
                    $"Child did not reach the post-FLD-commit crash cut. stdout={await stdout} stderr={await stderr}");
            }

            int ownerPid = int.Parse(File.ReadAllText(ownerPidPath));
            using (var owner = System.Diagnostics.Process.GetProcessById(ownerPid))
            {
                owner.Kill(entireProcessTree: true);
                await owner.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }
            if (!child.HasExited)
                child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));

            // Reopen every durable store after the owner process has been killed.
            using var fldA = FldDatabase.Open(Path.Combine(directory, "fld-a"), namespaceId, authorA);
            using var fldB = FldDatabase.Open(Path.Combine(directory, "fld-b"), namespaceId, authorB);
            using var providerB = new FldDatabaseDataProvider(fldB, () => 1);
            using var storageA = StorageDatabase.Open(Path.Combine(directory, "storage-a"), 721);
            using var storageB = StorageDatabase.Open(Path.Combine(directory, "storage-b"), 722);
            using var metadataB = new StorageMetadataStore(storageB);
            using var nodeA = StorageNodePersistence.Load(storageA, NodeId.From(authorA));
            using var nodeB = StorageNodePersistence.Load(storageB, NodeId.From(authorB));
            var coordinatorB = new StorageSyncCoordinator(
                nodeB, providerB, checked((int)namespaceId), metadataB, scope,
                remoteBatchCommitStore: new FldRemoteBatchApplyStore(fldB));

            Assert.Single(metadataB.ReadPendingApplyIntents(scope, nodeB.NodeId));
            Assert.True(fldB.TryGetBytes(recordId, 0x0601, out byte[]? committed));
            Assert.Equal("hard-kill-after-fld-commit"u8.ToArray(), committed);
            Assert.Equal(0UL, fldB.GetOutboxHighWatermark());

            Assert.Equal(1, coordinatorB.RecoverPendingApplies(
                node => StorageNodePersistence.Save(storageB, node)));
            Assert.Empty(metadataB.ReadPendingApplyIntents(scope, nodeB.NodeId));
            Assert.Equal(0, coordinatorB.RecoverPendingApplies(
                node => StorageNodePersistence.Save(storageB, node)));

            // The sender had no ACK at the kill point. Its pre-send Node checkpoint
            // must still contain the unpublished-to-peer operation; receiver recovery
            // is verified independently below. Duplicate retry/ACK is covered by the
            // paired exception-injection test, which keeps the sender process alive.
            Assert.Single(nodeA.ReadOutboxEntries(
                checked((int)namespaceId), nodeA.NodeId, 0, 10));
            StorageNodePersistence.Save(storageB, nodeB);
            Assert.True(fldB.TryGetBytes(recordId, 0x0601, out byte[]? afterRecovery));
            Assert.Equal("hard-kill-after-fld-commit"u8.ToArray(), afterRecovery);
            Assert.Equal(0UL, fldB.GetOutboxHighWatermark());
        }
        finally
        {
            if (child is not null)
            {
                try { if (!child.HasExited) child.Kill(entireProcessTree: true); } catch { }
                try { await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
                child.Dispose();
            }
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Fld_process_kill_child_probe()
    {
        string? directory = Environment.GetEnvironmentVariable("BIN_SYNC_FLD_CRASH_DIR");
        string? readyPath = Environment.GetEnvironmentVariable("BIN_SYNC_FLD_CRASH_READY");
        string? ownerPidPath = Environment.GetEnvironmentVariable("BIN_SYNC_FLD_CRASH_PID");
        if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(readyPath) ||
            string.IsNullOrWhiteSpace(ownerPidPath))
            return;

        const uint namespaceId = 11;
        const string scope = "fld-process-kill-after-commit";
        byte[] authorA = Id(0xF1);
        byte[] authorB = Id(0xF2);
        byte[] recordId = Id(0xF3);
        using var fldA = FldDatabase.Open(Path.Combine(directory, "fld-a"), namespaceId, authorA);
        using var fldB = FldDatabase.Open(Path.Combine(directory, "fld-b"), namespaceId, authorB);
        using var storageA = StorageDatabase.Open(Path.Combine(directory, "storage-a"), 721);
        using var storageB = StorageDatabase.Open(Path.Combine(directory, "storage-b"), 722);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var providerA = new FldDatabaseDataProvider(fldA, () => 1);
        using var providerB = new FldDatabaseDataProvider(fldB, () => 1);
        using var nodeA = new NodeBuilder().WithNodeId(NodeId.From(authorA)).WithEpoch(1).Build();
        using var nodeB = new NodeBuilder().WithNodeId(NodeId.From(authorB)).WithEpoch(1).Build();

        var coordinatorA = new StorageSyncCoordinator(
            nodeA, providerA, checked((int)namespaceId), metadataA, scope);
        providerA.AppendLocalChange(new ProviderMutation
        {
            RecordId = RecordId.From(recordId),
            FieldId = 0x0601,
            HlcWall = 6001,
            AuthorNodeId = NodeId.From(authorA),
            KeyEpoch = 0,
            Value = "hard-kill-after-fld-commit"u8.ToArray(),
            ChangeId = BatchId.From(Id(0xF4)),
            OriginAuthorEpoch = 1
        });
        Assert.Equal(1, coordinatorA.PublishLocalChanges(
            node => StorageNodePersistence.Save(storageA, node)));


        var (toA, toB) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, toB);
        var connectTask = SyncSession.ConnectAsync(nodeA, toA);
        await using var sessionB = await acceptTask;
        await using var sessionA = await connectTask;
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);

        Action signalAndBlock = () =>
        {
            File.WriteAllText(ownerPidPath, Environment.ProcessId.ToString(
                System.Globalization.CultureInfo.InvariantCulture));
            File.WriteAllText(readyPath, "after-fld-commit");
            Thread.Sleep(Timeout.Infinite);
        };
        var blockingStore = new ApplyAndBlockStore(new FldRemoteBatchApplyStore(fldB), signalAndBlock);
        var coordinatorB = new StorageSyncCoordinator(
            nodeB, providerB, checked((int)namespaceId), metadataB, scope,
            remoteBatchCommitStore: blockingStore);

        Task<BatchResult> receiveTask = coordinatorB.ReceiveBatchBeforeAckAsync(
            sessionB, node => StorageNodePersistence.Save(storageB, node));
        Task<bool> sendTask = sessionA.PushBatchAsync(nodeA, checked((int)namespaceId));
        await Task.WhenAll(receiveTask, sendTask);
    }

    [Fact]
    public async Task Metadata_owned_sender_reservation_and_peer_cursor_survive_full_restart()
    {
        const uint namespaceId = 12;
        const string scope = "fld-metadata-owned-restart";
        byte[] authorA = Id(0x71);
        byte[] authorB = Id(0x72);
        byte[] recordA = Id(0x73);
        byte[] recordB = Id(0x74);
        byte[] valueA = "metadata-owned-first"u8.ToArray();
        byte[] valueB = "metadata-owned-after-restart"u8.ToArray();
        string fldAPath = Path.Combine(_directory, "metadata-fld-a");
        string fldBPath = Path.Combine(_directory, "metadata-fld-b");
        string storageAPath = Path.Combine(_directory, "metadata-storage-a");
        string storageBPath = Path.Combine(_directory, "metadata-storage-b");

        // First process: publish one local operation, seal its WAL prefix, then
        // cross the durable Metadata-owned publication cutover and establish an
        // explicit empty-peer baseline before the first delta stream.
        using (var fldA = FldDatabase.Open(fldAPath, namespaceId, authorA))
        using (var fldB = FldDatabase.Open(fldBPath, namespaceId, authorB))
        using (var storageA = StorageDatabase.Open(storageAPath, 731))
        using (var storageB = StorageDatabase.Open(storageBPath, 732))
        using (var metadataA = new StorageMetadataStore(storageA))
        using (var metadataB = new StorageMetadataStore(storageB))
        using (var providerA = new FldDatabaseDataProvider(
            fldA, () => 1,
            publicationMode: new StorageMetadataPublicationMode(
                metadataA, scope, NodeId.From(authorA), checked((int)namespaceId))))
        using (var providerB = new FldDatabaseDataProvider(fldB, () => 1))
        using (var nodeA = new NodeBuilder().WithNodeId(NodeId.From(authorA)).WithEpoch(1).Build())
        using (var nodeB = new NodeBuilder().WithNodeId(NodeId.From(authorB)).WithEpoch(1).Build())
        {
            var remoteA = new FldRemoteBatchApplyStore(fldA);
            var remoteB = new FldRemoteBatchApplyStore(fldB);
            var coordinatorA = new StorageSyncCoordinator(
                nodeA, providerA, checked((int)namespaceId), metadataA, scope,
                remoteBatchCommitStore: remoteA);
            var coordinatorB = new StorageSyncCoordinator(
                nodeB, providerB, checked((int)namespaceId), metadataB, scope,
                remoteBatchCommitStore: remoteB);
            var mapA = new StorageRelayProvenanceMap(storageA, scope, nodeA.NodeId);
            var adapterA = new FldBinSyncHostAdapter(
                fldA, providerA, metadataA, coordinatorA, mapA, scope, remoteA);

            providerA.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(recordA),
                FieldId = 0x0701,
                HlcWall = 7001,
                AuthorNodeId = NodeId.From(authorA),
                KeyEpoch = 0,
                Value = valueA,
                ChangeId = BatchId.From(Id(0x75)),
                OriginAuthorEpoch = 1
            });
            Assert.Equal(1, coordinatorA.PublishLocalChanges(
                node => StorageNodePersistence.Save(storageA, node)));
            Assert.NotEqual(0UL, fldA.Checkpoint());
            Assert.Equal(fldA.PendingFlushedCommitSeq, fldA.LastSealedCommitSeq);
            coordinatorA.EnableMetadataStoreOwnedPublication(
                node => StorageNodePersistence.Save(storageA, node));

            var (toA, toB) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(nodeB, toB);
            var connectTask = SyncSession.ConnectAsync(nodeA, toA);
            await using var sessionB = await acceptTask.WaitAsync(TimeSpan.FromSeconds(20));
            await using var sessionA = await connectTask.WaitAsync(TimeSpan.FromSeconds(20));

            metadataA.RegisterInterestedPeer(
                scope, nodeA.NodeId, nodeB.NodeId, checked((uint)nodeB.Epoch),
                checked((int)namespaceId));
            metadataA.CompletePeerInitialSync(
                scope, nodeA.NodeId, nodeB.NodeId, checked((uint)nodeB.Epoch),
                checked((int)namespaceId),
                new[] { new MetadataPeerStreamBaseline(nodeA.NodeId, 0, checked((uint)nodeA.Epoch)) });
            StorageNodePersistence.Save(storageA, nodeA);
            StorageNodePersistence.Save(storageB, nodeB);

            Task<BatchResult> receiveAtB = coordinatorB.ReceiveBatchBeforeAckAsync(
                sessionB, node => StorageNodePersistence.Save(storageB, node));
            Assert.True(await adapterA.RecoverAndPublishAllAsync(
                node => StorageNodePersistence.Save(storageA, node),
                (watermarks, ct) => adapterA.PublishReadyOutboxAsync(
                    sessionA, nodeA, metadataA, watermarks, cancellationToken: ct)));
            BatchResult first = await receiveAtB.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(1, first.Applied);
            Assert.True(fldB.TryGetBytes(recordA, 0x0701, out byte[]? firstValue));
            Assert.Equal(valueA, firstValue);
            MetadataPeerStreamState firstState = metadataA.GetPeerStreamState(
                scope, nodeA.NodeId, nodeB.NodeId, checked((uint)nodeB.Epoch),
                checked((int)namespaceId), nodeA.NodeId);
            Assert.Equal(1UL, firstState.AckedCursor);
            Assert.Null(firstState.PendingBatch);
        }

        // Second process: restore both FLD stores, Metadata stores, provenance map
        // and Node images. Add another local draft after the cutover and publish it
        // through the same host publication gate using the persisted peer cursor.
        using (var fldA = FldDatabase.Open(fldAPath, namespaceId, authorA))
        using (var fldB = FldDatabase.Open(fldBPath, namespaceId, authorB))
        using (var storageA = StorageDatabase.Open(storageAPath, 731))
        using (var storageB = StorageDatabase.Open(storageBPath, 732))
        using (var metadataA = new StorageMetadataStore(storageA))
        using (var metadataB = new StorageMetadataStore(storageB))
        using (var providerA = new FldDatabaseDataProvider(
            fldA, () => 1,
            publicationMode: new StorageMetadataPublicationMode(
                metadataA, scope, NodeId.From(authorA), checked((int)namespaceId))))
        using (var providerB = new FldDatabaseDataProvider(fldB, () => 1))
        using (var nodeA = StorageNodePersistence.Load(storageA, NodeId.From(authorA)))
        using (var nodeB = StorageNodePersistence.Load(storageB, NodeId.From(authorB)))
        {
            var remoteA = new FldRemoteBatchApplyStore(fldA);
            var remoteB = new FldRemoteBatchApplyStore(fldB);
            var coordinatorA = new StorageSyncCoordinator(
                nodeA, providerA, checked((int)namespaceId), metadataA, scope,
                remoteBatchCommitStore: remoteA);
            var coordinatorB = new StorageSyncCoordinator(
                nodeB, providerB, checked((int)namespaceId), metadataB, scope,
                remoteBatchCommitStore: remoteB);
            var mapA = new StorageRelayProvenanceMap(storageA, scope, nodeA.NodeId);
            var adapterA = new FldBinSyncHostAdapter(
                fldA, providerA, metadataA, coordinatorA, mapA, scope, remoteA);

            providerA.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(recordB),
                FieldId = 0x0701,
                HlcWall = 7002,
                AuthorNodeId = NodeId.From(authorA),
                KeyEpoch = 0,
                Value = valueB,
                ChangeId = BatchId.From(Id(0x76)),
                OriginAuthorEpoch = 1
            });
            Assert.Equal(1, adapterA.PublishLocalChanges(
                node => StorageNodePersistence.Save(storageA, node)));

            var (toA, toB) = PipeTransport.CreatePair();
            var acceptTask = SyncSession.AcceptAsync(nodeB, toB);
            var connectTask = SyncSession.ConnectAsync(nodeA, toA);
            await using var sessionB = await acceptTask.WaitAsync(TimeSpan.FromSeconds(20));
            await using var sessionA = await connectTask.WaitAsync(TimeSpan.FromSeconds(20));
            StorageNodePersistence.Save(storageA, nodeA);
            StorageNodePersistence.Save(storageB, nodeB);

            Task<BatchResult> receiveAtB = coordinatorB.ReceiveBatchBeforeAckAsync(
                sessionB, node => StorageNodePersistence.Save(storageB, node));
            Assert.True(await adapterA.RecoverAndPublishAllAsync(
                node => StorageNodePersistence.Save(storageA, node),
                (watermarks, ct) => adapterA.PublishReadyOutboxAsync(
                    sessionA, nodeA, metadataA, watermarks, cancellationToken: ct)));
            BatchResult second = await receiveAtB.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(1, second.Applied);
            Assert.True(fldB.TryGetBytes(recordA, 0x0701, out byte[]? persistedFirst));
            Assert.Equal(valueA, persistedFirst);
            Assert.True(fldB.TryGetBytes(recordB, 0x0701, out byte[]? persistedSecond));
            Assert.Equal(valueB, persistedSecond);

            MetadataPeerStreamState finalState = metadataA.GetPeerStreamState(
                scope, nodeA.NodeId, nodeB.NodeId, checked((uint)nodeB.Epoch),
                checked((int)namespaceId), nodeA.NodeId);
            Assert.Equal(2UL, finalState.AckedCursor);
            Assert.Null(finalState.PendingBatch);
            Assert.Equal(0UL, fldB.GetOutboxHighWatermark());
        }
    }

    [Fact]
    public void Remote_record_and_field_tombstones_keep_distinct_fld_lifecycle_semantics()
    {
        const uint namespaceId = 9;
        byte[] localAuthor = Id(0x91);
        byte[] issuer = Id(0x92);
        byte[] recordTombstoneId = Id(0x93);
        byte[] fieldTombstoneId = Id(0x94);
        var mutations = new[]
        {
            new ProviderMutation
            {
                RecordId = RecordId.From(recordTombstoneId),
                FieldId = 0,
                IsRecordTombstone = true,
                HlcWall = 2001,
                AuthorNodeId = NodeId.From(issuer),
                Value = Array.Empty<byte>(),
                ChangeId = BatchId.From(Id(0x95)),
                OriginAuthorEpoch = 3,
                OriginSequence = 1
            },
            new ProviderMutation
            {
                RecordId = RecordId.From(fieldTombstoneId),
                FieldId = 0x0401,
                IsTombstone = true,
                HlcWall = 2002,
                AuthorNodeId = NodeId.From(issuer),
                Value = Array.Empty<byte>(),
                ChangeId = BatchId.From(Id(0x96)),
                OriginAuthorEpoch = 3,
                OriginSequence = 2
            }
        };
        var batch = new ProviderRemoteBatchCommit(
            Guid.NewGuid(),
            NodeId.From(Id(0x97)),
            senderTransportEpoch: 2,
            namespaceId: checked((int)namespaceId),
            NodeId.From(issuer),
            issuerEpoch: 3,
            BatchId.From(Id(0x98)),
            cursorStart: 1,
            cursorEnd: 2,
            new byte[16],
            Id(0x99),
            Id(0x9A),
            protocolStateDelta: Array.Empty<byte>(),
            mutations);

        using var database = FldDatabase.Open(_directory, namespaceId, localAuthor);
        var adapter = new FldRemoteBatchApplyStore(database);
        Assert.False(adapter.ApplyRemoteBatch(batch).IsDuplicate);

        Assert.True(database.TryGetBytes(
            recordTombstoneId, FldLifecycle.FieldId, out byte[]? lifecycle));
        Assert.Equal(FldLifecycle.DeadValue.ToArray(), lifecycle);
        Assert.True(database.TryResolve(fieldTombstoneId, 0x0401, out var field));
        Assert.True(field.Found);
        Assert.True(field.IsTombstone);
    }

    private sealed class ApplyAndBlockStore : IProviderRemoteBatchCommitStore
    {
        private readonly IProviderRemoteBatchCommitStore _inner;
        private readonly Action _block;

        public ApplyAndBlockStore(IProviderRemoteBatchCommitStore inner, Action block)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _block = block ?? throw new ArgumentNullException(nameof(block));
        }

        public ProviderApplyReceipt ApplyRemoteBatch(ProviderRemoteBatchCommit batch)
        {
            ProviderApplyReceipt receipt = _inner.ApplyRemoteBatch(batch);
            _block();
            return receipt;
        }
    }

    private sealed class ApplyThenThrowOnceStore : IProviderRemoteBatchCommitStore
    {
        private readonly IProviderRemoteBatchCommitStore _inner;
        private int _hasThrown;

        public ApplyThenThrowOnceStore(IProviderRemoteBatchCommitStore inner)
            => _inner = inner ?? throw new ArgumentNullException(nameof(inner));

        public ProviderApplyReceipt ApplyRemoteBatch(ProviderRemoteBatchCommit batch)
        {
            ProviderApplyReceipt receipt = _inner.ApplyRemoteBatch(batch);
            if (Interlocked.Exchange(ref _hasThrown, 1) == 0)
                throw new SimulatedProcessCrashException();
            return receipt;
        }
    }

    private sealed class SimulatedProcessCrashException()
        : Exception("Injected crash after durable FLD APPLY and before metadata receipt finalization.");

    private static byte[] Id(byte first)
    {
        byte[] bytes = new byte[16];
        bytes[0] = first;
        bytes[15] = (byte)(first ^ 0xA5);
        return bytes;
    }
}
