using BinSync;
using BinSync.FldHost;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Fld1.Engine.Db;
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
            Assert.Equal(0UL, reopened.GetOutboxHighWatermark());
            Assert.Empty(reopened.ReadRelayBatches(issuer, afterJournalSeq: 0, limit: 10));

            var duplicateAfterRestart = adapter.ApplyRemoteBatch(batch);
            Assert.True(duplicateAfterRestart.IsDuplicate);
            Assert.Equal(firstRelayWatermark, reopened.GetRelayJournalHighWatermark());
        }
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
    public async Task Two_fld_databases_exchange_local_changes_through_binsync_sessions()
    {
        const int namespaceId = 7;
        const string scope = "fld-host-two-node-roundtrip";
        byte[] authorA = Id(0xA1);
        byte[] authorB = Id(0xB1);
        byte[] recordA = Id(0xA2);
        byte[] recordB = Id(0xB2);
        byte[] valueA = "from-fld-a"u8.ToArray();
        byte[] valueB = "from-fld-b"u8.ToArray();

        using var fldA = FldDatabase.Open(Path.Combine(_directory, "fld-a"), (uint)namespaceId, authorA);
        using var fldB = FldDatabase.Open(Path.Combine(_directory, "fld-b"), (uint)namespaceId, authorB);
        using var providerA = new FldDatabaseDataProvider(fldA, () => 1);
        using var providerB = new FldDatabaseDataProvider(fldB, () => 1);
        using var storageA = StorageDatabase.Open(Path.Combine(_directory, "binsync-a"), 701);
        using var storageB = StorageDatabase.Open(Path.Combine(_directory, "binsync-b"), 702);
        using var metadataA = new StorageMetadataStore(storageA);
        using var metadataB = new StorageMetadataStore(storageB);
        using var nodeA = new NodeBuilder().WithNodeId(authorA).WithEpoch(1).Build();
        using var nodeB = new NodeBuilder().WithNodeId(authorB).WithEpoch(1).Build();

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
            HlcWall = 1001,
            AuthorNodeId = NodeId.From(authorA),
            KeyEpoch = 1,
            Value = valueA,
            ChangeId = BatchId.From(Id(0xA3)),
            OriginAuthorEpoch = 1
        });
        providerB.AppendLocalChange(new ProviderMutation
        {
            RecordId = RecordId.From(recordB),
            FieldId = 0x0301,
            HlcWall = 1002,
            AuthorNodeId = NodeId.From(authorB),
            KeyEpoch = 1,
            Value = valueB,
            ChangeId = BatchId.From(Id(0xB3)),
            OriginAuthorEpoch = 1
        });

        Assert.Equal(1, coordinatorA.PublishLocalChanges(node => StorageNodePersistence.Save(storageA, node)));
        Assert.Equal(1, coordinatorB.PublishLocalChanges(node => StorageNodePersistence.Save(storageB, node)));

        var (toA, toB) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(nodeB, toB);
        var connectTask = SyncSession.ConnectAsync(nodeA, toA);
        await using var sessionB = await acceptTask;
        await using var sessionA = await connectTask;
        // Persist negotiated peer aliases before the durable receive-before-ACK path.
        StorageNodePersistence.Save(storageA, nodeA);
        StorageNodePersistence.Save(storageB, nodeB);

        Task<BatchResult> receiveAtB = coordinatorB.ReceiveBatchBeforeAckAsync(
            sessionB, node => StorageNodePersistence.Save(storageB, node));
        Assert.True(await sessionA.PushBatchAsync(nodeA, namespaceId));
        BatchResult resultAtB = await receiveAtB;
        Assert.Equal(1, resultAtB.Applied);
        Assert.True(fldB.TryGetBytes(recordA, 0x0301, out byte[]? receivedAtB));
        Assert.Equal(valueA, receivedAtB);

        Task<BatchResult> receiveAtA = coordinatorA.ReceiveBatchBeforeAckAsync(
            sessionA, node => StorageNodePersistence.Save(storageA, node));
        Assert.True(await sessionB.PushBatchAsync(nodeB, namespaceId));
        BatchResult resultAtA = await receiveAtA;
        Assert.Equal(1, resultAtA.Applied);
        Assert.True(fldA.TryGetBytes(recordB, 0x0301, out byte[]? receivedAtA));
        Assert.Equal(valueB, receivedAtA);

        Assert.True(providerA.TryReadCurrent(RecordId.From(recordB), 0x0301, out ProviderMutation? currentA));
        Assert.Equal(valueB, currentA!.Value);
        Assert.True(providerB.TryReadCurrent(RecordId.From(recordA), 0x0301, out ProviderMutation? currentB));
        Assert.Equal(valueA, currentB!.Value);
    }

    private static byte[] Id(byte first)
    {
        byte[] bytes = new byte[16];
        bytes[0] = first;
        bytes[15] = (byte)(first ^ 0xA5);
        return bytes;
    }
}
