using BinSync;
using BinSync.FldHost;
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

    private static byte[] Id(byte first)
    {
        byte[] bytes = new byte[16];
        bytes[0] = first;
        bytes[15] = (byte)(first ^ 0xA5);
        return bytes;
    }
}
