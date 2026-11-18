using BinSync;
using BinSync.Storage;\nusing Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class LiveRecordReferenceFilterTests
{
    [Fact]
    public void Bloom_filter_has_no_false_negatives_for_250k_deterministic_record_ids()
    {
        const int count = 250_000;
        var filter = new LiveRecordReferenceFilter(count);
        var inserted = new RecordId[count];

        for (int i = 0; i < count; i++)
        {
            var id = new RecordId((ulong)i + 1, ((ulong)i << 32) ^ 0x9E3779B97F4A7C15UL);
            inserted[i] = id;
            filter.Add(id);
        }

        foreach (RecordId id in inserted)
            Assert.True(filter.MightContain(id), $"Bloom filter produced a false negative for {id}.");

        Assert.False(filter.MightContain(default));

        int falsePositives = 0;
        for (int i = 0; i < 100_000; i++)
        {
            var absent = new RecordId((ulong)i + 0x1_0000_0000UL,
                ((ulong)i << 32) ^ 0xD1B54A32D192ED03UL);
            if (filter.MightContain(absent))
                falsePositives++;
        }

        Assert.True(falsePositives < 100,
            $"Expected a low false-positive rate, observed {falsePositives} / 100000.");
    }

    [Fact]
    public void Bloom_prefilter_refuses_pushed_but_unacknowledged_peer_reservation()
    {
        static byte[] Id(byte value)
        {
            byte[] bytes = new byte[16];
            bytes[15] = value;
            return bytes;
        }

        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-live-record-filter-reservation-" + Guid.NewGuid().ToString("N"));
        const string scope = "live-record-filter-reservation";
        const int namespaceId = 19;
        NodeId local = NodeId.From(Id(0x41));
        NodeId peer = NodeId.From(Id(0x42));
        try
        {
            using var storage = StorageDatabase.Open(directory, 954);
            using var metadata = new StorageMetadataStore(storage);

            metadata.RegisterInterestedPeer(scope, local, peer, 1, namespaceId);
            metadata.CompletePeerInitialSync(scope, local, peer, 1, namespaceId,
                new[] { new MetadataPeerStreamBaseline(peer, 0, 3) });
            var reservation = new MetadataOutboundBatchReservation
            {
                BatchId = BatchId.From(Id(0x43)),
                IssuerEpoch = 3,
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)0x44, 16).ToArray(),
                Payload = new byte[] { 0x45 }
            };
            metadata.ReserveOutboundBatch(
                scope, local, peer, 1, namespaceId, peer, reservation);
            metadata.MarkOutboundBatchPushed(
                scope, local, peer, 1, namespaceId, peer, reservation.BatchId);

            Assert.False(metadata.TryCreateLiveRecordReferenceFilter(
                scope, local, namespaceId, out _));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Empty_filter_never_matches()
    {
        var filter = new LiveRecordReferenceFilter(0);
        Assert.False(filter.MightContain(new RecordId(1, 2)));
    }
}
