using BinSync;
using BinSync.Storage;
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
    public void Empty_filter_never_matches()
    {
        var filter = new LiveRecordReferenceFilter(0);
        Assert.False(filter.MightContain(new RecordId(1, 2)));
    }
}
