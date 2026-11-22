using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class MergeFieldTombTests
{
    private static readonly RecordId TestRecordId = RecordId.From(new byte[16]);
    private static readonly NodeId AuthorA = NodeId.From(new byte[] { 1 });
    private static readonly NodeId AuthorB = NodeId.From(new byte[] { 2 });

    private static FieldState Value(string value, ulong wall, NodeId author) =>
        new(System.Text.Encoding.UTF8.GetBytes(value), new Hlc(wall, 0), author);

    private static FieldState Tomb(ulong wall, NodeId author) =>
        new(Array.Empty<byte>(), new Hlc(wall, 0), author);

    [Fact]
    public void Newer_field_tombstone_rejects_older_write()
    {
        var record = new Record(TestRecordId, 1);
        Assert.True(Merge.ApplyFieldTomb(record, 0x0100, Tomb(10, AuthorA)));

        Assert.False(Merge.ApplyFieldWrite(record, 0x0100, Value("stale", 9, AuthorB)));
        Assert.False(record.State.ContainsKey(0x0100));
        Assert.True(record.HasFieldTombs);
    }

    [Fact]
    public void Newer_write_supersedes_and_removes_field_tombstone()
    {
        var record = new Record(TestRecordId, 1);
        Assert.True(Merge.ApplyFieldTomb(record, 0x0100, Tomb(10, AuthorA)));

        Assert.True(Merge.ApplyFieldWrite(record, 0x0100, Value("new", 11, AuthorB)));
        Assert.True(record.State.ContainsKey(0x0100));
        Assert.False(record.HasFieldTombs);
        Assert.Equal("new", System.Text.Encoding.UTF8.GetString(record.State[0x0100].Value));
    }

    [Fact]
    public void Older_tombstone_does_not_replace_newer_value_or_remain_in_digest_state()
    {
        var record = new Record(TestRecordId, 1);
        Assert.True(Merge.ApplyFieldWrite(record, 0x0100, Value("new", 10, AuthorA)));

        Assert.False(Merge.ApplyFieldTomb(record, 0x0100, Tomb(9, AuthorB)));
        Assert.True(record.State.ContainsKey(0x0100));
        Assert.False(record.HasFieldTombs);
    }

    [Fact]
    public void Record_digest_distinguishes_empty_value_from_tombstone_with_same_metadata()
    {
        var timestamp = new Hlc(10, 2);
        var valueRecord = new Record(TestRecordId, 1);
        valueRecord.State[0x0100] = new FieldState(Array.Empty<byte>(), timestamp, AuthorA);

        var tombRecord = new Record(TestRecordId, 1);
        tombRecord.FieldTombs[0x0100] = new FieldState(Array.Empty<byte>(), timestamp, AuthorA);

        var valueDigest = Digests.RecordDigest(valueRecord);
        var tombDigest = Digests.RecordDigest(tombRecord);
        Assert.False(valueDigest.AsSpan().SequenceEqual(tombDigest));
        Assert.Equal("BIN-SYNC/RECORD/v2", System.Text.Encoding.ASCII.GetString(Domains.DomainRecord));
    }

    [Fact]
    public void Record_digest_does_not_materialize_empty_lazy_tomb_dictionary()
    {
        var record = new Record(TestRecordId, 1);
        Assert.False(record.HasFieldTombs);

        _ = Digests.RecordDigest(record);

        Assert.False(record.HasFieldTombs);
    }
}
