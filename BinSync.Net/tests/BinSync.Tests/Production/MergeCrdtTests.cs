// §8.4–8.9 COUNTER_G / COUNTER_PN / SET_OR / MANUAL.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class MergeCrdtTests
{
    static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    static FieldState Fs(byte[] value, ulong wall, byte authorTail) =>
        new(value, new Hlc(wall, 0), Author(authorTail));

    [Fact]
    public void CounterG_per_author_max()
    {
        var a = Author(1);
        var b = Author(2);
        var left = CounterG.Encode(new Dictionary<byte[], ulong>(new BA()) { [a] = 3, [b] = 1 });
        var right = CounterG.Encode(new Dictionary<byte[], ulong>(new BA()) { [a] = 2, [b] = 5 });
        var merged = CounterG.Merge(left, right);
        var map = CounterG.Decode(merged);
        Assert.Equal(3UL, map[a]);
        Assert.Equal(5UL, map[b]);
        Assert.Equal(8UL, CounterG.Total(merged));
    }

    [Fact]
    public void CounterG_field_merge_policy()
    {
        var a = Author(1);
        var local = Fs(CounterG.Encode(new Dictionary<byte[], ulong>(new BA()) { [a] = 1 }), wall: 1, authorTail: 1);
        var remote = Fs(CounterG.Encode(new Dictionary<byte[], ulong>(new BA()) { [a] = 4 }), wall: 2, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.CounterG);
        Assert.True(changed);
        Assert.Equal(4UL, CounterG.Total(w.Value));
    }

    [Fact]
    public void CounterPn_value_is_pos_minus_neg()
    {
        var a = Author(1);
        var b = Author(2);
        var payload = CounterPn.Encode(
            new Dictionary<byte[], ulong>(new BA()) { [a] = 10, [b] = 3 },
            new Dictionary<byte[], ulong>(new BA()) { [a] = 2 });
        Assert.Equal(11L, CounterPn.Value(payload)); // 10+3-2
    }

    [Fact]
    public void CounterPn_merge_componentwise()
    {
        var a = Author(1);
        var left = CounterPn.Encode(
            new Dictionary<byte[], ulong>(new BA()) { [a] = 5 },
            new Dictionary<byte[], ulong>(new BA()) { [a] = 1 });
        var right = CounterPn.Encode(
            new Dictionary<byte[], ulong>(new BA()) { [a] = 3 },
            new Dictionary<byte[], ulong>(new BA()) { [a] = 4 });
        var merged = CounterPn.Merge(left, right);
        // pos max=5, neg max=4 → value 1
        Assert.Equal(1L, CounterPn.Value(merged));
    }

    [Fact]
    public void SetOr_visible_excludes_removed_dots()
    {
        var actor = Author(1);
        var adds = new List<SetOr.Add>
        {
            new() { Element = new byte[] { 0xAA }, Actor = actor, Seq = 1 },
            new() { Element = new byte[] { 0xBB }, Actor = actor, Seq = 2 },
        };
        var rems = new List<SetOr.Dot> { new() { Actor = actor, Seq = 1 } };
        var payload = SetOr.Encode(adds, rems);
        var visible = SetOr.Visible(payload);
        Assert.Single(visible);
        Assert.Equal(new byte[] { 0xBB }, visible[0]);
    }

    [Fact]
    public void SetOr_merge_unions_adds_and_removes()
    {
        var a1 = Author(1);
        var a2 = Author(2);
        var left = SetOr.Encode(
            new[] { new SetOr.Add { Element = new byte[] { 1 }, Actor = a1, Seq = 1 } },
            Array.Empty<SetOr.Dot>());
        var right = SetOr.Encode(
            new[] { new SetOr.Add { Element = new byte[] { 2 }, Actor = a2, Seq = 1 } },
            new[] { new SetOr.Dot { Actor = a1, Seq = 1 } });
        var merged = SetOr.Merge(left, right);
        var visible = SetOr.Visible(merged);
        Assert.Single(visible);
        Assert.Equal(new byte[] { 2 }, visible[0]);
    }

    [Fact]
    public void Manual_does_not_overwrite_divergent_value()
    {
        var local = Fs(new byte[] { 0x01 }, wall: 1, authorTail: 1);
        var remote = Fs(new byte[] { 0x02 }, wall: 99, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.Manual);
        Assert.False(changed);
        Assert.Equal(new byte[] { 0x01 }, w.Value);
    }

    [Fact]
    public void Manual_advances_metadata_when_value_identical()
    {
        var local = Fs(new byte[] { 0xAA }, wall: 1, authorTail: 1);
        var remote = Fs(new byte[] { 0xAA }, wall: 5, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.Manual);
        Assert.True(changed);
        Assert.Equal(5UL, w.Timestamp.Wall);
        Assert.Equal(new byte[] { 0xAA }, w.Value);
    }

    [Fact]
    public void Spec_wire_codes_match()
    {
        Assert.Equal(0x03, (byte)MergePolicy.CounterG);
        Assert.Equal(0x04, (byte)MergePolicy.CounterPn);
        Assert.Equal(0x05, (byte)MergePolicy.SetOr);
        Assert.Equal(0x09, (byte)MergePolicy.Manual);
        Assert.Equal(Model.FieldResolveId, 0x00FF);
    }
}
