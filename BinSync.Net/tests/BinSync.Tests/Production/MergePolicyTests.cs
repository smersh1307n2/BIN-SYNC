// Extra merge policies: MAX/MIN bytes and u64.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class MergePolicyTests
{
    static FieldState Fs(byte[] value, ulong wall, uint logical, byte authorTail)
    {
        var author = new byte[16];
        author[15] = authorTail;
        return new FieldState(value, new Hlc(wall, logical), author);
    }

    [Fact]
    public void MaxBytes_prefers_greater_value_over_newer_hlc()
    {
        var olderBigger = Fs(new byte[] { 0x10 }, wall: 1, logical: 0, authorTail: 1);
        var newerSmaller = Fs(new byte[] { 0x01 }, wall: 99, logical: 0, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(olderBigger, newerSmaller, MergePolicy.MaxBytes);
        Assert.True(changed);
        Assert.Equal(new byte[] { 0x10 }, w.Value);
    }

    [Fact]
    public void MinBytes_prefers_lesser_value()
    {
        var a = Fs(new byte[] { 0x05 }, wall: 1, logical: 0, authorTail: 1);
        var b = Fs(new byte[] { 0x02 }, wall: 1, logical: 1, authorTail: 2);
        var (w, _) = Merge.MergeFieldWithPolicy(a, b, MergePolicy.MinBytes);
        Assert.Equal(new byte[] { 0x02 }, w.Value);
    }

    [Fact]
    public void MaxU64_numeric_compare()
    {
        // LE: 100 vs 200
        var low = Fs(BitConverter.GetBytes(100UL), wall: 5, logical: 0, authorTail: 1);
        var high = Fs(BitConverter.GetBytes(200UL), wall: 1, logical: 0, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(low, high, MergePolicy.MaxU64);
        Assert.True(changed);
        Assert.Equal(200UL, Merge.AsU64(w.Value));
    }

    [Fact]
    public void MinU64_numeric_compare()
    {
        var low = Fs(BitConverter.GetBytes(10UL), wall: 1, logical: 0, authorTail: 1);
        var high = Fs(BitConverter.GetBytes(50UL), wall: 9, logical: 0, authorTail: 2);
        var (w, _) = Merge.MergeFieldWithPolicy(high, low, MergePolicy.MinU64);
        Assert.Equal(10UL, Merge.AsU64(w.Value));
    }

    [Fact]
    public void Equal_values_fall_back_to_LWW()
    {
        var older = Fs(new byte[] { 0xAA }, wall: 1, logical: 0, authorTail: 1);
        var newer = Fs(new byte[] { 0xAA }, wall: 2, logical: 0, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(older, newer, MergePolicy.MaxBytes);
        Assert.True(changed);
        Assert.Equal(2UL, w.Timestamp.Wall);
        Assert.Equal(2, w.AuthorNodeId[15]);
    }

    [Fact]
    public void Lww_unchanged_default_behaviour()
    {
        var older = Fs(new byte[] { 0xFF }, wall: 1, logical: 0, authorTail: 1);
        var newer = Fs(new byte[] { 0x00 }, wall: 2, logical: 0, authorTail: 2);
        var (w, changed) = Merge.MergeFieldWithPolicy(older, newer, MergePolicy.LwwField);
        Assert.True(changed);
        Assert.Equal(new byte[] { 0x00 }, w.Value);
    }

    [Fact]
    public void Echo_suppression_still_applies()
    {
        var a = Fs(new byte[] { 1 }, wall: 3, logical: 1, authorTail: 7);
        var b = Fs(new byte[] { 1 }, wall: 3, logical: 1, authorTail: 7);
        var (_, changed) = Merge.MergeFieldWithPolicy(a, b, MergePolicy.MaxBytes);
        Assert.False(changed);
    }

    [Fact]
    public void Registry_defaults_to_Lww()
    {
        var reg = new MergePolicyRegistry();
        Assert.Equal(MergePolicy.LwwField, reg.Get(1, 0x100));
        reg.Set(1, 0x100, MergePolicy.MaxU64);
        Assert.Equal(MergePolicy.MaxU64, reg.Get(1, 0x100));
        Assert.Equal(MergePolicy.LwwField, reg.Get(1, 0x101));
        Assert.True(reg.Remove(1, 0x100));
        Assert.Equal(MergePolicy.LwwField, reg.Get(1, 0x100));
    }

    [Fact]
    public void ApplyFieldWrite_respects_policy()
    {
        var rec = new Record(new byte[16], 1);
        var low = Fs(BitConverter.GetBytes(1UL), wall: 10, logical: 0, authorTail: 1);
        var high = Fs(BitConverter.GetBytes(99UL), wall: 1, logical: 0, authorTail: 2);
        Assert.True(Merge.ApplyFieldWrite(rec, 0x100, low, MergePolicy.MaxU64));
        Assert.True(Merge.ApplyFieldWrite(rec, 0x100, high, MergePolicy.MaxU64));
        Assert.Equal(99UL, Merge.AsU64(rec.State[0x100].Value));
    }
}
