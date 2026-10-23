// IBLT unit tests: insert/peel, subtract set-diff, encode roundtrip.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class IbltTests
{
    [Fact]
    public void Iblt_insert_peel_single()
    {
        var t = new Iblt(16);
        var k = Iblt.NormalizeKey(Fix.Rid(1));
        t.Insert(k);
        var (local, remote) = t.Peel();
        Assert.Single(local);
        Assert.Empty(remote);
        Assert.True(Fix.Eq(k, local[0]));
    }

    [Fact]
    public void Iblt_subtract_recovers_symmetric_difference()
    {
        var a = new Iblt(32);
        var b = new Iblt(32);
        var kA = Iblt.NormalizeKey(Fix.Rid(10));
        var kB = Iblt.NormalizeKey(Fix.Rid(11));
        var kShared = Iblt.NormalizeKey(Fix.Rid(12));
        a.Insert(kA); a.Insert(kShared);
        b.Insert(kB); b.Insert(kShared);

        a.Subtract(b);
        var (onlyA, onlyB) = a.Peel();
        Assert.Contains(onlyA, x => Fix.Eq(x, kA));
        Assert.Contains(onlyB, x => Fix.Eq(x, kB));
        Assert.DoesNotContain(onlyA, x => Fix.Eq(x, kShared));
        Assert.DoesNotContain(onlyB, x => Fix.Eq(x, kShared));
    }

    [Fact]
    public void Iblt_encode_decode_roundtrip()
    {
        var t = new Iblt(16);
        t.Insert(Fix.Rid(20));
        t.Insert(Fix.Rid(21));
        var back = Iblt.Decode(Iblt.Encode(t));
        Assert.Equal(t.CellCount, back.CellCount);
        for (int i = 0; i < t.CellCount; i++)
        {
            Assert.Equal(t.Cells[i].Count, back.Cells[i].Count);
            Assert.Equal(t.Cells[i].HashXor, back.Cells[i].HashXor);
            Assert.Equal(t.Cells[i].KeyXor, back.Cells[i].KeyXor);
        }
    }

    [Fact]
    public void Iblt_BuildFromNode_peels_missing_on_peer()
    {
        var a = Fix.MakeNode(70);
        var b = Fix.MakeNode(71);
        var ridOnlyA = Fix.Rid(90);
        var ridOnlyB = Fix.Rid(91);
        a.WriteField(1, ridOnlyA, 0x100, new byte[] { 1 });
        b.WriteField(1, ridOnlyB, 0x100, new byte[] { 2 });

        var ta = Iblt.BuildFromNode(a, 1, cellCount: 32);
        var tb = Iblt.BuildFromNode(b, 1, cellCount: 32);
        ta.Subtract(tb);
        var (onlyA, onlyB) = ta.Peel();
        Assert.Contains(onlyA, x => Fix.Eq(Iblt.NormalizeKey(ridOnlyA), Iblt.NormalizeKey(x)));
        Assert.Contains(onlyB, x => Fix.Eq(Iblt.NormalizeKey(ridOnlyB), Iblt.NormalizeKey(x)));
    }
}
