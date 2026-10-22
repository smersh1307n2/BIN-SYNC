// P1: STATE_DIGEST build / round-trip / divergence detection.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ReconcileTests
{
    [Fact]
    public void Identical_nodes_share_root_digest()
    {
        var a = Fix.MakeNode(1);
        var b = Fix.MakeNode(2);
        a.WriteField(1, Fix.Rid(10), 0x100, new byte[] { 1, 2 });
        // Copy state onto b via in-process sync
        Transport.MakeSession(a, b);
        Transport.SyncRound(a, b, 1);

        var da = Reconcile.BuildStateDigest(a, 1);
        var db = Reconcile.BuildStateDigest(b, 1);
        Assert.True(da.RootDigest.AsSpan().SequenceEqual(db.RootDigest));
        Assert.Empty(Reconcile.DivergentPrefixes(da, db));
    }

    [Fact]
    public void Divergent_field_produces_different_root_and_prefix()
    {
        var a = Fix.MakeNode(3);
        var b = Fix.MakeNode(4);
        a.WriteField(1, Fix.Rid(20), 0x100, new byte[] { 9 });
        b.WriteField(1, Fix.Rid(20), 0x100, new byte[] { 8 });

        var da = Reconcile.BuildStateDigest(a, 1);
        var db = Reconcile.BuildStateDigest(b, 1);
        Assert.False(da.RootDigest.AsSpan().SequenceEqual(db.RootDigest));
        var diverged = Reconcile.DivergentPrefixes(da, db);
        Assert.NotEmpty(diverged);
    }

    [Fact]
    public void StateDigest_wire_roundtrip()
    {
        var a = Fix.MakeNode(5);
        a.WriteField(1, Fix.Rid(1), 0x10, new byte[] { 7, 7, 7 });
        var msg = Reconcile.BuildStateDigest(a, 1);
        var raw = Reconcile.EncodeStateDigest(msg);
        var back = Reconcile.DecodeStateDigest(raw);
        Assert.Equal(msg.NamespaceId, back.NamespaceId);
        Assert.Equal(msg.OutboxCursor, back.OutboxCursor);
        Assert.Equal(msg.RadixBits, back.RadixBits);
        Assert.True(msg.RootDigest.AsSpan().SequenceEqual(back.RootDigest));
        Assert.Equal(msg.Buckets.Count, back.Buckets.Count);
    }

    [Fact]
    public void NeedRange_and_NeedKeys_roundtrip()
    {
        var nr = new NeedRangeMessage { NamespaceId = 1, Prefix = new byte[] { 0xAB } };
        var nr2 = Reconcile.DecodeNeedRange(Reconcile.EncodeNeedRange(nr));
        Assert.Equal(1, nr2.NamespaceId);
        Assert.Equal(new byte[] { 0xAB }, nr2.Prefix);

        var nk = new NeedKeysMessage
        {
            NamespaceId = 2,
            RecordIds = { Fix.Rid(3), Fix.Rid(4) },
        };
        var nk2 = Reconcile.DecodeNeedKeys(Reconcile.EncodeNeedKeys(nk));
        Assert.Equal(2, nk2.NamespaceId);
        Assert.Equal(2, nk2.RecordIds.Count);
        Assert.True(Fix.Eq(Fix.Rid(3), nk2.RecordIds[0]));
    }

    [Fact]
    public void LocalKeysUnderPrefix_finds_record()
    {
        var a = Fix.MakeNode(6);
        var rid = Fix.Rid(99);
        a.WriteField(1, rid, 0x100, new byte[] { 1 });
        var key = BucketAccumulator.BucketKey(rid);
        var prefix = key[..1];
        var found = Reconcile.LocalKeysUnderPrefix(a, 1, prefix);
        Assert.Contains(found, id => Fix.Eq(id, rid));
    }
}
