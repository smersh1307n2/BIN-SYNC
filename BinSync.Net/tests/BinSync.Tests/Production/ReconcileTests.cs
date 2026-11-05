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
    public void Outbox_cursors_are_not_part_of_replicated_state_root()
    {
        var a = Fix.MakeNode(7);
        var b = Fix.MakeNode(8);
        var authorABytes = new byte[16]; authorABytes[0] = 70;
        var authorBBytes = new byte[16]; authorBBytes[0] = 80;
        var authorA = NodeId.From(authorABytes);
        var authorB = NodeId.From(authorBBytes);
        a.NextOutboxSeq(authorA, 1);
        a.NextOutboxSeq(authorA, 1);
        b.NextOutboxSeq(authorB, 1);
        b.NextOutboxSeq(authorB, 1);
        b.NextOutboxSeq(authorB, 1);
        var da = Reconcile.BuildStateDigest(a, 1);
        var db = Reconcile.BuildStateDigest(b, 1);
        Assert.Equal(0UL, da.OutboxCursor);
        Assert.Equal(0UL, db.OutboxCursor);
        Assert.True(da.RootDigest.AsSpan().SequenceEqual(db.RootDigest));
        Assert.Empty(Reconcile.DivergentPrefixes(da, db));
    }

    [Fact]
    public void DivergentPrefixes_does_not_trust_unverified_matching_root_digest()
    {
        var node = Fix.MakeNode(9);
        node.WriteField(1, Fix.Rid(91), 0x10, new byte[] { 1 });
        var local = Reconcile.BuildStateDigest(node, 1);
        var remote = new StateDigestMessage
        {
            NamespaceId = local.NamespaceId, OutboxCursor = local.OutboxCursor,
            RadixBits = local.RadixBits, RootDigest = local.RootDigest.ToArray(),
            Buckets = local.Buckets.Select(b => new DigestBucket
            {
                Prefix = b.Prefix.ToArray(), Count = b.Count + 1,
                Xor16 = b.Xor16.ToArray(), Sum16 = b.Sum16.ToArray(), Rollup32 = b.Rollup32.ToArray(),
            }).ToList(),
        };
        Assert.NotEmpty(Reconcile.DivergentPrefixes(local, remote));
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
        Assert.Equal(0UL, back.OutboxCursor);
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
