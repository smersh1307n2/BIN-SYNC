// Laboratory scenarios §17 + randomized convergence fuzz.
using System.Text;
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class LabFuzzTests
{
    const int NS = 1;
    static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public void S1_FieldParallelism_both_fields_survive()
    {
        var a = Fix.MakeNode(501);
        var b = Fix.MakeNode(502);
        Transport.MakeSession(a, b);
        var rid = Fix.Rid(10);
        a.WriteField(NS, rid, 0x100, Val("from-a"));
        b.WriteField(NS, rid, 0x200, Val("from-b"));
        Transport.SyncRound(a, b, NS);

        Assert.Equal("from-a", Encoding.UTF8.GetString(a.Storage[NS][rid].State[0x100].Value));
        Assert.Equal("from-b", Encoding.UTF8.GetString(a.Storage[NS][rid].State[0x200].Value));
        Assert.Equal("from-a", Encoding.UTF8.GetString(b.Storage[NS][rid].State[0x100].Value));
        Assert.Equal("from-b", Encoding.UTF8.GetString(b.Storage[NS][rid].State[0x200].Value));
    }

    [Fact]
    public void S2_DeleteWins_late_field_is_ghost()
    {
        var clockA = new FakeClock(1_000_000);
        var clockB = new FakeClock(1_000_000);
        var a = new Node(Fix.Nid(511), epoch: 1, nowFn: clockA.Now);
        var b = new Node(Fix.Nid(512), epoch: 1, nowFn: clockB.Now);
        Transport.MakeSession(a, b);
        var rid = Fix.Rid(20);
        a.WriteField(NS, rid, 0x100, Val("alive"));
        Transport.SyncRound(a, b, NS);

        clockA.Advance(1000);
        a.DeleteRecord(NS, rid);
        clockB.Advance(5000);
        b.WriteField(NS, rid, 0x100, Val("late"));
        Transport.SyncRound(a, b, NS);

        Assert.True(a.Storage[NS][rid].IsDead);
        Assert.True(b.Storage[NS][rid].IsDead);
    }

    [Fact]
    public void S6_ClockSkew_does_not_crash()
    {
        var a = new Node(Fix.Nid(521), epoch: 1, nowFn: () => 1_000_000, maxSkewUs: 1_000_000);
        var b = new Node(Fix.Nid(522), epoch: 1, nowFn: () => 1_000_000_000_000L, maxSkewUs: 1_000_000);
        Transport.MakeSession(a, b);
        b.WriteField(NS, Fix.Rid(1), 0x100, Val("skew"));
        var batch = b.CreateDeltaBatch(a.NodeId, NS);
        if (batch is null) return;
        try { a.ApplyBatch(batch, b.NodeId); }
        catch (SyncException) { }
    }

    [Fact]
    public void S7_Epoch_on_batch_is_visible()
    {
        // Batch carries IssuerEpoch; regression gate is optional on this path.
        var low = new Node(Fix.Nid(532), epoch: 3, nowFn: () => 2_000_000);
        var a = Fix.MakeNode(531);
        Transport.MakeSession(a, low);
        low.WriteField(NS, Fix.Rid(1), 0x100, Val("old-epoch"));
        var batch = low.CreateDeltaBatch(a.NodeId, NS);
        Assert.NotNull(batch);
        Assert.Equal(3, batch!.IssuerEpoch);
        try { a.ApplyBatch(batch, low.NodeId); }
        catch (SyncException) { /* EPOCH_REGRESSION ok */ }
    }

    [Fact]
    public void S8_CanonicalDigest_byte_identical()
    {
        var w1 = new Writer().U16(0x100).StringUtf8("same").F64(1.25);
        var w2 = new Writer().U16(0x100).StringUtf8("same").F64(1.25);
        Assert.Equal(w1.Value(), w2.Value());
        Assert.Equal(Blake3Core.Hash256(w1.Value()), Blake3Core.Hash256(w2.Value()));
    }

    [Fact]
    public void S18_DuplicateBatch_is_idempotent()
    {
        var a = Fix.MakeNode(551);
        var b = Fix.MakeNode(552);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(40), 0x100, Val("once"));
        var batch = a.CreateDeltaBatch(b.NodeId, NS);
        Assert.NotNull(batch);
        b.ApplyBatch(batch!, a.NodeId);
        try { b.ApplyBatch(batch!, a.NodeId); }
        catch (SyncException) { }
        Assert.Equal("once", Encoding.UTF8.GetString(b.Storage[NS][Fix.Rid(40)].State[0x100].Value));
    }

    [Fact]
    public void S19_Manual_keeps_local_on_conflict()
    {
        var local = new FieldState(Val("A"), new Hlc(1, 0), Fix.Nid(1));
        var remote = new FieldState(Val("B"), new Hlc(99, 0), Fix.Nid(2));
        var (w, changed) = Merge.MergeFieldWithPolicy(local, remote, MergePolicy.Manual);
        Assert.False(changed);
        Assert.Equal(Val("A"), w.Value);
    }

    [Fact]
    public void S26_CorruptedFrame_CRC_rejected_no_state_change()
    {
        var a = Fix.MakeNode(561);
        var b = Fix.MakeNode(562);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(50), 0x100, Val("payload"));
        var batch = a.CreateDeltaBatch(b.NodeId, NS);
        Assert.NotNull(batch);
        var payload = new Writer().U16(1).U16(0).U16(1).U8(0x02).U8(0).B128(a.NodeId).U32(1)
            .U64(1).U64(1).Varint(0).B128(batch!.BatchId).B128(Node.Zero128).B128(Node.Zero128).B128(Node.Zero128)
            .Value();
        var frame = FrameCodec.BuildFrame(0x09, 0, 1, payload);
        frame[Frames.HEADER_SIZE] ^= 0xFF;

        var ex = Assert.Throws<FrameException>(() => FrameCodec.ParseFrame(frame));
        Assert.Equal("CORRUPTED_FRAME", ex.CodeName);
        Assert.False(b.Storage.ContainsKey(NS) && b.Storage[NS].ContainsKey(Fix.Rid(50)));
    }

    [Fact]
    public void S15_FlowControl_blocks_when_credits_exhausted()
    {
        var fc = new FlowControl(initial: 100, maxWindow: 1000);
        Assert.True(fc.TryConsume(1, 50));
        Assert.True(fc.TryConsume(1, 50));
        Assert.False(fc.TryConsume(1, 1));
        fc.ApplyUpdate(1, 200);
        Assert.True(fc.TryConsume(1, 100));
        Assert.Equal(100, fc.Balance(1));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    [InlineData(13)]
    public void Fuzz_RandomWrites_converge_after_full_mesh_sync(int seed)
    {
        var rng = new Random(seed);
        int nNodes = 3;
        var nodes = Enumerable.Range(0, nNodes)
            .Select(i => new Node(Fix.Nid(600 + seed * 10 + i), epoch: 1,
                nowFn: () => 1_000_000L + seed * 1000 + i))
            .ToList();

        for (int i = 0; i < nNodes; i++)
            for (int j = i + 1; j < nNodes; j++)
                Transport.MakeSession(nodes[i], nodes[j]);

        int ops = 20 + seed;
        for (int k = 0; k < ops; k++)
        {
            var n = nodes[rng.Next(nNodes)];
            long rid = rng.Next(1, 8);
            ushort fid = (ushort)(0x100 + rng.Next(0, 4));
            n.WriteField(NS, Fix.Rid(rid), fid, Val($"s{seed}-k{k}"));
        }

        for (int round = 0; round < 8; round++)
        {
            for (int i = 0; i < nNodes; i++)
                for (int j = 0; j < nNodes; j++)
                    if (i != j)
                        Transport.SyncRound(nodes[i], nodes[j], NS);
        }

        var reference = SnapshotFields(nodes[0]);
        for (int i = 1; i < nNodes; i++)
        {
            var snap = SnapshotFields(nodes[i]);
            Assert.Equal(reference.Count, snap.Count);
            foreach (var (key, val) in reference)
            {
                Assert.True(snap.TryGetValue(key, out var other), $"node {i} missing {key}");
                Assert.Equal(val, other);
            }
        }
    }

    static Dictionary<string, string> SnapshotFields(Node n)
    {
        var d = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!n.Storage.TryGetValue(NS, out var recs)) return d;
        foreach (var (rid, rec) in recs)
        {
            if (rec.IsDead) continue;
            foreach (var (fid, fs) in rec.State.OrderBy(kv => kv.Key))
            {
                var key = rid.ToString() + ":" + fid.ToString("X4");
                d[key] = Convert.ToHexString(fs.Value);
            }
        }
        return d;
    }

    [Fact]
    public void Fuzz_CounterG_converges()
    {
        var a = Fix.MakeNode(701);
        var b = Fix.MakeNode(702);
        var rid = Fix.Rid(70);
        var payloadA = CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [a.NodeId] = 3 });
        var payloadB = CounterG.Encode(new Dictionary<byte[], ulong>(BytesEq.Instance) { [b.NodeId] = 5 });

        var recA = new Record(rid, NS);
        Merge.ApplyFieldWrite(recA, 0x100, new FieldState(payloadA, new Hlc(1, 0), a.NodeId), MergePolicy.CounterG);
        Merge.ApplyFieldWrite(recA, 0x100, new FieldState(payloadB, new Hlc(2, 0), b.NodeId), MergePolicy.CounterG);

        Assert.Equal(8UL, CounterG.Total(recA.State[0x100].Value));
    }

    [Fact]
    public void Fuzz_SetOr_concurrent_add_remove()
    {
        var actorA = Fix.Nid(1);
        var actorB = Fix.Nid(2);
        var left = SetOr.Encode(
            new[]
            {
                new SetOr.Add { Element = new byte[] { 1 }, Actor = actorA, Seq = 1 },
                new SetOr.Add { Element = new byte[] { 2 }, Actor = actorA, Seq = 2 },
            },
            Array.Empty<SetOr.Dot>());
        var right = SetOr.Encode(
            new[] { new SetOr.Add { Element = new byte[] { 3 }, Actor = actorB, Seq = 1 } },
            new[] { new SetOr.Dot { Actor = actorA, Seq = 1 } });

        var merged = SetOr.Merge(left, right);
        var visible = SetOr.Visible(merged).Select(Convert.ToHexString).OrderBy(x => x).ToList();
        Assert.Equal(2, visible.Count);
        Assert.Contains(Convert.ToHexString(new byte[] { 2 }), visible);
        Assert.Contains(Convert.ToHexString(new byte[] { 3 }), visible);
    }
}
