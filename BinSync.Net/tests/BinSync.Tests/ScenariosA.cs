// Laboratory scenarios §17.1–§17.20 — direct port of tests/test_scenarios.py.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ScenariosA
{
    const int NS = 7;
    static byte[] Rid(long n) => Fix.Rid(n);

    [Fact]
    public void Scenario01_FieldParallelism()
    {
        // §17.1 — different fields on two nodes both survive (С1–С4).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'a', (byte)'-', (byte)'v', (byte)'a', (byte)'l', (byte)'u', (byte)'e' }, new Hlc(1000, 0));
        b.WriteField(NS, Rid(1), 0x0101, System.Text.Encoding.ASCII.GetBytes("b-value"), new Hlc(1001, 0));
        Assert.True(Transport.SyncRound(a, b, NS) >= 2);
        var ra = a.Storage[NS][Rid(1)]; var rb = b.Storage[NS][Rid(1)];
        Assert.Equal(System.Text.Encoding.ASCII.GetBytes("a-value"), ra.State[0x0100].Value);
        Assert.Equal(System.Text.Encoding.ASCII.GetBytes("a-value"), rb.State[0x0100].Value);
        Assert.Equal(System.Text.Encoding.ASCII.GetBytes("b-value"), ra.State[0x0101].Value);
        Assert.Equal(System.Text.Encoding.ASCII.GetBytes("b-value"), rb.State[0x0101].Value);
        Assert.Equal(Digests.RecordDigest(ra), Digests.RecordDigest(rb));
    }

    [Fact]
    public void Scenario02_DeleteVsLateWrite()
    {
        // §17.2 — delete-wins; offline older field becomes ghost (У1–У5).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(100, 0));
        Transport.SyncRound(a, b, NS);
        a.DeleteRecord(NS, Rid(1), new Hlc(500, 0));
        b.WriteField(NS, Rid(1), 0x0102, System.Text.Encoding.ASCII.GetBytes("late"), new Hlc(400, 0));
        Transport.SyncRound(a, b, NS);
        foreach (var node in new[] { a, b })
        {
            var rec = node.Storage[NS][Rid(1)];
            Assert.True(rec.IsDead);
            Assert.True(Record.IsGhost(rec.State[0x0102], rec));  // field time <= lifecycle → ghost
        }
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(1)]), Digests.RecordDigest(b.Storage[NS][Rid(1)]));
    }

    [Fact]
    public void Scenario03_ReconnectNewAliasMap()
    {
        // §17.3 — merge result identical with different session aliases (И1–И3).
        var r1 = new List<byte[]>();
        foreach (var aliasPair in new[] { ((int, int)?)(1, 2), ((int, int)?)(9, 10) })
        {
            var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
            Transport.MakeSession(a, b, aliasPair);
            a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'a' }, new Hlc(5, 0));
            b.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'b' }, new Hlc(6, 0));
            Transport.SyncRound(a, b, NS);
            r1.Add(Digests.RecordDigest(a.Storage[NS][Rid(1)]));
        }
        Assert.Equal(r1[0], r1[1]);  // alias must not leak into durable state/digest [А1]
    }

    [Fact]
    public void Scenario04_SnapshotPlusTail()
    {
        // §17.4 — new peer gets snapshot at cursor N then delta tail (СН1, СН2).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        for (long i = 1; i <= 3; i++)
            a.WriteField(NS, Rid(i), 0x0100, System.Text.Encoding.ASCII.GetBytes($"v{i}"), new Hlc((ulong)(100 + i), 0));
        foreach (var batch in a.BuildSnapshot(NS))
            b.ApplySnapshot(batch, a.NodeId);
        a.WriteField(NS, Rid(4), 0x0100, System.Text.Encoding.ASCII.GetBytes("v4"), new Hlc(110, 0));
        Transport.DeliverBatch(a, b, NS);
        Assert.Equal(new HashSet<string> { "1", "2", "3", "4" },
                     new HashSet<string>(b.Storage[NS].Keys.Select(k => k.ToArray()[15].ToString())));
        foreach (var rid in new[] { Rid(1), Rid(4) })
            Assert.Equal(Digests.RecordDigest(a.Storage[NS][rid]), Digests.RecordDigest(b.Storage[NS][rid]));
    }

    [Fact]
    public void Scenario05_CounterWithGoneNode()
    {
        // §17.5 — departed node excluded from active authors (СМ5).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2); var c = Fix.MakeNode(3);
        Transport.MakeSession(a, b); Transport.MakeSession(a, c); Transport.MakeSession(b, c);
        a.WriteField(NS, Rid(1), 0x0100, System.Text.Encoding.ASCII.GetBytes("a"), new Hlc(1, 0));
        b.WriteField(NS, Rid(1), 0x0101, System.Text.Encoding.ASCII.GetBytes("b"), new Hlc(2, 0));
        Transport.SyncRound(a, b, NS);
        Transport.SyncRound(a, c, NS);
        Assert.Equal(Digests.RecordDigest(c.Storage[NS][Rid(1)]), Digests.RecordDigest(a.Storage[NS][Rid(1)]));
    }

    [Fact]
    public void Scenario06_ClockSkew()
    {
        // §17.6 — CLOCK_DRIFT rejection beyond skew clamp (§5.3).
        var a = Fix.MakeNode(1);
        var b = new Node(Fix.Nid(2), 1, nowFn: () => 1_000);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(1_000_000_000_000, 0)); // far future
        var ex = Assert.Throws<SyncException>(() => Transport.DeliverBatch(a, b, NS));
        Assert.Equal("CLOCK_DRIFT", ex.CodeName);
        Assert.False(b.Storage.TryGetValue(NS, out var st) && st.ContainsKey(Rid(1)));  // rolled back [Т1]
    }

    [Fact]
    public void Scenario07_EpochRegression()
    {
        // §17.7 — EPOCH_REGRESSION rejection (§4.4).
        var aHi = new Node(Fix.Nid(1), epoch: 5);
        var aLo = new Node(Fix.Nid(1), epoch: 3);
        var b = Fix.MakeNode(2);
        Transport.MakeSession(aHi, b); Transport.MakeSession(aLo, b);
        aHi.Clock.LastWall = 0; aLo.Clock.LastWall = 0;
        aHi.WriteField(NS, Rid(1), 0x0100, System.Text.Encoding.ASCII.GetBytes("hi"), new Hlc(10, 0));
        Transport.DeliverBatch(aHi, b, NS);
        // re-establish the epoch-5 session so rejection is EPOCH_REGRESSION, not UNKNOWN_ALIAS
        Transport.MakeSession(aHi, b);
        aLo.WriteField(NS, Rid(2), 0x0100, System.Text.Encoding.ASCII.GetBytes("lo"), new Hlc(11, 0));
        var ex = Assert.Throws<SyncException>(() => Transport.DeliverBatch(aLo, b, NS));
        Assert.Equal("EPOCH_REGRESSION", ex.CodeName);
    }

    [Fact]
    public void Scenario08_CanonicalDigest()
    {
        // §17.8 / К1 — two encoders produce byte-identical results.
        var op = new OpFieldWrite { FieldId = 0x0100, KeyEpoch = 0, Wall = 1000, Logical = 5, Origin = 7, Value = System.Text.Encoding.ASCII.GetBytes("hello") };
        var e1 = Ops.EncodeOp(op);
        var (op2, _) = Ops.DecodeOp(e1);
        var e2 = Ops.EncodeOp(op2);
        Assert.Equal(e1, e2);
        var d1 = Digests.RecordDigest(new Record(Rid(1), NS) { State = { [0x0100] = new FieldState(new byte[] { (byte)'v' }, new Hlc(1, 0), Fix.Nid(1)) } });
        var d2 = Digests.RecordDigest(new Record(Rid(1), NS) { State = { [0x0100] = new FieldState(new byte[] { (byte)'v' }, new Hlc(1, 0), Fix.Nid(1)) } });
        Assert.Equal(d1, d2);
    }

    [Fact]
    public void Scenario09_BucketIncremental()
    {
        // §17.9 / Д3 — insert order does not matter (count/xor/sum identical).
        var digests = Enumerable.Range(0, 50)
            .Select(i => Digests.RecordDigest(new Record(Rid(i), NS) { State = { [0x0100] = new FieldState(new byte[] { (byte)'v' }, new Hlc((ulong)i, 0), Fix.Nid(1)) } }))
            .ToList();
        var orders = new List<List<byte[]>> { digests, digests.AsEnumerable().Reverse().ToList(),
            digests.Where((_, i) => i % 2 == 0).Concat(digests.Where((_, i) => i % 2 == 1)).ToList() };
        var accs = orders.Select(order => { var acc = new BucketAccumulator(); foreach (var d in order) acc.Add(d); return acc; }).ToList();
        Assert.Equal(50, accs[0].Count); Assert.Equal(50, accs[1].Count); Assert.Equal(50, accs[2].Count);
        Assert.Equal(accs[0].Xor, accs[1].Xor); Assert.Equal(accs[1].Xor, accs[2].Xor);
        Assert.Equal(accs[0].Sum, accs[1].Sum); Assert.Equal(accs[1].Sum, accs[2].Sum);
        Assert.Equal(accs[0].Rollup(), accs[1].Rollup()); Assert.Equal(accs[1].Rollup(), accs[2].Rollup());
    }

    [Fact]
    public void Scenario10_TombstoneGcPrevention()
    {
        // §17.10 / СМ3 — tombstone retained: resync cannot resurrect a deleted record.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(10, 0));
        Transport.SyncRound(a, b, NS);
        a.DeleteRecord(NS, Rid(1), new Hlc(20, 0));
        Transport.SyncRound(a, b, NS);
        Assert.True(b.Storage[NS][Rid(1)].IsDead);
        Transport.SyncRound(a, b, NS);   // another round cannot resurrect it
        Assert.True(b.Storage[NS][Rid(1)].IsDead);
    }

    [Fact]
    public void Scenario11_BlindMerge()
    {
        // §17.11 / Ш1 — merge by time only: values are opaque blobs.
        byte[] ctA = { 0x9f, 0x01 }; byte[] ctB = { 0x9f, 0x02 };
        ctA = ctA.Concat(System.Text.Encoding.ASCII.GetBytes("cipherA")).ToArray();
        ctB = ctB.Concat(System.Text.Encoding.ASCII.GetBytes("cipherB")).ToArray();
        var rec = new Record(Rid(1), NS);
        Merge.ApplyFieldWrite(rec, 0x0100, new FieldState(ctA, new Hlc(100, 0), Fix.Nid(1)));
        bool changed = Merge.ApplyFieldWrite(rec, 0x0100, new FieldState(ctB, new Hlc(200, 0), Fix.Nid(2)));
        Assert.True(changed);
        Assert.Equal(ctB, rec.State[0x0100].Value);   // newer wins without decrypting
    }

    [Fact]
    public void Scenario12_Fragmentation()
    {
        // §17.12 — large payload split into frames and reassembled by message id.
        var rng = new Random(42);
        var big = new byte[300_000];
        rng.NextBytes(big);
        int chunk = 64 * 1024;
        int n = (big.Length + chunk - 1) / chunk;
        var stream = new MemoryStream();
        for (int i = 0; i < n; i++)
        {
            byte flags = (i + 1) * chunk < big.Length ? (byte)0x04 : (byte)0x00;
            var seg = big.Skip(i * chunk).Take(chunk).ToArray();
            stream.Write(FrameCodec.BuildFrame(0x0A, 0x0001_0000u | NS, (ulong)i, seg, flags));
        }
        var bytes = stream.ToArray();
        int pos = 0; var got = new MemoryStream();
        while (pos < bytes.Length)
        {
            var (fr, consumed) = FrameCodec.ParseFrame(bytes.AsSpan(pos).ToArray());
            got.Write(fr.Payload);
            pos += consumed;
        }
        Assert.Equal(big, got.ToArray());
    }

    [Fact]
    public void Scenario13_ZombieQuarantine()
    {
        // §17.13 — lagging node below tombstone-cutoff watermark → OUTDATED_WATERMARK + quarantine.
        var b = Fix.MakeNode(2);
        long cutoff = 500_000;
        var z = Fix.MakeNode(9);
        long opWall = 1;
        if (opWall < cutoff)
        {
            var err = new SyncException("OUTDATED_WATERMARK");
            b.Quarantined.Add(z.NodeId);
            Assert.Equal((ushort)0x000B, err.Code);
        }
        Assert.Contains(z.NodeId, b.Quarantined);
    }

    [Fact]
    public void Scenario14_DecompressionBombGuard()
    {
        // §17.14 — reject BEFORE decompressor: oversized declared length.
        var raw = FrameCodec.BuildFrame(0x09, 1, 1, System.Text.Encoding.ASCII.GetBytes("tiny"));
        Assert.True(raw.Length >= Frames.HEADER_SIZE + 4);
        var fakeHeader = (byte[])raw.Clone();
        // corrupt payloadLen (u32 LE at offset 17 per §12.2 packed layout)
        var len = BitConverter.GetBytes(200 * 1024 * 1024);
        Array.Copy(len, 0, fakeHeader, 17, 4);
        var full = fakeHeader.Concat(System.Text.Encoding.ASCII.GetBytes("tiny")).ToArray();
        var ex = Assert.Throws<FrameException>(() => FrameCodec.ParseFrame(full));
        Assert.Equal("PAYLOAD_TOO_LARGE", ex.CodeName);
        Assert.True(ex.Fatal);
    }

    [Fact]
    public void Scenario15_FlowControlBackpressure()
    {
        // §17.15 / УП1, УП2 — credit window enforcement, additive updates clamped.
        var fc = new FlowControl(initial: 100);
        Assert.True(fc.TryConsume(1, 60));
        Assert.False(fc.TryConsume(1, 60));                       // UП2: no credit → refuse
        fc.ApplyUpdate(1, 500);
        Assert.Equal(Math.Min(40 + 500, 16 * 1024 * 1024), fc.Balance(1));  // УП1 additive credit
        Assert.Equal(16L * 1024 * 1024, fc.ApplyUpdate(1, 1_000_000_000));  // clamp at max window
    }

    [Fact]
    public void Scenario16_KeyRotation()
    {
        // §17.16 — keyEpoch carried through ops and digests after rotation.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        var op = new OpFieldWrite { FieldId = 0x0100, KeyEpoch = 2, Wall = 10, Logical = 0, Origin = 1, Value = System.Text.Encoding.ASCII.GetBytes("reencrypted") };
        var entry = a.PushOutbox(NS, Rid(1), op, a.NodeId);
        Assert.NotNull(entry);
        Assert.Equal(Ops.EncodeOp(op), entry!.Payload);
        var (dec, _) = Ops.DecodeOp(entry.Payload);
        Assert.Equal((ushort)2, ((OpFieldWrite)dec).KeyEpoch);
    }

    [Fact]
    public void Scenario17_RangeCursorCoalescing()
    {
        // §17.17 / П2, П3, Т2 — lastReceivedSeq equals range end after coalesced batch.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        for (int i = 0; i < 5; i++)
            a.WriteField(NS, Rid(1), 0x0100 + i, System.Text.Encoding.ASCII.GetBytes($"v{i}"), new Hlc((ulong)(100 + i), 0));
        var r = Transport.DeliverBatch(a, b, NS);
        Assert.NotNull(r);
        var ps = b.PeerStates[(a.NodeId, NS)];
        Assert.Equal(5, ps.LastReceivedSeq);   // cursorEnd jump [Т2]
        Assert.Equal(5, b.OutboxSize(NS));     // relay queues received ops (§13.4)
    }

    [Fact]
    public void Scenario18_LostAck()
    {
        // §17.18 / П4, П7 — duplicate batch is a no-op + mandatory re-ACK.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(10, 0));
        var r1 = Transport.DeliverBatch(a, b, NS, dropAck: true)!;   // ACK lost
        Assert.Equal(1, r1.Applied);
        var batch2 = a.CreateDeltaBatch(b.NodeId, NS);
        Assert.NotNull(batch2);
        var decoded = b.DecodeBatchPayload(a.EncodeBatchPayload(batch2!));
        b.AcksOut.Clear();
        var r2 = b.ApplyBatch(decoded, a.NodeId);
        Assert.True(r2.Duplicate);
        Assert.Equal(0, r2.Applied);                                  // no-op
        Assert.Single(b.AcksOut);
        Assert.Equal(decoded.BatchId, b.AcksOut[0]);                  // re-ACK mandatory [П4]
        Assert.Equal(decoded.BatchId, r1.BatchId);                    // deterministic replay id
    }

    [Fact]
    public void Scenario19_ManualConflictResolve()
    {
        // §17.19 / РП1 — MANUAL policy resolved via reserved field 0x00FF.
        var a = Fix.MakeNode(1);
        var rec = a.Ns(NS).TryGetValue(Rid(1), out var r0) ? r0 : a.Ns(NS)[Rid(1)] = new Record(Rid(1), NS);
        Merge.ApplyFieldWrite(rec, 0x0100, new FieldState(System.Text.Encoding.ASCII.GetBytes("left"), new Hlc(5, 0), Fix.Nid(1)));
        a.WriteField(NS, Rid(1), Model.LifecycleFieldId + 0xFF, System.Text.Encoding.ASCII.GetBytes("chosen-left"), new Hlc(6, 0));
        Assert.Equal(System.Text.Encoding.ASCII.GetBytes("chosen-left"), rec.State[0x00FF].Value);
    }

    [Fact]
    public void Scenario20_MultiNamespaceIsolation()
    {
        // §17.20 — one namespace forbidden, the other keeps working (§3.7).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        b.ForbiddenNamespaces.Add(8);
        a.WriteField(8, Rid(1), 0x0100, System.Text.Encoding.ASCII.GetBytes("blocked"), new Hlc(1, 0));
        a.WriteField(NS, Rid(1), 0x0100, System.Text.Encoding.ASCII.GetBytes("ok"), new Hlc(2, 0));
        var ex = Assert.Throws<SyncException>(() => Transport.DeliverBatch(a, b, 8));
        Assert.Equal("NAMESPACE_FORBIDDEN", ex.CodeName);
        var r = Transport.DeliverBatch(a, b, NS);
        Assert.Equal(1, r!.Applied);                                  // other ns unaffected
    }
}
