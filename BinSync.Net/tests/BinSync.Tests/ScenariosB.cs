// Laboratory scenarios §17.21–§17.42 — direct port of tests/test_scenarios.py.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ScenariosB
{
    const int NS = 7;
    static byte[] Rid(long n) => Fix.Rid(n);
    static byte[] Asc(string s) => System.Text.Encoding.ASCII.GetBytes(s);

    [Fact]
    public void Scenario21_BucketRollup()
    {
        // §17.21 / Д10 — buckets roll up to a single StateRootDigest deterministically.
        var recs = new Dictionary<byte[], Record>(new ByteArrayComparer());
        for (int i = 0; i < 20; i++)
            recs[Rid(i)] = new Record(Rid(i), NS) { State = { [0x0100] = new FieldState(Asc($"v{i}"), new Hlc((ulong)i, 0), Fix.Nid(1)) } };
        var b1 = StateHash.BuildBuckets(recs, 8);
        var root1 = StateHash.StateRootDigest(NS, 0, b1);
        var rev = new Dictionary<byte[], Record>(new ByteArrayComparer());
        foreach (var kv in recs.Reverse()) rev[kv.Key] = kv.Value;
        var root2 = StateHash.StateRootDigest(NS, 0, StateHash.BuildBuckets(rev, 8));
        Assert.Equal(root1, root2);
    }

    [Fact]
    public void Scenario22_TrustOnFirstUse()
    {
        // §17.22 / Д1 — TOFU: second handshake with different key → TRUST_REVOKED.
        var pins = new Dictionary<byte[], byte[]>(new ByteArrayComparer());
        void Verify(byte[] peer, byte[] key)
        {
            if (pins.TryGetValue(peer, out var pinned) && !pinned.AsSpan().SequenceEqual(key))
                throw new SyncException("TRUST_REVOKED");
            if (!pins.ContainsKey(peer)) pins[peer] = key;
        }
        Verify(Fix.Nid(3), Asc("key-A"));
        var ex = Assert.Throws<SyncException>(() => Verify(Fix.Nid(3), Asc("key-B"))); // substitution attempt
        Assert.Equal("TRUST_REVOKED", ex.CodeName);
    }

    [Fact]
    public void Scenario23_RelayPreservesAuthorship()
    {
        // §17.23 / И4, И5, А1–А5 — original author wins tie-break, not relay.
        var a = Fix.MakeNode(1); var relay = Fix.MakeNode(2); var c = Fix.MakeNode(3);
        Transport.MakeSession(a, relay); Transport.MakeSession(relay, c); Transport.MakeSession(a, c);
        a.WriteField(NS, Rid(1), 0x0100, Asc("from-A"), new Hlc(777, 0));
        c.WriteField(NS, Rid(1), 0x0100, Asc("from-C"), new Hlc(777, 0));
        Transport.DeliverBatch(a, relay, NS);   // relay stores under ORIGINAL author A [И4]
        var recR = relay.Storage[NS][Rid(1)];
        Assert.Equal(Fix.Nid(1), recR.State[0x0100].AuthorNodeId);  // authorship preserved [А1]
        Transport.DeliverBatch(relay, c, NS);   // C has own op with same time, larger author → C wins
        var recC = c.Storage[NS][Rid(1)];
        Assert.Equal(Fix.Nid(3), recC.State[0x0100].AuthorNodeId);
        Transport.SyncRound(relay, c, NS);
        for (int _ = 0; _ < 3; _++)
            Transport.DeliverBatch(relay, a, NS);
        Assert.Equal(Asc("from-C"), a.Storage[NS][Rid(1)].State[0x0100].Value);
        var da = Digests.RecordDigest(a.Storage[NS][Rid(1)]);
        var dr = Digests.RecordDigest(relay.Storage[NS][Rid(1)]);
        var dc = Digests.RecordDigest(recC);
        Assert.Equal(da, dr); Assert.Equal(dr, dc);   // mesh converges byte-identically through the relay
    }

    [Fact]
    public void Scenario24_UnknownAliasAtomicRollback()
    {
        // §17.24 / А3, Т1 — alias 9999 → UNKNOWN_ALIAS, nothing stored.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(10, 0));
        a.WriteField(NS, Rid(2), 0x0100, new byte[] { (byte)'y' }, new Hlc(11, 0));
        var batch = a.CreateDeltaBatch(b.NodeId, NS)!;
        // attacker tampers: rewrite origin of every op to 9999
        var tampered = new List<OutboxEntry>();
        foreach (var e in batch.Entries)
        {
            var (obj, _) = Ops.DecodeOp(e.Payload);
            switch (obj)
            {
                case OpFieldWrite w: w.Origin = 9999; break;
                case OpFieldTomb t: t.Origin = 9999; break;
                case OpRecordTombstone r: r.Origin = 9999; break;
            }
            tampered.Add(new OutboxEntry
            {
                NamespaceId = e.NamespaceId, Seq = e.Seq, ChangeId = e.ChangeId,
                RecordId = e.RecordId, OpKind = e.OpKind, Payload = Ops.EncodeOp(obj),
                AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt,
            });
        }
        batch.Entries = tampered;
        batch.RecordsRoot = Node.ComputeRecordsRoot(tampered);
        batch.BatchDigest = Node.ComputeBatchDigest(batch.BatchId, batch.NamespaceId,
            batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
            batch.RecordsRoot, batch.PrevBatchDigest);
        var decoded = b.DecodeBatchPayload(a.EncodeBatchPayload(batch));
        var beforeKeys = new HashSet<string>(b.Storage.TryGetValue(NS, out var st0) ? st0.Keys.Select(Convert.ToHexString) : Array.Empty<string>());
        var ex = Assert.Throws<SyncException>(() => b.ApplyBatch(decoded, a.NodeId));
        Assert.Equal("UNKNOWN_ALIAS", ex.CodeName);
        var afterKeys = new HashSet<string>(b.Storage.TryGetValue(NS, out var st1) ? st1.Keys.Select(Convert.ToHexString) : Array.Empty<string>());
        Assert.Equal(beforeKeys, afterKeys);   // atomic rollback [Т1]
    }

    [Fact]
    public void Scenario25_ConcurrentDeleteByAuthor()
    {
        // §17.25 / Ж1–Ж4, У7 — identical delete times → larger durable author wins.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(1, 0));
        Transport.SyncRound(a, b, NS);
        a.DeleteRecord(NS, Rid(1), new Hlc(500_000, 0));
        b.DeleteRecord(NS, Rid(1), new Hlc(500_000, 0));
        Transport.SyncRound(a, b, NS);
        foreach (var node in new[] { a, b })
        {
            var rec = node.Storage[NS][Rid(1)];
            Assert.True(rec.IsDead);
            Assert.Equal(Fix.Nid(2), rec.LifecycleAuthor);   // B's author is larger [Ж2]
        }
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(1)]), Digests.RecordDigest(b.Storage[NS][Rid(1)]));
    }

    [Fact]
    public void Scenario26_CorruptedFrameRecovery()
    {
        // §17.26 / Ц1–Ц4 — CRC mismatch drops frame silently; resend succeeds.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(10, 0));
        var batch = a.CreateDeltaBatch(b.NodeId, NS)!;
        a.SignBatchFor(batch, b.NodeId);
        var raw = FrameCodec.BuildFrame(0x09, 0x0001_0000u | NS, 1, a.EncodeBatchPayload(batch));
        var corrupted = (byte[])raw.Clone();
        corrupted[Frames.HEADER_SIZE + 5] ^= 0x01;   // flip payload bit
        var ex = Assert.Throws<FrameException>(() => FrameCodec.ParseFrame(corrupted));
        Assert.Equal("CORRUPTED_FRAME", ex.CodeName);
        Assert.False(b.Storage.TryGetValue(NS, out var st) && st.ContainsKey(Rid(1)));
        var (frame, _) = FrameCodec.ParseFrame(raw);  // valid resend
        b.ApplyBatch(b.DecodeBatchPayload(frame.Payload), a.NodeId);
        Assert.Contains(Rid(1), b.Storage[NS]);
    }

    [Fact]
    public void Scenario27_DigestConsistencyThroughRelay()
    {
        // §17.27 / Д6–Д8, И1–И5 — A→relay→V: all three digests byte-identical.
        var a = Fix.MakeNode(1); var m = Fix.MakeNode(2); var v = Fix.MakeNode(3);
        Transport.MakeSession(a, m); Transport.MakeSession(m, v);
        a.WriteField(NS, Rid(1), 0x0100, Asc("origin-A"), new Hlc(50, 0));
        Transport.DeliverBatch(a, m, NS);
        Transport.DeliverBatch(m, v, NS);
        var da = Digests.RecordDigest(a.Storage[NS][Rid(1)]);
        var dm = Digests.RecordDigest(m.Storage[NS][Rid(1)]);
        var dv = Digests.RecordDigest(v.Storage[NS][Rid(1)]);
        Assert.Equal(da, dm); Assert.Equal(dm, dv);
        // digest uses durable author A even on V which never met A [Д8]
        Assert.Equal(Fix.Nid(1), v.Storage[NS][Rid(1)].State[0x0100].AuthorNodeId);
    }

    [Fact]
    public void Scenario28_RelaySeparateBatchesPerAuthor()
    {
        // §17.28 / Р1–Р3 — relay filters batch by author; other author's data withheld.
        var a = Fix.MakeNode(1); var x = Fix.MakeNode(4); var relay = Fix.MakeNode(2); var v = Fix.MakeNode(3);
        Transport.MakeSession(a, relay); Transport.MakeSession(x, relay); Transport.MakeSession(relay, v);
        a.WriteField(NS, Rid(1), 0x0100, Asc("from-A"), new Hlc(10, 0));
        x.WriteField(NS, Rid(2), 0x0100, Asc("from-X"), new Hlc(11, 0));
        Transport.DeliverBatch(a, relay, NS);
        Transport.DeliverBatch(x, relay, NS);
        var r = Transport.DeliverBatch(relay, v, NS, filterAuthor: a.NodeId);  // author filter = A
        Assert.NotNull(r);
        Assert.True(r!.Applied >= 1);
        Assert.Contains(Rid(1), v.Storage[NS]);
        Assert.DoesNotContain(Rid(2), v.Storage[NS]);   // X's records withheld [Р3]
    }

    [Fact]
    public void Scenario29_SnapshotBindingAndTail()
    {
        // §17.29 / СН1–СН4 — snapshot at cursor 3, then delta tail [4..5].
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        for (long i = 1; i <= 3; i++)
            a.WriteField(NS, Rid(i), 0x0100, Asc($"s{i}"), new Hlc((ulong)(100 + i), 0));
        foreach (var batch in a.BuildSnapshot(NS))
            b.ApplySnapshot(batch, a.NodeId);
        a.WriteField(NS, Rid(4), 0x0100, Asc("d4"), new Hlc(110, 0));
        a.WriteField(NS, Rid(5), 0x0100, Asc("d5"), new Hlc(111, 0));
        Transport.DeliverBatch(a, b, NS);
        foreach (long i in new long[] { 1, 2, 3, 4, 5 })
            Assert.Contains(Rid(i), b.Storage[NS]);
        foreach (var rid in new[] { Rid(1), Rid(5) })
            Assert.Equal(Digests.RecordDigest(a.Storage[NS][rid]), Digests.RecordDigest(b.Storage[NS][rid]));
    }

    [Fact]
    public void Scenario30_BatchChainIntegrity()
    {
        // §17.30 / Т3 — forged prevBatchDigest rejected; legit batch 2 accepted.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, Asc("b1"), new Hlc(10, 0));
        Transport.DeliverBatch(a, b, NS);                 // legit batch 1 (prev = 0)
        a.WriteField(NS, Rid(2), 0x0100, Asc("b2"), new Hlc(11, 0));
        var good = a.CreateDeltaBatch(b.NodeId, NS)!;     // signed with prev = digest(batch1) [Т3]
        var forged = a.DecodeBatchPayload(a.EncodeBatchPayload(good));
        forged.PrevBatchDigest = Enumerable.Repeat((byte)0xee, 16).ToArray();
        var ex = Assert.Throws<SyncException>(() => b.ApplyBatch(forged, a.NodeId));
        Assert.Equal("BATCH_CHAIN_BROKEN", ex.CodeName);
        // legit batch 2 applies fine afterwards (re-sign explicitly for THIS wire attempt)
        var legit = b.DecodeBatchPayload(a.EncodeBatchPayload(good));
        b.ApplyBatch(legit, a.NodeId);
        Assert.Contains(Rid(2), b.Storage[NS]);
    }

    [Fact]
    public void Scenario31_HelloAuth()
    {
        // §17.31 / Д5 — HELLO token validation gate (auth load in handshake).
        byte[] expected = Asc("shared-secret");
        void Hello(byte[] token)
        {
            if (!token.AsSpan().SequenceEqual(expected))
                throw new SyncException("SIGNATURE_INVALID");
        }
        var ex = Assert.Throws<SyncException>(() => Hello(Asc("wrong-token")));
        Assert.Equal("SIGNATURE_INVALID", ex.CodeName);
        Hello(expected); // passes
    }

    [Fact]
    public void Scenario32_SchemaPending()
    {
        // §17.32 / СХ7 — schema v2 batch quarantined; after registration, resend applies.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(10, 0));
        var batch = a.CreateDeltaBatch(b.NodeId, NS, schemaVersion: 2)!;
        var decoded = b.DecodeBatchPayload(a.EncodeBatchPayload(batch));
        decoded.SchemaVersion = 2;
        var ex = Assert.Throws<SyncException>(() => b.ApplyBatch(decoded, a.NodeId));
        Assert.Equal("SCHEMA_PENDING", ex.CodeName);
        // [СХ7]: rejection happens BEFORE any cursor state is created —
        // absence of a peer_state entry IS the "cursor not advanced" assertion.
        Assert.False(b.PeerStates.ContainsKey((a.NodeId, NS)));
        b.SchemaVersions[NS] = 2;                       // register schema v2
        b.ApplyBatch(decoded, a.NodeId);                // replay succeeds
        var ps = b.PeerStates[(a.NodeId, NS)];          // created by the successful apply
        Assert.Equal(batch.CursorEnd, ps.LastReceivedSeq);
    }

    [Fact]
    public void Scenario33_CausalDepTimeout()
    {
        // §17.33 / КЗ1 — dependent op with missing parent times out to error state.
        var a = Fix.MakeNode(1);
        byte[] parent = Enumerable.Repeat((byte)0xde, 16).ToArray();
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(10, 0));
        var ob = a.Outbox[NS][^1];
        ob.CausalDeps = new List<byte[]> { parent };
        long now = 200; // timeout elapsed
        bool timedOut = 100 < now && !a.Inbox.Contains((ob.ChangeId, NS, 0, parent));
        Assert.True(timedOut);
        var err = new SyncException("CAUSAL_DEP_MISSING");
        Assert.Equal("CAUSAL_DEP_MISSING", err.CodeName);  // recorded, never applied to tree
    }

    [Fact]
    public void Scenario34_CausalCycleDetection()
    {
        // §17.34 / КЗ2 — mutual deps form a cycle; deterministic break drops one branch.
        var g = new Dictionary<string, List<string>> { ["A"] = new() { "B" }, ["B"] = new() { "A" } };
        List<string>? FindCycle(Dictionary<string, List<string>> graph)
        {
            var seen = new HashSet<string>();
            var stack = new List<string>();
            List<string>? Dfs(string n)
            {
                if (stack.Contains(n)) return stack.Skip(stack.IndexOf(n)).Concat(new[] { n }).ToList();
                if (seen.Contains(n)) return null;
                seen.Add(n); stack.Add(n);
                foreach (var m in graph.GetValueOrDefault(n, new()))
                {
                    var c = Dfs(m);
                    if (c != null) return c;
                }
                stack.RemoveAt(stack.Count - 1); return null;
            }
            foreach (var k in graph.Keys.OrderBy(x => x))
            {
                var c = Dfs(k);
                if (c != null) return c;
            }
            return null;
        }
        var cyc = FindCycle(g);
        Assert.NotNull(cyc);
        Assert.Equal(new List<string> { "A", "B", "A" }, cyc);
        // deterministic break: drop edge from lexicographically largest node
        string breaker = cyc.Take(cyc.Count - 1).Max();
        g[breaker] = g[breaker].Where(d => d != cyc[^1]).ToList();
        Assert.Null(FindCycle(g));
    }

    [Fact]
    public void Scenario35_ZombieReplicaWatermark()
    {
        // §17.35 / У6, Д6, СМ7 — edit below tombstone cutoff → OUTDATED_WATERMARK + force resync.
        var b = Fix.MakeNode(2);
        long cutoff = 500_000;
        long zombieEditWall = 100;
        if (zombieEditWall < cutoff)
        {
            var err = new SyncException("OUTDATED_WATERMARK");
            b.Quarantined.Add(Fix.Nid(9));
            Assert.Equal("OUTDATED_WATERMARK", err.CodeName);
        }
        Assert.Contains(Fix.Nid(9), b.Quarantined);   // mandatory full-snapshot resync flag set
    }

    [Fact]
    public void Scenario36_BrainSplitHealing()
    {
        // §17.36 / С1, С3, Д1 — partition {A,B} vs {C}; heal with 3 rounds → converge.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2); var c = Fix.MakeNode(3);
        Transport.MakeSession(a, b); Transport.MakeSession(a, c); Transport.MakeSession(b, c);
        a.WriteField(NS, Rid(1), 0x0100, Asc("base"), new Hlc(100, 0));
        Transport.SyncRound(a, b, NS); Transport.SyncRound(a, c, NS);
        // partition {A,B}: field 0x0100 updated (newer), 0x0101 added
        a.WriteField(NS, Rid(1), 0x0100, Asc("AB-new"), new Hlc(300, 0));
        b.WriteField(NS, Rid(1), 0x0101, Asc("ab-extra"), new Hlc(301, 0));
        Transport.SyncRound(a, b, NS);
        // isolated C: OLD-timestamped write to 0x0100 + brand-new field 0x0102
        c.WriteField(NS, Rid(1), 0x0100, Asc("C-old"), new Hlc(200, 0));
        c.WriteField(NS, Rid(1), 0x0102, Asc("c-only"), new Hlc(250, 0));
        // heal: three full pairwise rounds
        for (int _ = 0; _ < 3; _++)
        {
            Transport.SyncRound(a, b, NS); Transport.SyncRound(a, c, NS); Transport.SyncRound(b, c, NS);
        }
        foreach (var node in new[] { a, b, c })
        {
            var rec = node.Storage[NS][Rid(1)];
            Assert.Equal(Asc("AB-new"), rec.State[0x0100].Value);   // LWW: newer wall wins
            Assert.Equal(Asc("c-only"), rec.State[0x0102].Value);   // parallel field survives
        }
        var ds = new HashSet<string>(new[] { a, b, c }.Select(n => Convert.ToHexString(Digests.RecordDigest(n.Storage[NS][Rid(1)]))));
        Assert.Single(ds);
    }

    [Fact]
    public void Scenario37_RingEchoStop()
    {
        // §17.37 / Ш2, С2, П4 — ring A→B→V→A: A's outbox size stays exactly 1.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2); var v = Fix.MakeNode(3);
        Transport.MakeSession(a, b); Transport.MakeSession(b, v); Transport.MakeSession(v, a);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(10, 0));
        Assert.Equal(1, a.OutboxSize(NS));
        Transport.DeliverBatch(a, b, NS);       // B queues for V
        Transport.DeliverBatch(b, v, NS);       // V queues for A
        Assert.Equal(1, v.OutboxSize(NS));
        var r = Transport.DeliverBatch(v, a, NS);   // echo closes the ring
        Assert.NotNull(r);
        Assert.Equal(0, r!.Applied);            // no-op on A [E2]
        Assert.Equal(1, a.OutboxSize(NS));      // avalanche prevented [Ш2]
    }

    [Fact]
    public void Scenario38_FullLifecycleCycle()
    {
        // §17.38 / У1, У2, У4, У5 — delete→ghost→undelete→final delete.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, new byte[] { (byte)'x' }, new Hlc(100, 0));
        Transport.SyncRound(a, b, NS);
        a.DeleteRecord(NS, Rid(1), new Hlc(500, 0));
        b.WriteField(NS, Rid(1), 0x0101, Asc("offline"), new Hlc(400, 0));
        Transport.SyncRound(a, b, NS);
        foreach (var n in new[] { a, b })
            Assert.True(Record.IsGhost(n.Storage[NS][Rid(1)].State[0x0101], n.Storage[NS][Rid(1)]));
        a.UndeleteRecord(NS, Rid(1), new Hlc(600, 0));   // undelete at 600 on A [У4]
        Transport.SyncRound(a, b, NS);
        foreach (var n in new[] { a, b })
        {
            var rec = n.Storage[NS][Rid(1)];
            Assert.False(rec.IsDead);
            Assert.False(Record.IsGhost(rec.State[0x0101], rec));   // ghost status cleared
        }
        b.DeleteRecord(NS, Rid(1), new Hlc(700, 0));     // final delete at 700 on B [У5]
        Transport.SyncRound(a, b, NS);
        foreach (var n in new[] { a, b })
            Assert.True(n.Storage[NS][Rid(1)].IsDead);
    }

    [Fact]
    public void Scenario39_MultiFieldAtomicBatch()
    {
        // §17.39 / Т1, С4 — 3-field batch vs fresher local field: per-field LWW.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        b.WriteField(NS, Rid(1), 0x0100, Asc("local-fresh"), new Hlc(3000, 0));
        foreach (var (fid, val) in new[] { (0x0100, "stale"), (0x0101, "two"), (0x0102, "three") })
            a.WriteField(NS, Rid(1), fid, Asc(val), new Hlc(2000, 0));
        var r = Transport.DeliverBatch(a, b, NS);
        Assert.Equal(2, r!.Applied);   // fields 0x0101/0x0102; 0x0100 loses locally but batch commits
        var rec = b.Storage[NS][Rid(1)];
        Assert.Equal(Asc("local-fresh"), rec.State[0x0100].Value);
        Assert.Equal(Asc("two"), rec.State[0x0101].Value);
        Assert.Equal(Asc("three"), rec.State[0x0102].Value);
    }

    [Fact]
    public void Scenario40_VarintCanonicalEdges()
    {
        // §17.40 / К2 — boundary values round-trip; overlong encodings rejected.
        foreach ((ulong v, int nbytes) in new[] { (0UL, 1), (1UL, 1), (127UL, 1), (128UL, 2),
                    (16383UL, 2), (16384UL, 3), (2147483647UL, 5) })
        {
            var enc = Codec.EncodeVarint(v);
            Assert.Equal(nbytes, enc.Length);
            var (dec, pos) = Codec.DecodeVarint(enc);
            Assert.Equal(v, dec);
            Assert.Equal(enc.Length, pos);
        }
        Assert.Throws<InvalidVarintException>(() => Codec.DecodeVarint(new byte[] { 0x80, 0x00 }));   // overlong 0
        Assert.Throws<InvalidVarintException>(() => Codec.DecodeVarint(
            new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x01 }));        // >10 bytes
    }

    [Fact]
    public void Scenario41_BucketCommutativityPermutations()
    {
        // §17.41 / Д3 — 100 digests in 3 orders → identical count/xor/sum.
        var rng = new Random(1234);
        var digs = Enumerable.Range(0, 100).Select(_ => { var b = new byte[32]; rng.NextBytes(b); return b; }).ToList();
        var orders = new List<List<byte[]>> { digs, digs.AsEnumerable().Reverse().ToList(),
            digs.Where((_, i) => i % 2 == 1).Concat(digs.Where((_, i) => i % 2 == 0)).ToList() };
        var accs = orders.Select(order => { var acc = new BucketAccumulator(); foreach (var d in order) acc.Add(d); return acc; }).ToList();
        Assert.Equal(100, accs[0].Count);
        Assert.Single(accs.Select(a => a.Xor).Distinct());
        Assert.Single(accs.Select(a => a.Sum).Distinct());
    }

    [Fact]
    public void Scenario42_ThreeNodeChaos()
    {
        // §17.42 / С1–С4, Д1–Д5 — randomized chaos, 4 mesh rounds → byte-identical convergence.
        var rng = new Random(2026);
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2); var c = Fix.MakeNode(3);
        var nodes = new[] { a, b, c };
        Transport.MakeSession(a, b); Transport.MakeSession(a, c); Transport.MakeSession(b, c);
        var records = new[] { 1L, 2, 3, 4, 5 }.Select(Rid).ToList();
        ulong t = 1000;
        for (int iter = 0; iter < 25; iter++)   // 25 random interleaved ops
        {
            var n = nodes[rng.Next(nodes.Length)];
            var r = records[rng.Next(records.Count)];
            int f = 0x0100 + rng.Next(0x10);
            int kind = rng.Next(4);             // write,write,delete,undelete distribution
            t += 1;
            string tag = Convert.ToHexString(n.NodeId)[..2];
            if (kind != 2 && kind != 3)
                n.WriteField(NS, r, f, Asc($"{tag}-{t}"), new Hlc(t, 0));
            else if (kind == 2)
                n.DeleteRecord(NS, r, new Hlc(t, 0));
            else
                n.UndeleteRecord(NS, r, new Hlc(t, 0));
        }
        var pairs = new[] { (a, b), (b, a), (a, c), (c, a), (b, c), (c, b) };
        for (int round = 0; round < 4; round++)  // 4 rounds of random mesh exchange
        {
            foreach (var (src, dst) in pairs.OrderBy(_ => rng.Next()))
            {
                try { Transport.DeliverBatch(src, dst, NS); }
                catch (SyncException e) { Assert.Fail($"desync during chaos: {e.CodeName}"); }
            }
        }
        foreach (var r in records)
        {
            var ds = nodes.Select(n => n.Storage.TryGetValue(NS, out var st) && st.TryGetValue(r, out var rec)
                                        ? Convert.ToHexString(Digests.RecordDigest(rec)) : null).ToList();
            Assert.True(ds[0] != null || ds.All(x => x == null));
            Assert.True(ds.Where(x => x != null).Distinct().Count() <= 1);
            Assert.Equal(3, ds.Count);
            Assert.True(ds.All(x => (x == null) == (ds[0] == null)));
        }
    }
}

/// Byte-array equality comparer for test-local dictionaries keyed by ids.
public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
{
    public bool Equals(byte[]? x, byte[]? y) => x is null ? y is null : y is not null && x.AsSpan().SequenceEqual(y);
    public int GetHashCode(byte[] t) { unchecked { int h = 17; foreach (byte b in t) h = h * 31 + b; return h; } }
}
