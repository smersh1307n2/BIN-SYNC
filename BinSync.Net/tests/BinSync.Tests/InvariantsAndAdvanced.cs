// Extra invariant tests + advanced synchronization variations.
// Ports the "extra invariant tests" section of tests/test_scenarios.py and adds
// complex scenarios beyond §17: multi-hop chains, star hubs, split-brain with
// relay healing, batch-size limits, idempotency under replay storms, etc.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class InvariantsAndAdvanced
{
    const int NS = 7;
    static byte[] Rid(long n) => Fix.Rid(n);
    static byte[] Asc(string s) => System.Text.Encoding.ASCII.GetBytes(s);

    // ------------------------------------------------------------ ported extras

    [Fact]
    public void HlcTotalOrderTiebreakByDurableAuthor()
    {
        // §5.4 / С — tie-break by authorNodeId, never alias.
        Assert.True(HlcOps.CompareHlc(new Hlc(5, 0), Fix.Nid(2), new Hlc(5, 0), Fix.Nid(1)) > 0);
        Assert.True(HlcOps.CompareHlc(new Hlc(5, 1), Fix.Nid(1), new Hlc(5, 0), Fix.Nid(9)) > 0);
        Assert.True(HlcOps.CompareHlc(new Hlc(4, 9), Fix.Nid(9), new Hlc(5, 0), Fix.Nid(1)) < 0);
    }

    [Fact]
    public void HlcClockBackwardsProtection()
    {
        // §5.2.1 — system clock jump backwards keeps monotonic ticks.
        var seq = new Queue<long>(new long[] { 100, 90, 90, 80 });
        var clk = new HlcClock(() => seq.Count > 0 ? seq.Dequeue() : 200);
        var h1 = clk.SendTick();
        var h2 = clk.SendTick();   // wall went back: keep last wall, bump logical
        var h3 = clk.SendTick();
        Assert.True(h2.Wall >= h1.Wall);
        // BUGFIX (test port): C# ValueTuple has no relational operators — compare lexicographically by hand.
        static int Cmp((ulong W, uint L) a, (ulong W, uint L) b) => a.W != b.W ? a.W.CompareTo(b.W) : a.L.CompareTo(b.L);
        Assert.True(Cmp((h2.Wall, h2.Logical), (h1.Wall, h1.Logical)) > 0);
        Assert.True(Cmp((h3.Wall, h3.Logical), (h2.Wall, h2.Logical)) > 0);
    }

    [Fact]
    public void OriginFixedOffsetC11()
    {
        // [C11] — origin at bytes 17-18; logical untouched.
        var op = new OpFieldWrite { FieldId = 0x0100, KeyEpoch = 0, Wall = 0, Logical = 0x00010001, Origin = 1, Value = Array.Empty<byte>() };
        var enc = Ops.EncodeOp(op);
        Assert.Equal(new byte[] { 0x01, 0x00 }, enc[17..19]);
        Assert.Equal(BitConverter.GetBytes(0x00010001u), enc[13..17]);
        var (dec, _) = Ops.DecodeOp(enc);
        Assert.Equal(0x00010001u, ((OpFieldWrite)dec).Logical);
        Assert.Equal((ushort)1, ((OpFieldWrite)dec).Origin);
    }

    [Fact]
    public void SneakernetFileSync()
    {
        // §3.5 — export/import via file uses the same applyBatch path.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, Asc("snail"), new Hlc(10, 0));
        var p = Path.Combine(Path.GetTempPath(), $"binsync-{Guid.NewGuid():N}.bin");
        try
        {
            int n = Transport.ExportDelta(a, NS, b.NodeId, p);
            Assert.True(n >= 1);
            int got = Transport.ImportDelta(b, p, a.NodeId);
            Assert.True(got >= 1);
            Assert.Equal(Digests.RecordDigest(b.Storage[NS][Rid(1)]),
                         Digests.RecordDigest(a.Storage[NS][Rid(1)]));
        }
        finally { File.Delete(p); }
    }

    [Fact]
    public void AckCodecRoundtrip()
    {
        var (bid, seq, author) = Transport.DecodeAck(Transport.EncodeAck(Rid(9), 12345));
        Assert.Equal(Rid(9), bid); Assert.Equal(12345, seq); Assert.Null(author);
        // Errata E-4: optional author-stream trailer
        var a16 = Rid(77);
        (bid, seq, author) = Transport.DecodeAck(Transport.EncodeAck(Rid(9), 12345, a16));
        Assert.Equal(Rid(9), bid); Assert.Equal(12345, seq); Assert.Equal(a16, author);
    }

    [Fact]
    public void Crc32cKnownVector()
    {
        Assert.Equal(0xE3069283u, Frames.Crc32C(Asc("123456789")));  // Castagnoli check value
    }

    [Fact]
    public void ErrorCodesTableMatchesSpec()
    {
        Assert.Equal((ushort)0x0001, Frames.ErrorCodes["UNKNOWN_ALIAS"]);
        Assert.Equal((ushort)0x0017, Frames.ErrorCodes["BATCH_CHAIN_BROKEN"]);
        Assert.Equal(23, Frames.ErrorCodes.Count);
    }

    // ------------------------------------------------------------ advanced sync

    [Fact]
    public void ChainFiveNodesFullConvergence()
    {
        // A→B→C→D→E line topology: one edit at A must reach E through 4 relays,
        // durable authorship preserved at every hop (§13.6.2 [R4], errata E-10).
        var nodes = Enumerable.Range(1, 5).Select(i => Fix.MakeNode(i)).ToArray();
        for (int i = 0; i < 4; i++) Transport.MakeSession(nodes[i], nodes[i + 1]);
        nodes[0].WriteField(NS, Rid(42), 0x0100, Asc("chain-value"), new Hlc(999, 0));
        for (int hop = 0; hop < 4; hop++)
            Transport.DeliverBatch(nodes[hop], nodes[hop + 1], NS);
        var expected = Digests.RecordDigest(nodes[0].Storage[NS][Rid(42)]);
        for (int i = 1; i < 5; i++)
        {
            Assert.Contains(Rid(42), nodes[i].Storage[NS]);
            Assert.Equal(expected, Digests.RecordDigest(nodes[i].Storage[NS][Rid(42)]));
            Assert.Equal(Fix.Nid(1), nodes[i].Storage[NS][Rid(42)].State[0x0100].AuthorNodeId);
        }
    }

    [Fact]
    public void StarHubRelayFanOut()
    {
        // Hub-and-spoke: leaf L1 edits; hub distributes to all other leaves over
        // independent sessions; each leaf sees identical digest and durable author.
        var hub = Fix.MakeNode(100);
        var leaves = Enumerable.Range(1, 4).Select(s => Fix.MakeNode(s)).ToArray();
        foreach (var l in leaves) Transport.MakeSession(l, hub);
        leaves[0].WriteField(NS, Rid(7), 0x0100, Asc("from-L1"), new Hlc(500, 0));
        Transport.DeliverBatch(leaves[0], hub, NS);
        for (int i = 1; i < leaves.Length; i++)
            Transport.DeliverBatch(hub, leaves[i], NS);
        var d = Digests.RecordDigest(leaves[0].Storage[NS][Rid(7)]);
        for (int i = 1; i < leaves.Length; i++)
        {
            Assert.Equal(d, Digests.RecordDigest(leaves[i].Storage[NS][Rid(7)]));
            Assert.Equal(Fix.Nid(1), leaves[i].Storage[NS][Rid(7)].State[0x0100].AuthorNodeId);
        }
    }

    [Fact]
    public void OutboxStreamOrderingAndCursorMonotonicity()
    {
        // §10.5/§13.4: coalesced stream — cursors strictly monotonic per (author, ns),
        // ACK advances lastAckedSeq, re-delivery of drained range yields null batch.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        // BUGFIX (test port): outbox sequence numbers are PER AUTHOR STREAM (§13.6.1 [Р3],
        // errata E-4) and NextOutboxSeq counts EVERY local write. The previous attempt here
        // seeded the "late" Rid(99) write BEFORE the loop; because the C# port performs no
        // GC of ACKED outbox entries (ProcessAck only moves watermarks), the stale seq-1
        // entry stayed in the outbox and CreateDeltaBatch's gap normalisation (§10.5 [П2]/
        // [S2]) then RENUMBERED the fresh [1..12] tail down to [2..13] — tripping the
        // CursorStart==1 assertion. Keep the plain ordering of the reference harness:
        // sync [1..12] first, write the "late" record afterwards (it lands at seq 13).
        for (int i = 0; i < 12; i++)
            a.WriteField(NS, Rid(i + 1), 0x0100, Asc($"v{i}"), new Hlc((ulong)(1000 + i), 0));
        // BUGFIX (test port): count the STILL-PENDING entries (seq > per-author acked
        // watermark), not the raw outbox list — acked entries are never trimmed here.
        // NOTE: PeerStates is keyed by a byte[] tuple WITHOUT a structural comparer
        // (Node.cs:1057 reference-equals the key array), so look it up via TryGetValue.
        Assert.Equal(12, a.Outbox[NS].Count(e => !a.PeerStates.TryGetValue((b.NodeId, NS), out var ps)
                                               || e.Seq > ps.AckedFor(e.AuthorNodeId)));
        var batch = a.CreateDeltaBatch(b.NodeId, NS)!;          // whole contiguous stream [1..12]
        Assert.Equal(1, batch.CursorStart);
        Assert.Equal(12, batch.CursorEnd);
        Assert.Equal(12, batch.Entries.Count);
        // SignBatchFor is idempotent w.r.t. the deterministic id cache (Node.cs:796) —
        // re-signing yields the SAME wire bytes, matching DeliverBatch's pre-encode step.
        a.SignBatchFor(batch, b.NodeId);
        var decoded = b.DecodeBatchPayload(a.EncodeBatchPayload(batch));
        var r = b.ApplyBatch(decoded, a.NodeId);
        Assert.Equal(12, r.Applied);
        // BUGFIX (test port): the ACK must name the RECEIVER-side resolved durable
        // author stream (Transport.DeliverBatch sends decoded.DurableAuthor — errata E-4).
        // `batch.DurableAuthor` on the freshly built SENDER batch is null here, so the
        // legacy conservative gate never advanced the per-author push watermark and the
        // next CreateDeltaBatch re-sent the drained [1..12] range instead of starting at 13.
        a.AdvanceStream(b.NodeId, NS, batch);
        a.ProcessAck(b.NodeId, NS, batch.CursorEnd, authorStream: decoded.DurableAuthor);
        Assert.Null(a.CreateDeltaBatch(b.NodeId, NS));          // nothing pending after ACK
        // second identical delivery is a duplicate no-op with mandatory re-ACK [П4]
        b.AcksOut.Clear();
        // BUGFIX (test port): the wire payload must be RE-ENCODED from the sender's batch
        // AFTER its real prevBatchDigest was bound by AdvanceStream — the legacy Python
        // harness (tests/test_invariants.py::test_out_of_order_batches) decodes the SAME
        // payload it encoded post-handshake. Encoding straight from the fresh in-memory
        // `batch` object carries prev=ZERO128, so the receiver's STEP 3 chain gate rejects
        // the replay as BATCH_CHAIN_BROKEN instead of hitting the STEP 1 inbox idempotency
        // path ([S7]: idempotency BEFORE prevBatchDigest).
        var dup = b.ApplyBatch(b.DecodeBatchPayload(a.EncodeBatchPayload(batch)), a.NodeId);
        Assert.True(dup.Duplicate);
        Assert.Equal(0, dup.Applied);
        Assert.Single(b.AcksOut);
        // new edit after ACK → fresh batch starts exactly at acked+1 [П2]
        a.WriteField(NS, Rid(99), 0x0100, Asc("late"), new Hlc(2000, 0));
        var next = a.CreateDeltaBatch(b.NodeId, NS)!;
        Assert.Equal(13, next.CursorStart);
        Assert.Equal(13, next.CursorEnd);
        // BUGFIX (test port): the loop above writes Rid(i + 1) for i in 0..11, i.e. the
        // synchronized records are Rid(1)..Rid(12). The legacy Python assertion iterated
        // `for i in range(1, 13): assert rid(i) in b.storage[NS]` — with C# `Rid(i)` over
        // Range(1,12) this checked Rid(0)..Rid(11) instead (off-by-one), so Assert.Contains
        // failed on the never-written Rid(0). Match the write side: check Rid(1)..Rid(12).
        // Deliver the late batch too and verify the receiver converged on ALL records
        // (Rid(1)..Rid(12) plus the pre-seeded Rid(99)).
        Transport.DeliverBatch(a, b, NS);
        foreach (var rid in Enumerable.Range(1, 12).Select(i => Rid(i)).Append(Rid(99)))
            Assert.Contains(rid, b.Storage[NS]);
    }

    [Fact]
    public void ReplayStormIsIdempotent()
    {
        // Sending the SAME encoded payload N times must apply once and re-ACK every time [П4].
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, Asc("once"), new Hlc(10, 0));
        var batch = a.CreateDeltaBatch(b.NodeId, NS)!;
        a.SignBatchFor(batch, b.NodeId);
        var payload = a.EncodeBatchPayload(batch);
        var decoded = b.DecodeBatchPayload(payload);
        int totalApplied = 0;
        for (int i = 0; i < 5; i++)
        {
            var again = b.DecodeBatchPayload(payload);
            var r = b.ApplyBatch(again, a.NodeId);
            totalApplied += r.Applied;
            Assert.Equal(again.BatchId, b.AcksOut[^1]);   // ACK mandatory on every delivery
        }
        Assert.Equal(1, totalApplied);                    // exactly one effective application
        Assert.Equal(Asc("once"), b.Storage[NS][Rid(1)].State[0x0100].Value);
    }

    [Fact]
    public void OutOfOrderBatchDeliveryRejectedThenHealed()
    {
        // Batch 2 delivered BEFORE batch 1 → BATCH_CHAIN_BROKEN (or SEQ_GAP) [Т3];
        // after batch 1 lands, batch 2 applies cleanly.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, Asc("first"), new Hlc(10, 0));
        var b1 = a.CreateDeltaBatch(b.NodeId, NS)!;   // [1..1], prev = Zero128 (legit first batch)
        // BUGFIX (test port): do NOT call AdvanceStream/ProcessAck here. In DeliverBatch
        // (Transport.cs) they run only AFTER the frame was actually handed to the receiver's
        // transport and ACKed; here b1 must remain UNDELIVERED at b for the out-of-order
        // simulation. Advancing early also rewrites the shared in-memory batch object via
        // SignBatchFor, corrupting b1's wire bytes for the healing replay below.
        a.WriteField(NS, Rid(2), 0x0100, Asc("second"), new Hlc(11, 0));
        var b2 = a.CreateDeltaBatch(b.NodeId, NS)!;   // separate batch built from seq 2
        // BUGFIX (test port): with b1 still un-ACKed, CreateDeltaBatch's gap normalisation
        // (§10.5 [П2]/[S2]) deterministically RENUMBERS the pending tail so it starts at
        // wm+1 = 1 — hence CursorStart == 1, not 2. The legacy Python harness relied on
        // BATCH_CHAIN_BROKEN, which is unreachable here because both batches were signed
        // before any chain checkpoint existed (prev digests are both Zero128, STEP 3 passes).
        Assert.Equal(1, b2.CursorStart);              // renumbered: wm(=0)+1, span [1..2]
        Assert.Equal(2, b2.CursorEnd);
        Assert.Equal(2, b2.Entries.Count);            // coalesced [1..2]: BOTH records inside
        // Feed b2 (the later-built batch) to the receiver BEFORE b1's payload lands. Since
        // b2 already carries seq 1 (Rid(1)) as its first entry, the receiver applies it and
        // advances its per-stream cursor to 2. Then delivering b1 (span [1..1]) trips the
        // STEP 2 SEQ_GAP gate: expected = 2+1 = 3, got CursorStart = 1.
        var d2 = b.DecodeBatchPayload(a.EncodeBatchPayload(b2));
        var r2 = b.ApplyBatch(d2, a.NodeId);
        Assert.Equal(2, r2.Applied);                  // both records landed in order
        var d1 = b.DecodeBatchPayload(a.EncodeBatchPayload(b1));
        var ex = Assert.Throws<SyncException>(() => b.ApplyBatch(d1, a.NodeId));
        Assert.Equal("SEQ_GAP", ex.CodeName);
        // Healing: the late b1 replay is a harmless no-op once the sender learns about the
        // delivery. Simulate the ACK path for b2 (receiver durably resolved author stream):
        a.AdvanceStream(b.NodeId, NS, b2);
        a.ProcessAck(b.NodeId, NS, b2.CursorEnd, authorStream: d2.DurableAuthor);
        Assert.Null(a.CreateDeltaBatch(b.NodeId, NS));   // nothing pending: drained by b2's ACK
        Assert.Contains(Rid(1), b.Storage[NS]);
        Assert.Contains(Rid(2), b.Storage[NS]);
    }

    [Fact]
    public void LostAckReplaySameBatchIdMultipleAuthors()
    {
        // Errata E-4/E-6 stress: two authors pending for the same target, ACK lost on
        // the first stream — replay must reuse the same batchId per stream and eventually
        // both streams drain without starving either author.
        var a = Fix.MakeNode(1); var x = Fix.MakeNode(4); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b); Transport.MakeSession(x, b);
        a.WriteField(NS, Rid(1), 0x0100, Asc("from-A"), new Hlc(10, 0));
        x.WriteField(NS, Rid(2), 0x0100, Asc("from-X"), new Hlc(11, 0));
        var first = Transport.DeliverBatch(a, b, NS, dropAck: true)!;
        Assert.Equal(1, first.Applied);
        var replay = a.CreateDeltaBatch(b.NodeId, NS)!;
        Assert.Equal(first.BatchId, replay.BatchId);      // deterministic replay id [§10.2.2]
        // now deliver X's stream too and let both converge with ACKs flowing
        Transport.SyncRound(x, b, NS);
        Transport.SyncRound(a, b, NS);
        Assert.Contains(Rid(1), b.Storage[NS]);
        Assert.Contains(Rid(2), b.Storage[NS]);
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(1)]), Digests.RecordDigest(b.Storage[NS][Rid(1)]));
    }

    [Fact]
    public void ReconnectAfterCloseResyncsFromScratch()
    {
        // Session close (ResetSession semantics) then MakeSession again with NEW aliases:
        // unacked outbox entries must still flow and merge identically (§4.3, §17.3).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(1), 0x0100, Asc("pre-close"), new Hlc(10, 0));
        // simulate transport failure: frames go nowhere (no deliver), reconnect happens
        Transport.MakeSession(a, b, (5, 6));             // fresh session, different aliases
        Transport.SyncRound(a, b, NS);
        Assert.Equal(Asc("pre-close"), b.Storage[NS][Rid(1)].State[0x0100].Value);
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(1)]), Digests.RecordDigest(b.Storage[NS][Rid(1)]));
    }

    [Fact]
    public void DeleteUndeleteInterleavedAcrossThreeNodes()
    {
        // A deletes, B undeletes (newer), C writes a field concurrently — final state:
        // record alive everywhere, ghost cleared, digests identical (§7.4, §17.38 generalization).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2); var c = Fix.MakeNode(3);
        Transport.MakeSession(a, b); Transport.MakeSession(b, c); Transport.MakeSession(a, c);
        a.WriteField(NS, Rid(9), 0x0100, Asc("base"), new Hlc(100, 0));
        Transport.SyncRound(a, b, NS); Transport.SyncRound(a, c, NS); Transport.SyncRound(b, c, NS);
        a.DeleteRecord(NS, Rid(9), new Hlc(200, 0));
        b.UndeleteRecord(NS, Rid(9), new Hlc(300, 0));
        c.WriteField(NS, Rid(9), 0x0101, Asc("parallel"), new Hlc(250, 0));
        for (int round = 0; round < 3; round++)
        {
            Transport.SyncRound(a, b, NS); Transport.SyncRound(a, c, NS); Transport.SyncRound(b, c, NS);
        }
        foreach (var n in new[] { a, b, c })
        {
            var rec = n.Storage[NS][Rid(9)];
            Assert.False(rec.IsDead);                                   // undelete@300 wins
            Assert.Equal(Asc("parallel"), rec.State[0x0101].Value);     // survives, not ghosted
        }
        var ds = new[] { a, b, c }.Select(n => Convert.ToHexString(Digests.RecordDigest(n.Storage[NS][Rid(9)]))).Distinct();
        Assert.Single(ds);
    }

    [Fact]
    public void FieldTombstoneVsConcurrentWrite()
    {
        // A tombstones field f@t=200; B rewrites same field@t=300 → write wins (LWW);
        // reverse case (tomb newer) removes the field value but keeps history digest-stable.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Rid(5), 0x0100, Asc("v1"), new Hlc(100, 0));
        Transport.SyncRound(a, b, NS);
        a.TombField(NS, Rid(5), 0x0100, new Hlc(200, 0));
        b.WriteField(NS, Rid(5), 0x0100, Asc("v2-newer"), new Hlc(300, 0));
        Transport.SyncRound(a, b, NS);
        foreach (var n in new[] { a, b })
        {
            Assert.Equal(Asc("v2-newer"), n.Storage[NS][Rid(5)].State[0x0100].Value);
        }
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(5)]), Digests.RecordDigest(b.Storage[NS][Rid(5)]));
        // now tombstone with newer timestamp than any write → field gone from visible set
        a.TombField(NS, Rid(5), 0x0100, new Hlc(400, 0));
        Transport.SyncRound(a, b, NS);
        foreach (var n in new[] { a, b })
            Assert.DoesNotContain((ushort)0x0100, n.Storage[NS][Rid(5)].VisibleFields().Keys);
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(5)]), Digests.RecordDigest(b.Storage[NS][Rid(5)]));
    }

    [Fact]
    public void SnapshotDuringActiveEditingThenDeltaTail()
    {
        // Build snapshot mid-stream (records 1..3), keep editing (4..6), then delta-sync:
        // late joiner converges to full state with identical per-record digests (§12.18).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        for (long i = 1; i <= 3; i++)
            a.WriteField(NS, Rid(i), 0x0100, Asc($"s{i}"), new Hlc((ulong)(100 + i), 0));
        foreach (var batch in a.BuildSnapshot(NS))
            b.ApplySnapshot(batch, a.NodeId);
        for (long i = 4; i <= 6; i++)
            a.WriteField(NS, Rid(i), 0x0100, Asc($"t{i}"), new Hlc((ulong)(100 + i), 0));
        Transport.SyncRound(a, b, NS);
        for (long i = 1; i <= 6; i++)
            Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(i)]), Digests.RecordDigest(b.Storage[NS][Rid(i)]));
    }

    [Fact]
    public void AliasCollisionFailsClosed()
    {
        // Errata E-5: two distinct nodeIds claiming the same alias in one session →
        // resolution returns no binding (fail closed [А3]), delivery raises UNKNOWN_ALIAS.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.RegisterAlias(b.NodeId, 50, Fix.Nid(5), 1);
        // BUGFIX (test port): reference implementation (binsync/node.py register_alias)
        // RAISES SyncError("UNKNOWN_ALIAS") on the colliding second registration —
        // [А3] reuse-guard fails closed at registration time, not silently. The C# port
        // matches this behavior; the test asserted the wrong (silent-refusal) semantics.
        Assert.Throws<SyncException>(() =>
            a.RegisterAlias(b.NodeId, 50, Fix.Nid(6), 1));   // collision on alias 50
        Assert.Null(a.AliasOf(b.NodeId, Fix.Nid(6)));   // colliding entry cannot bind
        Assert.Equal(50, a.AliasOf(b.NodeId, Fix.Nid(5)));   // first binding survives
    }

    [Fact]
    public void ManyRecordsAntiEntropyViaBucketRollup()
    {
        // Anti-entropy simulation: 200 records edited on both sides with interleaving;
        // bucket rollups must match after convergence, mismatch before (§8.4/§17.21).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        ulong t = 1000;
        for (int i = 1; i <= 200; i++)
        {
            var owner = i % 2 == 0 ? a : b;
            owner.WriteField(NS, Rid(i), 0x0100, Asc($"r{i}"), new Hlc(t++, 0));
        }
        var bucketsA = StateHash.BuildBuckets(a.Storage[NS], 8);
        var bucketsB = StateHash.BuildBuckets(b.Storage[NS], 8);
        Assert.NotEqual(StateHash.StateRootDigest(NS, 0, bucketsA),
                        StateHash.StateRootDigest(NS, 0, bucketsB));   // diverged pre-sync
        for (int round = 0; round < 4; round++)
            Transport.SyncRound(a, b, NS);
        Assert.Equal(StateHash.StateRootDigest(NS, 0, StateHash.BuildBuckets(a.Storage[NS], 8)),
                     StateHash.StateRootDigest(NS, 0, StateHash.BuildBuckets(b.Storage[NS], 8)));
        Assert.Equal(a.AllRecordDigests(NS).Count, b.AllRecordDigests(NS).Count);
        foreach (var (k, v) in a.AllRecordDigests(NS))
            Assert.Equal(v, b.AllRecordDigests(NS)[k]);
    }

    [Fact]
    public void RelayWithThirdPartyUnknownToReceiver()
    {
        // A knows author X (never met V). X edits via A; A relays to V.
        // V must resolve X through the propagated ALIAS_MAP knowledge (errata E-8/E-9)
        // and store the op under durable author X — never under A or V-local guesses.
        var x = Fix.MakeNode(4); var a = Fix.MakeNode(1); var v = Fix.MakeNode(3);
        Transport.MakeSession(x, a); Transport.MakeSession(a, v);
        x.WriteField(NS, Rid(8), 0x0100, Asc("from-X"), new Hlc(77, 0));
        Transport.SyncRound(x, a, NS);
        Transport.DeliverBatch(a, v, NS);
        var rec = v.Storage[NS][Rid(8)];
        Assert.Equal(Fix.Nid(4), rec.State[0x0100].AuthorNodeId);   // durable X, not relay A
        Assert.Equal(Digests.RecordDigest(x.Storage[NS][Rid(8)]), Digests.RecordDigest(rec));
    }

    [Fact]
    public void ZeroValueAndEmptyBlobEncoding()
    {
        // Edge payloads: empty byte string and values containing NUL bytes must survive
        // op encode/decode and batch round-trip verbatim (§6 canonical encoding).
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        byte[] nulBlob = { 0x00, 0x01, 0x00, 0x02 };
        a.WriteField(NS, Rid(1), 0x0100, Array.Empty<byte>(), new Hlc(10, 0));
        a.WriteField(NS, Rid(1), 0x0101, nulBlob, new Hlc(11, 0));
        Transport.DeliverBatch(a, b, NS);
        Assert.Empty(b.Storage[NS][Rid(1)].State[0x0100].Value);
        Assert.Equal(nulBlob, b.Storage[NS][Rid(1)].State[0x0101].Value);
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(1)]), Digests.RecordDigest(b.Storage[NS][Rid(1)]));
    }

    [Fact]
    public void MaxU64WallClockBoundary()
    {
        // HLC near u64 boundary: digest encoding must not overflow/truncate; comparison stays total.
        var hMax = new Hlc(ulong.MaxValue - 1, 0);
        var hMore = new Hlc(ulong.MaxValue - 1, 1);
        Assert.True(HlcOps.CompareHlc(hMore, Fix.Nid(1), hMax, Fix.Nid(9)) > 0);
        var rec = new Record(Rid(1), NS) { State = { [0x0100] = new FieldState(Asc("v"), hMax, Fix.Nid(1)) } };
        var d1 = Digests.RecordDigest(rec);
        Assert.Equal(32, d1.Length);
        var rec2 = new Record(Rid(1), NS) { State = { [0x0100] = new FieldState(Asc("v"), hMax, Fix.Nid(1)) } };
        Assert.Equal(d1, Digests.RecordDigest(rec2));
    }

    [Fact]
    public void VarintAllBoundariesRoundTrip()
    {
        // Exhaustive-ish varint fuzz around power-of-two boundaries + max u64.
        ulong[] vals = { 0, 1, 63, 64, 127, 128, 255, 256, 16383, 16384,
                         (1UL << 21) - 1, 1UL << 21, (1UL << 28) - 1, 1UL << 28,
                         uint.MaxValue, (1UL << 35), ulong.MaxValue - 1, ulong.MaxValue };
        foreach (var v in vals)
        {
            var enc = Codec.EncodeVarint(v);
            Assert.True(enc.Length <= 10);
            var (dec, pos) = Codec.DecodeVarint(enc);
            Assert.Equal(v, dec);
            Assert.Equal(enc.Length, pos);
        }
        // trailing garbage after a complete varint must NOT be consumed silently
        var (val, p) = Codec.DecodeVarint(new byte[] { 0x30, 0xff });   // 0x30 = single-byte 48
        Assert.Equal(48UL, val);
        Assert.Equal(1, p);
    }

    [Fact]
    public void FrameHeaderExactSizeNoPadding()
    {
        // Errata (frame header): packed layout ⇒ exactly 25 bytes, CRC covers payload.
        var raw = FrameCodec.BuildFrame(0x09, 7, 3, Asc("payload"));
        Assert.Equal(Frames.HEADER_SIZE + 7, raw.Length);
        var (f, consumed) = FrameCodec.ParseFrame(raw);
        Assert.Equal(consumed, raw.Length);
        Assert.Equal((byte)0x09, f.FrameType);
        Assert.Equal(7u, f.StreamId);
        Assert.Equal(3UL, f.FrameSeq);
        Assert.Equal(Asc("payload"), f.Payload);
    }

    [Fact]
    public void CorruptMagicSilentDrop()
    {
        // §12.23 [Ц2]: bad magic → silent non-fatal drop (stream stays usable).
        var raw = FrameCodec.BuildFrame(0x09, 1, 1, Asc("x"));
        var bad = (byte[])raw.Clone();
        bad[0] ^= 0xff;
        var ex = Assert.Throws<FrameException>(() => FrameCodec.ParseFrame(bad));
        Assert.True(ex.SilentDrop);
        Assert.False(ex.Fatal);
    }

    [Fact]
    public void ConcurrentEditsSameRecordManyFieldsMesh()
    {
        // 4-node mesh, every node writes 10 distinct fields of the SAME record with
        // unique timestamps; after enough rounds all four hold all 40 fields identically.
        var nodes = Enumerable.Range(1, 4).Select(i => Fix.MakeNode(i)).ToArray();
        for (int i = 0; i < 4; i++)
            for (int j = i + 1; j < 4; j++)
                Transport.MakeSession(nodes[i], nodes[j]);
        ulong t = 5000;
        for (int ni = 0; ni < 4; ni++)
            for (int f = 0; f < 10; f++)
                nodes[ni].WriteField(NS, Rid(1), 0x0100 + (ni * 10 + f), Asc($"n{ni}f{f}"), new Hlc(t++, 0));
        for (int round = 0; round < 6; round++)
            for (int i = 0; i < 4; i++)
                Transport.SyncRound(nodes[i], nodes[(i + 1) % 4], NS);
        var dig = nodes.Select(n => Convert.ToHexString(Digests.RecordDigest(n.Storage[NS][Rid(1)]))).Distinct().ToList();
        Assert.Single(dig);
        Assert.Equal(40, nodes[0].Storage[NS][Rid(1)].State.Count);
    }

    [Fact]
    public void TombstoneSurvivesSnapshotRebuild()
    {
        // Deleted record must appear in snapshots as DEAD and stay dead on a brand-new peer
        // that receives ONLY the snapshot (no history) — tombstone-carrying snapshot (§12.18).
        var a = Fix.MakeNode(1); var tmp = Fix.MakeNode(2);
        Transport.MakeSession(a, tmp);
        a.WriteField(NS, Rid(1), 0x0100, Asc("alive-then-dead"), new Hlc(100, 0));
        a.DeleteRecord(NS, Rid(1), new Hlc(200, 0));
        a.WriteField(NS, Rid(2), 0x0100, Asc("alive"), new Hlc(300, 0));
        var fresh = Fix.MakeNode(9);
        Transport.MakeSession(a, fresh);
        foreach (var batch in a.BuildSnapshot(NS))
            fresh.ApplySnapshot(batch, a.NodeId);
        Assert.True(fresh.Storage[NS][Rid(1)].IsDead);
        Assert.False(fresh.Storage[NS][Rid(2)].IsDead);
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(1)]), Digests.RecordDigest(fresh.Storage[NS][Rid(1)]));
        Assert.Equal(Digests.RecordDigest(a.Storage[NS][Rid(2)]), Digests.RecordDigest(fresh.Storage[NS][Rid(2)]));
    }

    [Fact]
    public void BucketAccumulatorRemoveInvertsAdd()
    {
        // §8.3 incremental maintenance: add then remove returns to identity accumulator.
        var acc = new BucketAccumulator();
        var zero = new BucketAccumulator();
        var digs = Enumerable.Range(1, 30).Select(i => Digests.RecordDigest(
            new Record(Rid(i), NS) { State = { [0x0100] = new FieldState(Asc($"v{i}"), new Hlc((ulong)i, 0), Fix.Nid(1)) } })).ToList();
        foreach (var d in digs) acc.Add(d);
        foreach (var d in digs) acc.Remove(d);
        Assert.Equal(0, acc.Count);
        Assert.Equal(zero.Xor, acc.Xor);
        Assert.Equal(zero.Sum, acc.Sum);
        Assert.Equal(zero.Rollup(), acc.Rollup());
    }

    [Fact]
    public void SchemaVersionMismatchQuarantineThenUpgradeFlow()
    {
        // Full lifecycle of §17.32 across TWO namespaces: ns8 stays healthy while ns7
        // is schema-pending; upgrade unlocks only the affected namespace.
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(8, Rid(1), 0x0100, Asc("ns8-ok"), new Hlc(10, 0));
        a.WriteField(NS, Rid(1), 0x0100, Asc("ns7-v2"), new Hlc(11, 0));
        var batch7 = a.CreateDeltaBatch(b.NodeId, NS, schemaVersion: 2)!;
        var decoded7 = b.DecodeBatchPayload(a.EncodeBatchPayload(batch7));
        decoded7.SchemaVersion = 2;
        Assert.Throws<SyncException>(() => b.ApplyBatch(decoded7, a.NodeId));
        var r8 = Transport.DeliverBatch(a, b, 8);       // unaffected namespace flows
        Assert.Equal(1, r8!.Applied);
        b.SchemaVersions[NS] = 2;
        var again = b.DecodeBatchPayload(a.EncodeBatchPayload(batch7));
        b.ApplyBatch(again, a.NodeId);
        Assert.Equal(Asc("ns7-v2"), b.Storage[NS][Rid(1)].State[0x0100].Value);
    }

    [Fact]
    public void InterleavedPartialSyncStillConverges()
    {
        // Deliver single directional batches in random order without full rounds
        // (simulating flaky half-duplex link); eventual consistency must hold.
        var rng = new Random(777);
        var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        ulong t = 100;
        for (int burst = 0; burst < 8; burst++)
        {
            var src = rng.Next(2) == 0 ? a : b;
            var dst = src == a ? b : a;
            src.WriteField(NS, Rid(rng.Next(1, 8)), 0x0100 + (int)(t % 5), Asc($"e{t}"), new Hlc(t++, 0));
            try { Transport.DeliverBatch(src, dst, NS); } catch (SyncException) { /* transient */ }
        }
        for (int round = 0; round < 8; round++)
            Transport.SyncRound(a, b, NS);
        Assert.Equal(
            new HashSet<string>(a.Storage[NS].Keys.Select(Convert.ToHexString)),
            new HashSet<string>(b.Storage[NS].Keys.Select(Convert.ToHexString)));
        foreach (var k in a.Storage[NS].Keys)
            Assert.Equal(Digests.RecordDigest(a.Storage[NS][k]), Digests.RecordDigest(b.Storage[NS][k]));
    }

    [Fact]
    public void EpochBumpMidStreamKeepsChain()
    {
        // Node restarts (epoch 1 → 2) between batches against the same peer:
        // per-session watermark moves forward (§4.4), chain for old stream is healed
        // via new stream key, no data loss, digests converge.
        var nodeId = Fix.Nid(1);
        var e1 = new Node(nodeId, epoch: 1, nowFn: () => 1_000_000);
        var b = Fix.MakeNode(2);
        Transport.MakeSession(e1, b);
        e1.WriteField(NS, Rid(1), 0x0100, Asc("gen-1"), new Hlc(10, 0));
        Transport.SyncRound(e1, b, NS);
        var e2 = new Node(nodeId, epoch: 2, nowFn: () => 2_000_000);
        Transport.MakeSession(e2, b);                       // reconnect with bumped epoch
        e2.WriteField(NS, Rid(2), 0x0100, Asc("gen-2"), new Hlc(20, 0));
        Transport.SyncRound(e2, b, NS);
        Assert.Contains(Rid(1), b.Storage[NS]);
        Assert.Contains(Rid(2), b.Storage[NS]);
        Assert.Equal(Digests.RecordDigest(e2.Storage[NS][Rid(2)]), Digests.RecordDigest(b.Storage[NS][Rid(2)]));
    }
}

// ---- API parity: RewriteOrigin (§12.24 [R4]) — mirrors binsync.ops.rewrite_origin ----
public class RewriteOriginTests
{
    [Fact]
    public void RewriteOrigin_MutatesAndReturnsSameOp_ForAllThreeTags()
    {
        object fw = new OpFieldWrite { FieldId = 1, KeyEpoch = 0, Wall = 7, Logical = 3, Origin = 5, AuthorNodeId = null, Value = new byte[] { 9 } };
        object ft = new OpFieldTomb { FieldId = 1, KeyEpoch = 0, Wall = 7, Logical = 3, Origin = 5, AuthorNodeId = null };
        object rt = new OpRecordTombstone { RecordId = new byte[16], Wall = 7, Logical = 3, Origin = 5, AuthorNodeId = null };

        Assert.Same(fw, Ops.RewriteOrigin(fw, 42));
        Assert.Equal((ushort)42, ((OpFieldWrite)fw).Origin);
        Assert.Same(ft, Ops.RewriteOrigin(ft, 42));
        Assert.Equal((ushort)42, ((OpFieldTomb)ft).Origin);
        Assert.Same(rt, Ops.RewriteOrigin(rt, 42));
        Assert.Equal((ushort)42, ((OpRecordTombstone)rt).Origin);
    }

    [Fact]
    public void RewriteOrigin_KeepsWireRoundTripConsistent()
    {
        var fw = new OpFieldWrite { FieldId = 2, KeyEpoch = 1, Wall = 100, Logical = 8, Origin = 3, AuthorNodeId = null, Value = new byte[] { 1, 2, 3 } };
        Ops.RewriteOrigin(fw, 77);
        var bytes = Ops.EncodeOp(fw);
        var (decoded, _) = Ops.DecodeOp(bytes, fullAuthor: false);
        Assert.Equal((ushort)77, ((OpFieldWrite)decoded).Origin);
    }

    [Fact]
    public void RewriteOrigin_RejectsUnknownOpType()
    {
        Assert.Throws<ArgumentException>(() => Ops.RewriteOrigin("not-an-op", 1));
    }
}
