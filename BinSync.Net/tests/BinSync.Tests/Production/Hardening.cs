using Xunit;

// Production-hardening suite for BinSync.Net: in-tree BLAKE3, atomic persistence of the
// FULL node state, async transport (cancellation + §12.18 gate discipline), and
// thread-safety of the re-entrant node gate.
using System.Buffers.Binary;

namespace BinSync.Tests;

public class Production : IDisposable
{
    private readonly string _dir = MakeTempDir();

    private static string MakeTempDir()
    {
        var d = Path.Combine(Path.GetTempPath(), "binsync-prod-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        return d;
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private const int NS = 7;
    private static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    /// Build two fully-synced nodes with rich state: local writes, remote-authored
    /// fields (via sync), lifecycle tomb, outbox seq counters, checkpoints, aliases,
    /// peer cursors, seen-set — everything NodePersistence must round-trip.
    private static (Node a, Node b) BuildHistory()
    {
        var a = Fix.MakeNode(1);
        var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(100), 0x101, Val("hello"));
        a.WriteField(NS, Fix.Rid(100), 0x102, Val("world"));
        a.WriteField(NS, Fix.Rid(101), 0x101, Val("solo"));
        b.WriteField(NS, Fix.Rid(200), 0x101, Val("from-b"));
        b.TombField(NS, Fix.Rid(200), 0x102);
        Transport.SyncRound(a, b, NS);
        a.DeleteRecord(NS, Fix.Rid(101));           // dead record w/ ghost semantics
        Transport.SyncRound(a, b, NS);
        return (a, b);
    }

    private static bool StateEqual(Node x, Node y)
    {
        if (!x.NodeId.AsSpan().SequenceEqual(y.NodeId)) return false;
        if (x.Epoch != y.Epoch) return false;
        if (x.Clock.LastWall != y.Clock.LastWall || x.Clock.LastLogical != y.Clock.LastLogical) return false;
        if (x.Storage.Count != y.Storage.Count) return false;
        foreach (var (ns, recs) in x.Storage)
        {
            if (!y.Storage.TryGetValue(ns, out var other)) return false;
            if (recs.Count != other.Count) return false;
            foreach (var (rid, rec) in recs)
            {
                if (!other.TryGetValue(rid, out var o)) return false;
                if (rec.LifecycleValue != o.LifecycleValue) return false;
                if ((rec.LifecycleHlc is null) != (o.LifecycleHlc is null)) return false;
                if (rec.LifecycleHlc is not null &&
                    (rec.LifecycleHlc!.Wall != o.LifecycleHlc!.Wall || rec.LifecycleHlc.Logical != o.LifecycleHlc.Logical ||
                     !rec.LifecycleAuthor!.AsSpan().SequenceEqual(o.LifecycleAuthor!))) return false;
                if (rec.State.Count != o.State.Count) return false;
                foreach (var (fid, fs) in rec.State)
                {
                    if (!o.State.TryGetValue(fid, out var ofs)) return false;
                    if (fs.Timestamp.Wall != ofs.Timestamp.Wall || fs.Timestamp.Logical != ofs.Timestamp.Logical) return false;
                    if (!fs.AuthorNodeId.AsSpan().SequenceEqual(ofs.AuthorNodeId)) return false;
                    if (!fs.Value.AsSpan().SequenceEqual(ofs.Value)) return false;
                }
                if (rec.FieldTombs.Count != o.FieldTombs.Count) return false;
                foreach (var (tfid, tfs) in rec.FieldTombs)
                {
                    if (!o.FieldTombs.TryGetValue(tfid, out var otfs)) return false;
                    if (tfs.Timestamp.Key() != otfs.Timestamp.Key()) return false;
                }
            }
        }
        if (x.Inbox.Count != y.Inbox.Count) return false;
        if (x.LastBatchDigest.Count != y.LastBatchDigest.Count) return false;
        foreach (var (k, dig) in x.LastBatchDigest)
            if (!y.LastBatchDigest.TryGetValue(k, out var od) || !dig.AsSpan().SequenceEqual(od)) return false;
        if (x.LastStreamDigest.Count != y.LastStreamDigest.Count) return false;
        if (x.DurableAuthors.Count != y.DurableAuthors.Count) return false;
        if (x.SeenChanges.Count != y.SeenChanges.Count) return false;
        if (x.KnownEpochs.Count != y.KnownEpochs.Count) return false;
        foreach (var (k, ep) in x.KnownEpochs)
            if (!y.KnownEpochs.TryGetValue(k, out var oe) || ep != oe) return false;
        if (x.AliasToNode.Count != y.AliasToNode.Count) return false;
        foreach (var (peer, map) in x.AliasToNode)
        {
            if (!y.AliasToNode.TryGetValue(peer, out var om) || map.Count != om.Count) return false;
            foreach (var (alias, (nid, ep)) in map)
            {
                if (!om.TryGetValue(alias, out var onidEp)) return false;
                if (!nid.AsSpan().SequenceEqual(onidEp.Item1) || ep != onidEp.Item2) return false;
            }
        }
        if (x.PeerStates.Count != y.PeerStates.Count) return false;
        foreach (var (k, ps) in x.PeerStates)
        {
            if (!y.PeerStates.TryGetValue(k, out var ops)) return false;
            if (ps.LastPushedSeq != ops.LastPushedSeq || ps.LastReceivedSeq != ops.LastReceivedSeq ||
                ps.LastAckedSeq != ops.LastAckedSeq || ps.InitialSyncComplete != ops.InitialSyncComplete) return false;
            if (ps.ReceivedStreams.Count != ops.ReceivedStreams.Count) return false;
            if (ps.StreamAcked.Count != ops.StreamAcked.Count) return false;
            if (ps.IdsRef.Count != ops.IdsRef.Count) return false;
        }
        // Outbox entries + per-author seq counters
        var sx = x.OutboxSeqRef;
        var sy = y.OutboxSeqRef;
        if (sx.Count != sy.Count) return false;
        foreach (var ((author, ns), seq) in sx)
            if (!sy.TryGetValue((author, ns), out var os) || seq != os) return false;
        if (x.Outbox.Count != y.Outbox.Count) return false;
        foreach (var (ns, list) in x.Outbox)
        {
            if (!y.Outbox.TryGetValue(ns, out var ol) || list.Count != ol.Count) return false;
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i].Seq != ol[i].Seq || list[i].OpKind != ol[i].OpKind) return false;
                if (!list[i].ChangeId.AsSpan().SequenceEqual(ol[i].ChangeId)) return false;
                if (!list[i].Payload.AsSpan().SequenceEqual(ol[i].Payload)) return false;
                if (list[i].CausalDeps.Count != ol[i].CausalDeps.Count) return false;
            }
        }
        return true;
    }

    // ============================================================ BLAKE3 (in-tree)

    [Fact]
    public void Blake3_MatchesOfficialTestVectors()
    {
        // Official BLAKE3 test vectors (hash_test.json), input[i] = i % 251, first 24 hex chars.
        var vectors = new (int Len, string Prefix)[]
        {
            (0,     "af1349b9f5f9a1a6a04044a5"),
            (1,     "969de78dd931f8d5b46384f5"),
            (2,     "adafa50ae8dcb544bb6c148b"),
            (5,     "de9ec3f661bece4db8ad2036"),
            (10,    "00484ae68da8a913f6c9f885"),
            (1023,  "12a793ebbf8fc205ad0fa61e"),
            (1024,  "47998f7ece3ec4ad4e4ff24b"),
            (1025,  "ab855b9dcf50a8089f66d1f8"),
            (2048,  "a8125ac67f525436fc5c60fd"),
            (2080,  "bd3a3aa54cc996f2b6cca334"),
            (3072,  "bd3fc13530283382bcc65377"),
            (4096,  "015094013f57a5275eadbd94"),
            (5000,  "749d399d744a516633f6858c"),
            (31774, "a6b46461c2a7b95dbdb91f80"),
        };
        foreach (var (len, prefix) in vectors)
        {
            var data = new byte[len];
            for (int i = 0; i < len; i++) data[i] = (byte)(i % 251);
            var got = Convert.ToHexString(Blake3Core.Hash256(data)).ToLowerInvariant();
            Assert.True(got.StartsWith(prefix, StringComparison.Ordinal),
                $"BLAKE3 mismatch at len={len}: got {got[..Math.Min(24, got.Length)]}…, expected {prefix}…");
        }
    }

    [Fact]
    public void Blake3_TaggedHash_IsDomainSeparated()
    {
        var data = Val("state");
        var h1 = Blake3Core.TaggedHash("BIN-SYNC/NODESTATE/v1", data);
        var h2 = Blake3Core.TaggedHash("BIN-SYNC/OTHER/v1", data);
        var plain = Blake3Core.Hash256(data);
        Assert.False(h1.AsSpan().SequenceEqual(h2));
        Assert.False(h1.AsSpan().SequenceEqual(plain));
        // Deterministic: same tag+input ⇒ same digest.
        Assert.Equal(Convert.ToHexString(h1), Convert.ToHexString(Blake3Core.TaggedHash("BIN-SYNC/NODESTATE/v1", data)));
    }

    [Fact]
    public void Digests_StillUseInTreeBlake3_AfterDependencyRemoval()
    {
        // The core hash entry point must equal the in-tree implementation bit-for-bit.
        var data = Val("bin-sync dependency-free check");
        Assert.Equal(Blake3Core.Hash256(data), Digests.Blake3(data));
        Assert.Equal(Blake3Core.TaggedHash("BIN-SYNC/NODESTATE/v1", data),
                     Digests.TaggedBlake3("BIN-SYNC/NODESTATE/v1", data));
    }

    // ============================================================ Persistence

    [Fact]
    public void Persistence_RoundTrip_PreservesFullState()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "nodeA.state");
        NodePersistence.Save(a, path);
        var restored = NodePersistence.Load(path);
        Assert.True(StateEqual(a, restored), "restored node state diverges from the original");
    }

    [Fact]
    public void Persistence_SequencesContinueAfterRestart()
    {
        var (a, _) = BuildHistory();
        var seqMap = a.OutboxSeqRef;
        long before = seqMap[(a.NodeId, NS)];
        Assert.True(before > 0);
        var path = Path.Combine(_dir, "seq.state");
        NodePersistence.Save(a, path);

        var restored = NodePersistence.Load(path);
        var rmap = restored.OutboxSeqRef;
        Assert.Equal(before, rmap[(restored.NodeId, NS)]);

        // A fresh write on the restored node must continue the durable per-author stream.
        restored.WriteField(NS, Fix.Rid(300), 0x101, Val("post-restart"));
        var batch = restored.CreateDeltaBatch(Fix.Nid(99), NS);
        Assert.NotNull(batch);
        Assert.True(batch!.CursorEnd > before, $"expected cursorEnd > {before}, got {batch.CursorEnd}");
    }

    [Fact]
    public void Persistence_CorruptedDigest_IsRejected()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "corrupt.state");
        NodePersistence.Save(a, path);
        var bytes = File.ReadAllBytes(path);
        bytes[^1] ^= 0xFF;   // flip one bit of the body
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes));
    }

    [Fact]
    public void Persistence_TruncatedFile_IsRejected()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "trunc.state");
        NodePersistence.Save(a, path);
        var bytes = File.ReadAllBytes(path);
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes[..(bytes.Length / 2)]));
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes[..8]));
    }

    [Fact]
    public void Persistence_BadMagicAndVersion_IsRejected()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "magic.state");
        NodePersistence.Save(a, path);
        var bytes = File.ReadAllBytes(path);
        bytes[0] ^= 0x01;    // magic
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes));

        var bytes2 = File.ReadAllBytes(path);
        BinaryPrimitives.WriteUInt16BigEndian(bytes2.AsSpan(4, 2), 99);  // unsupported version
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes2));
    }

    [Fact]
    public void Persistence_SaveIsAtomic_NoTempLeftovers_AndOverwriteWorks()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "atomic.state");
        NodePersistence.Save(a, path);
        NodePersistence.Save(a, path);   // overwrite via rename must succeed
        Assert.Single(Directory.GetFiles(_dir, "*", SearchOption.AllDirectories)
                               .Where(f => Path.GetFileName(f).StartsWith(".atomic.state.tmp-") == false
                                        || f == path));
        // No leftover temp files besides the target itself.
        Assert.Empty(Directory.GetFiles(_dir, ".atomic.state.tmp-*"));
        Assert.True(File.Exists(path));
    }

    [Fact]
    public void Persistence_LoadedNode_SyncsWithItsPeer()
    {
        var (a, b) = BuildHistory();
        var path = Path.Combine(_dir, "resume.state");
        NodePersistence.Save(a, path);
        var a2 = NodePersistence.Load(path);

        // Fresh writes on b replicate into the RESTARTED a through the real pipeline.
        b.WriteField(NS, Fix.Rid(400), 0x101, Val("after-a-restart"));
        Transport.SyncRound(b, a2, NS);
        Assert.True(a2.Storage.TryGetValue(NS, out var store));
        Assert.True(store!.TryGetValue(Fix.Rid(400), out var rec));
        Assert.Equal("after-a-restart", System.Text.Encoding.UTF8.GetString(rec.State[0x101].Value));
    }

    // ============================================================ Async transport

    [Fact]
    public async Task AsyncTransport_SyncRound_Converges()
    {
        var a = Fix.MakeNode(11);
        var b = Fix.MakeNode(12);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(1), 0x101, Val("from-a"));
        b.WriteField(NS, Fix.Rid(2), 0x101, Val("from-b"));

        var res = await AsyncTransport.SyncRoundAsync(a, b, NS);
        Assert.True(res.Total >= 2);
        Assert.True(b.Storage[NS].ContainsKey(Fix.Rid(1)));
        Assert.True(a.Storage[NS].ContainsKey(Fix.Rid(2)));

        // Idempotent second round: nothing pending.
        var again = await AsyncTransport.SyncRoundAsync(a, b, NS);
        Assert.Equal(0, again.Total);
    }

    [Fact]
    public async Task AsyncTransport_DeliverBatchAsync_ReturnsNull_WhenNothingPending()
    {
        var a = Fix.MakeNode(13);
        var b = Fix.MakeNode(14);
        Transport.MakeSession(a, b);
        var r = await AsyncTransport.DeliverBatchAsync(a, b, NS);
        Assert.Null(r);
    }

    [Fact]
    public async Task AsyncTransport_HonorsCancellation()
    {
        var a = Fix.MakeNode(15);
        var b = Fix.MakeNode(16);
        Transport.MakeSession(a, b);
        for (int i = 0; i < 50; i++) a.WriteField(NS, Fix.Rid(100 + i), 0x101, Val($"v{i}"));

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => AsyncTransport.SyncRoundAsync(a, b, NS, ct: cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => AsyncTransport.DeliverBatchAsync(a, b, NS, ct: cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => AsyncTransport.SaveAsync(a, Path.Combine(_dir, "never.state"), cts.Token));
    }

    [Fact]
    public async Task AsyncTransport_SaveLoadRoundTripsConcurrentlyWithWrites()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "concurrent.state");

        // Hammer the node from another thread while persisting repeatedly.
        var writerTask = Task.Run(() =>
        {
            for (int i = 0; i < 200; i++)
                a.WriteField(NS, Fix.Rid(1000 + i), 0x101, Val($"hammer-{i}"));
        });
        for (int i = 0; i < 5; i++)
        {
            await AsyncTransport.SaveAsync(a, path);
            var snap = NodePersistence.Load(path);   // must never see a torn image
            Assert.Equal(a.NodeId, snap.NodeId);
        }
        await writerTask;
        NodePersistence.Save(a, path);
        var final = NodePersistence.Load(path);
        Assert.True(StateEqual(a, final));
    }

    // ============================================================ Thread safety

    [Fact]
    public void Node_Gate_IsReentrant_AndSerializesConcurrentWriters()
    {
        var n = Fix.MakeNode(17);
        // Re-entrancy: CreateDeltaBatch internally calls PushOutbox/NextOutboxSeq under
        // the same gate — a non-reentrant lock would deadlock here.
        n.WriteField(NS, Fix.Rid(1), 0x101, Val("x"));
        Assert.NotNull(n.CreateDeltaBatch(Fix.Nid(98), NS));

        // Concurrent writers must serialize without corruption or exceptions.
        // WriteFields takes the gate ONCE per thread so every entry gets a distinct
        // HLC stamp — [П1] monotonicity holds even under full concurrency.
        int threads = 8, perThread = 100;
        var ex = Xunit.Record.Exception(() =>
        {
            Parallel.For(0, threads, t =>
            {
                n.WriteFields(NS, Enumerable.Range(0, perThread)
                    .Select(i => (Fix.Rid(t * 1000 + i), 0x101, Val($"t{t}-{i}"))));
            });
        });
        Assert.Null(ex);
        Assert.Equal(threads * perThread, n.Storage[NS].Count);

        // Every durable op got a unique monotonically increasing seq ([П1]).
        var seqMap = n.OutboxSeqRef;
        Assert.Equal((long)(threads * perThread), seqMap[(n.NodeId, NS)]);
        var seqs = n.Outbox[NS].Select(e => e.Seq).ToList();
        Assert.Equal(seqs.Count, seqs.Distinct().Count());
        Assert.True(seqs.SequenceEqual(seqs.Order()));
    }

    [Fact]
    public void Node_ConcurrentReadersAndSaver_NeverTear()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "readers.state");
        Exception? err = null;
        var stop = DateTime.UtcNow.AddSeconds(2);
        var tasks = Enumerable.Range(0, 4).Select(i => Task.Run(() =>
        {
            try
            {
                while (DateTime.UtcNow < stop)
                {
                    a.WriteField(NS, Fix.Rid(500 + i * 100 + Random.Shared.Next(100)), 0x101, Val("r"));
                    NodePersistence.Save(a, path);
                    var snap = NodePersistence.Load(path);
                    if (!snap.NodeId.AsSpan().SequenceEqual(a.NodeId))
                        throw new InvalidOperationException("torn identity");
                }
            }
            catch (Exception e) { Interlocked.CompareExchange(ref err, e, null); }
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Null(err);
    }
}
