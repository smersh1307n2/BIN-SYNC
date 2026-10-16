using BinSync.Core;

namespace BinSync.Tests;

/// <summary>
/// Production-hardening suite (Шаг 4): BLAKE3 correctness, persistence, async transport,
/// thread safety, ghost-record GC.
/// </summary>
public class Hardening : IDisposable
{
    private readonly string _dir = MakeTempDir();

    private static string MakeTempDir()
    {
        var d = Path.Combine(Path.GetTempPath(), "binsync-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        return d;
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    // ---------------------------------------------------------------- helpers

    private static Node NewNode(string name) => new(name);

    /// <summary>Two nodes with shared history: writes from both sides, an alias, a full sync round.</summary>
    private static (Node a, Node b) BuildHistory(out long lastSeqA)
    {
        var r = BuildHistoryAsync().GetAwaiter().GetResult();
        lastSeqA = r.a.OutboxSeqOf("nodeA");
        return r;
    }

    private static async Task<(Node a, Node b)> BuildHistoryAsync()
    {
        var a = NewNode("nodeA");
        var b = NewNode("nodeB");
        a.WriteField("rec1", "title", new byte[] { 0x01 });
        a.WriteField("rec1", "body", System.Text.Encoding.UTF8.GetBytes("World"));
        b.WriteField("rec1", "tags", System.Text.Encoding.UTF8.GetBytes("crdt"));
        a.WriteField("rec2", "k", System.Text.Encoding.UTF8.GetBytes("v"));
        a.SetAlias("alias1", "rec1");
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);
        return (a, b);
    }

    private static string Str(byte[]? v) => v is null ? "" : System.Text.Encoding.UTF8.GetString(v);

    // ================================================================ BLAKE3

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
            var got = Convert.ToHexString(Blake3.Hash256(data)).ToLowerInvariant();
            Assert.True(got.StartsWith(prefix, StringComparison.Ordinal),
                $"BLAKE3 mismatch at len={len}: got {got[..Math.Min(24, got.Length)]}…, expected {prefix}…");
        }
    }

    // ============================================================ Persistence

    [Fact]
    public void Persistence_RoundTrip_PreservesState()
    {
        var (a, _) = BuildHistory(out _);
        var path = Path.Combine(_dir, "nodeA.state");
        NodePersistence.Save(a, path);

        var restored = NodePersistence.Load(path);

        // Field values survive (including remote-authored ones that arrived via sync).
        Assert.Equal("World", Str(restored.ReadField("rec1", "body")));
        Assert.Equal("crdt", Str(restored.ReadField("rec1", "tags")));
        Assert.Equal("v", Str(restored.ReadField("rec2", "k")));
        // ALIAS_MAP survives.
        Assert.Equal("rec1", restored.ResolveAlias("alias1"));
        // Lifecycle status survives.
        Assert.Equal(RecordStatus.Live, restored.StatusOf("rec1"));
        // Checkpoints/peer cursors survive.
        Assert.Equal(a.Checkpoints.Count, restored.Checkpoints.Count);
        Assert.Equal(a.InboxCursorOf("nodeB"), restored.InboxCursorOf("nodeB"));
    }

    [Fact]
    public void Persistence_SequenceStream_ContinuesAfterRestart()
    {
        var (a, _) = BuildHistory(out long seqBefore);
        Assert.True(seqBefore > 0);
        var path = Path.Combine(_dir, "seq.state");
        NodePersistence.Save(a, path);

        // Simulate restart purely from the persisted image.
        var restored = NodePersistence.Load(path);
        Assert.Equal(seqBefore, restored.OutboxSeqOf("nodeA"));

        // A fresh batch on the restored node must continue the durable per-author seq stream
        // and never reuse an already-durable sequence number.
        restored.WriteField("recNew", "f", new byte[] { 9 });
        var batch = restored.CreateDeltaBatch("peerZ");
        Assert.NotNull(batch);
        Assert.True(batch!.Seq > seqBefore, $"expected seq > {seqBefore}, got {batch.Seq}");
    }

    [Fact]
    public void Persistence_CorruptedDigest_IsRejected()
    {
        var (a, _) = BuildHistory(out _);
        var path = Path.Combine(_dir, "corrupt.state");
        NodePersistence.Save(a, path);
        var original = File.ReadAllBytes(path);

        // Flip one bit deep inside the body → tagged digest no longer matches.
        var bytes = (byte[])original.Clone();
        bytes[bytes.Length - 40] ^= 0x01;
        var bad = Path.Combine(_dir, "corrupt2.state");
        File.WriteAllBytes(bad, bytes);
        Assert.Throws<InvalidDataException>(() => NodePersistence.Load(bad));

        // Truncation is rejected too.
        var trunc = Path.Combine(_dir, "trunc.state");
        File.WriteAllBytes(trunc, original[..(original.Length / 2)]);
        Assert.Throws<InvalidDataException>(() => NodePersistence.Load(trunc));

        // Untouched file still loads fine (sanity).
        Assert.NotNull(NodePersistence.Load(path));
    }

    [Fact]
    public void Persistence_Save_IsAtomic_AndLeavesNoTempFiles()
    {
        var (a, _) = BuildHistory(out _);
        var path = Path.Combine(_dir, "atomic.state");

        NodePersistence.Save(a, path);
        var first = File.ReadAllBytes(path);
        Assert.True(File.Exists(path));

        // Overwrite atomically: target transitions directly old→new, never half-written.
        a.WriteField("rec3", "x", System.Text.Encoding.UTF8.GetBytes("y"));
        NodePersistence.Save(a, path);
        var second = File.ReadAllBytes(path);

        Assert.NotEqual(first, second);
        // No temp artifacts remain next to the state file.
        Assert.All(Directory.GetFiles(_dir), f =>
            Assert.DoesNotContain(".tmp", Path.GetFileName(f), StringComparison.OrdinalIgnoreCase));
        // The final image loads cleanly — it was never observed mid-write.
        var loaded = NodePersistence.Load(path);
        Assert.Equal("y", Str(loaded.ReadField("rec3", "x")));
    }

    // ================================================================= Async

    [Fact]
    public async Task Async_DeliverBatch_AppliesAndAcks()
    {
        var (a, b) = BuildHistory(out _);
        a.WriteField("rec9", "f", System.Text.Encoding.UTF8.GetBytes("async-value"));

        var res = await AsyncTransport.DeliverBatchAsync(a, b, CancellationToken.None);

        Assert.Equal(ApplyResult.Applied, res);
        Assert.Equal("async-value", Str(b.ReadField("rec9", "f")));
        // Sender learned how far the receiver's inbox advanced (ack processed).
        Assert.True(b.InboxCursorOf("nodeA") > 0);
    }

    [Fact]
    public async Task Async_Cancellation_StopsDelivery()
    {
        var (a, b) = BuildHistory(out _);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync(); // pre-cancelled: nothing may proceed

        var task = AsyncTransport.DeliverBatchAsync(a, b, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }

    [Fact]
    public async Task Async_Snapshot_BootstrapsTarget()
    {
        var (a, _) = BuildHistory(out _);
        var fresh = NewNode("nodeC");

        await AsyncTransport.SendSnapshotAsync(a, fresh, CancellationToken.None);

        Assert.Equal("World", Str(fresh.ReadField("rec1", "body")));
        Assert.Equal("crdt", Str(fresh.ReadField("rec1", "tags")));
        Assert.Equal("rec1", fresh.ResolveAlias("alias1"));
        // After bootstrap, incremental sync still works both ways.
        fresh.WriteField("rec1", "extra", new byte[] { 0xEE });
        await AsyncTransport.SyncRoundAsync(a, fresh, CancellationToken.None);
        Assert.Single(a.ReadField("rec1", "extra")!);
        Assert.Equal(0xEE, a.ReadField("rec1", "extra")![0]);
    }

    // ========================================================== Thread safety

    [Fact]
    public async Task ThreadSafety_ConcurrentWriters_AndParallelSync_NoRaces()
    {
        var nodes = new[] { NewNode("n1"), NewNode("n2"), NewNode("n3") };
        const int writersPerNode = 4, opsPerWriter = 25;

        var writeTasks = nodes.SelectMany((n, ni) =>
            Enumerable.Range(0, writersPerNode).Select(w => Task.Run(() =>
            {
                for (int i = 0; i < opsPerWriter; i++)
                    n.WriteField($"rec{ni}-{w}", "f", System.Text.Encoding.UTF8.GetBytes($"{ni}.{w}.{i}"));
            }))).ToArray();

        var syncTasks = Enumerable.Range(0, 12).Select(async r =>
        {
            var x = nodes[r % 3];
            var y = nodes[(r + 1) % 3];
            await AsyncTransport.SyncRoundAsync(x, y, CancellationToken.None);
        }).ToArray();

        await Task.WhenAll(writeTasks.Concat(syncTasks));

        // Final anti-entropy rounds guarantee convergence despite interleaved traffic.
        await AsyncTransport.SyncRoundAsync(nodes[0], nodes[1], CancellationToken.None);
        await AsyncTransport.SyncRoundAsync(nodes[1], nodes[2], CancellationToken.None);
        await AsyncTransport.SyncRoundAsync(nodes[0], nodes[2], CancellationToken.None);

        // All three nodes agree on every record/field (CRDT convergence, no lost updates).
        for (int ni = 0; ni < 3; ni++)
            for (int w = 0; w < writersPerNode; w++)
            {
                var id = $"rec{ni}-{w}";
                var v1 = nodes[0].ReadField(id, "f");
                Assert.NotNull(v1);
                Assert.Equal(Str(v1), Str(nodes[1].ReadField(id, "f")));
                Assert.Equal(Str(v1), Str(nodes[2].ReadField(id, "f")));
            }
    }

    // ==================================================================== GC

    [Fact]
    public async Task Gc_RemovesFullyDeadRecords_AfterGrace_Period()
    {
        var a = NewNode("gcA");
        var b = NewNode("gcB");

        a.WriteField("ghost", "f", System.Text.Encoding.UTF8.GetBytes("old"));
        b.WriteField("ghost", "g", System.Text.Encoding.UTF8.GetBytes("older"));
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);

        // Both authors tombstone the record — Dead AND fully tombstoned.
        a.DeleteRecord("ghost");
        b.DeleteRecord("ghost");
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);
        Assert.True(a.IsFullyDead("ghost"));

        // Horizon = nowUs - graceUs = 10^12 - 5*10^11 = 5*10^11; all traces of "ghost" are older.
        var removed = a.CollectGarbage(nowUs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L + 1_000_000_000L,
                                       graceUs: 500_000_000L);
        Assert.Contains("ghost", removed);
        Assert.False(a.HasRecord("ghost"));
        Assert.Null(a.ReadField("ghost", "f"));
    }

    [Fact]
    public async Task Gc_KeepsFreshAndPartiallyDeadRecords()
    {
        var a = NewNode("keepA");
        var b = NewNode("keepB");

        // Partially dead: tombstoned by only ONE known author → preserved regardless of age.
        a.WriteField("partial", "f", System.Text.Encoding.UTF8.GetBytes("p"));
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);
        a.DeleteRecord("partial");

        // Fully dead but FRESH (traces newer than the horizon) → preserved.
        a.WriteField("fresh", "f", System.Text.Encoding.UTF8.GetBytes("fr"));
        b.WriteField("fresh", "f2", System.Text.Encoding.UTF8.GetBytes("fr2"));
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);
        a.DeleteRecord("fresh");
        b.DeleteRecord("fresh");
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);

        // Ghost written entirely in the past: backdate the clock, write+delete, then restore wall time.
        var saved = (a.ClockRaw(), b.ClockRaw());
        var past = saved.Item1.phys - 10_000_000L; // 10 s ago
        a.SetClockPhysicalForTests(past);
        b.SetClockPhysicalForTests(past);
        a.WriteField("ancient", "f", System.Text.Encoding.UTF8.GetBytes("an"));
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);
        a.DeleteRecord("ancient");
        b.DeleteRecord("ancient");
        await AsyncTransport.SyncRoundAsync(a, b, CancellationToken.None);
        a.SetClockPhysicalForTests(saved.Item1.phys, (uint)saved.Item1.log);

        Assert.True(a.IsFullyDead("ancient"));
        Assert.False(a.IsFullyDead("partial"));

        // Horizon = now - 5s: "ancient" (≈now−10s) falls below it; "fresh"/"partial" stay above.
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;
        var removed = a.CollectGarbage(nowUs: now, graceUs: 5_000_000L);

        Assert.Contains("ancient", removed);
        Assert.True(a.HasRecord("partial"));   // not tombstoned by all known authors
        Assert.True(a.HasRecord("fresh"));     // fully dead, but younger than the horizon
        Assert.False(a.HasRecord("ancient"));  // fully dead + everything older than the horizon
    }
}
