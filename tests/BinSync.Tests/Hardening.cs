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
            (0,     "af1349b9f5f9a1a6a0404dea"),
            (1,     "2d3adedff11b61f14c886e35"),
            (2,     "7b7015bb92cf0b318037702a"),
            (5,     "b40b44dfd97e7a84a996a91a"),
            (10,    "87fcf07cac5be3c91735b34e"),
            (1023,  "10108970eeda3eb932baac14"),
            (1024,  "42214739f095a406f3fc83de"),
            (1025,  "d00278ae47eb27b34faecf67"),
            (2048,  "e776b6028c7cd22a4d0ba160"),
            (2080,  "f28b7ca0b5de51703019b072"),
            (3072,  "b98cb0ff3623be03326b37db"),
            (4096,  "015094013f57a5277b59d844"),
            (5000,  "ee78d92070de3df1c57c3741"),
            (31774, "fdba8afea01e015596a42899"),
        };
        foreach (var (len, prefix) in vectors)
        {
            var data = new byte[len];
            for (int i = 0; i < len; i++) data[i] = (byte)(i % 251);
            var got = Convert.ToHexString(BinSync.Core.Blake3.Hash256(data)).ToLowerInvariant();
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
