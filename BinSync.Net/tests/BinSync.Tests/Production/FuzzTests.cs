// Deterministic fuzz / stress harness (P2).
// No external fuzzer dependency — seeded PRNG + fixed iteration counts for CI.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class FuzzTests
{
    static FieldState Fs(byte[] value, ulong wall, uint logical, byte[] author)
        => new(value, new Hlc(wall, logical), author);

    static byte[] Author(int seed)
    {
        var a = new byte[16];
        BitConverter.TryWriteBytes(a.AsSpan(0, 4), seed);
        a[15] = (byte)(seed & 0xFF);
        return a;
    }

    static byte[] RandBytes(Random rng, int maxLen)
    {
        int n = rng.Next(0, maxLen + 1);
        var b = new byte[n];
        rng.NextBytes(b);
        return b;
    }

    /// <summary>
    /// Concurrent MaxU64 merges from many authors must converge to the global numeric max
    /// regardless of application order (commutativity under the policy).
    /// </summary>
    [Fact]
    public void Fuzz_MaxU64_converges_to_global_max()
    {
        var rng = new Random(42);
        const int N = 200;
        var states = new FieldState[N];
        ulong globalMax = 0;
        for (int i = 0; i < N; i++)
        {
            ulong v = (ulong)rng.NextInt64(0, 1L << 40);
            if (v > globalMax) globalMax = v;
            states[i] = Fs(BitConverter.GetBytes(v), wall: (ulong)rng.Next(1, 1000),
                logical: (uint)rng.Next(0, 50), Author(rng.Next()));
        }

        // Two different shuffle orders
        FieldState? a = null, b = null;
        var orderA = Enumerable.Range(0, N).OrderBy(_ => rng.Next()).ToArray();
        var orderB = Enumerable.Range(0, N).OrderBy(_ => rng.Next()).ToArray();
        foreach (int i in orderA)
            a = Merge.MergeFieldWithPolicy(a, states[i], MergePolicy.MaxU64).Winner;
        foreach (int i in orderB)
            b = Merge.MergeFieldWithPolicy(b, states[i], MergePolicy.MaxU64).Winner;

        Assert.NotNull(a);
        Assert.NotNull(b);
        Assert.Equal(globalMax, Merge.AsU64(a!.Value));
        Assert.Equal(globalMax, Merge.AsU64(b!.Value));
        Assert.Equal(a.Value, b.Value);
    }

    /// <summary>LWW: highest (wall, logical, author) wins; order-independent for the same set.</summary>
    [Fact]
    public void Fuzz_Lww_converges_to_same_winner()
    {
        var rng = new Random(7);
        const int N = 150;
        var states = new FieldState[N];
        for (int i = 0; i < N; i++)
            states[i] = Fs(RandBytes(rng, 8), (ulong)rng.Next(1, 5000),
                (uint)rng.Next(0, 100), Author(rng.Next()));

        FieldState? a = null, b = null;
        foreach (int i in Enumerable.Range(0, N).OrderBy(_ => rng.Next()))
            a = Merge.MergeFieldWithPolicy(a, states[i], MergePolicy.LwwField).Winner;
        foreach (int i in Enumerable.Range(0, N).OrderBy(_ => rng.Next()))
            b = Merge.MergeFieldWithPolicy(b, states[i], MergePolicy.LwwField).Winner;

        Assert.NotNull(a);
        Assert.NotNull(b);
        Assert.Equal(a!.Timestamp.Wall, b!.Timestamp.Wall);
        Assert.Equal(a.Timestamp.Logical, b.Timestamp.Logical);
        Assert.Equal(a.AuthorNodeId, b.AuthorNodeId);
        Assert.Equal(a.Value, b.Value);
    }

    /// <summary>WAL: random field writes survive close/reopen + ReplayFile.</summary>
    [Fact]
    public void Fuzz_Wal_replay_matches_in_memory()
    {
        var rng = new Random(99);
        var dir = Path.Combine(Path.GetTempPath(), "binsync-fuzz-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var live = new Node(Author(1), epoch: 1);
            const int N = 80;

            using (var wal = Wal.Open(walPath))
            {
                for (int i = 0; i < N; i++)
                {
                    var rid = Author(1000 + i);
                    ushort fid = (ushort)(0x100 + (i % 16));
                    var value = RandBytes(rng, 12);
                    var hlc = new Hlc((ulong)(1000 + i), (uint)(i % 7));
                    var author = Author(rng.Next(1, 50));
                    live.ApplyRemoteField(1, rid, fid, value, hlc, author);
                    wal.AppendFieldWrite(1, rid, fid, value, hlc, author);
                }
            }

            var recovered = new Node(Author(2), epoch: 1);
            int applied = Wal.ReplayFile(walPath, recovered);
            Assert.Equal(N, applied);

            // Compare storage under ns=1
            Assert.True(live.Storage.ContainsKey(1));
            Assert.True(recovered.Storage.ContainsKey(1));
            Assert.Equal(live.Storage[1].Count, recovered.Storage[1].Count);
            foreach (var (rid, rec) in live.Storage[1])
            {
                Assert.True(recovered.Storage[1].ContainsKey(rid));
                var r2 = recovered.Storage[1][rid];
                foreach (var (fid, fs) in rec.State)
                {
                    Assert.True(r2.State.ContainsKey(fid));
                    Assert.Equal(fs.Value, r2.State[fid].Value);
                    Assert.Equal(fs.Timestamp.Wall, r2.State[fid].Timestamp.Wall);
                    Assert.Equal(fs.Timestamp.Logical, r2.State[fid].Timestamp.Logical);
                }
            }
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    /// <summary>Codec: random Bytes + B128 round-trips.</summary>
    [Fact]
    public void Fuzz_Codec_bytes_and_b128_roundtrip()
    {
        var rng = new Random(12345);
        for (int i = 0; i < 300; i++)
        {
            var payload = RandBytes(rng, 64);
            var id = new byte[16];
            rng.NextBytes(id);

            var w = new Writer();
            w.Bytes(payload);
            w.B128(id);
            w.Varint((ulong)rng.Next(0, int.MaxValue));
            var raw = w.Value();

            var r = new Reader(raw);
            Assert.Equal(payload, r.Bytes());
            Assert.Equal(id, r.B128());
            _ = r.Varint();
        }
    }

    /// <summary>MinBytes: global lexicographic minimum under random order.</summary>
    [Fact]
    public void Fuzz_MinBytes_converges_to_global_min()
    {
        var rng = new Random(55);
        const int N = 100;
        var states = new FieldState[N];
        byte[]? globalMin = null;
        for (int i = 0; i < N; i++)
        {
            var v = RandBytes(rng, 6);
            if (v.Length == 0) v = new byte[] { 0xFF }; // avoid empty edge for stable min
            states[i] = Fs(v, (ulong)rng.Next(1, 100), 0, Author(i + 1));
            if (globalMin is null || Merge.CompareBytes(v, globalMin) < 0)
                globalMin = v;
        }

        FieldState? acc = null;
        foreach (int i in Enumerable.Range(0, N).OrderBy(_ => rng.Next()))
            acc = Merge.MergeFieldWithPolicy(acc, states[i], MergePolicy.MinBytes).Winner;

        Assert.NotNull(acc);
        Assert.Equal(globalMin, acc!.Value);
    }
}
