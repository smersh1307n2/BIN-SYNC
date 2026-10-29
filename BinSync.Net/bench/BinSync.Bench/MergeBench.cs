using BenchmarkDotNet.Attributes;
using BinSync;

namespace BinSync.Bench;

/// <summary>Merge policies (§8) — order-independent apply cost.</summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MergeBench
{
    private FieldState[] _states = null!;
    private FieldState _local = null!;
    private FieldState _remote = null!;

    [Params(32, 128)]
    public int N { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        _states = new FieldState[N];
        for (int i = 0; i < N; i++)
        {
            var val = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(val, (ulong)rng.NextInt64(0, 1L << 40));
            _states[i] = new FieldState(
                val,
                new Hlc((ulong)rng.Next(1, 10_000), (uint)rng.Next(0, 50)),
                BenchFix.Nid(i + 1));
        }
        _local = _states[0];
        _remote = _states[^1];
    }

    [Benchmark(Description = "LWW single pair")]
    public bool Lww_Pair()
    {
        var (_, changed) = Merge.MergeFieldWithPolicy(_local, _remote, MergePolicy.LwwField);
        return changed;
    }

    [Benchmark(Description = "MaxU64 single pair")]
    public bool MaxU64_Pair()
    {
        var (_, changed) = Merge.MergeFieldWithPolicy(_local, _remote, MergePolicy.MaxU64);
        return changed;
    }

    [Benchmark(Description = "LWW fold N states")]
    public int Lww_FoldN()
    {
        FieldState? acc = null;
        int changes = 0;
        foreach (var s in _states)
        {
            var (w, ch) = Merge.MergeFieldWithPolicy(acc, s, MergePolicy.LwwField);
            acc = w;
            if (ch) changes++;
        }
        return changes;
    }

    [Benchmark(Description = "MaxU64 fold N states")]
    public int MaxU64_FoldN()
    {
        FieldState? acc = null;
        int changes = 0;
        foreach (var s in _states)
        {
            var (w, ch) = Merge.MergeFieldWithPolicy(acc, s, MergePolicy.MaxU64);
            acc = w;
            if (ch) changes++;
        }
        return changes;
    }

    [Benchmark(Description = "MinBytes fold N states")]
    public int MinBytes_FoldN()
    {
        FieldState? acc = null;
        int changes = 0;
        foreach (var s in _states)
        {
            var (w, ch) = Merge.MergeFieldWithPolicy(acc, s, MergePolicy.MinBytes);
            acc = w;
            if (ch) changes++;
        }
        return changes;
    }
}
