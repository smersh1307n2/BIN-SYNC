using BenchmarkDotNet.Attributes;
using BinSync;

namespace BinSync.Bench;

/// <summary>
/// End-to-end batch path: WriteField → CreateDeltaBatch → Encode → ApplyBatch.
/// This is the primary sync hot path for production profiling.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BatchBench
{
    private Node _sender = null!;
    private Node _receiver = null!;
    private Batch _batch = null!;
    private byte[] _payload = null!;

    [Params(10, 100, 500)]
    public int FieldCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _sender = BenchFix.MakeNode(1001, () => 2_000_000);
        _receiver = BenchFix.MakeNode(1002, () => 2_000_000);
        Transport.MakeSession(_sender, _receiver);

        for (int i = 0; i < FieldCount; i++)
        {
            _sender.WriteField(
                BenchFix.NS,
                BenchFix.Rid(10_000 + i),
                0x100,
                BenchFix.Val(i),
                new Hlc((ulong)(2_000_000 + i), 0));
        }

        _batch = _sender.CreateDeltaBatch(_receiver.NodeId, BenchFix.NS)
                 ?? throw new InvalidOperationException("CreateDeltaBatch returned null");
        _payload = _sender.EncodeBatchPayload(_batch);
    }

    [Benchmark(Description = "CreateDeltaBatch")]
    public int CreateDeltaBatch()
    {
        // Fresh receiver cursor each time would change semantics; measure encode of
        // already-staged outbox by re-creating from a clone-like second sender setup
        // is expensive. Here we re-call on same sender (idempotent empty after first).
        // Prefer ApplyBatch + Encode as primary; this still exercises batch assembly.
        var b = _sender.CreateDeltaBatch(_receiver.NodeId, BenchFix.NS);
        return b?.Ops.Count ?? 0;
    }

    [Benchmark(Description = "EncodeBatchPayload")]
    public int EncodeBatchPayload()
        => _sender.EncodeBatchPayload(_batch).Length;

    [Benchmark(Description = "DecodeBatchPayload")]
    public int DecodeBatchPayload()
        => _receiver.DecodeBatchPayload(_payload).Ops.Count;

    [Benchmark(Description = "ApplyBatch (idempotent replay)")]
    public int ApplyBatch_Replay()
    {
        // Same batch id → idempotent path after first apply in setup-less cold receiver.
        // Use a fresh receiver per iteration via [IterationSetup] would dominate cost;
        // measure steady-state idempotent ApplyBatch (duplicate detection).
        var r = _receiver.ApplyBatch(_batch, _sender.NodeId);
        return r.AppliedCount;
    }
}

/// <summary>
/// Cold ApplyBatch: new receiver every iteration — true first-apply cost.
/// Heavier; run with --filter *ApplyBatch_Cold*.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BatchApplyColdBench
{
    private Node _sender = null!;
    private Batch _batch = null!;

    [Params(10, 100)]
    public int FieldCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _sender = BenchFix.MakeNode(2001, () => 3_000_000);
        var bootstrap = BenchFix.MakeNode(2002, () => 3_000_000);
        Transport.MakeSession(_sender, bootstrap);

        for (int i = 0; i < FieldCount; i++)
        {
            _sender.WriteField(
                BenchFix.NS,
                BenchFix.Rid(20_000 + i),
                0x100,
                BenchFix.Val(i),
                new Hlc((ulong)(3_000_000 + i), 0));
        }

        _batch = _sender.CreateDeltaBatch(bootstrap.NodeId, BenchFix.NS)
                 ?? throw new InvalidOperationException("null batch");
    }

    [Benchmark(Description = "ApplyBatch cold receiver")]
    public int ApplyBatch_Cold()
    {
        var recv = BenchFix.MakeNode(2002, () => 3_000_000);
        Transport.MakeSession(_sender, recv);
        var r = recv.ApplyBatch(_batch, _sender.NodeId);
        return r.AppliedCount;
    }
}
