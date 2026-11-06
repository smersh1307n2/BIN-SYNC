using BenchmarkDotNet.Attributes;
using BinSync;

namespace BinSync.Bench;

/// <summary>
/// Isolates the transport delta builder from the public Batch DTO path.
/// Both benchmarks operate on the same pending outbox shape and include
/// record selection, alias translation, records-root hashing and wire encoding.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class TransportDeltaBench
{
    private Node _sender = null!;
    private Node _receiver = null!;

    [Params(10, 100, 500)]
    public int FieldCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _sender = BenchFix.MakeNode(3001, () => 4_000_000);
        _receiver = BenchFix.MakeNode(3002, () => 4_000_000);
        Transport.MakeSession(_sender, _receiver);

        for (int i = 0; i < FieldCount; i++)
        {
            _sender.WriteField(
                BenchFix.NS,
                BenchFix.Rid(30_000 + i),
                0x100,
                BenchFix.Val(i),
                new Hlc((ulong)(4_000_000 + i), 0));
        }
    }

    [Benchmark(Baseline = true, Description = "Public Batch + Encode")]
    public int PublicBatchAndEncode()
    {
        var batch = _sender.CreateDeltaBatch(_receiver.NodeId, BenchFix.NS);
        return batch is null ? 0 : _sender.EncodeBatchPayload(batch).Length;
    }

    [Benchmark(Description = "Internal transport payload")]
    public int InternalTransportPayload()
    {
        var payload = _sender.CreateDeltaBatchPayloadForTransport(_receiver.NodeId, BenchFix.NS);
        return payload?.Payload.Length ?? 0;
    }
}
