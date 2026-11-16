using BenchmarkDotNet.Attributes;
using BinSync;

namespace BinSync.Bench;

/// <summary>Codec / Writer / Reader hot path (§6).</summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class CodecBench
{
    private byte[] _payload = null!;
    private byte[] _id = null!;
    private byte[] _encoded = null!;
    private ulong[] _varints = null!;

    [Params(16, 64, 256)]
    public int PayloadLen { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadLen];
        Random.Shared.NextBytes(_payload);
        _id = BenchFix.Nid(42);
        _varints = new ulong[] { 0, 127, 128, 16_383, 1_000_000, ulong.MaxValue >> 1 };

        var w = new Writer();
        w.Bytes(_payload).B128(_id).Varint(1_000_000).U64(0xDEADBEEFCAFEBABEUL).U32(0xAABBCCDDu);
        _encoded = w.Value();
    }

    [Benchmark(Description = "WriteVarint x6 mix")]
    public int WriteVarint_Mix()
    {
        int total = 0;
        Span<byte> buf = stackalloc byte[16];
        foreach (var v in _varints)
            total += Codec.WriteVarint(buf, v);
        return total;
    }

    [Benchmark(Description = "EncodeVarint alloc x6")]
    public int EncodeVarint_Alloc()
    {
        int total = 0;
        foreach (var v in _varints)
            total += Codec.EncodeVarint(v).Length;
        return total;
    }

    [Benchmark(Description = "Writer Bytes+B128+Varint+U64")]
    public int Writer_Composite()
    {
        var w = new Writer();
        w.Bytes(_payload).B128(_id).Varint((ulong)_payload.Length).U64(1).U32(2);
        return w.Value().Length;
    }

    [Benchmark(Description = "Reader Bytes+B128+Varint+U64+U32")]
    public ulong Reader_Composite()
    {
        var r = new Reader(_encoded);
        _ = r.Bytes();
        _ = r.B128();
        var v = r.Varint();
        _ = r.U64();
        _ = r.U32();
        return v;
    }

    [Benchmark(Description = "CanonicalB128")]
    public int CanonicalB128()
    {
        var c = Codec.CanonicalB128(_id);
        return c.Length;
    }
}
