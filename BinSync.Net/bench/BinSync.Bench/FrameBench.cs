using BenchmarkDotNet.Attributes;
using BinSync;

namespace BinSync.Bench;

/// <summary>Wire frame build/parse (§12) + optional fragment path.</summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class FrameBench
{
    private byte[] _payload = null!;
    private byte[] _frame = null!;
    private byte[] _bigPayload = null!;
    private List<byte[]> _fragments = null!;

    [Params(64, 1024, 8192)]
    public int PayloadLen { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadLen];
        Random.Shared.NextBytes(_payload);
        _frame = FrameCodec.BuildFrame(0x09, streamId: 1, frameSeq: 42, _payload);

        _bigPayload = new byte[Math.Max(PayloadLen, 500)];
        Random.Shared.NextBytes(_bigPayload);
        _fragments = FragmentCodec.Split(_bigPayload, maxBody: 100, msgId: BenchFix.Nid(900))
            .ToList();
    }

    [Benchmark(Description = "BuildFrame")]
    public int BuildFrame()
        => FrameCodec.BuildFrame(0x09, 1, 42, _payload).Length;

    [Benchmark(Description = "ParseFrame")]
    public int ParseFrame()
    {
        var (fr, consumed) = FrameCodec.ParseFrame(_frame);
        return consumed + fr.Payload.Length;
    }

    [Benchmark(Description = "Build+Parse roundtrip")]
    public int Roundtrip()
    {
        var raw = FrameCodec.BuildFrame(0x09, 1, 99, _payload);
        var (fr, n) = FrameCodec.ParseFrame(raw);
        return n + fr.Payload.Length;
    }

    [Benchmark(Description = "Fragment split+reassemble")]
    public int Fragment_Roundtrip()
    {
        var parts = FragmentCodec.Split(_bigPayload, maxBody: 100, msgId: BenchFix.Nid(901));
        var re = new FragmentReassembler();
        byte[]? done = null;
        foreach (var p in parts)
            done = re.Accept(p) ?? done;
        return done?.Length ?? 0;
    }

    [Benchmark(Description = "Crc32C only")]
    public uint Crc32C()
        => Frames.Crc32C(_payload);
}
