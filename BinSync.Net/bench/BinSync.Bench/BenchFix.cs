// Shared fixtures for benchmarks (mirrors tests Fix, no xunit dependency).
using System.Buffers.Binary;
using System.Text;
using BinSync;

namespace BinSync.Bench;

internal static class BenchFix
{
    public const int NS = 1;

    public static byte[] Rid(long n)
    {
        var b = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(b.AsSpan(8), n);
        return b;
    }

    public static byte[] Nid(int seed) => Rid(seed);

    public static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);

    public static byte[] Val(int n)
    {
        var b = new byte[Math.Max(4, n)];
        BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(0, 4), n);
        return b;
    }

    public static Node MakeNode(int seed, Func<long>? nowFn = null) =>
        new(Nid(seed), epoch: 1, nowFn: nowFn ?? (() => 1_000_000L + seed));
}
