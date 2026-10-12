// Shared fixtures for the C# port of the BIN-SYNC test-suite.
// Mirrors tests/conftest.py (FakeClock semantics per harness BUGFIX note).
using System.Buffers.Binary;
using BinSync;

namespace BinSync.Tests;

public static class Fix
{
    /// Deterministic 128-bit nodeId from a small seed (big-endian padded).
    public static byte[] Rid(long n)
    {
        var b = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(b.AsSpan(8), n);
        return b;
    }

    public static byte[] Nid(int seed) => Rid(seed);

    public static Node MakeNode(int seed, Func<long>? nowFn = null) =>
        new(Nid(seed), epoch: 1, nowFn: nowFn ?? (() => (long)seed * 1_000_000));

    public static bool Eq(byte[]? a, byte[]? b) =>
        a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);

    public static string Hex(byte[] a) => Convert.ToHexString(a);
}

/// Controllable µs clock (same semantics as conftest.FakeClock): returns exactly
/// what was set; explicit advancement via Set()/Advance().
public sealed class FakeClock
{
    public long T;
    public FakeClock(long t = 1_000_000) => T = t;
    public long Now() => T;
    public void Set(long t) => T = t;
    public void Advance(long dt = 1) => T += dt;
}
