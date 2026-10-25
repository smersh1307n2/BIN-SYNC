// Idiomatic C# key comparers for byte[]-keyed dictionaries/sets.
// Replaces the legacy zoo of ad-hoc comparers (BA, BAT, BA4, BAI2, ByteArrEq, TT, …)
// ported from Python. All comparators here are:
//   * singleton-based (no per-call allocations inside Equals/GetHashCode),
//   * structurally correct for nulls,
//   * hash-consistent with ByteArrayComparer across tuple arities,
//   * content-equality based on SequenceEqual (constant-time not required for map keys;
//     cryptographic comparison lives in Crypto/, see ConstantTimeEquals there).
namespace BinSync;

/// <summary>
/// Content equality comparer for <see cref="byte"/> arrays (map/set keys). Public so
/// consumers of the engine (and tests) can key their own dictionaries with exactly the
/// same semantics as the engine internals — one comparer to rule them all.
/// </summary>
public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
{
    /// <summary>Shared stateless instance (no per-call allocations).</summary>
    public static readonly ByteArrayComparer Instance = new();

    private ByteArrayComparer() { }

    public bool Equals(byte[]? x, byte[]? y) =>
        ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

    // FNV-1a — cheap and well-distributed for short fixed-size ids (16-byte nodeIds).
    public int GetHashCode(byte[] a)
    {
        unchecked
        {
            int h = 71; // seed: distinguishes empty array from default int hashes
            foreach (byte b in a) h = (h * 0x01000193) ^ b;
            return h;
        }
    }
}

/// <summary>Content equality comparer for pairs of byte arrays.</summary>
internal sealed class ByteArrayPairComparer : IEqualityComparer<(byte[], byte[])>
{
    public static readonly ByteArrayPairComparer Instance = new();

    public bool Equals((byte[], byte[]) x, (byte[], byte[]) y) =>
        ByteArrayComparer.Instance.Equals(x.Item1, y.Item1) &&
        ByteArrayComparer.Instance.Equals(x.Item2, y.Item2);

    public int GetHashCode((byte[], byte[]) t) =>
        HashCode.Combine(ByteArrayComparer.Instance.GetHashCode(t.Item1),
                         ByteArrayComparer.Instance.GetHashCode(t.Item2));
}

/// <summary>Content equality comparer for triples of byte arrays.</summary>
internal sealed class ByteArrayTripleComparer : IEqualityComparer<(byte[], byte[], byte[])>
{
    public static readonly ByteArrayTripleComparer Instance = new();

    public bool Equals((byte[], byte[], byte[]) x, (byte[], byte[], byte[]) y) =>
        ByteArrayComparer.Instance.Equals(x.Item1, y.Item1) &&
        ByteArrayComparer.Instance.Equals(x.Item2, y.Item2) &&
        ByteArrayComparer.Instance.Equals(x.Item3, y.Item3);

    public int GetHashCode((byte[], byte[], byte[]) t) =>
        HashCode.Combine(ByteArrayComparer.Instance.GetHashCode(t.Item1),
                         ByteArrayComparer.Instance.GetHashCode(t.Item2),
                         ByteArrayComparer.Instance.GetHashCode(t.Item3));
}

// Tuple comparers below mix one byte[] component (content-compared) with scalar
// components (value-compared). Naming: ByteArray{N}[suffix], where N is the arity
// and the suffix names the scalar kinds: L = long, I = int, B = trailing byte[].

/// <summary>(bytes, long, long) — deterministic batchId cache key (stream, cursorStart, cursorEnd).</summary>
internal sealed class ByteArrayLLComparer : IEqualityComparer<(byte[], long, long)>
{
    public static readonly ByteArrayLLComparer Instance = new();

    public bool Equals((byte[], long, long) x, (byte[], long, long) y) =>
        ByteArrayComparer.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;

    public int GetHashCode((byte[], long, long) t) =>
        HashCode.Combine(ByteArrayComparer.Instance.GetHashCode(t.Item1), t.Item2, t.Item3);
}

/// <summary>(bytes, int) — (author, namespace) / (nodeId, epoch) style keys.</summary>
internal sealed class ByteArrayIntComparer : IEqualityComparer<(byte[], int)>
{
    public static readonly ByteArrayIntComparer Instance = new();

    public bool Equals((byte[], int) x, (byte[], int) y) =>
        ByteArrayComparer.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2;

    public int GetHashCode((byte[], int) t) =>
        HashCode.Combine(ByteArrayComparer.Instance.GetHashCode(t.Item1), t.Item2);
}

/// <summary>(bytes, int, int) — (senderPeer, ns, epoch)-style stream keys.</summary>
internal sealed class ByteArrayIIComparer : IEqualityComparer<(byte[], int, int)>
{
    public static readonly ByteArrayIIComparer Instance = new();

    public bool Equals((byte[], int, int) x, (byte[], int, int) y) =>
        ByteArrayComparer.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;

    public int GetHashCode((byte[], int, int) t) =>
        HashCode.Combine(ByteArrayComparer.Instance.GetHashCode(t.Item1), t.Item2, t.Item3);
}

/// <summary>(bytes, int, int, bytes) — session-stream key (peer, ns, epoch, authorStream).</summary>
internal sealed class ByteArrayIIBComparer : IEqualityComparer<(byte[], int, int, byte[])>
{
    public static readonly ByteArrayIIBComparer Instance = new();

    public bool Equals((byte[], int, int, byte[]) x, (byte[], int, int, byte[]) y) =>
        ByteArrayComparer.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 &&
        x.Item3 == y.Item3 && ByteArrayComparer.Instance.Equals(x.Item4, y.Item4);

    public int GetHashCode((byte[], int, int, byte[]) t) =>
        HashCode.Combine(ByteArrayComparer.Instance.GetHashCode(t.Item1), t.Item2, t.Item3,
                         ByteArrayComparer.Instance.GetHashCode(t.Item4));
}

/// <summary>((bytes, int, int), bytes) — ((issuer, ns, epoch), targetPeer) stream-chain key.</summary>
internal sealed class ByteArrayIIBNestedComparer : IEqualityComparer<((byte[], int, int), byte[])>
{
    public static readonly ByteArrayIIBNestedComparer Instance = new();

    public bool Equals(((byte[], int, int), byte[]) x, ((byte[], int, int), byte[]) y) =>
        ByteArrayIIComparer.Instance.Equals(x.Item1, y.Item1) &&
        ByteArrayComparer.Instance.Equals(x.Item2, y.Item2);

    public int GetHashCode(((byte[], int, int), byte[]) t) =>
        HashCode.Combine(ByteArrayIIComparer.Instance.GetHashCode(t.Item1),
                         ByteArrayComparer.Instance.GetHashCode(t.Item2));
}

/// <summary>((bytes, int), (bytes, int)) — pair of (bytes, int) keys.</summary>
internal sealed class ByteArrayIntPairComparer : IEqualityComparer<((byte[], int), (byte[], int))>
{
    public static readonly ByteArrayIntPairComparer Instance = new();

    public bool Equals(((byte[], int), (byte[], int)) x, ((byte[], int), (byte[], int)) y) =>
        ByteArrayIntComparer.Instance.Equals(x.Item1, y.Item1) &&
        ByteArrayIntComparer.Instance.Equals(x.Item2, y.Item2);

    public int GetHashCode(((byte[], int), (byte[], int)) t) =>
        HashCode.Combine(ByteArrayIntComparer.Instance.GetHashCode(t.Item1),
                         ByteArrayIntComparer.Instance.GetHashCode(t.Item2));
}

/// <summary>(int, bytes, int) — discriminator-prefixed key.</summary>
internal sealed class IntByteArrayIntComparer : IEqualityComparer<(int, byte[], int)>
{
    public static readonly IntByteArrayIntComparer Instance = new();

    public bool Equals((int, byte[], int) x, (int, byte[], int) y) =>
        x.Item1 == y.Item1 && ByteArrayComparer.Instance.Equals(x.Item2, y.Item2) && x.Item3 == y.Item3;

    public int GetHashCode((int, byte[], int) t) =>
        HashCode.Combine(t.Item1, ByteArrayComparer.Instance.GetHashCode(t.Item2), t.Item3);
}

/// <summary>
/// Public seen-change key comparer: (senderPeer, ns, changeId) [Errata E-10/E-13].
/// Kept public because it backs the publicly visible <c>Node.SeenChanges</c> set.
/// </summary>
public sealed class SeenKeyComparer : IEqualityComparer<(byte[], int, byte[])>
{
    public static readonly SeenKeyComparer Instance = new();

    public bool Equals((byte[], int, byte[]) x, (byte[], int, byte[]) y) =>
        ByteArrayComparer.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 &&
        ByteArrayComparer.Instance.Equals(x.Item3, y.Item3);

    public int GetHashCode((byte[], int, byte[]) t) =>
        HashCode.Combine(ByteArrayComparer.Instance.GetHashCode(t.Item1), t.Item2,
                         ByteArrayComparer.Instance.GetHashCode(t.Item3));
}
