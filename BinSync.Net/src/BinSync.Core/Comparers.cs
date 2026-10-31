// Byte-content equality comparers for protocol maps (keys are often fresh byte[] allocations).
// Extracted from Node.cs — pure naming/structure; no behavior change.

namespace BinSync;

internal sealed class BytesEq : IEqualityComparer<byte[]>
{
    public static readonly BytesEq Instance = new();
    public bool Equals(byte[]? x, byte[]? y) =>
        x is null ? y is null : y is not null && x.AsSpan().SequenceEqual(y);
    public int GetHashCode(byte[] t)
    {
        var h = new HashCode();
        h.AddBytes(t);
        return h.ToHashCode();
    }
}

internal sealed class BytesLongLongEq : IEqualityComparer<(byte[], long, long)>
{
    public static readonly BytesLongLongEq Instance = new();
    public bool Equals((byte[], long, long) x, (byte[], long, long) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    public int GetHashCode((byte[], long, long) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3);
}

internal sealed class BytesIntIntBytesEq : IEqualityComparer<(byte[], int, int, byte[])>
{
    public static readonly BytesIntIntBytesEq Instance = new();
    public bool Equals((byte[], int, int, byte[]) x, (byte[], int, int, byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3
        && BytesEq.Instance.Equals(x.Item4, y.Item4);
    public int GetHashCode((byte[], int, int, byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3, BytesEq.Instance.GetHashCode(t.Item4));
}

internal sealed class BytesPairEq : IEqualityComparer<(byte[], byte[])>
{
    public static readonly BytesPairEq Instance = new();
    public bool Equals((byte[], byte[]) x, (byte[], byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && BytesEq.Instance.Equals(x.Item2, y.Item2);
    public int GetHashCode((byte[], byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), BytesEq.Instance.GetHashCode(t.Item2));
}

// Mixed-key comparers for KnownEpochs: keys are (peer|author, ns|-1|0) — the second
// component is an INT discriminator, so the tuple comparer must be (bytes, int).
internal sealed class BytesIntEq : IEqualityComparer<(byte[], int)>
{
    public static readonly BytesIntEq Instance = new();
    public bool Equals((byte[], int) x, (byte[], int) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2;
    public int GetHashCode((byte[], int) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2);
}

internal sealed class BytesIntIntEq : IEqualityComparer<(byte[], int, int)>
{
    public static readonly BytesIntIntEq Instance = new();
    public bool Equals((byte[], int, int) x, (byte[], int, int) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    public int GetHashCode((byte[], int, int) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3);
}

internal sealed class BytesIntPairEq : IEqualityComparer<((byte[], int), (byte[], int))>
{
    public static readonly BytesIntPairEq Instance = new();
    public bool Equals(((byte[], int), (byte[], int)) x, ((byte[], int), (byte[], int)) y) =>
        BytesIntEq.Instance.Equals(x.Item1, y.Item1) && BytesIntEq.Instance.Equals(x.Item2, y.Item2);
    public int GetHashCode(((byte[], int), (byte[], int)) t) =>
        HashCode.Combine(BytesIntEq.Instance.GetHashCode(t.Item1), BytesIntEq.Instance.GetHashCode(t.Item2));
}

internal sealed class Bytes3Eq : IEqualityComparer<(byte[], byte[], byte[])>
{
    public static readonly Bytes3Eq Instance = new();
    public bool Equals((byte[], byte[], byte[]) x, (byte[], byte[], byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && BytesEq.Instance.Equals(x.Item2, y.Item2)
        && BytesEq.Instance.Equals(x.Item3, y.Item3);
    public int GetHashCode((byte[], byte[], byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), BytesEq.Instance.GetHashCode(t.Item2), BytesEq.Instance.GetHashCode(t.Item3));
}

internal sealed class BytesIntIntEqB : IEqualityComparer<(byte[], int, int)>
{
    public static readonly BytesIntIntEqB Instance = new();
    public bool Equals((byte[], int, int) x, (byte[], int, int) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    public int GetHashCode((byte[], int, int) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3);
}

internal sealed class BytesIntIntBytesKeyEq : IEqualityComparer<((byte[], int, int), byte[])>
{
    public static readonly BytesIntIntBytesKeyEq Instance = new();
    public bool Equals(((byte[], int, int), byte[]) x, ((byte[], int, int), byte[]) y) =>
        BytesIntIntEqB.Instance.Equals(x.Item1, y.Item1) && BytesEq.Instance.Equals(x.Item2, y.Item2);
    public int GetHashCode(((byte[], int, int), byte[]) t) =>
        HashCode.Combine(BytesIntIntEqB.Instance.GetHashCode(t.Item1), BytesEq.Instance.GetHashCode(t.Item2));
}

internal sealed class BytesIntIntBytesEqB : IEqualityComparer<(byte[], int, int, byte[])>
{
    public static readonly BytesIntIntBytesEqB Instance = new();
    public bool Equals((byte[], int, int, byte[]) x, (byte[], int, int, byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3
        && BytesEq.Instance.Equals(x.Item4, y.Item4);
    public int GetHashCode((byte[], int, int, byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3, BytesEq.Instance.GetHashCode(t.Item4));
}

internal sealed class BytesPairEq2 : IEqualityComparer<(byte[], byte[])>
{
    public static readonly BytesPairEq2 Instance = new();
    public bool Equals((byte[], byte[]) x, (byte[], byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && BytesEq.Instance.Equals(x.Item2, y.Item2);
    public int GetHashCode((byte[], byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), BytesEq.Instance.GetHashCode(t.Item2));
}

internal sealed class IntBytesIntEq : IEqualityComparer<(int, byte[], int)>
{
    public static readonly IntBytesIntEq Instance = new();
    public bool Equals((int, byte[], int) x, (int, byte[], int) y) =>
        x.Item1 == y.Item1 && BytesEq.Instance.Equals(x.Item2, y.Item2) && x.Item3 == y.Item3;
    public int GetHashCode((int, byte[], int) t) =>
        HashCode.Combine(t.Item1, BytesEq.Instance.GetHashCode(t.Item2), t.Item3);
}

public sealed class SeenKeyComparer : IEqualityComparer<(byte[], int, byte[])>
{
    public static readonly SeenKeyComparer Instance = new();
    public bool Equals((byte[], int, byte[]) x, (byte[], int, byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && BytesEq.Instance.Equals(x.Item3, y.Item3);
    public int GetHashCode((byte[], int, byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, BytesEq.Instance.GetHashCode(t.Item3));
}

