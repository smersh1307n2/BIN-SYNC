// Byte-content equality comparers for protocol maps (keys are often fresh byte[] allocations).
// Extracted from Node.cs — pure naming/structure; no behavior change.

namespace BinSync;

internal sealed class BytesEq : IEqualityComparer<byte[]>
{
    /// <summary>Instance.</summary>
    public static readonly BytesEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals(byte[]? x, byte[]? y) =>
        x is null ? y is null : y is not null && x.AsSpan().SequenceEqual(y);
    /// <summary>GetHashCode.</summary>
    public int GetHashCode(byte[] t)
    {
        var h = new HashCode();
        h.AddBytes(t);
        return h.ToHashCode();
    }
}

internal sealed class BytesLongLongEq : IEqualityComparer<(byte[], long, long)>
{
    /// <summary>Instance.</summary>
    public static readonly BytesLongLongEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((byte[], long, long) x, (byte[], long, long) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((byte[], long, long) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3);
}

internal sealed class BytesIntIntBytesEq : IEqualityComparer<(byte[], int, int, byte[])>
{
    /// <summary>Instance.</summary>
    public static readonly BytesIntIntBytesEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((byte[], int, int, byte[]) x, (byte[], int, int, byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3
        && BytesEq.Instance.Equals(x.Item4, y.Item4);
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((byte[], int, int, byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3, BytesEq.Instance.GetHashCode(t.Item4));
}

internal sealed class BytesPairEq : IEqualityComparer<(byte[], byte[])>
{
    /// <summary>Instance.</summary>
    public static readonly BytesPairEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((byte[], byte[]) x, (byte[], byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && BytesEq.Instance.Equals(x.Item2, y.Item2);
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((byte[], byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), BytesEq.Instance.GetHashCode(t.Item2));
}

// Mixed-key comparers for KnownEpochs: keys are (peer|author, ns|-1|0) — the second
// component is an INT discriminator, so the tuple comparer must be (bytes, int).
internal sealed class BytesIntEq : IEqualityComparer<(byte[], int)>
{
    /// <summary>Instance.</summary>
    public static readonly BytesIntEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((byte[], int) x, (byte[], int) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2;
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((byte[], int) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2);
}

internal sealed class BytesIntIntEq : IEqualityComparer<(byte[], int, int)>
{
    /// <summary>Instance.</summary>
    public static readonly BytesIntIntEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((byte[], int, int) x, (byte[], int, int) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((byte[], int, int) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, t.Item3);
}

internal sealed class BytesIntPairEq : IEqualityComparer<((byte[], int), (byte[], int))>
{
    /// <summary>Instance.</summary>
    public static readonly BytesIntPairEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals(((byte[], int), (byte[], int)) x, ((byte[], int), (byte[], int)) y) =>
        BytesIntEq.Instance.Equals(x.Item1, y.Item1) && BytesIntEq.Instance.Equals(x.Item2, y.Item2);
    /// <summary>GetHashCode.</summary>
    public int GetHashCode(((byte[], int), (byte[], int)) t) =>
        HashCode.Combine(BytesIntEq.Instance.GetHashCode(t.Item1), BytesIntEq.Instance.GetHashCode(t.Item2));
}

internal sealed class Bytes3Eq : IEqualityComparer<(byte[], byte[], byte[])>
{
    /// <summary>Instance.</summary>
    public static readonly Bytes3Eq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((byte[], byte[], byte[]) x, (byte[], byte[], byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && BytesEq.Instance.Equals(x.Item2, y.Item2)
        && BytesEq.Instance.Equals(x.Item3, y.Item3);
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((byte[], byte[], byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), BytesEq.Instance.GetHashCode(t.Item2), BytesEq.Instance.GetHashCode(t.Item3));
}


internal sealed class BytesIntIntBytesKeyEq : IEqualityComparer<((byte[], int, int), byte[])>
{
    /// <summary>Instance.</summary>
    public static readonly BytesIntIntBytesKeyEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals(((byte[], int, int), byte[]) x, ((byte[], int, int), byte[]) y) =>
        BytesIntIntEq.Instance.Equals(x.Item1, y.Item1) && BytesEq.Instance.Equals(x.Item2, y.Item2);
    /// <summary>GetHashCode.</summary>
    public int GetHashCode(((byte[], int, int), byte[]) t) =>
        HashCode.Combine(BytesIntIntEq.Instance.GetHashCode(t.Item1), BytesEq.Instance.GetHashCode(t.Item2));
}



internal sealed class IntBytesIntEq : IEqualityComparer<(int, byte[], int)>
{
    /// <summary>Instance.</summary>
    public static readonly IntBytesIntEq Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((int, byte[], int) x, (int, byte[], int) y) =>
        x.Item1 == y.Item1 && BytesEq.Instance.Equals(x.Item2, y.Item2) && x.Item3 == y.Item3;
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((int, byte[], int) t) =>
        HashCode.Combine(t.Item1, BytesEq.Instance.GetHashCode(t.Item2), t.Item3);
}

/// <summary>Represents IEqualityComparer.</summary>
public sealed class SeenKeyComparer : IEqualityComparer<(byte[], int, byte[])>
{
    /// <summary>Instance.</summary>
    public static readonly SeenKeyComparer Instance = new();
    /// <summary>Equals.</summary>
    public bool Equals((byte[], int, byte[]) x, (byte[], int, byte[]) y) =>
        BytesEq.Instance.Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && BytesEq.Instance.Equals(x.Item3, y.Item3);
    /// <summary>GetHashCode.</summary>
    public int GetHashCode((byte[], int, byte[]) t) =>
        HashCode.Combine(BytesEq.Instance.GetHashCode(t.Item1), t.Item2, BytesEq.Instance.GetHashCode(t.Item3));
}

