// Typed 16-byte identities for dictionary keys (no byte[] content-comparer on hot maps).
using System.Buffers.Binary;

namespace BinSync;

/// <summary>
/// Fixed 16-byte node identity. Equality and hash are value-based (two UInt64 limbs)
/// so maps do not need BA.
/// </summary>
public readonly struct NodeId : IEquatable<NodeId>
{
    public readonly ulong Lo;
    public readonly ulong Hi;

    public NodeId(ulong lo, ulong hi) { Lo = lo; Hi = hi; }

    public static NodeId From(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length == 16)
            return new NodeId(
                BinaryPrimitives.ReadUInt64LittleEndian(bytes),
                BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)));
        var can = Codec.CanonicalB128(bytes.ToArray());
        return new NodeId(
            BinaryPrimitives.ReadUInt64LittleEndian(can),
            BinaryPrimitives.ReadUInt64LittleEndian(can.AsSpan(8)));
    }

    public static NodeId From(byte[]? bytes) =>
        bytes is null || bytes.Length == 0 ? default : From(bytes.AsSpan());

    public static implicit operator NodeId(byte[]? bytes) => From(bytes);

    /// <summary>Wire form (b128). Allocates; prefer <see cref="WriteTo"/> on hot paths.</summary>
    public static implicit operator byte[](NodeId id) => id.ToArray();

    public void WriteTo(Span<byte> dest)
    {
        if (dest.Length < 16) throw new ArgumentException("need 16 bytes", nameof(dest));
        BinaryPrimitives.WriteUInt64LittleEndian(dest, Lo);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), Hi);
    }

    public byte[] ToArray()
    {
        var a = new byte[16];
        WriteTo(a);
        return a;
    }

    public bool IsZero => Lo == 0 && Hi == 0;

    /// <summary>Compare to a 16-byte wire id without allocating.</summary>
    public bool EqualsBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16) return false;
        Span<byte> tmp = stackalloc byte[16];
        WriteTo(tmp);
        return tmp.SequenceEqual(bytes);
    }

    public bool Equals(NodeId other) => Lo == other.Lo && Hi == other.Hi;
    public override bool Equals(object? obj) => obj is NodeId n && Equals(n);
    public override int GetHashCode() => HashCode.Combine(Lo, Hi);
    public static bool operator ==(NodeId a, NodeId b) => a.Equals(b);
    public static bool operator !=(NodeId a, NodeId b) => !a.Equals(b);

    public override string ToString() => Convert.ToHexString(ToArray());
}

/// <summary>
/// Fixed 16-byte record identity. Same layout and equality rules as <see cref="NodeId"/>.
/// </summary>
public readonly struct RecordId : IEquatable<RecordId>
{
    public readonly ulong Lo;
    public readonly ulong Hi;

    public RecordId(ulong lo, ulong hi) { Lo = lo; Hi = hi; }

    public static RecordId From(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length == 16)
            return new RecordId(
                BinaryPrimitives.ReadUInt64LittleEndian(bytes),
                BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)));
        var can = Codec.CanonicalB128(bytes.ToArray());
        return new RecordId(
            BinaryPrimitives.ReadUInt64LittleEndian(can),
            BinaryPrimitives.ReadUInt64LittleEndian(can.AsSpan(8)));
    }

    public static RecordId From(byte[]? bytes) =>
        bytes is null || bytes.Length == 0 ? default : From(bytes.AsSpan());

    public static implicit operator RecordId(byte[]? bytes) => From(bytes);

    /// <summary>Wire form (b128). Allocates; prefer <see cref="WriteTo"/> on hot paths.</summary>
    public static implicit operator byte[](RecordId id) => id.ToArray();

    public void WriteTo(Span<byte> dest)
    {
        if (dest.Length < 16) throw new ArgumentException("need 16 bytes", nameof(dest));
        BinaryPrimitives.WriteUInt64LittleEndian(dest, Lo);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), Hi);
    }

    public byte[] ToArray()
    {
        var a = new byte[16];
        WriteTo(a);
        return a;
    }

    public bool IsZero => Lo == 0 && Hi == 0;

    public bool EqualsBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16) return false;
        Span<byte> tmp = stackalloc byte[16];
        WriteTo(tmp);
        return tmp.SequenceEqual(bytes);
    }

    public bool Equals(RecordId other) => Lo == other.Lo && Hi == other.Hi;
    public override bool Equals(object? obj) => obj is RecordId r && Equals(r);
    public override int GetHashCode() => HashCode.Combine(Lo, Hi);
    public static bool operator ==(RecordId a, RecordId b) => a.Equals(b);
    public static bool operator !=(RecordId a, RecordId b) => !a.Equals(b);

    public override string ToString() => Convert.ToHexString(ToArray());
}


/// <summary>
/// Fixed 16-byte batch identity (batchId b128). Value equality like <see cref="NodeId"/>.
/// </summary>
public readonly struct BatchId : IEquatable<BatchId>
{
    public readonly ulong Lo;
    public readonly ulong Hi;

    public BatchId(ulong lo, ulong hi) { Lo = lo; Hi = hi; }

    public static BatchId From(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length == 16)
            return new BatchId(
                System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes),
                System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)));
        var can = Codec.CanonicalB128(bytes.ToArray());
        return new BatchId(
            System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(can),
            System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(can.AsSpan(8)));
    }

    public static BatchId From(byte[]? bytes) =>
        bytes is null || bytes.Length == 0 ? default : From(bytes.AsSpan());

    public static implicit operator BatchId(byte[]? bytes) => From(bytes);

    /// <summary>Wire form (b128). Allocates; prefer <see cref="WriteTo"/> on hot paths.</summary>
    public static implicit operator byte[](BatchId id) => id.ToArray();

    public void WriteTo(Span<byte> dest)
    {
        if (dest.Length < 16) throw new ArgumentException("need 16 bytes", nameof(dest));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest, Lo);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), Hi);
    }

    public byte[] ToArray()
    {
        var a = new byte[16];
        WriteTo(a);
        return a;
    }

    public bool IsZero => Lo == 0 && Hi == 0;

    public bool Equals(BatchId other) => Lo == other.Lo && Hi == other.Hi;
    public override bool Equals(object? obj) => obj is BatchId b && Equals(b);
    public override int GetHashCode() => HashCode.Combine(Lo, Hi);
    public static bool operator ==(BatchId a, BatchId b) => a.Equals(b);
    public static bool operator !=(BatchId a, BatchId b) => !a.Equals(b);

    public override string ToString() => Convert.ToHexString(ToArray());
}
