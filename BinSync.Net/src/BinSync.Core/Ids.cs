// Typed 16-byte identities for dictionary keys (no byte[] content-comparer on hot maps).
using System.Buffers.Binary;

namespace BinSync;

/// <summary>
/// Fixed 16-byte node identity. Equality and hash are value-based (two UInt64 limbs)
/// so maps do not need BA.
/// </summary>
public readonly partial struct NodeId : IEquatable<NodeId>
{
    /// <summary>Low 64 bits of the 128-bit identity.</summary>
    public readonly ulong Lo;
    /// <summary>High 64 bits of the 128-bit identity.</summary>
    public readonly ulong Hi;

    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public NodeId(ulong lo, ulong hi) { Lo = lo; Hi = hi; }

    /// <summary>Create an id from a 16-byte sequence.</summary>
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

    /// <summary>Create an id from a 16-byte sequence.</summary>
    public static NodeId From(byte[]? bytes) =>
        bytes is null || bytes.Length == 0 ? default : From(bytes.AsSpan());

    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public static implicit operator NodeId(byte[]? bytes) => From(bytes);
    /// <summary>Create an id from a 16-byte sequence.</summary>
    public static NodeId From(ReadOnlyMemory<byte> bytes) => From(bytes.Span);
    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public static implicit operator NodeId(ReadOnlyMemory<byte> bytes) => From(bytes);

    /// <summary>Wire form (b128). Allocates; prefer <see cref="WriteTo"/> on hot paths.</summary>
    public static implicit operator byte[](NodeId id) => id.ToArray();

    /// <summary>Write the 16 bytes into a destination span.</summary>
    public void WriteTo(Span<byte> dest)
    {
        if (dest.Length < 16) throw new ArgumentException("need 16 bytes", nameof(dest));
        BinaryPrimitives.WriteUInt64LittleEndian(dest, Lo);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), Hi);
    }

    /// <summary>Return an owned array for the memory (copy if it is a slice).</summary>
    public byte[] ToArray()
    {
        var a = new byte[16];
        WriteTo(a);
        return a;
    }

    /// <summary>True when all 16 bytes are zero.</summary>
    public bool IsZero => Lo == 0 && Hi == 0;

    /// <summary>Compare to a 16-byte wire id without allocating.</summary>
    public bool EqualsBytes(ReadOnlyMemory<byte> bytes) => EqualsBytes(bytes.Span);
    /// <summary>Compare this id to a byte span for content equality.</summary>
    public bool EqualsBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16) return false;
        Span<byte> tmp = stackalloc byte[16];
        WriteTo(tmp);
        return tmp.SequenceEqual(bytes);
    }

    /// <summary>Content equality comparison.</summary>
    public bool Equals(NodeId other) => Lo == other.Lo && Hi == other.Hi;
    /// <summary>Content equality comparison.</summary>
    public override bool Equals(object? obj) => obj is NodeId n && Equals(n);
    /// <summary>Hash code consistent with content equality.</summary>
    public override int GetHashCode() => HashCode.Combine(Lo, Hi);
    /// <summary>Content equality comparison.</summary>
    public static bool operator ==(NodeId a, NodeId b) => a.Equals(b);
    /// <summary>Content equality comparison.</summary>
    public static bool operator !=(NodeId a, NodeId b) => !a.Equals(b);

    /// <summary>Hex string of the 16-byte identity (uppercase).</summary>
    public override string ToString() => Convert.ToHexString(ToArray());
}

/// <summary>
/// Fixed 16-byte record identity. Same layout and equality rules as <see cref="NodeId"/>.
/// </summary>
public readonly partial struct RecordId : IEquatable<RecordId>
{
    /// <summary>Low 64 bits of the 128-bit identity.</summary>
    public readonly ulong Lo;
    /// <summary>High 64 bits of the 128-bit identity.</summary>
    public readonly ulong Hi;

    /// <summary>128-bit record identity with value equality.</summary>
    public RecordId(ulong lo, ulong hi) { Lo = lo; Hi = hi; }

    /// <summary>Create an id from a 16-byte sequence.</summary>
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

    /// <summary>Create an id from a 16-byte sequence.</summary>
    public static RecordId From(byte[]? bytes) =>
        bytes is null || bytes.Length == 0 ? default : From(bytes.AsSpan());

    /// <summary>128-bit record identity with value equality.</summary>
    public static implicit operator RecordId(byte[]? bytes) => From(bytes);
    /// <summary>Create an id from a 16-byte sequence.</summary>
    public static RecordId From(ReadOnlyMemory<byte> bytes) => From(bytes.Span);
    /// <summary>128-bit record identity with value equality.</summary>
    public static implicit operator RecordId(ReadOnlyMemory<byte> bytes) => From(bytes);

    /// <summary>Wire form (b128). Allocates; prefer <see cref="WriteTo"/> on hot paths.</summary>
    public static implicit operator byte[](RecordId id) => id.ToArray();

    /// <summary>Write the 16 bytes into a destination span.</summary>
    public void WriteTo(Span<byte> dest)
    {
        if (dest.Length < 16) throw new ArgumentException("need 16 bytes", nameof(dest));
        BinaryPrimitives.WriteUInt64LittleEndian(dest, Lo);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), Hi);
    }

    /// <summary>Return an owned array for the memory (copy if it is a slice).</summary>
    public byte[] ToArray()
    {
        var a = new byte[16];
        WriteTo(a);
        return a;
    }

    /// <summary>True when all 16 bytes are zero.</summary>
    public bool IsZero => Lo == 0 && Hi == 0;

    /// <summary>Compare this id to a byte span for content equality.</summary>
    public bool EqualsBytes(ReadOnlyMemory<byte> bytes) => EqualsBytes(bytes.Span);
    /// <summary>Compare this id to a byte span for content equality.</summary>
    public bool EqualsBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16) return false;
        Span<byte> tmp = stackalloc byte[16];
        WriteTo(tmp);
        return tmp.SequenceEqual(bytes);
    }

    /// <summary>Content equality comparison.</summary>
    public bool Equals(RecordId other) => Lo == other.Lo && Hi == other.Hi;
    /// <summary>Content equality comparison.</summary>
    public override bool Equals(object? obj) => obj is RecordId r && Equals(r);
    /// <summary>Hash code consistent with content equality.</summary>
    public override int GetHashCode() => HashCode.Combine(Lo, Hi);
    /// <summary>Content equality comparison.</summary>
    public static bool operator ==(RecordId a, RecordId b) => a.Equals(b);
    /// <summary>Content equality comparison.</summary>
    public static bool operator !=(RecordId a, RecordId b) => !a.Equals(b);

    /// <summary>Hex string of the 16-byte identity (uppercase).</summary>
    public override string ToString() => Convert.ToHexString(ToArray());
}


/// <summary>
/// Fixed 16-byte batch identity (batchId b128). Value equality like <see cref="NodeId"/>.
/// </summary>
public readonly partial struct BatchId : IEquatable<BatchId>
{
    /// <summary>Low 64 bits of the 128-bit identity.</summary>
    public readonly ulong Lo;
    /// <summary>High 64 bits of the 128-bit identity.</summary>
    public readonly ulong Hi;

    /// <summary>128-bit batch identity with value equality.</summary>
    public BatchId(ulong lo, ulong hi) { Lo = lo; Hi = hi; }

    /// <summary>Create an id from a 16-byte sequence.</summary>
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

    /// <summary>Create an id from a 16-byte sequence.</summary>
    public static BatchId From(byte[]? bytes) =>
        bytes is null || bytes.Length == 0 ? default : From(bytes.AsSpan());

    /// <summary>128-bit batch identity with value equality.</summary>
    public static implicit operator BatchId(byte[]? bytes) => From(bytes);
    public static BatchId From(ReadOnlyMemory<byte> bytes) => From(bytes.Span);
    public static implicit operator BatchId(ReadOnlyMemory<byte> bytes) => From(bytes);

    /// <summary>Wire form (b128). Allocates; prefer <see cref="WriteTo"/> on hot paths.</summary>
    public static implicit operator byte[](BatchId id) => id.ToArray();

    /// <summary>Write the 16 bytes into a destination span.</summary>
    public void WriteTo(Span<byte> dest)
    {
        if (dest.Length < 16) throw new ArgumentException("need 16 bytes", nameof(dest));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest, Lo);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), Hi);
    }

    /// <summary>Return an owned array for the memory (copy if it is a slice).</summary>
    public byte[] ToArray()
    {
        var a = new byte[16];
        WriteTo(a);
        return a;
    }

    /// <summary>True when all 16 bytes are zero.</summary>
    public bool IsZero => Lo == 0 && Hi == 0;

    /// <summary>Content equality comparison.</summary>
    public bool Equals(BatchId other) => Lo == other.Lo && Hi == other.Hi;
    /// <summary>Content equality comparison.</summary>
    public override bool Equals(object? obj) => obj is BatchId b && Equals(b);
    /// <summary>Hash code consistent with content equality.</summary>
    public override int GetHashCode() => HashCode.Combine(Lo, Hi);
    /// <summary>Content equality comparison.</summary>
    public static bool operator ==(BatchId a, BatchId b) => a.Equals(b);
    /// <summary>Content equality comparison.</summary>
    public static bool operator !=(BatchId a, BatchId b) => !a.Equals(b);

    /// <summary>Hex string of the 16-byte identity (uppercase).</summary>
    public override string ToString() => Convert.ToHexString(ToArray());
}
