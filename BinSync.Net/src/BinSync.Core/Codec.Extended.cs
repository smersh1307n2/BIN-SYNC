// Codec §6.1–6.5 extensions: signed ints, b256, TLV, UTF-8 strings, IEEE floats.
// Keeps Codec.cs (varint / b128) untouched — additive surface only.
using System.Buffers.Binary;
using System.Text;

namespace BinSync;

/// <summary>Raised when a float is NaN (§6.3 → INVALID_FLOAT).</summary>
public class InvalidFloatException : Exception
{
    public InvalidFloatException(string msg) : base(msg) { }
}

/// <summary>One raw TLV triple for unknown / extension fields (§6.5).</summary>
public readonly struct Tlv
{
    public byte Tag { get; }
    public byte[] Value { get; }
    public Tlv(byte tag, byte[] value)
    {
        Tag = tag;
        Value = value ?? Array.Empty<byte>();
    }
}

public static partial class Codec
{
    /// <summary>Canonical LE encode of a signed 64-bit integer (two's complement).</summary>
    public static byte[] EncodeI64(long v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(b, v);
        return b;
    }

    public static long DecodeI64(ReadOnlySpan<byte> data)
    {
        if (data.Length < 8) throw new EndOfStreamException("truncated i64");
        return BinaryPrimitives.ReadInt64LittleEndian(data);
    }

    public static byte[] EncodeI32(int v)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(b, v);
        return b;
    }

    public static int DecodeI32(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4) throw new EndOfStreamException("truncated i32");
        return BinaryPrimitives.ReadInt32LittleEndian(data);
    }

    /// <summary>32-byte identity blob (pad or BLAKE3-truncate like b128).</summary>
    public static byte[] CanonicalB256(byte[] v)
    {
        if (v.Length == 32) return v;
        if (v.Length < 32)
        {
            var r = new byte[32];
            Array.Copy(v, r, v.Length);
            return r;
        }
        // Longer → BLAKE3-256
        return Blake3Core.Hash256(v);
    }

    /// <summary>TLV: tag u8 | len varint | value bytes (§6.1).</summary>
    public static byte[] EncodeTlv(byte tag, ReadOnlySpan<byte> value)
    {
        var w = new Writer();
        w.U8(tag).Varint((ulong)value.Length).Raw(value.ToArray());
        return w.Value();
    }

    public static Tlv DecodeTlv(byte[] data, int pos, out int next)
    {
        var r = new Reader(data, pos);
        byte tag = r.U8();
        byte[] value = r.Bytes();
        next = r.Pos;
        return new Tlv(tag, value);
    }

    /// <summary>UTF-8 string without BOM: len varint | bytes (§6.1).</summary>
    public static byte[] EncodeString(string s)
    {
        var utf8 = Encoding.UTF8.GetBytes(s ?? "");
        var w = new Writer();
        w.Varint((ulong)utf8.Length).Raw(utf8);
        return w.Value();
    }

    public static string DecodeString(byte[] data, int pos, out int next)
    {
        var r = new Reader(data, pos);
        var bytes = r.Bytes();
        next = r.Pos;
        // Reject BOM if present as first three bytes of content (spec: no BOM)
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
            return Encoding.UTF8.GetString(bytes, 3, bytes.Length - 3);
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// IEEE 754 binary64 LE. NaN → INVALID_FLOAT; −0.0 normalized to +0.0 (§6.3).
    /// </summary>
    public static byte[] EncodeF64(double v)
    {
        if (double.IsNaN(v))
            throw new InvalidFloatException("NaN is not a legal BIN-SYNC float");
        if (v == 0.0 && BitConverter.DoubleToInt64Bits(v) < 0)
            v = 0.0; // −0.0 → +0.0
        var b = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(b, v);
        return b;
    }

    public static double DecodeF64(ReadOnlySpan<byte> data)
    {
        if (data.Length < 8) throw new EndOfStreamException("truncated f64");
        double v = BinaryPrimitives.ReadDoubleLittleEndian(data);
        if (double.IsNaN(v))
            throw new InvalidFloatException("NaN is not a legal BIN-SYNC float");
        if (v == 0.0 && BitConverter.DoubleToInt64Bits(v) < 0)
            return 0.0;
        return v;
    }

    /// <summary>
    /// Sort field entries by fieldId ascending and reject duplicates (§6.2).
    /// Returns a new list; does not mutate input.
    /// </summary>
    public static List<KeyValuePair<ushort, byte[]>> CanonicalizeFields(
        IEnumerable<KeyValuePair<ushort, byte[]>> fields)
    {
        var sorted = fields.OrderBy(kv => kv.Key).ToList();
        for (int i = 1; i < sorted.Count; i++)
        {
            if (sorted[i].Key == sorted[i - 1].Key)
                throw new ArgumentException($"duplicate fieldId 0x{sorted[i].Key:X4}");
        }
        return sorted;
    }
}

public partial class Writer
{
    public Writer I32(int v)
    {
        Span<byte> s = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(s, v);
        _buf.Write(s);
        return this;
    }

    public Writer I64(long v)
    {
        Span<byte> s = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(s, v);
        _buf.Write(s);
        return this;
    }

    public Writer B256(byte[] v)
    {
        if (v.Length != 32) v = Codec.CanonicalB256(v);
        _buf.Write(v);
        return this;
    }

    public Writer Tlv(byte tag, byte[] value)
    {
        U8(tag).Varint((ulong)value.Length).Raw(value);
        return this;
    }

    public Writer StringUtf8(string s)
    {
        var utf8 = Encoding.UTF8.GetBytes(s ?? "");
        Varint((ulong)utf8.Length).Raw(utf8);
        return this;
    }

    public Writer F64(double v)
    {
        Raw(Codec.EncodeF64(v));
        return this;
    }
}

public partial class Reader
{
    public int I32() => BinaryPrimitives.ReadInt32LittleEndian(Take(4));
    public long I64() => BinaryPrimitives.ReadInt64LittleEndian(Take(8));
    public byte[] B256() => Take(32);

    public Tlv Tlv()
    {
        byte tag = U8();
        byte[] value = Bytes();
        return new Tlv(tag, value);
    }

    public string StringUtf8()
    {
        var bytes = Bytes();
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
            return Encoding.UTF8.GetString(bytes, 3, bytes.Length - 3);
        return Encoding.UTF8.GetString(bytes);
    }

    public double F64() => Codec.DecodeF64(Take(8));
}
