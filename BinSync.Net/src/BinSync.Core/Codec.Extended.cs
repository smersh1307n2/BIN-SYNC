// Codec §6.1–6.5 extensions: signed ints, b256, TLV, UTF-8 strings, IEEE floats.
// Keeps Codec.cs (varint / b128) untouched — additive surface only.
using System.Buffers.Binary;
using System.Text;

namespace BinSync;

/// <summary>Raised when a float is NaN (§6.3 → INVALID_FLOAT).</summary>
public class InvalidFloatException : Exception
{
    /// <summary>InvalidFloatException for codec/protocol errors.</summary>
    public InvalidFloatException(string msg) : base(msg) { }
}

/// <summary>One raw TLV triple for unknown / extension fields (§6.5).</summary>
public readonly struct Tlv
{
    /// <summary>Discriminating tag for this element or format.</summary>
    public byte Tag { get; }
    /// <summary>Return a copy of the written buffer as a new byte array.</summary>
    public byte[] Value { get; }
    /// <summary>Tag-length-value element used in extended codec payloads.</summary>
    public Tlv(byte tag, byte[] value)
    {
        Tag = tag;
        Value = value ?? Array.Empty<byte>();
    }
}

/// <summary>Canonical binary codec: ULEB128 varints, little-endian fixed ints, b128, TLV, strings, floats.</summary>
public static partial class Codec
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    internal static byte[] EncodeUtf8(string? value) => StrictUtf8.GetBytes(value ?? string.Empty);

    internal static string DecodeUtf8(ReadOnlySpan<byte> bytes)
    {
        // Preserve the existing compatibility rule: strip one leading UTF-8 BOM.
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
            bytes = bytes[3..];
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException ex)
        {
            throw new InvalidDataException("Invalid UTF-8 string.", ex);
        }
    }

    /// <summary>Canonical LE encode of a signed 64-bit integer (two's complement).</summary>
    public static byte[] EncodeI64(long v)
    {
        var b = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(b, v);
        return b;
    }

    /// <summary>Decode a little-endian signed 64-bit integer.</summary>
    public static long DecodeI64(ReadOnlySpan<byte> data)
    {
        if (data.Length < 8) throw new EndOfStreamException("truncated i64");
        return BinaryPrimitives.ReadInt64LittleEndian(data);
    }

    /// <summary>Encode a signed 32-bit integer as little-endian bytes.</summary>
    public static byte[] EncodeI32(int v)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(b, v);
        return b;
    }

    /// <summary>Decode a little-endian signed 32-bit integer.</summary>
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

    /// <summary>Decode a TLV (tag + length-prefixed value) starting at pos; returns the element and next offset.</summary>
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
        var utf8 = EncodeUtf8(s);
        var w = new Writer();
        w.Varint((ulong)utf8.Length).Raw(utf8);
        return w.Value();
    }

    /// <summary>Decode a length-prefixed UTF-8 string; strips a leading BOM if present.</summary>
    public static string DecodeString(byte[] data, int pos, out int next)
    {
        var r = new Reader(data, pos);
        var bytes = r.Bytes();
        next = r.Pos;
        return DecodeUtf8(bytes);
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

    /// <summary>Decode a little-endian IEEE-754 binary64; rejects NaN.</summary>
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

/// <summary>Growable (or fixed-capacity) binary writer for canonical BIN-SYNC payloads.</summary>
public partial class Writer
{
    /// <summary>Append a little-endian signed 32-bit integer.</summary>
    public Writer I32(int v)
    {
        Span<byte> s = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(s, v);
        Write(s);
        return this;
    }

    /// <summary>Append a little-endian signed 64-bit integer.</summary>
    public Writer I64(long v)
    {
        Span<byte> s = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(s, v);
        Write(s);
        return this;
    }

    /// <summary>Append a fixed 32-byte blob.</summary>
    public Writer B256(byte[] v)
    {
        if (v.Length != 32) v = Codec.CanonicalB256(v);
        Write(v);
        return this;
    }

    /// <summary>Tag-length-value element used in extended codec payloads.</summary>
    public Writer Tlv(byte tag, byte[] value)
    {
        U8(tag).Varint((ulong)value.Length).Raw(value);
        return this;
    }

    /// <summary>Append a length-prefixed UTF-8 string.</summary>
    public Writer StringUtf8(string s)
    {
        var utf8 = Codec.EncodeUtf8(s);
        Varint((ulong)utf8.Length).Raw(utf8);
        return this;
    }

    /// <summary>Append a little-endian IEEE-754 binary64; rejects NaN.</summary>
    public Writer F64(double v)
    {
        Raw(Codec.EncodeF64(v));
        return this;
    }
}

/// <summary>Sequential binary reader over a byte array or ReadOnlyMemory slice.</summary>
public partial class Reader
{
    /// <summary>Append a little-endian signed 32-bit integer.</summary>
    public int I32() => BinaryPrimitives.ReadInt32LittleEndian(Take(4));
    /// <summary>Append a little-endian signed 64-bit integer.</summary>
    public long I64() => BinaryPrimitives.ReadInt64LittleEndian(Take(8));
    /// <summary>Append a fixed 32-byte blob.</summary>
    public byte[] B256() => Take(32);

    /// <summary>Tag-length-value element used in extended codec payloads.</summary>
    public Tlv Tlv()
    {
        byte tag = U8();
        byte[] value = Bytes();
        return new Tlv(tag, value);
    }

    /// <summary>Append a length-prefixed UTF-8 string.</summary>
    public string StringUtf8()
    {
        var bytes = Bytes();
        return Codec.DecodeUtf8(bytes);
    }

    /// <summary>Append a little-endian IEEE-754 binary64; rejects NaN.</summary>
    public double F64() => Codec.DecodeF64(Take(8));
}
