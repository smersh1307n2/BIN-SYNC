// Canonical binary codec (§6): ULEB128 varint (overlong rejected), LE fixed ints, b128.
// Port of binsync/codec.py — all SPEC HARDENING / errata notes preserved.
using System.Buffers.Binary;
using Blake3;

namespace BinSync;

public class InvalidVarintException : Exception
{
    public InvalidVarintException(string msg) : base(msg) { }
}

public static class Codec
{
    public const ulong VarintMaxValue = ulong.MaxValue;

    public static byte[] EncodeVarint(ulong value)
    {
        var outBuf = new List<byte>();
        while (true)
        {
            byte b = (byte)(value & 0x7F);
            value >>= 7;
            if (value != 0) outBuf.Add((byte)(b | 0x80));
            else { outBuf.Add(b); break; }
        }
        return outBuf.ToArray();
    }

    // [К2] per-byte minimality rule (errata E-5): reject non-canonical encodings as
    // soon as redundancy appears ("\x80\x00"), keep b"\xff"*9+b"\xff" (=2^64-1) legal.
    // BUGFIX (port): mirror the Python reference exactly — width bound applies to
    // CONTINUATION bytes only (a canonical n-byte ULEB128 whose last byte is terminal
    // legitimately carries up to 7n bits); the domain bound (value <= 2^64-1) applies
    // always. Computed overflow-free: `(payload | (result >> shift)) >> width` equals
    // bitlen(result | payload<<shift) > width without wrapping at shift >= 64.
    private static bool CandidateOverflow(ulong result, int shift, int payload, int nbytes, bool last)
    {
        int width = 7 * nbytes;
        if (!last && ((ulong)payload | (result >> shift)) >>> width != 0UL) return true;
        // shift >= 64: payload bits are outside the u64 domain entirely.
        if (shift >= 64) return payload != 0;
        return result > ulong.MaxValue - ((ulong)payload << shift);
    }

    public static (ulong Value, int Pos) DecodeVarint(byte[] data, int pos = 0)
    {
        ulong result = 0;
        int shift = 0;
        int nbytes = 0;
        while (true)
        {
            if (pos >= data.Length) throw new InvalidVarintException("truncated varint");
            byte b = data[pos];
            pos += 1;
            nbytes += 1;
            int payload = b & 0x7F;
            bool last = (b & 0x80) == 0;
            if (payload == 0 && nbytes > 1 && last)
                throw new InvalidVarintException("overlong varint");
            if (shift >= 64 || CandidateOverflow(result, shift, payload, nbytes, last))
                throw new InvalidVarintException("overlong varint");
            result |= (ulong)payload << shift;
            shift += 7;
            if (last) break;
        }
        return (result, pos);
    }

    /// Deterministic widening/narrowing of an identity to exactly 16 bytes (§6.2 hardening).
    public static byte[] CanonicalB128(byte[] v)
    {
        if (v.Length == 16) return v;
        if (v.Length < 16)
        {
            var r = new byte[16];
            Array.Copy(v, r, v.Length);
            return r;
        }
        var pre = new byte["BIN-SYNC/AUTHID/v1".Length + v.Length];
        System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/AUTHID/v1").CopyTo(pre, 0);
        v.CopyTo(pre, "BIN-SYNC/AUTHID/v1".Length);
        return Hasher.Hash(pre).AsSpan().ToArray();
    }
}

public class Writer
{
    private readonly MemoryStream _buf = new();

    public Writer U8(byte v) { _buf.WriteByte(v); return this; }
    public Writer U16(ushort v) { Span<byte> s = stackalloc byte[2]; BinaryPrimitives.WriteUInt16LittleEndian(s, v); _buf.Write(s); return this; }
    public Writer U32(uint v) { Span<byte> s = stackalloc byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(s, v); _buf.Write(s); return this; }
    public Writer U64(ulong v) { Span<byte> s = stackalloc byte[8]; BinaryPrimitives.WriteUInt64LittleEndian(s, v); _buf.Write(s); return this; }

    public Writer B128(byte[] v)
    {
        if (v.Length != 16) v = Codec.CanonicalB128(v);
        _buf.Write(v);
        return this;
    }

    public Writer Varint(ulong v) { _buf.Write(Codec.EncodeVarint(v)); return this; }

    public Writer Bytes(byte[] b) { Varint((ulong)b.Length); _buf.Write(b); return this; }
    public Writer Raw(byte[] b) { _buf.Write(b); return this; }
    public byte[] Value() => _buf.ToArray();
}

public class Reader
{
    private readonly byte[] _data;
    public int Pos;
    public Reader(byte[] data, int pos = 0) { _data = data; Pos = pos; }

    private byte[] Take(int n)
    {
        if (Pos + n > _data.Length) throw new EndOfStreamException("truncated");
        var v = new byte[n];
        Array.Copy(_data, Pos, v, 0, n);
        Pos += n;
        return v;
    }

    public byte U8() => Take(1)[0];
    public ushort U16() => BinaryPrimitives.ReadUInt16LittleEndian(Take(2));
    public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));
    public ulong U64() => BinaryPrimitives.ReadUInt64LittleEndian(Take(8));
    public byte[] B128() => Take(16);
    public ulong Varint() { var (v, p) = Codec.DecodeVarint(_data, Pos); Pos = p; return v; }
    public byte[] Bytes() => Take((int)Varint());
    public byte[] Raw(int n) => Take(n);
    public bool Eof() => Pos >= _data.Length;
    public int Remaining => _data.Length - Pos;
}
