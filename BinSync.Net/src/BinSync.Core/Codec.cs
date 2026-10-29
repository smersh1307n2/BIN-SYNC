// Canonical binary codec (§6): ULEB128 varint (overlong rejected), LE fixed ints, b128.
// Port of binsync/codec.py — all SPEC HARDENING / errata notes preserved.
// Extended surface (TLV, strings, floats, signed, b256): Codec.Extended.cs.
using System.Buffers.Binary;


namespace BinSync;

public class InvalidVarintException : Exception
{
    public InvalidVarintException(string msg) : base(msg) { }
}

public static partial class Codec
{
    public const ulong VarintMaxValue = ulong.MaxValue;

    /// <summary>Write ULEB128 into <paramref name="dest"/>; returns bytes written (1..10).</summary>
    public static int WriteVarint(Span<byte> dest, ulong value)
    {
        int i = 0;
        while (true)
        {
            byte b = (byte)(value & 0x7F);
            value >>= 7;
            if (value != 0) dest[i++] = (byte)(b | 0x80);
            else { dest[i++] = b; break; }
        }
        return i;
    }

    public static byte[] EncodeVarint(ulong value)
    {
        Span<byte> tmp = stackalloc byte[10];
        int n = WriteVarint(tmp, value);
        return tmp[..n].ToArray();
    }

    /// <summary>Number of bytes EncodeVarint would emit for <paramref name="value"/>.</summary>
    public static int VarintLength(ulong value)
    {
        int n = 1;
        while ((value >>= 7) != 0) n++;
        return n;
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
        // Exact overflow-free mirror of the Python reference:
        //   top = result | (payload << shift)          (arbitrary precision)
        //   (a) if not last: bitlen(top) > 7*nbytes    -> width bound
        //   (b) always:      top > 2**64 - 1           -> u64 domain bound
        // `top` exceeds u64 only via payload bits shifted past index 63, i.e.
        // payload >>> (64 - shift) != 0 for shift in [57,63], or any nonzero
        // payload at shift >= 64. Within-domain bits are just result | payload<<shift.
        // BUGFIX (port): `>>>` is an unsigned shift, but C#'s `int >>> 56` promotes to
        // int and keeps the SIGN bit — payload values >= 0x40 became huge ulongs.
        // Cast to ulong BEFORE shifting. AND: C# masks the shift count by & 63, so a
        // shift of exactly 64 (first byte) is a NO-OP instead of yielding 0 — guard it.
        ulong carry = shift == 0 ? 0UL
            : shift >= 64 ? ((ulong)payload << 32 << 32)
            : ((ulong)payload >>> (64 - shift));
        if (carry != 0UL) return true;                       // domain bound (b)
        ulong top = result | ((ulong)payload << shift);
        int width = 7 * nbytes;                              // width bound (a)
        return !last && top >>> width != 0UL;                // width <= 63 here (nbytes<=9, continuation)
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
            // BUGFIX (port): do NOT pre-reject shift>=64 here — Python folds the byte
            // only after candidate_overflow, and an 11th TERMINAL zero-payload byte is
            // already rejected by the overlong rule above; significant payloads at
            // shift>=64 are rejected by CandidateOverflow's domain bound instead.
            if (CandidateOverflow(result, shift, payload, nbytes, last))
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
        return Blake3Core.Hash256(pre);
    }
}

public partial class Writer
{
    private readonly MemoryStream _buf;

    public Writer() : this(256) { }
    public Writer(int capacity) => _buf = new MemoryStream(Math.Max(16, capacity));

    public int Length => (int)_buf.Length;

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

    /// <summary>Zero-alloc varint encode into a stack buffer, then write.</summary>
    public Writer Varint(ulong v)
    {
        Span<byte> tmp = stackalloc byte[10];
        int n = Codec.WriteVarint(tmp, v);
        _buf.Write(tmp[..n]);
        return this;
    }

    public Writer Bytes(byte[] b) { Varint((ulong)b.Length); _buf.Write(b); return this; }
    public Writer Raw(byte[] b) { _buf.Write(b); return this; }
    public Writer Raw(ReadOnlySpan<byte> b) { _buf.Write(b); return this; }
    public byte[] Value() => _buf.ToArray();
}

public partial class Reader
{
    private readonly byte[] _data;
    public int Pos;
    public Reader(byte[] data, int pos = 0) { _data = data; Pos = pos; }

    private void Ensure(int n)
    {
        if (Pos + n > _data.Length) throw new EndOfStreamException("truncated");
    }

    private byte[] Take(int n)
    {
        Ensure(n);
        var v = new byte[n];
        Array.Copy(_data, Pos, v, 0, n);
        Pos += n;
        return v;
    }

    /// <summary>Zero-alloc fixed-width reads (hot path).</summary>
    public byte U8()
    {
        Ensure(1);
        return _data[Pos++];
    }

    public ushort U16()
    {
        Ensure(2);
        ushort v = BinaryPrimitives.ReadUInt16LittleEndian(_data.AsSpan(Pos, 2));
        Pos += 2;
        return v;
    }

    public uint U32()
    {
        Ensure(4);
        uint v = BinaryPrimitives.ReadUInt32LittleEndian(_data.AsSpan(Pos, 4));
        Pos += 4;
        return v;
    }

    public ulong U64()
    {
        Ensure(8);
        ulong v = BinaryPrimitives.ReadUInt64LittleEndian(_data.AsSpan(Pos, 8));
        Pos += 8;
        return v;
    }

    public byte[] B128() => Take(16);
    public ulong Varint() { var (v, p) = Codec.DecodeVarint(_data, Pos); Pos = p; return v; }
    public byte[] Bytes() => Take((int)Varint());
    public byte[] Raw(int n) => Take(n);
    public bool Eof() => Pos >= _data.Length;
    public int Remaining => _data.Length - Pos;
}
