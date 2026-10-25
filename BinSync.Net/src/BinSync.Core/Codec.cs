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

    /// <summary>
    /// Encodes <paramref name="value"/> as canonical ULEB128 directly into
    /// <paramref name="destination"/>. Returns the number of bytes written, or 0 when
    /// the destination is too small (max width for u64 is 10 bytes). Zero-allocation.
    /// </summary>
    public static bool TryWriteVarint(Span<byte> destination, ulong value, out int bytesWritten)
    {
        int n = 0;
        while (true)
        {
            if (n >= destination.Length) { bytesWritten = 0; return false; }
            byte b = (byte)(value & 0x7F);
            value >>= 7;
            if (value != 0) destination[n++] = (byte)(b | 0x80);
            else { destination[n++] = b; break; }
        }
        bytesWritten = n;
        return true;
    }

    public static byte[] EncodeVarint(ulong value)
    {
        // One exact allocation — stack scratch, then a precise copy to the heap.
        Span<byte> tmp = stackalloc byte[10]; // max width of a u64 ULEB128
        TryWriteVarint(tmp, value, out int n);
        byte[] r = new byte[n];
        tmp[..n].CopyTo(r);
        return r;
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

    /// <summary>
    /// Canonical ULEB128 decoder over a span (allocation-free hot path). Throws
    /// <see cref="InvalidVarintException"/> on truncated/overlong encodings.
    /// </summary>
    public static ulong DecodeVarint(ReadOnlySpan<byte> data, ref int pos)
    {
        if (!TryDecodeVarint(data, ref pos, out ulong value, out VarintFailure failure))
            throw new InvalidVarintException(failure == VarintFailure.Truncated
                ? "truncated varint" : "overlong varint");
        return value;
    }

    /// <summary>Why a canonical ULEB128 decode failed.</summary>
    public enum VarintFailure
    {
        None = 0,
        /// <summary>Ran out of bytes before the terminator.</summary>
        Truncated,
        /// <summary>Non-canonical (overlong / out-of-domain) encoding — errata E-5.</summary>
        Overlong,
    }

    /// <summary>
    /// Try-pattern decoder: never throws for malformed input. On failure the original
    /// <paramref name="pos"/> is restored so callers can report precise offsets.
    /// </summary>
    public static bool TryDecodeVarint(ReadOnlySpan<byte> data, ref int pos, out ulong value)
        => TryDecodeVarint(data, ref pos, out value, out _);

    /// <summary>Try-pattern decoder with explicit failure classification.</summary>
    public static bool TryDecodeVarint(ReadOnlySpan<byte> data, ref int pos, out ulong value,
                                       out VarintFailure failure)
    {
        value = 0;
        failure = VarintFailure.None;
        ulong result = 0;
        int shift = 0;
        int nbytes = 0;
        int start = pos;
        while (true)
        {
            if (pos >= data.Length)
            {
                failure = VarintFailure.Truncated;
                pos = start;
                return false;
            }
            byte b = data[pos];
            pos += 1;
            nbytes += 1;
            int payload = b & 0x7F;
            bool last = (b & 0x80) == 0;
            if (payload == 0 && nbytes > 1 && last)
            {
                failure = VarintFailure.Overlong;
                pos = start;
                return false;
            }
            // BUGFIX (port): do NOT pre-reject shift>=64 here — Python folds the byte
            // only after candidate_overflow, and an 11th TERMINAL zero-payload byte is
            // already rejected by the overlong rule above; significant payloads at
            // shift>=64 are rejected by CandidateOverflow's domain bound instead.
            if (CandidateOverflow(result, shift, payload, nbytes, last))
            {
                failure = VarintFailure.Overlong;
                pos = start;
                return false;
            }
            result |= (ulong)payload << shift;
            shift += 7;
            if (last) break;
        }
        value = result;
        return true;
    }

    /// <summary>Array-based convenience overload kept for API compatibility.</summary>
    public static (ulong Value, int Pos) DecodeVarint(byte[] data, int pos = 0)
    {
        int p = pos;
        ulong v = DecodeVarint(data.AsSpan(), ref p);
        return (v, p);
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

/// <summary>
/// Canonical wire writer. Fixed-width scalars and varints are encoded straight into a
/// stack scratch space before being appended, so the hot path performs no intermediate
/// heap allocations per field; the payload itself accumulates in a <see cref="MemoryStream"/>
/// whose GetBuffer exposes the raw backing span for zero-copy appends.
/// Not thread-safe by design — each encoding gets its own instance (they are cheap).
/// </summary>
public partial class Writer
{
    private readonly MemoryStream _buf = new();

    /// <summary>Number of bytes written so far.</summary>
    public int Length => (int)_buf.Length;

    /// <summary>Appends <paramref name="count"/> bytes from the stack scratch space.</summary>
    private void WriteScratch(Span<byte> s, int count)
    {
        // MemoryStream.GetBuffer is the canonical zero-copy append path here:
        // capacity growth happens inside the stream, we only move the tail once.
        byte[] backing = _buf.GetBuffer();
        int pos = (int)_buf.Length;
        if (pos + count > backing.Length)
        {
            _buf.SetLength(pos + count);   // grow (may reallocate the backing array)
            backing = _buf.GetBuffer();
        }
        s.Slice(0, count).CopyTo(backing.AsSpan(pos, count));
        _buf.SetLength(pos + count);
    }

    public Writer U8(byte v) { _buf.WriteByte(v); return this; }

    public Writer U16(ushort v)
    {
        Span<byte> s = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(s, v);
        WriteScratch(s, 2);
        return this;
    }

    public Writer U32(uint v)
    {
        Span<byte> s = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(s, v);
        WriteScratch(s, 4);
        return this;
    }

    public Writer U64(ulong v)
    {
        Span<byte> s = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(s, v);
        WriteScratch(s, 8);
        return this;
    }

    public Writer B128(byte[] v)
    {
        if (v.Length != 16) v = Codec.CanonicalB128(v);
        _buf.Write(v);
        return this;
    }

    public Writer Varint(ulong v)
    {
        // Encode on the stack, then append exactly the emitted width — no temp array.
        Span<byte> s = stackalloc byte[10]; // max width of a u64 ULEB128
        Codec.TryWriteVarint(s, v, out int n);
        WriteScratch(s, n);
        return this;
    }

    public Writer Bytes(byte[] b) { Varint((ulong)b.Length); _buf.Write(b); return this; }
    public Writer Raw(byte[] b) { _buf.Write(b); return this; }

    /// <summary>Exact-size snapshot of the encoded payload (heap copy).</summary>
    public byte[] Value() => _buf.ToArray();
}

/// <summary>
/// Canonical wire reader over a contiguous byte region. Fixed-width scalars and varints
/// decode from spans without any copying; only genuinely new values (<see cref="B128"/>,
/// <see cref="Bytes"/>) allocate. Bounds checks are strict: every read requires the full
/// field to be present, otherwise <see cref="EndOfStreamException"/> ("truncated").
/// </summary>
public partial class Reader
{
    private readonly byte[] _data;
    public int Pos;
    public Reader(byte[] data, int pos = 0) { _data = data; Pos = pos; }

    private Span<byte> TakeSpan(int n)
    {
        if (Pos + n > _data.Length) throw new EndOfStreamException("truncated");
        var s = _data.AsSpan(Pos, n);
        Pos += n;
        return s;
    }

    private byte[] Take(int n) => TakeSpan(n).ToArray();

    public byte U8() => TakeSpan(1)[0];

    public ushort U16() => BinaryPrimitives.ReadUInt16LittleEndian(TakeSpan(2));
    public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(TakeSpan(4));
    public ulong U64() => BinaryPrimitives.ReadUInt64LittleEndian(TakeSpan(8));
    public byte[] B128() => Take(16);
    public ulong Varint()
    {
        // LEGACY COMPAT (Python port): the reference reader mutates its position while
        // scanning a varint and never rewinds it — even when the decode fails. Callers
        // depend on that side effect to recover positions after malformed or truncated
        // runs (e.g. Ops.DecodeOp's truncated-outbox-header path). Both failure modes
        // throw InvalidVarintException with "truncated"/"overlong" messages, matching
        // Codec.DecodeVarint exactly. Do not "clean this up" without auditing those paths.
        int p = Pos;
        ulong result = 0;
        int shift = 0;
        int nbytes = 0;
        while (true)
        {
            if (p >= _data.Length) { Pos = p; throw new InvalidVarintException("truncated varint"); }
            byte b = _data[p];
            p += 1;
            nbytes += 1;
            int payload = b & 0x7F;
            bool last = (b & 0x80) == 0;
            if (payload == 0 && nbytes > 1 && last) { Pos = p; throw new InvalidVarintException("overlong varint"); }
            if (Codec.CandidateOverflowPublic(result, shift, payload, nbytes, last)) { Pos = p; throw new InvalidVarintException("overlong varint"); }
            result |= (ulong)payload << shift;
            shift += 7;
            if (last) break;
        }
        Pos = p;
        return result;
    }
    public byte[] Bytes() => Take((int)Varint());
    public byte[] Raw(int n) => Take(n);
    public bool Eof() => Pos >= _data.Length;
    public int Remaining => _data.Length - Pos;
}
