// Canonical binary codec (§6): ULEB128 varint (overlong rejected), LE fixed ints, b128.
// Port of binsync/codec.py — all SPEC HARDENING / errata notes preserved.
// Extended surface (TLV, strings, floats, signed, b256): Codec.Extended.cs.
using System.Buffers.Binary;


namespace BinSync;

/// <summary>Thrown when a varint is overlong, truncated, or otherwise non-canonical.</summary>
public class InvalidVarintException : Exception
{
    /// <summary>InvalidVarintException for codec/protocol errors.</summary>
    public InvalidVarintException(string msg) : base(msg) { }
}

/// <summary>Canonical binary codec: ULEB128 varints, little-endian fixed ints, b128, TLV, strings, floats.</summary>
public static partial class Codec
{
    /// <summary>Maximum value representable by a 10-byte ULEB128 (ulong.MaxValue).</summary>
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

    /// <summary>Encode an unsigned integer as canonical ULEB128.</summary>
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

    /// <summary>Decode a canonical ULEB128 varint; rejects overlong encodings.</summary>
    public static (ulong Value, int Pos) DecodeVarint(byte[] data, int pos = 0)
        => DecodeVarint((ReadOnlySpan<byte>)data, pos);

    /// <summary>Decode a canonical ULEB128 varint; rejects overlong encodings.</summary>
    public static (ulong Value, int Pos) DecodeVarint(ReadOnlySpan<byte> data, int pos = 0)
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
            if (CandidateOverflow(result, shift, payload, nbytes, last))
                throw new InvalidVarintException("overlong varint");
            result |= (ulong)payload << shift;
            shift += 7;
            if (last) break;
        }
        return (result, pos);
    }

    /// Deterministic widening/narrowing of an identity to exactly 16 bytes (§6.2 hardening).
    /// <summary>Normalize a byte array to 16-byte b128 (pad/truncate policy per codec).</summary>
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

/// <summary>Growable (or fixed-capacity) binary writer for canonical BIN-SYNC payloads.</summary>
public partial class Writer
{
    private byte[] _buf;
    private int _len;
    private readonly bool _fixed;
    private readonly bool _pooled;

    /// <summary>Growable (or fixed-capacity) binary writer for canonical BIN-SYNC payloads.</summary>
    public Writer() : this(256) { }
    /// <summary>Growable (or fixed-capacity) binary writer for canonical BIN-SYNC payloads.</summary>
    public Writer(int capacity)
    {
        _buf = new byte[Math.Max(16, capacity)];
        _fixed = false;
        _pooled = false;
    }

    /// <summary>Write into an existing buffer; capacity is fixed (no growth).</summary>
    public Writer(byte[] buffer, int startLength = 0)
    {
        _buf = buffer ?? throw new ArgumentNullException(nameof(buffer));
        _len = startLength;
        _fixed = true;
        _pooled = false;
    }

    /// <summary>ArrayPool-backed writer; call <see cref="ReturnPool"/> when done if not using <see cref="ToArrayReturn"/>.</summary>
    public static Writer Rent(int capacity)
    {
        var buf = System.Buffers.ArrayPool<byte>.Shared.Rent(Math.Max(16, capacity));
        return new Writer(buf, pooled: true);
    }

    private Writer(byte[] buffer, bool pooled)
    {
        _buf = buffer;
        _len = 0;
        _fixed = true;
        _pooled = pooled;
    }

    /// <summary>Number of bytes written so far.</summary>
    public int Length => _len;
    /// <summary>Current underlying buffer capacity.</summary>
    public int Capacity => _buf.Length;

    private void Ensure(int n)
    {
        if (_len + n <= _buf.Length) return;
        if (_fixed)
            throw new InvalidOperationException($"Writer buffer full (need {_len + n}, cap {_buf.Length})");
        int cap = _buf.Length;
        while (cap < _len + n)
            cap = cap < 1_048_576 ? cap * 2 : cap + n + (cap >> 1);
        Array.Resize(ref _buf, cap);
    }

    private void Write(ReadOnlySpan<byte> s)
    {
        Ensure(s.Length);
        s.CopyTo(_buf.AsSpan(_len));
        _len += s.Length;
    }

    /// <summary>Read one unsigned byte.</summary>
    public Writer U8(byte v) { Ensure(1); _buf[_len++] = v; return this; }
    /// <summary>Read a little-endian unsigned 16-bit integer.</summary>
    public Writer U16(ushort v) { Ensure(2); BinaryPrimitives.WriteUInt16LittleEndian(_buf.AsSpan(_len), v); _len += 2; return this; }
    /// <summary>Read a little-endian unsigned 32-bit integer.</summary>
    public Writer U32(uint v) { Ensure(4); BinaryPrimitives.WriteUInt32LittleEndian(_buf.AsSpan(_len), v); _len += 4; return this; }
    /// <summary>Read a little-endian unsigned 64-bit integer.</summary>
    public Writer U64(ulong v) { Ensure(8); BinaryPrimitives.WriteUInt64LittleEndian(_buf.AsSpan(_len), v); _len += 8; return this; }

    /// <summary>Read a 16-byte b128 identity as a new array.</summary>
    public Writer B128(byte[] v) => B128((ReadOnlySpan<byte>)v);

    /// <summary>Read a 16-byte b128 identity as a new array.</summary>
    public Writer B128(ReadOnlyMemory<byte> v) => B128(v.Span);

    /// <summary>Read a 16-byte b128 identity as a new array.</summary>
    public Writer B128(ReadOnlySpan<byte> v)
    {
        if (v.Length != 16)
        {
            var can = Codec.CanonicalB128(v.ToArray());
            Write(can);
        }
        else
            Write(v);
        return this;
    }

    /// <summary>Read a 16-byte b128 identity as a new array.</summary>
    public Writer B128(NodeId id)
    {
        Span<byte> s = stackalloc byte[16];
        id.WriteTo(s);
        Write(s);
        return this;
    }

    /// <summary>Read a 16-byte b128 identity as a new array.</summary>
    public Writer B128(RecordId id)
    {
        Span<byte> s = stackalloc byte[16];
        id.WriteTo(s);
        Write(s);
        return this;
    }

    /// <summary>Read a 16-byte b128 identity as a new array.</summary>
    public Writer B128(BatchId id)
    {
        Span<byte> s = stackalloc byte[16];
        id.WriteTo(s);
        Write(s);
        return this;
    }

    /// <summary>Write a 16-byte digest.</summary>
    public Writer B128(Digest16 d)
    {
        Span<byte> s = stackalloc byte[16];
        d.WriteTo(s);
        Write(s);
        return this;
    }

    /// <summary>Zero-alloc varint encode into a stack buffer, then write.</summary>
    public Writer Varint(ulong v)
    {
        Span<byte> tmp = stackalloc byte[10];
        int n = Codec.WriteVarint(tmp, v);
        Write(tmp[..n]);
        return this;
    }

    /// <summary>Append a length-prefixed byte string (varint length + raw bytes).</summary>
    public Writer Bytes(byte[] b) => Bytes((ReadOnlySpan<byte>)b);
    /// <summary>Append a length-prefixed byte string (varint length + raw bytes).</summary>
    public Writer Bytes(ReadOnlySpan<byte> b) { Varint((ulong)b.Length); Write(b); return this; }
    /// <summary>Append raw bytes without a length prefix.</summary>
    public Writer Raw(byte[] b) { Write(b); return this; }
    /// <summary>Append raw bytes without a length prefix.</summary>
    public Writer Raw(ReadOnlyMemory<byte> b) { Write(b.Span); return this; }
    /// <summary>Append raw bytes without a length prefix.</summary>
    public Writer Raw(ReadOnlySpan<byte> b) { Write(b); return this; }

    /// <summary>Exact-sized writers return the backing buffer with no extra copy.</summary>
    public byte[] Value()
    {
        if (_pooled)
            throw new InvalidOperationException("Use ToArrayReturn() for pooled writers");
        if (_len == _buf.Length) return _buf;
        return _buf.AsSpan(0, _len).ToArray();
    }

    /// <summary>Copy written bytes to a new array and return the pool buffer (if any).</summary>
    public byte[] ToArrayReturn()
    {
        var r = _buf.AsSpan(0, _len).ToArray();
        ReturnPool();
        return r;
    }

    /// <summary>Return a rented buffer to the shared ArrayPool when the writer used pooling.</summary>
    public void ReturnPool()
    {
        if (!_pooled) return;
        System.Buffers.ArrayPool<byte>.Shared.Return(_buf);
        _buf = Array.Empty<byte>();
        _len = 0;
    }

    /// <summary>Written span over the backing buffer (valid until next write / ReturnPool).</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buf.AsSpan(0, _len);
}

/// <summary>Sequential binary reader over a byte array or ReadOnlyMemory slice.</summary>
public partial class Reader
{
    private readonly ReadOnlyMemory<byte> _data;
    /// <summary>Current read offset within the underlying buffer.</summary>
    public int Pos;
    /// <summary>Sequential binary reader over a byte array or ReadOnlyMemory slice.</summary>
    public Reader(byte[] data, int pos = 0) { _data = data ?? Array.Empty<byte>(); Pos = pos; }
    /// <summary>Sequential binary reader over a byte array or ReadOnlyMemory slice.</summary>
    public Reader(ReadOnlyMemory<byte> data, int pos = 0) { _data = data; Pos = pos; }

    private ReadOnlySpan<byte> Span => _data.Span;

    private void Ensure(int n)
    {
        if (Pos + n > _data.Length) throw new EndOfStreamException("truncated");
    }

    private byte[] Take(int n)
    {
        Ensure(n);
        var v = new byte[n];
        Span.Slice(Pos, n).CopyTo(v);
        Pos += n;
        return v;
    }

    /// <summary>Advance and return a view into the underlying buffer (no alloc). Lifetime = buffer.</summary>
    public ReadOnlySpan<byte> TakeSpan(int n)
    {
        Ensure(n);
        var s = Span.Slice(Pos, n);
        Pos += n;
        return s;
    }

    /// <summary>Advance and return a Memory slice into the underlying buffer (no alloc).</summary>
    public ReadOnlyMemory<byte> TakeMemory(int n)
    {
        Ensure(n);
        var m = _data.Slice(Pos, n);
        Pos += n;
        return m;
    }

    /// <summary>Zero-alloc fixed-width reads (hot path).</summary>
    public byte U8()
    {
        Ensure(1);
        return Span[Pos++];
    }

    /// <summary>Read a little-endian unsigned 16-bit integer.</summary>
    public ushort U16()
    {
        Ensure(2);
        ushort v = BinaryPrimitives.ReadUInt16LittleEndian(Span.Slice(Pos, 2));
        Pos += 2;
        return v;
    }

    /// <summary>Read a little-endian unsigned 32-bit integer.</summary>
    public uint U32()
    {
        Ensure(4);
        uint v = BinaryPrimitives.ReadUInt32LittleEndian(Span.Slice(Pos, 4));
        Pos += 4;
        return v;
    }

    /// <summary>Read a little-endian unsigned 64-bit integer.</summary>
    public ulong U64()
    {
        Ensure(8);
        ulong v = BinaryPrimitives.ReadUInt64LittleEndian(Span.Slice(Pos, 8));
        Pos += 8;
        return v;
    }

    /// <summary>Read a 16-byte b128 identity as a new array.</summary>
    public byte[] B128() => Take(16);

    /// <summary>Read a 16-byte node id without allocating.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public NodeId ReadNodeId()
    {
        Ensure(16);
        var v = new NodeId(
            BinaryPrimitives.ReadUInt64LittleEndian(Span.Slice(Pos, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(Span.Slice(Pos + 8, 8)));
        Pos += 16;
        return v;
    }

    /// <summary>Read a 16-byte record id without allocating.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public RecordId ReadRecordId()
    {
        Ensure(16);
        var v = new RecordId(
            BinaryPrimitives.ReadUInt64LittleEndian(Span.Slice(Pos, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(Span.Slice(Pos + 8, 8)));
        Pos += 16;
        return v;
    }

    /// <summary>Read a 16-byte batch id without allocating.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public BatchId ReadBatchId()
    {
        Ensure(16);
        var v = new BatchId(
            BinaryPrimitives.ReadUInt64LittleEndian(Span.Slice(Pos, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(Span.Slice(Pos + 8, 8)));
        Pos += 16;
        return v;
    }

    /// <summary>Read a 16-byte digest without allocating.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public Digest16 ReadDigest16()
    {
        Ensure(16);
        var v = Digest16.From(Span.Slice(Pos, 16));
        Pos += 16;
        return v;
    }
    /// <summary>Read a 16-byte b128 identity as a span into the underlying buffer.</summary>
    public ReadOnlySpan<byte> B128Span() => TakeSpan(16);
    /// <summary>Read a 16-byte b128 identity as ReadOnlyMemory (may slice the frame).</summary>
    public ReadOnlyMemory<byte> B128Memory() => TakeMemory(16);
    /// <summary>Read a canonical ULEB128 and advance the position.</summary>
    public ulong Varint() { var (v, p) = Codec.DecodeVarint(Span, Pos); Pos = p; return v; }
    /// <summary>Append a length-prefixed byte string (varint length + raw bytes).</summary>
    public byte[] Bytes() => Take((int)Varint());
    /// <summary>Read a length-prefixed blob as a span into the underlying buffer.</summary>
    public ReadOnlySpan<byte> BytesSpan()
    {
        int n = (int)Varint();
        return TakeSpan(n);
    }
    /// <summary>Read a length-prefixed blob as ReadOnlyMemory (may slice the frame).</summary>
    public ReadOnlyMemory<byte> BytesMemory()
    {
        int n = (int)Varint();
        return TakeMemory(n);
    }
    /// <summary>Append raw bytes without a length prefix.</summary>
    public byte[] Raw(int n) => Take(n);
    /// <summary>Return a span of n raw bytes and advance the position.</summary>
    public ReadOnlySpan<byte> RawSpan(int n) => TakeSpan(n);
    /// <summary>Return ReadOnlyMemory of n raw bytes and advance the position.</summary>
    public ReadOnlyMemory<byte> RawMemory(int n) => TakeMemory(n);
    /// <summary>True when the reader has consumed all bytes.</summary>
    public bool Eof() => Pos >= _data.Length;
    /// <summary>Number of unread bytes left in the buffer.</summary>
    public int Remaining => _data.Length - Pos;
}
