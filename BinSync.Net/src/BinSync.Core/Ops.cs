// Wire operations (§12.24): FieldWrite / FieldTomb / RecordTombstone byte layouts.
// Invariant [C11]: origin at FIXED offset 17 (bytes 17-18) for FieldWrite/FieldTomb;
// offsets 13-16 are hlc.logical (u32 LE).
using System.Buffers.Binary;

namespace BinSync;

/// <summary>Wire tag constants for operation kinds.</summary>
public static class OpsConst
{
    /// <summary>Wire tag for a field-write operation.</summary>
    public const byte TagFieldWrite = 0x01;
    /// <summary>Wire tag for a field-tombstone operation.</summary>
    public const byte TagFieldTomb = 0x02;
    /// <summary>Wire tag for a whole-record tombstone operation.</summary>
    public const byte TagRecordTomb = 0x03;
    /// <summary>Constant origin offset.</summary>
    public const int OriginOffset = 17;              // [C11] FieldWrite/FieldTomb
    /// <summary>Constant origin offset record tomb.</summary>
    public const int OriginOffsetRecordTomb = 29;    // RecordTombstone
}

/// <summary>Decoded field-write operation.</summary>
public class OpFieldWrite
{
    /// <summary>Key epoch associated with the field write.</summary>
    public ushort FieldId, KeyEpoch;
    /// <summary>HLC wall-clock component in microseconds.</summary>
    public ulong Wall; public uint Logical;
    /// <summary>Compact origin alias (or full-author mode flag path).</summary>
    public ushort Origin;
    /// <summary>Durable author node id when full-author mode is used.</summary>
    public NodeId? AuthorNodeId;
    /// <summary>Return a copy of the written buffer as a new byte array.</summary>
    public byte[] Value = Array.Empty<byte>();
    /// <summary>Wire op-kind tag for this operation type.</summary>
    public byte Tag => OpsConst.TagFieldWrite;
    /// <summary>True when this op was decoded from / is encoded in FULL_AUTHOR_ID mode (§12.24.4).</summary>
    public bool FullAuthorMode { get; internal set; }
    /// <summary>When decoded from a truncated payload: position of the value-length varint
    /// inside the source buffer (persistence byte-stability). -1 = not applicable.</summary>
    internal int ValueLenPos = -1;

    // Production hardening: payload-length variant of the auto-detect heuristic in
    // Ops.DecodeOp (single source of truth for both encode & decode).
    internal int CompactPayloadLen => 19 + Codec.VarintLength((ulong)Value.Length) + Value.Length;
    internal int FullAuthorPayloadLen => 33 + Codec.VarintLength((ulong)Value.Length) + Value.Length;
}

/// <summary>Decoded field-tombstone operation.</summary>
public class OpFieldTomb
{
    /// <summary>Key epoch associated with the field write.</summary>
    public ushort FieldId, KeyEpoch;
    /// <summary>HLC wall-clock component in microseconds.</summary>
    public ulong Wall; public uint Logical;
    /// <summary>Compact origin alias (or full-author mode flag path).</summary>
    public ushort Origin;
    /// <summary>Durable author node id when full-author mode is used.</summary>
    public NodeId? AuthorNodeId;
    /// <summary>Wire op-kind tag for this operation type.</summary>
    public byte Tag => OpsConst.TagFieldTomb;
    /// <summary>True when this op was decoded from / is encoded in FULL_AUTHOR_ID mode (§12.24.4).</summary>
    public bool FullAuthorMode { get; internal set; }

    internal const int CompactPayloadLen = 19;
    internal const int FullAuthorPayloadLen = 33;
}

/// <summary>Decoded whole-record tombstone operation.</summary>
public class OpRecordTombstone
{
    /// <summary>128-bit record identity with value equality.</summary>
    public RecordId RecordId;
    /// <summary>HLC wall-clock component in microseconds.</summary>
    public ulong Wall; public uint Logical;
    /// <summary>Compact origin alias (or full-author mode flag path).</summary>
    public ushort Origin;
    /// <summary>Durable author node id when full-author mode is used.</summary>
    public NodeId? AuthorNodeId;
    /// <summary>Wire op-kind tag for this operation type.</summary>
    public byte Tag => OpsConst.TagRecordTomb;
    /// <summary>True when this op was decoded from / is encoded in FULL_AUTHOR_ID mode (§12.24.4).</summary>
    public bool FullAuthorMode { get; internal set; }

    internal const int CompactPayloadLen = 31;
    internal const int FullAuthorPayloadLen = 45;
}

/// <summary>Encode and decode FieldWrite / FieldTomb / RecordTombstone operations.</summary>
public static partial class Ops
{
    /// <summary>Encode an operation forcing full-author mode on the wire.</summary>
    public static byte[] EncodeOpFull(object op) => EncodeOp(op, fullAuthor: true);

    /// <summary>Upper bound on <see cref="EncodeOp(object, bool?, int?)"/> size.</summary>
    public static int EstimateEncodeOpSize(object op, bool? fullAuthor = null, int? valueLenOverride = null)
    {
        if (op is OpFieldWrite fw)
        {
            bool full = fullAuthor ?? fw.FullAuthorMode;
            int header = OpsConst.OriginOffset + (full ? 16 : 2);
            if (valueLenOverride is null && fw.ValueLenPos >= 0)
                return header + fw.ValueLenPos + 1 + fw.Value.Length;
            int vlen = valueLenOverride ?? fw.Value.Length;
            return header + Codec.VarintLength((ulong)vlen) + fw.Value.Length;
        }
        if (op is OpFieldTomb ft)
        {
            bool full = fullAuthor ?? ft.FullAuthorMode;
            return OpsConst.OriginOffset + (full ? 16 : 2);
        }
        if (op is OpRecordTombstone rt)
        {
            bool full = fullAuthor ?? rt.FullAuthorMode;
            return OpsConst.OriginOffsetRecordTomb + (full ? 16 : 2);
        }
        throw new ArgumentException($"unknown op {op}");
    }

    /// Canonical encoding. Compact mode: u16 origin; FULL_AUTHOR_ID mode: durable b128 (§12.24.4).
    /// When <paramref name="fullAuthor"/> is null the mode is taken from the op itself
    /// (<c>FullAuthorMode</c>, i.e. how it was decoded) — this keeps outbox payloads
    /// byte-stable across persistence round-trips (production hardening).
    /// <summary>Encode an operation object to canonical wire bytes.</summary>
    public static byte[] EncodeOp(object op, bool? fullAuthor = null, int? valueLenOverride = null)
    {
        int est = Math.Max(EstimateEncodeOpSize(op, fullAuthor, valueLenOverride), 16);
        var dest = new byte[est];
        var w = new Writer(dest);
        WriteOp(w, op, fullAuthor, valueLenOverride);
        if (w.Length == dest.Length) return dest;
        return dest.AsSpan(0, w.Length).ToArray();
    }

    /// <summary>Encode into <paramref name="destination"/>; returns bytes written.</summary>
    public static int EncodeOp(object op, Span<byte> destination, bool? fullAuthor = null, int? valueLenOverride = null)
    {
        int est = EstimateEncodeOpSize(op, fullAuthor, valueLenOverride);
        if (destination.Length < est)
            throw new ArgumentException($"destination too small (need ~{est})", nameof(destination));
        var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(est);
        try
        {
            var w = new Writer(rented);
            WriteOp(w, op, fullAuthor, valueLenOverride);
            w.WrittenSpan.CopyTo(destination);
            return w.Length;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void WriteOp(Writer w, object op, bool? fullAuthor, int? valueLenOverride)
    {
        if (op is OpFieldWrite fw)
        {
            bool full = fullAuthor ?? fw.FullAuthorMode;
            w.U8(OpsConst.TagFieldWrite).U16(fw.FieldId).U16(fw.KeyEpoch).U64(fw.Wall).U32(fw.Logical);
            if (w.Length != OpsConst.OriginOffset)
                throw new InvalidOperationException("[C11] prefix must be exactly 17 bytes");
            if (full) w.B128(fw.AuthorNodeId ?? throw new InvalidOperationException("Full-author field write requires an author NodeId."));
            else w.U16(fw.Origin);
            int vlen = fw.Value.Length;
            if (valueLenOverride is null && fw.ValueLenPos >= 0)
            {
                // Payload stores ONLY the varint width (value bytes live elsewhere, e.g. in the
                // record's FieldState after a persistence round-trip): re-emit the same-width
                // zero-length varint so the payload stays byte-stable across save/load.
                Span<byte> pad = stackalloc byte[16];
                int padLen = fw.ValueLenPos + 1;
                if (padLen > pad.Length)
                {
                    w.Raw(new byte[padLen]);
                }
                else
                {
                    pad[..padLen].Clear();
                    w.Raw(pad[..padLen]);
                }
            }
            else
            {
                if (valueLenOverride is not null) vlen = valueLenOverride.Value;
                w.Varint((ulong)vlen);
            }
            w.Raw(fw.Value);
            return;
        }
        if (op is OpFieldTomb ft)
        {
            bool full = fullAuthor ?? ft.FullAuthorMode;
            w.U8(OpsConst.TagFieldTomb).U16(ft.FieldId).U16(ft.KeyEpoch).U64(ft.Wall).U32(ft.Logical);
            if (w.Length != OpsConst.OriginOffset)
                throw new InvalidOperationException("[C11] prefix must be exactly 17 bytes");
            if (full) w.B128(ft.AuthorNodeId ?? throw new InvalidOperationException("Full-author field tomb requires an author NodeId."));
            else w.U16(ft.Origin);
            return;
        }
        if (op is OpRecordTombstone rt)
        {
            bool full = fullAuthor ?? rt.FullAuthorMode;
            w.U8(OpsConst.TagRecordTomb).B128(rt.RecordId).U64(rt.Wall).U32(rt.Logical);
            if (w.Length != OpsConst.OriginOffsetRecordTomb)
                throw new InvalidOperationException("origin at offset 29");
            if (full) w.B128(rt.AuthorNodeId ?? throw new InvalidOperationException("Full-author record tombstone requires an author NodeId."));
            else w.U16(rt.Origin);
            return;
        }
        throw new ArgumentException($"unknown op {op}");
    }

    /// <summary>
    /// Clone compact payload bytes and patch the fixed-offset <c>origin</c> ([C11]).
    /// Returns an owned array, including when the origin already matches. Returns null
    /// when the payload is full-author, truncated, or ambiguous.
    /// </summary>
    public static byte[]? TryCloneWithOrigin(ReadOnlyMemory<byte> payload, ushort origin)
    {
        var result = TryCloneWithOriginShared(payload, origin);
        return result?.ToArray();
    }

    /// <summary>Clone compact payload bytes with a new origin; never aliases the caller's array.</summary>
    public static byte[]? TryCloneWithOrigin(byte[] payload, ushort origin)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var result = TryCloneWithOriginShared(payload, origin);
        return result?.ToArray();
    }

    // Internal zero-copy fast path for engine-owned immutable Outbox buffers.
    internal static byte[]? TryCloneWithOriginShared(ReadOnlyMemory<byte> payload, ushort origin)
    {
        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(payload, out ArraySegment<byte> seg)
            && seg.Array is not null && seg.Offset == 0 && seg.Count == seg.Array.Length)
            return TryCloneWithOriginShared(seg.Array, origin);
        if (payload.Length < 19) return null;
        return TryCloneWithOriginShared(payload.ToArray(), origin);
    }

    internal static byte[]? TryCloneWithOriginShared(byte[] payload, ushort origin)
    {
        if (payload.Length < 19) return null;
        int off;
        byte tag = payload[0];
        if (tag == OpsConst.TagFieldWrite)
        {
            if (!FieldWriteConsumesExact(payload, OpsConst.OriginOffset, full: false)) return null;
            if (FieldWriteConsumesExact(payload, OpsConst.OriginOffset, full: true)) return null;
            off = OpsConst.OriginOffset;
        }
        else if (tag == OpsConst.TagFieldTomb)
        {
            if (payload.Length != OpFieldTomb.CompactPayloadLen) return null;
            off = OpsConst.OriginOffset;
        }
        else if (tag == OpsConst.TagRecordTomb)
        {
            if (payload.Length != OpRecordTombstone.CompactPayloadLen) return null;
            off = OpsConst.OriginOffsetRecordTomb;
        }
        else return null;

        if (BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(off)) == origin)
            return payload; // already stamped — share the outbox buffer

        var copy = new byte[payload.Length];
        payload.AsSpan().CopyTo(copy);
        BinaryPrimitives.WriteUInt16LittleEndian(copy.AsSpan(off), origin);
        return copy;
    }

    /// <summary>Decode an operation from wire bytes.</summary>
    public static (object Op, int Pos) DecodeOp(byte[] data, int pos = 0, bool? fullAuthor = null)
        => DecodeOp((ReadOnlyMemory<byte>)data, pos, fullAuthor);

    /// <summary>Decode an operation from wire bytes.</summary>
    public static (object Op, int Pos) DecodeOp(ReadOnlyMemory<byte> data, int pos = 0, bool? fullAuthor = null)
    {
        var r = new Reader(data, pos);
        byte tag = r.U8();
        if (tag == OpsConst.TagFieldWrite)
        {
            ushort fid = r.U16(), ke = r.U16();
            ulong wall = r.U64(); uint logical = r.U32();
            bool full = fullAuthor ?? DetectFieldWriteFullAuthor(data, r.Pos);
            NodeId? author; ushort origin = 0;
            if (full) { author = r.ReadNodeId(); }
            else { origin = r.U16(); author = null; }
            if (r.Pos != OpsConst.OriginOffset + (full ? 16 : 2))
                throw new InvalidOperationException("[C11] author slot at fixed offset");
            int vlenPos = r.Pos;
            byte[] value;
            int endPos;
            try { value = r.Bytes(); endPos = r.Pos; }
            catch (EndOfStreamException)
            {
                // Truncated payload (outbox entries store only the fixed header): return the
                // op with an empty tail and remember where the value-length varint lives so
                // EncodeOp can reproduce the original byte width (persistence byte-stability).
                value = Array.Empty<byte>();
                endPos = vlenPos;
            }
            var opw = new OpFieldWrite { FieldId = fid, KeyEpoch = ke, Wall = wall, Logical = logical, Origin = origin, AuthorNodeId = author, Value = value, FullAuthorMode = full };
            if (endPos != r.Pos) { opw.ValueLenPos = vlenPos; }
            return (opw, endPos);
        }
        if (tag == OpsConst.TagFieldTomb)
        {
            ushort fid = r.U16(), ke = r.U16();
            ulong wall = r.U64(); uint logical = r.U32();
            bool full = fullAuthor ?? (r.Remaining == 16);
            NodeId? author; ushort origin = 0;
            if (full) { author = r.ReadNodeId(); }
            else { origin = r.U16(); author = null; }
            if (r.Pos != OpsConst.OriginOffset + (full ? 16 : 2))
                throw new InvalidOperationException("malformed op payload");
            return (new OpFieldTomb { FieldId = fid, KeyEpoch = ke, Wall = wall, Logical = logical, Origin = origin, AuthorNodeId = author, FullAuthorMode = full }, r.Pos);
        }
        if (tag == OpsConst.TagRecordTomb)
        {
            var rid = r.ReadRecordId();
            ulong wall = r.U64(); uint logical = r.U32();
            bool full = fullAuthor ?? (r.Remaining == 16);
            NodeId? author; ushort origin = 0;
            if (full) { author = r.ReadNodeId(); }
            else { origin = r.U16(); author = null; }
            if (r.Pos != OpsConst.OriginOffsetRecordTomb + (full ? 16 : 2))
                throw new InvalidOperationException("malformed op payload");
            return (new OpRecordTombstone { RecordId = rid, Wall = wall, Logical = logical, Origin = origin, AuthorNodeId = author, FullAuthorMode = full }, r.Pos);
        }
        throw new ArgumentException($"unknown op tag 0x{tag:x}");
    }

    /// Compact FieldWrite is `u16 origin + varint len + value`. Full-author is
    /// `b128 author + varint len + value` (§12.24.4). A remaining-length threshold
    /// (`>= 17`) treats any compact op whose value is longer than ~14 bytes as
    /// full-author and steals the value bytes as a node id — the receiver then
    /// keys the seq cursor on a garbage author and raises SEQ_GAP. Pick the mode
    /// whose framing consumes the buffer exactly; if both do (rare), prefer full
    /// so snapshot encodings win. Truncated outbox headers fall back to the
    /// length heuristic so persistence can still recover ValueLenPos.
    private static bool DetectFieldWriteFullAuthor(ReadOnlyMemory<byte> data, int pos)
    {
        bool compact = FieldWriteConsumesExact(data, pos, full: false);
        bool full = FieldWriteConsumesExact(data, pos, full: true);
        if (compact && !full) return false;
        if (full && !compact) return true;
        if (full && compact) return true;
        return data.Length - pos >= 17;
    }

    private static bool FieldWriteConsumesExact(byte[] data, int pos, bool full)
        => FieldWriteConsumesExact((ReadOnlyMemory<byte>)data, pos, full);

    private static bool FieldWriteConsumesExact(ReadOnlyMemory<byte> data, int pos, bool full)
    {
        try
        {
            var r = new Reader(data, pos);
            if (full)
            {
                if (r.Remaining < 16) return false;
                r.RawSpan(16); // Skip the fixed-width author id without allocating a byte array.
            }
            else
            {
                if (r.Remaining < 2) return false;
                r.U16();
            }
            int len = checked((int)r.Varint());
            if (len < 0 || r.Remaining < len) return false;
            if (len > 0) r.Raw(len);
            return r.Remaining == 0;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// [R4] Relay alias translation: rewrite the session alias (origin) of an operation.
    /// Mirrors binsync.ops.rewrite_origin from the Python reference; exposed publicly
    /// so relays and tests can remap origin aliases without touching Node internals.
    /// Mutates and returns the same op instance (same semantics as the Python version).
    /// </summary>
    public static object RewriteOrigin(object op, ushort newOrigin)
    {
        switch (op)
        {
            case OpFieldWrite x: x.Origin = newOrigin; return x;
            case OpFieldTomb x: x.Origin = newOrigin; return x;
            case OpRecordTombstone x: x.Origin = newOrigin; return x;
            default: throw new ArgumentException($"unknown op type {op?.GetType().Name}");
        }
    }
}
