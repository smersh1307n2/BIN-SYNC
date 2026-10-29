// Wire operations (§12.24): FieldWrite / FieldTomb / RecordTombstone byte layouts.
// Invariant [C11]: origin at FIXED offset 17 (bytes 17-18) for FieldWrite/FieldTomb;
// offsets 13-16 are hlc.logical (u32 LE).
namespace BinSync;

public static class OpsConst
{
    public const byte TagFieldWrite = 0x01;
    public const byte TagFieldTomb = 0x02;
    public const byte TagRecordTomb = 0x03;
    public const int OriginOffset = 17;              // [C11] FieldWrite/FieldTomb
    public const int OriginOffsetRecordTomb = 29;    // RecordTombstone
}

public class OpFieldWrite
{
    public ushort FieldId, KeyEpoch;
    public ulong Wall; public uint Logical;
    public ushort Origin;
    public byte[]? AuthorNodeId;
    public byte[] Value = Array.Empty<byte>();
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

public class OpFieldTomb
{
    public ushort FieldId, KeyEpoch;
    public ulong Wall; public uint Logical;
    public ushort Origin;
    public byte[]? AuthorNodeId;
    public byte Tag => OpsConst.TagFieldTomb;
    /// <summary>True when this op was decoded from / is encoded in FULL_AUTHOR_ID mode (§12.24.4).</summary>
    public bool FullAuthorMode { get; internal set; }

    internal const int CompactPayloadLen = 19;
    internal const int FullAuthorPayloadLen = 33;
}

public class OpRecordTombstone
{
    public byte[] RecordId = Array.Empty<byte>();
    public ulong Wall; public uint Logical;
    public ushort Origin;
    public byte[]? AuthorNodeId;
    public byte Tag => OpsConst.TagRecordTomb;
    /// <summary>True when this op was decoded from / is encoded in FULL_AUTHOR_ID mode (§12.24.4).</summary>
    public bool FullAuthorMode { get; internal set; }

    internal const int CompactPayloadLen = 31;
    internal const int FullAuthorPayloadLen = 45;
}

public static class Ops
{
    /// Snapshot-mode encoding: always carries the durable author (mirrors Python
    /// build_snapshot which calls encode_op(op) with default full_author=True).
    public static byte[] EncodeOpFull(object op) => EncodeOp(op, fullAuthor: true);

    /// Canonical encoding. Compact mode: u16 origin; FULL_AUTHOR_ID mode: durable b128 (§12.24.4).
    /// When <paramref name="fullAuthor"/> is null the mode is taken from the op itself
    /// (<c>FullAuthorMode</c>, i.e. how it was decoded) — this keeps outbox payloads
    /// byte-stable across persistence round-trips (production hardening).
    public static byte[] EncodeOp(object op, bool? fullAuthor = null, int? valueLenOverride = null)
    {
        var w = new Writer();
        if (op is OpFieldWrite fw)
        {
            bool full = fullAuthor ?? fw.FullAuthorMode;
            w.U8(OpsConst.TagFieldWrite).U16(fw.FieldId).U16(fw.KeyEpoch).U64(fw.Wall).U32(fw.Logical);
            if (w.Value().Length != OpsConst.OriginOffset)
                throw new InvalidOperationException("[C11] prefix must be exactly 17 bytes");
            if (full) w.B128(fw.AuthorNodeId!);
            else w.U16(fw.Origin);
            int vlen = fw.Value.Length;
            if (valueLenOverride is null && fw.ValueLenPos >= 0)
            {
                // Payload stores ONLY the varint width (value bytes live elsewhere, e.g. in the
                // record's FieldState after a persistence round-trip): re-emit the same-width
                // zero-length varint so the payload stays byte-stable across save/load.
                w.Raw(new byte[fw.ValueLenPos + 1]);
            }
            else
            {
                if (valueLenOverride is not null) vlen = valueLenOverride.Value;
                w.Varint((ulong)vlen);
            }
            w.Raw(fw.Value);
            return w.Value();
        }
        if (op is OpFieldTomb ft)
        {
            bool full = fullAuthor ?? ft.FullAuthorMode;
            w.U8(OpsConst.TagFieldTomb).U16(ft.FieldId).U16(ft.KeyEpoch).U64(ft.Wall).U32(ft.Logical);
            if (w.Value().Length != OpsConst.OriginOffset)
                throw new InvalidOperationException("[C11] prefix must be exactly 17 bytes");
            if (full) w.B128(ft.AuthorNodeId!);
            else w.U16(ft.Origin);
            return w.Value();
        }
        if (op is OpRecordTombstone rt)
        {
            bool full = fullAuthor ?? rt.FullAuthorMode;
            w.U8(OpsConst.TagRecordTomb).B128(rt.RecordId).U64(rt.Wall).U32(rt.Logical);
            if (w.Value().Length != OpsConst.OriginOffsetRecordTomb)
                throw new InvalidOperationException("origin at offset 29");
            if (full) w.B128(rt.AuthorNodeId!);
            else w.U16(rt.Origin);
            return w.Value();
        }
        throw new ArgumentException($"unknown op {op}");
    }

    /// fullAuthor=null → AUTO-DETECT the mode from payload length (BUGFIX note in Python port).
    /// The second tuple element is the reader position after the op; for FieldWrite decoded
    /// from a TRUNCATED payload it points at the end of the fixed header, and
    /// <see cref="OpFieldWrite.ValueLenPos"/> carries the position of the value-length varint so
    /// re-encoding can preserve its original byte width (persistence byte-stability).
    public static (object Op, int Pos) DecodeOp(byte[] data, int pos = 0, bool? fullAuthor = null)
    {
        var r = new Reader(data, pos);
        byte tag = r.U8();
        if (tag == OpsConst.TagFieldWrite)
        {
            ushort fid = r.U16(), ke = r.U16();
            ulong wall = r.U64(); uint logical = r.U32();
            bool full = fullAuthor ?? DetectFieldWriteFullAuthor(data, r.Pos);
            byte[]? author; ushort origin = 0;
            if (full) { author = r.B128(); }
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
            byte[]? author; ushort origin = 0;
            if (full) { author = r.B128(); }
            else { origin = r.U16(); author = null; }
            if (r.Pos != OpsConst.OriginOffset + (full ? 16 : 2))
                throw new InvalidOperationException();
            return (new OpFieldTomb { FieldId = fid, KeyEpoch = ke, Wall = wall, Logical = logical, Origin = origin, AuthorNodeId = author, FullAuthorMode = full }, r.Pos);
        }
        if (tag == OpsConst.TagRecordTomb)
        {
            var rid = r.B128();
            ulong wall = r.U64(); uint logical = r.U32();
            bool full = fullAuthor ?? (r.Remaining == 16);
            byte[]? author; ushort origin = 0;
            if (full) { author = r.B128(); }
            else { origin = r.U16(); author = null; }
            if (r.Pos != OpsConst.OriginOffsetRecordTomb + (full ? 16 : 2))
                throw new InvalidOperationException();
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
    private static bool DetectFieldWriteFullAuthor(byte[] data, int pos)
    {
        bool compact = FieldWriteConsumesExact(data, pos, full: false);
        bool full = FieldWriteConsumesExact(data, pos, full: true);
        if (compact && !full) return false;
        if (full && !compact) return true;
        if (full && compact) return true;
        return data.Length - pos >= 17;
    }

    private static bool FieldWriteConsumesExact(byte[] data, int pos, bool full)
    {
        try
        {
            var r = new Reader(data, pos);
            if (full)
            {
                if (r.Remaining < 16) return false;
                r.B128();
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
