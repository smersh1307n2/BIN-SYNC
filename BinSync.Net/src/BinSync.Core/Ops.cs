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
}

public class OpFieldTomb
{
    public ushort FieldId, KeyEpoch;
    public ulong Wall; public uint Logical;
    public ushort Origin;
    public byte[]? AuthorNodeId;
    public byte Tag => OpsConst.TagFieldTomb;
}

public class OpRecordTombstone
{
    public byte[] RecordId = Array.Empty<byte>();
    public ulong Wall; public uint Logical;
    public ushort Origin;
    public byte[]? AuthorNodeId;
    public byte Tag => OpsConst.TagRecordTomb;
}

public static class Ops
{
    /// Snapshot-mode encoding: always carries the durable author (mirrors Python
    /// build_snapshot which calls encode_op(op) with default full_author=True).
    public static byte[] EncodeOpFull(object op) => EncodeOp(op, fullAuthor: true);

    /// Canonical encoding. Compact mode: u16 origin; FULL_AUTHOR_ID mode: durable b128 (§12.24.4).
    public static byte[] EncodeOp(object op, bool fullAuthor = false)
    {
        var w = new Writer();
        if (op is OpFieldWrite fw)
        {
            w.U8(OpsConst.TagFieldWrite).U16(fw.FieldId).U16(fw.KeyEpoch).U64(fw.Wall).U32(fw.Logical);
            if (w.Value().Length != OpsConst.OriginOffset)
                throw new InvalidOperationException("[C11] prefix must be exactly 17 bytes");
            if (fullAuthor) w.B128(fw.AuthorNodeId!);
            else w.U16(fw.Origin);
            w.Varint((ulong)fw.Value.Length).Raw(fw.Value);
            return w.Value();
        }
        if (op is OpFieldTomb ft)
        {
            w.U8(OpsConst.TagFieldTomb).U16(ft.FieldId).U16(ft.KeyEpoch).U64(ft.Wall).U32(ft.Logical);
            if (w.Value().Length != OpsConst.OriginOffset)
                throw new InvalidOperationException("[C11] prefix must be exactly 17 bytes");
            if (fullAuthor) w.B128(ft.AuthorNodeId!);
            else w.U16(ft.Origin);
            return w.Value();
        }
        if (op is OpRecordTombstone rt)
        {
            w.U8(OpsConst.TagRecordTomb).B128(rt.RecordId).U64(rt.Wall).U32(rt.Logical);
            if (w.Value().Length != OpsConst.OriginOffsetRecordTomb)
                throw new InvalidOperationException("origin at offset 29");
            if (fullAuthor) w.B128(rt.AuthorNodeId!);
            else w.U16(rt.Origin);
            return w.Value();
        }
        throw new ArgumentException($"unknown op {op}");
    }

    /// fullAuthor=null → AUTO-DETECT the mode from payload length (BUGFIX note in Python port).
    public static (object Op, int Pos) DecodeOp(byte[] data, int pos = 0, bool? fullAuthor = null)
    {
        var r = new Reader(data, pos);
        byte tag = r.U8();
        if (tag == OpsConst.TagFieldWrite)
        {
            ushort fid = r.U16(), ke = r.U16();
            ulong wall = r.U64(); uint logical = r.U32();
            bool full = fullAuthor ?? (r.Remaining >= 17);
            byte[]? author; ushort origin = 0;
            if (full) { author = r.B128(); }
            else { origin = r.U16(); author = null; }
            if (r.Pos != OpsConst.OriginOffset + (full ? 16 : 2))
                throw new InvalidOperationException("[C11] author slot at fixed offset");
            var value = r.Bytes();
            return (new OpFieldWrite { FieldId = fid, KeyEpoch = ke, Wall = wall, Logical = logical, Origin = origin, AuthorNodeId = author, Value = value }, r.Pos);
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
            return (new OpFieldTomb { FieldId = fid, KeyEpoch = ke, Wall = wall, Logical = logical, Origin = origin, AuthorNodeId = author }, r.Pos);
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
            return (new OpRecordTombstone { RecordId = rid, Wall = wall, Logical = logical, Origin = origin, AuthorNodeId = author }, r.Pos);
        }
        throw new ArgumentException($"unknown op tag 0x{tag:x}");
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
