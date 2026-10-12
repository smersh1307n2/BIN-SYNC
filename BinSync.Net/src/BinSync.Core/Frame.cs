// Frame format (§12.2): 25-byte header, magic 0x4253, CRC32C, hard limits; flow control (§3.6).
using System.Buffers.Binary;

namespace BinSync;

public static class Frames
{
    public const ushort MAGIC = 0x4253;
    public const byte VERSION = 0x00;
    public const int HEADER_SIZE = 25;
    public const int MAX_FRAME_SIZE = 16 * 1024 * 1024;            // §12.3
    public const int MAX_PAYLOAD_SIZE = MAX_FRAME_SIZE - HEADER_SIZE;
    public const int INITIAL_WINDOW = 256 * 1024;                  // §3.6.1
    public const int MAX_WINDOW_SIZE = 16 * 1024 * 1024;

    public const byte FLAG_COMPRESSED = 0x01;
    public const byte FLAG_SIGNED = 0x02;
    public const byte FLAG_FRAGMENT = 0x04;
    public const byte FLAG_ENCRYPTED = 0x08;

    public static readonly Dictionary<int, string> FrameTypes = new()
    {
        [0x01] = "HELLO", [0x02] = "HELLO_ACK", [0x03] = "ALIAS_MAP", [0x04] = "SCHEMA_MAP",
        [0x05] = "MEMBERSHIP", [0x06] = "STATE_DIGEST", [0x07] = "NEED_RANGE", [0x08] = "NEED_KEYS",
        [0x09] = "DELTA_BATCH", [0x0A] = "SNAPSHOT_CHUNK", [0x0B] = "ACK", [0x0C] = "GC_NOTICE",
        [0x0D] = "ERROR", [0x0E] = "WINDOW_UPDATE", [0x0F] = "PING", [0x10] = "PONG",
        [0x11] = "KEY_ROTATION", [0x12] = "IBLT_REQUEST", [0x13] = "IBLT_RESPONSE",
    };

    // §12.23 error codes (canonical numeric values)
    public static readonly Dictionary<string, ushort> ErrorCodes = new()
    {
        ["UNKNOWN_ALIAS"] = 0x0001, ["EPOCH_REGRESSION"] = 0x0002, ["CLOCK_DRIFT"] = 0x0003,
        ["INVALID_VARINT"] = 0x0004, ["INVALID_FLOAT"] = 0x0005, ["SCHEMA_MISMATCH"] = 0x0006,
        ["SEQ_GAP"] = 0x0007, ["DUPLICATE_BATCH"] = 0x0008, ["NAMESPACE_FORBIDDEN"] = 0x0009,
        ["INTERNAL"] = 0x000A, ["OUTDATED_WATERMARK"] = 0x000B,
        ["DECOMPRESSION_LIMIT_EXCEEDED"] = 0x000C, ["BATCH_TRANSACTION_FAILED"] = 0x000D,
        ["PAYLOAD_TOO_LARGE"] = 0x000E, ["FLOW_CONTROL_VIOLATION"] = 0x000F,
        ["UNKNOWN_KEY_EPOCH"] = 0x0010, ["SCHEMA_PENDING"] = 0x0011, ["CAUSAL_DEP_MISSING"] = 0x0012,
        ["SIGNATURE_INVALID"] = 0x0013, ["TRUST_REVOKED"] = 0x0014, ["CORRUPTED_FRAME"] = 0x0015,
        ["FRAME_REJECTED"] = 0x0016, ["BATCH_CHAIN_BROKEN"] = 0x0017,
    };

    private static readonly uint[] CrcTable = BuildTable();
    private static uint[] BuildTable()
    {
        const uint poly = 0x82F63B78u;   // CRC32C Castagnoli reflected
        var t = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? (c >> 1) ^ poly : c >> 1;
            t[i] = c;
        }
        return t;
    }

    public static uint Crc32C(byte[] data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data) crc = CrcTable[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }
}

public sealed record class Frame(byte FrameType, byte Flags, uint StreamId, ulong FrameSeq, byte[] Payload)
{
    public string Name => Frames.FrameTypes.TryGetValue(FrameType, out var n) ? n : $"0x{FrameType:x2}";
}

public class FrameException : Exception
{
    public string CodeName;
    public ushort Code => Frames.ErrorCodes.TryGetValue(CodeName, out var c) ? c : (ushort)0;
    public bool Fatal;      // true → session must be torn down ([Ц3])
    public bool SilentDrop; // bad magic [Ц2]
    public FrameException(string codeName, bool fatal = false) : base(codeName) { CodeName = codeName; Fatal = fatal; }
}

public static class FrameCodec
{
    /// Header (§12.2 packed): magic u16 | version u8 | type u8 | flags u8 | streamId u32 |
    /// frameSeq u64 | payloadLen u32 | crc32c u32 — exactly 25 bytes (BUGFIX: no struct padding).
    public static byte[] BuildFrame(byte frameType, uint streamId, ulong frameSeq, byte[] payload, byte flags = 0)
    {
        if (payload.Length > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        uint crc = Frames.Crc32C(payload);
        var header = new byte[Frames.HEADER_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(0), Frames.MAGIC);
        header[2] = Frames.VERSION; header[3] = frameType; header[4] = flags;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(5), streamId);
        BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(9), frameSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(17), (uint)payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(21), crc);
        return header.Concat(payload).ToArray();
    }

    public static (Frame Frame, int Consumed) ParseFrame(byte[] data)
    {
        if (data.Length < Frames.HEADER_SIZE) throw new FrameException("FRAME_REJECTED");
        ushort magic = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(0));
        byte ftype = data[3], flags = data[4];
        uint streamId = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(5));
        ulong frameSeq = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(9));
        uint payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(17));
        uint crc = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(21));
        if (magic != Frames.MAGIC)
        {
            var err = new FrameException("BAD_MAGIC") { SilentDrop = true };
            throw err;   // [Ц2] silent drop
        }
        if (payloadLen > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        if (data.Length < Frames.HEADER_SIZE + (int)payloadLen)
            throw new FrameException("FRAME_REJECTED");   // incomplete
        var payload = data[Frames.HEADER_SIZE..(Frames.HEADER_SIZE + (int)payloadLen)];
        if (Frames.Crc32C(payload) != crc)
            throw new FrameException("CORRUPTED_FRAME");  // [Ц1] session preserved
        return (new Frame(ftype, flags, streamId, frameSeq, payload), Frames.HEADER_SIZE + (int)payloadLen);
    }
}

/// §3.6 additive-credit window per streamId. Payload bytes only, not headers.
public class FlowControl
{
    public int Initial;
    public int MaxWindow;
    private readonly Dictionary<uint, long> _credit = new();

    public FlowControl(int initial = Frames.INITIAL_WINDOW, int maxWindow = Frames.MAX_WINDOW_SIZE)
    { Initial = initial; MaxWindow = maxWindow; }

    public long Balance(uint streamId) => _credit.TryGetValue(streamId, out var v) ? v : (_credit[streamId] = Initial);

    public bool TryConsume(uint streamId, long payloadBytes)
    {
        var bal = Balance(streamId);
        if (bal < payloadBytes) return false;   // [УП2]
        _credit[streamId] = bal - payloadBytes;
        return true;
    }

    /// WINDOW_UPDATE is additive; clamp at MAX_WINDOW_SIZE [УП1].
    public long ApplyUpdate(uint streamId, long credits)
    {
        long nv = Math.Min(Balance(streamId) + credits, MaxWindow);
        _credit[streamId] = nv;
        return nv;
    }
}
