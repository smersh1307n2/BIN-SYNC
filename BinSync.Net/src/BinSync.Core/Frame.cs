// Frame format (§12.2): 25-byte header, magic 0x4253, CRC32C, hard limits; flow control (§3.6).
using System.Buffers.Binary;
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics.Arm;

namespace BinSync;

/// <summary>Frame constants: magic, version, header size, CRC32C, error code table.</summary>
public static class Frames
{
    /// <summary>Frame magic constant 0x4253 ("BS") as little-endian u16.</summary>
    public const ushort MAGIC = 0x4253;
    /// <summary>Frame format version byte (currently 0x00).</summary>
    public const byte VERSION = 0x00;
    /// <summary>Established-session WINDOW_UPDATE frame type (§3.6).</summary>
    public const byte TYPE_WINDOW_UPDATE = 0x0E;
    /// <summary>Fixed frame header size in bytes (25).</summary>
    public const int HEADER_SIZE = 25;
    /// <summary>Absolute maximum frame size including header (§12.3).</summary>
    public const int MAX_FRAME_SIZE = 16 * 1024 * 1024;            // §12.3
    /// <summary>Fixed frame header size in bytes (25).</summary>
    public const int MAX_PAYLOAD_SIZE = MAX_FRAME_SIZE - HEADER_SIZE;
    /// <summary>Initial flow-control window size in bytes (§3.6.1).</summary>
    public const int INITIAL_WINDOW = 256 * 1024;                  // §3.6.1
    /// <summary>Maximum flow-control window size in bytes.</summary>
    public const int MAX_WINDOW_SIZE = 16 * 1024 * 1024;

    /// <summary>Header flag: payload is compressed.</summary>
    public const byte FLAG_COMPRESSED = 0x01;
    /// <summary>Header flag: payload carries a signature.</summary>
    public const byte FLAG_SIGNED = 0x02;
    /// <summary>Header flag: payload is a fragment of a larger message.</summary>
    public const byte FLAG_FRAGMENT = 0x04;
    /// <summary>Header flag: payload is encrypted.</summary>
    public const byte FLAG_ENCRYPTED = 0x08;

    /// <summary>Map of frame type byte to symbolic name (DELTA_BATCH, ACK, …).</summary>
    private static readonly Dictionary<int, string> FrameTypeMap = new()
    {
        [0x01] = "HELLO",
        [0x02] = "HELLO_ACK",
        [0x03] = "ALIAS_MAP",
        [0x04] = "SCHEMA_MAP",
        [0x05] = "MEMBERSHIP",
        [0x06] = "STATE_DIGEST",
        [0x07] = "NEED_RANGE",
        [0x08] = "NEED_KEYS",
        [0x09] = "DELTA_BATCH",
        [0x0A] = "SNAPSHOT_CHUNK",
        [0x0B] = "ACK",
        [0x0C] = "GC_NOTICE",
        [0x0D] = "ERROR",
        [0x0E] = "WINDOW_UPDATE",
        [0x0F] = "PING",
        [0x10] = "PONG",
        [0x11] = "KEY_ROTATION",
        [0x12] = "IBLT_REQUEST",
        [0x13] = "IBLT_RESPONSE",
    };

    /// <summary>Read-only map of frame type byte to symbolic name.</summary>
    public static IReadOnlyDictionary<int, string> FrameTypes { get; } =
        new ReadOnlyDictionary<int, string>(FrameTypeMap);

    // §12.23 error codes (canonical numeric values)
    private static readonly Dictionary<string, ushort> ErrorCodeMap = new()
    {
        ["UNKNOWN_ALIAS"] = 0x0001,
        ["EPOCH_REGRESSION"] = 0x0002,
        ["CLOCK_DRIFT"] = 0x0003,
        ["INVALID_VARINT"] = 0x0004,
        ["INVALID_FLOAT"] = 0x0005,
        ["SCHEMA_MISMATCH"] = 0x0006,
        ["SEQ_GAP"] = 0x0007,
        ["DUPLICATE_BATCH"] = 0x0008,
        ["NAMESPACE_FORBIDDEN"] = 0x0009,
        ["INTERNAL"] = 0x000A,
        ["OUTDATED_WATERMARK"] = 0x000B,
        ["DECOMPRESSION_LIMIT_EXCEEDED"] = 0x000C,
        ["BATCH_TRANSACTION_FAILED"] = 0x000D,
        ["PAYLOAD_TOO_LARGE"] = 0x000E,
        ["FLOW_CONTROL_VIOLATION"] = 0x000F,
        ["UNKNOWN_KEY_EPOCH"] = 0x0010,
        ["SCHEMA_PENDING"] = 0x0011,
        ["CAUSAL_DEP_MISSING"] = 0x0012,
        ["SIGNATURE_INVALID"] = 0x0013,
        ["TRUST_REVOKED"] = 0x0014,
        ["CORRUPTED_FRAME"] = 0x0015,
        ["FRAME_REJECTED"] = 0x0016,
        ["BATCH_CHAIN_BROKEN"] = 0x0017,
        ["ACK_OUT_OF_RANGE"] = 0x0018,
    };

    /// <summary>Read-only map of symbolic error names to numeric wire codes.</summary>
    public static IReadOnlyDictionary<string, ushort> ErrorCodes { get; } =
        new ReadOnlyDictionary<string, ushort>(ErrorCodeMap);

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

    /// <summary>Compute Castagnoli CRC32C over the data (SSE4.2 / ARM CRC32 when available).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Crc32C(ReadOnlySpan<byte> data)
    {
        if (Sse42.IsSupported)
            return Crc32CSse42(data);
        if (Crc32.Arm64.IsSupported)
            return Crc32CArm64(data);
        if (Crc32.IsSupported)
            return Crc32CArm(data);
        return Crc32CSoftware(data);
    }

    /// <summary>Compute Castagnoli CRC32C over the data.</summary>
    public static uint Crc32C(byte[] data) => Crc32C(data.AsSpan());

    private static uint Crc32CSoftware(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data)
            crc = CrcTable[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }

    private static uint Crc32CSse42(ReadOnlySpan<byte> data)
    {
        if (Sse42.X64.IsSupported)
        {
            ulong crc = 0xFFFF_FFFFUL;
            while (data.Length >= 8)
            {
                crc = Sse42.X64.Crc32(crc, BinaryPrimitives.ReadUInt64LittleEndian(data));
                data = data.Slice(8);
            }
            uint c = (uint)crc;
            while (data.Length > 0)
            {
                c = Sse42.Crc32(c, data[0]);
                data = data.Slice(1);
            }
            return c ^ 0xFFFF_FFFFu;
        }
        // 32-bit path: process 4 bytes at a time
        uint crc32 = 0xFFFF_FFFFu;
        while (data.Length >= 4)
        {
            crc32 = Sse42.Crc32(crc32, BinaryPrimitives.ReadUInt32LittleEndian(data));
            data = data.Slice(4);
        }
        while (data.Length > 0)
        {
            crc32 = Sse42.Crc32(crc32, data[0]);
            data = data.Slice(1);
        }
        return crc32 ^ 0xFFFF_FFFFu;
    }

    private static uint Crc32CArm64(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFF_FFFFu;
        while (data.Length >= 8)
        {
            crc = Crc32.Arm64.ComputeCrc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data));
            data = data.Slice(8);
        }
        while (data.Length > 0)
        {
            crc = Crc32.ComputeCrc32C(crc, data[0]);
            data = data.Slice(1);
        }
        return crc ^ 0xFFFF_FFFFu;
    }

    private static uint Crc32CArm(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFF_FFFFu;
        while (data.Length >= 4)
        {
            crc = Crc32.ComputeCrc32C(crc, BinaryPrimitives.ReadUInt32LittleEndian(data));
            data = data.Slice(4);
        }
        while (data.Length > 0)
        {
            crc = Crc32.ComputeCrc32C(crc, data[0]);
            data = data.Slice(1);
        }
        return crc ^ 0xFFFF_FFFFu;
    }
}

/// <summary>class: class.</summary>
public sealed record class Frame(byte FrameType, byte Flags, uint StreamId, ulong FrameSeq, byte[] Payload)
{
    /// <summary>Symbolic frame type name when known.</summary>
    public string Name => Frames.FrameTypes.TryGetValue(FrameType, out var n) ? n : $"0x{FrameType:x2}";
}

/// <summary>Frame parse/build failure (magic, CRC, size). Distinct from <see cref="SyncException"/>.</summary>
public class FrameException : Exception
{
    /// <summary>Symbolic code (e.g. PAYLOAD_TOO_LARGE, BAD_MAGIC).</summary>
    public string CodeName { get; }

    /// <summary>Numeric wire code when known; otherwise 0.</summary>
    public ushort Code => Frames.ErrorCodes.TryGetValue(CodeName, out var c) ? c : (ushort)0;

    /// <summary>True → session must be torn down ([Ц3]).</summary>
    public bool Fatal { get; }

    /// <summary>True → silent drop (bad magic [Ц2]).</summary>
    public bool SilentDrop { get; set; }

    /// <summary>Create with code name as message.</summary>
    public FrameException(string codeName, bool fatal = false)
        : base(codeName)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        Fatal = fatal;
    }

    /// <summary>Create with a distinct message.</summary>
    public FrameException(string codeName, string message, bool fatal = false)
        : base(message)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        Fatal = fatal;
    }

    /// <summary>Create with message and inner exception.</summary>
    public FrameException(string codeName, string message, Exception innerException, bool fatal = false)
        : base(message, innerException)
    {
        CodeName = codeName ?? throw new ArgumentNullException(nameof(codeName));
        Fatal = fatal;
    }
}

/// <summary>Build and parse framed packets with a fixed 25-byte header and CRC32C.</summary>
public static class FrameCodec
{
    /// Header (§12.2 packed): magic u16 | version u8 | type u8 | flags u8 | streamId u32 |
    /// frameSeq u64 | payloadLen u32 | crc32c u32 — exactly 25 bytes (BUGFIX: no struct padding).
    /// <summary>Build a framed packet with CRC32C header.</summary>
    public static byte[] BuildFrame(byte frameType, uint streamId, ulong frameSeq, byte[] payload, byte flags = 0) =>
        BuildFrame(frameType, streamId, frameSeq, (ReadOnlySpan<byte>)payload, flags);

    /// <summary>Build a framed packet with CRC32C header from a span payload.</summary>
    public static byte[] BuildFrame(byte frameType, uint streamId, ulong frameSeq, ReadOnlySpan<byte> payload, byte flags = 0)
    {
        if (payload.Length > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        uint crc = Frames.Crc32C(payload);
        var frame = new byte[Frames.HEADER_SIZE + payload.Length];
        BinaryPrimitives.WriteUInt16LittleEndian(frame.AsSpan(0), Frames.MAGIC);
        frame[2] = Frames.VERSION; frame[3] = frameType; frame[4] = flags;
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(5), streamId);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(9), frameSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(17), (uint)payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(21), crc);
        payload.CopyTo(frame.AsSpan(Frames.HEADER_SIZE));
        return frame;
    }

    /// <summary>Write frame into destination; returns bytes written.</summary>
    public static int BuildFrame(byte frameType, uint streamId, ulong frameSeq, ReadOnlySpan<byte> payload, Span<byte> destination, byte flags = 0)
    {
        int need = Frames.HEADER_SIZE + payload.Length;
        if (destination.Length < need)
            throw new ArgumentException($"destination too small (need {need})", nameof(destination));
        if (payload.Length > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        uint crc = Frames.Crc32C(payload);
        BinaryPrimitives.WriteUInt16LittleEndian(destination, Frames.MAGIC);
        destination[2] = Frames.VERSION; destination[3] = frameType; destination[4] = flags;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(5), streamId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(9), frameSeq);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(17), (uint)payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(21), crc);
        payload.CopyTo(destination.Slice(Frames.HEADER_SIZE));
        return need;
    }

    /// <summary>Parse and validate a framed packet.</summary>
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
        var payloadSpan = data.AsSpan(Frames.HEADER_SIZE, (int)payloadLen);
        if (Frames.Crc32C(payloadSpan) != crc)
            throw new FrameException("CORRUPTED_FRAME");  // [Ц1] session preserved
        var payload = payloadSpan.ToArray();
        return (new Frame(ftype, flags, streamId, frameSeq, payload), Frames.HEADER_SIZE + (int)payloadLen);
    }

    /// <summary>Validate magic, length and CRC without allocating a <see cref="Frame"/> or payload copy.</summary>
    public static void ValidateFrame(ReadOnlySpan<byte> data)
    {
        if (data.Length < Frames.HEADER_SIZE)
            throw new FrameException("FRAME_REJECTED");
        ushort magic = BinaryPrimitives.ReadUInt16LittleEndian(data);
        if (magic != Frames.MAGIC)
            throw new FrameException("BAD_MAGIC") { SilentDrop = true };
        uint payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(17));
        if (payloadLen > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        if (data.Length < Frames.HEADER_SIZE + (int)payloadLen)
            throw new FrameException("FRAME_REJECTED");
        uint crc = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(21));
        var payloadSpan = data.Slice(Frames.HEADER_SIZE, (int)payloadLen);
        if (Frames.Crc32C(payloadSpan) != crc)
            throw new FrameException("CORRUPTED_FRAME");
    }
}

/// <summary>Per-stream byte-window accounting shared by send and receive paths.</summary>
public sealed class FlowControl
{
    private readonly object _gate = new();
    private readonly Dictionary<uint, long> _credit = new();
    private readonly Dictionary<uint, long> _inFlight = new();
    private readonly Dictionary<uint, TaskCompletionSource> _waiters = new();
    private Exception? _failure;

    /// <summary>Configured initial flow-control window.</summary>
    public int Initial { get; }

    /// <summary>Configured maximum flow-control window.</summary>
    public int MaxWindow { get; }

    public FlowControl(int initial = Frames.INITIAL_WINDOW, int maxWindow = Frames.MAX_WINDOW_SIZE)
    {
        if (initial < 0) throw new ArgumentOutOfRangeException(nameof(initial));
        if (maxWindow <= 0) throw new ArgumentOutOfRangeException(nameof(maxWindow));
        if (initial > maxWindow) throw new ArgumentException("Initial window cannot exceed maximum window.", nameof(initial));
        Initial = initial;
        MaxWindow = maxWindow;
    }

    private long BalanceInternal(uint streamId) =>
        _credit.TryGetValue(streamId, out var value) ? value : (_credit[streamId] = Initial);

    /// <summary>Remaining credit on a stream window (bytes that may still be sent).</summary>
    public long Balance(uint streamId)
    {
        lock (_gate) return BalanceInternal(streamId);
    }

    /// <summary>Attempt to consume payloadBytes from the stream window; false if insufficient credit.</summary>
    public bool TryConsume(uint streamId, long payloadBytes)
    {
        if (payloadBytes < 0) throw new ArgumentOutOfRangeException(nameof(payloadBytes));
        if (payloadBytes > MaxWindow) return false;
        lock (_gate)
        {
            if (_failure is not null) throw new IOException("Flow-control session has failed.", _failure);
            var balance = BalanceInternal(streamId);
            if (balance < payloadBytes) return false;
            _credit[streamId] = balance - payloadBytes;
            _inFlight.TryGetValue(streamId, out long inFlight);
            _inFlight[streamId] = checked(inFlight + payloadBytes);
            return true;
        }
    }

    /// <summary>Wait until payload credit is available, then reserve it atomically.</summary>
    public ValueTask WaitForCreditAsync(uint streamId, long payloadBytes, CancellationToken ct = default)
    {
        if (payloadBytes < 0 || payloadBytes > MaxWindow)
            throw new ArgumentOutOfRangeException(nameof(payloadBytes));
        if (TryConsume(streamId, payloadBytes)) return ValueTask.CompletedTask;
        return new ValueTask(WaitSlowAsync(streamId, payloadBytes, ct));
    }

    private async Task WaitSlowAsync(uint streamId, long payloadBytes, CancellationToken ct)
    {
        while (true)
        {
            Task waitTask;
            lock (_gate)
            {
                if (_failure is not null) throw new IOException("Flow-control session has failed.", _failure);
                long balance = BalanceInternal(streamId);
                if (balance >= payloadBytes)
                {
                    _credit[streamId] = balance - payloadBytes;
                    _inFlight.TryGetValue(streamId, out long inFlight);
                    _inFlight[streamId] = checked(inFlight + payloadBytes);
                    return;
                }
                if (!_waiters.TryGetValue(streamId, out var waiter))
                {
                    waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    _waiters.Add(streamId, waiter);
                }
                waitTask = waiter.Task;
            }
            await waitTask.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>Apply positive additive credits, capped at the configured maximum.</summary>
    public long ApplyUpdate(uint streamId, long credits)
    {
        if (credits <= 0) throw new ArgumentOutOfRangeException(nameof(credits));
        TaskCompletionSource? waiter;
        long next;
        lock (_gate)
        {
            long balance = BalanceInternal(streamId);
            long room = MaxWindow - balance;
            next = credits >= room ? MaxWindow : balance + credits;
            _credit[streamId] = next;
            _inFlight.TryGetValue(streamId, out long inFlight);
            _inFlight[streamId] = credits >= inFlight ? 0 : inFlight - credits;
            _waiters.Remove(streamId, out waiter);
        }
        waiter?.TrySetResult();
        return next;
    }

    /// <summary>Fail all current and future credit waiters when the session can no longer progress.</summary>
    public void Fail(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        TaskCompletionSource[] waiters;
        lock (_gate)
        {
            if (_failure is not null) return;
            _failure = failure;
            waiters = _waiters.Values.ToArray();
            _waiters.Clear();
        }
        foreach (var waiter in waiters) waiter.TrySetResult();
    }

    /// <summary>Apply a peer update only when it is strictly within the negotiated window.</summary>
    public bool TryApplyUpdate(uint streamId, long credits)
    {
        if (credits <= 0) return false;
        TaskCompletionSource? waiter;
        lock (_gate)
        {
            if (_failure is not null) return false;
            long balance = BalanceInternal(streamId);
            if (credits > MaxWindow - balance) return false;
            if (!_inFlight.TryGetValue(streamId, out long inFlight) || credits > inFlight)
                return false;
            _credit[streamId] = balance + credits;
            _inFlight[streamId] = inFlight - credits;
            _waiters.Remove(streamId, out waiter);
        }
        waiter?.TrySetResult();
        return true;
    }
}
