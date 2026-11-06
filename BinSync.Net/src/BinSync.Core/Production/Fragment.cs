using System.Buffers;
// Logical message fragmentation (§12 FLAG_FRAGMENT, lab §17 scenario 12).
// Wire fragment payload: msgId b128 | index u16 | count u16 | body bytes.
namespace BinSync;

/// <summary>One fragment of a logical message larger than the transport MTU.</summary>
public readonly struct FrameFragment
{
    /// <summary>Fragment message id grouping parts of one logical frame.</summary>
    public byte[] MsgId { get; }
    /// <summary>Zero-based index of this fragment within the message.</summary>
    public ushort Index { get; }
    /// <summary>Signed multiplicity of digests folded into this cell.</summary>
    public ushort Count { get; }
    /// <summary>Fragment payload body bytes.</summary>
    public byte[] Body { get; }

    /// <summary>One fragment of a larger frame (msg id, index, count, payload).</summary>
    public FrameFragment(byte[] msgId, ushort index, ushort count, byte[] body)
    {
        MsgId = msgId;
        Index = index;
        Count = count;
        Body = body;
    }
}

/// <summary>Encode and decode FLAG_FRAGMENT frame bodies.</summary>
public static class FragmentCodec
{
    /// <summary>Split <paramref name="logicalPayload"/> into fragments of at most <paramref name="maxBody"/> bytes.</summary>
    public static List<byte[]> Split(byte[] logicalPayload, int maxBody, byte[]? msgId = null)
    {
        ArgumentNullException.ThrowIfNull(logicalPayload);
        if (maxBody < 1 || maxBody > Frames.MAX_PAYLOAD_SIZE - 24)
            throw new ArgumentOutOfRangeException(nameof(maxBody));
        if (logicalPayload.Length > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        msgId ??= Guid.NewGuid().ToByteArray();
        if (msgId.Length != 16) msgId = Codec.CanonicalB128(msgId);
        if (msgId.AsSpan().IndexOfAnyExcept((byte)0) < 0)
            throw new ArgumentException("Fragment message id must be non-zero.", nameof(msgId));

        if (logicalPayload.Length == 0)
        {
            return new List<byte[]> { EncodeOne(msgId, 0, 1, Array.Empty<byte>()) };
        }

        int count = checked((int)(((long)logicalPayload.Length + maxBody - 1) / maxBody));
        if (count > ushort.MaxValue) throw new ArgumentException("too many fragments");
        var list = new List<byte[]>(count);
        for (int i = 0; i < count; i++)
        {
            int off = i * maxBody;
            int len = Math.Min(maxBody, logicalPayload.Length - off);
            list.Add(EncodeOne(msgId, (ushort)i, (ushort)count, logicalPayload.AsSpan(off, len)));
        }
        return list;
    }

    /// <summary>Encode one into canonical wire bytes.</summary>
    public static byte[] EncodeOne(byte[] msgId, ushort index, ushort count, byte[] body)
    {
        ArgumentNullException.ThrowIfNull(body);
        return EncodeOne(msgId, index, count, (ReadOnlySpan<byte>)body);
    }

    /// <summary>Encode one fragment without requiring an owned body array.</summary>
    public static byte[] EncodeOne(byte[] msgId, ushort index, ushort count, ReadOnlySpan<byte> body)
    {
        ArgumentNullException.ThrowIfNull(msgId);
        if (msgId.Length != 16 || msgId.AsSpan().IndexOfAnyExcept((byte)0) < 0)
            throw new ArgumentException("Fragment message id must be a non-zero 16-byte value.", nameof(msgId));
        if (count == 0 || index >= count)
            throw new ArgumentOutOfRangeException(nameof(index));
        int est = 24 + body.Length;
        var rented = ArrayPool<byte>.Shared.Rent(est);
        try
        {
            var w = new Writer(rented);
            // msgId is already 16-byte from Split
            w.B128(msgId).U16(index).U16(count).Bytes(body);
            int n = w.Length;
            var result = new byte[n];
            w.WrittenSpan.CopyTo(result);
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Decode one from canonical wire bytes.</summary>
    public static FrameFragment DecodeOne(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var r = new Reader(payload);
        var id = r.B128();
        ushort idx = r.U16();
        ushort cnt = r.U16();
        var body = r.Bytes();
        if (!r.Eof() || cnt == 0 || idx >= cnt || id.AsSpan().IndexOfAnyExcept((byte)0) < 0)
            throw new FrameException("FRAME_REJECTED");
        return new FrameFragment(id, idx, cnt, body);
    }

    /// <summary>Build FLAG_FRAGMENT frames around split payloads.</summary>
    public static List<byte[]> BuildFrames(
        byte logicalType, uint streamId, ulong baseSeq, byte[] logicalPayload, int maxBody)
    {
        var parts = Split(logicalPayload, maxBody);
        if (parts.Count > 0 && (ulong)(parts.Count - 1) > ulong.MaxValue - baseSeq)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        var frames = new List<byte[]>(parts.Count);
        for (int i = 0; i < parts.Count; i++)
        {
            frames.Add(FrameCodec.BuildFrame(
                logicalType, streamId, baseSeq + (ulong)i, parts[i],
                flags: Frames.FLAG_FRAGMENT));
        }
        return frames;
    }
}

/// <summary>
/// Reassembles FLAG_FRAGMENT frames by msgId. Returns complete logical payload when all parts arrive.
/// </summary>
public sealed class FragmentReassembler
{
    private sealed class Slot
    {
        /// <summary>Signed multiplicity of digests folded into this cell.</summary>
        public ushort Count;
        /// <summary>Received fragment parts awaiting reassembly (null slots still missing).</summary>
        public byte[]?[] Parts = Array.Empty<byte[]?>();
        /// <summary>Number of fragment parts received so far.</summary>
        public int Have;
    }

    private readonly Dictionary<BatchId, Slot> _pending = new();
    private long _pendingBytes;
    private int _pendingSlots;
    private const int MaxPendingBytes = Frames.MAX_PAYLOAD_SIZE;
    private const int MaxPendingMessages = 64;
    private const int MaxPendingFragments = 262_144;

    /// <summary>In-progress fragment reassembly keyed by message id.</summary>
    public int PendingMessages => _pending.Count;

    /// <summary>
    /// Feed a fragment payload. Returns reassembled body when complete; otherwise null.
    /// </summary>
    public byte[]? Accept(byte[] fragmentPayload) => Accept(FragmentCodec.DecodeOne(fragmentPayload));

    /// <summary>Accept an already decoded fragment without allocating a second body copy.</summary>
    public byte[]? Accept(FrameFragment f)
    {
        if (f.MsgId is null || f.MsgId.Length != 16 ||
            f.MsgId.AsSpan().IndexOfAnyExcept((byte)0) < 0 ||
            f.Body is null || f.Count == 0 || f.Index >= f.Count)
            throw new FrameException("FRAME_REJECTED");

        var key = BatchId.From(f.MsgId);
        Slot slot;
        if (_pending.TryGetValue(key, out var existingSlot))
        {
            slot = existingSlot;
        }
        else
        {
            if (f.Body.Length > MaxPendingBytes - _pendingBytes ||
                _pending.Count >= MaxPendingMessages ||
                f.Count > MaxPendingFragments - _pendingSlots)
                throw new FrameException("FLOW_CONTROL_VIOLATION");
            slot = new Slot { Count = f.Count, Parts = new byte[f.Count][], Have = 0 };
            _pending[key] = slot;
            _pendingSlots += f.Count;
        }
        if (slot.Count != f.Count)
        {
            RemoveSlot(key, slot);
            throw new FrameException("FRAME_REJECTED");
        }

        var existing = slot.Parts[f.Index];
        if (existing is not null)
        {
            if (!existing.AsSpan().SequenceEqual(f.Body))
            {
                RemoveSlot(key, slot);
                throw new FrameException("FRAME_REJECTED");
            }
            return null;
        }

        if (f.Body.Length > MaxPendingBytes - _pendingBytes)
        {
            RemoveSlot(key, slot);
            throw new FrameException("FLOW_CONTROL_VIOLATION");
        }
        slot.Parts[f.Index] = f.Body;
        slot.Have++;
        _pendingBytes += f.Body.Length;

        if (slot.Have < slot.Count) return null;

        // Complete
        RemoveSlot(key, slot);
        int total = 0;
        foreach (var p in slot.Parts) total += p!.Length;
        var buf = new byte[total];
        int off = 0;
        foreach (var p in slot.Parts)
        {
            Array.Copy(p!, 0, buf, off, p!.Length);
            off += p.Length;
        }
        return buf;
    }

    private void RemoveSlot(BatchId key, Slot slot)
    {
        _pending.Remove(key);
        _pendingSlots -= slot.Parts.Length;
        foreach (var part in slot.Parts)
            if (part is not null) _pendingBytes -= part.Length;
    }

    /// <summary>Drop all pending reassembly state.</summary>
    public void Clear() { _pending.Clear(); _pendingBytes = 0; _pendingSlots = 0; }
}
