// Logical message fragmentation (§12 FLAG_FRAGMENT, lab §17 scenario 12).
// Wire fragment payload: msgId b128 | index u16 | count u16 | body bytes.
namespace BinSync;

/// <summary>One fragment of a logical message larger than the transport MTU.</summary>
public readonly struct FrameFragment
{
    public byte[] MsgId { get; }
    public ushort Index { get; }
    public ushort Count { get; }
    public byte[] Body { get; }

    public FrameFragment(byte[] msgId, ushort index, ushort count, byte[] body)
    {
        MsgId = msgId;
        Index = index;
        Count = count;
        Body = body;
    }
}

public static class FragmentCodec
{
    /// <summary>Split <paramref name="logicalPayload"/> into fragments of at most <paramref name="maxBody"/> bytes.</summary>
    public static List<byte[]> Split(byte[] logicalPayload, int maxBody, byte[]? msgId = null)
    {
        if (maxBody < 1) throw new ArgumentOutOfRangeException(nameof(maxBody));
        msgId ??= Guid.NewGuid().ToByteArray();
        if (msgId.Length != 16) msgId = Codec.CanonicalB128(msgId);

        if (logicalPayload.Length == 0)
        {
            return new List<byte[]> { EncodeOne(msgId, 0, 1, Array.Empty<byte>()) };
        }

        int count = (logicalPayload.Length + maxBody - 1) / maxBody;
        if (count > ushort.MaxValue) throw new ArgumentException("too many fragments");
        var list = new List<byte[]>(count);
        for (int i = 0; i < count; i++)
        {
            int off = i * maxBody;
            int len = Math.Min(maxBody, logicalPayload.Length - off);
            var body = new byte[len];
            Array.Copy(logicalPayload, off, body, 0, len);
            list.Add(EncodeOne(msgId, (ushort)i, (ushort)count, body));
        }
        return list;
    }

    public static byte[] EncodeOne(byte[] msgId, ushort index, ushort count, byte[] body)
    {
        var w = new Writer();
        w.B128(Codec.CanonicalB128(msgId)).U16(index).U16(count).Bytes(body);
        return w.Value();
    }

    public static FrameFragment DecodeOne(byte[] payload)
    {
        var r = new Reader(payload);
        var id = r.B128();
        ushort idx = r.U16();
        ushort cnt = r.U16();
        var body = r.Bytes();
        return new FrameFragment(id, idx, cnt, body);
    }

    /// <summary>Build FLAG_FRAGMENT frames around split payloads.</summary>
    public static List<byte[]> BuildFrames(
        byte logicalType, uint streamId, ulong baseSeq, byte[] logicalPayload, int maxBody)
    {
        var parts = Split(logicalPayload, maxBody);
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
        public ushort Count;
        public byte[]?[] Parts = Array.Empty<byte[]?>();
        public int Have;
    }

    private readonly Dictionary<string, Slot> _pending = new(StringComparer.Ordinal);

    public int PendingMessages => _pending.Count;

    /// <summary>
    /// Feed a fragment payload. Returns reassembled body when complete; otherwise null.
    /// </summary>
    public byte[]? Accept(byte[] fragmentPayload)
    {
        var f = FragmentCodec.DecodeOne(fragmentPayload);
        if (f.Count == 0) throw new FrameException("FRAME_REJECTED");
        if (f.Index >= f.Count) throw new FrameException("FRAME_REJECTED");

        var key = Convert.ToHexString(f.MsgId);
        if (!_pending.TryGetValue(key, out var slot))
        {
            slot = new Slot { Count = f.Count, Parts = new byte[f.Count][], Have = 0 };
            _pending[key] = slot;
        }
        else if (slot.Count != f.Count)
        {
            throw new FrameException("FRAME_REJECTED"); // count mismatch
        }

        if (slot.Parts[f.Index] is null)
        {
            slot.Parts[f.Index] = f.Body;
            slot.Have++;
        }

        if (slot.Have < slot.Count) return null;

        // Complete
        _pending.Remove(key);
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

    public void Clear() => _pending.Clear();
}
