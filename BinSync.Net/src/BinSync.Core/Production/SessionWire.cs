using System.Buffers;
// Wire payloads for session bootstrap: HELLO, HELLO_ACK, ALIAS_MAP (§12.7+, §4.3–4.4).
namespace BinSync;

/// <summary>Capability bits exchanged in HELLO (subset of §12 capabilities).</summary>
[Flags]
public enum SessionCapabilities : uint
{
    None = 0,
    /// <summary>§12.24.4 — FieldWrite carries full 16-byte author instead of origin u16.</summary>
    FullAuthorId = 1 << 0,
    /// <summary>§3.6.2 — peer will honor WINDOW_UPDATE on this transport.</summary>
    FlowControl = 1 << 1,
}

/// <summary>struct: struct.</summary>
public readonly record struct HelloPayload(
    NodeId NodeId, int Epoch, SessionCapabilities Capabilities);

/// <summary>struct: struct.</summary>
public readonly record struct AliasMapEntry(
    ushort Alias, NodeId NodeId, int Epoch);

/// <summary>
/// Encode / decode session-control payloads. Frame types: HELLO=0x01, HELLO_ACK=0x02,
/// ALIAS_MAP=0x03, ERROR=0x0D. Control plane uses <c>streamId = 0</c> (§3.7).
/// </summary>
public static class SessionWire
{
    /// <summary>Control message type: HELLO.</summary>
    public const byte TypeHello = 0x01;
    /// <summary>Constant type hello ack.</summary>
    public const byte TypeHelloAck = 0x02;
    /// <summary>Control message type: ALIAS_MAP.</summary>
    public const byte TypeAliasMap = 0x03;
    /// <summary>Control message type: ERROR.</summary>
    public const byte TypeError = 0x0D;
    /// <summary>Stream id reserved for session control messages (0).</summary>
    public const uint ControlStreamId = 0;

    /// <summary>Encode hello into canonical wire bytes.</summary>
    public static byte[] EncodeHello(HelloPayload h)
    {
        var rented = ArrayPool<byte>.Shared.Rent(48);
        try
        {
            var w = new Writer(rented);
            w.B128(h.NodeId).U32((uint)h.Epoch).U32((uint)h.Capabilities);
            return FrameCodec.BuildFrame(TypeHello, ControlStreamId, 1, w.WrittenSpan);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Decode hello from canonical wire bytes.</summary>
    public static HelloPayload DecodeHello(byte[] frameBytes) => DecodeHelloPayload(frameBytes, TypeHello);

    /// <summary>Decode HELLO_ACK; HELLO and HELLO_ACK are not interchangeable.</summary>
    public static HelloPayload DecodeHelloAck(byte[] frameBytes) => DecodeHelloPayload(frameBytes, TypeHelloAck);

    private static HelloPayload DecodeHelloPayload(byte[] frameBytes, byte expectedType)
    {
        var frame = ParseControlFrame(frameBytes, expectedType);
        if (frame.Payload.Length != 24)
            throw new FrameException("FRAME_REJECTED");
        var r = new Reader(frame.Payload);
        var nodeId = NodeId.From(r.B128());
        uint epoch = r.U32();
        uint capabilities = r.U32();
        if (epoch > int.MaxValue)
            throw new FrameException("FRAME_REJECTED");
        return new HelloPayload(nodeId, (int)epoch, (SessionCapabilities)capabilities);
    }

    private static Frame ParseControlFrame(byte[] frameBytes, byte expectedType)
    {
        if (frameBytes is null || frameBytes.Length < Frames.HEADER_SIZE ||
            frameBytes[2] != Frames.VERSION)
            throw new FrameException("FRAME_REJECTED");
        var (frame, consumed) = FrameCodec.ParseFrame(frameBytes);
        if (consumed != frameBytes.Length || frame.FrameType != expectedType ||
            frame.StreamId != ControlStreamId || frame.Flags != 0)
            throw new FrameException("FRAME_REJECTED");
        return frame;
    }

    /// <summary>Encode hello ack into canonical wire bytes.</summary>
    public static byte[] EncodeHelloAck(HelloPayload h)
    {
        var rented = ArrayPool<byte>.Shared.Rent(48);
        try
        {
            var w = new Writer(rented);
            w.B128(h.NodeId).U32((uint)h.Epoch).U32((uint)h.Capabilities);
            return FrameCodec.BuildFrame(TypeHelloAck, ControlStreamId, 2, w.WrittenSpan);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Encode alias map into canonical wire bytes.</summary>
    public static byte[] EncodeAliasMap(IReadOnlyList<AliasMapEntry> entries)
    {
        int est = 8 + entries.Count * 24;
        var rented = ArrayPool<byte>.Shared.Rent(est);
        try
        {
            var w = new Writer(rented);
            w.Varint((ulong)entries.Count);
            foreach (var e in entries)
                w.U16(e.Alias).B128(e.NodeId).U32((uint)e.Epoch);
            return FrameCodec.BuildFrame(TypeAliasMap, ControlStreamId, 3, w.WrittenSpan);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Decode alias map from canonical wire bytes.</summary>
    public static List<AliasMapEntry> DecodeAliasMap(byte[] frameBytes)
    {
        var frame = ParseControlFrame(frameBytes, TypeAliasMap);
        var r = new Reader(frame.Payload);
        ulong count = r.Varint();
        // Each entry is exactly 22 bytes: alias u16, nodeId b128, epoch u32.
        if (count > ushort.MaxValue || count > (ulong)(r.Remaining / 22))
            throw new FrameException("FRAME_REJECTED");
        int n = (int)count;
        var list = new List<AliasMapEntry>(n);
        var aliases = new HashSet<ushort>();
        var nodes = new HashSet<NodeId>();
        for (int i = 0; i < n; i++)
        {
            ushort alias = r.U16();
            var nodeId = NodeId.From(r.B128());
            uint epoch = r.U32();
            if (alias == 0 || epoch > int.MaxValue ||
                !aliases.Add(alias) || !nodes.Add(nodeId))
                throw new FrameException("FRAME_REJECTED");
            list.Add(new AliasMapEntry(alias, nodeId, (int)epoch));
        }
        if (r.Remaining != 0)
            throw new FrameException("FRAME_REJECTED");
        return list;
    }

    /// <summary>Encode error into canonical wire bytes.</summary>
    public static byte[] EncodeError(string codeName, string? detail = null)
    {
        ushort code = Frames.ErrorCodes.TryGetValue(codeName, out var c) ? c : (ushort)0;
        int est = 16 + (detail is null ? 0 : System.Text.Encoding.UTF8.GetByteCount(detail) + 5);
        var rented = ArrayPool<byte>.Shared.Rent(est);
        try
        {
            var w = new Writer(rented).U16(code);
            if (detail is not null)
                w.Bytes(System.Text.Encoding.UTF8.GetBytes(detail));
            return FrameCodec.BuildFrame(TypeError, ControlStreamId, 0, w.WrittenSpan);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Apply a decoded ALIAS_MAP onto <paramref name="local"/> for the session with
    /// <paramref name="peerNodeId"/> (symmetric registration, errata E-11).
    /// </summary>
    public static void ApplyAliasMap(Node local, NodeId peerNodeId, IReadOnlyList<AliasMapEntry> entries)
    {
        local.ApplyAliasMapAtomic(peerNodeId, entries);
    }

    /// <summary>
    /// Build the ALIAS_MAP this node would advertise: self + peer + durable third parties
    /// (same deterministic assignment as <see cref="Transport.MakeSession"/>).
    /// </summary>
    public static List<AliasMapEntry> BuildAliasMapTable(Node local, NodeId peerNodeId, int peerEpoch,
        int selfAlias = 1, int peerAlias = 2)
    {
        var authors = new Dictionary<NodeId, int>()
        {
            [local.NodeId] = local.Epoch,
            [peerNodeId] = peerEpoch,
        };
        foreach (var (nid, ep) in local.DurableAuthors)
            if (!authors.ContainsKey(nid)) authors[nid] = ep;

        var ordered = authors.OrderBy(kv => kv.Key.ToString()).ToList();
        var used = new HashSet<int> { selfAlias, peerAlias };
        var assigned = new Dictionary<NodeId, int>()
        {
            [local.NodeId] = selfAlias,
            [peerNodeId] = peerAlias,
        };
        int next = 1;
        foreach (var kv in ordered)
        {
            if (assigned.ContainsKey(kv.Key)) continue;
            while (used.Contains(next)) next++;
            assigned[kv.Key] = next;
            used.Add(next);
        }

        var result = new List<AliasMapEntry>(ordered.Count);
        foreach (var kv in ordered)
            result.Add(new AliasMapEntry((ushort)assigned[kv.Key], kv.Key, kv.Value));
        return result;
    }

    /// <summary>
    /// Two-node in-process handshake over <see cref="ISyncTransport"/> pairs:
    /// HELLO ↔ HELLO_ACK, then mutual ALIAS_MAP, then register session epochs.
    /// Does not replace <see cref="Transport.MakeSession"/> for unit tests; this is the
    /// byte-level path production hosts should use.
    /// </summary>
    public static async Task HandshakeAsync(
        Node local, ISyncTransport toPeer,
        Node remote, ISyncTransport toLocal,
        SessionCapabilities caps = SessionCapabilities.None,
        CancellationToken ct = default)
    {
        await toPeer.SendAsync(EncodeHello(new HelloPayload(local.NodeId, local.Epoch, caps)), ct)
            .ConfigureAwait(false);
        var helloAb = await toLocal.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("peer closed during HELLO");
        var remoteHello = DecodeHello(helloAb);

        await toLocal.SendAsync(EncodeHelloAck(new HelloPayload(remote.NodeId, remote.Epoch, caps)), ct)
            .ConfigureAwait(false);
        var ackBa = await toPeer.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("peer closed during HELLO_ACK");
        _ = DecodeHelloAck(ackBa);

        var table = BuildAliasMapTable(local, remote.NodeId, remote.Epoch);
        var mapFrame = EncodeAliasMap(table);
        await toPeer.SendAsync(mapFrame, ct).ConfigureAwait(false);
        await toLocal.SendAsync(mapFrame, ct).ConfigureAwait(false);

        var mapOnLocal = await toPeer.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("peer closed during ALIAS_MAP");
        var mapOnRemote = await toLocal.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("peer closed during ALIAS_MAP");
        var localMap = DecodeAliasMap(mapOnLocal);
        var remoteMap = DecodeAliasMap(mapOnRemote);

        // Both complete maps are available: atomically begin the new session on
        // each endpoint before publishing either alias table.
        local.ResetSession(remote.NodeId);
        remote.ResetSession(local.NodeId);
        remote.RegisterSessionEpoch(local.NodeId, local.Epoch);
        local.RegisterSessionEpoch(remote.NodeId, remoteHello.Epoch);
        ApplyAliasMap(local, remote.NodeId, localMap);
        ApplyAliasMap(remote, local.NodeId, remoteMap);

        local.PeersByNid ??= new Dictionary<NodeId, Node>();
        remote.PeersByNid ??= new Dictionary<NodeId, Node>();
        local.PeersByNid[remote.NodeId] = remote;
        remote.PeersByNid[local.NodeId] = local;
    }
}
