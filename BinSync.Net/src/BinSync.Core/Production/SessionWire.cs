using System.Buffers;
using System.Buffers.Binary;
// Wire payloads for session bootstrap: HELLO, HELLO_ACK, ALIAS_MAP (§12.7+, §4.3–4.4).
namespace BinSync;

/// <summary>Capability bits exchanged in HELLO (subset of §12 capabilities).</summary>
[Flags]
public enum SessionCapabilities : uint
{
    None = 0,
    /// <summary>§12.24.4 — FieldWrite carries full 16-byte author instead of origin u16.</summary>
    FullAuthorId = 1 << 0,
    /// <summary>§3.6.3 — peer will honor WINDOW_UPDATE on this transport.</summary>
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
/// ALIAS_MAP=0x03, ERROR=0x0D, WINDOW_UPDATE=0x0E. Control plane uses <c>streamId = 0</c> (§3.7).
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

    /// <summary>Encode a positive WINDOW_UPDATE credit amount as one LE u32.</summary>
    public static byte[] EncodeWindowUpdate(uint credits)
    {
        if (credits == 0) throw new ArgumentOutOfRangeException(nameof(credits));
        byte[] payload = new byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, credits);
        return payload;
    }

    /// <summary>Decode the canonical four-byte positive WINDOW_UPDATE payload.</summary>
    public static bool TryDecodeWindowUpdate(ReadOnlySpan<byte> payload, out uint credits)
    {
        if (payload.Length != sizeof(uint))
        {
            credits = 0;
            return false;
        }
        credits = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        return credits != 0;
    }

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

    private const byte AliasMapUpdateMagic = 0xA7;
    private const byte AliasMapUpdateRequest = 1;
    private const byte AliasMapUpdateResponse = 2;

    internal readonly record struct AliasMapUpdate(
        uint RequestId, bool IsResponse, bool Accepted, AliasMapEntry Entry);

    /// <summary>
    /// Encode one established-session ALIAS_MAP proposal. The receiver applies the
    /// entry atomically and replies with the same request id and negotiated entry.
    /// </summary>
    internal static byte[] EncodeAliasMapUpdateRequest(uint requestId, AliasMapEntry entry)
        => EncodeAliasMapUpdate(requestId, AliasMapUpdateRequest, accepted: true, entry);

    internal static byte[] EncodeAliasMapUpdateResponse(
        uint requestId, bool accepted, AliasMapEntry entry = default)
        => EncodeAliasMapUpdate(requestId, AliasMapUpdateResponse, accepted, entry);

    private static byte[] EncodeAliasMapUpdate(
        uint requestId, byte kind, bool accepted, AliasMapEntry entry)
    {
        if (requestId == 0) throw new ArgumentOutOfRangeException(nameof(requestId));
        bool hasEntry = kind == AliasMapUpdateRequest || accepted;
        if (hasEntry && (entry.Alias == 0 || entry.NodeId.IsZero || entry.Epoch < 0))
            throw new ArgumentException("Invalid incremental alias binding.", nameof(entry));

        byte[] payload = new byte[6 + (kind == AliasMapUpdateResponse ? 1 : 0) + 1 + (hasEntry ? 22 : 0)];
        payload[0] = AliasMapUpdateMagic;
        payload[1] = kind;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(2), requestId);
        int offset = 6;
        if (kind == AliasMapUpdateResponse)
            payload[offset++] = accepted ? (byte)0 : (byte)1;
        payload[offset++] = hasEntry ? (byte)1 : (byte)0;
        if (hasEntry)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(offset), entry.Alias);
            offset += 2;
            entry.NodeId.WriteTo(payload.AsSpan(offset));
            offset += 16;
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(offset), checked((uint)entry.Epoch));
            offset += 4;
        }
        return FrameCodec.BuildFrame(TypeAliasMap, ControlStreamId, 3, payload);
    }

    /// <summary>Decode an established-session incremental ALIAS_MAP request or response.</summary>
    internal static AliasMapUpdate DecodeAliasMapUpdate(byte[] frameBytes)
    {
        var frame = ParseControlFrame(frameBytes, TypeAliasMap);
        ReadOnlySpan<byte> payload = frame.Payload;
        if (payload.Length < 8 || payload[0] != AliasMapUpdateMagic ||
            payload[1] is not (AliasMapUpdateRequest or AliasMapUpdateResponse))
            throw new FrameException("FRAME_REJECTED");

        byte kind = payload[1];
        uint requestId = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(2, 4));
        if (requestId == 0) throw new FrameException("FRAME_REJECTED");
        int offset = 6;
        bool accepted = true;
        if (kind == AliasMapUpdateResponse)
        {
            if (offset >= payload.Length || payload[offset] > 1)
                throw new FrameException("FRAME_REJECTED");
            accepted = payload[offset++] == 0;
        }

        if (offset >= payload.Length) throw new FrameException("FRAME_REJECTED");
        byte count = payload[offset++];
        if (count > 1 || (kind == AliasMapUpdateRequest && count != 1) ||
            (kind == AliasMapUpdateResponse && accepted != (count == 1)))
            throw new FrameException("FRAME_REJECTED");

        AliasMapEntry entry = default;
        if (count == 1)
        {
            if (payload.Length - offset != 22) throw new FrameException("FRAME_REJECTED");
            ushort alias = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(offset, 2));
            offset += 2;
            NodeId nodeId = NodeId.From(payload.Slice(offset, 16));
            offset += 16;
            uint epoch = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(offset, 4));
            if (alias == 0 || nodeId.IsZero || epoch > int.MaxValue)
                throw new FrameException("FRAME_REJECTED");
            entry = new AliasMapEntry(alias, nodeId, (int)epoch);
        }
        if (offset != payload.Length) throw new FrameException("FRAME_REJECTED");
        return new AliasMapUpdate(requestId, kind == AliasMapUpdateResponse, accepted, entry);
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
        foreach (var (nid, ep) in local.DurableAuthorsSnapshot())
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
    /// Two-node in-process handshake helper for loopback/test setups. Production
    /// transport hosts should use <see cref="SyncSession.EstablishAsync"/>.
    /// </summary>
    public static async Task HandshakeAsync(
        Node local, ISyncTransport toPeer,
        Node remote, ISyncTransport toLocal,
        SessionCapabilities caps = SessionCapabilities.None,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(remote);
        ArgumentNullException.ThrowIfNull(toPeer);
        ArgumentNullException.ThrowIfNull(toLocal);
        local.ValidateSessionEpoch(remote.NodeId, remote.Epoch);
        remote.ValidateSessionEpoch(local.NodeId, local.Epoch);
        SyncSession.EnsureHandshakeRole(local.NodeId, remote.NodeId, initiator: true);

        IDisposable? localLease = null;
        IDisposable? remoteLease = null;
        try
        {
            // Acquire in protocol-role order (lower NodeId first) to match the
            // transport-independent session path and prevent lock-order inversion.
            localLease = await PeerSessionLease.AcquireAsync(local, remote.NodeId, ct).ConfigureAwait(false);
            remoteLease = await PeerSessionLease.AcquireAsync(remote, local.NodeId, ct).ConfigureAwait(false);

            await toPeer.SendAsync(EncodeHello(new HelloPayload(local.NodeId, local.Epoch, caps)), ct)
                .ConfigureAwait(false);
            var helloBytes = await toLocal.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during HELLO");
            var remoteHello = DecodeHello(helloBytes);
            if (remoteHello.NodeId != local.NodeId || remoteHello.Epoch != local.Epoch)
                throw new FrameException("FRAME_REJECTED");

            await toLocal.SendAsync(EncodeHelloAck(new HelloPayload(remote.NodeId, remote.Epoch, caps)), ct)
                .ConfigureAwait(false);
            var ackBytes = await toPeer.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during HELLO_ACK");
            var ack = DecodeHelloAck(ackBytes);
            if (ack.NodeId != remote.NodeId || ack.Epoch != remote.Epoch)
                throw new FrameException("FRAME_REJECTED");

            var proposal = BuildAliasMapTable(local, remote.NodeId, remote.Epoch);
            await toPeer.SendAsync(EncodeAliasMap(proposal), ct).ConfigureAwait(false);
            var proposalBytes = await toLocal.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during ALIAS_MAP");
            var remoteProposal = DecodeAliasMap(proposalBytes);
            var negotiated = SyncSession.MergeAliasMap(remote, local.NodeId, local.Epoch, remoteProposal);

            await toLocal.SendAsync(EncodeAliasMap(negotiated), ct).ConfigureAwait(false);
            var negotiatedBytes = await toPeer.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during negotiated ALIAS_MAP");
            var localMap = DecodeAliasMap(negotiatedBytes);

            local.CommitSession(remote.NodeId, remoteHello.Epoch, localMap);
            remote.CommitSession(local.NodeId, local.Epoch, negotiated);
            local.RegisterLoopbackPeer(remote);
            remote.RegisterLoopbackPeer(local);
        }
        finally
        {
            remoteLease?.Dispose();
            localLease?.Dispose();
        }
    }
}
