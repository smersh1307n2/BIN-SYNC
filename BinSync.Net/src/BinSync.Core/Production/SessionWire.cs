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

public readonly record struct HelloPayload(
    byte[] NodeId, int Epoch, SessionCapabilities Capabilities);

public readonly record struct AliasMapEntry(
    ushort Alias, byte[] NodeId, int Epoch);

/// <summary>
/// Encode / decode session-control payloads. Frame types: HELLO=0x01, HELLO_ACK=0x02,
/// ALIAS_MAP=0x03, ERROR=0x0D. Control plane uses <c>streamId = 0</c> (§3.7).
/// </summary>
public static class SessionWire
{
    public const byte TypeHello = 0x01;
    public const byte TypeHelloAck = 0x02;
    public const byte TypeAliasMap = 0x03;
    public const byte TypeError = 0x0D;
    public const uint ControlStreamId = 0;

    public static byte[] EncodeHello(HelloPayload h)
    {
        var w = new Writer();
        w.B128(h.NodeId).U32((uint)h.Epoch).U32((uint)h.Capabilities);
        return FrameCodec.BuildFrame(TypeHello, ControlStreamId, 1, w.Value());
    }

    public static HelloPayload DecodeHello(byte[] frameBytes)
    {
        var (frame, _) = FrameCodec.ParseFrame(frameBytes);
        if (frame.FrameType is not TypeHello and not TypeHelloAck)
            throw new FrameException("FRAME_REJECTED");
        var r = new Reader(frame.Payload);
        return new HelloPayload(r.B128(), (int)r.U32(), (SessionCapabilities)r.U32());
    }

    public static byte[] EncodeHelloAck(HelloPayload h)
    {
        var w = new Writer();
        w.B128(h.NodeId).U32((uint)h.Epoch).U32((uint)h.Capabilities);
        return FrameCodec.BuildFrame(TypeHelloAck, ControlStreamId, 2, w.Value());
    }

    public static byte[] EncodeAliasMap(IReadOnlyList<AliasMapEntry> entries)
    {
        var w = new Writer();
        w.Varint((ulong)entries.Count);
        foreach (var e in entries)
            w.U16(e.Alias).B128(e.NodeId).U32((uint)e.Epoch);
        return FrameCodec.BuildFrame(TypeAliasMap, ControlStreamId, 3, w.Value());
    }

    public static List<AliasMapEntry> DecodeAliasMap(byte[] frameBytes)
    {
        var (frame, _) = FrameCodec.ParseFrame(frameBytes);
        if (frame.FrameType != TypeAliasMap)
            throw new FrameException("FRAME_REJECTED");
        var r = new Reader(frame.Payload);
        int n = (int)r.Varint();
        var list = new List<AliasMapEntry>(n);
        for (int i = 0; i < n; i++)
            list.Add(new AliasMapEntry(r.U16(), r.B128(), (int)r.U32()));
        return list;
    }

    public static byte[] EncodeError(string codeName, string? detail = null)
    {
        ushort code = Frames.ErrorCodes.TryGetValue(codeName, out var c) ? c : (ushort)0;
        var w = new Writer().U16(code);
        if (detail is not null)
            w.Bytes(System.Text.Encoding.UTF8.GetBytes(detail));
        return FrameCodec.BuildFrame(TypeError, ControlStreamId, 0, w.Value());
    }

    /// <summary>
    /// Apply a decoded ALIAS_MAP onto <paramref name="local"/> for the session with
    /// <paramref name="peerNodeId"/> (symmetric registration, errata E-11).
    /// </summary>
    public static void ApplyAliasMap(Node local, byte[] peerNodeId, IReadOnlyList<AliasMapEntry> entries)
    {
        foreach (var e in entries)
            local.RegisterAlias(peerNodeId, e.Alias, e.NodeId, e.Epoch);
    }

    /// <summary>
    /// Build the ALIAS_MAP this node would advertise: self + peer + durable third parties
    /// (same deterministic assignment as <see cref="Transport.MakeSession"/>).
    /// </summary>
    public static List<AliasMapEntry> BuildAliasMapTable(Node local, byte[] peerNodeId, int peerEpoch,
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
            result.Add(new AliasMapEntry((ushort)assigned[kv.Key], kv.Key.ToArray(), kv.Value));
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
        remote.RegisterSessionEpoch(local.NodeId, local.Epoch);
        local.RegisterSessionEpoch(remote.NodeId, remoteHello.Epoch);

        await toLocal.SendAsync(EncodeHelloAck(new HelloPayload(remote.NodeId, remote.Epoch, caps)), ct)
            .ConfigureAwait(false);
        var ackBa = await toPeer.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("peer closed during HELLO_ACK");
        _ = DecodeHello(ackBa);

        var table = BuildAliasMapTable(local, remote.NodeId, remote.Epoch);
        var mapFrame = EncodeAliasMap(table);
        await toPeer.SendAsync(mapFrame, ct).ConfigureAwait(false);
        await toLocal.SendAsync(mapFrame, ct).ConfigureAwait(false);

        var mapOnLocal = await toPeer.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("peer closed during ALIAS_MAP");
        var mapOnRemote = await toLocal.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("peer closed during ALIAS_MAP");
        ApplyAliasMap(local, remote.NodeId, DecodeAliasMap(mapOnLocal));
        ApplyAliasMap(remote, local.NodeId, DecodeAliasMap(mapOnRemote));

        local.PeersByNid ??= new Dictionary<NodeId, Node>();
        remote.PeersByNid ??= new Dictionary<NodeId, Node>();
        local.PeersByNid[remote.NodeId] = remote;
        remote.PeersByNid[local.NodeId] = local;
    }
}
