using System.Buffers;

namespace BinSync;

/// <summary>Transport-independent BIN-SYNC session over any complete-packet channel.</summary>
public partial class SyncSession : IAsyncDisposable
{
    protected readonly ISyncTransport _transport;
    private readonly bool _ownsTransport;
    /// <summary>Remote peer identity established by HELLO.</summary>
    public NodeId PeerNodeId { get; }
    /// <summary>Peer incarnation epoch established by HELLO.</summary>
    public int PeerEpoch { get; }
    /// <summary>Capabilities advertised by the peer.</summary>
    public SessionCapabilities PeerCapabilities { get; }

    protected SyncSession(ISyncTransport transport, HelloPayload peer, bool ownsTransport)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _ownsTransport = ownsTransport;
        PeerNodeId = peer.NodeId;
        PeerEpoch = peer.Epoch;
        PeerCapabilities = peer.Capabilities;
        if (transport is IPeerAwareSyncTransport aware) aware.PeerNodeId = peer.NodeId;
    }

    /// <summary>Establish a session as initiator over a caller-provided channel.</summary>
    public static async Task<SyncSession> ConnectAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps = SessionCapabilities.None, bool ownsTransport = true, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(transport);
        try { EnsureTransportCapabilities(transport); return new SyncSession(transport, await InitiateAsync(local, transport, caps, ct).ConfigureAwait(false), ownsTransport); }
        catch { if (ownsTransport) await transport.DisposeAsync().ConfigureAwait(false); throw; }
    }

    /// <summary>Establish a session as acceptor over a caller-provided channel.</summary>
    public static async Task<SyncSession> AcceptAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps = SessionCapabilities.None, bool ownsTransport = true, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(transport);
        try { EnsureTransportCapabilities(transport); return new SyncSession(transport, await AcceptHandshakeAsync(local, transport, caps, ct).ConfigureAwait(false), ownsTransport); }
        catch { if (ownsTransport) await transport.DisposeAsync().ConfigureAwait(false); throw; }
    }

    /// <summary>Push one pending delta and wait for its ACK.</summary>
    public async Task<bool> PushBatchAsync(Node sender, int namespaceId, int schemaVersion = 1, CancellationToken ct = default)
    {
        var batch = sender.CreateDeltaBatch(PeerNodeId, namespaceId, schemaVersion: schemaVersion);
        if (batch is null) return false;
        sender.SignBatchFor(batch, PeerNodeId);
        var payload = sender.EncodeBatchPayload(batch);
        await SendFrameAsync(0x09, 0x0001_0000u | (uint)namespaceId, 1, payload, ct).ConfigureAwait(false);
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed before ACK");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != 0x0B) throw new FrameException("FRAME_REJECTED");
        var (id, seq, author) = Transport.DecodeAck(frame.Payload);
        if (!batch.BatchId.EqualsBytes(id)) throw new InvalidOperationException("ACK batch id mismatch");
        if (seq != batch.CursorEnd || author is null || NodeId.From(author) != batch.DurableAuthor)
            throw new SyncException("ACK_OUT_OF_RANGE", batch.BatchId);
        sender.AdvanceStream(PeerNodeId, namespaceId, batch);
        sender.ProcessAck(PeerNodeId, namespaceId, seq, authorStream: NodeId.From(author));
        return true;
    }

    /// <summary>Receive one DELTA_BATCH, apply it and return the ACK.</summary>
    public async Task<BatchResult> ReceiveBatchAsync(Node receiver, CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != 0x09) throw new FrameException("FRAME_REJECTED");
        var result = receiver.ApplyBatchPayload(frame.Payload, PeerNodeId);
        var ack = Transport.EncodeAck(result.BatchId, result.CursorEnd, result.DurableAuthor);
        await SendFrameAsync(0x0B, 0x0002_0000u | (uint)result.NamespaceId, 2, ack, ct).ConfigureAwait(false);
        return result;
    }

    /// <summary>Build and send one framed packet through the configured channel.</summary>
    protected async Task SendFrameAsync(byte type, uint streamId, ulong seq, ReadOnlyMemory<byte> payload,
        CancellationToken ct, byte flags = 0)
    {
        int size = Frames.HEADER_SIZE + payload.Length;
        var rented = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            int length = FrameCodec.BuildFrame(type, streamId, seq, payload.Span, rented.AsSpan(0, size), flags);
            await _transport.SendAsync(rented.AsMemory(0, length), ct).ConfigureAwait(false);
        }
        finally { ArrayPool<byte>.Shared.Return(rented); }
    }

    /// <summary>Dispose the underlying channel when this session owns it.</summary>
    public virtual async ValueTask DisposeAsync()
    {
        if (_ownsTransport) await _transport.DisposeAsync().ConfigureAwait(false);
    }

    private static void EnsureTransportCapabilities(ISyncTransport transport)
    {
        const SyncTransportCapabilities required =
            SyncTransportCapabilities.ReliableDelivery | SyncTransportCapabilities.OrderedDelivery;
        if ((transport.Capabilities & required) != required)
            throw new ArgumentException(
                "BIN-SYNC sessions require reliable, ordered packet delivery. Wrap datagram transports in a reliable ordered adapter.",
                nameof(transport));
    }

    internal static async Task<HelloPayload> InitiateAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps, CancellationToken ct)
    {
        await transport.SendAsync(SessionWire.EncodeHello(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
        var raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO_ACK");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SessionWire.TypeHelloAck) throw new FrameException("FRAME_REJECTED");
        var peer = SessionWire.DecodeHelloAck(raw);
        local.RegisterSessionEpoch(peer.NodeId, peer.Epoch);
        var aliases = SessionWire.BuildAliasMapTable(local, peer.NodeId, peer.Epoch, selfAlias: 1, peerAlias: 2);
        SessionWire.ApplyAliasMap(local, peer.NodeId, aliases);
        await transport.SendAsync(SessionWire.EncodeAliasMap(aliases), ct).ConfigureAwait(false);
        raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
        SessionWire.ApplyAliasMap(local, peer.NodeId, SessionWire.DecodeAliasMap(raw));
        return peer;
    }

    internal static async Task<HelloPayload> AcceptHandshakeAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps, CancellationToken ct)
    {
        var raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SessionWire.TypeHello) throw new FrameException("FRAME_REJECTED");
        var peer = SessionWire.DecodeHello(raw);
        local.RegisterSessionEpoch(peer.NodeId, peer.Epoch);
        await transport.SendAsync(SessionWire.EncodeHelloAck(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
        raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
        var merged = MergeAliasMap(local, peer.NodeId, peer.Epoch, SessionWire.DecodeAliasMap(raw));
        SessionWire.ApplyAliasMap(local, peer.NodeId, merged);
        await transport.SendAsync(SessionWire.EncodeAliasMap(merged), ct).ConfigureAwait(false);
        return peer;
    }

    internal static List<AliasMapEntry> MergeAliasMap(Node local, NodeId peerNodeId, int peerEpoch,
        IReadOnlyList<AliasMapEntry> remote)
    {
        // HELLO establishes the current incarnation of both session endpoints.
        // An ALIAS_MAP may add third parties, but it must not silently downgrade or
        // replace either endpoint's identity/epoch binding.
        foreach (var entry in remote)
        {
            if ((entry.NodeId == local.NodeId && entry.Epoch != local.Epoch) ||
                (entry.NodeId == peerNodeId && entry.Epoch != peerEpoch))
                throw new FrameException("FRAME_REJECTED");
        }

        var result = new List<AliasMapEntry>(remote);
        var known = new HashSet<NodeId>(result.Select(x => x.NodeId));
        var extras = new List<(NodeId Id, int Epoch)>();
        void Add(NodeId id, int epoch) { if (known.Add(id)) extras.Add((id, epoch)); }
        Add(local.NodeId, local.Epoch); Add(peerNodeId, peerEpoch);
        foreach (var (id, epoch) in local.DurableAuthors) Add(id, epoch);
        extras.Sort(static (a, b) =>
        {
            Span<byte> left = stackalloc byte[16]; Span<byte> right = stackalloc byte[16];
            a.Id.WriteTo(left); b.Id.WriteTo(right); return left.SequenceCompareTo(right);
        });
        int next = 1;
        foreach (var item in extras)
        {
            while (result.Any(x => x.Alias == next)) next++;
            result.Add(new AliasMapEntry((ushort)next, item.Id, item.Epoch)); next++;
        }
        return result;
    }
}
