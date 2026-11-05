using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace BinSync;

/// <summary>Transport-independent BIN-SYNC session over any complete-packet channel.</summary>
public partial class SyncSession : IAsyncDisposable
{
    protected readonly ISyncTransport _transport;
    private readonly bool _ownsTransport;
    private IDisposable? _sessionLease;
    private int _disposed;
    /// <summary>Remote peer identity established by HELLO.</summary>
    public NodeId PeerNodeId { get; }
    /// <summary>Peer incarnation epoch established by HELLO.</summary>
    public int PeerEpoch { get; }
    /// <summary>Capabilities advertised by the peer.</summary>
    public SessionCapabilities PeerCapabilities { get; }

    protected SyncSession(ISyncTransport transport, HelloPayload peer, bool ownsTransport, IDisposable sessionLease)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _ownsTransport = ownsTransport;
        _sessionLease = sessionLease ?? throw new ArgumentNullException(nameof(sessionLease));
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
        IDisposable? lease = null;
        try
        {
            EnsureTransportCapabilities(transport);
            var handshake = await InitiateAsync(local, transport, caps, ct).ConfigureAwait(false);
            lease = handshake.Lease;
            var session = new SyncSession(transport, handshake.Peer, ownsTransport, lease);
            lease = null;
            return session;
        }
        catch
        {
            lease?.Dispose();
            if (ownsTransport) await transport.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Establish a session as acceptor over a caller-provided channel.</summary>
    public static async Task<SyncSession> AcceptAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps = SessionCapabilities.None, bool ownsTransport = true, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(transport);
        IDisposable? lease = null;
        try
        {
            EnsureTransportCapabilities(transport);
            var handshake = await AcceptHandshakeAsync(local, transport, caps, ct).ConfigureAwait(false);
            lease = handshake.Lease;
            var session = new SyncSession(transport, handshake.Peer, ownsTransport, lease);
            lease = null;
            return session;
        }
        catch
        {
            lease?.Dispose();
            if (ownsTransport) await transport.DisposeAsync().ConfigureAwait(false);
            throw;
        }
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
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try
        {
            if (_ownsTransport) await _transport.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Exchange(ref _sessionLease, null)?.Dispose();
        }
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

    internal static async Task<SessionHandshakeResult> InitiateAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps, CancellationToken ct)
    {
        await transport.SendAsync(SessionWire.EncodeHello(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
        var raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO_ACK");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SessionWire.TypeHelloAck) throw new FrameException("FRAME_REJECTED");
        var peer = SessionWire.DecodeHelloAck(raw);
        EnsureHandshakeRole(local.NodeId, peer.NodeId, initiator: true);
        var lease = await PeerSessionLease.AcquireAsync(local, peer.NodeId, ct).ConfigureAwait(false);
        try
        {
        var aliases = SessionWire.BuildAliasMapTable(local, peer.NodeId, peer.Epoch, selfAlias: 1, peerAlias: 2);
        await transport.SendAsync(SessionWire.EncodeAliasMap(aliases), ct).ConfigureAwait(false);
        raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
        var remoteAliases = SessionWire.DecodeAliasMap(raw);

        // A completed handshake starts a fresh session. Delay reset until the peer's
        // complete map has been decoded, so a failed/incomplete handshake preserves
        // the previous live session state.
        // The acceptor's returned map is the negotiated union; commit it atomically.
        local.CommitSession(peer.NodeId, peer.Epoch, remoteAliases);
        return new SessionHandshakeResult(peer, lease);
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }

    internal static async Task<SessionHandshakeResult> AcceptHandshakeAsync(Node local, ISyncTransport transport,
        SessionCapabilities caps, CancellationToken ct)
    {
        var raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during HELLO");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != SessionWire.TypeHello) throw new FrameException("FRAME_REJECTED");
        var peer = SessionWire.DecodeHello(raw);
        EnsureHandshakeRole(local.NodeId, peer.NodeId, initiator: false);
        var lease = await PeerSessionLease.AcquireAsync(local, peer.NodeId, ct).ConfigureAwait(false);
        try
        {
        await transport.SendAsync(SessionWire.EncodeHelloAck(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
        raw = await transport.ReceiveAsync(ct).ConfigureAwait(false) ?? throw new IOException("peer closed during ALIAS_MAP");
        var merged = MergeAliasMap(local, peer.NodeId, peer.Epoch, SessionWire.DecodeAliasMap(raw));
        await transport.SendAsync(SessionWire.EncodeAliasMap(merged), ct).ConfigureAwait(false);

        // Commit the new session only after the complete peer map was decoded and
        // our response was sent. Failed handshakes leave the previous session intact.
        local.CommitSession(peer.NodeId, peer.Epoch, merged);
        return new SessionHandshakeResult(peer, lease);
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }

    private static void EnsureHandshakeRole(NodeId local, NodeId peer, bool initiator)
    {
        Span<byte> localBytes = stackalloc byte[16];
        Span<byte> peerBytes = stackalloc byte[16];
        local.WriteTo(localBytes);
        peer.WriteTo(peerBytes);
        int order = localBytes.SequenceCompareTo(peerBytes);
        if (order == 0 || (initiator ? order >= 0 : order <= 0))
            throw new FrameException("FRAME_REJECTED");
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


internal readonly record struct SessionHandshakeResult(HelloPayload Peer, IDisposable Lease);

/// <summary>Reserves the peer-scoped alias/session slot for the lifetime of a SyncSession.</summary>
internal static class PeerSessionLease
{
    private static readonly ConditionalWeakTable<Node, ConcurrentDictionary<NodeId, SemaphoreSlim>> Gates = new();

    internal static async ValueTask<IDisposable> AcquireAsync(Node local, NodeId peer, CancellationToken ct)
    {
        var perPeer = Gates.GetValue(local, static _ => new ConcurrentDictionary<NodeId, SemaphoreSlim>());
        var gate = perPeer.GetOrAdd(peer, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        return new Lease(gate);
    }

    private sealed class Lease : IDisposable
    {
        private SemaphoreSlim? _gate;
        internal Lease(SemaphoreSlim gate) => _gate = gate;
        public void Dispose() => Interlocked.Exchange(ref _gate, null)?.Release();
    }
}
