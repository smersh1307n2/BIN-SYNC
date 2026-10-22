// TCP accept/connect host (§3.2) over StreamSyncTransport.
// Session bootstrap: initiator sends HELLO, acceptor answers HELLO_ACK,
// initiator publishes the canonical ALIAS_MAP (self=alias 1, peer=alias 2),
// acceptor may append its own durable third parties and returns the merged map.
using System.Net;
using System.Net.Sockets;

namespace BinSync;

public sealed class TcpSyncListener : IAsyncDisposable
{
    private readonly TcpListener _listener;

    public int Port { get; }

    private TcpSyncListener(TcpListener listener)
    {
        _listener = listener;
        Port = ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    public static TcpSyncListener Start(int port = 0, IPAddress? address = null)
    {
        var listener = new TcpListener(address ?? IPAddress.Loopback, port);
        listener.Start();
        return new TcpSyncListener(listener);
    }

    public async Task<TcpSyncSession> AcceptAsync(
        Node local, SessionCapabilities caps = SessionCapabilities.None, CancellationToken ct = default)
    {
        var client = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
        client.NoDelay = true;
        try
        {
            return await TcpSyncSession.CompleteAcceptAsync(local, client, caps, ct).ConfigureAwait(false);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        _listener.Stop();
        return ValueTask.CompletedTask;
    }
}

/// <summary>One accepted or connected TCP session after HELLO / ALIAS_MAP.</summary>
public sealed class TcpSyncSession : IAsyncDisposable
{
    private readonly TcpClient _client;
    private readonly StreamSyncTransport _transport;

    public byte[] PeerNodeId { get; }
    public int PeerEpoch { get; }
    public SessionCapabilities PeerCapabilities { get; }

    private TcpSyncSession(TcpClient client, StreamSyncTransport transport, HelloPayload peer)
    {
        _client = client;
        _transport = transport;
        PeerNodeId = peer.NodeId;
        PeerEpoch = peer.Epoch;
        PeerCapabilities = peer.Capabilities;
        _transport.PeerNodeId = peer.NodeId;
    }

    public static async Task<TcpSyncSession> ConnectAsync(
        Node local, string host, int port,
        SessionCapabilities caps = SessionCapabilities.None, CancellationToken ct = default)
    {
        var client = new TcpClient { NoDelay = true };
        try
        {
            await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
            var transport = new StreamSyncTransport(client.GetStream(), ownsStream: false);
            var peer = await InitiateAsync(local, transport, caps, ct).ConfigureAwait(false);
            return new TcpSyncSession(client, transport, peer);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    internal static async Task<TcpSyncSession> CompleteAcceptAsync(
        Node local, TcpClient client, SessionCapabilities caps, CancellationToken ct)
    {
        var transport = new StreamSyncTransport(client.GetStream(), ownsStream: false);
        var peer = await AcceptHandshakeAsync(local, transport, caps, ct).ConfigureAwait(false);
        return new TcpSyncSession(client, transport, peer);
    }

    /// <summary>
    /// Push one pending delta to the peer and wait for its ACK.
    /// Returns false when the local outbox has nothing for this peer.
    /// </summary>
    public async Task<bool> PushBatchAsync(
        Node sender, int namespaceId, int schemaVersion = 1, CancellationToken ct = default)
    {
        var batch = sender.CreateDeltaBatch(PeerNodeId, namespaceId, schemaVersion: schemaVersion);
        if (batch is null) return false;
        sender.SignBatchFor(batch, PeerNodeId);
        var payload = sender.EncodeBatchPayload(batch);
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        await _transport.SendAsync(FrameCodec.BuildFrame(0x09, streamId, 1, payload), ct).ConfigureAwait(false);
        var ackRaw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed before ACK");
        var (ackFrame, _) = FrameCodec.ParseFrame(ackRaw);
        if (ackFrame.FrameType != 0x0B)
            throw new FrameException("FRAME_REJECTED");
        var (bid, seq, author) = Transport.DecodeAck(ackFrame.Payload);
        if (!bid.AsSpan().SequenceEqual(batch.BatchId))
            throw new InvalidOperationException("ACK batch id mismatch");
        sender.AdvanceStream(PeerNodeId, namespaceId, batch);
        sender.ProcessAck(PeerNodeId, namespaceId, seq, authorStream: author);
        return true;
    }

    /// <summary>Read one DELTA_BATCH, apply it, and send the ACK.</summary>
    public async Task<BatchResult> ReceiveBatchAsync(Node receiver, CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != 0x09)
            throw new FrameException("FRAME_REJECTED");
        var decoded = receiver.DecodeBatchPayload(frame.Payload);
        var result = receiver.ApplyBatch(decoded, PeerNodeId);
        var ack = FrameCodec.BuildFrame(
            0x0B, 0x0002_0000u | (uint)decoded.NamespaceId, 2,
            Transport.EncodeAck(decoded.BatchId, decoded.CursorEnd, decoded.DurableAuthor));
        await _transport.SendAsync(ack, ct).ConfigureAwait(false);
        return result;
    }

    public async ValueTask DisposeAsync()
    {
        await _transport.DisposeAsync().ConfigureAwait(false);
        _client.Dispose();
    }

    private static async Task<HelloPayload> InitiateAsync(
        Node local, StreamSyncTransport transport, SessionCapabilities caps, CancellationToken ct)
    {
        await transport.SendAsync(
            SessionWire.EncodeHello(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);
        var ackRaw = await transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during HELLO_ACK");
        var (frame, _) = FrameCodec.ParseFrame(ackRaw);
        if (frame.FrameType != SessionWire.TypeHelloAck)
            throw new FrameException("FRAME_REJECTED");
        var peer = SessionWire.DecodeHello(ackRaw);
        local.RegisterSessionEpoch(peer.NodeId, peer.Epoch);

        var mine = SessionWire.BuildAliasMapTable(local, peer.NodeId, peer.Epoch, selfAlias: 1, peerAlias: 2);
        SessionWire.ApplyAliasMap(local, peer.NodeId, mine);
        await transport.SendAsync(SessionWire.EncodeAliasMap(mine), ct).ConfigureAwait(false);

        var mergedRaw = await transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during ALIAS_MAP");
        SessionWire.ApplyAliasMap(local, peer.NodeId, SessionWire.DecodeAliasMap(mergedRaw));
        return peer;
    }

    private static async Task<HelloPayload> AcceptHandshakeAsync(
        Node local, StreamSyncTransport transport, SessionCapabilities caps, CancellationToken ct)
    {
        var helloRaw = await transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during HELLO");
        var (helloFrame, _) = FrameCodec.ParseFrame(helloRaw);
        if (helloFrame.FrameType != SessionWire.TypeHello)
            throw new FrameException("FRAME_REJECTED");
        var hello = SessionWire.DecodeHello(helloRaw);
        local.RegisterSessionEpoch(hello.NodeId, hello.Epoch);
        await transport.SendAsync(
            SessionWire.EncodeHelloAck(new HelloPayload(local.NodeId, local.Epoch, caps)), ct).ConfigureAwait(false);

        var mapRaw = await transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during ALIAS_MAP");
        var remoteMap = SessionWire.DecodeAliasMap(mapRaw);
        var merged = MergeAliasMap(local, hello.NodeId, hello.Epoch, remoteMap);
        SessionWire.ApplyAliasMap(local, hello.NodeId, merged);
        await transport.SendAsync(SessionWire.EncodeAliasMap(merged), ct).ConfigureAwait(false);
        return hello;
    }

    /// <summary>
    /// Keep the initiator's alias numbers. Append acceptor-only durable authors
    /// (sorted by nodeId) into the next free alias slots.
    /// </summary>
    internal static List<AliasMapEntry> MergeAliasMap(
        Node local, byte[] peerNodeId, int peerEpoch, IReadOnlyList<AliasMapEntry> remote)
    {
        var result = new List<AliasMapEntry>(remote);
        var known = new HashSet<string>(result.Select(e => Convert.ToHexString(e.NodeId)));
        var extras = new List<(byte[] Id, int Epoch)>();
        void Consider(byte[] id, int epoch)
        {
            var hex = Convert.ToHexString(id);
            if (!known.Add(hex)) return;
            extras.Add((id, epoch));
        }
        Consider(local.NodeId, local.Epoch);
        Consider(peerNodeId, peerEpoch);
        foreach (var (nid, ep) in local.DurableAuthors)
            Consider(nid, ep);
        extras.Sort((a, b) => string.CompareOrdinal(Convert.ToHexString(a.Id), Convert.ToHexString(b.Id)));
        int next = 1;
        foreach (var extra in extras)
        {
            while (result.Any(e => e.Alias == next)) next++;
            result.Add(new AliasMapEntry((ushort)next, extra.Id, extra.Epoch));
            next++;
        }
        return result;
    }
}
