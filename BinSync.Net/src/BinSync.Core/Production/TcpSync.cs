using System.Buffers;
// TCP accept/connect host (§3.2) over StreamSyncTransport, optional TLS (SslStream).
// Session bootstrap: initiator sends HELLO, acceptor answers HELLO_ACK,
// initiator publishes the canonical ALIAS_MAP (self=alias 1, peer=alias 2),
// acceptor may append its own durable third parties and returns the merged map.
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace BinSync;

/// <summary>TCP listener accepting BIN-SYNC sessions.</summary>
public sealed class TcpSyncListener : IAsyncDisposable
{
    private readonly TcpListener _listener;

    /// <summary>TCP port the listener is bound to.</summary>
    public int Port { get; }

    private TcpSyncListener(TcpListener listener)
    {
        _listener = listener;
        Port = ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    /// <summary>Start a TCP sync listener on the given port (0 = ephemeral).</summary>
    public static TcpSyncListener Start(int port = 0, IPAddress? address = null)
    {
        var listener = new TcpListener(address ?? IPAddress.Loopback, port);
        listener.Start();
        return new TcpSyncListener(listener);
    }

    /// <summary>
    /// Accept one TCP client. When <paramref name="serverCertificate"/> is set, perform
    /// a TLS handshake (server role) before the BinSync HELLO exchange.
    /// </summary>
    public async Task<TcpSyncSession> AcceptAsync(
        Node local,
        SessionCapabilities caps = SessionCapabilities.None,
        X509Certificate2? serverCertificate = null,
        CancellationToken ct = default)
    {
        var client = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
        client.NoDelay = true;
        try
        {
            return await TcpSyncSession.CompleteAcceptAsync(local, client, caps, serverCertificate, ct)
                .ConfigureAwait(false);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>Release unmanaged or pooled resources.</summary>
    public ValueTask DisposeAsync()
    {
        _listener.Stop();
        return ValueTask.CompletedTask;
    }
}

/// <summary>One accepted or connected TCP (optionally TLS) session after HELLO / ALIAS_MAP.</summary>
public sealed partial class TcpSyncSession : IAsyncDisposable
{
    private readonly TcpClient _client;
    private readonly StreamSyncTransport _transport;
    private readonly Stream? _ownedOuter; // SslStream when TLS; disposed after transport

    /// <summary>Remote peer node id this state is tracked for.</summary>
    public byte[] PeerNodeId { get; }
    /// <summary>Session epoch advertised by the connected peer.</summary>
    public int PeerEpoch { get; }
    /// <summary>Capability bits advertised by the connected peer.</summary>
    public SessionCapabilities PeerCapabilities { get; }
    /// <summary>True when this session transports frames over TLS.</summary>
    public bool IsTls { get; }

    /// <summary>Per-frame receive timeout; null uses only the caller's CancellationToken.</summary>
    public TimeSpan? ReadTimeout
    {
        get => _transport.ReadTimeout;
        set => _transport.ReadTimeout = value;
    }

    private TcpSyncSession(
        TcpClient client, StreamSyncTransport transport, HelloPayload peer,
        Stream? ownedOuter, bool isTls)
    {
        _client = client;
        _transport = transport;
        _ownedOuter = ownedOuter;
        PeerNodeId = peer.NodeId;
        PeerEpoch = peer.Epoch;
        PeerCapabilities = peer.Capabilities;
        IsTls = isTls;
        _transport.PeerNodeId = peer.NodeId;
    }

    /// <summary>
    /// Connect to a peer. When <paramref name="sslOptions"/> is set, negotiate TLS
    /// as the client before the BinSync HELLO exchange.
    /// </summary>
    public static async Task<TcpSyncSession> ConnectAsync(
        Node local, string host, int port,
        SessionCapabilities caps = SessionCapabilities.None,
        SslClientAuthenticationOptions? sslOptions = null,
        CancellationToken ct = default)
    {
        var client = new TcpClient { NoDelay = true };
        Stream? outer = null;
        try
        {
            await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
            Stream stream = client.GetStream();
            if (sslOptions is not null)
            {
                var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                await ssl.AuthenticateAsClientAsync(sslOptions, ct).ConfigureAwait(false);
                outer = ssl;
                stream = ssl;
            }
            var transport = new StreamSyncTransport(stream, ownsStream: false);
            var peer = await InitiateAsync(local, transport, caps, ct).ConfigureAwait(false);
            return new TcpSyncSession(client, transport, peer, outer, isTls: outer is not null);
        }
        catch
        {
            if (outer is not null) await outer.DisposeAsync().ConfigureAwait(false);
            client.Dispose();
            throw;
        }
    }

    internal static async Task<TcpSyncSession> CompleteAcceptAsync(
        Node local, TcpClient client, SessionCapabilities caps,
        X509Certificate2? serverCertificate, CancellationToken ct)
    {
        Stream stream = client.GetStream();
        Stream? outer = null;
        if (serverCertificate is not null)
        {
            var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
            var serverOpts = new SslServerAuthenticationOptions
            {
                ServerCertificate = serverCertificate,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                ClientCertificateRequired = false,
            };
            await ssl.AuthenticateAsServerAsync(serverOpts, ct).ConfigureAwait(false);
            outer = ssl;
            stream = ssl;
        }
        var transport = new StreamSyncTransport(stream, ownsStream: false);
        var peer = await AcceptHandshakeAsync(local, transport, caps, ct).ConfigureAwait(false);
        return new TcpSyncSession(client, transport, peer, outer, isTls: outer is not null);
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
        await SendFrameAsync(0x09, streamId, 1, payload, ct).ConfigureAwait(false);
        var ackRaw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed before ACK");
        var (ackFrame, _) = FrameCodec.ParseFrame(ackRaw);
        if (ackFrame.FrameType != 0x0B)
            throw new FrameException("FRAME_REJECTED");
        var (bid, seq, author) = Transport.DecodeAck(ackFrame.Payload);
        if (!batch.BatchId.EqualsBytes(bid))
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
        var result = receiver.ApplyBatchPayload(frame.Payload, PeerNodeId);
        var ackPayload = Transport.EncodeAck(result.BatchId, result.CursorEnd, result.DurableAuthor);
        await SendFrameAsync(0x0B, 0x0002_0000u | (uint)result.NamespaceId, 2, ackPayload, ct).ConfigureAwait(false);
        return result;
    }

    /// <summary>Build frame into an ArrayPool buffer and send without an extra intermediate copy.</summary>
    private async Task SendFrameAsync(byte type, uint streamId, ulong seq, ReadOnlyMemory<byte> payload, CancellationToken ct, byte flags = 0)
    {
        int need = Frames.HEADER_SIZE + payload.Length;
        var rented = ArrayPool<byte>.Shared.Rent(need);
        try
        {
            int n = FrameCodec.BuildFrame(type, streamId, seq, payload.Span, rented.AsSpan(0, need), flags);
            await _transport.SendAsync(rented.AsMemory(0, n), ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Release unmanaged or pooled resources.</summary>
    public async ValueTask DisposeAsync()
    {
        await _transport.DisposeAsync().ConfigureAwait(false);
        if (_ownedOuter is not null)
            await _ownedOuter.DisposeAsync().ConfigureAwait(false);
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
        Node local, NodeId peerNodeId, int peerEpoch, IReadOnlyList<AliasMapEntry> remote)
    {
        var result = new List<AliasMapEntry>(remote);
        var known = new HashSet<NodeId>();
        foreach (var e in result) known.Add(e.NodeId);
        var extras = new List<(NodeId Id, int Epoch)>();
        void Consider(NodeId id, int epoch)
        {
            if (!known.Add(id)) return;
            extras.Add((id, epoch));
        }
        Consider(local.NodeId, local.Epoch);
        Consider(peerNodeId, peerEpoch);
        foreach (var (nid, ep) in local.DurableAuthors)
            Consider(nid, ep);
        extras.Sort(static (a, b) =>
        {
            Span<byte> ba = stackalloc byte[16];
            Span<byte> bb = stackalloc byte[16];
            a.Id.WriteTo(ba);
            b.Id.WriteTo(bb);
            return ba.SequenceCompareTo(bb);
        });
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

/// <summary>
/// Test / lab helpers for TLS. Production hosts should load real certificates
/// from the OS store or a provisioned PEM path — not this API.
/// </summary>
public static class TlsTestCert
{
    /// <summary>Ephemeral self-signed RSA cert valid for ~1 day (CN=localhost).</summary>
    public static X509Certificate2 CreateEphemeral(string cn = "localhost")
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(
            $"CN={cn}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(false, false, 0, false));
        req.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        req.CertificateExtensions.Add(
            new X509SubjectKeyIdentifierExtension(req.PublicKey, false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(cn);
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        san.AddIpAddress(IPAddress.IPv6Loopback);
        req.CertificateExtensions.Add(san.Build());
        var cert = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddDays(1));
        // Ensure private key is exportable for SslStream on all platforms.
        return X509CertificateLoader.LoadPkcs12(
            cert.Export(X509ContentType.Pfx), password: null,
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.EphemeralKeySet);
    }

    /// <summary>
    /// Client options that accept a specific server certificate thumbprint
    /// (or any cert when <paramref name="expectedThumbprint"/> is null — lab only).
    /// </summary>
    public static SslClientAuthenticationOptions ClientOptions(
        string targetHost = "localhost", string? expectedThumbprint = null)
    {
        return new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            RemoteCertificateValidationCallback = (sender, cert, chain, errors) =>
            {
                if (cert is null) return false;
                if (expectedThumbprint is null) return true; // lab: trust any presented cert
                return string.Equals(
                    cert.GetCertHashString(), expectedThumbprint,
                    StringComparison.OrdinalIgnoreCase);
            },
        };
    }
}
