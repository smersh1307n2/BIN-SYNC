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
    private TcpSyncListener(TcpListener listener) { _listener = listener; Port = ((IPEndPoint)listener.LocalEndpoint).Port; }
    /// <summary>Start a TCP listener; port zero selects an ephemeral port.</summary>
    public static TcpSyncListener Start(int port = 0, IPAddress? address = null)
    {
        var listener = new TcpListener(address ?? IPAddress.Loopback, port); listener.Start(); return new TcpSyncListener(listener);
    }
    /// <summary>Accept one TCP client and perform HELLO/ALIAS_MAP, optionally using TLS.</summary>
    public async Task<TcpSyncSession> AcceptAsync(Node local, SessionCapabilities caps = SessionCapabilities.None,
        X509Certificate2? serverCertificate = null, CancellationToken ct = default)
    {
        var client = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false); client.NoDelay = true;
        try { return await TcpSyncSession.CompleteAcceptAsync(local, client, caps, serverCertificate, ct).ConfigureAwait(false); }
        catch { client.Dispose(); throw; }
    }
    /// <summary>Stop listening.</summary>
    public ValueTask DisposeAsync() { _listener.Stop(); return ValueTask.CompletedTask; }
}

/// <summary>TCP/TLS host adapter; all synchronization methods are inherited from <see cref="SyncSession"/>.</summary>
public sealed class TcpSyncSession : SyncSession
{
    private readonly TcpClient _client;
    private readonly StreamSyncTransport _streamTransport;
    private readonly Stream? _ownedOuter;
    /// <summary>True when this session uses TLS.</summary>
    public bool IsTls { get; }
    /// <summary>Per-frame receive timeout.</summary>
    public TimeSpan? ReadTimeout { get => _streamTransport.ReadTimeout; set => _streamTransport.ReadTimeout = value; }
    private TcpSyncSession(TcpClient client, StreamSyncTransport transport, HelloPayload peer, Stream? outer)
        : base(transport, peer, ownsTransport: true)
    { _client = client; _streamTransport = transport; _ownedOuter = outer; IsTls = outer is not null; }

    /// <summary>Connect to a TCP peer and perform BIN-SYNC handshake.</summary>
    public static async Task<TcpSyncSession> ConnectAsync(Node local, string host, int port,
        SessionCapabilities caps = SessionCapabilities.None, SslClientAuthenticationOptions? sslOptions = null,
        CancellationToken ct = default)
    {
        var client = new TcpClient { NoDelay = true }; Stream? outer = null;
        try
        {
            await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
            Stream stream = client.GetStream();
            if (sslOptions is not null)
            { var ssl = new SslStream(stream, leaveInnerStreamOpen: false); await ssl.AuthenticateAsClientAsync(sslOptions, ct).ConfigureAwait(false); outer = ssl; stream = ssl; }
            var transport = new StreamSyncTransport(stream, ownsStream: false);
            var peer = await InitiateAsync(local, transport, caps, ct).ConfigureAwait(false);
            return new TcpSyncSession(client, transport, peer, outer);
        }
        catch { if (outer is not null) await outer.DisposeAsync().ConfigureAwait(false); client.Dispose(); throw; }
    }

    internal static async Task<TcpSyncSession> CompleteAcceptAsync(Node local, TcpClient client,
        SessionCapabilities caps, X509Certificate2? certificate, CancellationToken ct)
    {
        Stream stream = client.GetStream(); Stream? outer = null;
        if (certificate is not null)
        {
            var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
            await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            { ServerCertificate = certificate, EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13, ClientCertificateRequired = false }, ct).ConfigureAwait(false);
            outer = ssl; stream = ssl;
        }
        var transport = new StreamSyncTransport(stream, ownsStream: false);
        var peer = await AcceptHandshakeAsync(local, transport, caps, ct).ConfigureAwait(false);
        return new TcpSyncSession(client, transport, peer, outer);
    }

    /// <summary>Dispose TCP socket, optional TLS stream and packet channel.</summary>
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync().ConfigureAwait(false);
        if (_ownedOuter is not null) await _ownedOuter.DisposeAsync().ConfigureAwait(false);
        _client.Dispose();
    }
}

/// <summary>Test-only helpers for ephemeral TLS certificates.</summary>
public static class TlsTestCert
{
    /// <summary>Create a short-lived self-signed localhost certificate.</summary>
    public static X509Certificate2 CreateEphemeral(string cn = "localhost")
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest($"CN={cn}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        req.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(req.PublicKey, false));
        var san = new SubjectAlternativeNameBuilder(); san.AddDnsName(cn); san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback); san.AddIpAddress(IPAddress.IPv6Loopback); req.CertificateExtensions.Add(san.Build());
        var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        return X509CertificateLoader.LoadPkcs12(cert.Export(X509ContentType.Pfx), password: null,
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.EphemeralKeySet);
    }
    /// <summary>Create TLS client options; optional thumbprint pinning is for lab use.</summary>
    public static SslClientAuthenticationOptions ClientOptions(string targetHost = "localhost", string? expectedThumbprint = null) => new()
    {
        TargetHost = targetHost, EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
        RemoteCertificateValidationCallback = (sender, cert, chain, errors) => cert is not null &&
            (expectedThumbprint is null || string.Equals(cert.GetCertHashString(), expectedThumbprint, StringComparison.OrdinalIgnoreCase)),
    };
}
