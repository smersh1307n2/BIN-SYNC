using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace BinSync;

/// <summary>TCP listener accepting transport-independent BIN-SYNC sessions.</summary>
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

    /// <summary>Start a TCP listener; port zero selects an ephemeral port.</summary>
    public static TcpSyncListener Start(int port = 0, IPAddress? address = null)
    {
        var listener = new TcpListener(address ?? IPAddress.Loopback, port);
        listener.Start();
        return new TcpSyncListener(listener);
    }

    /// <summary>Accept one TCP client and establish a generic SyncSession, optionally using TLS.</summary>
    public async Task<SyncSession> AcceptAsync(
        Node local,
        SessionCapabilities caps = SessionCapabilities.None,
        X509Certificate2? serverCertificate = null,
        CancellationToken ct = default)
    {
        var client = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
        client.NoDelay = true;
        Stream? stream = null;
        try
        {
            stream = client.GetStream();
            if (serverCertificate is not null)
            {
                var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                {
                    ServerCertificate = serverCertificate,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    ClientCertificateRequired = false,
                }, ct).ConfigureAwait(false);
                stream = ssl;
            }

            var transport = new StreamSyncTransport(stream, ownsStream: true);
            return await SyncSession.AcceptAsync(local, transport, caps, ownsTransport: true, ct).ConfigureAwait(false);
        }
        catch
        {
            if (stream is not null)
                await stream.DisposeAsync().ConfigureAwait(false);
            client.Dispose();
            throw;
        }
    }

    /// <summary>Stop listening.</summary>
    public ValueTask DisposeAsync()
    {
        _listener.Stop();
        return ValueTask.CompletedTask;
    }
}

/// <summary>TCP/TLS connector producing the same generic SyncSession as every other transport.</summary>
public static class TcpSync
{
    /// <summary>Connect to a TCP peer and establish a generic BIN-SYNC session.</summary>
    public static async Task<SyncSession> ConnectAsync(
        Node local,
        string host,
        int port,
        SessionCapabilities caps = SessionCapabilities.None,
        SslClientAuthenticationOptions? sslOptions = null,
        CancellationToken ct = default)
    {
        var client = new TcpClient { NoDelay = true };
        Stream? stream = null;
        try
        {
            await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
            stream = client.GetStream();
            if (sslOptions is not null)
            {
                var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                await ssl.AuthenticateAsClientAsync(sslOptions, ct).ConfigureAwait(false);
                stream = ssl;
            }

            var transport = new StreamSyncTransport(stream, ownsStream: true);
            return await SyncSession.ConnectAsync(local, transport, caps, ownsTransport: true, ct).ConfigureAwait(false);
        }
        catch
        {
            if (stream is not null)
                await stream.DisposeAsync().ConfigureAwait(false);
            client.Dispose();
            throw;
        }
    }
}
