// ISyncTransport over any Stream (TCP NetworkStream, ssl, pipes) via FrameStream.
namespace BinSync;

/// <summary>
/// Adapts a bidirectional <see cref="Stream"/> to <see cref="ISyncTransport"/>
/// using self-describing frames (§3.4). Dispose closes the stream only when the
/// constructor was given ownsStream=true.
/// </summary>
public sealed class StreamSyncTransport : IPeerAwareSyncTransport
{
    private readonly Stream _stream;
    private readonly bool _ownsStream;
    private NodeId _peerNodeId;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SemaphoreSlim _readLock = new(1, 1);

    /// <summary>
    /// Optional per-receive timeout. When set, a receive that does not complete within
    /// this period is cancelled (linked with the caller token).
    /// </summary>
    public TimeSpan? ReadTimeout { get; set; }

    /// <summary>Length-prefixed frame transport over an arbitrary Stream.</summary>
    public StreamSyncTransport(Stream stream, bool ownsStream = false)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _ownsStream = ownsStream;
    }

    /// <summary>Remote peer node id this state is tracked for.</summary>
    public NodeId PeerNodeId
    {
        get => _peerNodeId;
        set => _peerNodeId = value;
    }

    /// <summary>Send one packet on the transport.</summary>
    public async ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
    {
        await _writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await FrameStream.WriteFrameAsync(_stream, packet, ct).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>Receive one packet from the transport; null when the peer closed.</summary>
    public async ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default)
    {
        await _readLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (ReadTimeout is { } timeout && timeout > TimeSpan.Zero)
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(timeout);
                return await FrameStream.ReadFrameAsync(_stream, cts.Token).ConfigureAwait(false);
            }
            return await FrameStream.ReadFrameAsync(_stream, ct).ConfigureAwait(false);
        }
        finally
        {
            _readLock.Release();
        }
    }

    /// <summary>Release unmanaged or pooled resources.</summary>
    public async ValueTask DisposeAsync()
    {
        _writeLock.Dispose();
        _readLock.Dispose();
        if (_ownsStream)
            await _stream.DisposeAsync().ConfigureAwait(false);
    }
}
