// ISyncTransport over any Stream (TCP NetworkStream, ssl, pipes) via FrameStream.
namespace BinSync;

/// <summary>
/// Adapts a bidirectional <see cref="Stream"/> to <see cref="ISyncTransport"/>
/// using self-describing frames (§3.4). Dispose closes the stream only when the
/// constructor was given ownsStream=true.
/// </summary>
public sealed class StreamSyncTransport : ISyncTransport
{
    private readonly Stream _stream;
    private readonly bool _ownsStream;
    private byte[] _peerNodeId = Array.Empty<byte>();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SemaphoreSlim _readLock = new(1, 1);

    public StreamSyncTransport(Stream stream, bool ownsStream = false)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _ownsStream = ownsStream;
    }

    public byte[] PeerNodeId
    {
        get => _peerNodeId;
        set => _peerNodeId = value ?? Array.Empty<byte>();
    }

    public async ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
    {
        await _writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await FrameStream.WriteFrameAsync(_stream, packet.ToArray(), ct).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default)
    {
        await _readLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await FrameStream.ReadFrameAsync(_stream, ct).ConfigureAwait(false);
        }
        finally
        {
            _readLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _writeLock.Dispose();
        _readLock.Dispose();
        if (_ownsStream)
            await _stream.DisposeAsync().ConfigureAwait(false);
    }
}
