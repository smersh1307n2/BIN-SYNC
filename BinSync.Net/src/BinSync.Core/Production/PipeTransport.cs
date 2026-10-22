// In-process ISyncTransport for tests and single-process multi-node hosts (§3.2).
using System.Threading.Channels;

namespace BinSync;

/// <summary>
/// Pair of linked transports: bytes written on one side appear on the other.
/// Suitable for unit tests and for driving <see cref="SessionWire.HandshakeAsync"/>.
/// </summary>
public sealed class PipeTransport : ISyncTransport
{
    private readonly ChannelReader<byte[]> _reader;
    private readonly ChannelWriter<byte[]> _writer;
    private byte[] _peerNodeId = Array.Empty<byte>();
    private bool _disposed;

    private PipeTransport(ChannelReader<byte[]> reader, ChannelWriter<byte[]> writer)
    {
        _reader = reader;
        _writer = writer;
    }

    public byte[] PeerNodeId
    {
        get => _peerNodeId;
        set => _peerNodeId = value ?? Array.Empty<byte>();
    }

    /// <summary>Create two endpoints of a bidirectional in-memory pipe.</summary>
    public static (PipeTransport Left, PipeTransport Right) CreatePair()
    {
        var ab = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
        var ba = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
        var left = new PipeTransport(ba.Reader, ab.Writer);
        var right = new PipeTransport(ab.Reader, ba.Writer);
        return (left, right);
    }

    public ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var copy = packet.ToArray();
        return _writer.TryWrite(copy)
            ? ValueTask.CompletedTask
            : new ValueTask(_writer.WriteAsync(copy, ct).AsTask());
    }

    public async ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        try
        {
            if (_reader.Completion.IsCompleted && !_reader.TryRead(out _))
                return null;
            return await _reader.ReadAsync(ct).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            return null;
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        _writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
