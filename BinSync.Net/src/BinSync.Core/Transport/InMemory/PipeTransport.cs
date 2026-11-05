// In-process ISyncTransport for tests and single-process multi-node hosts (§3.2).
using System.Threading.Channels;

namespace BinSync;

/// <summary>
/// Pair of linked transports: bytes written on one side appear on the other.
/// Suitable for unit tests and for driving <see cref="SessionWire.HandshakeAsync"/>.
/// </summary>
public sealed class PipeTransport : IPeerAwareSyncTransport
{
    private readonly ChannelReader<byte[]> _reader;
    private readonly ChannelWriter<byte[]> _writer;
    private NodeId _peerNodeId;
    private bool _disposed;

    private PipeTransport(ChannelReader<byte[]> reader, ChannelWriter<byte[]> writer)
    {
        _reader = reader;
        _writer = writer;
    }

    /// <summary>Remote peer node id this state is tracked for.</summary>
    public SyncTransportCapabilities Capabilities => SyncTransportCapabilities.ReliableDelivery | SyncTransportCapabilities.OrderedDelivery;

    public NodeId PeerNodeId
    {
        get => _peerNodeId;
        set => _peerNodeId = value;
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

    /// <summary>Send one packet on the transport.</summary>
    public ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        // Channel owns the array until Receive — must copy; skip zero-length special case.
        byte[] copy;
        if (packet.Length == 0)
            copy = Array.Empty<byte>();
        else
        {
            copy = GC.AllocateUninitializedArray<byte>(packet.Length);
            packet.Span.CopyTo(copy);
        }
        return _writer.TryWrite(copy)
            ? ValueTask.CompletedTask
            : new ValueTask(_writer.WriteAsync(copy, ct).AsTask());
    }

    /// <summary>Receive one packet from the transport; null when the peer closed.</summary>
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

    /// <summary>Release unmanaged or pooled resources.</summary>
    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        _writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
