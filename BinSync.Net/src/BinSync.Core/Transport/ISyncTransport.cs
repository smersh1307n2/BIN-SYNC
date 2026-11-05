// §3.2 transport abstraction — host-owned byte pipe; the engine never opens sockets.
namespace BinSync;

/// <summary>
/// Transport-agnostic byte channel between this replica and one peer (§3.1–3.2).
/// Implementations include in-memory pipes, TCP streams, UDP datagrams and WebRTC data channels.
/// Frames are self-describing (§3.4 / §12.2); the transport only moves raw bytes.
/// </summary>
public interface ISyncTransport : IAsyncDisposable
{
    /// <summary>Durable peer identity once the session is known; may be empty before HELLO.</summary>
    NodeId PeerNodeId { get; }

    /// <summary>Send one complete framed packet (25-byte header + payload).</summary>
    ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default);

    /// <summary>
    /// Receive the next complete framed packet, or null when the channel is closed cleanly.
    /// Must not return a partial frame.
    /// </summary>
    ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default);

    /// <summary>Optional max packet size advertised by the channel (§3.2).</summary>
    int? MaxPacketSize => null;
}

/// <summary>Optional peer-identity binding for channels that expose the negotiated peer.</summary>
public interface IPeerAwareSyncTransport : ISyncTransport
{
    /// <summary>Remote peer identity, set after the BIN-SYNC handshake.</summary>
    new NodeId PeerNodeId { get; set; }
}
