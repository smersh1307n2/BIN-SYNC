// §3.2 transport abstraction — host-owned byte pipe; the engine never opens sockets.
namespace BinSync;

/// <summary>Delivery guarantees provided by a packet transport.</summary>
[Flags]
public enum SyncTransportCapabilities
{
    /// <summary>No delivery guarantees.</summary>
    None = 0,
    /// <summary>Packets are not silently lost while the channel remains open.</summary>
    ReliableDelivery = 1,
    /// <summary>Packets are delivered in the same order in which they were sent.</summary>
    OrderedDelivery = 2,
}

/// <summary>
/// Transport-agnostic byte channel between this replica and one peer (§3.1–3.2).
/// Implementations include in-memory pipes, TCP streams, reliable UDP adapters and WebRTC data channels.
/// Frames are self-describing (§3.4 / §12.2). Send/Receive preserve complete packet boundaries.
/// </summary>
public interface ISyncTransport : IAsyncDisposable
{
    /// <summary>Durable peer identity once the session is known; may be empty before HELLO.</summary>
    NodeId PeerNodeId { get; }

    /// <summary>Declared delivery guarantees. SyncSession requires reliable, ordered delivery.</summary>
    SyncTransportCapabilities Capabilities { get; }

    /// <summary>Send one complete framed packet (25-byte header + payload).</summary>
    ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default);

    /// <summary>
    /// Receive the next complete framed packet, or null when the channel is closed cleanly.
    /// Must not return partial frames, silently drop packets, duplicate them, or reorder them while open.
    /// </summary>
    ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default);

    /// <summary>Optional maximum complete packet size in bytes, including the BIN-SYNC frame header.</summary>
    int? MaxPacketSize => null;
}

/// <summary>Optional peer-identity binding for channels that expose the negotiated peer.</summary>
public interface IPeerAwareSyncTransport : ISyncTransport
{
    /// <summary>Remote peer identity, set after the BIN-SYNC handshake.</summary>
    new NodeId PeerNodeId { get; set; }
}
