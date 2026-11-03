// GC_NOTICE exchange on TcpSyncSession.
namespace BinSync;

/// <summary>Established TCP (optionally TLS) sync session with a peer.</summary>
public sealed partial class TcpSyncSession
{
    /// <summary>Send a GC notice frame to the peer.</summary>
    public async Task SendGcNoticeAsync(
        Node local, int namespaceId, ulong tombstoneCutoffWall = 0, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var msg = GcNotice.Build(local, namespaceId, tombstoneCutoffWall);
        await SendFrameAsync(ReconcileFrames.GcNotice, streamId, 16, GcNotice.Encode(msg), ct).ConfigureAwait(false);
    }

    /// <summary>Receive and apply a GC notice; returns how many entries were learned.</summary>
    public async Task<int> ReceiveGcNoticeAsync(Node local, CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during GC_NOTICE");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.GcNotice)
            throw new FrameException("FRAME_REJECTED");
        var msg = GcNotice.Decode(frame.Payload);
        return GcNotice.Apply(local, msg);
    }

    /// <summary>Bidirectional GC notice exchange; returns learned counts both ways.</summary>
    public async Task<(int LearnedLocal, int LearnedRemote)> ExchangeGcNoticeAsync(
        Node local, int namespaceId, bool initiate,
        ulong tombstoneCutoffWall = 0, CancellationToken ct = default)
    {
        if (initiate)
        {
            await SendGcNoticeAsync(local, namespaceId, tombstoneCutoffWall, ct).ConfigureAwait(false);
            int learned = await ReceiveGcNoticeAsync(local, ct).ConfigureAwait(false);
            return (0, learned);
        }
        else
        {
            int learned = await ReceiveGcNoticeAsync(local, ct).ConfigureAwait(false);
            await SendGcNoticeAsync(local, namespaceId, tombstoneCutoffWall, ct).ConfigureAwait(false);
            return (learned, 0);
        }
    }
}
