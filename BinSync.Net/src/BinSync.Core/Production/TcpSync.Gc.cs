// GC_NOTICE exchange on TcpSyncSession.
namespace BinSync;

public sealed partial class TcpSyncSession
{
    public async Task SendGcNoticeAsync(
        Node local, int namespaceId, ulong tombstoneCutoffWall = 0, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var msg = GcNotice.Build(local, namespaceId, tombstoneCutoffWall);
        await _transport.SendAsync(
            FrameCodec.BuildFrame(ReconcileFrames.GcNotice, streamId, 16, GcNotice.Encode(msg)),
            ct).ConfigureAwait(false);
    }

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
