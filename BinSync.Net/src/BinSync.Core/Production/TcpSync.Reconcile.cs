// STATE_DIGEST / NEED_RANGE session helpers (§13.1 discovery phase).
namespace BinSync;

public sealed partial class TcpSyncSession
{
    /// <summary>
    /// Send our STATE_DIGEST and receive the peer's. When initiate is true we send first;
    /// otherwise we wait then reply (matching pair on the other side).
    /// </summary>
    public async Task<(StateDigestMessage Local, StateDigestMessage Remote, List<byte[]> Diverged)>
        ExchangeStateDigestAsync(Node local, int namespaceId, bool initiate, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var mine = Reconcile.BuildStateDigest(local, namespaceId);
        if (initiate)
        {
            await _transport.SendAsync(
                FrameCodec.BuildFrame(ReconcileFrames.StateDigest, streamId, 10, Reconcile.EncodeStateDigest(mine)),
                ct).ConfigureAwait(false);
            var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during STATE_DIGEST");
            var (frame, _) = FrameCodec.ParseFrame(raw);
            if (frame.FrameType != ReconcileFrames.StateDigest)
                throw new FrameException("FRAME_REJECTED");
            var peer = Reconcile.DecodeStateDigest(frame.Payload);
            return (mine, peer, Reconcile.DivergentPrefixes(mine, peer));
        }
        else
        {
            var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during STATE_DIGEST");
            var (frame, _) = FrameCodec.ParseFrame(raw);
            if (frame.FrameType != ReconcileFrames.StateDigest)
                throw new FrameException("FRAME_REJECTED");
            var peer = Reconcile.DecodeStateDigest(frame.Payload);
            await _transport.SendAsync(
                FrameCodec.BuildFrame(ReconcileFrames.StateDigest, streamId, 10, Reconcile.EncodeStateDigest(mine)),
                ct).ConfigureAwait(false);
            return (mine, peer, Reconcile.DivergentPrefixes(mine, peer));
        }
    }

    /// <summary>Send one NEED_RANGE for a divergent bucket prefix.</summary>
    public async Task SendNeedRangeAsync(int namespaceId, byte[] prefix, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)namespaceId;
        var payload = Reconcile.EncodeNeedRange(new NeedRangeMessage { NamespaceId = namespaceId, Prefix = prefix });
        await _transport.SendAsync(
            FrameCodec.BuildFrame(ReconcileFrames.NeedRange, streamId, 11, payload), ct).ConfigureAwait(false);
    }

    /// <summary>Receive one NEED_RANGE frame.</summary>
    public async Task<NeedRangeMessage> ReceiveNeedRangeAsync(CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during NEED_RANGE");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != ReconcileFrames.NeedRange)
            throw new FrameException("FRAME_REJECTED");
        return Reconcile.DecodeNeedRange(frame.Payload);
    }
}
