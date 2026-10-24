// KEY_ROTATION (0x11) exchange on TcpSyncSession (§14.9).
namespace BinSync;

public sealed partial class TcpSyncSession
{
    /// <summary>Send a signed KEY_ROTATION notice to the peer.</summary>
    public async Task SendKeyRotationAsync(KeyRotationNotice notice, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)notice.NamespaceId;
        await _transport.SendAsync(
            KeyRotation.BuildFrame(notice, frameSeq: 17, streamId: streamId),
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Receive one KEY_ROTATION frame, verify+apply into <paramref name="store"/>.
    /// Returns true when the local epoch advanced.
    /// </summary>
    public async Task<bool> ReceiveKeyRotationAsync(KeyEpochStore store, CancellationToken ct = default)
    {
        var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
            ?? throw new IOException("peer closed during KEY_ROTATION");
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != KeyRotation.FrameType)
            throw new FrameException("FRAME_REJECTED");
        var notice = KeyRotation.Decode(frame.Payload);
        return store.TryApply(notice);
    }

    /// <summary>
    /// Bilateral exchange: each side may publish a notice (null = send nothing / only receive).
    /// </summary>
    public async Task<(bool AppliedLocal, bool AppliedRemote)> ExchangeKeyRotationAsync(
        KeyEpochStore localStore,
        KeyRotationNotice? outbound,
        bool initiate,
        CancellationToken ct = default)
    {
        if (initiate)
        {
            if (outbound is not null)
                await SendKeyRotationAsync(outbound, ct).ConfigureAwait(false);
            bool appliedRemote = await ReceiveKeyRotationAsync(localStore, ct).ConfigureAwait(false);
            return (false, appliedRemote);
        }
        else
        {
            bool appliedLocal = await ReceiveKeyRotationAsync(localStore, ct).ConfigureAwait(false);
            if (outbound is not null)
                await SendKeyRotationAsync(outbound, ct).ConfigureAwait(false);
            return (appliedLocal, false);
        }
    }
}
