// KEY_ROTATION (0x11) exchange on SyncSession (§14.9).
namespace BinSync;

/// <summary>Established TCP (optionally TLS) sync session with a peer.</summary>
public partial class SyncSession
{
    /// <summary>Send a signed KEY_ROTATION notice to the peer.</summary>
    public async Task SendKeyRotationAsync(KeyRotationNotice notice, CancellationToken ct = default)
    {
        uint streamId = 0x0001_0000u | (uint)notice.NamespaceId;
        await SendFrameAsync(KeyRotation.FrameType, streamId, 17ul, KeyRotation.Encode(notice), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Receive one KEY_ROTATION frame, verify+apply into <paramref name="store"/>.
    /// Returns true when the local epoch advanced.
    /// </summary>
    public async Task<bool> ReceiveKeyRotationAsync(KeyEpochStore store, CancellationToken ct = default) =>
        await ReceiveKeyRotationAsync(store, local: null, ct).ConfigureAwait(false);

    /// <summary>
    /// Receive one KEY_ROTATION frame; when <paramref name="local"/> is set, raises <see cref="Node.KeyRotationApplied"/>.
    /// </summary>
    public async Task<bool> ReceiveKeyRotationAsync(KeyEpochStore store, Node? local, CancellationToken ct = default)
    {
        var raw = await ReceiveExpectedExclusiveAsync(KeyRotation.FrameType, ct).ConfigureAwait(false)
           ;
        var (frame, _) = FrameCodec.ParseFrame(raw);
        if (frame.FrameType != KeyRotation.FrameType)
            throw new FrameException("FRAME_REJECTED");
        var notice = KeyRotation.Decode(frame.Payload);
        uint expectedStreamId = 0x0001_0000u | (uint)notice.NamespaceId;
        if (frame.StreamId != expectedStreamId || frame.Flags != 0)
            throw new FrameException("FRAME_REJECTED");
        return local is not null ? local.ApplyKeyRotation(notice, store) : store.TryApply(notice);
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
