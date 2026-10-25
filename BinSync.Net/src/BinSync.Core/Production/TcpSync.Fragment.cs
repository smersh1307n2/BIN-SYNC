// FLAG_FRAGMENT send/receive on an established TCP session (§12 / lab S12).
namespace BinSync;

public sealed partial class TcpSyncSession
{
    /// <summary>
    /// Split <paramref name="logicalPayload"/> and send FLAG_FRAGMENT frames.
    /// </summary>
    public async Task SendFragmentedAsync(
        byte logicalType, uint streamId, ulong baseSeq, byte[] logicalPayload, int maxBody,
        CancellationToken ct = default)
    {
        var frames = FragmentCodec.BuildFrames(logicalType, streamId, baseSeq, logicalPayload, maxBody);
        foreach (var fr in frames)
            await _transport.SendAsync(fr, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Read frames until a logical payload is complete.
    /// A non-fragment frame is returned as-is. Fragment frames are reassembled by msgId.
    /// </summary>
    public async Task<byte[]> ReceiveReassembledAsync(CancellationToken ct = default)
    {
        var re = new FragmentReassembler();
        while (true)
        {
            var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                ?? throw new IOException("peer closed during fragment reassembly");
            var (frame, _) = FrameCodec.ParseFrame(raw);
            if ((frame.Flags & Frames.FLAG_FRAGMENT) == 0)
                return frame.Payload;
            var done = re.Accept(frame.Payload);
            if (done is not null)
                return done;
        }
    }
}
