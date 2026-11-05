// FLAG_FRAGMENT send/receive on an established session (§12 / lab S12).
namespace BinSync;

/// <summary>Fragment send/reassembly helpers for an established sync session.</summary>
public partial class SyncSession
{
    /// <summary>Split a logical payload and send FLAG_FRAGMENT frames.</summary>
    public async Task SendFragmentedAsync(
        byte logicalType, uint streamId, ulong baseSeq, byte[] logicalPayload, int maxBody,
        CancellationToken ct = default)
    {
        var parts = FragmentCodec.Split(logicalPayload, maxBody);
        for (int i = 0; i < parts.Count; i++)
            await SendFrameAsync(logicalType, streamId, baseSeq + (ulong)i, parts[i], ct, Frames.FLAG_FRAGMENT)
                .ConfigureAwait(false);
    }

    /// <summary>
    /// Read raw frames until a logical payload is complete. This raw receive mode is
    /// exclusive for the session and cannot be mixed with typed receive operations.
    /// </summary>
    public async Task<byte[]> ReceiveReassembledAsync(CancellationToken ct = default)
    {
        await _rawReceiveGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            BeginRawReceiveMode();
            var reassembler = new FragmentReassembler();
            while (true)
            {
                var raw = await _transport.ReceiveAsync(ct).ConfigureAwait(false)
                    ?? throw new IOException("peer closed during fragment reassembly");
                var (frame, consumed) = FrameCodec.ParseFrame(raw);
                if (consumed != raw.Length) throw new FrameException("FRAME_REJECTED");
                if ((frame.Flags & Frames.FLAG_FRAGMENT) == 0)
                    return frame.Payload;
                var complete = reassembler.Accept(frame.Payload);
                if (complete is not null)
                    return complete;
            }
        }
        finally
        {
            _rawReceiveGate.Release();
        }
    }
}
