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
        if (logicalPayload.Length > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        var parts = FragmentCodec.Split(logicalPayload, maxBody);
        if (parts.Count > 0 && (ulong)(parts.Count - 1) > ulong.MaxValue - baseSeq)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);
        if (_flowControlEnabled && parts.Any(part => part.Length > _sendFlowControl.Initial))
            throw new FrameException("FLOW_CONTROL_VIOLATION");
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
        await _exclusiveReceiveGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            BeginRawReceiveMode();
            var reassembler = new FragmentReassembler();
            while (true)
            {
                var raw = await _rawReceiveChannel.Reader.ReadAsync(ct).ConfigureAwait(false);
                byte flags = raw[4];
                int payloadLength = checked((int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(17)));
                byte[] payload = raw.AsSpan(Frames.HEADER_SIZE, payloadLength).ToArray();
                if ((flags & Frames.FLAG_FRAGMENT) == 0)
                {
                    await CompleteReceiveAsync(raw, ct).ConfigureAwait(false);
                    return payload;
                }
                var complete = reassembler.Accept(payload);
                await CompleteReceiveAsync(raw, ct).ConfigureAwait(false);
                if (complete is not null)
                    return complete;
            }
        }
        finally
        {
            _exclusiveReceiveGate.Release();
        }
    }
}
