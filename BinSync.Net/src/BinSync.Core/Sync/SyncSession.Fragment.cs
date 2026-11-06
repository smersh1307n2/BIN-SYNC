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
    /// Read one complete logical frame, reassembling fragments while preserving type,
    /// stream, sequence and non-fragment flags. Raw receive mode is exclusive for the
    /// session; repeated calls continue the same reassembly state.
    /// </summary>
    public async Task<Frame> ReceiveReassembledAsync(CancellationToken ct = default)
    {
        await _exclusiveReceiveGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            BeginRawReceiveMode();
            while (true)
            {
                var raw = await _rawReceiveChannel.Reader.ReadAsync(ct).ConfigureAwait(false);
                byte[]? logicalFrame;
                try { logicalFrame = TryReassembleFrame(raw); }
                finally { await CompleteReceiveAsync(raw).ConfigureAwait(false); }

                if (logicalFrame is null) continue;
                var (frame, consumed) = FrameCodec.ParseFrame(logicalFrame);
                if (consumed != logicalFrame.Length)
                    throw new FrameException("FRAME_REJECTED", fatal: true);
                return frame;
            }
        }
        finally
        {
            _exclusiveReceiveGate.Release();
        }
    }

    private byte[]? TryReassembleFrame(byte[] raw)
    {
        byte frameType = raw[3];
        byte frameFlags = raw[4];
        if ((frameFlags & Frames.FLAG_FRAGMENT) == 0)
            return raw;

        const byte knownFlags = Frames.FLAG_COMPRESSED | Frames.FLAG_SIGNED |
                                Frames.FLAG_FRAGMENT | Frames.FLAG_ENCRYPTED;
        if ((frameFlags & ~knownFlags) != 0)
        {
            _typedFragmentState.Clear();
            throw new FrameException("FRAME_REJECTED", fatal: true);
        }

        uint streamId = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(5));
        ulong frameSequence = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(raw.AsSpan(9));
        int payloadLength = checked((int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(17)));
        FrameFragment part;
        try
        {
            part = FragmentCodec.DecodeOne(raw.AsSpan(Frames.HEADER_SIZE, payloadLength).ToArray());
            if (frameSequence < part.Index)
                throw new FrameException("FRAME_REJECTED");
        }
        catch
        {
            _typedFragmentState.Clear();
            throw;
        }

        var messageId = BatchId.From(part.MsgId);
        ulong baseSequence = frameSequence - part.Index;
        byte logicalFlags = (byte)(frameFlags & ~Frames.FLAG_FRAGMENT);
        byte[]? complete;
        FragmentContext context;
        lock (_typedFragmentState.Gate)
        {
            if (!_typedFragmentState.Contexts.TryGetValue(messageId, out context))
            {
                context = new FragmentContext(frameType, streamId, baseSequence, logicalFlags, part.Count);
                _typedFragmentState.Contexts.Add(messageId, context);
            }
            else if (context.FrameType != frameType || context.StreamId != streamId ||
                     context.BaseSequence != baseSequence || context.Flags != logicalFlags ||
                     context.Count != part.Count)
            {
                _typedFragmentState.Reassembler.Clear();
                _typedFragmentState.Contexts.Clear();
                throw new FrameException("FRAME_REJECTED", fatal: true);
            }

            try
            {
                complete = _typedFragmentState.Reassembler.Accept(part);
            }
            catch
            {
                _typedFragmentState.Reassembler.Clear();
                _typedFragmentState.Contexts.Clear();
                throw;
            }

            if (complete is not null)
                _typedFragmentState.Contexts.Remove(messageId);
        }

        return complete is null
            ? null
            : FrameCodec.BuildFrame(frameType, context.StreamId, context.BaseSequence,
                complete, context.Flags);
    }
}
