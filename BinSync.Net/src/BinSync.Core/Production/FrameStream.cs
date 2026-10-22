// Stream ↔ self-describing BIN-SYNC frames (§3.4, §12.2).
using System.Buffers.Binary;

namespace BinSync;

/// <summary>
/// Reads and writes complete protocol frames on an arbitrary <see cref="Stream"/>
/// (TCP, named pipe, file). No length prefix beyond the frame header itself.
/// </summary>
public static class FrameStream
{
    public static async Task WriteFrameAsync(Stream stream, byte[] frame, CancellationToken ct = default)
    {
        await stream.WriteAsync(frame, ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Block until one full frame is available. Returns null on clean EOF before any bytes.
    /// Throws <see cref="FrameException"/> on truncated / corrupt frames.
    /// </summary>
    public static async Task<byte[]?> ReadFrameAsync(Stream stream, CancellationToken ct = default)
    {
        var header = new byte[Frames.HEADER_SIZE];
        int n = await ReadExactAsync(stream, header, 0, header.Length, ct).ConfigureAwait(false);
        if (n == 0) return null;
        if (n < header.Length)
            throw new FrameException("FRAME_REJECTED");

        uint payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(17));
        if (payloadLen > Frames.MAX_PAYLOAD_SIZE)
            throw new FrameException("PAYLOAD_TOO_LARGE", fatal: true);

        var frame = new byte[Frames.HEADER_SIZE + (int)payloadLen];
        Buffer.BlockCopy(header, 0, frame, 0, Frames.HEADER_SIZE);
        if (payloadLen > 0)
        {
            int got = await ReadExactAsync(stream, frame, Frames.HEADER_SIZE, (int)payloadLen, ct)
                .ConfigureAwait(false);
            if (got < (int)payloadLen)
                throw new FrameException("FRAME_REJECTED");
        }

        _ = FrameCodec.ParseFrame(frame);
        return frame;
    }

    private static async Task<int> ReadExactAsync(Stream stream, byte[] buf, int offset, int count, CancellationToken ct)
    {
        int total = 0;
        while (total < count)
        {
            int r = await stream.ReadAsync(buf.AsMemory(offset + total, count - total), ct)
                .ConfigureAwait(false);
            if (r == 0) return total;
            total += r;
        }
        return total;
    }
}
