namespace BinSync;

/// <summary>
/// Capability-gated transport for v3.1 authoritative FULL_REPLACE transfer envelopes.
/// Provider staging, digest verification, roster validation and atomic commit remain
/// explicit caller responsibilities; receiving a frame never switches provider state.
/// </summary>
public partial class SyncSession
{
    private const uint FullReplaceStreamPrefix = 0x0002_0000u;

    public async Task SendFullReplaceBeginAsync(
        FullReplaceTransferProtocol.Begin begin, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(begin);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceBegin, GetFullReplaceStream(begin.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeBegin(begin), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.Begin> ReceiveFullReplaceBeginAsync(
        int namespaceId, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceBegin, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceBegin);
        ValidateFullReplaceStream(frame, namespaceId);
        var begin = FullReplaceTransferProtocol.DecodeBegin(frame.Payload);
        if (begin.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        return begin;
    }

    public async Task SendFullReplaceChunkAsync(
        FullReplaceTransferProtocol.Chunk chunk, ulong frameSequence,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(chunk);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceChunk, GetFullReplaceStream(chunk.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeChunk(chunk), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.Chunk> ReceiveFullReplaceChunkAsync(
        int namespaceId, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceChunk, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceChunk);
        ValidateFullReplaceStream(frame, namespaceId);
        var chunk = FullReplaceTransferProtocol.DecodeChunk(frame.Payload);
        if (chunk.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        return chunk;
    }

    public async Task SendFullReplaceCommitAsync(
        FullReplaceTransferProtocol.TransferId transfer, ulong frameSequence,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceCommit, GetFullReplaceStream(transfer.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeTransferId(transfer), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.TransferId> ReceiveFullReplaceCommitAsync(
        int namespaceId, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceCommit, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceCommit);
        ValidateFullReplaceStream(frame, namespaceId);
        var transfer = FullReplaceTransferProtocol.DecodeTransferId(frame.Payload);
        if (transfer.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        return transfer;
    }

    public async Task SendFullReplaceAbortAsync(
        FullReplaceTransferProtocol.TransferId transfer, ulong frameSequence,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceAbort, GetFullReplaceStream(transfer.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeTransferId(transfer), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.TransferId> ReceiveFullReplaceAbortAsync(
        int namespaceId, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceAbort, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceAbort);
        ValidateFullReplaceStream(frame, namespaceId);
        var transfer = FullReplaceTransferProtocol.DecodeTransferId(frame.Payload);
        if (transfer.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        return transfer;
    }

    private static uint GetFullReplaceStream(int namespaceId)
    {
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        return FullReplaceStreamPrefix | (uint)namespaceId;
    }

    private static void ValidateFullReplaceStream(Frame frame, int namespaceId)
    {
        if (frame.StreamId != GetFullReplaceStream(namespaceId))
            throw new FrameException("FRAME_REJECTED");
    }
}
