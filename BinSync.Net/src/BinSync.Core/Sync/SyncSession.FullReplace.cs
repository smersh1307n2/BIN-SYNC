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
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(begin);
        ValidateFullReplaceSequence(frameSequence);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceBegin, GetFullReplaceStream(begin.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeBegin(begin), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.Begin> ReceiveFullReplaceBeginAsync(
        int namespaceId, NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        ulong expectedFrameSequence, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        ArgumentNullException.ThrowIfNull(trustStore);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceBegin, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceBegin);
        ValidateFullReplaceStream(frame, namespaceId, expectedFrameSequence);
        var begin = FullReplaceTransferProtocol.DecodeBegin(frame.Payload);
        if (begin.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        FullReplaceTransferProtocol.VerifyBegin(begin, acceptedRoster, trustStore);
        return begin;
    }

    public async Task SendFullReplaceReadyAsync(
        FullReplaceTransferProtocol.Progress progress, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(progress);
        ValidateFullReplaceSequence(frameSequence);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceReady, GetFullReplaceStream(progress.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeProgress(progress), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.Progress> ReceiveFullReplaceReadyAsync(
        int namespaceId, ulong expectedFrameSequence, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceReady, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceReady);
        ValidateFullReplaceStream(frame, namespaceId, expectedFrameSequence);
        var progress = FullReplaceTransferProtocol.DecodeProgress(frame.Payload);
        if (progress.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        return progress;
    }

    public async Task SendFullReplaceChunkAsync(
        FullReplaceTransferProtocol.Chunk chunk, ulong frameSequence,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(chunk);
        ValidateFullReplaceSequence(frameSequence);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceChunk, GetFullReplaceStream(chunk.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeChunk(chunk), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.Chunk> ReceiveFullReplaceChunkAsync(
        int namespaceId, ulong expectedFrameSequence = 1, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceChunk, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceChunk);
        ValidateFullReplaceStream(frame, namespaceId, expectedFrameSequence);
        var chunk = FullReplaceTransferProtocol.DecodeChunk(frame.Payload);
        if (chunk.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        return chunk;
    }

    public async Task SendFullReplaceCommitAsync(
        FullReplaceTransferProtocol.TransferId transfer, ulong frameSequence,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ValidateFullReplaceSequence(frameSequence);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceCommit, GetFullReplaceStream(transfer.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeTransferId(transfer), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.TransferId> ReceiveFullReplaceCommitAsync(
        int namespaceId, ulong expectedFrameSequence = 1, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceCommit, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceCommit);
        ValidateFullReplaceStream(frame, namespaceId, expectedFrameSequence);
        var transfer = FullReplaceTransferProtocol.DecodeTransferId(frame.Payload);
        if (transfer.NamespaceId != namespaceId)
            throw new FrameException("FRAME_REJECTED");
        return transfer;
    }

    public async Task SendFullReplaceAbortAsync(
        FullReplaceTransferProtocol.TransferId transfer, ulong frameSequence,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ValidateFullReplaceSequence(frameSequence);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceAbort, GetFullReplaceStream(transfer.NamespaceId),
            frameSequence, FullReplaceTransferProtocol.EncodeTransferId(transfer), ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceTransferProtocol.TransferId> ReceiveFullReplaceAbortAsync(
        int namespaceId, ulong expectedFrameSequence = 1, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceAbort, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceAbort);
        ValidateFullReplaceStream(frame, namespaceId, expectedFrameSequence);
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

    private static void ValidateFullReplaceSequence(ulong sequence)
    {
        if (sequence == 0) throw new ArgumentOutOfRangeException(nameof(sequence));
    }

    private static void ValidateFullReplaceStream(Frame frame, int namespaceId, ulong expectedFrameSequence)
    {
        if (expectedFrameSequence == 0 || frame.FrameSeq != expectedFrameSequence ||
            frame.StreamId != GetFullReplaceStream(namespaceId))
            throw new FrameException("FRAME_REJECTED");
    }
}
