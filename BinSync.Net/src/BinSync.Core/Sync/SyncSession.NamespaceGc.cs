namespace BinSync;

/// <summary>Negotiated v3.1 signed namespace-GC control messages.</summary>
public partial class SyncSession
{
    public async Task SendNamespaceRosterAsync(
        NamespaceRosterCertificate roster, byte[] authoritySeed, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31);
        byte[] payload = NamespaceRosterProtocol.Sign(roster, authoritySeed);
        await SendFrameAsync(NamespaceGcFrames.NamespaceRoster, SessionWire.ControlStreamId,
            frameSequence, payload, ct).ConfigureAwait(false);
    }

    public async Task<NamespaceRosterCertificate> ReceiveNamespaceRosterAsync(
        TrustStore trustStore, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31);
        ArgumentNullException.ThrowIfNull(trustStore);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.NamespaceRoster, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.NamespaceRoster);
        ValidateControlStream(frame);
        return NamespaceRosterProtocol.DecodeAndVerify(frame.Payload, trustStore);
    }

    public async Task SendOriginFrontierAsync(
        AppliedOriginFrontierReport report, byte[] memberSeed, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.OriginFrontierV31);
        byte[] payload = OriginFrontierProtocol.Sign(report, memberSeed);
        await SendFrameAsync(NamespaceGcFrames.OriginFrontier, SessionWire.ControlStreamId,
            frameSequence, payload, ct).ConfigureAwait(false);
    }

    public async Task<AppliedOriginFrontierReport> ReceiveOriginFrontierAsync(
        NamespaceRosterCertificate acceptedRoster, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.OriginFrontierV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.OriginFrontier, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.OriginFrontier);
        ValidateControlStream(frame);
        return OriginFrontierProtocol.DecodeAndVerify(frame.Payload, acceptedRoster);
    }

    public async Task SendRetentionCheckpointAsync(
        NamespaceRetentionCheckpoint checkpoint, byte[] authoritySeed, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.OriginFrontierV31);
        byte[] payload = NamespaceRetentionCheckpointProtocol.Sign(checkpoint, authoritySeed);
        await SendFrameAsync(NamespaceGcFrames.RetentionCheckpoint, SessionWire.ControlStreamId,
            frameSequence, payload, ct).ConfigureAwait(false);
    }

    public async Task<NamespaceRetentionCheckpoint> ReceiveRetentionCheckpointAsync(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.OriginFrontierV31);
        ArgumentNullException.ThrowIfNull(trustStore);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.RetentionCheckpoint, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.RetentionCheckpoint);
        ValidateControlStream(frame);
        return NamespaceRetentionCheckpointProtocol.DecodeAndVerify(frame.Payload, acceptedRoster, trustStore);
    }

    public async Task SendFullReplaceInstalledAckAsync(
        FullReplaceInstalledAck acknowledgement, byte[] memberSeed, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        byte[] payload = FullReplaceInstalledAckProtocol.Sign(acknowledgement, memberSeed);
        await SendFrameAsync(NamespaceGcFrames.FullReplaceAck, SessionWire.ControlStreamId,
            frameSequence, payload, ct).ConfigureAwait(false);
    }

    public async Task<FullReplaceInstalledAck> ReceiveFullReplaceInstalledAckAsync(
        NamespaceRosterCertificate acceptedRoster, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.FullReplaceAck, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.FullReplaceAck);
        ValidateControlStream(frame);
        return FullReplaceInstalledAckProtocol.DecodeAndVerify(frame.Payload, acceptedRoster);
    }

    private void EnsureV31Capability(SessionCapabilities capability)
    {
        if ((NegotiatedCapabilities & capability) != capability)
            throw new InvalidOperationException($"The peer did not negotiate required BIN-SYNC v3.1 capability {capability}.");
    }
}
