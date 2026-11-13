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

    /// <summary>
    /// Send one member-owned MBR1 report. The supplied seed must correspond to the
    /// report member in the accepted roster; the receiver binds that identity to
    /// this authenticated session peer before durable acceptance.
    /// </summary>
    public async Task SendMetadataBaselineReportAsync(
        MetadataBaselineReport report, byte[] memberSeed,
        NamespaceRosterCertificate acceptedRoster, Guid expectedGeneration,
        ReadOnlyMemory<byte> expectedSnapshotDigest, ulong frameSequence = 1,
        CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(report);
        ArgumentNullException.ThrowIfNull(memberSeed);
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        byte[] encoded = MetadataBaselineReportProtocol.Sign(report, memberSeed);
        _ = MetadataBaselineReportProtocol.DecodeAndVerify(
            encoded, acceptedRoster, expectedGeneration, expectedSnapshotDigest.Span);
        await SendFrameAsync(NamespaceGcFrames.MetadataBaselineReport, SessionWire.ControlStreamId,
            frameSequence, encoded, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Receive and cryptographically validate MBR1, then bind its signer and transport
    /// epoch to the authenticated session peer. Callers durably accept the returned
    /// report only after this method succeeds.
    /// </summary>
    public async Task<MetadataBaselineReport> ReceiveMetadataBaselineReportAsync(
        NamespaceRosterCertificate acceptedRoster, Guid expectedGeneration,
        ReadOnlyMemory<byte> expectedSnapshotDigest, CancellationToken ct = default)
    {
        EnsureV31Capability(SessionCapabilities.NamespaceRosterV31 | SessionCapabilities.FullReplaceV31);
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        byte[] raw = await ReceiveExpectedExclusiveAsync(NamespaceGcFrames.MetadataBaselineReport, ct)
            .ConfigureAwait(false);
        var frame = ParseExpectedSessionFrame(raw, NamespaceGcFrames.MetadataBaselineReport);
        ValidateControlStream(frame);
        MetadataBaselineReport report = MetadataBaselineReportProtocol.DecodeAndVerify(
            frame.Payload, acceptedRoster, expectedGeneration, expectedSnapshotDigest.Span);
        if (PeerNodeId.IsZero || PeerEpoch <= 0 ||
            report.MemberNodeId != PeerNodeId ||
            report.MemberTransportEpoch != checked((uint)PeerEpoch))
            throw new InvalidDataException(
                "MBR1 signer identity/transport epoch does not match the authenticated session peer.");
        return report;
    }

    private void EnsureV31Capability(SessionCapabilities capability)
    {
        if ((NegotiatedCapabilities & capability) != capability)
            throw new InvalidOperationException($"The peer did not negotiate required BIN-SYNC v3.1 capability {capability}.");
    }
}
