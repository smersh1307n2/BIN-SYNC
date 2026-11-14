namespace BinSync;

/// <summary>
/// Optional durable v3.1 namespace-GC state capability. Implementations must
/// persist each accepted signed object before returning and reject rollback/forks.
/// </summary>
public interface IMetadataNamespaceGcStore
{
    NamespaceRosterCertificate AcceptNamespaceRoster(ReadOnlySpan<byte> encoded, TrustStore trustStore);
    bool TryReadNamespaceRoster(int namespaceId, TrustStore trustStore, out NamespaceRosterCertificate? roster);

    AppliedOriginFrontierReport AcceptOriginFrontier(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster);
    IReadOnlyList<AppliedOriginFrontierReport> ReadOriginFrontiers(NamespaceRosterCertificate acceptedRoster);

    NamespaceRetentionCheckpoint AcceptRetentionCheckpoint(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster, TrustStore trustStore);
    bool TryReadRetentionCheckpoint(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        out NamespaceRetentionCheckpoint? checkpoint);

    FullReplaceInstalledAck AcceptFullReplaceInstalledAck(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster);

    /// <summary>Durably accepts one member-owned MBR1 report after its exact FULL_REPLACE installation ACK.</summary>
    MetadataBaselineReport AcceptMetadataBaselineReport(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster,
        Guid expectedGeneration, ReadOnlySpan<byte> expectedSnapshotDigest);
    IReadOnlyList<MetadataBaselineReport> ReadMetadataBaselineReports(
        NamespaceRosterCertificate acceptedRoster, Guid generation, ReadOnlySpan<byte> snapshotDigest);
    IReadOnlyList<FullReplaceInstalledAck> ReadFullReplaceInstalledAcks(
        NamespaceRosterCertificate acceptedRoster, Guid generation);

    bool AreAllActiveMembersInstalled(
        NamespaceRosterCertificate acceptedRoster, Guid generation, ReadOnlySpan<byte> digest);

    bool IsOriginCoveredByRetentionCheckpoint(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        NodeId originAuthor, uint originEpoch, ulong originSequence);

    /// <summary>
    /// Protocol proof component. This proves the signed author-issued/retirement horizon,
    /// all-member applied frontier and FULL_REPLACE HLC floor. Callers must additionally
    /// prove that no PendingApply, relay/provider intent, reservation or snapshot pin
    /// references the tombstone before reclamation.
    /// </summary>
    bool HasProtocolTombstoneGcProof(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        NodeId originAuthor, uint originEpoch, ulong originSequence,
        ulong tombstoneHlcWall, uint tombstoneHlcLogical,
        Guid fullReplaceGeneration, ReadOnlySpan<byte> fullReplaceDigest);
}
