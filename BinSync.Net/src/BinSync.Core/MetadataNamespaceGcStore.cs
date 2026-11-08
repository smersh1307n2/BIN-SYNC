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
    IReadOnlyList<FullReplaceInstalledAck> ReadFullReplaceInstalledAcks(
        NamespaceRosterCertificate acceptedRoster, Guid generation);

    bool AreAllActiveMembersInstalled(
        NamespaceRosterCertificate acceptedRoster, Guid generation, ReadOnlySpan<byte> digest);

    bool IsOriginCoveredByRetentionCheckpoint(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        NodeId originAuthor, uint originEpoch, ulong originSequence);

    /// <summary>
    /// Protocol proof component only. v3.1 currently fails closed because frontiers
    /// do not yet bind every author's issued high-watermark and the post-FULL_REPLACE
    /// HLC floor. Callers must additionally prove that no PendingApply, relay/provider
    /// intent, reservation or snapshot pin references the tombstone before reclamation.
    /// </summary>
    bool HasProtocolTombstoneGcProof(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        NodeId originAuthor, uint originEpoch, ulong originSequence,
        Guid fullReplaceGeneration, ReadOnlySpan<byte> fullReplaceDigest);
}
