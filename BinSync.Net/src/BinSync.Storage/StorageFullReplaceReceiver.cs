namespace BinSync.Storage;

/// <summary>
/// Durable receiver-side FULL_REPLACE orchestration. The provider owns staged bytes;
/// the namespace-GC store owns signed installation evidence. Commit requires an
/// application-supplied exclusive guard covering every cross-store live reference.
/// </summary>
public sealed class StorageFullReplaceReceiver
{
    private const int AbortPageSize = 4096;
    private readonly object _gate = new();
    private readonly IProviderFullReplace _fullReplace;
    private readonly IMetadataNamespaceGcStore _gcStore;
    private readonly TrustStore _trustStore;
    private readonly int _namespaceId;
    private readonly NodeId _authorityPeer;
    private readonly uint _authorityPeerEpoch;
    private readonly NodeId _localMember;
    private readonly uint _localMemberEpoch;
    private readonly byte[] _localMemberSeed;
    private readonly Func<IDisposable?> _acquireCommitGuard;
    private readonly Action _afterCommit;
    private NamespaceRosterCertificate? _roster;
    private FullReplaceTransferProtocol.Begin? _manifest;

    public StorageFullReplaceReceiver(
        IDataProvider provider,
        IMetadataNamespaceGcStore gcStore,
        TrustStore trustStore,
        int namespaceId,
        NodeId authorityPeer,
        uint authorityPeerEpoch,
        NodeId localMember,
        uint localMemberEpoch,
        byte[] localMemberSeed,
        Func<IDisposable?> acquireCommitGuard,
        Action afterCommit)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _fullReplace = provider as IProviderFullReplace
            ?? throw new NotSupportedException("The provider does not support durable FULL_REPLACE staging.");
        _gcStore = gcStore ?? throw new ArgumentNullException(nameof(gcStore));
        _trustStore = trustStore ?? throw new ArgumentNullException(nameof(trustStore));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (authorityPeer.IsZero || authorityPeerEpoch == 0 ||
            localMember.IsZero || localMemberEpoch == 0)
            throw new ArgumentException("FULL_REPLACE peer and local member identities/epochs must be non-zero.");
        ArgumentNullException.ThrowIfNull(localMemberSeed);
        if (localMemberSeed.Length != 32)
            throw new ArgumentException("Ed25519 member seed must be 32 bytes.", nameof(localMemberSeed));
        _acquireCommitGuard = acquireCommitGuard
            ?? throw new ArgumentNullException(nameof(acquireCommitGuard));
        _afterCommit = afterCommit ?? throw new ArgumentNullException(nameof(afterCommit));
        _namespaceId = namespaceId;
        _authorityPeer = authorityPeer;
        _authorityPeerEpoch = authorityPeerEpoch;
        _localMember = localMember;
        _localMemberEpoch = localMemberEpoch;
        _localMemberSeed = localMemberSeed.ToArray();
    }

    /// <summary>Verify authority/roster binding, then begin or resume durable staging.</summary>
    public ProviderFullReplaceProgress Begin(FullReplaceTransferProtocol.Begin manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        lock (_gate)
        {
            if (manifest.NamespaceId != _namespaceId)
                throw new InvalidDataException("FULL_REPLACE manifest targets another namespace.");
            if (!_gcStore.TryReadNamespaceRoster(_namespaceId, _trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted authoritative namespace roster is available.");
            NamespaceRosterProtocol.VerifyOrThrow(roster, _trustStore);

            if (roster.AuthorityNodeId != _authorityPeer)
                throw new InvalidDataException("FULL_REPLACE must be sent by the pinned namespace authority.");
            NamespaceRosterMember authority = FindMember(roster, _authorityPeer);
            if (authority.Status != NamespaceMemberStatus.Active ||
                authority.TransportPeerEpoch != _authorityPeerEpoch)
                throw new InvalidDataException("FULL_REPLACE authority is not Active at this transport epoch.");
            NamespaceRosterMember local = FindMember(roster, _localMember);
            if (local.Status == NamespaceMemberStatus.Retired ||
                local.TransportPeerEpoch != _localMemberEpoch)
                throw new InvalidDataException("Local member is retired or its transport epoch does not match the roster.");

            FullReplaceTransferProtocol.VerifyBegin(manifest, roster, _trustStore);
            if (_manifest is not null &&
                (_manifest.Generation != manifest.Generation ||
                 !_manifest.ExpectedDigest.AsSpan().SequenceEqual(manifest.ExpectedDigest)))
                throw new InvalidOperationException("Another FULL_REPLACE transfer is already active on this receiver.");

            _fullReplace.BeginFullReplace(manifest.Generation, manifest.SourceChangeFeedWatermark,
                manifest.ExpectedEntryCount, manifest.ExpectedDigest);
            if (!_fullReplace.TryGetFullReplaceProgress(manifest.Generation, out ProviderFullReplaceProgress? progress) ||
                progress is null)
                throw new InvalidDataException("Provider did not persist the FULL_REPLACE staging manifest.");
            ValidateProgress(manifest, progress);
            _roster = roster;
            _manifest = manifest;
            return progress;
        }
    }

    /// <summary>Persist the next contiguous, bounded page. Resume position is read from durable provider state.</summary>
    public ProviderFullReplaceProgress Stage(FullReplaceTransferProtocol.Chunk chunk)
    {
        ArgumentNullException.ThrowIfNull(chunk);
        lock (_gate)
        {
            FullReplaceTransferProtocol.Begin manifest = RequireManifest();
            if (chunk.NamespaceId != _namespaceId || chunk.Generation != manifest.Generation)
                throw new InvalidDataException("FULL_REPLACE chunk does not belong to the active transfer.");
            if (!_fullReplace.TryGetFullReplaceProgress(manifest.Generation, out ProviderFullReplaceProgress? progress) ||
                progress is null)
                throw new InvalidDataException("FULL_REPLACE staging state disappeared.");
            ValidateProgress(manifest, progress);
            if (progress.IsCommitted)
                throw new InvalidOperationException("Committed FULL_REPLACE cannot accept more chunks.");
            if (chunk.StartIndex != progress.NextIndex)
                throw new InvalidDataException("FULL_REPLACE chunk is not the next durable page; resume from the stored cursor.");
            if (chunk.Entries.Count > progress.ExpectedEntryCount - progress.NextIndex)
                throw new InvalidDataException("FULL_REPLACE chunk exceeds the manifest entry count.");

            _fullReplace.StageFullReplacePage(manifest.Generation, chunk.StartIndex, chunk.Entries);
            if (!_fullReplace.TryGetFullReplaceProgress(manifest.Generation, out progress) || progress is null)
                throw new InvalidDataException("Provider lost FULL_REPLACE progress after staging.");
            ValidateProgress(manifest, progress);
            return progress;
        }
    }

    /// <summary>
    /// Verify complete staging and switch the provider under the caller's cross-store
    /// commit guard. The signed local installation ACK is persisted only after commit.
    /// </summary>
    public FullReplaceInstalledAck Commit(FullReplaceTransferProtocol.TransferId transfer)
    {
        ArgumentNullException.ThrowIfNull(transfer);
        lock (_gate)
        {
            FullReplaceTransferProtocol.Begin manifest = RequireManifest();
            NamespaceRosterCertificate roster = _roster
                ?? throw new InvalidOperationException("Accepted roster is missing.");
            if (transfer.NamespaceId != _namespaceId || transfer.Generation != manifest.Generation)
                throw new InvalidDataException("FULL_REPLACE COMMIT does not match the active transfer.");
            if (!_fullReplace.TryGetFullReplaceProgress(manifest.Generation, out ProviderFullReplaceProgress? progress) ||
                progress is null)
                throw new InvalidDataException("FULL_REPLACE staging state disappeared.");
            ValidateProgress(manifest, progress);
            if (!progress.IsCommitted && progress.NextIndex != progress.ExpectedEntryCount)
                throw new InvalidOperationException("FULL_REPLACE cannot commit before every manifest entry is staged.");

            IDisposable? commitGuard = _acquireCommitGuard();
            if (commitGuard is null)
                throw new InvalidOperationException("Cross-store FULL_REPLACE commit proof is unavailable; provider state was not switched.");
            using (commitGuard)
            {
                if (!progress.IsCommitted)
                    _fullReplace.CommitFullReplace(manifest.Generation);
                _afterCommit();
                var acknowledgement = new FullReplaceInstalledAck
                {
                    NamespaceId = _namespaceId,
                    RosterGeneration = roster.RosterGeneration,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                    Generation = manifest.Generation,
                    Digest = manifest.ExpectedDigest.ToArray(),
                    MemberNodeId = _localMember,
                    MemberTransportEpoch = _localMemberEpoch,
                    Installed = true
                };
                byte[] signed = FullReplaceInstalledAckProtocol.Sign(acknowledgement, _localMemberSeed);
                return _gcStore.AcceptFullReplaceInstalledAck(signed, roster);
            }
        }
    }

    /// <summary>Abort uncommitted staged rows in bounded durable batches.</summary>
    public void Abort(FullReplaceTransferProtocol.TransferId transfer)
    {
        ArgumentNullException.ThrowIfNull(transfer);
        lock (_gate)
        {
            FullReplaceTransferProtocol.Begin manifest = RequireManifest();
            if (transfer.NamespaceId != _namespaceId || transfer.Generation != manifest.Generation)
                throw new InvalidDataException("FULL_REPLACE ABORT does not match the active transfer.");
            if (_fullReplace.TryGetFullReplaceProgress(manifest.Generation, out ProviderFullReplaceProgress? progress) &&
                progress is { IsCommitted: true })
                throw new InvalidOperationException("Committed FULL_REPLACE cannot be aborted.");
            while (!_fullReplace.AbortFullReplace(manifest.Generation, AbortPageSize)) { }
            _manifest = null;
            _roster = null;
        }
    }

    private FullReplaceTransferProtocol.Begin RequireManifest()
        => _manifest ?? throw new InvalidOperationException("FULL_REPLACE BEGIN has not been accepted.");

    private static void ValidateProgress(
        FullReplaceTransferProtocol.Begin manifest, ProviderFullReplaceProgress progress)
    {
        if (progress.Generation != manifest.Generation ||
            progress.SourceChangeFeedWatermark != manifest.SourceChangeFeedWatermark ||
            progress.ExpectedEntryCount != manifest.ExpectedEntryCount ||
            !progress.ExpectedDigest.AsSpan().SequenceEqual(manifest.ExpectedDigest))
            throw new InvalidDataException("Durable FULL_REPLACE progress conflicts with its signed manifest.");
    }

    private static NamespaceRosterMember FindMember(NamespaceRosterCertificate roster, NodeId nodeId)
    {
        NamespaceRosterMember? member = roster.Members.FirstOrDefault(item => item.NodeId == nodeId);
        return member ?? throw new InvalidDataException("Namespace roster does not contain the required member.");
    }
}
