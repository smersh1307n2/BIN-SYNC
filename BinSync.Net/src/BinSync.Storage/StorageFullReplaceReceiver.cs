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
    private readonly IDataProvider _provider;
    private readonly IProviderFullReplace _fullReplace;
    private readonly IMetadataNamespaceGcStore _gcStore;
    private readonly TrustStore _trustStore;
    private readonly int _namespaceId;
    private readonly string _scopeId;
    private readonly IApplicationGcReferenceScanner _externalState;
    private readonly NodeId _authorityPeer;
    private readonly uint _authorityPeerEpoch;
    private readonly NodeId _localMember;
    private readonly uint _localMemberEpoch;
    private readonly byte[] _localMemberSeed;
    private readonly Func<IDisposable?> _acquireCommitGuard;
    private readonly Action<ulong, uint> _advanceHlcAndRefresh;
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
        string scopeId,
        IApplicationGcReferenceScanner externalState,
        Func<IDisposable?> acquireCommitGuard,
        Action<ulong, uint> advanceHlcAndRefresh)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
        _fullReplace = provider as IProviderFullReplace
            ?? throw new NotSupportedException("The provider does not support durable FULL_REPLACE staging.");
        _gcStore = gcStore ?? throw new ArgumentNullException(nameof(gcStore));
        if (provider is StorageDataProvider storageProvider &&
            gcStore is StorageNamespaceGcStore storageGcStore &&
            !storageProvider.SharesStorageWith(storageGcStore))
            throw new ArgumentException("FULL_REPLACE provider and namespace-GC store must share one StorageDatabase.", nameof(gcStore));
        _trustStore = trustStore ?? throw new ArgumentNullException(nameof(trustStore));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (authorityPeer.IsZero || authorityPeerEpoch == 0 ||
            localMember.IsZero || localMemberEpoch == 0)
            throw new ArgumentException("FULL_REPLACE peer and local member identities/epochs must be non-zero.");
        ArgumentNullException.ThrowIfNull(localMemberSeed);
        if (localMemberSeed.Length != 32)
            throw new ArgumentException("Ed25519 member seed must be 32 bytes.", nameof(localMemberSeed));
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        _externalState = externalState ?? throw new ArgumentNullException(nameof(externalState));
        _scopeId = scopeId;
        _acquireCommitGuard = acquireCommitGuard
            ?? throw new ArgumentNullException(nameof(acquireCommitGuard));
        _advanceHlcAndRefresh = advanceHlcAndRefresh ?? throw new ArgumentNullException(nameof(advanceHlcAndRefresh));
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
            var localAckProbe = new FullReplaceInstalledAck
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
            byte[] probe = FullReplaceInstalledAckProtocol.Sign(localAckProbe, _localMemberSeed);
            _ = FullReplaceInstalledAckProtocol.DecodeAndVerify(probe, roster);
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

    public FullReplaceTransferProtocol.Progress GetWireProgress()
    {
        lock (_gate)
        {
            FullReplaceTransferProtocol.Begin manifest = RequireManifest();
            if (!_fullReplace.TryGetFullReplaceProgress(manifest.Generation, out ProviderFullReplaceProgress? progress) ||
                progress is null)
                throw new InvalidDataException("FULL_REPLACE staging state disappeared.");
            ValidateProgress(manifest, progress);
            return new FullReplaceTransferProtocol.Progress(
                _namespaceId, manifest.Generation, progress.ExpectedEntryCount,
                progress.NextIndex, progress.ExpectedDigest.ToArray(), progress.IsCommitted,
                FullReplaceTransferProtocol.GetNextFrameSequence(progress.NextIndex));
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
            int requiredCount = Math.Min(FullReplaceTransferProtocol.MaxChunkEntries,
                progress.ExpectedEntryCount - progress.NextIndex);
            if (chunk.Entries.Count != requiredCount)
                throw new InvalidDataException("FULL_REPLACE chunks must use canonical full pages, except for the final page.");

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
                using IApplicationGcReferenceScan hostScan =
                    _externalState.OpenScan(_scopeId, _localMember, _namespaceId);
                if (hostScan.UnpublishedLocalDraftHorizon.Count != 0)
                    throw new InvalidOperationException("Drain FLD/WAL and host-local drafts before committing FULL_REPLACE; the composed application commit guard keeps this horizon stable.");
                ProviderChangePage pendingLocal = _provider.ReadLocalChanges(0, 1);
                if (pendingLocal.Changes.Count != 0)
                    throw new InvalidOperationException("Drain and acknowledge local provider drafts before committing FULL_REPLACE; the application commit guard prevents new drafts during the switch.");

                if (!_gcStore.TryReadNamespaceRoster(_namespaceId, _trustStore,
                        out NamespaceRosterCertificate? currentRoster) || currentRoster is null ||
                    currentRoster.RosterGeneration != roster.RosterGeneration ||
                    !NamespaceRosterProtocol.ComputeDigest(currentRoster).AsSpan()
                        .SequenceEqual(NamespaceRosterProtocol.ComputeDigest(roster)))
                    throw new InvalidOperationException("Namespace roster changed before FULL_REPLACE commit.");
                if (!progress.IsCommitted)
                    _fullReplace.CommitFullReplace(manifest.Generation);
                (ulong hlcWall, uint hlcLogical) = ReadCommittedHlcFloor();
                _advanceHlcAndRefresh(hlcWall, hlcLogical);
                var acknowledgement = new FullReplaceInstalledAck
                {
                    NamespaceId = _namespaceId,
                    RosterGeneration = roster.RosterGeneration,
                    RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                    Generation = manifest.Generation,
                    Digest = manifest.ExpectedDigest.ToArray(),
                    MemberNodeId = _localMember,
                    MemberTransportEpoch = _localMemberEpoch,
                    HlcWallFloor = hlcWall,
                    HlcLogicalFloor = hlcLogical,
                    Installed = true
                };
                byte[] signed = FullReplaceInstalledAckProtocol.Sign(acknowledgement, _localMemberSeed);
                return _gcStore.AcceptFullReplaceInstalledAck(signed, roster);
            }
        }
    }

    /// <summary>Run the ordered receive loop, including concurrent ABORT detection.</summary>
    public async Task<FullReplaceInstalledAck?> ReceiveTransferAsync(
        SyncSession session, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (session.PeerNodeId != _authorityPeer ||
            session.PeerEpoch <= 0 || checked((uint)session.PeerEpoch) != _authorityPeerEpoch)
            throw new InvalidDataException("FULL_REPLACE session does not match the configured authority peer incarnation.");

        if (!_gcStore.TryReadNamespaceRoster(_namespaceId, _trustStore, out NamespaceRosterCertificate? roster) ||
            roster is null)
            throw new InvalidOperationException("No accepted authoritative namespace roster is available.");
        FullReplaceTransferProtocol.Begin manifest =
            await session.ReceiveFullReplaceBeginAsync(_namespaceId, roster, _trustStore,
                expectedFrameSequence: 1, ct).ConfigureAwait(false);
        ProviderFullReplaceProgress progress = Begin(manifest);
        await session.SendFullReplaceReadyAsync(GetWireProgress(), frameSequence: 1, ct)
            .ConfigureAwait(false);

        ulong expectedSequence = FullReplaceTransferProtocol.GetNextFrameSequence(progress.NextIndex);
        while (progress.NextIndex < progress.ExpectedEntryCount)
        {
            var next = await ReceiveChunkOrAbortAsync(
                session, expectedSequence, ct).ConfigureAwait(false);
            if (next.Abort is not null)
            {
                Abort(next.Abort);
                return null;
            }
            progress = Stage(next.Chunk!);
            expectedSequence = checked(expectedSequence + 1);
        }

        var terminal = await ReceiveCommitOrAbortAsync(
            session, expectedSequence, ct).ConfigureAwait(false);
        if (terminal.Abort is not null)
        {
            Abort(terminal.Abort);
            return null;
        }

        FullReplaceInstalledAck acknowledgement = Commit(terminal.Commit!);
        await session.SendFullReplaceInstalledAckAsync(
            acknowledgement, _localMemberSeed, frameSequence: 1, ct).ConfigureAwait(false);
        return acknowledgement;
    }

    private async Task<(FullReplaceTransferProtocol.Chunk? Chunk, FullReplaceTransferProtocol.TransferId? Abort)>
        ReceiveChunkOrAbortAsync(SyncSession session, ulong sequence, CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task<FullReplaceTransferProtocol.Chunk> chunkTask =
            session.ReceiveFullReplaceChunkAsync(_namespaceId, sequence, linked.Token);
        Task<FullReplaceTransferProtocol.TransferId> abortTask =
            session.ReceiveFullReplaceAbortAsync(_namespaceId, sequence, linked.Token);
        Task completed = await Task.WhenAny(chunkTask, abortTask).ConfigureAwait(false);
        if (chunkTask.IsCompletedSuccessfully && abortTask.IsCompletedSuccessfully)
            throw new FrameException("FRAME_REJECTED");
        if (completed == abortTask)
        {
            FullReplaceTransferProtocol.TransferId abort;
            try { abort = await abortTask.ConfigureAwait(false); }
            catch { linked.Cancel(); await ObserveCancelledAsync(chunkTask).ConfigureAwait(false); throw; }
            linked.Cancel();
            await ObserveCancelledAsync(chunkTask).ConfigureAwait(false);
            return (null, abort);
        }
        FullReplaceTransferProtocol.Chunk chunk;
        try { chunk = await chunkTask.ConfigureAwait(false); }
        catch { linked.Cancel(); await ObserveCancelledAsync(abortTask).ConfigureAwait(false); throw; }
        linked.Cancel();
        await ObserveCancelledAsync(abortTask).ConfigureAwait(false);
        return (chunk, null);
    }

    private async Task<(FullReplaceTransferProtocol.TransferId? Commit, FullReplaceTransferProtocol.TransferId? Abort)>
        ReceiveCommitOrAbortAsync(SyncSession session, ulong sequence, CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task<FullReplaceTransferProtocol.TransferId> commitTask =
            session.ReceiveFullReplaceCommitAsync(_namespaceId, sequence, linked.Token);
        Task<FullReplaceTransferProtocol.TransferId> abortTask =
            session.ReceiveFullReplaceAbortAsync(_namespaceId, sequence, linked.Token);
        Task completed = await Task.WhenAny(commitTask, abortTask).ConfigureAwait(false);
        if (commitTask.IsCompletedSuccessfully && abortTask.IsCompletedSuccessfully)
            throw new FrameException("FRAME_REJECTED");
        if (completed == abortTask)
        {
            FullReplaceTransferProtocol.TransferId abort;
            try { abort = await abortTask.ConfigureAwait(false); }
            catch { linked.Cancel(); await ObserveCancelledAsync(commitTask).ConfigureAwait(false); throw; }
            linked.Cancel();
            await ObserveCancelledAsync(commitTask).ConfigureAwait(false);
            return (null, abort);
        }
        FullReplaceTransferProtocol.TransferId commit;
        try { commit = await commitTask.ConfigureAwait(false); }
        catch { linked.Cancel(); await ObserveCancelledAsync(abortTask).ConfigureAwait(false); throw; }
        linked.Cancel();
        await ObserveCancelledAsync(abortTask).ConfigureAwait(false);
        return (commit, null);
    }

    private static async Task ObserveCancelledAsync(Task task)
    {
        try { await task.ConfigureAwait(false); }
        catch (OperationCanceledException) { }
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

    private (ulong Wall, uint Logical) ReadCommittedHlcFloor()
    {
        ProviderSnapshotHandle snapshot = _provider.OpenSnapshot();
        try
        {
            ulong maxWall = 0;
            uint maxLogical = 0;
            int afterIndex = 0;
            const int pageSize = 4096;
            while (true)
            {
                ProviderSnapshotPage page = _provider.ReadSnapshot(snapshot.Token, afterIndex, pageSize);
                foreach (ProviderMutation mutation in page.Entries)
                {
                    if (mutation.HlcWall > maxWall ||
                        (mutation.HlcWall == maxWall && mutation.HlcLogical > maxLogical))
                    {
                        maxWall = mutation.HlcWall;
                        maxLogical = mutation.HlcLogical;
                    }
                }
                if (page.NextIndex < afterIndex || page.NextIndex - afterIndex != page.Entries.Count)
                    throw new InvalidDataException("Committed FULL_REPLACE snapshot cursor is non-contiguous.");
                afterIndex = page.NextIndex;
                if (page.IsComplete) break;
                if (page.Entries.Count == 0)
                    throw new InvalidDataException("Committed FULL_REPLACE snapshot ended before its completion marker.");
            }
            return (maxWall, maxLogical);
        }
        finally
        {
            _provider.ReleaseSnapshot(snapshot.Token);
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
