using BinSync;
using BinSync.Storage;

namespace BinSync.FldHost;

/// <summary>Read-only readiness of one independent physical-GC domain.</summary>
public sealed record FldBinSyncGcDomainReadiness(
    string Domain,
    bool Enabled,
    bool PrerequisitesSatisfied,
    IReadOnlyList<string> BlockedReasons)
{
    public bool ReadyForAttempt => Enabled && PrerequisitesSatisfied;
}

/// <summary>
/// Read-only GC readiness snapshot. It reports global prerequisites only; every
/// candidate is still independently proof-checked by the bounded compactor.
/// This report never enables a feature switch or authorizes deletion.
/// </summary>
public sealed record FldBinSyncAsyncGcReadinessReport(
    int NamespaceId,
    NodeId LocalReplica,
    bool HasAcceptedRoster,
    bool HasRetentionCheckpoint,
    bool HasInstalledFullReplaceBaseline,
    ulong? RosterGeneration,
    Guid? FullReplaceGeneration,
    int UnpublishedDraftCount,
    FldBinSyncGcDomainReadiness Receipts,
    FldBinSyncGcDomainReadiness ChangeIds,
    FldBinSyncGcDomainReadiness Tombstones);

/// <summary>Result of one bounded async Host-owned maintenance round.</summary>
public sealed record FldBinSyncAsyncGcPassResult(
    FldBinSyncAsyncGcReadinessReport Readiness,
    int ReceiptsReclaimed,
    int ChangeIdsReclaimed,
    int TombstonesReclaimed);

/// <summary>
/// Async host composition for one FLD/BIN-SYNC namespace. The caller owns the
/// process-lifetime Node, Metadata Store, IPC providers and transport sessions.
/// Startup recovery must complete before sessions are accepted.
/// </summary>
public sealed class FldBinSyncAsyncHostAdapter
{
    private readonly AsyncStorageSyncCoordinator _coordinator;
    private readonly IAsyncDataProvider _provider;
    private readonly IAsyncProviderRemoteBatchCommitStore _remoteApply;
    private readonly Action<Node> _persistNode;
    private readonly int _publicationLimit;

    public FldBinSyncAsyncHostAdapter(
        AsyncStorageSyncCoordinator coordinator,
        IAsyncDataProvider provider,
        IAsyncProviderRemoteBatchCommitStore remoteApply,
        Action<Node> persistNode,
        int publicationLimit = 1024)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _remoteApply = remoteApply ?? throw new ArgumentNullException(nameof(remoteApply));
        _persistNode = persistNode ?? throw new ArgumentNullException(nameof(persistNode));
        if (publicationLimit is < 1 or > 4096)
            throw new ArgumentOutOfRangeException(nameof(publicationLimit));
        _publicationLimit = publicationLimit;
    }

    /// <summary>
    /// Recover PendingApply and publish the captured provider feed before the host
    /// begins accepting peer sessions.
    /// </summary>
    public Task<AsyncStorageStartupRecoveryResult> RecoverBeforeAcceptingSessionsAsync(
        CancellationToken cancellationToken = default)
        => _coordinator.RecoverBeforeAcceptingSessionsAsync(
            _provider, _remoteApply, _persistNode, _publicationLimit, cancellationToken);

    public Task<int> PublishLocalChangesAsync(CancellationToken cancellationToken = default)
        => _coordinator.PublishLocalChangesAsync(
            _provider, _persistNode, _publicationLimit, cancellationToken);

    /// <summary>
    /// Inspect the global prerequisites for each GC domain without deleting rows or
    /// changing feature switches. The temporary Host reference scan is disposed before
    /// returning. Candidate-level proofs, reservations and exact-version checks remain
    /// the responsibility of the compaction transaction.
    /// </summary>
    public async Task<FldBinSyncAsyncGcReadinessReport> GetGcReadinessAsync(
        StorageNamespaceGcStore gcStore,
        TrustStore trustStore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(gcStore);
        ArgumentNullException.ThrowIfNull(trustStore);

        var common = new List<string>();
        if (!_coordinator.IsNamespaceGcGuardAttached(gcStore, trustStore))
        {
            try
            {
                _coordinator.AttachNamespaceGcGuard(gcStore, trustStore);
            }
            catch (Exception exception) when (exception is InvalidOperationException or
                NotSupportedException or ArgumentException)
            {
                common.Add("Exact Metadata/namespace-GC/TrustStore binding is unavailable: " +
                    exception.GetType().Name + ": " + exception.Message);
            }
        }

        int namespaceId = _coordinator.NamespaceId;
        NodeId localReplica = _coordinator.LocalReplicaId;
        NamespaceRosterCertificate? roster = null;
        NamespaceRetentionCheckpoint? checkpoint = null;
        bool hasRoster = false;
        bool hasCheckpoint = false;
        bool fullReplaceInstalled = false;

        try
        {
            hasRoster = gcStore.TryReadNamespaceRoster(namespaceId, trustStore, out roster) &&
                roster is not null;
            if (!hasRoster)
            {
                common.Add("No accepted, verifiable namespace roster is available.");
            }
            else
            {
                hasCheckpoint = gcStore.TryReadRetentionCheckpoint(
                    roster!, trustStore, out checkpoint) && checkpoint is not null;
                if (!hasCheckpoint)
                    common.Add("No accepted, verifiable retention checkpoint is available.");
                else if (checkpoint!.FullReplaceGeneration is Guid generation &&
                    checkpoint.FullReplaceDigest is { Length: NamespaceRosterProtocol.DigestSize } digest)
                {
                    fullReplaceInstalled = gcStore.AreAllActiveMembersInstalled(roster!, generation, digest);
                }
            }
        }
        catch (InvalidDataException exception)
        {
            common.Add("Signed namespace retention state is invalid: " + exception.Message);
        }

        int unpublishedDraftCount = 0;
        IAsyncApplicationGcReferenceScanner? referenceScanner =
            _provider as IAsyncApplicationGcReferenceScanner;
        if (referenceScanner is null)
        {
            common.Add("Provider does not expose the Host-owned asynchronous GC reference scanner.");
        }
        else
        {
            try
            {
                await using IAsyncApplicationGcReferenceScan scan =
                    await referenceScanner.OpenScanAsync(
                        _coordinator.MetadataScopeId, localReplica, namespaceId,
                        cancellationToken).ConfigureAwait(false);
                ApplicationDraftHorizon drafts =
                    await scan.GetUnpublishedLocalDraftHorizonAsync(cancellationToken)
                        .ConfigureAwait(false);
                unpublishedDraftCount = drafts.Count;
                if (drafts.Count != 0 || drafts.MaxHlcWall != 0 || drafts.MaxHlcLogical != 0)
                    common.Add($"Host reports an unpublished local draft horizon (count={drafts.Count}).");
            }
            catch (Exception exception) when (exception is IOException or InvalidOperationException or
                NotSupportedException or TimeoutException)
            {
                common.Add("Host reference scan could not be opened: " +
                    exception.GetType().Name + ": " + exception.Message);
            }
        }

        var receiptReasons = new List<string>(common);
        var changeIdReasons = new List<string>(common);
        var tombstoneReasons = new List<string>(common);
        if (_provider is not IAsyncApplicationGcReferenceScanner)
        {
            // The common scanner blocker already explains this for every domain.
        }
        if (_provider is not IAsyncProviderTombstoneGc)
            tombstoneReasons.Add("Provider does not expose process-isolated tombstone candidate/delete operations.");
        if (!hasCheckpoint || checkpoint is null ||
            checkpoint.FullReplaceGeneration is not Guid fullReplaceGeneration ||
            checkpoint.FullReplaceDigest is not { Length: NamespaceRosterProtocol.DigestSize } fullReplaceDigest)
        {
            tombstoneReasons.Add("No authoritative FULL_REPLACE generation/digest is covered by the accepted checkpoint.");
        }
        else if (!fullReplaceInstalled)
        {
            tombstoneReasons.Add("At least one Active roster member has not durably installed the exact FULL_REPLACE baseline.");
        }

        return new FldBinSyncAsyncGcReadinessReport(
            namespaceId,
            localReplica,
            hasRoster,
            hasCheckpoint,
            fullReplaceInstalled,
            roster?.RosterGeneration,
            checkpoint?.FullReplaceGeneration,
            unpublishedDraftCount,
            new FldBinSyncGcDomainReadiness(
                "Receipt", gcStore.IsReceiptGcProofEnabled, receiptReasons.Count == 0, receiptReasons),
            new FldBinSyncGcDomainReadiness(
                "ChangeId", gcStore.IsChangeIdGcProofEnabled, changeIdReasons.Count == 0, changeIdReasons),
            new FldBinSyncGcDomainReadiness(
                "Record/Field Tombstone", gcStore.IsTombstoneGcProofEnabled,
                tombstoneReasons.Count == 0, tombstoneReasons));
    }

    /// <summary>
    /// Execute one bounded page for each independently enabled and globally ready GC
    /// domain. Domains are sequential and cancellation is observed between them.
    /// Repeated calls resume from each store's durable compaction cursor.
    /// </summary>
    public async Task<FldBinSyncAsyncGcPassResult> RunGcMaintenanceAsync(
        StorageNamespaceGcStore gcStore,
        TrustStore trustStore,
        int limit = 256,
        CancellationToken cancellationToken = default)
    {
        if (limit is < 1 or > 4096)
            throw new ArgumentOutOfRangeException(nameof(limit));

        FldBinSyncAsyncGcReadinessReport readiness =
            await GetGcReadinessAsync(gcStore, trustStore, cancellationToken).ConfigureAwait(false);

        int receipts = 0;
        int changeIds = 0;
        int tombstones = 0;
        if (_provider is IAsyncApplicationGcReferenceScanner referenceScanner)
        {
            if (readiness.Receipts.ReadyForAttempt)
            {
                cancellationToken.ThrowIfCancellationRequested();
                receipts = await _coordinator.CompactReceiptsAsync(
                    referenceScanner, limit, cancellationToken).ConfigureAwait(false);
            }

            if (readiness.ChangeIds.ReadyForAttempt)
            {
                cancellationToken.ThrowIfCancellationRequested();
                changeIds = await _coordinator.CompactChangeLedgerAsync(
                    referenceScanner, limit, cancellationToken).ConfigureAwait(false);
            }
        }

        if (readiness.Tombstones.ReadyForAttempt &&
            _provider is IAsyncProviderTombstoneGc tombstoneGc)
        {
            cancellationToken.ThrowIfCancellationRequested();
            tombstones = await _coordinator.CompactTombstonesAsync(
                tombstoneGc, limit, cancellationToken).ConfigureAwait(false);
        }

        return new FldBinSyncAsyncGcPassResult(readiness, receipts, changeIds, tombstones);
    }

    /// <summary>
    /// Run one bounded process-isolated FLD tombstone-GC pass. Candidate discovery is
    /// performed from a disposable provider snapshot; the final IPC operation rechecks
    /// Host references and exact candidate versions under the FLD commit monitor.
    /// </summary>
    public Task<int> RunTombstoneGcPassAsync(
        StorageNamespaceGcStore gcStore,
        TrustStore trustStore,
        int limit = 256,
        CancellationToken cancellationToken = default)
    {
        if (_provider is not IAsyncProviderTombstoneGc tombstoneGc)
            throw new NotSupportedException(
                "The configured async provider does not expose the process-isolated tombstone-GC capability.");
        _coordinator.AttachNamespaceGcGuard(gcStore, trustStore);
        return _coordinator.CompactTombstonesAsync(tombstoneGc, limit, cancellationToken);
    }

    /// <summary>
    /// Run bounded ChangeId and receipt compaction using the provider's one-scan-at-a-time
    /// Host IPC reference capability. The two retention domains are evaluated separately.
    /// </summary>
    public async Task<(int ChangeIds, int Receipts)> RunMetadataGcPassAsync(
        StorageNamespaceGcStore gcStore,
        TrustStore trustStore,
        int limit = 256,
        CancellationToken cancellationToken = default)
    {
        if (_provider is not IAsyncApplicationGcReferenceScanner referenceScanner)
            throw new NotSupportedException(
                "The configured async provider does not expose Host-owned GC reference scans.");
        _coordinator.AttachNamespaceGcGuard(gcStore, trustStore);
        int receipts = await _coordinator.CompactReceiptsAsync(
            referenceScanner, limit, cancellationToken).ConfigureAwait(false);
        int changeIds = await _coordinator.CompactChangeLedgerAsync(
            referenceScanner, limit, cancellationToken).ConfigureAwait(false);
        return (changeIds, receipts);
    }

    public Task<int> RunChangeIdGcPassAsync(
        StorageNamespaceGcStore gcStore,
        TrustStore trustStore,
        int limit = 256,
        CancellationToken cancellationToken = default)
    {
        if (_provider is not IAsyncApplicationGcReferenceScanner referenceScanner)
            throw new NotSupportedException(
                "The configured async provider does not expose Host-owned GC reference scans.");
        _coordinator.AttachNamespaceGcGuard(gcStore, trustStore);
        return _coordinator.CompactChangeLedgerAsync(referenceScanner, limit, cancellationToken);
    }

    public Task<int> RunReceiptGcPassAsync(
        StorageNamespaceGcStore gcStore,
        TrustStore trustStore,
        int limit = 256,
        CancellationToken cancellationToken = default)
    {
        if (_provider is not IAsyncApplicationGcReferenceScanner referenceScanner)
            throw new NotSupportedException(
                "The configured async provider does not expose Host-owned GC reference scans.");
        _coordinator.AttachNamespaceGcGuard(gcStore, trustStore);
        return _coordinator.CompactReceiptsAsync(referenceScanner, limit, cancellationToken);
    }

    /// <summary>Receive one DELTA_BATCH; transport ACK follows durable FLD APPLY and Metadata finalization.</summary>
    public Task<BatchResult> ReceiveBatchAsync(
        SyncSession session,
        CancellationToken cancellationToken = default)
        => _coordinator.ReceiveBatchWithAsyncProviderBeforeAckAsync(
            session, _remoteApply, _persistNode, cancellationToken);

    /// <summary>
    /// Publish local provider changes, then drain each peer's immutable Metadata
    /// outbound/relay stream. Per-peer reservation/ACK serialization is enforced by
    /// AsyncStorageSyncCoordinator; different peers can progress concurrently.
    /// </summary>
    public async Task<int> RunOutboundRoundAsync(
        IReadOnlyCollection<SyncSession> sessions,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sessions);
        await PublishLocalChangesAsync(cancellationToken).ConfigureAwait(false);
        SyncSession[] peers = ValidateSessions(sessions);
        if (peers.Length == 0) return 0;

        int sent = 0;
        var results = await Task.WhenAll(peers.Select(peer =>
            DrainPeerAsync(peer, cancellationToken))).ConfigureAwait(false);
        foreach (int count in results) sent = checked(sent + count);
        return sent;
    }

    /// <summary>Receive one peer batch, then publish and fan out the resulting relay state.</summary>
    public async Task<BatchResult> ReceiveAndPublishAsync(
        SyncSession inboundSession,
        IReadOnlyCollection<SyncSession> sessions,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inboundSession);
        SyncSession[] peers = ValidateSessions(sessions);
        if (!peers.Contains(inboundSession, ReferenceEqualityComparer.Instance))
            throw new ArgumentException("Inbound session must belong to the current peer set.", nameof(inboundSession));

        BatchResult result = await ReceiveBatchAsync(inboundSession, cancellationToken).ConfigureAwait(false);
        await RunOutboundRoundAsync(peers, cancellationToken).ConfigureAwait(false);
        return result;
    }

    private async Task<int> DrainPeerAsync(SyncSession peer, CancellationToken cancellationToken)
    {
        int sent = 0;
        while (await _coordinator.PushNextBatchAsync(peer, ct: cancellationToken).ConfigureAwait(false))
            sent = checked(sent + 1);
        return sent;
    }

    private static SyncSession[] ValidateSessions(IReadOnlyCollection<SyncSession> sessions)
    {
        ArgumentNullException.ThrowIfNull(sessions);
        var unique = new HashSet<SyncSession>(ReferenceEqualityComparer.Instance);
        foreach (SyncSession session in sessions)
        {
            ArgumentNullException.ThrowIfNull(session);
            unique.Add(session);
        }
        return unique.ToArray();
    }
}
