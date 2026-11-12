using BinSync;
using BinSync.Storage;

namespace BinSync.FldHost;

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
