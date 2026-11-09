using BinSync;
using BinSync.Storage;

namespace BinSync.FldHost;

/// <summary>
/// Host-side multi-session scheduler for one FLD/BIN-SYNC namespace. It enforces
/// one receive owner and one serialized sender per SyncSession, fans out each
/// publication-gate ceiling set to every current peer, and optionally drives
/// periodic outbound rounds. Transport creation and session disposal remain with
/// the application.
/// </summary>
public sealed class FldBinSyncSessionScheduler : IAsyncDisposable
{
    private sealed class SessionGates
    {
        public readonly SemaphoreSlim Send = new(1, 1);
        public readonly SemaphoreSlim Receive = new(1, 1);
    }

    private readonly object _lifecycleGate = new();
    private readonly FldBinSyncHostAdapter _adapter;
    private readonly Dictionary<SyncSession, SessionGates> _sessionGates =
        new(ReferenceEqualityComparer.Instance);
    private bool _disposed;

    public FldBinSyncSessionScheduler(FldBinSyncHostAdapter adapter)
        => _adapter = adapter ?? throw new ArgumentNullException(nameof(adapter));

    /// <summary>
    /// One namespace-wide outbound gate round. Every peer receives the same
    /// immutable author ceilings; a failure leaves the round retryable.
    /// </summary>
    public ValueTask RunOutboundRoundAsync(
        IReadOnlyCollection<SyncSession> sessions,
        Node sender,
        Action<Node> persistNode,
        IMetadataPeerStateStore? peerStateStore = null,
        int limit = 1024,
        int schemaVersion = 1,
        CancellationToken cancellationToken = default)
    {
        SyncSession[] peers = ValidateSessions(sessions);
        ArgumentNullException.ThrowIfNull(sender);
        ArgumentNullException.ThrowIfNull(persistNode);
        return _adapter.RunOutboundAsync(
            persistNode,
            (ceilings, token) => PublishToPeersAsync(
                peers, sender, peerStateStore, ceilings, schemaVersion, token),
            limit,
            cancellationToken);
    }

    /// <summary>
    /// Receives and durably applies one batch from one peer. A second receive call
    /// for the same session waits; sends to that session remain duplex and serialized.
    /// </summary>
    public async ValueTask<bool> ReceiveAndPublishOneAsync(
        SyncSession inboundSession,
        IReadOnlyCollection<SyncSession> allSessions,
        Node sender,
        Action<Node> persistNode,
        IMetadataPeerStateStore? peerStateStore = null,
        int limit = 1024,
        int schemaVersion = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inboundSession);
        ArgumentNullException.ThrowIfNull(sender);
        ArgumentNullException.ThrowIfNull(persistNode);
        SyncSession[] peers = ValidateSessions(allSessions);
        if (!peers.Contains(inboundSession, ReferenceEqualityComparer.Instance))
            throw new ArgumentException("Inbound session must belong to the scheduler's current peer set.", nameof(inboundSession));

        SessionGates gates = GetGates(inboundSession);
        await gates.Receive.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await _adapter.ReceiveAndPublishAllAsync(
                inboundSession,
                persistNode,
                (ceilings, token) => PublishToPeersAsync(
                    peers, sender, peerStateStore, ceilings, schemaVersion, token),
                limit,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gates.Receive.Release();
        }
    }

    /// <summary>
    /// Keeps one receive loop per supplied session. A false result means the
    /// publication gate failed closed and startup/source reconciliation is required.
    /// </summary>
    public async Task RunInboundLoopAsync(
        SyncSession inboundSession,
        Func<IReadOnlyCollection<SyncSession>> getCurrentSessions,
        Node sender,
        Action<Node> persistNode,
        IMetadataPeerStateStore? peerStateStore = null,
        int limit = 1024,
        int schemaVersion = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(getCurrentSessions);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IReadOnlyCollection<SyncSession> peers = getCurrentSessions()
                ?? throw new InvalidOperationException("The host returned a null session collection.");
            bool completed = await ReceiveAndPublishOneAsync(
                inboundSession, peers, sender, persistNode, peerStateStore,
                limit, schemaVersion, cancellationToken).ConfigureAwait(false);
            if (!completed)
                return;
        }
    }

    /// <summary>Runs outbound publication rounds at the host-selected cadence.</summary>
    public async Task RunPeriodicOutboundAsync(
        Func<IReadOnlyCollection<SyncSession>> getCurrentSessions,
        Node sender,
        Action<Node> persistNode,
        TimeSpan interval,
        IMetadataPeerStateStore? peerStateStore = null,
        int limit = 1024,
        int schemaVersion = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(getCurrentSessions);
        ArgumentNullException.ThrowIfNull(sender);
        ArgumentNullException.ThrowIfNull(persistNode);
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval));
        using var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            IReadOnlyCollection<SyncSession> peers = getCurrentSessions()
                ?? throw new InvalidOperationException("The host returned a null session collection.");
            await RunOutboundRoundAsync(
                peers, sender, persistNode, peerStateStore, limit, schemaVersion, cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_lifecycleGate)
        {
            if (_disposed) return ValueTask.CompletedTask;
            _disposed = true;
            // Do not dispose per-session semaphores while an inbound callback may be
            // waiting for that same session's send gate. Active calls retain their gate
            // objects until completion; clearing the registry releases idle gates to GC.
            _sessionGates.Clear();
        }
        return ValueTask.CompletedTask;
    }

    private async ValueTask PublishToPeersAsync(
        SyncSession[] peers,
        Node sender,
        IMetadataPeerStateStore? peerStateStore,
        IReadOnlyList<StorageRelayAuthorPublicationWatermark> ceilings,
        int schemaVersion,
        CancellationToken cancellationToken)
    {
        if (peers.Length == 0) return;
        Task[] sends = new Task[peers.Length];
        for (int i = 0; i < peers.Length; i++)
            sends[i] = PublishOneAsync(
                peers[i], sender, peerStateStore, ceilings, schemaVersion, cancellationToken);
        await Task.WhenAll(sends).ConfigureAwait(false);
    }

    private async Task PublishOneAsync(
        SyncSession session,
        Node sender,
        IMetadataPeerStateStore? peerStateStore,
        IReadOnlyList<StorageRelayAuthorPublicationWatermark> ceilings,
        int schemaVersion,
        CancellationToken cancellationToken)
    {
        SessionGates gates = GetGates(session);
        await gates.Send.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _adapter.PublishReadyOutboxAsync(
                session, sender, peerStateStore, ceilings, schemaVersion, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            gates.Send.Release();
        }
    }

    private SyncSession[] ValidateSessions(IReadOnlyCollection<SyncSession> sessions)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sessions);
        var unique = new HashSet<SyncSession>(ReferenceEqualityComparer.Instance);
        foreach (SyncSession session in sessions)
        {
            ArgumentNullException.ThrowIfNull(session);
            unique.Add(session);
        }
        return unique.ToArray();
    }

    private SessionGates GetGates(SyncSession session)
    {
        lock (_lifecycleGate)
        {
            ThrowIfDisposed();
            if (_sessionGates.TryGetValue(session, out SessionGates? gates))
                return gates;
            gates = new SessionGates();
            _sessionGates.Add(session, gates);
            return gates;
        }
    }

    private void ThrowIfDisposed()
    {
        lock (_lifecycleGate)
            ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
