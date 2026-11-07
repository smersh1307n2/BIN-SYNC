namespace BinSync;

/// <summary>
/// A session-exclusive set of reconciliation primitives. The scope keeps all
/// reconciliation frame types reserved for the duration of a custom exchange.
/// Batch push/receive remains independent.
/// </summary>
public partial class SyncSession
{
    /// <summary>
    /// Executes a custom multi-frame reconciliation exchange while reserving all
    /// reconciliation frame types. Use this when composing low-level operations
    /// into one protocol transaction.
    /// </summary>
    public async Task<TResult> RunReconciliationAsync<TResult>(
        Func<ReconciliationOperations, CancellationToken, Task<TResult>> operation,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        await _reconcileGate.WaitAsync(ct).ConfigureAwait(false);
        bool stateDigestHeld = false;
        bool needRangeHeld = false;
        bool needKeysHeld = false;
        bool snapshotHeld = false;
        ReconciliationOperations? operations = null;
        try
        {
            await _stateDigestGate.WaitAsync(ct).ConfigureAwait(false);
            stateDigestHeld = true;
            await _needRangeGate.WaitAsync(ct).ConfigureAwait(false);
            needRangeHeld = true;
            await _needKeysGate.WaitAsync(ct).ConfigureAwait(false);
            needKeysHeld = true;
            await _snapshotGate.WaitAsync(ct).ConfigureAwait(false);
            snapshotHeld = true;

            operations = new ReconciliationOperations(this);
            return await operation(operations, ct).ConfigureAwait(false);
        }
        finally
        {
            operations?.End();
            if (snapshotHeld) _snapshotGate.Release();
            if (needKeysHeld) _needKeysGate.Release();
            if (needRangeHeld) _needRangeGate.Release();
            if (stateDigestHeld) _stateDigestGate.Release();
            _reconcileGate.Release();
        }
    }

    /// <summary>Operations available inside a <see cref="RunReconciliationAsync{TResult}"/> scope.</summary>
    public sealed class ReconciliationOperations
    {
        private readonly SyncSession _session;
        private int _active = 1;

        internal ReconciliationOperations(SyncSession session) => _session = session;

        internal void End() => Interlocked.Exchange(ref _active, 0);

        private void EnsureActive() =>
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _active) == 0, this);

        /// <summary>Exchange STATE_DIGEST while this scope owns its frame type.</summary>
        public Task<(StateDigestMessage Local, StateDigestMessage Remote, List<byte[]> Diverged)>
            ExchangeStateDigestAsync(Node local, int namespaceId, bool initiate, CancellationToken ct = default)
        {
            EnsureActive();
            return _session.ExchangeStateDigestCoreAsync(local, namespaceId, initiate, ct);
        }

        /// <summary>Send NEED_RANGE while this scope owns its frame type.</summary>
        public Task SendNeedRangeAsync(int namespaceId, byte[] prefix, CancellationToken ct = default)
        {
            EnsureActive();
            return _session.SendNeedRangeCoreAsync(namespaceId, prefix, ct);
        }

        /// <summary>Receive NEED_RANGE while this scope owns its frame type.</summary>
        public Task<NeedRangeMessage> ReceiveNeedRangeAsync(CancellationToken ct = default)
        {
            EnsureActive();
            return _session.ReceiveNeedRangeCoreAsync(ct);
        }

        /// <summary>Send NEED_KEYS while this scope owns its frame type.</summary>
        public Task SendNeedKeysAsync(NeedKeysMessage keys, CancellationToken ct = default)
        {
            EnsureActive();
            return _session.SendNeedKeysCoreAsync(keys, ct);
        }

        /// <summary>Receive NEED_KEYS while this scope owns its frame type.</summary>
        public Task<NeedKeysMessage> ReceiveNeedKeysAsync(CancellationToken ct = default)
        {
            EnsureActive();
            return _session.ReceiveNeedKeysCoreAsync(ct);
        }

        /// <summary>Send a complete snapshot transfer while this scope owns its frame type.</summary>
        public Task SendSnapshotAsync(Node local, int namespaceId, byte[]? prefix = null, CancellationToken ct = default)
        {
            EnsureActive();
            return _session.SendSnapshotCoreAsync(local, namespaceId, prefix, ct);
        }

        /// <summary>Receive and apply a complete snapshot transfer while this scope owns its frame type.</summary>
        public Task<int> ReceiveSnapshotAsync(Node local, CancellationToken ct = default)
        {
            EnsureActive();
            return _session.ReceiveSnapshotCoreAsync(local, ct);
        }

        /// <summary>Fulfill NEED_RANGE using republished DELTA_BATCH frames.</summary>
        public Task<int> FulfillNeedRangeAsync(Node local, NeedRangeMessage need, CancellationToken ct = default)
        {
            EnsureActive();
            return _session.FulfillNeedRangeAsync(local, need, ct);
        }

        /// <summary>Fulfill NEED_RANGE using a snapshot transfer.</summary>
        public Task FulfillNeedRangeWithSnapshotAsync(
            Node local, NeedRangeMessage need, CancellationToken ct = default)
        {
            EnsureActive();
            return _session.SendSnapshotCoreAsync(local, need.NamespaceId, need.Prefix, ct);
        }
    }
}
