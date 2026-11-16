namespace BinSync.FldHost;

/// <summary>Independent runtime kill switches for proof-gated physical-GC domains.</summary>
public enum FldBinSyncGcDomain
{
    Receipt,
    ChangeId,
    Tombstone
}

/// <summary>
/// Process-local emergency control. It starts stopped with every domain disabled.
/// Start only opens the global gate; each domain must then be enabled separately.
/// Stop cancels in-flight domain tokens and closes all domain gates immediately.
/// Cancellation callbacks are invoked outside the state lock.
/// This is intentionally not durable configuration: production must persist and audit
/// operator intent outside this object, and construct the GC store with matching
/// StorageNamespaceGcFeatureOptions.
/// </summary>
public sealed class FldBinSyncGcMaintenanceControl : IDisposable
{
    private readonly object _gate = new();
    private CancellationTokenSource _stopSource = new();
    private bool _running;
    private bool _receiptEnabled;
    private bool _changeIdEnabled;
    private bool _tombstoneEnabled;
    private bool _disposed;

    public bool IsRunning { get { lock (_gate) return _running && !_disposed; } }

    public void Start()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            _running = true;
        }
    }

    /// <summary>Emergency stop: closes every domain and cancels active page operations.</summary>
    public void Stop()
    {
        CancellationTokenSource sourceToCancel;
        lock (_gate)
        {
            if (_disposed) return;
            _running = false;
            _receiptEnabled = false;
            _changeIdEnabled = false;
            _tombstoneEnabled = false;

            // Rotate while holding the state lock. A concurrent Start can safely reopen
            // the new generation while cancellation callbacks for the old one execute.
            sourceToCancel = _stopSource;
            _stopSource = new CancellationTokenSource();
        }

        CancelAndDispose(sourceToCancel);
    }

    public void SetEnabled(FldBinSyncGcDomain domain, bool enabled)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (enabled && !_running)
                throw new InvalidOperationException("Start the GC maintenance control before enabling a domain.");
            switch (domain)
            {
                case FldBinSyncGcDomain.Receipt: _receiptEnabled = enabled; break;
                case FldBinSyncGcDomain.ChangeId: _changeIdEnabled = enabled; break;
                case FldBinSyncGcDomain.Tombstone: _tombstoneEnabled = enabled; break;
                default: throw new ArgumentOutOfRangeException(nameof(domain));
            }
        }
    }

    public bool IsEnabled(FldBinSyncGcDomain domain)
    {
        lock (_gate)
        {
            if (!_running || _disposed) return false;
            return domain switch
            {
                FldBinSyncGcDomain.Receipt => _receiptEnabled,
                FldBinSyncGcDomain.ChangeId => _changeIdEnabled,
                FldBinSyncGcDomain.Tombstone => _tombstoneEnabled,
                _ => throw new ArgumentOutOfRangeException(nameof(domain))
            };
        }
    }

    /// <summary>Creates a caller-linked token only while this domain's runtime gate is open.</summary>
    public CancellationTokenSource? TryCreateLinkedTokenSource(
        FldBinSyncGcDomain domain, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_disposed || !_running || !IsEnabledCore(domain))
                return null;
            return CancellationTokenSource.CreateLinkedTokenSource(
                _stopSource.Token, cancellationToken);
        }
    }

    private bool IsEnabledCore(FldBinSyncGcDomain domain) => domain switch
    {
        FldBinSyncGcDomain.Receipt => _receiptEnabled,
        FldBinSyncGcDomain.ChangeId => _changeIdEnabled,
        FldBinSyncGcDomain.Tombstone => _tombstoneEnabled,
        _ => throw new ArgumentOutOfRangeException(nameof(domain))
    };

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(FldBinSyncGcMaintenanceControl));
    }

    private static void CancelAndDispose(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        finally
        {
            source.Dispose();
        }
    }

    public void Dispose()
    {
        CancellationTokenSource sourceToCancel;
        lock (_gate)
        {
            if (_disposed) return;
            _running = false;
            _receiptEnabled = _changeIdEnabled = _tombstoneEnabled = false;
            _disposed = true;
            sourceToCancel = _stopSource;
        }

        CancelAndDispose(sourceToCancel);
    }
}
