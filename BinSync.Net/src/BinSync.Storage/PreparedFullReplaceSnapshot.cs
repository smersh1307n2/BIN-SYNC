namespace BinSync.Storage;

/// <summary>
/// One immutable provider snapshot and its signed FULL_REPLACE manifest, prepared once
/// and reusable for every recipient in the same namespace generation. The instance owns
/// the provider snapshot token. Dispose requests release; active send leases keep the
/// token alive until their transfer exits, including on cancellation or failure.
/// </summary>
public sealed class PreparedFullReplaceSnapshot : IDisposable
{
    private readonly object _gate = new();
    private IDataProvider? _provider;
    private int _activeTransfers;
    private bool _disposeRequested;
    private bool _preserveOnDispose;

    internal PreparedFullReplaceSnapshot(
        IDataProvider provider,
        ProviderSnapshotHandle snapshot,
        FullReplaceTransferProtocol.Begin manifest,
        ulong hlcWallFloor,
        uint hlcLogicalFloor)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        Manifest = manifest ?? throw new ArgumentNullException(nameof(manifest));
        HlcWallFloor = hlcWallFloor;
        HlcLogicalFloor = hlcLogicalFloor;
    }

    internal IDataProvider Provider
    {
        get
        {
            lock (_gate)
                return _provider
                    ?? throw new ObjectDisposedException(nameof(PreparedFullReplaceSnapshot));
        }
    }

    internal ProviderSnapshotHandle Snapshot { get; }

    internal FullReplaceTransferProtocol.Begin Manifest { get; }

    public int NamespaceId => Manifest.NamespaceId;
    public Guid Generation => Manifest.Generation;
    public ulong RosterGeneration => Manifest.RosterGeneration;
    public ulong SourceChangeFeedWatermark => Manifest.SourceChangeFeedWatermark;
    public int ExpectedEntryCount => Manifest.ExpectedEntryCount;
    /// <summary>Maximum HLC found in the exact immutable provider snapshot.</summary>
    public ulong HlcWallFloor { get; }
    public uint HlcLogicalFloor { get; }
    public byte[] Digest => Manifest.ExpectedDigest.ToArray();
    public byte[] RosterDigest => Manifest.RosterDigest.ToArray();

    /// <summary>
    /// Marks this prepared generation as durably persisted. Ordinary Dispose then
    /// detaches the in-process provider handle but preserves the snapshot files and
    /// Metadata Store record for recovery after process restart.
    /// </summary>
    internal void MarkPersisted()
    {
        lock (_gate)
        {
            if (_disposeRequested || _provider is null)
                throw new ObjectDisposedException(nameof(PreparedFullReplaceSnapshot));
            _preserveOnDispose = true;
        }
    }

    /// <summary>Permanently releases a persisted generation after every transfer has finished.</summary>
    internal void ReleasePermanently(IDataProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        lock (_gate)
        {
            if (_activeTransfers != 0)
                throw new InvalidOperationException("Cannot permanently release a FULL_REPLACE snapshot while transfers are active.");
            _disposeRequested = true;
            _preserveOnDispose = false;
            _provider = null;
        }
        provider.ReleaseSnapshot(Snapshot.Token);
    }

    /// <summary>
    /// Retain the source provider snapshot while one transfer is using it. A concurrent
    /// Dispose is deferred until the last active transfer releases its lease.
    /// </summary>
    internal IDisposable AcquireTransferLease()
    {
        lock (_gate)
        {
            if (_disposeRequested || _provider is null)
                throw new ObjectDisposedException(nameof(PreparedFullReplaceSnapshot));
            _activeTransfers = checked(_activeTransfers + 1);
            return new TransferLease(this);
        }
    }

    public void Dispose()
    {
        IDataProvider? release = null;
        lock (_gate)
        {
            if (_disposeRequested)
                return;
            _disposeRequested = true;
            if (_activeTransfers == 0)
            {
                if (!_preserveOnDispose)
                    release = _provider;
                _provider = null;
            }
        }

        release?.ReleaseSnapshot(Snapshot.Token);
    }

    private void ReleaseTransferLease()
    {
        IDataProvider? release = null;
        lock (_gate)
        {
            if (_activeTransfers <= 0)
                throw new InvalidOperationException("FULL_REPLACE transfer lease underflow.");
            _activeTransfers--;
            if (_disposeRequested && _activeTransfers == 0)
            {
                if (!_preserveOnDispose)
                    release = _provider;
                _provider = null;
            }
        }

        release?.ReleaseSnapshot(Snapshot.Token);
    }

    private sealed class TransferLease : IDisposable
    {
        private PreparedFullReplaceSnapshot? _owner;

        public TransferLease(PreparedFullReplaceSnapshot owner) => _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.ReleaseTransferLease();
    }
}
