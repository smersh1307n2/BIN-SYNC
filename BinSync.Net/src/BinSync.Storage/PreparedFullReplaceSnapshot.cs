namespace BinSync.Storage;

/// <summary>
/// One immutable provider snapshot and its signed FULL_REPLACE manifest, prepared once
/// and reusable for every recipient in the same namespace generation. The instance owns
/// the provider snapshot token and releases it when disposed.
/// Do not dispose while a SendPreparedSnapshotAsync call is in progress.
/// </summary>
public sealed class PreparedFullReplaceSnapshot : IDisposable
{
    private IDataProvider? _provider;

    internal PreparedFullReplaceSnapshot(
        IDataProvider provider,
        ProviderSnapshotHandle snapshot,
        FullReplaceTransferProtocol.Begin manifest)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        Manifest = manifest ?? throw new ArgumentNullException(nameof(manifest));
    }

    internal IDataProvider Provider
        => Volatile.Read(ref _provider)
            ?? throw new ObjectDisposedException(nameof(PreparedFullReplaceSnapshot));

    internal ProviderSnapshotHandle Snapshot { get; }

    internal FullReplaceTransferProtocol.Begin Manifest { get; }

    public int NamespaceId => Manifest.NamespaceId;
    public Guid Generation => Manifest.Generation;
    public ulong RosterGeneration => Manifest.RosterGeneration;
    public ulong SourceChangeFeedWatermark => Manifest.SourceChangeFeedWatermark;
    public int ExpectedEntryCount => Manifest.ExpectedEntryCount;
    public byte[] Digest => Manifest.ExpectedDigest.ToArray();
    public byte[] RosterDigest => Manifest.RosterDigest.ToArray();

    public void Dispose()
    {
        IDataProvider? provider = Interlocked.Exchange(ref _provider, null);
        provider?.ReleaseSnapshot(Snapshot.Token);
    }
}
