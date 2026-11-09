using BinSync;

namespace BinSync.Storage;

/// <summary>
/// Namespace-scoped IProviderPublicationMode view over the durable Metadata Store.
/// The cutover marker is stored in a dedicated Storage keyspace and is therefore
/// not lost when Metadata Store partition rows are compacted or rewritten.
/// </summary>
public sealed class StorageMetadataPublicationMode : IProviderPublicationMode
{
    private readonly StorageMetadataStore _store;
    private readonly string _scopeId;
    private readonly NodeId _localReplica;
    private readonly int _namespaceId;

    public StorageMetadataPublicationMode(
        StorageMetadataStore store,
        string scopeId,
        NodeId localReplica,
        int namespaceId)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica must be non-zero.", nameof(localReplica));
        _ = checked((ushort)namespaceId);
        _scopeId = scopeId;
        _localReplica = localReplica;
        _namespaceId = namespaceId;
    }

    public bool IsBoundTo(StorageMetadataStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        return ReferenceEquals(_store, store);
    }

    public bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark)
        => _store.TryGetProviderPublicationCutover(
            _scopeId, _localReplica, _namespaceId, out nodeOutboxHighWatermark);

    public void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark)
        => _store.EnableProviderPublicationCutover(
            _scopeId, _localReplica, _namespaceId, nodeOutboxHighWatermark);
}
