using BinSync;

namespace BinSync.Storage;

/// <summary>
/// Explicit in-process bridge from the synchronous reference provider contract to
/// IAsyncDataProvider. Calls complete synchronously and never block on asynchronous
/// work; process-isolated providers must implement IAsyncDataProvider directly.
/// </summary>
public sealed class InProcessAsyncDataProviderAdapter : IAsyncDataProvider
{
    private readonly IDataProvider _provider;
    private readonly bool _ownsProvider;
    private int _disposed;

    public InProcessAsyncDataProviderAdapter(IDataProvider provider, bool ownsProvider = false)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _ownsProvider = ownsProvider;
    }

    public ValueTask<RecordId> ResolveRecordIdAsync(
        ReadOnlyMemory<byte> sourceItemKey, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(_provider.ResolveRecordId(sourceItemKey));
    }

    public ValueTask<ushort> ResolveFieldIdAsync(
        ReadOnlyMemory<byte> sourceFieldKey, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(_provider.ResolveFieldId(sourceFieldKey));
    }

    public ValueTask AppendLocalChangeAsync(
        ProviderMutation mutation, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        _provider.AppendLocalChange(mutation);
        return ValueTask.CompletedTask;
    }

    public ValueTask<ProviderChangePage> ReadLocalChangesAsync(
        ulong afterSequence, int limit, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(_provider.ReadLocalChanges(afterSequence, limit));
    }

    public ValueTask AcknowledgeLocalChangesAsync(
        ulong safeThroughSequence, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        _provider.AcknowledgeLocalChanges(safeThroughSequence);
        return ValueTask.CompletedTask;
    }

    public ValueTask<ulong> GetLocalChangeHighWatermarkAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(_provider.LocalChangeHighWatermark);
    }

    public ValueTask<ProviderSnapshotHandle> OpenSnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(_provider.OpenSnapshot());
    }

    public ValueTask<ProviderSnapshotPage> ReadSnapshotAsync(
        Guid snapshotToken, int afterIndex, int limit,
        CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(_provider.ReadSnapshot(snapshotToken, afterIndex, limit));
    }

    public ValueTask ReleaseSnapshotAsync(
        Guid snapshotToken, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        _provider.ReleaseSnapshot(snapshotToken);
        return ValueTask.CompletedTask;
    }

    public ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(
        Guid applyToken, IReadOnlyList<ProviderMutation> mutations,
        CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(_provider.ApplyRemoteBatch(applyToken, mutations));
    }

    public ValueTask<ProviderMutation?> ReadCurrentAsync(
        RecordId recordId, ushort fieldId, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return ValueTask.FromResult(
            _provider.TryReadCurrent(recordId, fieldId, out ProviderMutation? mutation)
                ? mutation : null);
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0 && _ownsProvider)
            _provider.Dispose();
        return ValueTask.CompletedTask;
    }

    private void ThrowIfUnavailable(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
    }
}

/// <summary>
/// In-process counterpart for providers that expose the richer synchronous remote
/// batch commit capability. It preserves the full immutable batch context.
/// </summary>
public sealed class InProcessAsyncRemoteBatchCommitStoreAdapter : IAsyncProviderRemoteBatchCommitStore
{
    private readonly IProviderRemoteBatchCommitStore _store;

    public InProcessAsyncRemoteBatchCommitStoreAdapter(IProviderRemoteBatchCommitStore store)
        => _store = store ?? throw new ArgumentNullException(nameof(store));

    public ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(
        ProviderRemoteBatchCommit batch, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_store.ApplyRemoteBatch(batch));
    }
}
