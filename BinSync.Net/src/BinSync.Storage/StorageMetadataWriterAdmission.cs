using System.Runtime.CompilerServices;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// One writer-admission boundary for every Metadata adapter attached to the same
/// StorageDatabase. Normal synchronous mutations are re-entrant on their calling
/// thread; async GC owns the semaphore across the complete Host reference-scan and
/// final Metadata delete interval.
/// </summary>
internal sealed class StorageMetadataWriterAdmission
{
    private static readonly ConditionalWeakTable<StorageDatabase, StorageMetadataWriterAdmission> Gates = new();

    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly ThreadLocal<int> _syncDepth = new(() => 0);

    public static StorageMetadataWriterAdmission For(StorageDatabase storage)
    {
        ArgumentNullException.ThrowIfNull(storage);
        return Gates.GetValue(storage, static _ => new StorageMetadataWriterAdmission());
    }

    public IDisposable Enter()
    {
        int depth = _syncDepth.Value;
        if (depth != 0)
        {
            _syncDepth.Value = checked(depth + 1);
            return new SyncLease(this);
        }

        _semaphore.Wait();
        _syncDepth.Value = 1;
        return new SyncLease(this);
    }

    public async ValueTask<IDisposable> EnterAsync(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new AsyncLease(_semaphore);
    }

    private void ExitSync()
    {
        int depth = _syncDepth.Value;
        if (depth <= 0)
            throw new SynchronizationLockException("Metadata writer-admission lease was released outside its owning call.");

        depth--;
        _syncDepth.Value = depth;
        if (depth == 0)
            _semaphore.Release();
    }

    private sealed class SyncLease(StorageMetadataWriterAdmission owner) : IDisposable
    {
        private StorageMetadataWriterAdmission? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.ExitSync();
    }

    private sealed class AsyncLease(SemaphoreSlim semaphore) : IDisposable
    {
        private SemaphoreSlim? _semaphore = semaphore;

        public void Dispose()
            => Interlocked.Exchange(ref _semaphore, null)?.Release();
    }
}
