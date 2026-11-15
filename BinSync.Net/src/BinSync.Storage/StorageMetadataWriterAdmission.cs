using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// One writer-admission boundary for every Metadata adapter attached to the same
/// StorageDatabase. Synchronous mutations are re-entrant on their owning thread;
/// async GC owns the semaphore across the complete Host reference-scan and final
/// Metadata delete interval.
/// </summary>
internal sealed class StorageMetadataWriterAdmission
{
    private static readonly ConditionalWeakTable<StorageDatabase, StorageMetadataWriterAdmission> Gates = new();

    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly ConcurrentDictionary<int, int> _syncDepthByThread = new();

    public static StorageMetadataWriterAdmission For(StorageDatabase storage)
    {
        ArgumentNullException.ThrowIfNull(storage);
        return Gates.GetValue(storage, static _ => new StorageMetadataWriterAdmission());
    }

    public IDisposable Enter()
    {
        int threadId = Environment.CurrentManagedThreadId;
        if (_syncDepthByThread.TryGetValue(threadId, out int depth))
        {
            _syncDepthByThread[threadId] = checked(depth + 1);
            return new SyncLease(this, threadId);
        }

        _semaphore.Wait();
        if (!_syncDepthByThread.TryAdd(threadId, 1))
        {
            _semaphore.Release();
            throw new SynchronizationLockException("Metadata writer-admission ownership was concurrently replaced.");
        }
        return new SyncLease(this, threadId);
    }

    public async ValueTask<IDisposable> EnterAsync(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new AsyncLease(_semaphore);
    }

    private void ExitSync(int threadId)
    {
        if (threadId != Environment.CurrentManagedThreadId ||
            !_syncDepthByThread.TryGetValue(threadId, out int depth) ||
            depth <= 0)
            throw new SynchronizationLockException("Metadata writer-admission lease was released outside its owning thread.");

        if (depth == 1)
        {
            if (!_syncDepthByThread.TryRemove(threadId, out _))
                throw new SynchronizationLockException("Metadata writer-admission ownership was lost.");
            _semaphore.Release();
        }
        else
        {
            _syncDepthByThread[threadId] = depth - 1;
        }
    }

    private sealed class SyncLease(
        StorageMetadataWriterAdmission owner, int threadId) : IDisposable
    {
        private StorageMetadataWriterAdmission? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.ExitSync(threadId);
    }

    private sealed class AsyncLease(SemaphoreSlim semaphore) : IDisposable
    {
        private SemaphoreSlim? _semaphore = semaphore;

        public void Dispose()
            => Interlocked.Exchange(ref _semaphore, null)?.Release();
    }
}
