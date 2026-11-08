using System.Security.Cryptography;
using System.Text;

namespace Fld1.Storage.Wal.IO;

/// <summary>
/// Holds a named, OS-managed single-writer mutex for the lifetime of one WAL
/// partition. A dedicated owner thread is used because Mutex ownership is
/// thread-affine, while StorageDatabase may be disposed by any caller thread.
/// The OS releases the mutex automatically if the process terminates.
/// </summary>
internal sealed class WalWriterFence : IDisposable
{
    private readonly Mutex _mutex;
    private readonly ManualResetEventSlim _ready = new(false);
    private readonly ManualResetEventSlim _release = new(false);
    private readonly Thread _ownerThread;
    private Exception? _failure;
    private int _disposed;

    private WalWriterFence(string name)
    {
        _mutex = new Mutex(initiallyOwned: false, name);
        _ownerThread = new Thread(AcquireAndHold)
        {
            IsBackground = true,
            Name = "Fld1.Storage.WalWriterFence"
        };

        try
        {
            _ownerThread.Start();
        }
        catch
        {
            _mutex.Dispose();
            _ready.Dispose();
            _release.Dispose();
            throw;
        }

        _ready.Wait();
        if (_failure is not null)
        {
            _release.Set();
            _ownerThread.Join();
            _mutex.Dispose();
            _ready.Dispose();
            _release.Dispose();
            throw new IOException("WAL partition is already open for writing.", _failure);
        }
    }

    public static WalWriterFence Acquire(string walDirectory, ulong partitionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(walDirectory);
        string fullPath = Path.GetFullPath(walDirectory);
        if (OperatingSystem.IsWindows() || OperatingSystem.IsMacOS())
            fullPath = fullPath.ToUpperInvariant();

        string identity = fullPath + "|" + partitionId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        string digest = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity)));
        string name = OperatingSystem.IsWindows()
            ? "Global\\Fld1Storage_" + digest
            : "Fld1Storage_" + digest;
        return new WalWriterFence(name);
    }

    private void AcquireAndHold()
    {
        try
        {
            bool acquired;
            try
            {
                acquired = _mutex.WaitOne(0);
            }
            catch (AbandonedMutexException)
            {
                // The prior process terminated while holding the fence. Ownership
                // is granted to this thread, and WAL recovery runs under the lock.
                acquired = true;
            }

            if (!acquired)
            {
                _failure = new IOException("Another process owns the WAL partition writer fence.");
                return;
            }

            _ready.Set();
            _release.Wait();
            _mutex.ReleaseMutex();
        }
        catch (Exception ex)
        {
            _failure = ex;
        }
        finally
        {
            _ready.Set();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _release.Set();
        _ownerThread.Join();
        _mutex.Dispose();
        _ready.Dispose();
        _release.Dispose();
    }
}
