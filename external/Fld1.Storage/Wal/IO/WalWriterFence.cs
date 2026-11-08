using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Fld1.Storage.Wal.IO;

/// <summary>
/// Holds a named, OS-managed single-writer mutex for one WAL partition. A single
/// process-wide owner thread acquires and releases every named Mutex so callers may
/// dispose StorageDatabase from any thread without violating Mutex thread affinity.
/// The OS releases all held mutexes if the process terminates.
/// </summary>
internal sealed class WalWriterFence : IDisposable
{
    private static readonly Lazy<Manager> SharedManager = new(static () => new Manager());
    private readonly Guid _token;
    private int _disposed;

    private WalWriterFence(Guid token) => _token = token;

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

        Guid token = SharedManager.Value.Acquire(name);
        return new WalWriterFence(token);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        SharedManager.Value.Release(_token);
    }

    private sealed class Manager
    {
        private readonly BlockingCollection<Request> _requests = new();
        private readonly Dictionary<Guid, Mutex> _held = new();

        public Manager()
        {
            var owner = new Thread(ProcessRequests)
            {
                IsBackground = true,
                Name = "Fld1.Storage.WalFenceManager"
            };
            owner.Start();
        }

        public Guid Acquire(string name)
        {
            var request = new Request(name, Guid.NewGuid(), release: false);
            _requests.Add(request);
            request.Completed.Wait();
            try
            {
                if (request.Error is not null)
                    throw new IOException("WAL partition is already open for writing.", request.Error);
                return request.Token;
            }
            finally
            {
                request.Completed.Dispose();
            }
        }

        public void Release(Guid token)
        {
            var request = new Request(null, token, release: true);
            _requests.Add(request);
            request.Completed.Wait();
            try
            {
                if (request.Error is not null)
                    throw new IOException("Failed to release the WAL partition writer fence.", request.Error);
            }
            finally
            {
                request.Completed.Dispose();
            }
        }

        private void ProcessRequests()
        {
            foreach (Request request in _requests.GetConsumingEnumerable())
            {
                try
                {
                    if (request.Release)
                    {
                        if (_held.Remove(request.Token, out Mutex? mutex))
                        {
                            try { mutex.ReleaseMutex(); }
                            finally { mutex.Dispose(); }
                        }
                    }
                    else
                    {
                        var mutex = new Mutex(initiallyOwned: false, request.Name!);
                        bool acquired;
                        try
                        {
                            acquired = mutex.WaitOne(0);
                        }
                        catch (AbandonedMutexException)
                        {
                            // The previous process terminated while owning the mutex.
                            // This thread now owns it; WAL recovery proceeds under fence.
                            acquired = true;
                        }

                        if (!acquired)
                        {
                            mutex.Dispose();
                            throw new IOException("Another process owns the WAL partition writer fence.");
                        }

                        _held.Add(request.Token, mutex);
                    }
                }
                catch (Exception ex)
                {
                    request.Error = ex;
                }
                finally
                {
                    request.Completed.Set();
                }
            }
        }
    }

    private sealed class Request
    {
        public string? Name { get; }
        public Guid Token { get; }
        public bool Release { get; }
        public ManualResetEventSlim Completed { get; } = new(false);
        public Exception? Error { get; set; }

        public Request(string? name, Guid token, bool release)
        {
            Name = name;
            Token = token;
            Release = release;
        }
    }
}
