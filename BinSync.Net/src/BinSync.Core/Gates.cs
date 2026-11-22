// Domain sub-gates for Node (Storage → Outbox → Meta).
namespace BinSync;

/// <summary>
/// Holds all three Node domain locks in canonical order. Dispose releases Meta, then Outbox, then Storage.
/// </summary>
public sealed class AllGateScope : IDisposable
{
    private readonly object _storage;
    private readonly object _outbox;
    private readonly object _meta;
    private readonly int _ownerThreadId;
    private int _held;

    /// <summary>Disposable scope holding Storage, Outbox, and Meta locks in order.</summary>
    internal AllGateScope(object storage, object outbox, object meta)
    {
        _storage = storage;
        _outbox = outbox;
        _meta = meta;
        Monitor.Enter(_storage);
        try
        {
            Monitor.Enter(_outbox);
            try { Monitor.Enter(_meta); }
            catch
            {
                Monitor.Exit(_outbox);
                throw;
            }
        }
        catch
        {
            Monitor.Exit(_storage);
            throw;
        }
        _ownerThreadId = Environment.CurrentManagedThreadId;
        _held = 1;
    }

    /// <summary>Release acquired resources.</summary>
    public void Dispose()
    {
        if (Volatile.Read(ref _held) == 0) return;
        if (Environment.CurrentManagedThreadId != _ownerThreadId)
            throw new InvalidOperationException("AllGateScope must be disposed on the thread that acquired it.");
        if (Interlocked.Exchange(ref _held, 0) == 0) return;
        Monitor.Exit(_meta);
        Monitor.Exit(_outbox);
        Monitor.Exit(_storage);
    }
}
