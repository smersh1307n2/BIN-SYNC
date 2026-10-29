// Domain sub-gates for Node (Storage → Outbox → Meta).
namespace BinSync;

/// <summary>
/// Holds all three Node domain locks in canonical order. Dispose releases Meta, then Outbox, then Storage.
/// </summary>
public readonly struct AllGateScope : IDisposable
{
    private readonly object _storage;
    private readonly object _outbox;
    private readonly object _meta;
    private readonly bool _held;

    public AllGateScope(object storage, object outbox, object meta)
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
        _held = true;
    }

    public void Dispose()
    {
        if (!_held) return;
        Monitor.Exit(_meta);
        Monitor.Exit(_outbox);
        Monitor.Exit(_storage);
    }
}
