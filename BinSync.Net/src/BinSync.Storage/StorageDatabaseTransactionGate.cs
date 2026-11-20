using System.Runtime.CompilerServices;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Serializes transactions from all adapters attached to the same in-process
/// StorageDatabase. StorageDatabase itself remains the single WAL writer; sharing
/// this gate prevents stale-base conflicts between provider, metadata and Node
/// checkpoint adapters that otherwise have unrelated instance locks.
/// </summary>
internal static class StorageDatabaseTransactionGate
{
    private static readonly ConditionalWeakTable<StorageDatabase, object> Gates = new();

    public static object For(StorageDatabase storage)
    {
        ArgumentNullException.ThrowIfNull(storage);
        return Gates.GetValue(storage, static _ => new object());
    }
}
