// Reconcile fulfill: re-emit storage fields as new outbox ops under a bucket prefix.
namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>
    /// Re-write every live field of records whose BLAKE3(recordId) starts with
    /// <paramref name="prefix"/>, producing fresh outbox entries (new HLC / changeId).
    /// Used to answer NEED_RANGE with DELTA_BATCH without a full SNAPSHOT path.
    /// Returns the number of field writes enqueued.
    /// </summary>
    public int RepublishUnderPrefix(int namespaceId, byte[] prefix)
    {
        ValidateNamespaceId(namespaceId);
        ArgumentNullException.ThrowIfNull(prefix);
        if (prefix.Length > 32)
            throw new ArgumentOutOfRangeException(nameof(prefix), "A BLAKE3 record digest is 32 bytes.");
        ThrowIfDisposed();
        var ownedPrefix = prefix.ToArray();

        lock (_lockStorage) lock (_lockOutbox)
        {
            ThrowIfDisposed();
            int n = 0;
            if (!_storage.TryGetValue(namespaceId, out var map)) return 0;
            foreach (var (rid, rec) in map.ToList())
            {
                var key = BucketAccumulator.BucketKey(rid);
                if (ownedPrefix.Length > key.Length || !key.AsSpan(0, ownedPrefix.Length).SequenceEqual(ownedPrefix))
                    continue;
                if (rec.IsDead) continue;
                foreach (var (fid, fs) in rec.State)
                {
                    WriteFieldInternal(namespaceId, rid, fid, fs.Value);
                    n++;
                }
            }
            return n;
        }
    }

    /// <summary>Re-publish specific recordIds (NEED_KEYS path).</summary>
    public int RepublishRecords(int namespaceId, IEnumerable<RecordId> recordIds)
    {
        ValidateNamespaceId(namespaceId);
        ArgumentNullException.ThrowIfNull(recordIds);
        ThrowIfDisposed();
        // Enumerate caller code before taking engine locks; the snapshot also prevents
        // a mutable caller collection from changing the selected set mid-operation.
        var ownedRecordIds = recordIds.ToArray();

        lock (_lockStorage) lock (_lockOutbox)
        {
            ThrowIfDisposed();
            int n = 0;
            if (!_storage.TryGetValue(namespaceId, out var map)) return 0;
            foreach (var rid in ownedRecordIds)
            {
                if (!map.TryGetValue(rid, out var rec) || rec.IsDead) continue;
                foreach (var (fid, fs) in rec.State)
                {
                    WriteFieldInternal(namespaceId, rid, fid, fs.Value);
                    n++;
                }
            }
            return n;
        }
    }
}
