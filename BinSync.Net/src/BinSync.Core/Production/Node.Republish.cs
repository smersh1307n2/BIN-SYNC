// Reconcile fulfill: re-emit storage fields as new outbox ops under a bucket prefix.
namespace BinSync;

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
        lock (_lockStorage) lock (_lockOutbox)
        {
            int n = 0;
            if (!_storage.TryGetValue(namespaceId, out var map)) return 0;
            foreach (var (rid, rec) in map.ToList())
            {
                var key = BucketAccumulator.BucketKey(rid);
                if (prefix.Length > key.Length || !key.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                    continue;
                if (rec.IsDead) continue;
                foreach (var (fid, fs) in rec.State)
                {
                    WriteFieldInternal(namespaceId, rid.ToArray(), fid, fs.Value);
                    n++;
                }
            }
            return n;
        }
    }

    /// <summary>Re-publish specific recordIds (NEED_KEYS path).</summary>
    public int RepublishRecords(int namespaceId, IEnumerable<byte[]> recordIds)
    {
        lock (_lockStorage) lock (_lockOutbox)
        {
            int n = 0;
            if (!_storage.TryGetValue(namespaceId, out var map)) return 0;
            foreach (var rid in recordIds)
            {
                if (!map.TryGetValue(rid, out var rec) || rec.IsDead) continue;
                foreach (var (fid, fs) in rec.State)
                {
                    WriteFieldInternal(namespaceId, rid.ToArray(), fid, fs.Value);
                    n++;
                }
            }
            return n;
        }
    }
}
