// SNAPSHOT_CHUNK (§12.6 0x0A / §13.1 ladder fallback): compact record dump for a prefix.
namespace BinSync;

/// <summary>One field row inside a snapshot record.</summary>
public sealed class SnapshotField
{
    /// <summary>Field identifier within a record.</summary>
    public ushort FieldId;
    /// <summary>HLC wall-clock component in microseconds.</summary>
    public ulong Wall;
    /// <summary>HLC logical counter component.</summary>
    public uint Logical;
    /// <summary>Durable author of the field or lifecycle entry in a snapshot record.</summary>
    public byte[] AuthorNodeId = Array.Empty<byte>();
    /// <summary>Key epoch associated with the field write.</summary>
    public ushort KeyEpoch;
    /// <summary>Return a copy of the written buffer as a new byte array.</summary>
    public byte[] Value = Array.Empty<byte>();
}

/// <summary>One record in a SNAPSHOT_CHUNK.</summary>
public sealed class SnapshotRecord
{
    /// <summary>128-bit record identity with value equality.</summary>
    public byte[] RecordId = Array.Empty<byte>();
    /// <summary>Field snapshots included for this record.</summary>
    public List<SnapshotField> Fields = new();
    /// <summary>Lifecycle byte: alive or tombstoned.</summary>
    public byte LifecycleValue = Model.Alive;
    /// <summary>Wall component of the record lifecycle HLC.</summary>
    public ulong LifecycleWall;
    /// <summary>Logical component of the record lifecycle HLC.</summary>
    public uint LifecycleLogical;
    /// <summary>Author of the last record-level lifecycle change.</summary>
    public byte[] LifecycleAuthor = Array.Empty<byte>();
}

/// <summary>Decoded SNAPSHOT_CHUNK payload.</summary>
public sealed class SnapshotChunkMessage
{
    /// <summary>Namespace identifier this message or entry applies to.</summary>
    public int NamespaceId;
    /// <summary>Zero-based index of this snapshot chunk.</summary>
    public uint ChunkIndex;
    /// <summary>Total number of chunks in the snapshot.</summary>
    public uint ChunkCount = 1;
    /// <summary>Records carried in this snapshot chunk.</summary>
    public List<SnapshotRecord> Records = new();
}

/// <summary>State-digest / NEED_RANGE / NEED_KEYS anti-entropy helpers.</summary>
public static partial class Reconcile
{
    /// <summary>Build a single-chunk snapshot of records under <paramref name="prefix"/>.</summary>
    public static SnapshotChunkMessage BuildSnapshot(
        Node node, int namespaceId, byte[]? prefix = null, int maxRecords = 10_000)
    {
        var msg = new SnapshotChunkMessage
        {
            NamespaceId = namespaceId,
            ChunkIndex = 0,
            ChunkCount = 1,
        };
        if (!node.Storage.TryGetValue(namespaceId, out var map)) return msg;

        foreach (var (rid, rec) in map)
        {
            if (prefix is { Length: > 0 })
            {
                var key = BucketAccumulator.BucketKey(rid);
                if (prefix.Length > key.Length || !key.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                    continue;
            }
            var sr = new SnapshotRecord
            {
                RecordId = rid.ToArray(),
                LifecycleValue = rec.LifecycleValue,
                LifecycleWall = rec.LifecycleHlc?.Wall ?? 0,
                LifecycleLogical = rec.LifecycleHlc?.Logical ?? 0,
                LifecycleAuthor = rec.LifecycleAuthor ?? Node.Zero128,
            };
            foreach (var (fid, fs) in rec.State)
            {
                sr.Fields.Add(new SnapshotField
                {
                    FieldId = fid,
                    Wall = fs.Timestamp.Wall,
                    Logical = fs.Timestamp.Logical,
                    AuthorNodeId = fs.AuthorNodeId,
                    KeyEpoch = fs.KeyEpoch,
                    Value = fs.Value,
                });
            }
            msg.Records.Add(sr);
            if (msg.Records.Count >= maxRecords) break;
        }
        return msg;
    }

    /// <summary>Encode snapshot chunk into canonical wire bytes.</summary>
    public static byte[] EncodeSnapshotChunk(SnapshotChunkMessage m)
    {
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U32(m.ChunkIndex);
        w.U32(m.ChunkCount);
        w.Varint((ulong)m.Records.Count);
        foreach (var r in m.Records)
        {
            w.B128(Codec.CanonicalB128(r.RecordId));
            w.Varint((ulong)r.Fields.Count);
            foreach (var f in r.Fields)
            {
                w.U16(f.FieldId);
                w.U64(f.Wall);
                w.U32(f.Logical);
                w.B128(Codec.CanonicalB128(f.AuthorNodeId));
                w.U16(f.KeyEpoch);
                w.Bytes(f.Value);
            }
            w.U8(r.LifecycleValue);
            w.U64(r.LifecycleWall);
            w.U32(r.LifecycleLogical);
            w.B128(Codec.CanonicalB128(r.LifecycleAuthor));
        }
        return w.Value();
    }

    /// <summary>Decode snapshot chunk from canonical wire bytes.</summary>
    public static SnapshotChunkMessage DecodeSnapshotChunk(byte[] payload)
    {
        var r = new Reader(payload);
        var m = new SnapshotChunkMessage
        {
            NamespaceId = r.U16(),
            ChunkIndex = r.U32(),
            ChunkCount = r.U32(),
        };
        int n = (int)r.Varint();
        for (int i = 0; i < n; i++)
        {
            var sr = new SnapshotRecord { RecordId = r.B128() };
            int fc = (int)r.Varint();
            for (int j = 0; j < fc; j++)
            {
                sr.Fields.Add(new SnapshotField
                {
                    FieldId = r.U16(),
                    Wall = r.U64(),
                    Logical = r.U32(),
                    AuthorNodeId = r.B128(),
                    KeyEpoch = r.U16(),
                    Value = r.Bytes(),
                });
            }
            sr.LifecycleValue = r.U8();
            sr.LifecycleWall = r.U64();
            sr.LifecycleLogical = r.U32();
            sr.LifecycleAuthor = r.B128();
            m.Records.Add(sr);
        }
        return m;
    }

    /// <summary>
    /// Apply a snapshot into <paramref name="node"/> using LWW with the original
    /// author + HLC (§5.4). Does not push outbox.
    /// </summary>
    public static int ApplySnapshot(Node node, SnapshotChunkMessage chunk)
    {
        int applied = 0;
        foreach (var rec in chunk.Records)
        {
            foreach (var f in rec.Fields)
            {
                var hlc = new Hlc(f.Wall, f.Logical);
                if (node.ApplyRemoteField(
                        chunk.NamespaceId, rec.RecordId, f.FieldId,
                        f.Value, hlc, f.AuthorNodeId, f.KeyEpoch))
                    applied++;
            }
        }
        return applied;
    }
}
