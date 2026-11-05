// SNAPSHOT_CHUNK (§12.6 0x0A / §13.1 ladder fallback): compact record dump for a prefix.
namespace BinSync;

/// <summary>One field row inside a snapshot record.</summary>
public sealed class SnapshotField
{
    /// <summary>Field identifier within a record.</summary>
    public ushort FieldId;
    /// <summary>True when this row is a field tombstone rather than a live value.</summary>
    public bool IsTombstone;
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
    /// <summary>Whether the record has an explicit lifecycle HLC.</summary>
    public bool HasLifecycle;
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
    // Hard parser bounds apply even when a host does not configure optional ResourceLimits.
    internal const uint MaxSnapshotChunks = 4096;
    internal const int MaxSnapshotRecordsPerChunk = 100_000;
    internal const int MaxSnapshotFieldsPerChunk = 250_000;
    /// <summary>Build the first bounded chunk of a consistent snapshot under <paramref name="prefix"/>.</summary>
    public static SnapshotChunkMessage BuildSnapshot(
        Node node, int namespaceId, byte[]? prefix = null, int maxRecords = 10_000) =>
        BuildSnapshotChunks(node, namespaceId, prefix, maxRecords)[0];

    /// <summary>
    /// Capture one consistent record view and partition it into bounded chunks.
    /// The ordered node gates are held only while copying state, never while sending over the network.
    /// </summary>
    public static IReadOnlyList<SnapshotChunkMessage> BuildSnapshotChunks(
        Node node, int namespaceId, byte[]? prefix = null, int maxRecords = 10_000)
    {
        if (maxRecords <= 0) throw new ArgumentOutOfRangeException(nameof(maxRecords));
        using var gates = node.EnterAllGates();
        var records = new List<SnapshotRecord>();
        if (node.Storage.TryGetValue(namespaceId, out var map))
        {
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
                    HasLifecycle = rec.LifecycleHlc is not null,
                    LifecycleValue = rec.LifecycleValue,
                    LifecycleWall = rec.LifecycleHlc?.Wall ?? 0,
                    LifecycleLogical = rec.LifecycleHlc?.Logical ?? 0,
                    LifecycleAuthor = (rec.LifecycleAuthor ?? Node.Zero128).ToArray(),
                };
                foreach (var (fid, fs) in rec.State)
                {
                    sr.Fields.Add(new SnapshotField
                    {
                        FieldId = fid, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical,
                        AuthorNodeId = fs.AuthorNodeId.ToArray(), KeyEpoch = fs.KeyEpoch, Value = fs.Value.ToArray(),
                    });
                }
                if (rec.HasFieldTombs)
                {
                    foreach (var (fid, fs) in rec.FieldTombs)
                    {
                        sr.Fields.Add(new SnapshotField
                        {
                            FieldId = fid, IsTombstone = true, Wall = fs.Timestamp.Wall,
                            Logical = fs.Timestamp.Logical, AuthorNodeId = fs.AuthorNodeId.ToArray(),
                            KeyEpoch = fs.KeyEpoch, Value = Array.Empty<byte>(),
                        });
                    }
                }
                records.Add(sr);
            }
        }

        int chunkCount = Math.Max(1, (int)(((long)records.Count + maxRecords - 1) / maxRecords));
        var chunks = new List<SnapshotChunkMessage>(chunkCount);
        for (int i = 0; i < chunkCount; i++)
        {
            int offset = i * maxRecords;
            int count = Math.Min(maxRecords, records.Count - offset);
            var chunk = new SnapshotChunkMessage
            {
                NamespaceId = namespaceId,
                ChunkIndex = (uint)i,
                ChunkCount = (uint)chunkCount,
                Records = count == 0 ? new List<SnapshotRecord>() : records.GetRange(offset, count),
            };
            chunks.Add(chunk);
        }
        return chunks;
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
                w.U8(f.IsTombstone ? (byte)1 : (byte)0);
                w.U64(f.Wall);
                w.U32(f.Logical);
                w.B128(Codec.CanonicalB128(f.AuthorNodeId));
                w.U16(f.KeyEpoch);
                w.Bytes(f.Value);
            }
            w.U8(r.HasLifecycle ? (byte)1 : (byte)0);
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
        if (m.ChunkCount == 0 || m.ChunkCount > MaxSnapshotChunks || m.ChunkIndex >= m.ChunkCount)
            throw new FormatException("Snapshot chunk index/count is outside protocol limits");

        ulong recordCount = r.Varint();
        if (recordCount > MaxSnapshotRecordsPerChunk)
            throw new FormatException("Snapshot chunk has too many records");
        int n = (int)recordCount;
        int totalFields = 0;
        for (int i = 0; i < n; i++)
        {
            var sr = new SnapshotRecord { RecordId = r.B128() };
            ulong fieldCount = r.Varint();
            if (fieldCount > (ulong)(MaxSnapshotFieldsPerChunk - totalFields))
                throw new FormatException("Snapshot chunk has too many fields");
            int fc = (int)fieldCount;
            totalFields += fc;
            for (int j = 0; j < fc; j++)
            {
                ushort fieldId = r.U16();
                bool tombstone = r.U8() switch { 0 => false, 1 => true, _ => throw new FormatException("Invalid snapshot field flags") };
                ulong wall = r.U64();
                uint logical = r.U32();
                byte[] author = r.B128();
                ushort keyEpoch = r.U16();
                ulong valueLength = r.Varint();
                if (valueLength > int.MaxValue || valueLength > (ulong)r.Remaining)
                    throw new FormatException("Invalid snapshot field value length");
                sr.Fields.Add(new SnapshotField
                {
                    FieldId = fieldId,
                    IsTombstone = tombstone,
                    Wall = wall,
                    Logical = logical,
                    AuthorNodeId = author,
                    KeyEpoch = keyEpoch,
                    Value = r.Raw((int)valueLength),
                });
            }
            sr.HasLifecycle = r.U8() switch { 0 => false, 1 => true, _ => throw new FormatException("Invalid snapshot lifecycle flags") };
            sr.LifecycleValue = r.U8();
            sr.LifecycleWall = r.U64();
            sr.LifecycleLogical = r.U32();
            sr.LifecycleAuthor = r.B128();
            m.Records.Add(sr);
        }
        if (!r.Eof())
            throw new FormatException("Trailing bytes in snapshot chunk");
        return m;
    }

    /// <summary>
    /// Apply a snapshot into <paramref name="node"/> using LWW with the original
    /// author + HLC (§5.4). Does not push outbox.
    /// </summary>
    public static int ApplySnapshot(Node node, SnapshotChunkMessage chunk)
    {
        if (chunk.ChunkCount != 1 || chunk.ChunkIndex != 0)
            throw new InvalidOperationException("MULTI_CHUNK_SNAPSHOT_REQUIRES_COMPLETE_TRANSFER");
        return ApplySnapshotChunks(node, new[] { chunk });
    }

    /// <summary>Validate and atomically publish a complete ordered snapshot transfer.</summary>
    public static int ApplySnapshotChunks(Node node, IReadOnlyList<SnapshotChunkMessage> chunks)
    {
        if (chunks.Count == 0) throw new FormatException("Snapshot transfer is empty");
        var first = chunks[0];
        if (first.ChunkCount == 0 || first.ChunkCount > MaxSnapshotChunks || chunks.Count != first.ChunkCount)
            throw new FormatException("Snapshot transfer is incomplete or exceeds protocol limits");
        for (int i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i];
            if (chunk.NamespaceId != first.NamespaceId ||
                chunk.ChunkCount != first.ChunkCount || chunk.ChunkIndex != (uint)i)
                throw new FormatException("Snapshot chunk sequence mismatch");
        }

        using var gates = node.EnterAllGates();
        int applied = 0;
        foreach (var chunk in chunks)
        {
            foreach (var rec in chunk.Records)
            {
                var recordId = RecordId.From(rec.RecordId);
                foreach (var f in rec.Fields)
                {
                    var hlc = new Hlc(f.Wall, f.Logical);
                    var author = NodeId.From(f.AuthorNodeId);
                    bool changed = f.IsTombstone
                        ? node.ApplyRemoteFieldTombstone(chunk.NamespaceId, recordId, f.FieldId, hlc, author, f.KeyEpoch)
                        : node.ApplyRemoteField(chunk.NamespaceId, recordId, f.FieldId, f.Value, hlc, author, f.KeyEpoch);
                    if (changed) applied++;
                }
                if (rec.HasLifecycle && node.ApplyRemoteLifecycle(
                        chunk.NamespaceId, recordId, new Hlc(rec.LifecycleWall, rec.LifecycleLogical),
                        NodeId.From(rec.LifecycleAuthor), rec.LifecycleValue))
                    applied++;
            }
        }
        return applied;
    }
}
