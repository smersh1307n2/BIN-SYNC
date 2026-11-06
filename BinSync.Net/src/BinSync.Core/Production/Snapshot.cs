// SNAPSHOT_CHUNK (§12.6 0x0A / §13.1 ladder fallback): compact record dump for a prefix.
namespace BinSync;

/// <summary>One field row inside a snapshot record.</summary>
public sealed class SnapshotField
{
    /// <summary>Field identifier within a record.</summary>
    public ushort FieldId { get; set; }
    /// <summary>True when this row is a field tombstone rather than a live value.</summary>
    public bool IsTombstone { get; set; }
    /// <summary>HLC wall-clock component in microseconds.</summary>
    public ulong Wall { get; set; }
    /// <summary>HLC logical counter component.</summary>
    public uint Logical { get; set; }
    /// <summary>Durable author of the field or lifecycle entry in a snapshot record.</summary>
    public NodeId AuthorNodeId { get; set; }
    /// <summary>Key epoch associated with the field write.</summary>
    public ushort KeyEpoch { get; set; }
    /// <summary>Return a copy of the written buffer as a new byte array.</summary>
    public byte[] Value { get; set; } = Array.Empty<byte>();
}

/// <summary>One record in a SNAPSHOT_CHUNK.</summary>
public sealed class SnapshotRecord
{
    /// <summary>128-bit record identity with value equality.</summary>
    public RecordId RecordId { get; set; }
    /// <summary>Field snapshots included for this record.</summary>
    public List<SnapshotField> Fields { get; set; } = new();
    /// <summary>Whether the record has an explicit lifecycle HLC.</summary>
    public bool HasLifecycle { get; set; }
    /// <summary>Lifecycle byte: alive or tombstoned.</summary>
    public byte LifecycleValue { get; set; } = Model.Alive;
    /// <summary>Wall component of the record lifecycle HLC.</summary>
    public ulong LifecycleWall { get; set; }
    /// <summary>Logical component of the record lifecycle HLC.</summary>
    public uint LifecycleLogical { get; set; }
    /// <summary>Author of the last record-level lifecycle change.</summary>
    public NodeId LifecycleAuthor { get; set; }
}

/// <summary>Decoded SNAPSHOT_CHUNK payload.</summary>
public sealed class SnapshotChunkMessage
{
    /// <summary>Namespace identifier this message or entry applies to.</summary>
    public int NamespaceId { get; set; }
    /// <summary>Zero-based index of this snapshot chunk.</summary>
    public uint ChunkIndex { get; set; }
    /// <summary>Total number of chunks in the snapshot.</summary>
    public uint ChunkCount { get; set; } = 1;
    /// <summary>Records carried in this snapshot chunk.</summary>
    public List<SnapshotRecord> Records { get; set; } = new();
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
        if (maxRecords <= 0 || maxRecords > MaxSnapshotRecordsPerChunk)
            throw new ArgumentOutOfRangeException(nameof(maxRecords));
        var ownedPrefix = prefix is { Length: > 0 } ? prefix.ToArray() : null;
        var records = new List<SnapshotRecord>();
        // Copy the complete record graph under the ordered node gates. Chunk packing
        // and size accounting are CPU-only work and must not extend the lock window.
        using (node.EnterAllGates())
        {
            if (node.Storage.TryGetValue(namespaceId, out var map))
            {
                foreach (var (rid, rec) in map)
                {
                    if (ownedPrefix is not null)
                    {
                        var key = BucketAccumulator.BucketKey(rid);
                        if (ownedPrefix.Length > key.Length ||
                            !key.AsSpan(0, ownedPrefix.Length).SequenceEqual(ownedPrefix))
                            continue;
                    }
                    var sr = new SnapshotRecord
                    {
                        RecordId = rid,
                        HasLifecycle = rec.LifecycleHlc is not null,
                        LifecycleValue = rec.LifecycleValue,
                        LifecycleWall = rec.LifecycleHlc?.Wall ?? 0,
                        LifecycleLogical = rec.LifecycleHlc?.Logical ?? 0,
                        LifecycleAuthor = rec.LifecycleAuthor ?? default,
                    };
                    foreach (var (fid, fs) in rec.State)
                    {
                        sr.Fields.Add(new SnapshotField
                        {
                            FieldId = fid, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical,
                            AuthorNodeId = fs.AuthorNodeId, KeyEpoch = fs.KeyEpoch, Value = fs.Value.ToArray(),
                        });
                    }
                    if (rec.HasFieldTombs)
                    {
                        foreach (var (fid, fs) in rec.FieldTombs)
                        {
                            sr.Fields.Add(new SnapshotField
                            {
                                FieldId = fid, IsTombstone = true, Wall = fs.Timestamp.Wall,
                                Logical = fs.Timestamp.Logical, AuthorNodeId = fs.AuthorNodeId,
                                KeyEpoch = fs.KeyEpoch, Value = Array.Empty<byte>(),
                            });
                        }
                    }
                    records.Add(sr);
                }
            }
        }

        // Pack by all parser/frame bounds, not just record count. A single dense
        // record or large field value must never create a chunk the receiver rejects.
        var partitions = new List<List<SnapshotRecord>>();
        var current = new List<SnapshotRecord>();
        int currentFields = 0;
        long currentRecordBytes = 0;
        foreach (var record in records)
        {
            int recordFields = record.Fields.Count;
            long recordBytes = SnapshotRecordEncodedSize(record);
            if (recordFields > MaxSnapshotFieldsPerChunk ||
                recordBytes + 10 + Codec.VarintLength(1) > Frames.MAX_PAYLOAD_SIZE)
                throw new SyncException("PAYLOAD_TOO_LARGE");

            bool mustFlush = current.Count > 0 &&
                (current.Count >= maxRecords ||
                 currentFields + recordFields > MaxSnapshotFieldsPerChunk ||
                 10L + Codec.VarintLength((ulong)(current.Count + 1)) +
                    currentRecordBytes + recordBytes > Frames.MAX_PAYLOAD_SIZE);
            if (mustFlush)
            {
                partitions.Add(current);
                current = new List<SnapshotRecord>();
                currentFields = 0;
                currentRecordBytes = 0;
            }

            current.Add(record);
            currentFields += recordFields;
            currentRecordBytes += recordBytes;
        }
        if (current.Count > 0 || partitions.Count == 0)
            partitions.Add(current);
        if (partitions.Count > MaxSnapshotChunks)
            throw new SyncException("PAYLOAD_TOO_LARGE");

        uint chunkCount = (uint)partitions.Count;
        var chunks = new List<SnapshotChunkMessage>(partitions.Count);
        for (int i = 0; i < partitions.Count; i++)
        {
            chunks.Add(new SnapshotChunkMessage
            {
                NamespaceId = namespaceId,
                ChunkIndex = (uint)i,
                ChunkCount = chunkCount,
                Records = partitions[i],
            });
        }
        return chunks;
    }

    private static long SnapshotRecordEncodedSize(SnapshotRecord record)
    {
        long size = 16 + Codec.VarintLength((ulong)record.Fields.Count) + 30;
        foreach (var field in record.Fields)
            size += 33 + Codec.VarintLength((ulong)field.Value.Length) + field.Value.Length;
        return size;
    }

    /// <summary>Encode snapshot chunk into canonical wire bytes.</summary>
    public static byte[] EncodeSnapshotChunk(SnapshotChunkMessage m)
    {
        ArgumentNullException.ThrowIfNull(m);
        Node.ValidateNamespaceId(m.NamespaceId);
        ArgumentNullException.ThrowIfNull(m.Records);
        var w = new Writer();
        w.U16((ushort)m.NamespaceId);
        w.U32(m.ChunkIndex);
        w.U32(m.ChunkCount);
        w.Varint((ulong)m.Records.Count);
        foreach (var r in m.Records)
        {
            w.B128(r.RecordId);
            w.Varint((ulong)r.Fields.Count);
            foreach (var f in r.Fields)
            {
                w.U16(f.FieldId);
                w.U8(f.IsTombstone ? (byte)1 : (byte)0);
                w.U64(f.Wall);
                w.U32(f.Logical);
                w.B128(f.AuthorNodeId);
                w.U16(f.KeyEpoch);
                w.Bytes(f.Value);
            }
            w.U8(r.HasLifecycle ? (byte)1 : (byte)0);
            w.U8(r.LifecycleValue);
            w.U64(r.LifecycleWall);
            w.U32(r.LifecycleLogical);
            w.B128(r.LifecycleAuthor);
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
            var sr = new SnapshotRecord { RecordId = r.ReadRecordId() };
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
                NodeId author = r.ReadNodeId();
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
            sr.LifecycleAuthor = r.ReadNodeId();
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

    private static SnapshotChunkMessage CloneSnapshotChunk(SnapshotChunkMessage? source)
    {
        if (source is null) return null!;
        return new SnapshotChunkMessage
        {
            NamespaceId = source.NamespaceId,
            ChunkIndex = source.ChunkIndex,
            ChunkCount = source.ChunkCount,
            Records = source.Records is null ? null! : source.Records.Select(record =>
                record is null ? null! : new SnapshotRecord
                {
                    RecordId = record.RecordId,
                    Fields = record.Fields is null ? null! : record.Fields.Select(field =>
                        field is null ? null! : new SnapshotField
                        {
                            FieldId = field.FieldId,
                            IsTombstone = field.IsTombstone,
                            Wall = field.Wall,
                            Logical = field.Logical,
                            AuthorNodeId = field.AuthorNodeId,
                            KeyEpoch = field.KeyEpoch,
                            Value = field.Value is null ? null! : field.Value.ToArray(),
                        }).ToList(),
                    HasLifecycle = record.HasLifecycle,
                    LifecycleValue = record.LifecycleValue,
                    LifecycleWall = record.LifecycleWall,
                    LifecycleLogical = record.LifecycleLogical,
                    LifecycleAuthor = record.LifecycleAuthor,
                }).ToList(),
        };
    }

    /// <summary>Validate and atomically publish a complete ordered snapshot transfer.</summary>
    public static int ApplySnapshotChunks(Node node, IReadOnlyList<SnapshotChunkMessage> chunks)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(chunks);
        var input = chunks.ToArray();
        if (input.Length == 0) throw new FormatException("Snapshot transfer is empty");
        // The DTO graph is caller-owned and mutable. Detach it before validation so
        // concurrent caller edits cannot change the values applied after preflight.
        var ownedChunks = input.Select(CloneSnapshotChunk).ToArray();
        var first = ownedChunks[0] ?? throw new FormatException("Snapshot chunk is null");
        Node.ValidateNamespaceId(first.NamespaceId);
        if (first.ChunkCount == 0 || first.ChunkCount > MaxSnapshotChunks || ownedChunks.Length != first.ChunkCount)
            throw new FormatException("Snapshot transfer is incomplete or exceeds protocol limits");

        // Validate the complete object graph before taking locks or touching live state.
        // Public DTOs can be constructed in-process and are not necessarily decoder output.
        for (int i = 0; i < ownedChunks.Length; i++)
        {
            var chunk = ownedChunks[i] ?? throw new FormatException("Snapshot chunk is null");
            if (chunk.NamespaceId != first.NamespaceId ||
                chunk.ChunkCount != first.ChunkCount || chunk.ChunkIndex != (uint)i)
                throw new FormatException("Snapshot chunk sequence mismatch");
            if (chunk.Records is null || chunk.Records.Count > MaxSnapshotRecordsPerChunk)
                throw new FormatException("Snapshot chunk has too many or missing records");

            int totalFields = 0;
            foreach (var rec in chunk.Records)
            {
                if (rec is null || rec.Fields is null)
                    throw new FormatException("Invalid snapshot record");
                if (rec.Fields.Count > MaxSnapshotFieldsPerChunk - totalFields)
                    throw new FormatException("Snapshot chunk has too many fields");
                totalFields += rec.Fields.Count;

                foreach (var field in rec.Fields)
                {
                    if (field is null || field.Value is null || (field.IsTombstone && field.Value.Length != 0) ||
                        (!field.IsTombstone && field.FieldId == Model.LifecycleFieldId &&
                         field.Value.Length > 0 && field.Value[0] is not Model.Alive and not Model.Dead))
                        throw new FormatException("Invalid snapshot field");
                }

                if (rec.HasLifecycle &&
                    (rec.LifecycleValue is not Model.Alive and not Model.Dead))
                    throw new FormatException("Invalid snapshot lifecycle");
            }
        }

        // Stage a copy-on-write view of every touched record. No live dictionary or
        // Record instance is mutated until all merges have completed successfully.
        using var gates = node.EnterAllGates();
        var staged = new Dictionary<RecordId, Record>();
        int applied = 0;
        foreach (var chunk in ownedChunks)
        {
            foreach (var rec in chunk.Records)
            {
                var recordId = rec.RecordId;
                if (!staged.TryGetValue(recordId, out var target))
                {
                    target = node.Storage.TryGetValue(chunk.NamespaceId, out var liveStore) &&
                             liveStore.TryGetValue(recordId, out var existing)
                        ? Node.Clone(existing)
                        : new Record(recordId, (ushort)chunk.NamespaceId);
                    staged.Add(recordId, target);
                }

                foreach (var field in rec.Fields)
                {
                    var hlc = new Hlc(field.Wall, field.Logical);
                    var author = field.AuthorNodeId;
                    bool changed;
                    if (field.IsTombstone)
                    {
                        changed = Merge.ApplyFieldTomb(target, field.FieldId,
                            new FieldState(Array.Empty<byte>(), hlc, author, field.KeyEpoch));
                    }
                    else if (field.FieldId == Model.LifecycleFieldId)
                    {
                        changed = Merge.ApplyLifecycle(target, hlc, author,
                            field.Value.Length > 0 ? field.Value[0] : Model.Alive);
                    }
                    else
                    {
                        changed = Merge.ApplyFieldWrite(target, field.FieldId,
                            new FieldState(field.Value, hlc, author, field.KeyEpoch));
                    }
                    if (changed) applied++;
                }

                if (rec.HasLifecycle && Merge.ApplyLifecycle(target,
                    new Hlc(rec.LifecycleWall, rec.LifecycleLogical),
                    rec.LifecycleAuthor, rec.LifecycleValue))
                    applied++;
            }
        }

        // Snapshot apply obeys the same configured storage cap as DELTA_BATCH.
        // Count only record identities not already present in the target namespace;
        // reject before publishing any staged record.
        var limits = node.Limits;
        if (limits is not null && staged.Count > 0)
        {
            int newRecords = 0;
            bool hasNamespace = node.Storage.TryGetValue(first.NamespaceId, out var existingStore);
            foreach (var recordId in staged.Keys)
                if (!hasNamespace || !existingStore!.ContainsKey(recordId))
                    newRecords++;
            limits.EnsureCanApplySnapshot(node, newRecords);
        }

        // Single publication phase while all node gates remain held.
        if (staged.Count > 0)
        {
            if (!node.Storage.TryGetValue(first.NamespaceId, out var store))
                node.Storage[first.NamespaceId] = store = new Dictionary<RecordId, Record>();
            foreach (var (recordId, record) in staged)
                store[recordId] = record;
        }
        // Release all node gates before notifying observers. Snapshot application is
        // a control-plane transfer, so event dispatch never runs under engine locks.
        gates.Dispose();
        node.RaiseSnapshotApplied(first.NamespaceId, applied);
        return applied;
    }
}
