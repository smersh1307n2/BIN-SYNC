using System.Buffers.Binary;
using System.Security.Cryptography;
using BinSync;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Durable BIN-SYNC Data Provider backed by the neutral Fld1.Storage byte API.
/// </summary>
public class StorageDataProvider : IDataProvider
{
    private const byte ItemMap = 0x01;
    private const byte ItemReverse = 0x02;
    private const byte FieldMap = 0x03;
    private const byte FieldReverse = 0x04;
    private const byte CurrentField = 0x10;
    private const byte LocalChange = 0x20;
    private const byte LocalHighWatermark = 0x21;
    private const byte LocalAckWatermark = 0x22;
    private const byte LocalReadWatermark = 0x23;
    private const byte ApplyReceipt = 0x30;

    private readonly object _gate = new();
    private readonly StorageDatabase _storage;
    private readonly string? _directory;
    private readonly bool _deleteOnDispose;
    private readonly bool _ownsStorage;
    private readonly Dictionary<Guid, StorageSnapshot> _snapshots = new();
    private bool _disposed;

    public StorageDataProvider(string directory, ulong partitionId = 1, bool deleteOnDispose = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = directory;
        _deleteOnDispose = deleteOnDispose;
        _ownsStorage = true;
        _storage = StorageDatabase.Open(directory, partitionId);
    }

    /// <summary>Attach to a caller-owned shared Storage database.</summary>
    public StorageDataProvider(StorageDatabase storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _ownsStorage = false;
        _deleteOnDispose = false;
    }

    public bool SupportsRestartDurability => true;

    public ulong LocalChangeHighWatermark
    {
        get
        {
            lock (_gate)
            {
                ThrowIfDisposed();
                return ReadUInt64(_storage, [LocalHighWatermark]);
            }
        }
    }

    public RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (sourceItemKey.IsEmpty)
                throw new ArgumentException("Source item key is required.", nameof(sourceItemKey));

            byte[] lookupKey = Prefixed(ItemMap, sourceItemKey.Span);
            using var batch = _storage.BeginWriteBatch();
            if (batch.TryGet(lookupKey, out var mapped))
                return RecordId.From(mapped!);

            byte[] digest = SHA256.HashData(sourceItemKey.Span);
            byte[] identityBytes = digest.AsSpan(0, 16).ToArray();
            if (identityBytes.AsSpan().IndexOfAnyExcept((byte)0) < 0)
                identityBytes[15] = 1;
            RecordId candidate = RecordId.From(identityBytes);
            byte[] reverseKey = Prefixed(ItemReverse, identityBytes);
            if (batch.TryGet(reverseKey, out var prior) &&
                !prior!.AsSpan().SequenceEqual(sourceItemKey.Span))
                throw new InvalidDataException("Stable source item identity collision.");

            batch.Put(lookupKey, identityBytes);
            batch.Put(reverseKey, sourceItemKey.Span);
            batch.Commit();
            return candidate;
        }
    }

    public ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (sourceFieldKey.IsEmpty)
                throw new ArgumentException("Source field key is required.", nameof(sourceFieldKey));

            byte[] lookupKey = Prefixed(FieldMap, sourceFieldKey.Span);
            using var batch = _storage.BeginWriteBatch();
            if (batch.TryGet(lookupKey, out var mapped))
                return BinaryPrimitives.ReadUInt16LittleEndian(mapped!);

            byte[] digest = SHA256.HashData(sourceFieldKey.Span);
            ushort candidate = BinaryPrimitives.ReadUInt16LittleEndian(digest);
            if (candidate < 0x0100) candidate = checked((ushort)(candidate + 0x0100));
            Span<byte> idBytes = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(idBytes, candidate);
            byte[] reverseKey = Prefixed(FieldReverse, idBytes);
            if (batch.TryGet(reverseKey, out var prior) &&
                !prior!.AsSpan().SequenceEqual(sourceFieldKey.Span))
                throw new InvalidDataException("Stable source field identity collision.");

            batch.Put(lookupKey, idBytes);
            batch.Put(reverseKey, sourceFieldKey.Span);
            batch.Commit();
            return candidate;
        }
    }

    public void AppendLocalChange(ProviderMutation mutation)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ValidateMutation(mutation);
            using var batch = _storage.BeginWriteBatch();
            ulong current = batch.TryGet([LocalHighWatermark], out var currentBytes)
                ? ReadUInt64(currentBytes!)
                : 0;
            ulong sequence = checked(current + 1);
            byte[] owned = EncodeMutation(mutation);
            byte[] currentKey = CurrentKey(mutation.RecordId, mutation.FieldId);
            if (!batch.TryGet(currentKey, out var currentBytes) ||
                Compare(mutation, DecodeMutation(currentBytes!)) > 0)
                batch.Put(currentKey, owned);
            batch.Put(ChangeKey(sequence), owned);
            batch.Put([LocalHighWatermark], EncodeUInt64(sequence));
            batch.Commit();
        }
    }

    public ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            using var snapshot = _storage.OpenSnapshot();
            ulong high = ReadUInt64(snapshot, [LocalHighWatermark]);
            ulong acked = ReadUInt64(snapshot, [LocalAckWatermark]);
            ulong highestRead = ReadUInt64(snapshot, [LocalReadWatermark]);
            bool snapshotBarrier = _snapshots.Values.Any(sourceSnapshot =>
                ReadUInt64(sourceSnapshot, [LocalHighWatermark]) == afterSequence);
            if (afterSequence > highestRead && !snapshotBarrier)
                throw new InvalidDataException("Requested local change cursor skips changes that have not been scanned or covered by a source snapshot.");

            ulong start = Math.Max(afterSequence, acked);
            var rows = snapshot.ScanPrefix([LocalChange], ChangeKey(start), limit);
            var changes = new List<ProviderChange>(rows.Count);
            foreach (var row in rows)
            {
                ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(1, 8));
                changes.Add(new ProviderChange(sequence, DecodeMutation(row.Value)));
            }

            ulong scanned = changes.Count == limit ? changes[^1].Sequence : high;
            if (scanned > highestRead)
            {
                using var cursorBatch = _storage.BeginWriteBatch();
                ulong actualBaseCommitSeq = _storage.LastCommitSeq;
                if (actualBaseCommitSeq != snapshot.CommitSeq)
                    throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
                cursorBatch.Put([LocalReadWatermark], EncodeUInt64(scanned));
                cursorBatch.Commit();
            }

            return new ProviderChangePage(changes, scanned);
        }
    }

    public void AcknowledgeLocalChanges(ulong safeThroughSequence)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            using var snapshot = _storage.OpenSnapshot();
            ulong high = ReadUInt64(snapshot, [LocalHighWatermark]);
            ulong acked = ReadUInt64(snapshot, [LocalAckWatermark]);
            ulong highestRead = ReadUInt64(snapshot, [LocalReadWatermark]);
            if (safeThroughSequence > high || safeThroughSequence > highestRead)
                throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
            if (safeThroughSequence <= acked) return;

            var rows = snapshot.ScanPrefix([LocalChange], ChangeKey(acked), int.MaxValue);
            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            foreach (var row in rows)
            {
                ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(1, 8));
                if (sequence > safeThroughSequence) break;
                batch.Delete(row.Key);
            }

            batch.Put([LocalAckWatermark], EncodeUInt64(safeThroughSequence));
            batch.Commit();
        }
    }

    public ProviderSnapshotHandle OpenSnapshot()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            StorageSnapshot snapshot = _storage.OpenSnapshot();
            Guid token = Guid.NewGuid();
            _snapshots.Add(token, snapshot);
            ulong watermark = ReadUInt64(snapshot, [LocalHighWatermark]);
            return new ProviderSnapshotHandle(token, watermark);
        }
    }

    public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_snapshots.TryGetValue(snapshotToken, out var snapshot))
                throw new ObjectDisposedException(nameof(snapshotToken));
            if (afterIndex < 0) throw new ArgumentOutOfRangeException(nameof(afterIndex));
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));

            var rows = snapshot.ScanPrefix([CurrentField], ReadOnlySpan<byte>.Empty, int.MaxValue);
            if (afterIndex > rows.Count) throw new ArgumentOutOfRangeException(nameof(afterIndex));
            var entries = new List<ProviderMutation>(Math.Min(limit, rows.Count - afterIndex));
            int end = Math.Min(rows.Count, checked(afterIndex + limit));
            for (int i = afterIndex; i < end; i++)
                entries.Add(DecodeMutation(rows[i].Value));
            return new ProviderSnapshotPage(entries, end, end == rows.Count);
        }
    }

    public void ReleaseSnapshot(Guid snapshotToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_snapshots.Remove(snapshotToken, out var snapshot))
                snapshot.Dispose();
        }
    }

    public ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (applyToken == Guid.Empty)
                throw new ArgumentException("Apply token must be non-empty.", nameof(applyToken));
            ArgumentNullException.ThrowIfNull(mutations);
            if (mutations.Count == 0)
                throw new ArgumentException("Remote batch must not be empty.", nameof(mutations));

            var seen = new HashSet<(RecordId, ushort)>();
            foreach (var mutation in mutations)
            {
                ValidateMutation(mutation);
                if (!seen.Add((mutation.RecordId, mutation.FieldId)))
                    throw new InvalidDataException("Remote batch contains duplicate canonical field keys.");
            }

            byte[] fingerprint = Fingerprint(mutations);
            byte[] receiptKey = Prefixed(ApplyReceipt, applyToken.ToByteArray());
            using var batch = _storage.BeginWriteBatch();
            if (batch.TryGet(receiptKey, out var prior))
            {
                if (!prior!.AsSpan().SequenceEqual(fingerprint))
                    throw new InvalidDataException("Apply token was reused with different canonical input.");
                return new ProviderApplyReceipt(applyToken, prior!.ToArray(), IsDuplicate: true);
            }

            foreach (var mutation in mutations)
            {
                byte[] key = CurrentKey(mutation.RecordId, mutation.FieldId);
                if (batch.TryGet(key, out var priorValue))
                {
                    ProviderMutation existing = DecodeMutation(priorValue!);
                    if (Compare(mutation, existing) <= 0) continue;
                }

                batch.Put(key, EncodeMutation(mutation));
            }

            batch.Put(receiptKey, fingerprint);
            batch.Commit();
            return new ProviderApplyReceipt(applyToken, fingerprint, IsDuplicate: false);
        }
    }

    public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_storage.TryGet(CurrentKey(recordId, fieldId), out var value))
            {
                mutation = DecodeMutation(value!);
                return true;
            }

            mutation = null;
            return false;
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            foreach (var snapshot in _snapshots.Values) snapshot.Dispose();
            _snapshots.Clear();
            if (_ownsStorage) _storage.Dispose();
            if (_deleteOnDispose && _directory is { } directory)
            {
                try { Directory.Delete(directory, recursive: true); }
                catch { /* best-effort cleanup for isolated test directories */ }
            }
        }
    }

    private static void ValidateMutation(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        if (mutation.RecordId.IsZero || mutation.AuthorNodeId.IsZero)
            throw new ArgumentException("RecordId and original AuthorNodeId must be non-zero.", nameof(mutation));
        ArgumentNullException.ThrowIfNull(mutation.Value);
        if (mutation.IsTombstone && mutation.Value.Length != 0)
            throw new ArgumentException("Tombstone mutation must not carry a value payload.", nameof(mutation));
    }

    private static int Compare(ProviderMutation left, ProviderMutation right)
        => HlcOps.CompareHlc(
            new Hlc(left.HlcWall, left.HlcLogical), left.AuthorNodeId,
            new Hlc(right.HlcWall, right.HlcLogical), right.AuthorNodeId);

    private static byte[] Fingerprint(IReadOnlyList<ProviderMutation> mutations)
    {
        var ordered = mutations.ToArray();
        Array.Sort(ordered, CompareCanonicalKey);

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> count = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(count, ordered.Length);
        hash.AppendData(count);

        Span<byte> fixedPart = stackalloc byte[53];
        foreach (var mutation in ordered)
        {
            mutation.RecordId.WriteTo(fixedPart);
            BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(16), mutation.FieldId);
            fixedPart[18] = mutation.IsTombstone ? (byte)1 : (byte)0;
            BinaryPrimitives.WriteUInt64LittleEndian(fixedPart.Slice(19), mutation.HlcWall);
            BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(27), mutation.HlcLogical);
            mutation.AuthorNodeId.WriteTo(fixedPart.Slice(31));
            BinaryPrimitives.WriteUInt16LittleEndian(fixedPart.Slice(47), mutation.KeyEpoch);
            BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(49), checked((uint)mutation.Value.Length));
            hash.AppendData(fixedPart);
            hash.AppendData(mutation.Value);
        }

        return hash.GetHashAndReset();
    }

    private static int CompareCanonicalKey(ProviderMutation left, ProviderMutation right)
    {
        int c = CompareRecordIds(left.RecordId, right.RecordId);
        return c != 0 ? c : left.FieldId.CompareTo(right.FieldId);
    }

    private static int CompareRecordIds(RecordId left, RecordId right)
    {
        ulong leftLo = left.Lo, rightLo = right.Lo;
        for (int i = 0; i < sizeof(ulong); i++)
        {
            int c = ((byte)(leftLo >> (i * 8))).CompareTo((byte)(rightLo >> (i * 8)));
            if (c != 0) return c;
        }

        ulong leftHi = left.Hi, rightHi = right.Hi;
        for (int i = 0; i < sizeof(ulong); i++)
        {
            int c = ((byte)(leftHi >> (i * 8))).CompareTo((byte)(rightHi >> (i * 8)));
            if (c != 0) return c;
        }

        return 0;
    }

    private static byte[] EncodeMutation(ProviderMutation mutation)
    {
        byte[] result = new byte[53 + mutation.Value.Length];
        mutation.RecordId.WriteTo(result);
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(16), mutation.FieldId);
        result[18] = mutation.IsTombstone ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt64LittleEndian(result.AsSpan(19), mutation.HlcWall);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(27), mutation.HlcLogical);
        mutation.AuthorNodeId.WriteTo(result.AsSpan(31));
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(47), mutation.KeyEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(49), checked((uint)mutation.Value.Length));
        mutation.Value.CopyTo(result.AsSpan(53));
        return result;
    }

    private static ProviderMutation DecodeMutation(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 53) throw new InvalidDataException("Stored provider mutation is truncated.");
        uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(49));
        if (valueLength > int.MaxValue || bytes.Length != 53 + (int)valueLength || bytes[18] > 1)
            throw new InvalidDataException("Stored provider mutation length or flags are invalid.");
        return new ProviderMutation
        {
            RecordId = RecordId.From(bytes.Slice(0, 16)),
            FieldId = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(16)),
            IsTombstone = bytes[18] != 0,
            HlcWall = BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(19)),
            HlcLogical = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(27)),
            AuthorNodeId = NodeId.From(bytes.Slice(31, 16)),
            KeyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(47)),
            Value = bytes.Slice(53).ToArray()
        };
    }

    private static byte[] CurrentKey(RecordId recordId, ushort fieldId)
    {
        byte[] key = new byte[19];
        key[0] = CurrentField;
        recordId.WriteTo(key.AsSpan(1, 16));
        BinaryPrimitives.WriteUInt16BigEndian(key.AsSpan(17), fieldId);
        return key;
    }

    private static byte[] ChangeKey(ulong sequence)
    {
        byte[] key = new byte[9];
        key[0] = LocalChange;
        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(1), sequence);
        return key;
    }

    private static byte[] Prefixed(byte prefix, ReadOnlySpan<byte> suffix)
    {
        byte[] key = new byte[1 + suffix.Length];
        key[0] = prefix;
        suffix.CopyTo(key.AsSpan(1));
        return key;
    }

    private static byte[] EncodeUInt64(ulong value)
    {
        byte[] bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static ulong ReadUInt64(StorageDatabase storage, ReadOnlySpan<byte> key)
        => storage.TryGet(key, out var bytes) ? ReadUInt64(bytes!) : 0;

    private static ulong ReadUInt64(StorageSnapshot snapshot, ReadOnlySpan<byte> key)
        => snapshot.TryGet(key, out var bytes) ? ReadUInt64(bytes!) : 0;

    private static ulong ReadUInt64(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 8) throw new InvalidDataException("Stored provider watermark has an invalid length.");
        return BinaryPrimitives.ReadUInt64LittleEndian(bytes);
    }

    private static void ThrowIfDisposed(StorageDataProvider adapter)
    {
        if (adapter._disposed) throw new ObjectDisposedException(nameof(StorageDataProvider));
    }

    private void ThrowIfDisposed() => ThrowIfDisposed(this);
}
