using System.Buffers.Binary;
using System.Security.Cryptography;
using BinSync;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Durable BIN-SYNC Data Provider backed by the neutral Fld1.Storage byte API.
/// </summary>
public class StorageDataProvider : IDataProvider, IProviderPublicationMode, IProviderFullReplace
{
    private const byte ItemMap = 0x01;
    private const byte ItemReverse = 0x02;
    private const byte FieldMap = 0x03;
    private const byte FieldReverse = 0x04;
    private const byte CurrentField = 0x10;
    private const byte CurrentRecordTombstone = 0x11;
    private const byte LocalChange = 0x20;
    private const byte LocalHighWatermark = 0x21;
    private const byte LocalAckWatermark = 0x22;
    private const byte LocalReadWatermark = 0x23;
    private const byte ProviderPublicationMode = 0x24;
    private const int MaxFullReplacePageEntries = 4096;
    private const byte ProviderFormatMarker = 0x25;
    private const byte ActiveGeneration = 0x26;
    private const byte FullReplaceSession = 0x41;
    private const byte ApplyReceipt = 0x30;
    private static ReadOnlySpan<byte> ProviderFormatV3 => [0x42, 0x53, 0x50, 0x33];
    private static readonly byte[] ProviderOwnedPrefixes =
        [ItemMap, ItemReverse, FieldMap, FieldReverse, CurrentField, CurrentRecordTombstone,
         LocalChange, LocalHighWatermark, LocalAckWatermark, LocalReadWatermark,
         ProviderPublicationMode, ActiveGeneration, FullReplaceSession, ApplyReceipt];

    private readonly object _gate;
    private readonly StorageDatabase _storage;
    private readonly string? _directory;
    private readonly bool _deleteOnDispose;
    private readonly bool _ownsStorage;
    private readonly Dictionary<Guid, ProviderSnapshotState> _snapshots = new();
    private bool _disposed;

    private sealed record ProviderSnapshotState(ulong LocalHighWatermark, StorageEntry[] Entries);
    private sealed record FullReplaceState(int ExpectedCount, ulong SourceFeedWatermark,
        byte[] ExpectedDigest, int NextIndex, bool IsCommitted, Guid BaseGeneration, byte[] LastSortKey);

    public StorageDataProvider(string directory, ulong partitionId = 1, bool deleteOnDispose = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = directory;
        _deleteOnDispose = deleteOnDispose;
        _ownsStorage = true;
        _storage = StorageDatabase.Open(directory, partitionId);
        _gate = StorageDatabaseTransactionGate.For(_storage);
        try { EnsureFormatMarker(); }
        catch { _storage.Dispose(); throw; }
    }

    /// <summary>Attach to a caller-owned shared Storage database.</summary>
    public StorageDataProvider(StorageDatabase storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _gate = StorageDatabaseTransactionGate.For(_storage);
        _ownsStorage = false;
        _deleteOnDispose = false;
        EnsureFormatMarker();
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
            Guid activeGeneration = ReadActiveGeneration(_storage);
            byte[] currentKey = mutation.IsRecordTombstone
                ? CurrentRecordTombstoneKey(activeGeneration, mutation.RecordId)
                : CurrentKey(activeGeneration, mutation.RecordId, mutation.FieldId);
            if (batch.TryGet(currentKey, out var currentFieldBytes))
            {
                ProviderMutation existing = DecodeMutation(currentFieldBytes!);
                int comparison = Compare(mutation, existing);
                if (comparison == 0)
                {
                    if (!SameMutation(mutation, existing))
                        throw new InvalidDataException("Local source feed equivocated at the same HLC and author.");
                    return;
                }

                if (comparison > 0) batch.Put(currentKey, owned);
            }
            else
            {
                batch.Put(currentKey, owned);
            }

            batch.Put(ChangeKey(sequence), EncodeLocalFeedChange(owned));
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
                sourceSnapshot.LocalHighWatermark == afterSequence);
            if (afterSequence > highestRead && !snapshotBarrier)
                throw new InvalidDataException("Requested local change cursor skips changes that have not been scanned or covered by a source snapshot.");

            ulong start = Math.Max(afterSequence, acked);
            var rows = snapshot.ScanPrefix([LocalChange], ChangeKey(start), limit);
            var changes = new List<ProviderChange>(rows.Count);
            foreach (var row in rows)
            {
                ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(row.Key.AsSpan(1, 8));
                var decoded = DecodeLocalFeedChange(row.Value);
                changes.Add(new ProviderChange(sequence, decoded.Mutation)
                {
                    ChangeId = decoded.ChangeId
                });
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
            using StorageSnapshot snapshot = _storage.OpenSnapshot();
            ulong watermark = ReadUInt64(snapshot, [LocalHighWatermark]);
            Guid activeGeneration = ReadActiveGeneration(snapshot);
            var fields = snapshot.ScanPrefix(CurrentPrefix(CurrentField, activeGeneration), ReadOnlySpan<byte>.Empty, int.MaxValue);
            var recordTombstones = snapshot.ScanPrefix(CurrentPrefix(CurrentRecordTombstone, activeGeneration), ReadOnlySpan<byte>.Empty, int.MaxValue);
            StorageEntry[] entries = MergeSnapshotEntries(fields, recordTombstones);
            Guid token = Guid.NewGuid();
            _snapshots.Add(token, new ProviderSnapshotState(watermark, entries));
            return new ProviderSnapshotHandle(token, watermark);
        }
    }

    public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_snapshots.TryGetValue(snapshotToken, out var state))
                throw new ObjectDisposedException(nameof(snapshotToken));
            if (afterIndex < 0) throw new ArgumentOutOfRangeException(nameof(afterIndex));
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            StorageEntry[] all = state.Entries;
            if (afterIndex > all.Length) throw new ArgumentOutOfRangeException(nameof(afterIndex));

            int end = Math.Min(all.Length, checked(afterIndex + limit));
            var entries = new List<ProviderMutation>(end - afterIndex);
            for (int i = afterIndex; i < end; i++)
                entries.Add(DecodeMutation(all[i].Value));
            return new ProviderSnapshotPage(entries, end, end == all.Length);
        }
    }

    public void ReleaseSnapshot(Guid snapshotToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            _snapshots.Remove(snapshotToken);
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

            // A single BIN-SYNC batch may contain several immutable operations
            // for one field (for example, multiple commits before the next sync).
            // Keep all operations in the receipt fingerprint; the LWW comparison below
            // folds them into one current value without losing batch identity.
            foreach (var mutation in mutations)
                ValidateMutation(mutation);

            byte[] fingerprint = Fingerprint(mutations);
            byte[] receiptKey = Prefixed(ApplyReceipt, applyToken.ToByteArray());
            using var batch = _storage.BeginWriteBatch();
            if (batch.TryGet(receiptKey, out var prior))
            {
                if (!prior!.AsSpan().SequenceEqual(fingerprint))
                    throw new InvalidDataException("Apply token was reused with different canonical input.");
                return new ProviderApplyReceipt(applyToken, prior!.ToArray(), IsDuplicate: true);
            }

            Guid activeGeneration = ReadActiveGeneration(_storage);
            foreach (var mutation in mutations)
            {
                byte[] key = mutation.IsRecordTombstone
                    ? CurrentRecordTombstoneKey(activeGeneration, mutation.RecordId)
                    : CurrentKey(activeGeneration, mutation.RecordId, mutation.FieldId);
                if (batch.TryGet(key, out var priorValue))
                {
                    ProviderMutation existing = DecodeMutation(priorValue!);
                    int comparison = Compare(mutation, existing);
                    if (comparison < 0) continue;
                    if (comparison == 0)
                    {
                        if (!SameMutation(mutation, existing))
                            throw new InvalidDataException("Remote provider apply equivocated at the same HLC and author.");
                        continue;
                    }
                }

                batch.Put(key, EncodeMutation(mutation));
            }

            batch.Put(receiptKey, fingerprint);
            batch.Commit();
            return new ProviderApplyReceipt(applyToken, fingerprint, IsDuplicate: false);
        }
    }

    /// <summary>
    /// Compute the canonical digest expected by BeginFullReplace. Mutations are sorted
    /// by RecordId, then record tombstone before fields, then FieldId. Duplicate
    /// snapshot identities are rejected instead of being silently collapsed.
    /// </summary>
    public static byte[] ComputeFullReplaceDigest(
        Guid generation, ulong sourceChangeFeedWatermark,
        IEnumerable<ProviderMutation> mutations)
    {
        if (generation == Guid.Empty) throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
        ArgumentNullException.ThrowIfNull(mutations);
        var ordered = mutations.ToArray();
        foreach (var mutation in ordered) ValidateMutation(mutation);
        Array.Sort(ordered, CompareSnapshotOrder);
        using var builder = new FullReplaceDigestBuilder(generation, sourceChangeFeedWatermark);
        for (int offset = 0; offset < ordered.Length; offset += MaxFullReplacePageEntries)
        {
            int count = Math.Min(MaxFullReplacePageEntries, ordered.Length - offset);
            builder.AppendPage(new ArraySegment<ProviderMutation>(ordered, offset, count));
        }
        return builder.Complete();
    }

    /// <summary>
    /// Incremental digest builder for a stable provider snapshot. Append pages in
    /// canonical order as returned by ReadSnapshot; no full-image materialization or
    /// sort is required. Entry count is appended at completion so the builder can
    /// stream before the total is known.
    /// </summary>
    public sealed class FullReplaceDigestBuilder : IDisposable
    {
        private readonly IncrementalHash _hash;
        private readonly byte[] _lastSortKey = new byte[19];
        private int _entryCount;
        private bool _completed;
        private bool _faulted;

        public FullReplaceDigestBuilder(Guid generation, ulong sourceChangeFeedWatermark)
        {
            if (generation == Guid.Empty)
                throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
            _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            AppendFullReplaceHeader(_hash, generation, sourceChangeFeedWatermark);
        }

        public int EntryCount => _entryCount;

        public void AppendPage(IReadOnlyList<ProviderMutation> entries)
        {
            if (_completed) throw new InvalidOperationException("Full-replace digest builder is already completed.");
            if (_faulted) throw new InvalidOperationException("Full-replace digest builder is faulted.");
            ArgumentNullException.ThrowIfNull(entries);
            if (entries.Count > MaxFullReplacePageEntries)
                throw new ArgumentOutOfRangeException(nameof(entries), "Digest page exceeds the bounded provider page size.");
            try
            {
                foreach (var mutation in entries)
                {
                    ValidateMutation(mutation);
                    byte[] sortKey = FullReplaceSortKey(mutation);
                    if (_entryCount > 0 && _lastSortKey.AsSpan().SequenceCompareTo(sortKey) >= 0)
                        throw new InvalidDataException("Full-replace digest pages must be strictly ordered and contain no duplicate identities.");
                    _hash.AppendData(EncodeMutation(mutation));
                    sortKey.CopyTo(_lastSortKey, 0);
                    _entryCount = checked(_entryCount + 1);
                }
            }
            catch
            {
                _faulted = true;
                throw;
            }
        }

        public byte[] Complete()
        {
            if (_completed) throw new InvalidOperationException("Full-replace digest builder is already completed.");
            if (_faulted) throw new InvalidOperationException("Full-replace digest builder is faulted.");
            AppendFullReplaceCount(_hash, _entryCount);
            _completed = true;
            return _hash.GetHashAndReset();
        }

        public void Dispose() => _hash.Dispose();
    }

    /// <summary>Start or idempotently resume staging one authoritative replacement generation.</summary>
    public void BeginFullReplace(Guid generation, ulong sourceChangeFeedWatermark,
        int expectedEntryCount, ReadOnlyMemory<byte> expectedDigest)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (generation == Guid.Empty) throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
            if (expectedEntryCount < 0) throw new ArgumentOutOfRangeException(nameof(expectedEntryCount));
            if (expectedDigest.Length != 32) throw new ArgumentException("Full-replace digest must be SHA-256.", nameof(expectedDigest));
            byte[] sessionKey = FullReplaceSessionKey(generation);
            using var snapshot = _storage.OpenSnapshot();
            Guid activeGeneration = ReadActiveGeneration(snapshot);
            if (snapshot.TryGet(sessionKey, out var existingBytes))
            {
                FullReplaceState existing = DecodeFullReplaceState(existingBytes!);
                if (existing.ExpectedCount != expectedEntryCount ||
                    existing.SourceFeedWatermark != sourceChangeFeedWatermark ||
                    !existing.ExpectedDigest.AsSpan().SequenceEqual(expectedDigest.Span))
                    throw new InvalidDataException("Full-replace generation was reused with conflicting manifest data.");
                if (existing.IsCommitted)
                {
                    if (activeGeneration == generation) return;
                    throw new InvalidDataException("Committed full-replace manifest is not the active generation.");
                }
                if (activeGeneration != existing.BaseGeneration)
                    throw new InvalidOperationException("Full-replace staging is stale relative to the active provider generation.");
                return;
            }
            if (activeGeneration == generation)
                throw new InvalidOperationException("Full-replace generation is already active.");

            if (snapshot.ScanPrefix(CurrentPrefix(CurrentField, generation), ReadOnlySpan<byte>.Empty, 1).Count != 0 ||
                snapshot.ScanPrefix(CurrentPrefix(CurrentRecordTombstone, generation), ReadOnlySpan<byte>.Empty, 1).Count != 0)
                throw new InvalidDataException("Full-replace generation has orphaned staged rows without a session manifest.");

            using var batch = _storage.BeginWriteBatch();
            if (_storage.LastCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, _storage.LastCommitSeq);
            batch.Put(sessionKey, EncodeFullReplaceState(new FullReplaceState(
                expectedEntryCount, sourceChangeFeedWatermark, expectedDigest.ToArray(),
                0, false, activeGeneration, new byte[19])));
            batch.Commit();
        }
    }

    /// <summary>Stage one contiguous, canonically ordered page; staged rows remain invisible until commit.</summary>
    public void StageFullReplacePage(Guid generation, int startIndex,
        IReadOnlyList<ProviderMutation> entries)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(entries);
            if (generation == Guid.Empty) throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
            if (startIndex < 0) throw new ArgumentOutOfRangeException(nameof(startIndex));
            if (entries.Count == 0) throw new ArgumentException("Full-replace page must not be empty.", nameof(entries));
            if (entries.Count > MaxFullReplacePageEntries)
                throw new ArgumentOutOfRangeException(nameof(entries), "Full-replace page exceeds the bounded provider page size.");

            using var snapshot = _storage.OpenSnapshot();
            byte[] sessionKey = FullReplaceSessionKey(generation);
            if (!snapshot.TryGet(sessionKey, out var stateBytes))
                throw new InvalidOperationException("Full-replace generation has not been begun.");
            FullReplaceState state = DecodeFullReplaceState(stateBytes!);
            if (state.IsCommitted) throw new InvalidOperationException("Committed full-replace generation cannot accept more pages.");
            if (ReadActiveGeneration(snapshot) != state.BaseGeneration)
                throw new InvalidOperationException("Full-replace staging is stale relative to the active provider generation.");
            if (startIndex != state.NextIndex)
                throw new InvalidDataException("Full-replace page is not contiguous with the durable staging cursor.");
            if (entries.Count > state.ExpectedCount - state.NextIndex)
                throw new InvalidDataException("Full-replace page exceeds the declared snapshot entry count.");

            using var batch = _storage.BeginWriteBatch();
            byte[] lastSortKey = state.LastSortKey;
            for (int i = 0; i < entries.Count; i++)
            {
                ProviderMutation mutation = entries[i];
                ValidateMutation(mutation);
                byte[] sortKey = FullReplaceSortKey(mutation);
                if (state.NextIndex + i > 0 &&
                    lastSortKey.AsSpan().SequenceCompareTo(sortKey) >= 0)
                    throw new InvalidDataException("Full-replace pages must be strictly ordered and contain no duplicate identities.");
                byte[] rowKey = mutation.IsRecordTombstone
                    ? CurrentRecordTombstoneKey(generation, mutation.RecordId)
                    : CurrentKey(generation, mutation.RecordId, mutation.FieldId);
                if (batch.TryGet(rowKey, out _))
                    throw new InvalidDataException("Full-replace snapshot contains a duplicate record/field row.");
                batch.Put(rowKey, EncodeMutation(mutation));
                lastSortKey = sortKey;
            }

            FullReplaceState updated = state with
            {
                NextIndex = checked(state.NextIndex + entries.Count),
                LastSortKey = lastSortKey
            };
            batch.Put(sessionKey, EncodeFullReplaceState(updated));
            if (_storage.LastCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, _storage.LastCommitSeq);
            batch.Commit();
        }
    }

    /// <summary>
    /// Verify the complete staged image and atomically switch the active generation.
    /// The old generation is retained; reclamation is a separate, explicit operation.
    /// </summary>
    public void CommitFullReplace(Guid generation)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (generation == Guid.Empty) throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
            using var snapshot = _storage.OpenSnapshot();
            byte[] sessionKey = FullReplaceSessionKey(generation);
            if (!snapshot.TryGet(sessionKey, out var stateBytes))
                throw new InvalidOperationException("Full-replace generation has not been begun.");
            FullReplaceState state = DecodeFullReplaceState(stateBytes!);
            Guid active = ReadActiveGeneration(snapshot);
            if (state.IsCommitted)
            {
                if (active != generation)
                    throw new InvalidDataException("Committed full-replace manifest does not match the active generation.");
                return;
            }
            if (active != state.BaseGeneration)
                throw new InvalidOperationException("Full-replace staging is stale; active provider generation changed before commit.");
            if (state.NextIndex != state.ExpectedCount)
                throw new InvalidOperationException("Full-replace snapshot is incomplete.");

            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            AppendFullReplaceHeader(hash, generation, state.SourceFeedWatermark);
            using var fields = EnumeratePrefix(snapshot, CurrentPrefix(CurrentField, generation)).GetEnumerator();
            using var tombstones = EnumeratePrefix(snapshot, CurrentPrefix(CurrentRecordTombstone, generation)).GetEnumerator();
            bool hasField = fields.MoveNext();
            bool hasTombstone = tombstones.MoveNext();
            ProviderMutation? previousMutation = null;
            int stagedCount = 0;
            while (hasField || hasTombstone)
            {
                StorageEntry entry;
                if (!hasField)
                {
                    entry = tombstones.Current;
                    hasTombstone = tombstones.MoveNext();
                }
                else if (!hasTombstone)
                {
                    entry = fields.Current;
                    hasField = fields.MoveNext();
                }
                else if (CompareSnapshotEntries(fields.Current, tombstones.Current) <= 0)
                {
                    entry = fields.Current;
                    hasField = fields.MoveNext();
                }
                else
                {
                    entry = tombstones.Current;
                    hasTombstone = tombstones.MoveNext();
                }

                ProviderMutation mutation = DecodeMutation(entry.Value);
                ValidateGenerationEntry(generation, entry, mutation);
                if (previousMutation is not null &&
                    CompareSnapshotOrder(previousMutation, mutation) >= 0)
                    throw new InvalidDataException("Full-replace staged image is not in canonical unique order.");
                previousMutation = mutation;
                hash.AppendData(entry.Value);
                stagedCount = checked(stagedCount + 1);
                if (stagedCount > state.ExpectedCount)
                    throw new InvalidDataException("Full-replace staged row count exceeds the signed manifest.");
            }
            if (stagedCount != state.ExpectedCount)
                throw new InvalidDataException("Full-replace staged row count differs from the signed manifest.");
            AppendFullReplaceCount(hash, stagedCount);
            byte[] actualDigest = hash.GetHashAndReset();
            if (!CryptographicOperations.FixedTimeEquals(actualDigest, state.ExpectedDigest))
                throw new InvalidDataException("Full-replace staged image digest does not match its manifest.");

            using var batch = _storage.BeginWriteBatch();
            if (_storage.LastCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, _storage.LastCommitSeq);
            byte[] generationBytes = new byte[16];
            generation.TryWriteBytes(generationBytes, true, out _);
            batch.Put([ActiveGeneration], generationBytes);
            batch.Put(sessionKey, EncodeFullReplaceState(state with { IsCommitted = true }));
            batch.Commit();
        }
    }

    /// <summary>Discard an uncommitted staged generation without changing active application state.</summary>
    public bool AbortFullReplace(Guid generation, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (generation == Guid.Empty) throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
            if (limit <= 0 || limit > MaxFullReplacePageEntries)
                throw new ArgumentOutOfRangeException(nameof(limit), "Abort page size must be between 1 and the provider bound.");
            if (ReadActiveGeneration(_storage) == generation)
                throw new InvalidOperationException("The active full-replace generation cannot be aborted.");
            using var snapshot = _storage.OpenSnapshot();
            byte[] sessionKey = FullReplaceSessionKey(generation);
            if (!snapshot.TryGet(sessionKey, out var stateBytes)) return true;
            if (DecodeFullReplaceState(stateBytes!).IsCommitted)
                throw new InvalidOperationException("Committed full-replace generation cannot be aborted.");

            var fields = snapshot.ScanPrefix(CurrentPrefix(CurrentField, generation), ReadOnlySpan<byte>.Empty, limit);
            int remaining = limit - fields.Count;
            var tombstones = remaining > 0
                ? snapshot.ScanPrefix(CurrentPrefix(CurrentRecordTombstone, generation), ReadOnlySpan<byte>.Empty, remaining)
                : Array.Empty<StorageEntry>();
            bool complete = fields.Count < limit && tombstones.Count < remaining;

            using var batch = _storage.BeginWriteBatch();
            foreach (var row in fields) batch.Delete(row.Key);
            foreach (var row in tombstones) batch.Delete(row.Key);
            if (complete) batch.Delete(sessionKey);
            if (_storage.LastCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, _storage.LastCommitSeq);
            batch.Commit();
            return complete;
        }
    }

    public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            Guid activeGeneration = ReadActiveGeneration(_storage);
            if (_storage.TryGet(CurrentKey(activeGeneration, recordId, fieldId), out var value))
            {
                ProviderMutation field = DecodeMutation(value!);
                if (_storage.TryGet(CurrentRecordTombstoneKey(activeGeneration, recordId), out var tombstoneBytes))
                {
                    ProviderMutation tombstone = DecodeMutation(tombstoneBytes!);
                    if (Compare(tombstone, field) >= 0)
                    {
                        mutation = null;
                        return false;
                    }
                }

                mutation = field;
                return true;
            }

            mutation = null;
            return false;
        }
    }

    public bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_storage.TryGet([ProviderPublicationMode], out var value))
            {
                nodeOutboxHighWatermark = 0;
                return false;
            }
            nodeOutboxHighWatermark = ReadUInt64(value!);
            return true;
        }
    }

    public void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            using var snapshot = _storage.OpenSnapshot();
            if (snapshot.TryGet([ProviderPublicationMode], out var existing))
            {
                ulong priorCutover = ReadUInt64(existing!);
                if (priorCutover != nodeOutboxHighWatermark)
                    throw new InvalidOperationException("Provider publication cutover watermark is immutable.");
                return;
            }

            ulong high = ReadUInt64(snapshot, [LocalHighWatermark]);
            ulong acknowledged = ReadUInt64(snapshot, [LocalAckWatermark]);
            if (high != acknowledged)
                throw new InvalidOperationException("Provider local feed must be fully acknowledged before Metadata-Store-owned publication is enabled.");

            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            batch.Put([ProviderPublicationMode], EncodeUInt64(nodeOutboxHighWatermark));
            batch.Commit();
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            _snapshots.Clear();
            if (_ownsStorage) _storage.Dispose();
            if (_deleteOnDispose && _directory is { } directory)
            {
                try { Directory.Delete(directory, recursive: true); }
                catch { /* best-effort cleanup for isolated test directories */ }
            }
        }
    }

    private void EnsureFormatMarker()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            using var snapshot = _storage.OpenSnapshot();
            if (snapshot.TryGet([ProviderFormatMarker], out var existing))
            {
                if (!existing!.AsSpan().SequenceEqual(ProviderFormatV3))
                    throw new InvalidDataException("Storage Data Provider format marker is not v3.");
                _ = ReadActiveGeneration(snapshot);
                return;
            }

            foreach (byte prefix in ProviderOwnedPrefixes)
            {
                if (snapshot.ScanPrefix([prefix], ReadOnlySpan<byte>.Empty, 1).Count != 0)
                    throw new InvalidDataException(
                        "Storage Data Provider partition has durable rows but no v3 format marker; refusing legacy/ambiguous state.");
            }

            using var batch = _storage.BeginWriteBatch();
            ulong actualBaseCommitSeq = _storage.LastCommitSeq;
            if (actualBaseCommitSeq != snapshot.CommitSeq)
                throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
            batch.Put([ProviderFormatMarker], ProviderFormatV3);
            batch.Put([ActiveGeneration], new byte[16]);
            batch.Commit();
        }
    }

    private static void ValidateMutation(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        if (mutation.RecordId.IsZero || mutation.AuthorNodeId.IsZero)
            throw new ArgumentException("RecordId and original AuthorNodeId must be non-zero.", nameof(mutation));
        ArgumentNullException.ThrowIfNull(mutation.Value);
        if (mutation.IsRecordTombstone)
        {
            if (mutation.IsTombstone || mutation.FieldId != Model.LifecycleFieldId ||
                mutation.KeyEpoch != 0 || mutation.Value.Length != 0)
                throw new ArgumentException("Record tombstone must use the lifecycle field, key epoch zero and no value.", nameof(mutation));
        }
        else if (mutation.IsTombstone && mutation.Value.Length != 0)
        {
            throw new ArgumentException("Field tombstone mutation must not carry a value payload.", nameof(mutation));
        }
    }

    private static int Compare(ProviderMutation left, ProviderMutation right)
        => HlcOps.CompareHlc(
            new Hlc(left.HlcWall, left.HlcLogical), left.AuthorNodeId,
            new Hlc(right.HlcWall, right.HlcLogical), right.AuthorNodeId);

    private static bool SameMutation(ProviderMutation left, ProviderMutation right)
        => left.RecordId == right.RecordId
            && left.FieldId == right.FieldId
            && left.IsTombstone == right.IsTombstone
            && left.IsRecordTombstone == right.IsRecordTombstone
            && left.HlcWall == right.HlcWall
            && left.HlcLogical == right.HlcLogical
            && left.AuthorNodeId == right.AuthorNodeId
            && left.KeyEpoch == right.KeyEpoch
            && left.Value.AsSpan().SequenceEqual(right.Value);

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
            fixedPart[18] = (byte)((mutation.IsTombstone ? 1 : 0) | (mutation.IsRecordTombstone ? 2 : 0));
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
        if (c != 0) return c;
        if (left.IsRecordTombstone != right.IsRecordTombstone)
            return left.IsRecordTombstone ? -1 : 1;
        return left.FieldId.CompareTo(right.FieldId);
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

    private static byte[] EncodeLocalFeedChange(byte[] encodedMutation)
    {
        byte[] identity = NewChangeIdentityBytes();
        byte[] result = new byte[checked(encodedMutation.Length + identity.Length)];
        encodedMutation.CopyTo(result, 0);
        identity.CopyTo(result, encodedMutation.Length);
        return result;
    }

    private static (ProviderMutation Mutation, BatchId ChangeId)
        DecodeLocalFeedChange(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 69)
            throw new InvalidDataException("Stored v3 provider local feed mutation or ChangeId is truncated.");
        uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(49, 4));
        if (valueLength > int.MaxValue)
            throw new InvalidDataException("Stored provider local feed value length is invalid.");
        int mutationLength = checked(53 + (int)valueLength);
        if (bytes.Length - mutationLength != 16)
            throw new InvalidDataException("Stored v3 provider local feed entry must contain exactly one ChangeId.");
        BatchId changeId = BatchId.From(bytes.Slice(mutationLength, 16));
        if (changeId.IsZero)
            throw new InvalidDataException("Stored provider local feed identity must not be zero.");
        return (DecodeMutation(bytes[..mutationLength]), changeId);
    }

    private static byte[] NewChangeIdentityBytes()
    {
        byte[] identity = new byte[16];
        Guid id;
        do { id = Guid.NewGuid(); } while (id == Guid.Empty);
        id.TryWriteBytes(identity);
        return identity;
    }

    private static byte[] EncodeMutation(ProviderMutation mutation)
    {
        byte[] result = new byte[53 + mutation.Value.Length];
        mutation.RecordId.WriteTo(result);
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(16), mutation.FieldId);
        result[18] = (byte)((mutation.IsTombstone ? 1 : 0) | (mutation.IsRecordTombstone ? 2 : 0));
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
        if (valueLength > int.MaxValue || bytes.Length != 53 + (int)valueLength ||
            bytes[18] > 3 || bytes[18] == 3)
            throw new InvalidDataException("Stored provider mutation length or flags are invalid.");
        var mutation = new ProviderMutation
        {
            RecordId = RecordId.From(bytes.Slice(0, 16)),
            FieldId = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(16)),
            IsTombstone = (bytes[18] & 1) != 0,
            IsRecordTombstone = (bytes[18] & 2) != 0,
            HlcWall = BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(19)),
            HlcLogical = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(27)),
            AuthorNodeId = NodeId.From(bytes.Slice(31, 16)),
            KeyEpoch = BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(47)),
            Value = bytes.Slice(53).ToArray()
        };
        try { ValidateMutation(mutation); }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Stored provider mutation violates its canonical contract.", exception);
        }
        return mutation;
    }

    private static ProviderMutation CloneMutation(ProviderMutation mutation)
        => new()
        {
            RecordId = mutation.RecordId,
            FieldId = mutation.FieldId,
            IsTombstone = mutation.IsTombstone,
            IsRecordTombstone = mutation.IsRecordTombstone,
            HlcWall = mutation.HlcWall,
            HlcLogical = mutation.HlcLogical,
            AuthorNodeId = mutation.AuthorNodeId,
            KeyEpoch = mutation.KeyEpoch,
            Value = mutation.Value.ToArray()
        };

    private static StorageEntry[] MergeSnapshotEntries(
        IReadOnlyList<StorageEntry> fields,
        IReadOnlyList<StorageEntry> recordTombstones)
    {
        var merged = new StorageEntry[checked(fields.Count + recordTombstones.Count)];
        int fieldIndex = 0;
        int tombstoneIndex = 0;
        int outputIndex = 0;

        while (fieldIndex < fields.Count || tombstoneIndex < recordTombstones.Count)
        {
            if (fieldIndex >= fields.Count)
            {
                merged[outputIndex++] = recordTombstones[tombstoneIndex++];
                continue;
            }
            if (tombstoneIndex >= recordTombstones.Count)
            {
                merged[outputIndex++] = fields[fieldIndex++];
                continue;
            }

            if (CompareSnapshotEntries(fields[fieldIndex], recordTombstones[tombstoneIndex]) <= 0)
                merged[outputIndex++] = fields[fieldIndex++];
            else
                merged[outputIndex++] = recordTombstones[tombstoneIndex++];
        }

        return merged;
    }

    private static int CompareSnapshotEntries(StorageEntry left, StorageEntry right)
    {
        if (left.Key.Length < 33 || right.Key.Length < 33)
            throw new InvalidDataException("Provider snapshot contains a truncated generation-scoped storage key.");
        if (!left.Key.AsSpan(1, 16).SequenceEqual(right.Key.AsSpan(1, 16)))
            throw new InvalidDataException("Provider snapshot mixes active generations.");

        int recordOrder = left.Key.AsSpan(17, 16).SequenceCompareTo(right.Key.AsSpan(17, 16));
        if (recordOrder != 0) return recordOrder;

        bool leftTombstone = left.Key[0] == CurrentRecordTombstone;
        bool rightTombstone = right.Key[0] == CurrentRecordTombstone;
        if (leftTombstone != rightTombstone) return leftTombstone ? -1 : 1;
        if (leftTombstone) return left.Key.Length == 33 && right.Key.Length == 33 ? 0 :
            throw new InvalidDataException("Provider snapshot contains an invalid record tombstone key.");

        if (left.Key[0] != CurrentField || right.Key[0] != CurrentField ||
            left.Key.Length != 35 || right.Key.Length != 35)
            throw new InvalidDataException("Provider snapshot contains an invalid current-field key.");

        return left.Key.AsSpan(33, 2).SequenceCompareTo(right.Key.AsSpan(33, 2));
    }

    private static int CompareSnapshotOrder(ProviderMutation left, ProviderMutation right)
    {
        int order = CompareRecordIds(left.RecordId, right.RecordId);
        if (order != 0) return order;
        if (left.IsRecordTombstone != right.IsRecordTombstone)
            return left.IsRecordTombstone ? -1 : 1;
        return left.FieldId.CompareTo(right.FieldId);
    }

    private static void ValidateGenerationEntry(Guid generation, StorageEntry entry, ProviderMutation mutation)
    {
        Span<byte> generationBytes = stackalloc byte[16];
        generation.TryWriteBytes(generationBytes, true, out _);
        if (entry.Key.Length < 17 || !entry.Key.AsSpan(1, 16).SequenceEqual(generationBytes))
            throw new InvalidDataException("Full-replace row belongs to a different generation.");

        Span<byte> recordBytes = stackalloc byte[16];
        mutation.RecordId.WriteTo(recordBytes);
        if (mutation.IsRecordTombstone)
        {
            if (entry.Key[0] != CurrentRecordTombstone || entry.Key.Length != 33 ||
                !entry.Key.AsSpan(17, 16).SequenceEqual(recordBytes))
                throw new InvalidDataException("Full-replace record tombstone key does not match its value.");
            return;
        }

        if (entry.Key[0] != CurrentField || entry.Key.Length != 35 ||
            !entry.Key.AsSpan(17, 16).SequenceEqual(recordBytes) ||
            BinaryPrimitives.ReadUInt16BigEndian(entry.Key.AsSpan(33, 2)) != mutation.FieldId)
            throw new InvalidDataException("Full-replace field key does not match its value.");
    }

    private static IEnumerable<StorageEntry> EnumeratePrefix(
        StorageSnapshot snapshot, byte[] prefix, int pageSize = 1024)
    {
        byte[] after = Array.Empty<byte>();
        while (true)
        {
            IReadOnlyList<StorageEntry> page = snapshot.ScanPrefix(prefix, after, pageSize);
            if (page.Count == 0) yield break;
            foreach (var row in page) yield return row;
            after = page[^1].Key;
            if (page.Count < pageSize) yield break;
        }
    }

    private static byte[] FullReplaceSessionKey(Guid generation)
    {
        byte[] key = new byte[17];
        key[0] = FullReplaceSession;
        generation.TryWriteBytes(key.AsSpan(1), true, out _);
        return key;
    }

    private static byte[] FullReplaceSortKey(ProviderMutation mutation)
    {
        byte[] key = new byte[19];
        mutation.RecordId.WriteTo(key);
        key[16] = mutation.IsRecordTombstone ? (byte)0 : (byte)1;
        BinaryPrimitives.WriteUInt16BigEndian(key.AsSpan(17), mutation.IsRecordTombstone ? (ushort)0 : mutation.FieldId);
        return key;
    }

    private static void AppendFullReplaceHeader(IncrementalHash hash, Guid generation,
        ulong sourceChangeFeedWatermark)
    {
        hash.AppendData("BIN-SYNC/FULL-REPLACE/v1"u8);
        Span<byte> header = stackalloc byte[24];
        generation.TryWriteBytes(header[..16], true, out _);
        BinaryPrimitives.WriteUInt64LittleEndian(header[16..24], sourceChangeFeedWatermark);
        hash.AppendData(header);
    }

    private static void AppendFullReplaceCount(IncrementalHash hash, int count)
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, count);
        hash.AppendData(bytes);
    }

    private static byte[] EncodeFullReplaceState(FullReplaceState state)
    {
        if (state.ExpectedDigest.Length != 32 || state.LastSortKey.Length != 19)
            throw new InvalidDataException("Full-replace state has an invalid fixed-width field.");
        byte[] bytes = new byte[85];
        bytes[0] = 1;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(1), state.ExpectedCount);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(5), state.SourceFeedWatermark);
        state.ExpectedDigest.CopyTo(bytes, 13);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(45), state.NextIndex);
        bytes[49] = state.IsCommitted ? (byte)1 : (byte)0;
        state.BaseGeneration.TryWriteBytes(bytes.AsSpan(50, 16), true, out _);
        state.LastSortKey.CopyTo(bytes, 66);
        return bytes;
    }

    private static FullReplaceState DecodeFullReplaceState(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 85 || bytes[0] != 1 || bytes[49] > 1)
            throw new InvalidDataException("Stored full-replace session manifest is malformed.");
        int expected = BinaryPrimitives.ReadInt32LittleEndian(bytes[1..]);
        int next = BinaryPrimitives.ReadInt32LittleEndian(bytes[45..]);
        if (expected < 0 || next < 0 || next > expected)
            throw new InvalidDataException("Stored full-replace staging cursor is invalid.");
        return new FullReplaceState(expected,
            BinaryPrimitives.ReadUInt64LittleEndian(bytes[5..]),
            bytes.Slice(13, 32).ToArray(), next, bytes[49] != 0,
            new Guid(bytes.Slice(50, 16), true), bytes.Slice(66, 19).ToArray());
    }

    private static byte[] CurrentKey(Guid generation, RecordId recordId, ushort fieldId)
    {
        byte[] key = new byte[35];
        key[0] = CurrentField;
        generation.TryWriteBytes(key.AsSpan(1, 16), true, out _);
        recordId.WriteTo(key.AsSpan(17, 16));
        BinaryPrimitives.WriteUInt16BigEndian(key.AsSpan(33), fieldId);
        return key;
    }

    private static byte[] CurrentRecordTombstoneKey(Guid generation, RecordId recordId)
    {
        byte[] key = new byte[33];
        key[0] = CurrentRecordTombstone;
        generation.TryWriteBytes(key.AsSpan(1, 16), true, out _);
        recordId.WriteTo(key.AsSpan(17));
        return key;
    }

    private static byte[] CurrentPrefix(byte kind, Guid generation)
    {
        byte[] prefix = new byte[17];
        prefix[0] = kind;
        generation.TryWriteBytes(prefix.AsSpan(1), true, out _);
        return prefix;
    }

    private static Guid ReadActiveGeneration(StorageDatabase storage)
    {
        if (!storage.TryGet([ActiveGeneration], out var value) || value!.Length != 16)
            throw new InvalidDataException("Storage Data Provider active generation marker is missing or malformed.");
        return new Guid(value!, true);
    }

    private static Guid ReadActiveGeneration(StorageSnapshot snapshot)
    {
        if (!snapshot.TryGet([ActiveGeneration], out var value) || value!.Length != 16)
            throw new InvalidDataException("Storage Data Provider active generation marker is missing or malformed.");
        return new Guid(value!, true);
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
