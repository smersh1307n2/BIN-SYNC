using System.Buffers.Binary;
using System.Security.Cryptography;
using BinSync;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>
/// Durable BIN-SYNC Data Provider backed by the neutral Fld1.Storage byte API.
/// </summary>
public class StorageDataProvider : IDataProvider, IProviderPublicationMode, IProviderFullReplace, IProviderPublishedOriginStore, IProviderAuthorEpochClosureStore, IProviderCommitGuard
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
    private const int MaxFullReplacePageEntries = ProviderFullReplaceDigestBuilder.MaxPageEntries;
    private const byte ProviderFormatMarker = 0x25;
    private const byte ClosedAuthorEpoch = 0x27;
    private const byte ClosedAuthorIdentity = 0x28;
    private const byte ActiveAuthorEpoch = 0x29;
    private const byte RetiredFinalAuthorEpoch = 0x2A;
    private const byte ActiveGeneration = 0x26;
    private const byte FullReplaceSession = 0x41;
    private const byte ApplyReceipt = 0x30;
    private static ReadOnlySpan<byte> ProviderFormatV3 => [0x42, 0x53, 0x50, 0x33];
    private static readonly byte[] ProviderOwnedPrefixes =
        [ItemMap, ItemReverse, FieldMap, FieldReverse, CurrentField, CurrentRecordTombstone,
         LocalChange, LocalHighWatermark, LocalAckWatermark, LocalReadWatermark,
         ProviderPublicationMode, ActiveGeneration, FullReplaceSession, ApplyReceipt, ClosedAuthorEpoch,
         ClosedAuthorIdentity, ActiveAuthorEpoch, RetiredFinalAuthorEpoch];

    private readonly object _gate;
    private readonly StorageDatabase _storage;
    private readonly string? _directory;
    private readonly bool _deleteOnDispose;
    private readonly bool _ownsStorage;
    private readonly Dictionary<Guid, ProviderSnapshotState> _snapshots = new();
    private bool _disposed;

    private sealed class ProviderSnapshotState : IDisposable
    {
        private readonly StorageSnapshot _snapshot;
        private readonly Guid _activeGeneration;
        private readonly byte[] _fieldPrefix;
        private readonly byte[] _recordTombstonePrefix;
        private StorageSnapshot.PrefixCursor _fieldCursor;
        private StorageSnapshot.PrefixCursor _recordTombstoneCursor;
        private ProviderMutation? _fieldMutation;
        private ProviderMutation? _recordTombstoneMutation;
        private bool _disposed;

        public ulong LocalHighWatermark { get; }
        public int NextIndex { get; private set; }

        public ProviderSnapshotState(
            StorageSnapshot snapshot, ulong localHighWatermark, Guid activeGeneration,
            byte[] fieldPrefix, byte[] recordTombstonePrefix)
        {
            _snapshot = snapshot;
            LocalHighWatermark = localHighWatermark;
            _activeGeneration = activeGeneration;
            _fieldPrefix = fieldPrefix;
            _recordTombstonePrefix = recordTombstonePrefix;
            _fieldCursor = snapshot.OpenPrefixCursor(fieldPrefix);
            _recordTombstoneCursor = snapshot.OpenPrefixCursor(recordTombstonePrefix);
        }

        public bool TryReadNext(out ProviderMutation? mutation)
        {
            EnsureOpen();
            bool hasField = TryPeekField(out ProviderMutation? field);
            bool hasRecordTombstone = TryPeekRecordTombstone(out ProviderMutation? recordTombstone);
            if (!hasField && !hasRecordTombstone)
            {
                mutation = null;
                return false;
            }

            if (hasRecordTombstone &&
                (!hasField || ProviderMutationCodec.CompareCanonical(recordTombstone!, field!) < 0))
            {
                mutation = recordTombstone;
                ConsumeRecordTombstone();
            }
            else
            {
                mutation = field;
                ConsumeField();
            }
            NextIndex = checked(NextIndex + 1);
            return true;
        }

        public bool HasNext()
        {
            EnsureOpen();
            return TryPeekField(out _) || TryPeekRecordTombstone(out _);
        }

        public void Reset()
        {
            EnsureOpen();
            _fieldCursor.Dispose();
            _recordTombstoneCursor.Dispose();
            _fieldCursor = _snapshot.OpenPrefixCursor(_fieldPrefix);
            _recordTombstoneCursor = _snapshot.OpenPrefixCursor(_recordTombstonePrefix);
            _fieldMutation = null;
            _recordTombstoneMutation = null;
            NextIndex = 0;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _fieldCursor.Dispose();
            _recordTombstoneCursor.Dispose();
            _snapshot.Dispose();
            _fieldMutation = null;
            _recordTombstoneMutation = null;
        }

        private bool TryPeekField(out ProviderMutation? mutation)
        {
            EnsureOpen();
            if (_fieldMutation is null)
            {
                if (!_fieldCursor.TryReadNext(out StorageEntryView row))
                {
                    mutation = null;
                    return false;
                }
                ProviderMutation decoded = DecodeMutation(row.Value);
                ValidateGenerationEntry(_activeGeneration, row, decoded);
                _fieldMutation = decoded;
            }
            mutation = _fieldMutation;
            return true;
        }

        private bool TryPeekRecordTombstone(out ProviderMutation? mutation)
        {
            EnsureOpen();
            if (_recordTombstoneMutation is null)
            {
                if (!_recordTombstoneCursor.TryReadNext(out StorageEntryView row))
                {
                    mutation = null;
                    return false;
                }
                ProviderMutation decoded = DecodeMutation(row.Value);
                ValidateGenerationEntry(_activeGeneration, row, decoded);
                _recordTombstoneMutation = decoded;
            }
            mutation = _recordTombstoneMutation;
            return true;
        }

        private void ConsumeField() => _fieldMutation = null;

        private void ConsumeRecordTombstone() => _recordTombstoneMutation = null;

        private void EnsureOpen() => ObjectDisposedException.ThrowIf(_disposed, this);
    }
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

    internal bool SharesStorageWith(StorageMetadataStore metadataStore)
    {
        ArgumentNullException.ThrowIfNull(metadataStore);
        return metadataStore.IsBoundTo(_storage);
    }

    internal bool SharesStorageWith(StorageNamespaceGcStore gcStore)
    {
        ArgumentNullException.ThrowIfNull(gcStore);
        return gcStore.IsBoundTo(_storage);
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

    /// <summary>Acquire the provider transaction gate exclusively; dispose on the acquiring thread.</summary>
    public IDisposable AcquireCommitGuard()
    {
        Monitor.Enter(_gate);
        return new ProviderCommitLease(_gate);
    }

    private sealed class ProviderCommitLease : IDisposable
    {
        private object? _gate;
        public ProviderCommitLease(object gate) => _gate = gate;
        public void Dispose()
        {
            object? gate = Interlocked.Exchange(ref _gate, null);
            if (gate is not null) Monitor.Exit(gate);
        }
    }

    public void CloseLocalAuthorIdentity(uint finalAuthorEpoch)
    {
        if (finalAuthorEpoch == 0) throw new ArgumentOutOfRangeException(nameof(finalAuthorEpoch));
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_storage.TryGet(ClosedAuthorIdentityKey(), out _))
            {
                if (_storage.TryGet(ActiveAuthorEpochKey(), out _))
                    throw new InvalidDataException("Retired author identity unexpectedly retains an active epoch marker.");
                if (!_storage.TryGet(RetiredFinalAuthorEpochKey(), out byte[]? retiredEpochBytes) ||
                    retiredEpochBytes!.Length != 4 ||
                    BinaryPrimitives.ReadUInt32LittleEndian(retiredEpochBytes) != finalAuthorEpoch)
                    throw new InvalidOperationException("Retried terminal retirement must name the exact previously retired final author epoch.");
                return;
            }

            if (!_storage.TryGet(ActiveAuthorEpochKey(), out byte[]? activeEpochBytes) ||
                activeEpochBytes!.Length != 4)
                throw new InvalidDataException("Active author identity is missing or has a malformed epoch marker.");
            uint activeEpoch = BinaryPrimitives.ReadUInt32LittleEndian(activeEpochBytes);
            if (activeEpoch != finalAuthorEpoch)
                throw new InvalidOperationException("Final author report epoch does not match the provider's active author epoch.");
            if (!_storage.TryGet(ClosedAuthorEpochKey(activeEpoch), out _))
                throw new InvalidOperationException("The active author epoch must be durably closed before retiring the author identity.");

            using StorageSnapshot snapshot = _storage.OpenSnapshot();
            using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(Prefixed(LocalChange, ReadOnlySpan<byte>.Empty));
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(1);
            if (page.Count != 0)
                throw new InvalidOperationException("Cannot retire the local author identity while provider feed drafts remain.");
            using var batch = _storage.BeginWriteBatch();
            batch.Put(ClosedAuthorIdentityKey(), Array.Empty<byte>());
            batch.Put(RetiredFinalAuthorEpochKey(), EncodeUInt32(finalAuthorEpoch));
            batch.Delete(ActiveAuthorEpochKey());
            batch.Commit();
        }
    }


    public void ActivateLocalAuthorEpoch(uint authorEpoch)
    {
        if (authorEpoch == 0) throw new ArgumentOutOfRangeException(nameof(authorEpoch));
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_storage.TryGet(ClosedAuthorIdentityKey(), out _))
                throw new InvalidOperationException("A new author epoch can be activated only after terminal author retirement.");
            if (_storage.TryGet(ActiveAuthorEpochKey(), out _))
                throw new InvalidDataException("Retired author identity unexpectedly retains an active epoch marker.");
            if (authorEpoch <= ReadMaxClosedAuthorEpoch())
                throw new InvalidOperationException("A rejoined author must use a fresh epoch greater than every previously closed epoch.");
            using var batch = _storage.BeginWriteBatch();
            batch.Delete(ClosedAuthorIdentityKey());
            batch.Put(ActiveAuthorEpochKey(), EncodeUInt32(authorEpoch));
            batch.Commit();
        }
    }

    public bool TryCloseLocalAuthorEpoch(uint authorEpoch)
    {
        if (authorEpoch == 0) throw new ArgumentOutOfRangeException(nameof(authorEpoch));
        lock (_gate)
        {
            ThrowIfDisposed();
            byte[] closureKey = ClosedAuthorEpochKey(authorEpoch);
            if (_storage.TryGet(closureKey, out _)) return true;

            using StorageSnapshot snapshot = _storage.OpenSnapshot();
            using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(Prefixed(LocalChange, ReadOnlySpan<byte>.Empty));
            const int scanPageSize = 1024;
            while (true)
            {
                IReadOnlyList<StorageEntry> page = cursor.ReadPage(scanPageSize);
                foreach (StorageEntry row in page)
                {
                    var (mutation, _) = DecodeLocalFeedChange(row.Value);
                    if (mutation.OriginAuthorEpoch == authorEpoch)
                        return false;
                }
                if (page.Count < scanPageSize) break;
            }

            using var batch = _storage.BeginWriteBatch();
            batch.Put(closureKey, Array.Empty<byte>());
            batch.Commit();
            return true;
        }
    }

    public void AppendLocalChange(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        lock (_gate)
        {
            ThrowIfDisposed();
            if (mutation.OriginAuthorEpoch == 0 || mutation.OriginSequence != 0)
                throw new ArgumentException("Local provider drafts require a non-zero author epoch and deferred origin sequence.", nameof(mutation));
            if (_storage.TryGet(ClosedAuthorIdentityKey(), out _))
                throw new InvalidOperationException("The local author identity is retired; activate a new epoch after membership rejoin.");
            if (_storage.TryGet(ClosedAuthorEpochKey(mutation.OriginAuthorEpoch), out _))
                throw new InvalidOperationException("The local author epoch has been terminally closed and cannot issue new changes.");
            if (_storage.TryGet(ActiveAuthorEpochKey(), out byte[]? activeEpochBytes))
            {
                if (activeEpochBytes!.Length != 4 ||
                    BinaryPrimitives.ReadUInt32LittleEndian(activeEpochBytes) != mutation.OriginAuthorEpoch)
                    throw new InvalidOperationException("Local provider mutation does not use the currently active author epoch.");
            }
            else
            {
                if (ReadMaxClosedAuthorEpoch() != 0)
                    throw new InvalidOperationException("A provider with retired author epochs requires explicit roster-backed epoch activation.");
                using var activate = _storage.BeginWriteBatch();
                activate.Put(ActiveAuthorEpochKey(), EncodeUInt32(mutation.OriginAuthorEpoch));
                activate.Commit();
            }
            using var batch = _storage.BeginWriteBatch();
            ulong current = batch.TryGet([LocalHighWatermark], out var currentBytes)
                ? ReadUInt64(currentBytes!)
                : 0;
            ulong sequence = checked(current + 1);
            BatchId changeId = mutation.ChangeId.IsZero
                ? BatchId.From(NewChangeIdentityBytes())
                : mutation.ChangeId;
            var ownedMutation = CloneMutation(mutation, changeId, mutation.OriginAuthorEpoch, originSequence: 0);
            ValidateMutation(ownedMutation);
            byte[] owned = EncodeMutation(ownedMutation);
            Guid activeGeneration = ReadActiveGeneration(_storage);
            byte[] currentKey = ownedMutation.IsRecordTombstone
                ? CurrentRecordTombstoneKey(activeGeneration, ownedMutation.RecordId)
                : CurrentKey(activeGeneration, ownedMutation.RecordId, ownedMutation.FieldId);
            if (batch.TryGet(currentKey, out var currentFieldBytes))
            {
                ProviderMutation existing = DecodeMutation(currentFieldBytes!);
                int comparison = Compare(ownedMutation, existing);
                if (comparison == 0)
                {
                    if (!SameMutationContent(ownedMutation, existing))
                        throw new InvalidDataException("Local source feed equivocated at the same HLC and author.");
                    if (existing.ChangeId.IsZero && existing.OriginAuthorEpoch == 0 &&
                        existing.OriginSequence == 0)
                    {
                        // Upgrade a legacy current row with the first durable source
                        // identity; the new feed entry below makes that identity replayable.
                        batch.Put(currentKey, owned);
                    }
                    else
                    {
                        if (existing.OriginAuthorEpoch != ownedMutation.OriginAuthorEpoch ||
                            (!mutation.ChangeId.IsZero && existing.ChangeId != mutation.ChangeId))
                            throw new InvalidDataException("Local source feed identity conflicts at the same HLC and author.");
                        return;
                    }
                }
                else if (comparison > 0)
                {
                    batch.Put(currentKey, owned);
                }
            }
            else
            {
                batch.Put(currentKey, owned);
            }

            batch.Put(ChangeKey(sequence), EncodeLocalFeedChange(owned, changeId));
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
            using StorageSnapshot.PrefixCursor cursor =
                snapshot.OpenPrefixCursor([LocalChange], ChangeKey(start));
            var changes = new List<ProviderChange>(Math.Min(limit, 64));
            cursor.VisitPage(limit, VisitChange);

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

            bool VisitChange(in StorageEntryView row)
            {
                ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(row.Key.Slice(1, 8));
                var decoded = DecodeLocalFeedChange(row.Value);
                changes.Add(new ProviderChange(sequence, decoded.Mutation)
                {
                    ChangeId = decoded.ChangeId
                });
                return true;
            }
        }
    }

    public void AcknowledgeLocalChanges(ulong safeThroughSequence)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            using (StorageSnapshot validation = _storage.OpenSnapshot())
            {
                ulong high = ReadUInt64(validation, [LocalHighWatermark]);
                ulong acked = ReadUInt64(validation, [LocalAckWatermark]);
                ulong highestRead = ReadUInt64(validation, [LocalReadWatermark]);
                if (safeThroughSequence > high || safeThroughSequence > highestRead)
                    throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
                if (safeThroughSequence <= acked) return;
            }

            // Delete and advance the ACK watermark in the same bounded transaction.
            // A crash can therefore resume from the last committed page without either
            // resurrecting deleted rows or skipping an uncommitted prefix.
            const int deletePageSize = 4096;
            while (true)
            {
                using var snapshot = _storage.OpenSnapshot();
                ulong acked = ReadUInt64(snapshot, [LocalAckWatermark]);
                if (safeThroughSequence <= acked)
                    return;

                using StorageSnapshot.PrefixCursor cursor =
                    snapshot.OpenPrefixCursor([LocalChange], ChangeKey(acked));
                using var batch = _storage.BeginWriteBatch();
                ulong lastDeleted = acked;
                bool foundBeyondSafeWatermark = false;
                int visited = cursor.VisitPage(deletePageSize, VisitDelete);

                ulong nextAck = foundBeyondSafeWatermark || visited < deletePageSize
                    ? safeThroughSequence
                    : lastDeleted;
                batch.Put([LocalAckWatermark], EncodeUInt64(nextAck));

                ulong actualBaseCommitSeq = _storage.LastCommitSeq;
                if (actualBaseCommitSeq != snapshot.CommitSeq)
                    throw new StorageConflictException(snapshot.CommitSeq, actualBaseCommitSeq);
                batch.Commit();

                if (nextAck >= safeThroughSequence)
                    return;

                bool VisitDelete(in StorageEntryView row)
                {
                    ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(row.Key.Slice(1, 8));
                    if (sequence > safeThroughSequence)
                    {
                        foundBeyondSafeWatermark = true;
                        return false;
                    }

                    batch.Delete(row.Key);
                    lastDeleted = sequence;
                    return true;
                }
            }
        }
    }

    public ProviderSnapshotHandle OpenSnapshot()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            StorageSnapshot snapshot = _storage.OpenSnapshot();
            try
            {
                ulong watermark = ReadUInt64(snapshot, [LocalHighWatermark]);
                Guid activeGeneration = ReadActiveGeneration(snapshot);
                Guid token = Guid.NewGuid();
                _snapshots.Add(token, new ProviderSnapshotState(
                    snapshot, watermark, activeGeneration,
                    CurrentPrefix(CurrentField, activeGeneration),
                    CurrentPrefix(CurrentRecordTombstone, activeGeneration)));
                return new ProviderSnapshotHandle(token, watermark);
            }
            catch
            {
                snapshot.Dispose();
                throw;
            }
        }
    }

    public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_snapshots.TryGetValue(snapshotToken, out ProviderSnapshotState? state))
                throw new ObjectDisposedException(nameof(snapshotToken));
            if (afterIndex < 0) throw new ArgumentOutOfRangeException(nameof(afterIndex));
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));

            // The API accepts an absolute offset. The common sender path is sequential
            // and O(page); a random/backward seek resets and advances a bounded cursor.
            if (afterIndex < state.NextIndex)
                state.Reset();
            while (state.NextIndex < afterIndex)
            {
                if (!state.TryReadNext(out _))
                    throw new ArgumentOutOfRangeException(nameof(afterIndex));
            }

            var entries = new List<ProviderMutation>(limit);
            while (entries.Count < limit && state.TryReadNext(out ProviderMutation? mutation))
                entries.Add(mutation!);
            bool complete = !state.HasNext();
            return new ProviderSnapshotPage(entries, state.NextIndex, complete);
        }
    }

    public void ReleaseSnapshot(Guid snapshotToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_snapshots.Remove(snapshotToken, out ProviderSnapshotState? state))
                state.Dispose();
        }
    }

    /// <summary>
    /// Stamp current-state rows with the immutable Origin assigned by Metadata Store
    /// freeze. The feed rows remain provisional and are deleted only by the later ACK;
    /// retries therefore recover the same published sequence from the ChangeId ledger.
    /// </summary>
    public void CommitPublishedOrigins(IReadOnlyList<ProviderPublishedOrigin> published)
    {
        ArgumentNullException.ThrowIfNull(published);
        if (published.Count == 0) return;
        lock (_gate)
        {
            ThrowIfDisposed();
            Guid activeGeneration = ReadActiveGeneration(_storage);
            using var batch = _storage.BeginWriteBatch();
            int updated = 0;
            foreach (ProviderPublishedOrigin entry in published)
            {
                ArgumentNullException.ThrowIfNull(entry);
                ProviderChange change = entry.Change
                    ?? throw new InvalidDataException("Published-origin entry has no provider feed change.");
                OperationOrigin origin = entry.Origin;
                ProviderMutation draft = change.Mutation
                    ?? throw new InvalidDataException("Published-origin feed mutation is missing.");
                if (change.ChangeId.IsZero || draft.ChangeId != change.ChangeId ||
                    draft.AuthorNodeId.IsZero || draft.OriginAuthorEpoch == 0 ||
                    draft.OriginSequence != 0 || !origin.IsValid ||
                    origin.AuthorNodeId != draft.AuthorNodeId ||
                    origin.AuthorEpoch != draft.OriginAuthorEpoch)
                    throw new InvalidDataException("Published Metadata Store origin does not match the provisional provider feed entry.");

                if (!batch.TryGet(ChangeKey(change.Sequence), out byte[]? feedBytes))
                    throw new InvalidDataException("Provider local feed entry disappeared before its publication identity was committed.");
                var (feedMutation, feedChangeId) = DecodeLocalFeedChange(feedBytes!);
                if (feedChangeId != change.ChangeId || !SameMutation(feedMutation, draft))
                    throw new InvalidDataException("Provider local feed row changed before its publication identity was committed.");

                byte[] currentKey = draft.IsRecordTombstone
                    ? CurrentRecordTombstoneKey(activeGeneration, draft.RecordId)
                    : CurrentKey(activeGeneration, draft.RecordId, draft.FieldId);
                if (!batch.TryGet(currentKey, out byte[]? currentBytes))
                    continue; // A newer state or authoritative snapshot superseded this operation.
                ProviderMutation current = DecodeMutation(currentBytes!);
                if (current.ChangeId != change.ChangeId)
                    continue; // Do not stamp over a newer local/remote winner.
                if (!SameMutationContent(current, draft) ||
                    current.OriginAuthorEpoch != draft.OriginAuthorEpoch)
                    throw new InvalidDataException("Current provider row conflicts with the frozen ChangeId content.");
                if (current.OriginSequence == origin.Sequence)
                    continue; // Idempotent retry after a successful stamp.
                if (current.OriginSequence != 0)
                    throw new InvalidDataException("Current provider row already carries a different immutable OriginSequence.");

                ProviderMutation stamped = CloneMutation(
                    current, current.ChangeId, current.OriginAuthorEpoch, origin.Sequence);
                ValidateMutation(stamped);
                batch.Put(currentKey, EncodeMutation(stamped));
                updated++;
            }

            if (updated != 0)
                batch.Commit();
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
                ProviderMutationCodec.ValidatePublished(mutation);

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
                        if (SameMutation(mutation, existing))
                            continue;
                        if (!SameMutationContent(mutation, existing))
                            throw new InvalidDataException("Remote provider apply equivocated at the same HLC and author.");
                        if (existing.ChangeId.IsZero && !mutation.ChangeId.IsZero)
                            batch.Put(key, EncodeMutation(mutation));
                        else if (existing.ChangeId == mutation.ChangeId &&
                            existing.OriginAuthorEpoch == mutation.OriginAuthorEpoch &&
                            existing.OriginSequence == 0 && mutation.OriginSequence != 0)
                            batch.Put(key, EncodeMutation(mutation)); // FULL_REPLACE draft -> immutable published operation.
                        else if (existing.ChangeId == mutation.ChangeId &&
                            existing.OriginAuthorEpoch == mutation.OriginAuthorEpoch &&
                            existing.OriginSequence != 0 && mutation.OriginSequence == 0)
                        {
                            // A later state-image replay must not erase the published origin.
                        }
                        else if (!existing.ChangeId.IsZero && mutation.ChangeId.IsZero)
                        {
                            // A legacy materialization may refresh visible fields, but
                            // it must never erase already-persisted origin identity.
                        }
                        else
                            throw new InvalidDataException("Provider mutation identity conflicts at the same HLC and author.");
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
        => ProviderFullReplaceDigest.Compute(generation, sourceChangeFeedWatermark, mutations);

    public bool TryGetFullReplaceProgress(Guid generation, out ProviderFullReplaceProgress? progress)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (generation == Guid.Empty)
                throw new ArgumentException("Full-replace generation must be non-empty.", nameof(generation));
            if (!_storage.TryGet(FullReplaceSessionKey(generation), out var bytes))
            {
                progress = null;
                return false;
            }
            FullReplaceState state = DecodeFullReplaceState(bytes!);
            progress = new ProviderFullReplaceProgress(
                generation, state.SourceFeedWatermark, state.ExpectedCount,
                state.NextIndex, state.ExpectedDigest.ToArray(), state.IsCommitted);
            return true;
        }
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
                ArgumentNullException.ThrowIfNull(mutation);
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
            ProviderFullReplaceDigest.AppendHeader(hash, generation, state.SourceFeedWatermark);
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
            ProviderFullReplaceDigest.AppendEntryCount(hash, stagedCount);
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

    /// <summary>
    /// Tombstone cleanup implementation. v3.1 deliberately returns zero until the signed checkpoint proves
    /// every Active member installed the exact active generation and the global
    /// causal horizon is closed. The caller must hold a cross-store lease that
    /// excludes PendingApply, relay/outbound reservations and external reader pins.
    /// </summary>
    public int CompactRecordTombstones(
        string scopeId, NodeId localReplica, int namespaceId,
        StorageMetadataStore metadataStore, StorageNamespaceGcStore gcStore, TrustStore trustStore,
        int limit, Func<IDisposable?> acquireGcLease,
        IApplicationGcReferenceScanner externalReferences)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
        ArgumentNullException.ThrowIfNull(metadataStore);
        ArgumentNullException.ThrowIfNull(gcStore);
        ArgumentNullException.ThrowIfNull(trustStore);
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        ArgumentNullException.ThrowIfNull(acquireGcLease);
        ArgumentNullException.ThrowIfNull(externalReferences);
        if (!SharesStorageWith(metadataStore) || !gcStore.IsBoundTo(_storage))
            throw new ArgumentException("Provider, Metadata Store and namespace-GC store must share one StorageDatabase.");
        if (!gcStore.IsTombstoneGcProofEnabled)
            return 0;

        IDisposable? lease = acquireGcLease();
        if (lease is null)
            throw new InvalidOperationException("Cross-store tombstone GC proof is unavailable; no tombstones were reclaimed.");
        using (lease)
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_snapshots.Count != 0)
                throw new InvalidOperationException("Provider snapshot readers are active; tombstone GC is blocked.");

            if (!gcStore.TryReadNamespaceRoster(namespaceId, trustStore, out NamespaceRosterCertificate? roster) ||
                roster is null)
                throw new InvalidOperationException("No accepted namespace roster is available for tombstone GC.");
            if (!gcStore.TryReadRetentionCheckpoint(roster, trustStore, out NamespaceRetentionCheckpoint? checkpoint) ||
                checkpoint is null || !checkpoint.FullReplaceGeneration.HasValue ||
                checkpoint.FullReplaceDigest is null ||
                checkpoint.FullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize)
                return 0;
            TombstoneGcProofSnapshot? tombstoneProof =
                gcStore.TryCreateTombstoneGcProofSnapshot(
                    roster, trustStore, checkpoint.FullReplaceGeneration.Value,
                    checkpoint.FullReplaceDigest);
            if (tombstoneProof is null)
                return 0;

            if (!metadataStore.TryBuildLiveRecordReferenceSet(
                    scopeId, localReplica, namespaceId, out HashSet<RecordId> liveRecordReferences))
                return 0; // Unknown payloads or pending reservations block the whole pass.
            using IApplicationGcReferenceScan externalScan =
                externalReferences.OpenScan(scopeId, localReplica, namespaceId);

            ulong examined = 0;
            int reclaimed = 0;
            byte[]? afterTombstoneKey = null;
            while (reclaimed < limit && examined < (ulong)limit * 8)
            {
                using StorageSnapshot snapshot = _storage.OpenSnapshot();
                Guid activeGeneration = ReadActiveGeneration(snapshot);
                if (activeGeneration != checkpoint.FullReplaceGeneration.Value)
                    return reclaimed;
                ulong high = ReadUInt64(snapshot, [LocalHighWatermark]);
                ulong acked = ReadUInt64(snapshot, [LocalAckWatermark]);
                if (high != acked)
                    throw new InvalidOperationException("Unpublished local provider changes block tombstone GC.");

                using (StorageSnapshot.PrefixCursor sessionCursor =
                    snapshot.OpenPrefixCursor([FullReplaceSession]))
                {
                    while (sessionCursor.VisitPage(256, ValidateCommittedFullReplaceSession) == 256)
                    {
                    }
                }

                byte[] tombstonePrefix = CurrentPrefix(CurrentRecordTombstone, activeGeneration);
                IReadOnlyList<StorageEntry> tombstoneRows = snapshot.ScanPrefix(
                    tombstonePrefix,
                    afterTombstoneKey is null ? ReadOnlySpan<byte>.Empty : afterTombstoneKey,
                    1);
                if (tombstoneRows.Count == 0)
                    break;

                StorageEntry tombstoneRow = tombstoneRows[0];
                ProviderMutation tombstone = DecodeMutation(tombstoneRow.Value);
                if (!tombstone.IsRecordTombstone || tombstoneRow.Key.Length != 33 ||
                    tombstone.RecordId != RecordId.From(tombstoneRow.Key.AsSpan(17, 16)) ||
                    tombstone.ChangeId.IsZero || tombstone.OriginAuthorEpoch == 0 ||
                    tombstone.OriginSequence == 0 ||
                    !tombstoneProof.HasProof(
                        tombstone.AuthorNodeId, tombstone.OriginAuthorEpoch,
                        tombstone.OriginSequence, tombstone.HlcWall, tombstone.HlcLogical) ||
                    liveRecordReferences.Contains(tombstone.RecordId) ||
                    externalScan.HasExternalChangeIdReference(tombstone.ChangeId) ||
                    externalScan.HasExternalRecordReference(tombstone.RecordId, tombstone.ChangeId))
                {
                    afterTombstoneKey = tombstoneRow.Key;
                    examined++;
                    continue;
                }

                byte[] fieldPrefix = new byte[33];
                fieldPrefix[0] = CurrentField;
                activeGeneration.TryWriteBytes(fieldPrefix.AsSpan(1, 16), true, out _);
                tombstone.RecordId.WriteTo(fieldPrefix.AsSpan(17, 16));
                byte[] expectedRecordId = new byte[16];
                tombstone.RecordId.WriteTo(expectedRecordId);
                var obsoleteFieldKeys = new List<byte[]>(4096);
                byte[]? afterFieldKey = null;
                bool moreFields = true;
                while (moreFields && obsoleteFieldKeys.Count < 4096)
                {
                    IReadOnlyList<StorageEntry> fieldRows = snapshot.ScanPrefix(
                        fieldPrefix,
                        afterFieldKey is null ? ReadOnlySpan<byte>.Empty : afterFieldKey,
                        4096);
                    if (fieldRows.Count == 0)
                        break;
                    foreach (StorageEntry fieldRow in fieldRows)
                    {
                        if (fieldRow.Key.Length != 35 || fieldRow.Key[0] != CurrentField ||
                            !fieldRow.Key.AsSpan(17, 16).SequenceEqual(expectedRecordId))
                            throw new InvalidDataException("Record field key does not match its tombstone record identity.");
                        ProviderMutation field = DecodeMutation(fieldRow.Value);
                        if (field.RecordId != tombstone.RecordId || field.IsRecordTombstone)
                            throw new InvalidDataException("Record field row contains an invalid mutation identity.");
                        if (Compare(field, tombstone) <= 0)
                            obsoleteFieldKeys.Add(fieldRow.Key);
                        afterFieldKey = fieldRow.Key;
                        if (obsoleteFieldKeys.Count == 4096)
                            break;
                    }
                    moreFields = fieldRows.Count == 4096;
                }

                using var batch = _storage.BeginWriteBatch();
                if (_storage.LastCommitSeq != snapshot.CommitSeq)
                    throw new StorageConflictException(snapshot.CommitSeq, _storage.LastCommitSeq);
                if (obsoleteFieldKeys.Count != 0)
                {
                    foreach (byte[] key in obsoleteFieldKeys)
                        batch.Delete(key);
                    batch.Commit();
                    // Keep the tombstone cursor unchanged and rescan the same record
                    // against a fresh snapshot until every hidden field has been removed.
                    continue;
                }

                batch.Delete(tombstoneRow.Key);
                batch.Commit();
                reclaimed++;
                // Keep afterTombstoneKey unchanged: the deleted row is now absent and
                // the next ScanPrefix call naturally returns the next tombstone.
            }
            return reclaimed;
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
            foreach (ProviderSnapshotState snapshot in _snapshots.Values)
                snapshot.Dispose();
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
        => ProviderMutationCodec.Validate(mutation);

    private static int Compare(ProviderMutation left, ProviderMutation right)
        => HlcOps.CompareHlc(
            new Hlc(left.HlcWall, left.HlcLogical), left.AuthorNodeId,
            new Hlc(right.HlcWall, right.HlcLogical), right.AuthorNodeId);

    private static bool SameMutationContent(ProviderMutation left, ProviderMutation right)
        => left.RecordId == right.RecordId
            && left.FieldId == right.FieldId
            && left.IsTombstone == right.IsTombstone
            && left.IsRecordTombstone == right.IsRecordTombstone
            && left.HlcWall == right.HlcWall
            && left.HlcLogical == right.HlcLogical
            && left.AuthorNodeId == right.AuthorNodeId
            && left.KeyEpoch == right.KeyEpoch
            && left.Value.AsSpan().SequenceEqual(right.Value);

    private static bool SameMutation(ProviderMutation left, ProviderMutation right)
        => SameMutationContent(left, right)
            && left.ChangeId == right.ChangeId
            && left.OriginAuthorEpoch == right.OriginAuthorEpoch
            && left.OriginSequence == right.OriginSequence;

    private static byte[] Fingerprint(IReadOnlyList<ProviderMutation> mutations)
    {
        var ordered = mutations.ToArray();
        Array.Sort(ordered, ProviderMutationCodec.CompareCanonical);

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> count = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(count, ordered.Length);
        hash.AppendData(count);

        Span<byte> fixedPart = stackalloc byte[81];
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
            mutation.ChangeId.WriteTo(fixedPart.Slice(53, 16));
            BinaryPrimitives.WriteUInt32LittleEndian(fixedPart.Slice(69), mutation.OriginAuthorEpoch);
            BinaryPrimitives.WriteUInt64LittleEndian(fixedPart.Slice(73), mutation.OriginSequence);
            hash.AppendData(fixedPart);
            hash.AppendData(mutation.Value);
        }

        return hash.GetHashAndReset();
    }



    private static byte[] EncodeLocalFeedChange(byte[] encodedMutation, BatchId changeId)
    {
        if (changeId.IsZero)
            throw new ArgumentException("Local feed ChangeId must be non-zero.", nameof(changeId));
        byte[] result = new byte[checked(encodedMutation.Length + 16)];
        encodedMutation.CopyTo(result, 0);
        changeId.WriteTo(result.AsSpan(encodedMutation.Length));
        return result;
    }

    private static (ProviderMutation Mutation, BatchId ChangeId)
        DecodeLocalFeedChange(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 69)
            throw new InvalidDataException("Stored v3 provider local feed mutation or ChangeId is truncated.");
        uint valueLength = BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(49, 4));
        if (valueLength > int.MaxValue - 81)
            throw new InvalidDataException("Stored provider local feed value length is invalid.");
        int mutationLength = checked(81 + (int)valueLength);
        if (bytes.Length - 16 != mutationLength)
            throw new InvalidDataException("Stored provider local feed entry must contain one canonical mutation and one ChangeId.");
        BatchId changeId = BatchId.From(bytes.Slice(mutationLength, 16));
        if (changeId.IsZero)
            throw new InvalidDataException("Stored provider local feed identity must not be zero.");
        ProviderMutation mutation = DecodeMutation(bytes[..mutationLength]);
        if (!mutation.ChangeId.IsZero && mutation.ChangeId != changeId)
            throw new InvalidDataException("Provider local feed ChangeId does not match its mutation provenance.");
        return (mutation, changeId);
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
        => ProviderMutationCodec.Encode(mutation);

    private static ProviderMutation DecodeMutation(ReadOnlySpan<byte> bytes)
        => ProviderMutationCodec.Decode(bytes);

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
            Value = mutation.Value.ToArray(),
            ChangeId = mutation.ChangeId,
            OriginAuthorEpoch = mutation.OriginAuthorEpoch,
            OriginSequence = mutation.OriginSequence
        };

    private static ProviderMutation CloneMutation(
        ProviderMutation mutation, BatchId changeId, uint originEpoch, ulong originSequence)
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
            Value = mutation.Value.ToArray(),
            ChangeId = changeId,
            OriginAuthorEpoch = originEpoch,
            OriginSequence = originSequence
        };

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
        if (leftTombstone)
            return left.Key.Length == 33 && right.Key.Length == 33 ? 0 :
                throw new InvalidDataException("Provider snapshot contains an invalid record tombstone key.");

        if (left.Key[0] != CurrentField || right.Key[0] != CurrentField ||
            left.Key.Length != 35 || right.Key.Length != 35)
            throw new InvalidDataException("Provider snapshot contains an invalid current-field key.");

        return left.Key.AsSpan(33, 2).SequenceCompareTo(right.Key.AsSpan(33, 2));
    }

    private static int CompareSnapshotOrder(ProviderMutation left, ProviderMutation right)
        => ProviderMutationCodec.CompareCanonical(left, right);

    private static void ValidateGenerationEntry(
        Guid generation, in StorageEntryView entry, ProviderMutation mutation)
    {
        ReadOnlySpan<byte> key = entry.Key;
        Span<byte> generationBytes = stackalloc byte[16];
        generation.TryWriteBytes(generationBytes, true, out _);
        if (key.Length < 17 || !key.Slice(1, 16).SequenceEqual(generationBytes))
            throw new InvalidDataException("Full-replace row belongs to a different generation.");

        Span<byte> recordBytes = stackalloc byte[16];
        mutation.RecordId.WriteTo(recordBytes);
        if (mutation.IsRecordTombstone)
        {
            if (key[0] != CurrentRecordTombstone || key.Length != 33 ||
                !key.Slice(17, 16).SequenceEqual(recordBytes))
                throw new InvalidDataException("Full-replace record tombstone key does not match its value.");
            return;
        }

        if (key[0] != CurrentField || key.Length != 35 ||
            !key.Slice(17, 16).SequenceEqual(recordBytes) ||
            BinaryPrimitives.ReadUInt16BigEndian(key.Slice(33, 2)) != mutation.FieldId)
            throw new InvalidDataException("Full-replace field key does not match its value.");
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
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(prefix);
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(pageSize);
            if (page.Count == 0) yield break;
            foreach (var row in page) yield return row;
            if (page.Count < pageSize) yield break;
        }
    }

    private static bool ValidateCommittedFullReplaceSession(in StorageEntryView row)
    {
        FullReplaceState staging = DecodeFullReplaceState(row.Value);
        if (!staging.IsCommitted)
            throw new InvalidOperationException("An uncommitted FULL_REPLACE staging generation blocks tombstone GC.");
        return true;
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
        ProviderMutationCodec.WriteSortKey(mutation, key);
        return key;
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

    private uint ReadMaxClosedAuthorEpoch()
    {
        using StorageSnapshot snapshot = _storage.OpenSnapshot();
        using StorageSnapshot.PrefixCursor cursor = snapshot.OpenPrefixCursor(Prefixed(ClosedAuthorEpoch, ReadOnlySpan<byte>.Empty));
        uint maximum = 0;
        while (true)
        {
            IReadOnlyList<StorageEntry> page = cursor.ReadPage(256);
            foreach (StorageEntry row in page)
            {
                if (row.Key.Length != 5 || row.Value.Length != 0)
                    throw new InvalidDataException("Stored author-epoch closure marker is malformed.");
                maximum = Math.Max(maximum, BinaryPrimitives.ReadUInt32LittleEndian(row.Key.AsSpan(1)));
            }
            if (page.Count < 256) break;
        }
        return maximum;
    }

    private static byte[] ClosedAuthorIdentityKey() => [ClosedAuthorIdentity];

    private static byte[] ActiveAuthorEpochKey() => [ActiveAuthorEpoch];

    private static byte[] RetiredFinalAuthorEpochKey() => [RetiredFinalAuthorEpoch];

    private static byte[] EncodeUInt32(uint value)
    {
        byte[] bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] ClosedAuthorEpochKey(uint authorEpoch)
    {
        byte[] key = new byte[5];
        key[0] = ClosedAuthorEpoch;
        BinaryPrimitives.WriteUInt32LittleEndian(key.AsSpan(1), authorEpoch);
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
