using System.Buffers.Binary;
using BinSync;
using BinSync.Storage;
using Fld1.Engine.Db;
using Fld1.Engine.Outbox;
using Fld1.Engine.Storage;

namespace BinSync.FldHost;

/// <summary>
/// IDataProvider bridge for an FLD database used as the synchronized user-data store.
/// FLD remains protocol-neutral: this adapter is the only layer that maps FLD's
/// RecordId/FieldId/Outbox coordinates to BIN-SYNC provider contracts.
/// </summary>
public sealed class FldDatabaseDataProvider :
    IDataProvider,
    IProviderPublishedOriginStore,
    IProviderCommitGuard,
    IProviderAuthorEpochClosureStore,
    IProviderPublicationMode,
    IProviderDurableSnapshotStore,
    IProviderFullReplace,
    IProviderSnapshotGcGuard
{
    private readonly object _gate = new();
    private readonly FldDatabase _database;
    private readonly Func<uint> _activeAuthorEpoch;
    private readonly Func<ReadOnlyMemory<byte>, RecordId> _recordIdResolver;
    private readonly Func<ReadOnlyMemory<byte>, ushort> _fieldIdResolver;
    private readonly IProviderPublicationMode? _publicationMode;
    private readonly OutboxLease _outboxLease;
    private readonly Dictionary<Guid, FldProviderSnapshot> _snapshots = new();
    private readonly HashSet<ulong> _snapshotBarriers = new();
    private ulong _highestReadSequence;
    private uint _closedAuthorEpoch;
    private bool _disposed;

    public bool IsBoundTo(FldDatabase database)
    {
        ArgumentNullException.ThrowIfNull(database);
        return ReferenceEquals(_database, database);
    }

    public FldDatabaseDataProvider(
        FldDatabase database,
        Func<uint> activeAuthorEpoch,
        Func<ReadOnlyMemory<byte>, RecordId>? recordIdResolver = null,
        Func<ReadOnlyMemory<byte>, ushort>? fieldIdResolver = null,
        IProviderPublicationMode? publicationMode = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _activeAuthorEpoch = activeAuthorEpoch ?? throw new ArgumentNullException(nameof(activeAuthorEpoch));
        _recordIdResolver = recordIdResolver ?? ResolveDirectRecordId;
        _fieldIdResolver = fieldIdResolver ?? ResolveDirectFieldId;
        _publicationMode = publicationMode;
        Span<byte> owner = stackalloc byte[16];
        Guid.NewGuid().TryWriteBytes(owner);
        _outboxLease = _database.AcquireOutboxLease(owner);
    }

    public RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey)
    {
        EnsureOpen();
        RecordId id = _recordIdResolver(sourceItemKey);
        if (id.IsZero) throw new ArgumentException("Resolved FLD RecordId must be non-zero.", nameof(sourceItemKey));
        return id;
    }

    public ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey)
    {
        EnsureOpen();
        ushort fieldId = _fieldIdResolver(sourceFieldKey);
        if (fieldId == FldLifecycle.FieldId)
            throw new ArgumentException("The FLD lifecycle field is reserved and is not a user field key.", nameof(sourceFieldKey));
        return fieldId;
    }

    /// <summary>
    /// Append a caller-supplied local mutation to FLD's own WAL/Outbox. The caller
    /// must supply the stable ChangeId and the currently active author epoch; the
    /// immutable OriginSequence is deliberately deferred until BIN-SYNC freeze.
    /// </summary>
    public void AppendLocalChange(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        EnsureOpen();
        ProviderMutationCodec.Validate(mutation);
        NodeId localAuthor = NodeId.From(_database.LocalAuthor);
        using IDisposable commitLease = _database.AcquireCommitGuard();
        uint activeEpoch = _activeAuthorEpoch();
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_closedAuthorEpoch == activeEpoch)
                throw new InvalidOperationException("The FLD local author epoch is closed.");
        }
        if (activeEpoch == 0 || mutation.AuthorNodeId != localAuthor ||
            mutation.ChangeId.IsZero || mutation.OriginAuthorEpoch != activeEpoch ||
            mutation.OriginSequence != 0)
            throw new InvalidDataException("FLD local provider drafts require the local author, stable ChangeId, active author epoch and deferred OriginSequence.");

        Span<byte> recordId = stackalloc byte[16];
        mutation.RecordId.WriteTo(recordId);
        byte[] value;
        bool isTombstone;
        if (mutation.IsRecordTombstone)
        {
            value = FldLifecycle.DeadValue.ToArray();
            isTombstone = false;
        }
        else
        {
            if (mutation.FieldId == FldLifecycle.FieldId)
                throw new InvalidDataException("The FLD lifecycle field can only be appended as a record tombstone.");
            value = mutation.IsTombstone ? Array.Empty<byte>() : mutation.Value.ToArray();
            isTombstone = mutation.IsTombstone;
        }

        _database.PutWithIdentity(
            recordId,
            mutation.FieldId,
            value,
            mutation.ChangeId.ToArray(),
            activeEpoch,
            mutation.HlcWall,
            mutation.HlcLogical,
            mutation.KeyEpoch,
            isTombstone);
        _ = _database.Commit();
    }

    public ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit)
    {
        EnsureOpen();
        if (limit <= 0 || limit > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        using IDisposable commitLease = _database.AcquireCommitGuard();
        ulong highWatermark = _database.GetOutboxHighWatermark();
        if (afterSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(afterSequence));
        lock (_gate)
        {
            ThrowIfDisposed();
            if (afterSequence > _highestReadSequence && !_snapshotBarriers.Contains(afterSequence))
                throw new InvalidDataException("Provider feed cursor skips changes that were neither scanned nor covered by an immutable source snapshot.");
        }

        uint activeEpoch = _activeAuthorEpoch();
        if (activeEpoch == 0)
            throw new InvalidOperationException("The FLD provider has no active BIN-SYNC author epoch.");

        var changes = new List<ProviderChange>(Math.Min(limit, 64));
        ulong lastScanned = afterSequence;
        int visited = _database.VisitOutboxBatch(
            afterSequence,
            limit,
            _outboxLease,
            (in OutboxEntryView row) =>
            {
                lastScanned = row.OutboxSeq;
                ProviderMutation? mutation = ToLocalMutation(row, activeEpoch);
                if (mutation is null)
                    return true; // Lifecycle Alive is local state, not a published field operation.

                BatchId changeId = BatchId.From(row.ChangeId);
                changes.Add(new ProviderChange(row.OutboxSeq, mutation)
                {
                    ChangeId = changeId
                });
                return true;
            });

        ulong scannedThrough = visited == limit ? lastScanned : highWatermark;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (scannedThrough > _highestReadSequence)
                _highestReadSequence = scannedThrough;
            _snapshotBarriers.RemoveWhere(barrier => barrier <= _highestReadSequence);
        }
        return new ProviderChangePage(changes, scannedThrough);
    }

    public bool IsBoundToMetadataStore(
        StorageMetadataStore store,
        string scopeId,
        NodeId localReplica,
        int namespaceId)
    {
        ArgumentNullException.ThrowIfNull(store);
        return _publicationMode is StorageMetadataPublicationMode mode &&
            mode.IsBoundTo(store, scopeId, localReplica, namespaceId);
    }

    public bool TryGetMetadataStoreOwnedPublicationCutover(out ulong nodeOutboxHighWatermark)
    {
        EnsureOpen();
        if (_publicationMode is null)
        {
            nodeOutboxHighWatermark = 0;
            return false;
        }
        return _publicationMode.TryGetMetadataStoreOwnedPublicationCutover(out nodeOutboxHighWatermark);
    }

    public void EnableMetadataStoreOwnedPublication(ulong nodeOutboxHighWatermark)
    {
        EnsureOpen();
        if (_publicationMode is null)
            throw new NotSupportedException("FLD provider requires a durable StorageMetadataPublicationMode for Metadata Store cutover.");
        using IDisposable commitLease = _database.AcquireCommitGuard();
        FldDraftHorizon drafts = _database.ReadUnpublishedLocalDraftHorizon();
        if (drafts.Count != 0 ||
            _database.GetOutboxHighWatermark() != _database.GetOutboxWatermark())
            throw new InvalidOperationException("FLD local drafts and Outbox entries must be fully published and acknowledged before Metadata Store cutover.");
        _publicationMode.EnableMetadataStoreOwnedPublication(nodeOutboxHighWatermark);
    }

    public bool TryCloseLocalAuthorEpoch(uint authorEpoch)
    {
        if (authorEpoch == 0) throw new ArgumentOutOfRangeException(nameof(authorEpoch));
        EnsureOpen();
        using IDisposable commitLease = _database.AcquireCommitGuard();
        if (_activeAuthorEpoch() != authorEpoch)
            return false;
        FldDraftHorizon drafts = _database.ReadUnpublishedLocalDraftHorizon();
        if (drafts.Count != 0 ||
            _database.GetOutboxHighWatermark() != _database.GetOutboxWatermark())
            return false;
        lock (_gate)
        {
            ThrowIfDisposed();
            _closedAuthorEpoch = authorEpoch;
        }
        return true;
    }

    public void CloseLocalAuthorIdentity(uint finalAuthorEpoch)
    {
        if (!TryCloseLocalAuthorEpoch(finalAuthorEpoch))
            throw new InvalidOperationException("FLD local author identity cannot close while drafts or unacknowledged Outbox entries remain.");
    }

    public void ActivateLocalAuthorEpoch(uint authorEpoch)
    {
        if (authorEpoch == 0) throw new ArgumentOutOfRangeException(nameof(authorEpoch));
        EnsureOpen();
        using IDisposable commitLease = _database.AcquireCommitGuard();
        if (_activeAuthorEpoch() != authorEpoch)
            throw new InvalidOperationException("The durable host author-epoch source does not expose the requested active epoch.");
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_closedAuthorEpoch != 0 && authorEpoch <= _closedAuthorEpoch)
                throw new InvalidOperationException("A closed FLD author epoch cannot be reactivated.");
            _closedAuthorEpoch = 0;
        }
    }

    public void AcknowledgeLocalChanges(ulong safeThroughSequence)
    {
        EnsureOpen();
        using IDisposable commitLease = _database.AcquireCommitGuard();
        ulong highWatermark = _database.GetOutboxHighWatermark();
        if (safeThroughSequence > highWatermark)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
        lock (_gate)
        {
            ThrowIfDisposed();
            if (safeThroughSequence > _highestReadSequence && !_snapshotBarriers.Contains(safeThroughSequence))
                throw new InvalidDataException("Provider ACK exceeds the scanned feed prefix and is not covered by a completed source snapshot.");
        }
        _database.AcknowledgeOutbox(safeThroughSequence, _outboxLease);
    }

    public ulong LocalChangeHighWatermark
    {
        get
        {
            EnsureOpen();
            return _database.GetOutboxHighWatermark();
        }
    }

    public ProviderSnapshotHandle OpenSnapshot()
    {
        EnsureOpen();
        using IDisposable commitLease = _database.AcquireCommitGuard();
        lock (_gate)
        {
            ThrowIfDisposed();
            FldProviderSnapshot snapshot = _database.OpenProviderSnapshot();
            _snapshots.Add(snapshot.Generation, snapshot);
            _snapshotBarriers.Add(snapshot.SourceFeedWatermark);
            return new ProviderSnapshotHandle(snapshot.Generation, snapshot.SourceFeedWatermark);
        }
    }

    public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit)
    {
        FldProviderSnapshot? snapshot;
        lock (_gate)
        {
            ThrowIfDisposed();
            _snapshots.TryGetValue(snapshotToken, out snapshot);
        }
        snapshot ??= _database.RestoreProviderSnapshot(snapshotToken);

        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_snapshots.TryGetValue(snapshotToken, out FldProviderSnapshot? current))
                _snapshots.Add(snapshotToken, snapshot);
            else
                snapshot = current;

            FldProviderSnapshotPage page = snapshot.ReadPage(afterIndex, limit);
            var entries = new ProviderMutation[page.Entries.Count];
            for (int i = 0; i < entries.Length; i++)
                entries[i] = ToProviderMutation(page.Entries[i]);
            return new ProviderSnapshotPage(entries, page.NextIndex, !page.HasMore);
        }
    }

    public void ReleaseSnapshot(Guid snapshotToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            _snapshots.Remove(snapshotToken);
        }
        _database.ReleaseProviderSnapshot(snapshotToken);
    }

    /// <summary>
    /// The context-free IDataProvider apply API is intentionally disabled. The host
    /// must inject FldRemoteBatchApplyStore as IProviderRemoteBatchCommitStore so the
    /// authenticated sender, transport epoch, cursor and digest chain reach FLD WAL.
    /// </summary>
    public ProviderApplyReceipt ApplyRemoteBatch(
        Guid applyToken,
        IReadOnlyList<ProviderMutation> mutations)
        => throw new NotSupportedException(
            "Use the contextual FldRemoteBatchApplyStore capability; context-free remote APPLY would discard sender/cursor/digest authority.");

    public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation)
    {
        EnsureOpen();
        Span<byte> recordBytes = stackalloc byte[16];
        recordId.WriteTo(recordBytes);
        if (!_database.TryResolve(recordBytes, fieldId, out FieldResolver.Resolved resolved) || !resolved.Found)
        {
            mutation = null;
            return false;
        }

        byte[] value = resolved.Value?.ToArray() ?? Array.Empty<byte>();
        bool recordTombstone = false;
        if (fieldId == FldLifecycle.FieldId)
        {
            if (resolved.IsTombstone || !value.AsSpan().SequenceEqual(FldLifecycle.DeadValue))
            {
                mutation = null;
                return false;
            }
            recordTombstone = true;
            value = Array.Empty<byte>();
        }

        byte[] authorBytes = resolved.AuthorNodeId ?? _database.LocalAuthor.ToArray();
        byte[] changeBytes = resolved.ChangeId is { Length: 16 } ? resolved.ChangeId : Array.Empty<byte>();
        uint originEpoch = resolved.OriginAuthorEpoch;
        if (originEpoch == 0 && changeBytes.Length == 16 &&
            NodeId.From(authorBytes) == NodeId.From(_database.LocalAuthor))
        {
            originEpoch = _activeAuthorEpoch();
            if (originEpoch == 0)
                throw new InvalidOperationException("The local FLD draft has a ChangeId but no active author epoch.");
        }
        mutation = new ProviderMutation
        {
            RecordId = recordId,
            FieldId = fieldId,
            IsTombstone = !recordTombstone && resolved.IsTombstone,
            IsRecordTombstone = recordTombstone,
            HlcWall = resolved.Wall,
            HlcLogical = resolved.Logical,
            AuthorNodeId = NodeId.From(authorBytes),
            KeyEpoch = recordTombstone ? (ushort)0 : resolved.KeyEpoch,
            Value = value,
            ChangeId = changeBytes.Length == 16 ? BatchId.From(changeBytes) : default,
            OriginAuthorEpoch = originEpoch,
            OriginSequence = resolved.OriginSequence
        };
        return true;
    }

    public void CommitPublishedOrigins(IReadOnlyList<ProviderPublishedOrigin> published)
    {
        ArgumentNullException.ThrowIfNull(published);
        EnsureOpen();
        if (published.Count == 0) return;
        var stamps = new FldPublishedOrigin[published.Count];
        for (int i = 0; i < published.Count; i++)
        {
            ProviderPublishedOrigin item = published[i]
                ?? throw new ArgumentException("Published-origin list contains null.", nameof(published));
            ProviderChange change = item.Change
                ?? throw new InvalidDataException("Published-origin entry has no provider change.");
            Span<byte> recordId = stackalloc byte[16];
            change.Mutation.RecordId.WriteTo(recordId);
            stamps[i] = new FldPublishedOrigin
            {
                RecordId = recordId.ToArray(),
                FieldId = change.Mutation.FieldId,
                ChangeId = change.ChangeId.ToArray(),
                OriginAuthorEpoch = item.Origin.AuthorEpoch,
                OriginSequence = item.Origin.Sequence
            };
        }
        using IDisposable commitLease = _database.AcquireCommitGuard();
        _database.CommitPublishedOrigins(stamps);
    }

    public IDisposable AcquireCommitGuard()
    {
        EnsureOpen();
        return _database.AcquireCommitGuard();
    }

    public IDisposable? TryAcquireSnapshotGcGuard()
    {
        EnsureOpen();
        // The caller already holds the composed application commit lease, including
        // the FLD gate. Capture the reference view without recursively acquiring it.
        FldReferenceSnapshot snapshot = _database.OpenReferenceSnapshot();
        if (snapshot.HasSnapshotPin)
        {
            snapshot.Dispose();
            return null;
        }
        return snapshot;
    }

    public bool TryGetFullReplaceProgress(Guid generation, out ProviderFullReplaceProgress? progress)
    {
        EnsureOpen();
        if (!_database.TryGetProviderReplacementProgress(generation, out FldFullReplaceProgress? state) ||
            state is null)
        {
            progress = null;
            return false;
        }
        progress = new ProviderFullReplaceProgress(
            state.Generation, state.SourceFeedWatermark, state.ExpectedEntryCount,
            state.NextIndex, state.ExpectedDigest.ToArray(), state.IsCommitted);
        return true;
    }

    public void BeginFullReplace(
        Guid generation,
        ulong sourceChangeFeedWatermark,
        int expectedEntryCount,
        ReadOnlyMemory<byte> expectedDigest)
    {
        EnsureOpen();
        _database.BeginProviderReplacement(
            generation, sourceChangeFeedWatermark, expectedEntryCount, expectedDigest);
    }

    public void StageFullReplacePage(
        Guid generation,
        int startIndex,
        IReadOnlyList<ProviderMutation> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        EnsureOpen();
        if (entries.Count == 0 || entries.Count > ProviderFullReplaceDigestBuilder.MaxPageEntries)
            throw new ArgumentOutOfRangeException(nameof(entries));
        var staged = new FldProviderSnapshotEntry[entries.Count];
        for (int i = 0; i < entries.Count; i++)
        {
            ProviderMutation mutation = entries[i]
                ?? throw new ArgumentException("FULL_REPLACE page contains a null mutation.", nameof(entries));
            ProviderMutationCodec.Validate(mutation);
            staged[i] = new FldProviderSnapshotEntry(
                ToRecordIdBytes(mutation.RecordId),
                mutation.FieldId,
                mutation.Value.ToArray(),
                mutation.HlcWall,
                mutation.HlcLogical,
                ToNodeIdBytes(mutation.AuthorNodeId),
                mutation.KeyEpoch,
                mutation.IsTombstone,
                mutation.ChangeId.IsZero ? Array.Empty<byte>() : mutation.ChangeId.ToArray(),
                mutation.OriginAuthorEpoch,
                mutation.OriginSequence,
                mutation.IsRecordTombstone);
        }
        _database.StageProviderReplacementPage(generation, startIndex, staged);
    }

    public void CommitFullReplace(Guid generation)
    {
        EnsureOpen();
        if (!_database.TryGetProviderReplacementProgress(generation, out FldFullReplaceProgress? progress) ||
            progress is null)
            throw new InvalidOperationException("FULL_REPLACE generation has not been begun.");
        if (progress.IsCommitted) return;
        if (progress.NextIndex != progress.ExpectedEntryCount)
            throw new InvalidOperationException("FULL_REPLACE snapshot is incomplete.");

        using var digest = new ProviderFullReplaceDigestBuilder(
            generation, progress.SourceFeedWatermark);
        int nextIndex = 0;
        while (nextIndex < progress.ExpectedEntryCount)
        {
            FldProviderSnapshotPage page = _database.ReadProviderReplacementPage(
                generation, nextIndex, ProviderFullReplaceDigestBuilder.MaxPageEntries);
            if (page.Entries.Count == 0 ||
                page.NextIndex != checked(nextIndex + page.Entries.Count) ||
                page.Entries.Count > ProviderFullReplaceDigestBuilder.MaxPageEntries)
                throw new InvalidDataException("FLD FULL_REPLACE staging returned a non-contiguous digest page.");
            var mutations = new ProviderMutation[page.Entries.Count];
            for (int i = 0; i < mutations.Length; i++)
                mutations[i] = FromStagedReplacementEntry(page.Entries[i]);
            digest.AppendPage(mutations);
            nextIndex = page.NextIndex;
            if (page.HasMore != (nextIndex < progress.ExpectedEntryCount))
                throw new InvalidDataException("FLD FULL_REPLACE staging continuation flag is inconsistent.");
        }

        byte[] actualDigest = digest.Complete();
        if (!System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
                actualDigest, progress.ExpectedDigest))
            throw new InvalidDataException("FLD FULL_REPLACE staged image digest does not match the signed manifest.");
        _database.CommitProviderReplacement(generation, actualDigest);
    }

    public bool AbortFullReplace(Guid generation, int limit)
    {
        EnsureOpen();
        return _database.AbortProviderReplacement(generation, limit);
    }

    private static ProviderMutation FromStagedReplacementEntry(FldProviderSnapshotEntry entry)
        => new()
        {
            RecordId = RecordId.From(entry.RecordId),
            FieldId = entry.FieldId,
            IsTombstone = entry.IsTombstone,
            IsRecordTombstone = entry.IsRecordTombstone,
            HlcWall = entry.HlcWall,
            HlcLogical = entry.HlcLogical,
            AuthorNodeId = NodeId.From(entry.AuthorNodeId),
            KeyEpoch = entry.KeyEpoch,
            Value = entry.Value.ToArray(),
            ChangeId = entry.ChangeId.Length == 16 ? BatchId.From(entry.ChangeId) : default,
            OriginAuthorEpoch = entry.OriginAuthorEpoch,
            OriginSequence = entry.OriginSequence
        };

    private static byte[] ToRecordIdBytes(RecordId id)
    {
        byte[] bytes = new byte[16];
        id.WriteTo(bytes);
        return bytes;
    }

    private static byte[] ToNodeIdBytes(NodeId id)
    {
        byte[] bytes = new byte[16];
        id.WriteTo(bytes);
        return bytes;
    }

    public bool HasPersistedSnapshot(Guid snapshotToken)
    {
        EnsureOpen();
        return _database.HasProviderSnapshot(snapshotToken);
    }

    /// <summary>
    /// Reclaims snapshot generations not referenced by recovered FULL_REPLACE sender
    /// progress. The host must pass the complete durable active-token set.
    /// </summary>
    public bool TryGetPersistedSnapshotWatermark(Guid snapshotToken, out ulong changeFeedWatermark)
    {
        EnsureOpen();
        return _database.TryGetProviderSnapshotWatermark(snapshotToken, out changeFeedWatermark);
    }

    public void PruneSnapshotsExcept(IReadOnlySet<Guid> activeGenerations)
    {
        ArgumentNullException.ThrowIfNull(activeGenerations);
        EnsureOpen();
        _database.PruneProviderSnapshotsExcept(activeGenerations);
        lock (_gate)
        {
            ThrowIfDisposed();
            foreach (Guid token in _snapshots.Keys.Where(token => !activeGenerations.Contains(token)).ToArray())
                _snapshots.Remove(token);
        }
    }

    public void Dispose()
    {
        FldProviderSnapshot[] snapshots;
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            snapshots = _snapshots.Values.ToArray();
            _snapshots.Clear();
        }
        foreach (FldProviderSnapshot snapshot in snapshots)
            snapshot.Dispose();
        _database.ReleaseOutboxLease(_outboxLease);
    }

    private static RecordId ResolveDirectRecordId(ReadOnlyMemory<byte> sourceItemKey)
    {
        if (sourceItemKey.Length != 16)
            throw new ArgumentException("Default FLD RecordId keys must be exactly 16 bytes.", nameof(sourceItemKey));
        return RecordId.From(sourceItemKey.Span);
    }

    private static ushort ResolveDirectFieldId(ReadOnlyMemory<byte> sourceFieldKey)
    {
        if (sourceFieldKey.Length != sizeof(ushort))
            throw new ArgumentException("Default FLD FieldId keys must be exactly two little-endian bytes.", nameof(sourceFieldKey));
        return BinaryPrimitives.ReadUInt16LittleEndian(sourceFieldKey.Span);
    }

    private static ProviderMutation? ToLocalMutation(in OutboxEntryView row, uint activeEpoch)
    {
        NodeId author = NodeId.From(row.AuthorNodeId);
        BatchId changeId = BatchId.From(row.ChangeId);
        if (row.FieldId == FldLifecycle.FieldId)
        {
            if (row.OpKind != 1 || !row.Value.SequenceEqual(FldLifecycle.DeadValue))
            {
                if (row.OpKind == 1 && row.Value.SequenceEqual(FldLifecycle.AliveValue))
                    return null;
                throw new InvalidDataException("FLD Outbox contains a non-canonical lifecycle operation.");
            }
            return new ProviderMutation
            {
                RecordId = RecordId.From(row.RecordId),
                FieldId = FldLifecycle.FieldId,
                IsRecordTombstone = true,
                HlcWall = row.Wall,
                HlcLogical = row.Logical,
                AuthorNodeId = author,
                KeyEpoch = 0,
                Value = Array.Empty<byte>(),
                ChangeId = changeId,
                OriginAuthorEpoch = activeEpoch,
                OriginSequence = 0
            };
        }

        bool tombstone = row.OpKind == 2;
        if (row.OpKind is not (1 or 2))
            throw new InvalidDataException("FLD Outbox contains an unknown operation kind.");
        return new ProviderMutation
        {
            RecordId = RecordId.From(row.RecordId),
            FieldId = row.FieldId,
            IsTombstone = tombstone,
            HlcWall = row.Wall,
            HlcLogical = row.Logical,
            AuthorNodeId = author,
            KeyEpoch = row.KeyEpoch,
            Value = tombstone ? Array.Empty<byte>() : row.Value.ToArray(),
            ChangeId = changeId,
            OriginAuthorEpoch = activeEpoch,
            OriginSequence = 0
        };
    }

    private ProviderMutation ToProviderMutation(FldProviderSnapshotEntry entry)
    {
        NodeId author = NodeId.From(entry.AuthorNodeId);
        BatchId changeId = entry.ChangeId.Length == 16 ? BatchId.From(entry.ChangeId) : default;
        uint originEpoch = entry.OriginAuthorEpoch;
        if (originEpoch == 0 && !changeId.IsZero && author == NodeId.From(_database.LocalAuthor))
        {
            originEpoch = _activeAuthorEpoch();
            if (originEpoch == 0)
                throw new InvalidOperationException("The local FLD snapshot draft has a ChangeId but no active author epoch.");
        }
        return new ProviderMutation
        {
            RecordId = RecordId.From(entry.RecordId),
            FieldId = entry.FieldId,
            IsTombstone = entry.IsTombstone,
            IsRecordTombstone = entry.IsRecordTombstone,
            HlcWall = entry.HlcWall,
            HlcLogical = entry.HlcLogical,
            AuthorNodeId = author,
            KeyEpoch = entry.KeyEpoch,
            Value = entry.Value,
            ChangeId = changeId,
            OriginAuthorEpoch = originEpoch,
            OriginSequence = entry.OriginSequence
        };
    }

    private void EnsureOpen()
    {
        lock (_gate) ThrowIfDisposed();
    }

    private void ThrowIfDisposed()
        => ObjectDisposedException.ThrowIf(_disposed, this);
}
