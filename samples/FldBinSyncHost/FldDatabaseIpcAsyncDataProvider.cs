using System.Buffers.Binary;
using BinSync;
using Fld1.Engine.Db;
using Fld1.Engine.Sync;
using Fld1.Engine.Storage;

namespace BinSync.FldHost;

/// <summary>
/// Process-isolated IDataProvider implementation over the FLD Database IPC facade.
/// It advertises IAsyncDataProvider and immutable published-origin stamping only;
/// async commit guards, publication cutover, author-epoch closure, FULL_REPLACE and GC
/// capabilities are not emulated.
/// </summary>
public sealed class FldDatabaseIpcAsyncDataProvider : IAsyncDataProvider, IAsyncProviderPublishedOriginStore
{
    private readonly FldDatabaseIpcClient _client;
    private readonly FldDatabaseIpcOutboxLease _outboxLease;
    private readonly NodeId _localAuthor;
    private readonly Func<uint> _activeAuthorEpoch;
    private readonly Func<ReadOnlyMemory<byte>, RecordId> _recordIdResolver;
    private readonly Func<ReadOnlyMemory<byte>, ushort> _fieldIdResolver;
    private readonly SemaphoreSlim _feedGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<Guid, FldDatabaseIpcSnapshotHandle> _snapshots = new();
    private readonly HashSet<ulong> _snapshotBarriers = new();
    private ulong _highestReadSequence;
    private bool _disposed;

    private FldDatabaseIpcAsyncDataProvider(
        FldDatabaseIpcClient client,
        FldDatabaseIpcOutboxLease outboxLease,
        NodeId localAuthor,
        Func<uint> activeAuthorEpoch,
        Func<ReadOnlyMemory<byte>, RecordId>? recordIdResolver,
        Func<ReadOnlyMemory<byte>, ushort>? fieldIdResolver)
    {
        _client = client;
        _outboxLease = outboxLease;
        _localAuthor = localAuthor;
        _activeAuthorEpoch = activeAuthorEpoch;
        _recordIdResolver = recordIdResolver ?? ResolveDirectRecordId;
        _fieldIdResolver = fieldIdResolver ?? ResolveDirectFieldId;
    }

    public static async Task<FldDatabaseIpcAsyncDataProvider> ConnectAsync(
        string rootDirectory,
        Guid clientId,
        TimeSpan timeout,
        NodeId localAuthor,
        Func<uint> activeAuthorEpoch,
        Func<ReadOnlyMemory<byte>, RecordId>? recordIdResolver = null,
        Func<ReadOnlyMemory<byte>, ushort>? fieldIdResolver = null,
        CancellationToken cancellationToken = default)
    {
        if (localAuthor.IsZero) throw new ArgumentException("FLD local author must be nonzero.", nameof(localAuthor));
        ArgumentNullException.ThrowIfNull(activeAuthorEpoch);
        FldDatabaseIpcClient client = await FldDatabaseIpcClient.ConnectToDatabaseAsync(
            rootDirectory, clientId, timeout, cancellationToken).ConfigureAwait(false);
        try
        {
            NodeId hostAuthor = NodeId.From(await client.GetLocalAuthorAsync(cancellationToken).ConfigureAwait(false));
            if (hostAuthor != localAuthor)
                throw new InvalidOperationException("Configured BIN-SYNC local author does not match the FLD Database Host LocalAuthor.");

            FldDatabaseIpcOutboxLease lease = await client.AcquireOutboxLeaseAsync(cancellationToken)
                .ConfigureAwait(false);
            return new FldDatabaseIpcAsyncDataProvider(
                client, lease, localAuthor, activeAuthorEpoch, recordIdResolver, fieldIdResolver);
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public ValueTask<RecordId> ResolveRecordIdAsync(
        ReadOnlyMemory<byte> sourceItemKey, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        RecordId id = _recordIdResolver(sourceItemKey);
        if (id.IsZero) throw new ArgumentException("Resolved FLD RecordId must be nonzero.", nameof(sourceItemKey));
        return ValueTask.FromResult(id);
    }

    public ValueTask<ushort> ResolveFieldIdAsync(
        ReadOnlyMemory<byte> sourceFieldKey, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        ushort id = _fieldIdResolver(sourceFieldKey);
        if (id == FldLifecycle.FieldId)
            throw new ArgumentException("The FLD lifecycle field is reserved.", nameof(sourceFieldKey));
        return ValueTask.FromResult(id);
    }

    public async ValueTask AppendLocalChangeAsync(
        ProviderMutation mutation, CancellationToken cancellationToken = default)
    {
        _ = await AppendLocalChangeWithReceiptAsync(mutation, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<ulong> AppendLocalChangeWithReceiptAsync(
        ProviderMutation mutation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        ThrowIfUnavailable(cancellationToken);
        await _feedGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable(cancellationToken);
            ProviderMutationCodec.Validate(mutation);
            uint activeEpoch = _activeAuthorEpoch();
            if (activeEpoch == 0 || mutation.AuthorNodeId != _localAuthor ||
                mutation.ChangeId.IsZero || mutation.OriginAuthorEpoch != activeEpoch ||
                mutation.OriginSequence != 0)
                throw new InvalidDataException(
                    "FLD local drafts require the configured local author, active author epoch, stable ChangeId and deferred OriginSequence.");

            byte[] recordId = mutation.RecordId.ToArray();
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
                    throw new InvalidDataException("The lifecycle field can only be appended as a record tombstone.");
                value = mutation.IsTombstone ? Array.Empty<byte>() : mutation.Value.ToArray();
                isTombstone = mutation.IsTombstone;
            }

            var local = new FldLocalProviderMutation(
                recordId, mutation.FieldId, value, mutation.ChangeId.ToArray(),
                activeEpoch, mutation.HlcWall, mutation.HlcLogical,
                mutation.KeyEpoch, isTombstone);
            Guid operationId = new(mutation.ChangeId.ToArray(), bigEndian: true);
            FldDatabaseIpcAutoSequenceWriteResult result =
                await _client.AppendLocalProviderBatchAutoSequenceAsync(
                    operationId, [local], cancellationToken).ConfigureAwait(false);
            if (result.ClientSequence == 0)
                throw new InvalidDataException("FLD auto-sequence append returned a zero receipt sequence.");
            return result.ClientSequence;
        }
        finally
        {
            _feedGate.Release();
        }
    }

    public async ValueTask AcknowledgeOperationReceiptsAsync(
        ulong throughSequence, CancellationToken cancellationToken = default)
    {
        if (throughSequence == 0)
            throw new ArgumentOutOfRangeException(nameof(throughSequence));
        ThrowIfUnavailable(cancellationToken);
        await _feedGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable(cancellationToken);
            bool acknowledged = await _client.AcknowledgeThroughAsync(
                throughSequence, cancellationToken).ConfigureAwait(false);
            if (!acknowledged)
                throw new InvalidOperationException("FLD did not acknowledge the requested IPC receipt prefix.");
        }
        finally
        {
            _feedGate.Release();
        }
    }

    public async ValueTask<ProviderChangePage> ReadLocalChangesAsync(
        ulong afterSequence, int limit, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        if (limit is < 1 or > 4096) throw new ArgumentOutOfRangeException(nameof(limit));
        await _feedGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable(cancellationToken);
            ulong highWatermark = await _client.GetOutboxHighWatermarkAsync(cancellationToken)
                .ConfigureAwait(false);
            if (afterSequence > highWatermark)
                throw new ArgumentOutOfRangeException(nameof(afterSequence));
            uint activeEpoch = _activeAuthorEpoch();
            if (activeEpoch == 0)
                throw new InvalidOperationException("The FLD provider has no active BIN-SYNC author epoch.");

            var changes = new List<ProviderChange>(Math.Min(limit, 64));
            ulong cursor = afterSequence;
            int scanned = 0;
            bool reachedEnd = false;
            while (scanned < limit)
            {
                FldDatabaseIpcOutboxEntryChunk? first = await _client.ReadOutboxEntryChunkAsync(
                    _outboxLease, cursor, maxChunkBytes: (uint)Math.Min(1024 * 1024, FldDatabaseIpcProtocol.MaxOutboxChunkBytes),
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                if (first is null || first.OutboxSeq > highWatermark)
                {
                    reachedEnd = true;
                    break;
                }
                if (first.OutboxSeq <= cursor)
                    throw new InvalidDataException("FLD Outbox cursor did not advance.");

                byte[] value = await ReadCompleteOutboxValueAsync(first, cursor, cancellationToken)
                    .ConfigureAwait(false);
                cursor = first.OutboxSeq;
                scanned++;

                ProviderMutation? mutation = ToLocalMutation(first, value, activeEpoch);
                if (mutation is not null)
                    changes.Add(new ProviderChange(first.OutboxSeq, mutation)
                    {
                        ChangeId = BatchId.From(first.ChangeId)
                    });
            }

            ulong scannedThrough = reachedEnd ? highWatermark : cursor;
            lock (_gate)
            {
                ThrowIfDisposed();
                if (scannedThrough > _highestReadSequence)
                    _highestReadSequence = scannedThrough;
                _snapshotBarriers.RemoveWhere(barrier => barrier <= _highestReadSequence);
            }
            return new ProviderChangePage(changes, scannedThrough);
        }
        finally
        {
            _feedGate.Release();
        }
    }

    public async ValueTask AcknowledgeLocalChangesAsync(
        ulong safeThroughSequence, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        await _feedGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable(cancellationToken);
            ulong highWatermark = await _client.GetOutboxHighWatermarkAsync(cancellationToken)
                .ConfigureAwait(false);
            if (safeThroughSequence > highWatermark)
                throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));
            lock (_gate)
            {
                ThrowIfDisposed();
                if (safeThroughSequence > _highestReadSequence &&
                    !_snapshotBarriers.Contains(safeThroughSequence))
                    throw new InvalidDataException(
                        "Provider ACK exceeds the scanned feed prefix and is not covered by a source snapshot.");
            }
            await _client.AcknowledgeOutboxThroughAsync(
                _outboxLease, safeThroughSequence, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _feedGate.Release();
        }
    }

    public ValueTask<ulong> GetLocalChangeHighWatermarkAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        return new ValueTask<ulong>(_client.GetOutboxHighWatermarkAsync(cancellationToken));
    }

    public async ValueTask<ProviderSnapshotHandle> OpenSnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        FldDatabaseIpcSnapshotHandle handle = await _client.OpenSnapshotAsync(
            Guid.NewGuid(), cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            ThrowIfDisposed();
            _snapshots.Add(handle.SnapshotToken, handle);
            _snapshotBarriers.Add(handle.SourceFeedWatermark);
        }
        return new ProviderSnapshotHandle(handle.SnapshotToken, handle.SourceFeedWatermark);
    }

    public async ValueTask<ProviderSnapshotPage> ReadSnapshotAsync(
        Guid snapshotToken, int afterIndex, int limit, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        FldDatabaseIpcSnapshotHandle handle;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_snapshots.TryGetValue(snapshotToken, out handle!))
                throw new InvalidOperationException("Unknown or released FLD IPC snapshot token.");
        }

        FldProviderSnapshotMetadataPage page = await _client.ReadSnapshotPageAsync(
            handle, afterIndex, limit, cancellationToken).ConfigureAwait(false);
        var entries = new ProviderMutation[page.Entries.Count];
        for (int i = 0; i < entries.Length; i++)
        {
            FldProviderSnapshotEntryMetadata metadata = page.Entries[i];
            byte[] value = metadata.IsRecordTombstone || metadata.IsTombstone
                ? Array.Empty<byte>()
                : await ReadCompleteSnapshotValueAsync(handle, metadata, cancellationToken).ConfigureAwait(false);
            entries[i] = ToProviderMutation(metadata, value);
        }
        return new ProviderSnapshotPage(entries, page.NextIndex, !page.HasMore);
    }

    public async ValueTask ReleaseSnapshotAsync(
        Guid snapshotToken, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        FldDatabaseIpcSnapshotHandle? handle;
        lock (_gate)
        {
            ThrowIfDisposed();
            _snapshots.TryGetValue(snapshotToken, out handle);
        }
        if (handle is null) return;
        await _client.ReleaseSnapshotAsync(handle, cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            if (_snapshots.TryGetValue(snapshotToken, out FldDatabaseIpcSnapshotHandle? current) &&
                current.SnapshotToken == handle.SnapshotToken &&
                current.HostGeneration == handle.HostGeneration)
                _snapshots.Remove(snapshotToken);
        }
    }

    /// <summary>
    /// Context-free APPLY is deliberately disabled. Remote state must arrive through
    /// IAsyncProviderRemoteBatchCommitStore with its complete authenticated batch context.
    /// </summary>
    public ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(
        Guid applyToken, IReadOnlyList<ProviderMutation> mutations,
        CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        throw new NotSupportedException(
            "Use FldDatabaseIpcRemoteBatchApplyStore; context-free APPLY would discard sender, cursor and digest authority.");
    }

    public async ValueTask<ProviderMutation?> ReadCurrentAsync(
        RecordId recordId, ushort fieldId, CancellationToken cancellationToken = default)
    {
        ThrowIfUnavailable(cancellationToken);
        byte[] recordBytes = new byte[16];
        recordId.WriteTo(recordBytes);

        for (int attempt = 0; attempt < 3; attempt++)
        {
            FldDatabaseIpcCurrentMetadata? metadata = await _client.ReadCurrentMetadataAsync(
                recordBytes, fieldId, cancellationToken).ConfigureAwait(false);
            if (metadata is null) return null;

            if (!metadata.IsTombstone && metadata.ValueLength > 0 && IsZero(metadata.ChangeId))
                return await ReadCurrentFromSnapshotAsync(
                    recordId, recordBytes, fieldId, cancellationToken).ConfigureAwait(false);

            byte[] value;
            try
            {
                value = metadata.IsTombstone
                    ? Array.Empty<byte>()
                    : await ReadCompleteCurrentValueAsync(
                        recordBytes, fieldId, metadata, cancellationToken).ConfigureAwait(false);
            }
            catch (CurrentValueChangedException)
            {
                if (attempt < 2) continue;
                break;
            }

            bool recordTombstone = false;
            if (fieldId == FldLifecycle.FieldId)
            {
                if (metadata.IsTombstone || !value.AsSpan().SequenceEqual(FldLifecycle.DeadValue))
                    return null;
                recordTombstone = true;
                value = Array.Empty<byte>();
            }

            BatchId changeId = IsZero(metadata.ChangeId) ? default : BatchId.From(metadata.ChangeId);
            NodeId author = NodeId.From(metadata.AuthorNodeId);
            if (author.IsZero) author = _localAuthor;
            uint originEpoch = metadata.OriginAuthorEpoch;
            if (originEpoch == 0 && !changeId.IsZero && author == _localAuthor)
            {
                originEpoch = _activeAuthorEpoch();
                if (originEpoch == 0)
                    throw new InvalidOperationException("The local FLD draft has a ChangeId but no active author epoch.");
            }

            return new ProviderMutation
            {
                RecordId = recordId,
                FieldId = fieldId,
                IsTombstone = !recordTombstone && metadata.IsTombstone,
                IsRecordTombstone = recordTombstone,
                HlcWall = metadata.HlcWall,
                HlcLogical = metadata.HlcLogical,
                AuthorNodeId = author,
                KeyEpoch = recordTombstone ? (ushort)0 : metadata.KeyEpoch,
                Value = value,
                ChangeId = changeId,
                OriginAuthorEpoch = originEpoch,
                OriginSequence = metadata.OriginSequence
            };
        }

        throw new InvalidDataException("Current FLD field changed repeatedly while its version-fenced value was read.");
    }

    private async ValueTask<ProviderMutation?> ReadCurrentFromSnapshotAsync(
        RecordId recordId,
        ReadOnlyMemory<byte> recordBytes,
        ushort fieldId,
        CancellationToken cancellationToken)
    {
        // Legacy rows may have no ChangeId, so they cannot use the point-read
        // version fence. Fall back to one immutable generation without creating
        // a snapshot ACK barrier: this read covers only one requested field.
        FldDatabaseIpcSnapshotHandle snapshot = await _client.OpenSnapshotAsync(
            Guid.NewGuid(), cancellationToken).ConfigureAwait(false);
        try
        {
            int cursor = 0;
            while (true)
            {
                FldProviderSnapshotMetadataPage page = await _client.ReadSnapshotPageAsync(
                    snapshot, cursor, 256, cancellationToken).ConfigureAwait(false);
                foreach (FldProviderSnapshotEntryMetadata metadata in page.Entries)
                {
                    if (metadata.FieldId != fieldId ||
                        !metadata.RecordId.AsSpan().SequenceEqual(recordBytes.Span))
                        continue;

                    byte[] value = metadata.IsRecordTombstone || metadata.IsTombstone
                        ? Array.Empty<byte>()
                        : await ReadCompleteSnapshotValueAsync(snapshot, metadata, cancellationToken)
                            .ConfigureAwait(false);
                    return ToProviderMutation(metadata, value);
                }

                if (!page.HasMore) return null;
                if (page.NextIndex <= cursor)
                    throw new InvalidDataException("FLD snapshot metadata cursor did not advance.");
                cursor = page.NextIndex;
            }
        }
        finally
        {
            await _client.ReleaseSnapshotAsync(snapshot, CancellationToken.None).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        FldDatabaseIpcSnapshotHandle[] snapshots;
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            snapshots = _snapshots.Values.ToArray();
            _snapshots.Clear();
        }
        List<Exception>? failures = null;
        foreach (FldDatabaseIpcSnapshotHandle snapshot in snapshots)
        {
            try { await _client.ReleaseSnapshotAsync(snapshot).ConfigureAwait(false); }
            catch (Exception error) { (failures ??= new List<Exception>()).Add(error); }
        }
        try { await _client.ReleaseOutboxLeaseAsync(_outboxLease).ConfigureAwait(false); }
        catch (Exception error) { (failures ??= new List<Exception>()).Add(error); }
        try { await _client.DisposeAsync().ConfigureAwait(false); }
        catch (Exception error) { (failures ??= new List<Exception>()).Add(error); }
        finally { _feedGate.Dispose(); }
        if (failures is not null)
            throw new AggregateException("One or more FLD IPC provider resources could not be released cleanly.", failures);
    }

    private async Task<byte[]> ReadCompleteOutboxValueAsync(
        FldDatabaseIpcOutboxEntryChunk first, ulong afterSequence, CancellationToken cancellationToken)
    {
        if (first.ValueLength > int.MaxValue)
            throw new InvalidDataException("FLD Outbox value exceeds the managed array limit.");
        if (first.ValueOffset != 0)
            throw new InvalidDataException("FLD Outbox entry did not start at value offset zero.");

        byte[] value = new byte[checked((int)first.ValueLength)];
        FldDatabaseIpcOutboxEntryChunk current = first;
        uint expectedOffset = 0;
        while (true)
        {
            uint remaining = checked((uint)value.Length - expectedOffset);
            ValidateOutboxContinuation(first, current, expectedOffset, remaining);
            if (!current.IsComplete && current.ValueChunk.Length == 0)
                throw new InvalidDataException("FLD Outbox returned an empty non-final value chunk.");

            current.ValueChunk.CopyTo(value, checked((int)expectedOffset));
            expectedOffset = checked(expectedOffset + (uint)current.ValueChunk.Length);
            if (current.IsComplete)
                return value;

            current = await _client.ReadOutboxEntryChunkAsync(
                _outboxLease, afterSequence, first.OutboxSeq, expectedOffset,
                (uint)Math.Min(1024 * 1024, FldDatabaseIpcProtocol.MaxOutboxChunkBytes),
                cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidDataException("FLD Outbox continuation disappeared during a value read.");
        }
    }

    private static void ValidateOutboxContinuation(
        FldDatabaseIpcOutboxEntryChunk first,
        FldDatabaseIpcOutboxEntryChunk current,
        uint expectedOffset,
        uint remaining)
    {
        bool expectedComplete = (ulong)expectedOffset + (uint)current.ValueChunk.Length == first.ValueLength;
        if (current.OutboxSeq != first.OutboxSeq || current.CommitSeq != first.CommitSeq ||
            current.FieldId != first.FieldId || current.OpKind != first.OpKind ||
            current.Wall != first.Wall || current.Logical != first.Logical ||
            current.KeyEpoch != first.KeyEpoch || current.ValueLength != first.ValueLength ||
            current.ValueOffset != expectedOffset || current.ValueChunk.Length > remaining ||
            current.IsComplete != expectedComplete ||
            !current.ChangeId.AsSpan().SequenceEqual(first.ChangeId) ||
            !current.RecordId.AsSpan().SequenceEqual(first.RecordId) ||
            !current.AuthorNodeId.AsSpan().SequenceEqual(first.AuthorNodeId))
            throw new InvalidDataException("FLD Outbox continuation changed entry identity, offset or bounds.");
    }

    private async Task<byte[]> ReadCompleteSnapshotValueAsync(
        FldDatabaseIpcSnapshotHandle handle, FldProviderSnapshotEntryMetadata metadata,
        CancellationToken cancellationToken)
    {
        if (metadata.ValueLength > int.MaxValue)
            throw new InvalidDataException("Snapshot value exceeds the managed array limit.");
        byte[] value = new byte[checked((int)metadata.ValueLength)];
        uint offset = 0;
        while (offset < metadata.ValueLength)
        {
            FldProviderSnapshotValueChunk chunk = await _client.ReadSnapshotValueChunkAsync(
                handle, metadata.RecordId, metadata.FieldId, offset,
                (uint)FldDatabaseIpcProtocol.MaxSnapshotValueChunkBytes, cancellationToken)
                .ConfigureAwait(false) ?? throw new InvalidDataException("Snapshot value disappeared from its pinned generation.");
            if (chunk.ValueLength != metadata.ValueLength || chunk.ValueOffset != offset ||
                chunk.ValueChunk.Length == 0 || (ulong)offset + (uint)chunk.ValueChunk.Length > metadata.ValueLength)
                throw new InvalidDataException("Snapshot value chunk is inconsistent with its metadata.");
            chunk.ValueChunk.CopyTo(value, checked((int)offset));
            offset = checked(offset + (uint)chunk.ValueChunk.Length);
        }
        return value;
    }

    private async Task<byte[]> ReadCompleteCurrentValueAsync(
        ReadOnlyMemory<byte> recordId, ushort fieldId, FldDatabaseIpcCurrentMetadata metadata,
        CancellationToken cancellationToken)
    {
        if (metadata.ValueLength > int.MaxValue)
            throw new InvalidDataException("Current field value exceeds the managed array limit.");
        if (metadata.ValueLength == 0) return Array.Empty<byte>();
        if (IsZero(metadata.ChangeId))
            throw new InvalidDataException("Current field has no stable ChangeId; a version-fenced value read is unavailable.");
        byte[] value = new byte[checked((int)metadata.ValueLength)];
        uint offset = 0;
        while (offset < metadata.ValueLength)
        {
            FldDatabaseIpcCurrentValueChunk chunk = await _client.ReadCurrentValueChunkAsync(
                recordId, fieldId, metadata.ChangeId, offset,
                (uint)Math.Min(1024 * 1024, FldDatabaseIpcProtocol.MaxCurrentValueChunkBytes),
                cancellationToken).ConfigureAwait(false);
            if (chunk.Status != FldDatabaseIpcCurrentReadStatus.Data)
                throw new CurrentValueChangedException();
            if (chunk.ValueLength != metadata.ValueLength || chunk.ValueOffset != offset ||
                chunk.ValueChunk.Length == 0 || (ulong)offset + (uint)chunk.ValueChunk.Length > metadata.ValueLength)
                throw new InvalidDataException("Current value chunk is inconsistent with its metadata.");
            chunk.ValueChunk.CopyTo(value, checked((int)offset));
            offset = checked(offset + (uint)chunk.ValueChunk.Length);
        }
        return value;
    }

    private ProviderMutation ToProviderMutation(
        FldProviderSnapshotEntryMetadata metadata, byte[] value)
    {
        bool recordTombstone = metadata.IsRecordTombstone;
        BatchId changeId = IsZero(metadata.ChangeId) ? default : BatchId.From(metadata.ChangeId);
        NodeId author = NodeId.From(metadata.AuthorNodeId);
        if (author.IsZero) author = _localAuthor;
        uint originEpoch = metadata.OriginAuthorEpoch;
        if (originEpoch == 0 && !changeId.IsZero && author == _localAuthor)
        {
            originEpoch = _activeAuthorEpoch();
            if (originEpoch == 0)
                throw new InvalidOperationException("The local FLD snapshot draft has a ChangeId but no active author epoch.");
        }
        return new ProviderMutation
        {
            RecordId = RecordId.From(metadata.RecordId),
            FieldId = metadata.FieldId,
            IsTombstone = !recordTombstone && metadata.IsTombstone,
            IsRecordTombstone = recordTombstone,
            HlcWall = metadata.HlcWall,
            HlcLogical = metadata.HlcLogical,
            AuthorNodeId = author,
            KeyEpoch = recordTombstone ? (ushort)0 : metadata.KeyEpoch,
            Value = value,
            ChangeId = changeId,
            OriginAuthorEpoch = originEpoch,
            OriginSequence = metadata.OriginSequence
        };
    }

    private static ProviderMutation? ToLocalMutation(
        FldDatabaseIpcOutboxEntryChunk entry, byte[] value, uint activeEpoch)
    {
        NodeId author = NodeId.From(entry.AuthorNodeId);
        BatchId changeId = BatchId.From(entry.ChangeId);
        if (entry.FieldId == FldLifecycle.FieldId)
        {
            if (entry.OpKind == 1 && value.AsSpan().SequenceEqual(FldLifecycle.AliveValue))
                return null;
            if (entry.OpKind != 1 || !value.AsSpan().SequenceEqual(FldLifecycle.DeadValue))
                throw new InvalidDataException("FLD Outbox contains a non-canonical lifecycle operation.");
            return new ProviderMutation
            {
                RecordId = RecordId.From(entry.RecordId),
                FieldId = FldLifecycle.FieldId,
                IsRecordTombstone = true,
                HlcWall = entry.Wall,
                HlcLogical = entry.Logical,
                AuthorNodeId = author,
                KeyEpoch = 0,
                Value = Array.Empty<byte>(),
                ChangeId = changeId,
                OriginAuthorEpoch = activeEpoch,
                OriginSequence = 0
            };
        }
        if (entry.OpKind is not (1 or 2))
            throw new InvalidDataException("FLD Outbox contains an unknown operation kind.");
        bool tombstone = entry.OpKind == 2;
        return new ProviderMutation
        {
            RecordId = RecordId.From(entry.RecordId),
            FieldId = entry.FieldId,
            IsTombstone = tombstone,
            HlcWall = entry.Wall,
            HlcLogical = entry.Logical,
            AuthorNodeId = author,
            KeyEpoch = entry.KeyEpoch,
            Value = tombstone ? Array.Empty<byte>() : value,
            ChangeId = changeId,
            OriginAuthorEpoch = activeEpoch,
            OriginSequence = 0
        };
    }

    private static RecordId ResolveDirectRecordId(ReadOnlyMemory<byte> key)
    {
        if (key.Length != 16) throw new ArgumentException("FLD RecordId keys must be 16 bytes.", nameof(key));
        return RecordId.From(key.Span);
    }

    private static ushort ResolveDirectFieldId(ReadOnlyMemory<byte> key)
    {
        if (key.Length != sizeof(ushort)) throw new ArgumentException("FLD FieldId keys must be two bytes.", nameof(key));
        return BinaryPrimitives.ReadUInt16LittleEndian(key.Span);
    }

    private sealed class CurrentValueChangedException : IOException
    {
        public CurrentValueChangedException()
            : base("Current FLD field changed while its value was being read.")
        {
        }
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte part in value) aggregate |= part;
        return aggregate == 0;
    }

    private void ThrowIfUnavailable(CancellationToken cancellationToken)
    {
        lock (_gate) ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(FldDatabaseIpcAsyncDataProvider));
    }
}

/// <summary>Context-preserving asynchronous remote APPLY over the Database IPC boundary.</summary>
public sealed class FldDatabaseIpcRemoteBatchApplyStore : IAsyncProviderRemoteBatchCommitStore
{
    private readonly FldDatabaseIpcClient _client;
    private readonly int _namespaceId;

    public FldDatabaseIpcRemoteBatchApplyStore(FldDatabaseIpcClient client, int namespaceId)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        if ((uint)namespaceId > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(namespaceId));
        _namespaceId = namespaceId;
    }

    public async ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(
        ProviderRemoteBatchCommit batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.NamespaceId != _namespaceId)
            throw new ArgumentException("Remote batch namespace does not match the FLD IPC provider.", nameof(batch));

        var writes = new RemoteFieldWrite[batch.Mutations.Count];
        for (int i = 0; i < writes.Length; i++)
            writes[i] = ToRemoteFieldWrite(batch.Mutations[i]);

        var commit = new RemoteBatchCommit(
            batch.SenderPeer.ToArray(), batch.SenderTransportEpoch,
            checked((ushort)batch.NamespaceId), batch.IssuerNodeId.ToArray(),
            batch.IssuerEpoch, batch.BatchId.ToArray(), batch.CursorStart,
            batch.CursorEnd, batch.PreviousBatchDigest, batch.BatchDigest,
            batch.RecordsRoot, writes, Array.Empty<RemoteFieldWrite>(), batch.ProtocolStateDelta);
        RemoteBatchReceipt receipt = await _client.ApplyRemoteBatchAsync(
            batch.ApplyToken, commit, cancellationToken).ConfigureAwait(false);
        ValidateReceipt(batch, receipt, writes.Length);
        return new ProviderApplyReceipt(batch.ApplyToken, batch.BatchDigest.ToArray(), receipt.IsDuplicate);
    }

    private static void ValidateReceipt(
        ProviderRemoteBatchCommit batch, RemoteBatchReceipt receipt, int writeCount)
    {
        if (receipt.CommitSeq == 0 || receipt.CursorStart != batch.CursorStart ||
            receipt.CursorEnd != batch.CursorEnd ||
            !receipt.BatchId.AsSpan().SequenceEqual(batch.BatchId.ToArray()) ||
            !receipt.ProtocolStateDelta.AsSpan().SequenceEqual(batch.ProtocolStateDelta) ||
            receipt.AppliedCount < 0 || receipt.AppliedCount > writeCount)
            throw new InvalidDataException("FLD remote APPLY receipt does not match the committed provider batch.");
    }

    private static RemoteFieldWrite ToRemoteFieldWrite(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        ProviderMutationCodec.ValidatePublished(mutation);
        if (mutation.IsRecordTombstone)
            return new RemoteFieldWrite(
                mutation.RecordId.ToArray(), FldLifecycle.FieldId, FldLifecycle.DeadValue.ToArray(),
                mutation.HlcWall, mutation.HlcLogical, mutation.AuthorNodeId.ToArray(),
                mutation.ChangeId.ToArray(), 0, false, mutation.OriginAuthorEpoch, mutation.OriginSequence);
        return new RemoteFieldWrite(
            mutation.RecordId.ToArray(), mutation.FieldId,
            mutation.IsTombstone ? Array.Empty<byte>() : mutation.Value.ToArray(),
            mutation.HlcWall, mutation.HlcLogical, mutation.AuthorNodeId.ToArray(),
            mutation.ChangeId.ToArray(), mutation.KeyEpoch, mutation.IsTombstone,
            mutation.OriginAuthorEpoch, mutation.OriginSequence);
    }
}
