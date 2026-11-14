namespace BinSync;

public sealed partial class Node
{
    private readonly HashSet<int> _metadataStoreOwnedPublicationFences = new();

    /// <summary>
    /// Fence the Node's legacy local-Outbox and Node-owned send APIs for one namespace
    /// after durable Metadata Store publication cutover. The provider cutover marker is
    /// authoritative; StorageSyncCoordinator reinstalls this in-memory fence on restart.
    /// </summary>
    internal void EnableMetadataStoreOwnedPublicationFence(int namespaceId)
    {
        ValidateNamespaceId(namespaceId);
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            _metadataStoreOwnedPublicationFences.Add(namespaceId);

            // The Metadata Store becomes the active ChangeId authority after cutover.
            // The coordinator first migrates each legacy origin-bound digest into the
            // same durable ChangeLedger row, then clears this duplicate Node projection.
            // SeenChanges remains a separate WAL-replay concern and is intentionally untouched.
        }
    }

    internal int DiscardMetadataStoreOwnedChangeLedgerShadow(int namespaceId)
    {
        ValidateNamespaceId(namespaceId);
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            if (!_metadataStoreOwnedPublicationFences.Contains(namespaceId))
                throw new InvalidOperationException("The Node ChangeId ledger can only be discarded after the Metadata Store publication fence is active.");

            var keys = GlobalChangeLedger.Keys
                .Where(key => key.Ns == namespaceId)
                .ToArray();
            foreach (var key in keys)
                GlobalChangeLedger.Remove(key);
            return keys.Length;
        }
    }

    private void EnsureNodeOwnedMutationAllowedInternal(int namespaceId)
    {
        if (_metadataStoreOwnedPublicationFences.Contains(namespaceId))
            throw new InvalidOperationException(
                "Legacy Node-owned local writes, receive validation and Outbox transport are disabled for this namespace after Metadata-Store-owned publication cutover. Use the Data Provider and StorageSyncCoordinator's Metadata-Store-authoritative receive path.");
    }

    /// <summary>
    /// Advance the local HLC to at least a floor recovered from an authoritative,
    /// fully verified provider snapshot. This is a restore operation, not a remote
    /// receive tick, so it does not apply network clock-skew rejection.
    /// </summary>
    public void AdvanceClockToFloor(ulong wall, uint logical)
    {
        var floor = new Hlc(wall, logical);
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            Clock.Observe(floor);
        }
    }

    /// <summary>
    /// Replace one namespace's in-memory record cache with a complete Data Provider
    /// snapshot. This is a rehydration operation: it does not create Outbox entries,
    /// WAL records or local ChangeIds. Pending remote ChangeIds are removed from the
    /// Node-only dedup cache so their durable PendingApply intents can be replayed
    /// after the provider snapshot has been installed.
    /// </summary>
    internal int RestoreProviderSnapshot(
        int namespaceId,
        IReadOnlyList<ProviderMutation> mutations,
        IReadOnlyCollection<BatchId> pendingRemoteChangeIds)
    {
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        ArgumentNullException.ThrowIfNull(mutations);
        ArgumentNullException.ThrowIfNull(pendingRemoteChangeIds);

        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            var restored = new Dictionary<RecordId, Record>();
            var seenFields = new HashSet<(RecordId RecordId, ushort FieldId)>();
            var seenRecordTombstones = new HashSet<RecordId>();
            foreach (var mutation in mutations)
            {
                if (mutation is null || mutation.RecordId.IsZero || mutation.AuthorNodeId.IsZero ||
                    mutation.Value is null)
                    throw new InvalidDataException("Provider snapshot contains an invalid mutation identity.");
                if (mutation.IsRecordTombstone)
                {
                    if (!seenRecordTombstones.Add(mutation.RecordId))
                        throw new InvalidDataException("Provider snapshot contains duplicate record lifecycle rows.");
                }
                else
                {
                    if (mutation.FieldId == Model.LifecycleFieldId ||
                        !seenFields.Add((mutation.RecordId, mutation.FieldId)))
                        throw new InvalidDataException("Provider snapshot contains a reserved or duplicate field row.");
                }
                if (mutation.IsRecordTombstone &&
                    (mutation.IsTombstone || mutation.FieldId != Model.LifecycleFieldId ||
                     mutation.KeyEpoch != 0 || mutation.Value.Length != 0))
                    throw new InvalidDataException("Provider record tombstone has an invalid lifecycle shape.");
                if (!mutation.IsRecordTombstone && mutation.IsTombstone &&
                    (mutation.FieldId == Model.LifecycleFieldId || mutation.Value.Length != 0))
                    throw new InvalidDataException("Provider field tombstone has an invalid lifecycle shape.");

                if (!restored.TryGetValue(mutation.RecordId, out var record))
                    restored.Add(mutation.RecordId,
                        record = new Record(mutation.RecordId, (ushort)namespaceId));

                object operation = mutation.IsRecordTombstone
                    ? new OpRecordTombstone
                    {
                        RecordId = mutation.RecordId,
                        Wall = mutation.HlcWall,
                        Logical = mutation.HlcLogical,
                        AuthorNodeId = mutation.AuthorNodeId
                    }
                    : mutation.IsTombstone
                        ? new OpFieldTomb
                        {
                            FieldId = mutation.FieldId,
                            Wall = mutation.HlcWall,
                            Logical = mutation.HlcLogical,
                            AuthorNodeId = mutation.AuthorNodeId,
                            KeyEpoch = mutation.KeyEpoch
                        }
                        : new OpFieldWrite
                        {
                            FieldId = mutation.FieldId,
                            Wall = mutation.HlcWall,
                            Logical = mutation.HlcLogical,
                            AuthorNodeId = mutation.AuthorNodeId,
                            KeyEpoch = mutation.KeyEpoch,
                            Value = mutation.Value.ToArray()
                        };

                ApplyOpInternal(record, operation, mutation.AuthorNodeId);
            }

            Storage.EnsureCapacity(Storage.Count + (Storage.ContainsKey(namespaceId) ? 0 : 1));
            if (restored.Count == 0)
                Storage.Remove(namespaceId);
            else
                Storage[namespaceId] = restored;

            foreach (var changeId in pendingRemoteChangeIds)
            {
                if (changeId.IsZero) continue;
                var key = (namespaceId, changeId);
                // Local outbound mutations retain their ChangeId in SeenChanges
                // (the WAL replay/provenance set). Remote accepted IDs have no such
                // marker and must be removed so durable PendingApply replay can apply
                // them against the freshly restored provider snapshot.
                if (GlobalChangeLedger.ContainsKey(key) && !SeenChanges.Contains(key))
                    GlobalChangeLedger.Remove(key);
            }

            foreach (var mutation in mutations)
                Clock.Observe(new Hlc(mutation.HlcWall, mutation.HlcLogical));

            return restored.Count;
        }
    }
    /// <summary>Validate provider mutations before they can be frozen into outbound metadata.</summary>
    internal void ValidateProviderMutation(ProviderMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        if (mutation.RecordId.IsZero || mutation.AuthorNodeId.IsZero || mutation.AuthorNodeId != NodeId)
            throw new InvalidDataException("Provider mutation identity does not match the local Node.");
        ArgumentNullException.ThrowIfNull(mutation.Value);

        if (mutation.IsRecordTombstone)
        {
            if (mutation.IsTombstone || mutation.FieldId != Model.LifecycleFieldId ||
                mutation.KeyEpoch != 0 || mutation.Value.Length != 0)
                throw new InvalidDataException("Provider record tombstone is malformed.");
        }
        else if (mutation.IsTombstone)
        {
            if (mutation.FieldId == 0 || mutation.FieldId == Model.LifecycleFieldId ||
                mutation.Value.Length != 0)
                throw new InvalidDataException("Provider field tombstone is malformed.");
        }
        else if (mutation.FieldId == 0 || mutation.FieldId == Model.LifecycleFieldId)
        {
            throw new InvalidDataException("Provider field write is malformed.");
        }

        _ = new Hlc(mutation.HlcWall, mutation.HlcLogical);
    }

    /// <summary>
    /// Apply the already-read local provider feed to the Node's in-memory cache without
    /// rebuilding the namespace from a full snapshot. Metadata Store remains the
    /// outbound ChangeId authority; this cache update creates no Outbox or identity
    /// ledger rows. The caller checkpoints the Node before acknowledging the feed.
    /// </summary>
    internal int ApplyProviderChanges(int namespaceId, IReadOnlyList<ProviderChange> changes)
    {
        ArgumentNullException.ThrowIfNull(changes);
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (changes.Count == 0)
            return 0;

        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            bool hasNamespace = Storage.TryGetValue(namespaceId, out var namespaceStore);
            namespaceStore ??= new Dictionary<RecordId, Record>();

            // Allocate staging maps only when a provider mutation actually changes
            // the Node cache. ACK-retry commonly replays an already-applied feed
            // prefix; cloning every record in that no-op replay was pure allocation.
            Dictionary<RecordId, Record>? working = null;
            HashSet<RecordId>? changedRecords = null;
            ulong previousSequence = 0;
            int applied = 0;

            for (int i = 0; i < changes.Count; i++)
            {
                ProviderChange change = changes[i];
                if (change.Sequence == 0 || change.Sequence <= previousSequence)
                    throw new InvalidDataException("Provider changes must have strictly increasing non-zero sequences.");
                previousSequence = change.Sequence;
                if (change.ChangeId.IsZero)
                    throw new InvalidDataException("Provider changes require stable non-zero ChangeIds.");

                ProviderMutation mutation = change.Mutation;
                ValidateProviderMutation(mutation);

                Record record;
                if (working is not null && working.TryGetValue(mutation.RecordId, out var staged))
                {
                    record = staged;
                }
                else
                {
                    if (namespaceStore.TryGetValue(mutation.RecordId, out var existing))
                    {
                        if (!WouldChangeProviderMutation(existing, mutation))
                            continue;
                        record = Clone(existing);
                    }
                    else
                    {
                        record = new Record(mutation.RecordId, (ushort)namespaceId);
                    }

                    working ??= new Dictionary<RecordId, Record>(Math.Min(changes.Count, 256));
                    working.Add(mutation.RecordId, record);
                }

                object operation;
                if (mutation.IsRecordTombstone)
                {
                    operation = new OpRecordTombstone
                    {
                        RecordId = mutation.RecordId,
                        Wall = mutation.HlcWall,
                        Logical = mutation.HlcLogical,
                        Origin = 1
                    };
                }
                else if (mutation.IsTombstone)
                {
                    operation = new OpFieldTomb
                    {
                        FieldId = mutation.FieldId,
                        KeyEpoch = mutation.KeyEpoch,
                        Wall = mutation.HlcWall,
                        Logical = mutation.HlcLogical,
                        Origin = 1
                    };
                }
                else
                {
                    operation = new OpFieldWrite
                    {
                        FieldId = mutation.FieldId,
                        KeyEpoch = mutation.KeyEpoch,
                        Wall = mutation.HlcWall,
                        Logical = mutation.HlcLogical,
                        Origin = 1,
                        Value = mutation.Value.ToArray()
                    };
                }

                if (ApplyOpInternal(record, operation, NodeId))
                {
                    changedRecords ??= new HashSet<RecordId>(Math.Min(changes.Count, 256));
                    changedRecords.Add(mutation.RecordId);
                    applied++;
                }
            }

            if (changedRecords is { Count: > 0 })
            {
                int newRecordCount = 0;
                foreach (RecordId recordId in changedRecords)
                    if (!namespaceStore.ContainsKey(recordId))
                        newRecordCount++;
                Limits?.EnsureCanApplySnapshot(this, newRecordCount);

                if (!hasNamespace)
                {
                    namespaceStore.EnsureCapacity(changedRecords.Count);
                    Storage.EnsureCapacity(Storage.Count + 1);
                    Storage.Add(namespaceId, namespaceStore);
                }
                else
                {
                    namespaceStore.EnsureCapacity(namespaceStore.Count + newRecordCount);
                }

                foreach (RecordId recordId in changedRecords)
                    namespaceStore[recordId] = working![recordId];
            }

            foreach (ProviderChange change in changes)
                Clock.Observe(new Hlc(change.Mutation.HlcWall, change.Mutation.HlcLogical));

            return applied;
        }
    }

    private static bool WouldChangeProviderMutation(Record record, ProviderMutation mutation)
    {
        var timestamp = new Hlc(mutation.HlcWall, mutation.HlcLogical);
        if (mutation.IsRecordTombstone)
            return Merge.MergeLifecycle(record.LifecycleHlc, record.LifecycleAuthor,
                record.LifecycleValue, timestamp, mutation.AuthorNodeId, Model.Dead).Changed;

        if (mutation.IsTombstone)
        {
            FieldState? previousTomb = null;
            if (record.HasFieldTombs &&
                record.FieldTombs.TryGetValue(mutation.FieldId, out var existingTomb))
                previousTomb = existingTomb;
            bool hadTomb = previousTomb is not null;
            var incoming = new FieldState(Array.Empty<byte>(), timestamp,
                mutation.AuthorNodeId, mutation.KeyEpoch);
            FieldState winner = incoming;
            bool tombChanged = !hadTomb;
            if (previousTomb is not null)
            {
                var merged = Merge.MergeField(previousTomb, incoming);
                winner = merged.Winner;
                tombChanged = merged.Changed;
            }

            if (record.State.TryGetValue(mutation.FieldId, out var live))
            {
                int order = HlcOps.CompareHlc(winner.Timestamp, winner.AuthorNodeId,
                    live.Timestamp, live.AuthorNodeId);
                return order >= 0 || hadTomb;
            }
            return tombChanged;
        }

        if (record.HasFieldTombs &&
            record.FieldTombs.TryGetValue(mutation.FieldId, out var tomb))
            return HlcOps.CompareHlc(timestamp, mutation.AuthorNodeId,
                tomb.Timestamp, tomb.AuthorNodeId) > 0;

        if (!record.State.TryGetValue(mutation.FieldId, out var current))
            return true;

        if (current.Timestamp.Wall == timestamp.Wall &&
            current.Timestamp.Logical == timestamp.Logical &&
            current.AuthorNodeId == mutation.AuthorNodeId &&
            current.Value.AsSpan().SequenceEqual(mutation.Value))
            return false;

        return HlcOps.CompareHlc(timestamp, mutation.AuthorNodeId,
            current.Timestamp, current.AuthorNodeId) > 0;
    }

    /// <summary>
    /// Drops the Node's duplicate local Outbox payload projection after a durable
    /// Metadata Store cutover. The sequence watermark and ChangeId/WAL replay markers
    /// are deliberately retained; the caller must have frozen the complete prefix
    /// into the authoritative Metadata Store before invoking this method.
    /// </summary>
    internal int DiscardOutboxPayloadsThrough(int namespaceId, NodeId author, long safeThroughSequence)
    {
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        if (author.IsZero)
            throw new ArgumentException("Author identity must be non-zero.", nameof(author));
        if (safeThroughSequence < 0)
            throw new ArgumentOutOfRangeException(nameof(safeThroughSequence));

        lock (_lockStorage) lock (_lockOutbox)
        {
            ThrowIfDisposed();
            long highWatermark = _outboxSeq.TryGetValue((author, namespaceId), out long current)
                ? current
                : 0;
            if (highWatermark != safeThroughSequence)
                throw new InvalidOperationException("Node Outbox changed after the Metadata Store cutover fence.");

            if (!Outbox.TryGetValue(namespaceId, out var entries))
                return 0;
            if (entries.Any(entry => entry.AuthorNodeId == author && entry.Seq > safeThroughSequence))
                throw new InvalidDataException("Node Outbox contains an entry beyond the requested cutover fence.");

            int removed = entries.RemoveAll(entry =>
                entry.AuthorNodeId == author && entry.Seq <= safeThroughSequence);
            if (entries.Count == 0)
                Outbox.Remove(namespaceId);
            return removed;
        }
    }

}
