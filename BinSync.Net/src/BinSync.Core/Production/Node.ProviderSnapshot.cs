namespace BinSync;

public sealed partial class Node
{
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
