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
            foreach (var mutation in mutations)
            {
                if (mutation is null || mutation.RecordId.IsZero || mutation.AuthorNodeId.IsZero ||
                    mutation.Value is null)
                    throw new InvalidDataException("Provider snapshot contains an invalid mutation identity.");
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
                if (GlobalChangeLedger.TryGetValue(key, out var prior) && !prior.IsLocalOutbound)
                    GlobalChangeLedger.Remove(key);
            }

            foreach (var mutation in mutations)
                Clock.Observe(new Hlc(mutation.HlcWall, mutation.HlcLogical));

            return restored.Count;
        }
    }
}
