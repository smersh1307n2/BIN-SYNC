// WAL replay for complete local mutations and accepted remote batches.
namespace BinSync;

public sealed partial class Node
{
    private Wal? _wal;

    /// <summary>
    /// Replay the WAL into this node and attach it for subsequent local mutations.
    /// The caller retains ownership and must dispose the WAL separately.
    /// </summary>
    public void AttachWal(Wal wal)
    {
        ArgumentNullException.ThrowIfNull(wal);
        using (EnterAllGates())
        {
            if (_wal is not null && !ReferenceEquals(_wal, wal))
                throw new InvalidOperationException("A WAL is already attached to this node.");
            if (_wal is null)
            {
                // Replay and attachment are one gated startup transition: no local
                // write can slip between recovery and enabling durable mutations.
                wal.Replay(this);
                _wal = wal;
            }
        }
    }

    /// <summary>Replay one durably accepted remote batch without writing it back to the WAL.</summary>
    internal bool ReplayRemoteBatch(NodeId senderPeer, byte[] payload)
    {
        var batch = DecodeBatchPayloadInternal((ReadOnlyMemory<byte>)payload);
        var result = ApplyBatchInternal(batch, senderPeer, persistWal: false);
        return !result.Duplicate;
    }

    /// <summary>Restore one durable local mutation, preserving its original Outbox identity.</summary>
    internal bool ReplayLocalMutation(OutboxEntry entry, bool applyToStorage)
    {
        if (entry.AuthorNodeId != NodeId)
            throw new InvalidDataException("WAL local mutation belongs to a different node identity.");

        lock (_lockStorage) lock (_lockOutbox)
        {
            var seenKey = (NodeId, entry.NamespaceId, entry.ChangeId);
            if (SeenChanges.Contains(seenKey)) return false;

            if (Outbox.TryGetValue(entry.NamespaceId, out var queued) &&
                queued.Any(existing => existing.ChangeId == entry.ChangeId))
                throw new InvalidDataException("WAL local mutation duplicates an Outbox change without matching provenance.");

            long currentSeq = _outboxSeq.TryGetValue((NodeId, entry.NamespaceId), out var seq) ? seq : 0;
            if (entry.Seq <= currentSeq || entry.Seq != currentSeq + 1)
                throw new InvalidDataException("WAL local-mutation sequence is not the next expected value.");

            var decoded = Ops.DecodeOp(entry.Payload).Op;
            Hlc timestamp = decoded switch
            {
                OpFieldWrite x => new Hlc(x.Wall, x.Logical),
                OpFieldTomb x => new Hlc(x.Wall, x.Logical),
                OpRecordTombstone x => new Hlc(x.Wall, x.Logical),
                _ => throw new InvalidDataException("Unsupported WAL local-mutation operation.")
            };

            if (applyToStorage)
            {
                bool hadNamespace = Storage.TryGetValue(entry.NamespaceId, out var store);
                store ??= new Dictionary<RecordId, Record>();
                var rec = store.TryGetValue(entry.RecordId, out var current)
                    ? Clone(current)
                    : new Record(entry.RecordId, (ushort)entry.NamespaceId);
                bool changed = ApplyOpInternal(rec, decoded, NodeId);
                if (changed)
                {
                    if (!hadNamespace) Storage[entry.NamespaceId] = store;
                    store[entry.RecordId] = rec;
                }
            }


            if (!Outbox.TryGetValue(entry.NamespaceId, out var list))
                Outbox[entry.NamespaceId] = list = new List<OutboxEntry>();
            list.Add(new OutboxEntry
            {
                NamespaceId = entry.NamespaceId,
                Seq = entry.Seq,
                ChangeId = entry.ChangeId,
                RecordId = entry.RecordId,
                OpKind = entry.OpKind,
                Payload = entry.Payload.ToArray(),
                AuthorNodeId = entry.AuthorNodeId,
                CreatedAt = entry.CreatedAt,
                CausalDeps = new List<BatchId>(entry.CausalDeps),
            });
            _outboxSeq[(NodeId, entry.NamespaceId)] = entry.Seq;
            SeenChanges.Add(seenKey);
            Clock.Observe(timestamp);
            if (entry.CreatedAt > 0) Clock.Observe(new Hlc((ulong)entry.CreatedAt, 0));
            return true;
        }
    }
}
