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
    internal bool ReplayRemoteBatch(NodeId senderPeer, int senderEpoch,
        IReadOnlyList<AliasMapEntry> aliases, byte[] payload)
    {
        var batch = DecodeBatchPayloadCausal(payload);
        var inboxKey = (batch.IssuerNodeId, batch.NamespaceId, batch.IssuerEpoch, batch.BatchId);

        // Recreate the peer placeholder even when the alias/session map is supplied
        // by this WAL frame. Transport.MakeSession uses a null placeholder to recognize
        // a resumed peer and preserve its durable receive cursor and digest chain.
        lock (_lockMeta)
        {
            PeersByNid ??= new Dictionary<NodeId, Node>();
            if (!PeersByNid.ContainsKey(senderPeer))
                PeersByNid[senderPeer] = null!;
        }

        // A WAL tail may still contain batches already included in a just-written
        // snapshot. Do not roll the snapshot's newer session map back to an older one.
        if (!InboxAuthors.ContainsKey(inboxKey))
        {
            if (aliases.Count > 0)
                CommitSession(senderPeer, senderEpoch, aliases);
            else
            {
                lock (_lockMeta)
                {
                    bool hasEpoch = KnownEpochs.TryGetValue((senderPeer, 0), out int previousEpoch);
                    if (hasEpoch && previousEpoch != senderEpoch)
                    {
                        ResetSessionInternal(senderPeer, resetReceiveState: true);
                        RegisterSessionEpochInternal(senderPeer, senderEpoch);
                    }
                    else if (!hasEpoch && senderEpoch > 0)
                        RegisterSessionEpochInternal(senderPeer, senderEpoch);
                }
            }
        }
        var result = ApplyBatchInternal(batch, senderPeer, persistWal: false);
        return !result.Duplicate;
    }

    /// <summary>Restore one durable local mutation, preserving its original Outbox identity.</summary>
    internal void CommitWalReplayFrom(Node staged)
    {
        ArgumentNullException.ThrowIfNull(staged);
        if (staged.NodeId != NodeId)
            throw new InvalidOperationException("Cannot publish WAL replay state from a different node identity.");

        // Allocate the replacement peer registry and all destination capacity before
        // mutating the live node. The following copy phase performs no collection growth.
        Dictionary<NodeId, Node>? restoredPeers = null;
        if (staged.PeersByNid is not null)
        {
            var livePeers = PeersByNid;
            restoredPeers = new Dictionary<NodeId, Node>(staged.PeersByNid.Count);
            foreach (var peerId in staged.PeersByNid.Keys)
            {
                if (livePeers is not null && livePeers.TryGetValue(peerId, out var live) && live is not null)
                    restoredPeers[peerId] = live;
                else
                    restoredPeers[peerId] = null!;
            }
        }

        _storage.EnsureCapacity(staged._storage.Count);
        Outbox.EnsureCapacity(staged.Outbox.Count);
        _outboxSeq.EnsureCapacity(staged._outboxSeq.Count);
        var restoredInboxAuthors = staged.InboxAuthors.Clone();
        var restoredProviderReceipts = new Dictionary<(int Ns, BatchId ChangeId), ProviderOperationReceiptState>(staged.ProviderOperationReceipts);
        var restoredProviderReceiptSequences = new Dictionary<(int Ns, ulong ClientSequence), BatchId>(staged.ProviderOperationReceiptSequences);
        var restoredProviderReceiptHighWatermarks = new Dictionary<int, ulong>(staged.ProviderOperationReceiptHighWatermarks);
        var restoredProviderReceiptAckedThrough = new Dictionary<int, ulong>(staged.ProviderOperationReceiptAckedThrough);
        LastBatchDigest.EnsureCapacity(staged.LastBatchDigest.Count);
        LastStreamDigest.EnsureCapacity(staged.LastStreamDigest.Count);
        AliasToNode.EnsureCapacity(staged.AliasToNode.Count);
        NodeToAlias.EnsureCapacity(staged.NodeToAlias.Count);
        DurableAuthors.EnsureCapacity(staged.DurableAuthors.Count);
        PeerStates.EnsureCapacity(staged.PeerStates.Count);
        SeenChanges.EnsureCapacity(staged.SeenChanges.Count);
        GlobalChangeLedger.EnsureCapacity(staged.GlobalChangeLedger.Count);
        ProviderOperationReceipts.EnsureCapacity(restoredProviderReceipts.Count);
        ProviderOperationReceiptSequences.EnsureCapacity(restoredProviderReceiptSequences.Count);
        ProviderOperationReceiptHighWatermarks.EnsureCapacity(restoredProviderReceiptHighWatermarks.Count);
        ProviderOperationReceiptAckedThrough.EnsureCapacity(restoredProviderReceiptAckedThrough.Count);
        LastStreamAuthor.EnsureCapacity(staged.LastStreamAuthor.Count);
        KnownEpochs.EnsureCapacity(staged.KnownEpochs.Count);
        PeerSessionEpochs.EnsureCapacity(staged.PeerSessionEpochs.Count);
        Quarantined.EnsureCapacity(staged.Quarantined.Count);
        ForbiddenNamespaces.EnsureCapacity(staged.ForbiddenNamespaces.Count);
        SchemaVersions.EnsureCapacity(staged.SchemaVersions.Count);
        ErrorsOut.EnsureCapacity(staged.ErrorsOut.Count);
        AcksOut.EnsureCapacity(staged.AcksOut.Count);

        ReplaceContents(_storage, staged._storage);
        ReplaceContents(Outbox, staged.Outbox);
        ReplaceContents(_outboxSeq, staged._outboxSeq);
        InboxAuthors.ReplaceWith(restoredInboxAuthors);
        ReplaceContents(LastBatchDigest, staged.LastBatchDigest);
        ReplaceContents(LastStreamDigest, staged.LastStreamDigest);
        ReplaceContents(AliasToNode, staged.AliasToNode);
        ReplaceContents(NodeToAlias, staged.NodeToAlias);
        ReplaceContents(DurableAuthors, staged.DurableAuthors);
        ReplaceContents(PeerStates, staged.PeerStates);
        ReplaceContents(SeenChanges, staged.SeenChanges);
        ReplaceContents(GlobalChangeLedger, staged.GlobalChangeLedger);
        ProviderOperationReceipts.Clear();
        foreach (var pair in restoredProviderReceipts) ProviderOperationReceipts.Add(pair.Key, pair.Value);
        ProviderOperationReceiptSequences.Clear();
        foreach (var pair in restoredProviderReceiptSequences) ProviderOperationReceiptSequences.Add(pair.Key, pair.Value);
        ReplaceContents(ProviderOperationReceiptHighWatermarks, restoredProviderReceiptHighWatermarks);
        ReplaceContents(ProviderOperationReceiptAckedThrough, restoredProviderReceiptAckedThrough);
        ReplaceContents(LastStreamAuthor, staged.LastStreamAuthor);
        ReplaceContents(KnownEpochs, staged.KnownEpochs);
        ReplaceContents(PeerSessionEpochs, staged.PeerSessionEpochs);
        ReplaceContents(Quarantined, staged.Quarantined);
        ReplaceContents(ForbiddenNamespaces, staged.ForbiddenNamespaces);
        ReplaceContents(SchemaVersions, staged.SchemaVersions);
        PeersByNid = restoredPeers;

        Epoch = staged.Epoch;
        var clock = staged.Clock.Snapshot();
        Clock.Observe(new Hlc(clock.LastWall, clock.LastLogical));
        ReplaceContents(ErrorsOut, staged.ErrorsOut);
        ReplaceContents(AcksOut, staged.AcksOut);
    }

    private static void ReplaceContents<TKey, TValue>(
        Dictionary<TKey, TValue> destination, Dictionary<TKey, TValue> source)
        where TKey : notnull
    {
        destination.Clear();
        foreach (var pair in source)
            destination.Add(pair.Key, pair.Value);
    }

    private static void ReplaceContents<T>(HashSet<T> destination, HashSet<T> source)
    {
        destination.Clear();
        foreach (var value in source)
            destination.Add(value);
    }

    private static void ReplaceContents<T>(List<T> destination, List<T> source)
    {
        destination.Clear();
        destination.AddRange(source);
    }

    /// <summary>True while receipt sequence mappings still require the WAL for recovery.</summary>
    internal bool HasPendingProviderOperationReceipts
    {
        get { lock (_lockMeta) return ProviderOperationReceipts.Count != 0; }
    }

    internal bool TryGetProviderOperationReceipt(
        int namespaceId, BatchId changeId, out ulong clientSequence, out bool published)
    {
        ValidateNamespaceId(namespaceId);
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            if (ProviderOperationReceipts.TryGetValue((namespaceId, changeId), out var state))
            {
                clientSequence = state.ClientSequence;
                published = state.Published;
                return true;
            }
            clientSequence = 0;
            published = false;
            return false;
        }
    }

    internal IReadOnlyList<ProviderOperationReceiptSnapshot> ReadProviderOperationReceipts(int namespaceId)
    {
        ValidateNamespaceId(namespaceId);
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            return ProviderOperationReceipts
                .Where(pair => pair.Key.Ns == namespaceId)
                .Select(pair => new ProviderOperationReceiptSnapshot(
                    pair.Key.ChangeId, pair.Value.ClientSequence, pair.Value.Published))
                .OrderBy(item => item.ClientSequence)
                .ToArray();
        }
    }

    /// <summary>
    /// Highest contiguous Published ClientSequence retained by the WAL. A new WAL
    /// generation may begin at an arbitrary sequence after a prior prefix was ACKed
    /// and checkpointed; gaps inside the retained generation are never skipped.
    /// </summary>
    internal ulong GetPublishedProviderOperationReceiptPrefix(int namespaceId)
    {
        ValidateNamespaceId(namespaceId);
        lock (_lockMeta)
        {
            ThrowIfDisposed();
            return GetPublishedProviderOperationReceiptPrefixUnsafe(namespaceId);
        }
    }

    internal void RecordProviderOperationReceipt(int namespaceId, BatchId changeId, ulong clientSequence)
    {
        ValidateNamespaceId(namespaceId);
        if (changeId.IsZero || clientSequence == 0)
            throw new ArgumentException("Provider receipt identity and ClientSequence must be non-zero.");
        using (EnterAllGates())
        {
            var key = (namespaceId, changeId);
            if (ProviderOperationReceipts.TryGetValue(key, out var existing))
            {
                if (existing.ClientSequence != clientSequence)
                    throw new InvalidDataException("One ChangeId was assigned conflicting provider ClientSequences.");
                return;
            }
            if (_wal is null)
                throw new InvalidOperationException("A durable Node WAL must be attached before recording provider operation receipts.");
            if (ProviderOperationReceiptSequences.ContainsKey((namespaceId, clientSequence)))
                throw new InvalidDataException("Provider ClientSequence is already assigned to another ChangeId.");

            ulong expected = ProviderOperationReceiptHighWatermarks.TryGetValue(namespaceId, out ulong high)
                ? checked(high + 1)
                : ProviderOperationReceiptAckedThrough.TryGetValue(namespaceId, out ulong acked)
                    ? checked(acked + 1)
                    : 0;
            if (expected != 0 && clientSequence != expected)
                throw new InvalidDataException("Provider ClientSequence is not contiguous with the retained receipt prefix.");

            _wal.AppendProviderOperationReceipt(namespaceId, changeId, clientSequence, published: false);
            ProviderOperationReceipts.Add(key, new ProviderOperationReceiptState(clientSequence, Published: false));
            ProviderOperationReceiptSequences.Add((namespaceId, clientSequence), changeId);
            ProviderOperationReceiptHighWatermarks[namespaceId] = clientSequence;
        }
    }

    internal void MarkProviderOperationReceiptPublished(int namespaceId, BatchId changeId, ulong clientSequence)
    {
        ValidateNamespaceId(namespaceId);
        using (EnterAllGates())
        {
            var key = (namespaceId, changeId);
            if (!ProviderOperationReceipts.TryGetValue(key, out var state) ||
                state.ClientSequence != clientSequence)
                throw new InvalidDataException("Cannot publish a provider receipt without its exact durable ClientSequence mapping.");
            if (state.Published) return;
            if (_wal is null)
                throw new InvalidOperationException("A durable Node WAL must be attached before marking provider receipts Published.");

            _wal.AppendProviderOperationReceipt(namespaceId, changeId, clientSequence, published: true);
            ProviderOperationReceipts[key] = state with { Published = true };
        }
    }

    /// <summary>
    /// Record a provider receipt ACK only after the provider confirms the same
    /// contiguous Published prefix. This record is what allows WAL checkpoint/truncate.
    /// </summary>
    internal int AcknowledgeProviderOperationReceipts(int namespaceId, ulong throughSequence)
    {
        ValidateNamespaceId(namespaceId);
        if (throughSequence == 0) throw new ArgumentOutOfRangeException(nameof(throughSequence));
        using (EnterAllGates())
        {
            ulong alreadyAcked = ProviderOperationReceiptAckedThrough.TryGetValue(namespaceId, out var acked)
                ? acked : 0;
            if (throughSequence <= alreadyAcked) return 0;
            ulong prefix = GetPublishedProviderOperationReceiptPrefixUnsafe(namespaceId);
            if (throughSequence > prefix)
                throw new InvalidDataException("Provider receipt ACK exceeds the contiguous Published ClientSequence prefix.");
            if (_wal is null)
                throw new InvalidOperationException("A durable Node WAL must be attached before acknowledging provider operation receipts.");

            _wal.AppendProviderOperationReceiptAck(namespaceId, throughSequence);
            int removed = 0;
            foreach (var pair in ProviderOperationReceiptSequences
                .Where(pair => pair.Key.Ns == namespaceId && pair.Key.ClientSequence <= throughSequence)
                .ToArray())
            {
                ProviderOperationReceiptSequences.Remove(pair.Key);
                ProviderOperationReceipts.Remove((namespaceId, pair.Value));
                removed++;
            }
            ProviderOperationReceiptAckedThrough[namespaceId] = throughSequence;
            if (!ProviderOperationReceiptSequences.Keys.Any(key => key.Ns == namespaceId))
                ProviderOperationReceiptHighWatermarks.Remove(namespaceId);
            return removed;
        }
    }

    internal int ReplayProviderOperationReceipt(
        int namespaceId, BatchId changeId, ulong clientSequence, bool published)
    {
        ValidateNamespaceId(namespaceId);
        if (changeId.IsZero || clientSequence == 0)
            throw new InvalidDataException("Invalid WAL provider operation receipt identity.");
        lock (_lockMeta)
        {
            var key = (namespaceId, changeId);
            if (ProviderOperationReceipts.TryGetValue(key, out var existing))
            {
                if (existing.ClientSequence != clientSequence)
                    throw new InvalidDataException("WAL provider receipt conflicts with an existing ChangeId mapping.");
                if (published && !existing.Published)
                {
                    ProviderOperationReceipts[key] = existing with { Published = true };
                    return 1;
                }
                return 0;
            }
            if (published)
                throw new InvalidDataException("WAL Published receipt marker has no preceding durable receipt mapping.");
            if (ProviderOperationReceiptSequences.ContainsKey((namespaceId, clientSequence)))
                throw new InvalidDataException("WAL provider ClientSequence is assigned to multiple ChangeIds.");

            ulong acked = ProviderOperationReceiptAckedThrough.TryGetValue(namespaceId, out var ack) ? ack : 0;
            ulong expected = ProviderOperationReceiptHighWatermarks.TryGetValue(namespaceId, out var high)
                ? checked(high + 1)
                : ack != 0 ? checked(ack + 1) : 0;
            if (expected != 0 && clientSequence != expected)
                throw new InvalidDataException("WAL provider receipt sequence contains a gap or reordering.");

            ProviderOperationReceipts.Add(key, new ProviderOperationReceiptState(clientSequence, Published: false));
            ProviderOperationReceiptSequences.Add((namespaceId, clientSequence), changeId);
            ProviderOperationReceiptHighWatermarks[namespaceId] = clientSequence;
            return 1;
        }
    }

    internal int ReplayProviderOperationReceiptAck(int namespaceId, ulong throughSequence)
    {
        ValidateNamespaceId(namespaceId);
        if (throughSequence == 0)
            throw new InvalidDataException("WAL provider receipt ACK sequence must be non-zero.");
        lock (_lockMeta)
        {
            ulong alreadyAcked = ProviderOperationReceiptAckedThrough.TryGetValue(namespaceId, out var acked)
                ? acked : 0;
            if (throughSequence <= alreadyAcked) return 0;
            if (throughSequence > GetPublishedProviderOperationReceiptPrefixUnsafe(namespaceId))
                throw new InvalidDataException("WAL provider receipt ACK exceeds the contiguous Published prefix.");

            int removed = 0;
            foreach (var pair in ProviderOperationReceiptSequences
                .Where(pair => pair.Key.Ns == namespaceId && pair.Key.ClientSequence <= throughSequence)
                .ToArray())
            {
                ProviderOperationReceiptSequences.Remove(pair.Key);
                ProviderOperationReceipts.Remove((namespaceId, pair.Value));
                removed++;
            }
            ProviderOperationReceiptAckedThrough[namespaceId] = throughSequence;
            if (!ProviderOperationReceiptSequences.Keys.Any(key => key.Ns == namespaceId))
                ProviderOperationReceiptHighWatermarks.Remove(namespaceId);
            return removed;
        }
    }

    private ulong GetPublishedProviderOperationReceiptPrefixUnsafe(int namespaceId)
    {
        ulong acked = ProviderOperationReceiptAckedThrough.TryGetValue(namespaceId, out var value)
            ? value : 0;
        ulong next = checked(acked + 1);
        if (!ProviderOperationReceiptSequences.ContainsKey((namespaceId, next)))
        {
            if (acked != 0 || !ProviderOperationReceiptHighWatermarks.TryGetValue(namespaceId, out _))
                return acked;
            ulong first = ProviderOperationReceiptSequences.Keys
                .Where(key => key.Ns == namespaceId)
                .Select(key => key.ClientSequence)
                .DefaultIfEmpty(0UL)
                .Min();
            if (first == 0) return 0;
            acked = first - 1;
            next = first;
        }

        ulong prefix = acked;
        while (ProviderOperationReceiptSequences.TryGetValue((namespaceId, next), out var changeId))
        {
            if (!ProviderOperationReceipts.TryGetValue((namespaceId, changeId), out var state) ||
                !state.Published)
                break;
            prefix = next;
            if (next == ulong.MaxValue) break;
            next++;
        }
        return prefix;
    }

    internal bool ReplayLocalMutation(OutboxEntry entry, bool applyToStorage)
    {
        if (entry.AuthorNodeId != NodeId)
            throw new InvalidDataException("WAL local mutation belongs to a different node identity.");

        lock (_lockStorage) lock (_lockOutbox)
        {
            var seenKey = (entry.NamespaceId, entry.ChangeId);
            if (SeenChanges.Contains(seenKey)) return false;

            if (Outbox.TryGetValue(entry.NamespaceId, out var queued) &&
                queued.Any(existing => existing.ChangeId == entry.ChangeId))
                throw new InvalidDataException("WAL local mutation duplicates an Outbox change without matching provenance.");

            long currentSeq = _outboxSeq.TryGetValue((NodeId, entry.NamespaceId), out var seq) ? seq : 0;
            if (entry.Seq <= currentSeq || entry.Seq != currentSeq + 1)
                throw new InvalidDataException("WAL local-mutation sequence is not the next expected value.");

            var decoded = Ops.DecodeOp(entry.Payload, fullAuthor: false).Op;
            var fingerprint = ChangeIdentity.LedgerFingerprint(entry.NamespaceId, entry.RecordId,
                entry.OpKind, decoded, entry.Provenance, entry.CausalDepsOrNull);
            var ledgerKey = (entry.NamespaceId, entry.ChangeId);
            if (GlobalChangeLedger.TryGetValue(ledgerKey, out var existingIdentity) &&
                existingIdentity.IsKnown && existingIdentity.Fingerprint != fingerprint)
                throw new InvalidDataException("WAL local mutation conflicts with the global ChangeId ledger.");
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
                OriginAuthorEpoch = entry.OriginAuthorEpoch,
                OriginSequence = entry.OriginSequence,
                RecordId = entry.RecordId,
                OpKind = entry.OpKind,
                Payload = entry.Payload.ToArray(),
                AuthorNodeId = entry.AuthorNodeId,
                CreatedAt = entry.CreatedAt,
                CausalDeps = new List<BatchId>(entry.CausalDeps),
            });
            _outboxSeq[(NodeId, entry.NamespaceId)] = entry.Seq;
            SeenChanges.Add(seenKey);
            GlobalChangeLedger[ledgerKey] = new ChangeLedgerValue(fingerprint, isKnown: true);
            Clock.Observe(timestamp);
            if (entry.CreatedAt > 0) Clock.Observe(new Hlc((ulong)entry.CreatedAt, 0));
            return true;
        }
    }
}
