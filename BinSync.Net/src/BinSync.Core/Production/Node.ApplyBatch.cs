// ApplyBatch / ApplyOp / ProcessAck / Snapshot. Behavior unchanged.

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ApplyBatchInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public BatchResult ApplyBatch(Batch batch, NodeId senderPeer)
    {
        ArgumentNullException.ThrowIfNull(batch);

        // Lost-ACK replay is common and must not deep-copy an entire public DTO graph.
        // Keep the same header-validation precedence as ApplyBatchInternal, then probe
        // Inbox while holding the node gates. A miss falls through to the owned-copy path.
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            if (TryGetDuplicateBatchResult(batch, senderPeer, out var duplicate))
                return duplicate;
        }

        // ApplyBatchInternal resolves and stores receiver-side author context on its batch.
        // Never mutate the caller's DTO or retain caller-owned entry buffers.
        var ownedBatch = CloneBatchForPublicRead(batch);
        BatchResult result;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
            result = ApplyBatchInternal(ownedBatch, senderPeer);
        // Skip pure idempotent replay (Duplicate, Applied==0) — hot path must stay ~ns.
        if (!result.Duplicate || result.Applied > 0)
            RaiseBatchApplied(result, senderPeer);
        return result;
    }

    // Must be called under all node gates. Mirrors the validation steps that precede
    // Inbox.Contains in ApplyBatchInternal; duplicate payload contents are not decoded.
    private bool TryGetDuplicateBatchResult(Batch batch, NodeId senderPeer, out BatchResult result)
    {
        ThrowIfDisposed();
        ValidateBatchForWire(batch);
        if (batch.MergePolicy != 0 && batch.MergePolicy != 0x02)
            throw new SyncException("SCHEMA_MISMATCH", batch.BatchId);

        result = new BatchResult(batch.BatchId)
        {
            CursorEnd = batch.CursorEnd,
            NamespaceId = batch.NamespaceId,
        };

        if (ForbiddenNamespaces.Contains(batch.NamespaceId))
            throw new SyncException("NAMESPACE_FORBIDDEN", batch.BatchId);

        var currentEpoch = SessionEpoch(senderPeer, batch.NamespaceId);
        if (currentEpoch is not null && batch.IssuerEpoch < currentEpoch.Value)
            throw new SyncException("EPOCH_REGRESSION", batch.BatchId);

        int supported = SchemaVersions.TryGetValue(batch.NamespaceId, out var version) ? version : 1;
        if (batch.SchemaVersion > supported)
            throw new SyncException("SCHEMA_PENDING", batch.BatchId);

        var key = (batch.IssuerNodeId, batch.NamespaceId, batch.IssuerEpoch, batch.BatchId);
        if (!Inbox.Contains(key))
            return false;

        // Preserve the public-copy path's structural null checks without allocating
        // entry objects, payload arrays, or causal-dependency lists for a replay.
        for (int i = 0; i < batch.Entries.Count; i++)
        {
            var entry = batch.Entries[i];
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentNullException.ThrowIfNull(entry.CausalDeps);
        }

        AcksOut.Add(batch.BatchId);
        result.Duplicate = true;
        result.Acked = true;
        if (InboxAuthors.TryGetValue(key, out var acceptedAuthor))
            result.DurableAuthor = acceptedAuthor;
        else if (LastStreamAuthor.TryGetValue((senderPeer, batch.NamespaceId), out var priorAuthor))
            result.DurableAuthor = priorAuthor;
        else
            result.DurableAuthor = batch.DurableAuthor;
        return true;
    }

    internal BatchResult ApplyBatchInternal(Batch batch, NodeId senderPeer, bool persistWal = true, byte[]? durablePayload = null)
    {
        ThrowIfDisposed();
        ValidateBatchForWire(batch);
        // This runtime currently applies LWW_FIELD only. Zero is accepted as the
        // legacy/default spelling; the canonical wire code is 0x02. Never silently
        // apply a different advertised policy using LWW semantics.
        if (batch.MergePolicy != 0 && batch.MergePolicy != 0x02)
            throw new SyncException("SCHEMA_MISMATCH", batch.BatchId);
        var result = new BatchResult(batch.BatchId)
        {
            CursorEnd = batch.CursorEnd,
            NamespaceId = batch.NamespaceId,
        };
        // STEP 0: namespace isolation (§3.7) — forbidden ns kills only that stream
        if (ForbiddenNamespaces.Contains(batch.NamespaceId))
            throw new SyncException("NAMESPACE_FORBIDDEN", batch.BatchId);
        // STEP 0.5: epoch regression (§4.4) — SPEC CORRECTIONS (errata E-2/E-3):
        // the watermark is keyed by the RECEIVING SESSION ((sender_peer, ns)) and compares
        // against the sender's CURRENT session incarnation epoch learned from HELLO/ALIAS_MAP.
        var chain = (batch.IssuerNodeId, batch.NamespaceId, batch.IssuerEpoch);
        var sesKey = (senderPeer, batch.NamespaceId);
        var curKnown = SessionEpoch(senderPeer, batch.NamespaceId);
        if (curKnown is not null && batch.IssuerEpoch < curKnown.Value)
            throw new SyncException("EPOCH_REGRESSION", batch.BatchId);
        // STEP 0.6: schema pending (§18.3 / [СХ7]) — do NOT advance lastReceivedSeq
        int supported = SchemaVersions.TryGetValue(batch.NamespaceId, out var sv) ? sv : 1;
        if (batch.SchemaVersion > supported)
            throw new SyncException("SCHEMA_PENDING", batch.BatchId);
        // STEP 1: idempotency FIRST ([S7], [П7], §10.2.2 lost-ACK replay)
        var key = (batch.IssuerNodeId, batch.NamespaceId, batch.IssuerEpoch, batch.BatchId);
        if (Inbox.Contains(key))
        {
            AcksOut.Add(batch.BatchId);   // duplicate → no-op but ACK mandatory [П4]
            result.Duplicate = true;
            result.Acked = true;
            // ACK must name the author stream of THIS batch, not merely the last
            // stream seen from this peer. A peer may interleave several authors.
            if (InboxAuthors.TryGetValue(key, out var acceptedAuthor))
                result.DurableAuthor = acceptedAuthor;
            else if (LastStreamAuthor.TryGetValue((senderPeer, batch.NamespaceId), out var priorAuthor))
                result.DurableAuthor = priorAuthor; // defensive fallback for pre-v3 in-memory state
            else
                result.DurableAuthor = batch.DurableAuthor;
            return result;
        }
        // STEP 2: cursor check [S2]/[П2] — errata E-3: keyed per SESSION stream.
        var pkey = (batch.IssuerNodeId, batch.NamespaceId);
        // Do not publish a new peer-state row until validation and resource preflight pass.
        PeerState? ps = PeerStates.TryGetValue(pkey, out var existingPs) ? existingPs : null;
        // Errata E-4: resolve the batch's AUTHOR STREAM durably and key the receive cursor
        // per stream — §13.6.1 [Р3] makes seq numbering per-author.
        // BUGFIX (persistence resume): restored ops carry Origin=0 as a stale sentinel but
        // embed the TRUE author in full-author mode (§13.6). Author resolution helper:
        // full-author ops resolve via their embedded nodeId; otherwise fall back to the
        // session alias map, fail-closed on UNKNOWN_ALIAS [А2]/[А3].
        NodeId ResolveAuthor(object op)
        {
            bool full = op switch { OpFieldWrite x => x.FullAuthorMode, OpFieldTomb x => x.FullAuthorMode, OpRecordTombstone x => x.FullAuthorMode, _ => false };
            var emb = GetAuthor(op);
            if (full && emb.HasValue) return emb.Value;
            try { return ResolveAliasInternal(senderPeer, GetOrigin(op)); }
            catch (SyncException) when (full && emb.HasValue) { return emb.Value; }
        }
        // Decode ops once — reused for stream author, apply loop, and relay (no batch.Ops()).
        int nEntEarly = batch.Entries.Count;
        var decodedOps = nEntEarly > 0 ? new object[nEntEarly] : Array.Empty<object>();
        for (int di = 0; di < nEntEarly; di++)
        {
            var entry = batch.Entries[di];
            var op = Ops.DecodeOp(entry.Payload).Op;
            ValidateEntryOperation(entry, op, batch.BatchId);
            decodedOps[di] = op;
        }

        // A non-zero ChangeId identifies one logical mutation. Reusing it for
        // different entries makes provenance/relay deduplication ambiguous.
        var batchChangeIds = new HashSet<BatchId>();
        for (int di = 0; di < nEntEarly; di++)
        {
            var changeId = batch.Entries[di].ChangeId;
            if (changeId.IsZero || !batchChangeIds.Add(changeId))
                throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        }

        NodeId streamAuthor;
        try
        {
            streamAuthor = nEntEarly > 0 ? ResolveAuthor(decodedOps[0]) : batch.IssuerNodeId;
        }
        catch (SyncException) { streamAuthor = batch.IssuerNodeId; }
        batch.AuthorStreamNodeId = streamAuthor;
        // [Р3]/errata E-4: a delta batch is one author stream. Validate every
        // decoded operation before cursor checks or any staging/WAL/state mutation;
        // otherwise a mixed-author batch could consume one stream's cursor while
        // applying changes belonging to another stream.
        for (int di = 1; di < nEntEarly; di++)
        {
            if (ResolveAuthor(decodedOps[di]) != streamAuthor)
                throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        }
        result.DurableAuthor = batch.DurableAuthor;
        var streamKey = (senderPeer, batch.IssuerEpoch, batch.NamespaceId, streamAuthor);
        long expected = (ps is not null && ps.ReceivedStreams.TryGetValue(streamKey, out var ex) ? ex : 0) + 1;
        if (batch.CursorStart != expected)
            throw new SyncException("SEQ_GAP", batch.BatchId);
        // STEP 3: batch digest chain (§12.17.2, [Т3]).
        // BUGFIX (port fidelity): the reference port checks prevBatchDigest UNCONDITIONALLY
        // after the cursor gate of STEP 2 has already passed. The earlier "streamEmpty"
        // guard was wrong in both directions: an out-of-order batch on a stream that is
        // still EMPTY at the receiver skipped the chain check entirely (b2 delivered before
        // b1 applied cleanly instead of raising), while a legitimate FIRST batch of a fresh
        // per-(issuer,ns,epoch) chain could hit a stale non-zero checkpoint. With STEP 2
        // guaranteeing cursor contiguity, an unconditional check is exactly [Т3].
        {
            var expectPrev = LastBatchDigest.TryGetValue(chain, out var ep2) ? ep2 : default;
            if (batch.PrevBatchDigest != expectPrev)
                throw new SyncException("BATCH_CHAIN_BROKEN", batch.BatchId);
        }
        var root = ComputeRecordsRoot(batch.Entries);
        if (batch.RecordsRoot != root)
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        var recomputed = ComputeBatchDigest(batch.BatchId, batch.NamespaceId, batch.IssuerNodeId,
            batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd, batch.RecordsRoot, batch.PrevBatchDigest);
        if (batch.BatchDigest != recomputed)
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        // STEP 4: atomic application [T1] — copy-on-write only for touched records.
        // Untouched records stay as shared references in nsStore; overlays discard on throw.
        bool hadNamespace = Storage.TryGetValue(batch.NamespaceId, out var nsStore);
        nsStore ??= new Dictionary<RecordId, Record>();
        var overlays = new Dictionary<RecordId, Record>();
        int applied = 0;
        int nEnt = nEntEarly;
        var changedFlags = nEnt > 0 ? new bool[nEnt] : Array.Empty<bool>();
        var relayFlags = nEnt > 0 ? new bool[nEnt] : Array.Empty<bool>();
        var globalDuplicateFlags = nEnt > 0 ? new bool[nEnt] : Array.Empty<bool>();
        var resolvedAuthors = nEnt > 0 ? new NodeId[nEnt] : Array.Empty<NodeId>();
        var stagedGlobalLedger = new Dictionary<(int Ns, BatchId ChangeId), ChangeLedgerValue>();
        // One wall sample for the whole batch (skew window is large vs apply latency).
        long nowWall = Clock.Now();
        if (nowWall < 0)
            throw new InvalidOperationException("HLC wall clock must be non-negative Unix microseconds.");
        ulong maxAllowedWall = Clock.MaxAllowedRemoteWall((ulong)nowWall);
        for (int idx = 0; idx < nEnt; idx++)
        {
            var e = batch.Entries[idx];
            var op = decodedOps[idx];
            var opAuthor = ResolveAuthor(op);   // [А2]; unknown → raise
            resolvedAuthors[idx] = opAuthor; // reuse after commit; no post-commit resolution
            // Fingerprints include the resolved durable author and FULL_AUTHOR_ID
            // encoding, so compact aliases and transport routes cannot affect identity.
            var ledgerKey = (batch.NamespaceId, e.ChangeId);
            var fingerprint = ChangeIdentity.Fingerprint(batch.NamespaceId, e.RecordId,
                e.OpKind, op, opAuthor, e.CausalDepsOrNull);
            if (GlobalChangeLedger.TryGetValue(ledgerKey, out var knownIdentity))
            {
                if (knownIdentity.IsKnown && knownIdentity.Fingerprint != fingerprint)
                    throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
                globalDuplicateFlags[idx] = true;
                if (!knownIdentity.IsKnown)
                    stagedGlobalLedger[ledgerKey] = new ChangeLedgerValue(fingerprint, isKnown: true);
            }
            else
                stagedGlobalLedger[ledgerKey] = new ChangeLedgerValue(fingerprint, isKnown: true);

            // clock skew clamp on incoming HLC wall (§5.3)
            if (WallOf(op) > maxAllowedWall)
                throw new SyncException("CLOCK_DRIFT", batch.BatchId);
            // An already accepted ChangeId must not be re-applied: doing so after
            // tombstone/state reclamation could resurrect stale data.
            if (globalDuplicateFlags[idx])
                continue;
            RecordId recId = op is OpRecordTombstone rt ? rt.RecordId : e.RecordId;
            if (!overlays.TryGetValue(recId, out var rec))
            {
                if (nsStore.TryGetValue(recId, out var existing))
                    rec = Clone(existing);
                else
                    rec = new Record(e.RecordId, (ushort)batch.NamespaceId);
                overlays[recId] = rec;
            }
            bool changed = ApplyOpInternal(rec, op, opAuthor);
            if (changed) { applied++; changedFlags[idx] = true; }
        }
        // Stage provenance and relay decisions BEFORE publishing any state. This is
        // required so configured resource limits cannot reject after storage/cursors commit.
        var relayChangeIds = batch.IssuerNodeId != NodeId && nEnt > 0
            ? new BatchId[nEnt]
            : Array.Empty<BatchId>();
        var relayIndices = batch.IssuerNodeId != NodeId
            ? new List<int>(Math.Min(nEnt, 16))
            : new List<int>(0);
        var newSeenKeys = batch.IssuerNodeId != NodeId
            ? new HashSet<(NodeId Peer, int Ns, BatchId ChangeId)>()
            : null;
        // The same provenance can arrive through different peers. Keep the
        // per-peer SeenChanges keys for echo detection, but enqueue a ChangeId
        // for relay at most once in this namespace.
        var queuedChangeIds = new HashSet<BatchId>();
        if (Outbox.TryGetValue(batch.NamespaceId, out var queuedEntries))
            for (int qi = 0; qi < queuedEntries.Count; qi++)
                if (!queuedEntries[qi].ChangeId.IsZero)
                    queuedChangeIds.Add(queuedEntries[qi].ChangeId);

        if (batch.IssuerNodeId != NodeId)
        {
            for (int idx = 0; idx < nEnt; idx++)
            {
                var e = batch.Entries[idx];
                var cid = e.ChangeId; // mandatory provenance identity; never synthesize on receive
                relayChangeIds[idx] = cid;
                var ck = (senderPeer, batch.NamespaceId, cid);
                if (SeenChanges.Contains(ck))
                    continue; // already accepted provenance consumes no new capacity
                if (!newSeenKeys!.Add(ck))
                    continue; // duplicate provenance within this batch
                if (globalDuplicateFlags[idx])
                    continue; // globally accepted already, regardless of receiving route
                if (!queuedChangeIds.Add(cid))
                    continue; // this logical change is already queued from another peer

                bool relayOk = true;
                if (!changedFlags[idx])
                {
                    var rop = decodedOps[idx];
                    ushort? rfid = rop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
                    if (rfid is not null)
                    {
                        var rAuthor = resolvedAuthors[idx];
                        // Evaluate against the staged post-apply record, not the live map.
                        Record? staged = overlays.TryGetValue(e.RecordId, out var stagedRec)
                            ? stagedRec
                            : (nsStore.TryGetValue(e.RecordId, out var liveRec) ? liveRec : null);
                        bool hasState = staged is not null &&
                            (staged.State.ContainsKey(rfid.Value) ||
                             (staged.HasFieldTombs && staged.FieldTombs.ContainsKey(rfid.Value)));
                        if (rAuthor == NodeId && hasState)
                            relayOk = false;
                    }
                }
                if (relayOk) relayFlags[idx] = true; // new provenance ⇒ relay, including a losing op
            }

            for (int idx = 0; idx < nEnt; idx++)
            {
                if (!relayFlags[idx] || GetAuthor(decodedOps[idx]) is not null)
                    continue; // full-author mode already carries its durable author
                relayIndices.Add(idx);
            }
        }

        // Stage opportunistic alias propagation once per distinct durable author.
        // The old per-entry loop repeatedly enumerated every peer and allocated a key
        // snapshot, making relay cost O(relay entries × peers) with avoidable garbage.
        var aliasPropagations = new List<(NodeId Author, int Epoch)>();
        var aliasPropagationSet = new HashSet<(NodeId Author, int Epoch)>();
        var aliasPeers = new List<Node>();
        Node? senderAliasPeer = null;
        if (relayIndices.Count > 0 &&
            AliasToNode.TryGetValue(senderPeer, out var senderAliasMap))
        {
            var aliasEpochByAuthor = new Dictionary<NodeId, int>(senderAliasMap.Count);
            foreach (var (_, (author, epoch)) in senderAliasMap)
                aliasEpochByAuthor.TryAdd(author, epoch); // preserve first registered binding

            foreach (int idx in relayIndices)
            {
                var author = resolvedAuthors[idx];
                if (aliasEpochByAuthor.TryGetValue(author, out int epoch) &&
                    aliasPropagationSet.Add((author, epoch)))
                    aliasPropagations.Add((author, epoch));
            }

            if (aliasPropagations.Count > 0)
            {
                foreach (var otherPeerId in AliasToNode.Keys)
                {
                    if (otherPeerId == senderPeer) continue;
                    if (PeersByNid is not null &&
                        PeersByNid.TryGetValue(otherPeerId, out var otherPeer) &&
                        otherPeer is not null)
                        aliasPeers.Add(otherPeer);
                }

                if (PeersByNid is not null)
                    PeersByNid.TryGetValue(senderPeer, out senderAliasPeer);
            }
        }

        // Reserve every relay sequence during preflight. Sequence exhaustion must
        // reject the batch before storage, cursors, Inbox or SeenChanges are published.
        var relaySequences = nEnt > 0 ? new long[nEnt] : Array.Empty<long>();
        var stagedOutboxSequences = new Dictionary<(NodeId Author, int NamespaceId), long>();
        foreach (int idx in relayIndices)
        {
            var sequenceKey = (resolvedAuthors[idx], batch.NamespaceId);
            long current;
            if (!stagedOutboxSequences.TryGetValue(sequenceKey, out current))
                current = _outboxSeq.TryGetValue(sequenceKey, out var persisted) ? persisted : 0;
            if (current == long.MaxValue)
                throw new SyncException("INTERNAL", batch.BatchId);
            long next = current + 1;
            stagedOutboxSequences[sequenceKey] = next;
            relaySequences[idx] = next;
        }

        // Count only genuinely new records and provenance keys. Limits are checked
        // against the complete batch, before any record, cursor, inbox or relay mutation.
        int newRecordCount = 0;
        foreach (var rid in overlays.Keys)
            if (!nsStore.ContainsKey(rid)) newRecordCount++;
        Limits?.EnsureCanApplyBatch(this, batch.NamespaceId, newRecordCount,
            relayIndices.Count, newSeenKeys?.Count ?? 0,
            stagedGlobalLedger.Keys.Count(key => !GlobalChangeLedger.ContainsKey(key)));

        // Prepare every object and reserve every collection slot before the WAL commit
        // point. After AppendRemoteBatch succeeds, publication is assignment/add-only.
        var hasRelayOutbox = Outbox.TryGetValue(batch.NamespaceId, out var relayOutbox);
        var stagedRelayEntries = relayIndices.Count == 0
            ? Array.Empty<OutboxEntry>()
            : new OutboxEntry[relayIndices.Count];
        for (int ri = 0; ri < relayIndices.Count; ri++)
        {
            int idx = relayIndices[ri];
            var e = batch.Entries[idx];
            var op = decodedOps[idx];
            stagedRelayEntries[ri] = new OutboxEntry
            {
                NamespaceId = e.NamespaceId != 0 ? e.NamespaceId : batch.NamespaceId,
                Seq = relaySequences[idx],
                ChangeId = relayChangeIds[idx],
                RecordId = e.RecordId,
                OpKind = e.OpKind,
                Payload = durablePayload is null ? e.Payload : e.Payload.ToArray(),
                AuthorNodeId = resolvedAuthors[idx],
                CreatedAt = (long)WallOf(op),
            };
            if (e.CausalDepsOrNull is { } causalDeps)
                stagedRelayEntries[ri].CausalDeps.AddRange(causalDeps);
        }

        if (stagedRelayEntries.Length > 0)
        {
            if (hasRelayOutbox)
                relayOutbox!.EnsureCapacity(relayOutbox.Count + stagedRelayEntries.Length);
            else
            {
                Outbox.EnsureCapacity(Outbox.Count + 1);
                relayOutbox = new List<OutboxEntry>(stagedRelayEntries.Length);
            }

            int newSequenceKeys = stagedOutboxSequences.Keys.Count(key => !_outboxSeq.ContainsKey(key));
            _outboxSeq.EnsureCapacity(_outboxSeq.Count + newSequenceKeys);
        }

        if (overlays.Count > 0)
        {
            if (!hadNamespace)
            {
                nsStore.EnsureCapacity(overlays.Count);
                foreach (var (rid, rec) in overlays)
                    nsStore.Add(rid, rec);
                Storage.EnsureCapacity(Storage.Count + 1);
            }
            else
                nsStore.EnsureCapacity(nsStore.Count + newRecordCount);
        }

        Inbox.EnsureCapacity(Inbox.Count + 1);
        InboxAuthors.EnsureCapacity(InboxAuthors.Count + 1);
        if (ps is null)
            PeerStates.EnsureCapacity(PeerStates.Count + 1);
        var peerStateToPublish = ps ?? new PeerState(batch.IssuerNodeId, batch.NamespaceId);
        peerStateToPublish.ReceivedStreams.EnsureCapacity(peerStateToPublish.ReceivedStreams.Count + 1);
        LastStreamAuthor.EnsureCapacity(LastStreamAuthor.Count +
            (LastStreamAuthor.ContainsKey((senderPeer, batch.NamespaceId)) ? 0 : 1));
        LastBatchDigest.EnsureCapacity(LastBatchDigest.Count + (LastBatchDigest.ContainsKey(chain) ? 0 : 1));
        KnownEpochs.EnsureCapacity(KnownEpochs.Count + (KnownEpochs.ContainsKey(sesKey) ? 0 : 1));
        AcksOut.EnsureCapacity(AcksOut.Count + 1);
        if (newSeenKeys is not null && newSeenKeys.Count > 0)
            SeenChanges.EnsureCapacity(SeenChanges.Count + newSeenKeys.Count);
        if (stagedGlobalLedger.Count > 0)
            GlobalChangeLedger.EnsureCapacity(GlobalChangeLedger.Count +
                stagedGlobalLedger.Keys.Count(key => !GlobalChangeLedger.ContainsKey(key)));

        // The accepted batch must be durable before publishing any effects. ReceiveBatchAsync
        // sends its ACK only after this method returns, so a crash after ACK cannot lose it.
        if (persistWal && _wal is not null)
        {
            var aliases = AliasToNode.TryGetValue(senderPeer, out var sessionMap)
                ? sessionMap.OrderBy(x => x.Key)
                    .Select(x => new AliasMapEntry((ushort)x.Key, x.Value.Id, x.Value.Epoch)).ToArray()
                : Array.Empty<AliasMapEntry>();
            int senderEpoch = PeerSessionEpochs.TryGetValue(senderPeer, out var knownPeerEpoch) ? knownPeerEpoch : 0;
            var walPayload = durablePayload ??
                (CausalWire.HasDeps(batch.Entries)
                    ? EncodeBatchPayloadCausal(batch)
                    : EncodeBatchPayloadInternal(batch));
            _wal.AppendRemoteBatch(senderPeer, senderEpoch, aliases, walPayload);
        }

        // Commit: all deterministic validation and resource checks have succeeded.
        if (!hadNamespace && overlays.Count > 0)
            Storage[batch.NamespaceId] = nsStore;
        else
            foreach (var (rid, rec) in overlays)
                nsStore[rid] = rec;

        if (ps is null)
            PeerStates[pkey] = ps = peerStateToPublish;
        LastStreamAuthor[(senderPeer, batch.NamespaceId)] = streamAuthor;   // E-11
        Inbox.Add(key);
        InboxAuthors[key] = streamAuthor;
        ps.LastReceivedSeq = batch.CursorEnd;   // [П3]/[Т2] atomic cursor jump (compat copy)
        ps.ReceivedStreams[streamKey] = batch.CursorEnd;   // errata E-3 per-session cursor
        LastBatchDigest[chain] = batch.BatchDigest;
        // SPEC CORRECTION (errata E-2): track epochs PER SESSION (sender_peer, ns).
        int knownS = KnownEpochs.TryGetValue(sesKey, out var ks) ? ks : 0;
        KnownEpochs[sesKey] = Math.Max(knownS, batch.IssuerEpoch);
        AcksOut.Add(batch.BatchId);
        result.Applied = applied;
        result.Acked = true;

        // Publish the global identity ledger only after the accepted batch WAL frame
        // is durable. Unknown legacy markers are upgraded to known fingerprints here.
        foreach (var pair in stagedGlobalLedger)
            GlobalChangeLedger[pair.Key] = pair.Value;

        // Relay only the entries selected and fully materialized during preflight.
        if (batch.IssuerNodeId != NodeId)
        {
            foreach (var ck in newSeenKeys!)
                SeenChanges.Add(ck);

            if (stagedRelayEntries.Length > 0)
            {
                if (!hasRelayOutbox)
                    Outbox[batch.NamespaceId] = relayOutbox!;
                foreach (var entry in stagedRelayEntries)
                {
                    relayOutbox!.Add(entry);
                    _outboxSeq[(entry.AuthorNodeId, batch.NamespaceId)] = entry.Seq;
                }
            }
        }

        // Alias propagation is post-commit, best-effort control-plane work. Run it
        // once per distinct author, reusing the pre-commit peer snapshot.
        foreach (var (author, epoch) in aliasPropagations)
        {
            foreach (var peer in aliasPeers)
                EnsureAliasBestEffort(peer, author, epoch);

            if (senderAliasPeer is not null && author != NodeId)
                EnsureAliasBestEffort(senderAliasPeer, author, epoch);
        }
        return result;
    }

    // Alias propagation is opportunistic control-plane work performed after the
    // batch has committed. A peer may be disposed concurrently; that must not make
    // an already accepted batch appear to have failed. Do not mask disposal of this
    // node itself or any other alias-negotiation error.
    private void EnsureAliasBestEffort(Node peer, NodeId author, int epoch)
    {
        try { EnsureAlias(peer, author, epoch); }
        catch (ObjectDisposedException) when (
            Volatile.Read(ref _disposed) == 0 &&
            Volatile.Read(ref peer._disposed) != 0)
        {
            // The destination peer is gone; its alias map will be rebuilt on reconnect.
        }
    }

    /// Errata E-13 follow-up 4 (scenario 37): does OUR OWN un-ACKed queue already carry a
    /// byte-equal winner for (recordId, fieldId)? If yes, the ring merely mirrors our own
    /// state back — enqueueing again would duplicate propagation [И2].
    private bool OwnQueueHolds(int ns, RecordId rid, object qop)
    {
        var list = Outbox.TryGetValue(ns, out var l) ? l : new List<OutboxEntry>();
        foreach (var q in list)
        {
            if (q.RecordId != rid || q.AuthorNodeId != NodeId) continue;
            object sOp;
            try { sOp = Ops.DecodeOp(q.Payload).Op; }
            catch (Exception)
            {
                // Corrupt outbox payload — skip this entry for OwnQueueHolds.
                continue;
            }
            ushort? sfid = sOp switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
            ushort? qfid = qop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
            if (sfid != qfid) continue;
            if (TagOf(sOp) == TagOf(qop) && WallOf(sOp) == WallOf(qop) &&
                LogicalOf(sOp) == LogicalOf(qop) &&
                ValueOf(sOp).AsSpan().SequenceEqual(ValueOf(qop)))
                return true;
        }
        return false;
    }


    private static ushort GetOrigin(object op) => op switch
    { OpFieldWrite x => x.Origin, OpFieldTomb x => x.Origin, OpRecordTombstone x => x.Origin, _ => (ushort)0 };
    private static NodeId? GetAuthor(object op) => op switch
    { OpFieldWrite x => x.AuthorNodeId, OpFieldTomb x => x.AuthorNodeId, OpRecordTombstone x => x.AuthorNodeId, _ => null };

    /// Apply one resolved op to a staged record. Echo suppression happens inside
    /// MergeField: identical (hlc, author, value) → changed=False → nothing queued.
    internal bool ApplyOpInternal(Record rec, object op, NodeId author)
    {
        if (op is OpFieldWrite fw)
        {
            if (fw.FieldId == Model.LifecycleFieldId)
            {
                byte val = fw.Value.Length > 0 ? fw.Value[0] : Model.Alive;
                return Merge.ApplyLifecycle(rec, new Hlc(fw.Wall, fw.Logical), author, val);
            }
            var fs = new FieldState(fw.Value, new Hlc(fw.Wall, fw.Logical), author, fw.KeyEpoch);
            return Merge.ApplyFieldWrite(rec, fw.FieldId, fs);
        }
        if (op is OpFieldTomb ft)
        {
            var fs = new FieldState(Array.Empty<byte>(), new Hlc(ft.Wall, ft.Logical), author, ft.KeyEpoch);
            return Merge.ApplyFieldTomb(rec, ft.FieldId, fs);
        }
        if (op is OpRecordTombstone rt)
            return Merge.ApplyLifecycle(rec, new Hlc(rt.Wall, rt.Logical), author, Model.Dead);
        throw new SyncException("INTERNAL");
    }

    /// Receiver ACK advances our push watermark; GC of outbox up to min acked.
    /// SPEC CORRECTION (errata E-4): the ACK names the acknowledged AUTHOR STREAM.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ProcessAckInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    /// <summary>Process an ACK: advance per-author watermarks for the peer session.</summary>
    public void ProcessAck(NodeId peerNode, int namespaceId, long ackedSeq, NodeId? authorStream = null)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
            ProcessAckInternal(peerNode, namespaceId, ackedSeq, authorStream);
        RaiseAckProcessed(peerNode, namespaceId, ackedSeq, authorStream);
    }

    internal void ProcessAckInternal(NodeId peerNode, int namespaceId, long ackedSeq, NodeId? authorStream = null)
    {
        ThrowIfDisposed();
        if (ackedSeq < 0 || (authorStream is not null && authorStream.Value.IsZero))
            throw new SyncException("ACK_OUT_OF_RANGE");
        if (!PeerStates.TryGetValue((peerNode, namespaceId), out var ps)) return;

        var entries = Outbox.TryGetValue(namespaceId, out var es) ? es : null;
        if (authorStream is not null)
        {
            var author = authorStream.Value;
            var current = ps.AckedFor(author);
            if (ackedSeq > current)
            {
                long offset = ps.StreamOffsets.TryGetValue(author, out var savedOffset) ? savedOffset : 0;
                long localCurrent = checked(current + offset);
                long localRequested = checked(ackedSeq + offset);
                if (!ps.StreamPushed.TryGetValue(author, out var pushed) || ackedSeq > pushed ||
                    !HasContiguousOutboxRange(entries, author, localCurrent, localRequested))
                    throw new SyncException("ACK_OUT_OF_RANGE");
                ps.StreamAcked[author] = ackedSeq;
            }
        }
        else
        {
            // Legacy ACKs have no author key. Bound them by the greatest sequence
            // actually handed to this peer, then advance each stream only through
            // a contiguous range still present in the outbox.
            if (ackedSeq > ps.LastPushedSeq)
                throw new SyncException("ACK_OUT_OF_RANGE");
            foreach (var (author, pushed) in ps.StreamPushed)
            {
                var current = ps.AckedFor(author);
                var target = Math.Min(ackedSeq, pushed);
                if (target <= current) continue;
                long offset = ps.StreamOffsets.TryGetValue(author, out var savedOffset) ? savedOffset : 0;
                long localCurrent = checked(current + offset);
                long localTarget = checked(target + offset);
                var contiguous = ContiguousOutboxEnd(entries, author, localCurrent, localTarget);
                if (contiguous > localCurrent)
                    ps.StreamAcked[author] = contiguous - offset;
            }
        }

        // legacy mirror: monotone scalar = min over known streams' watermarks.
        if (ps.StreamAcked.Count > 0)
            ps.LastAckedSeq = ps.StreamAcked.Values.Min();

        // Ids only makes retransmission deterministic while a cursor range is
        // outstanding. Once a validated contiguous ACK covers the complete range,
        // the cached BatchId/previous-digest mapping is no longer needed. Keep
        // LastStreamDigest untouched: it is the chain checkpoint for later batches.
        PruneAcknowledgedBatchIds(ps);
        CompactOutbox(namespaceId);
    }

    private static void PruneAcknowledgedBatchIds(PeerState peerState)
    {
        List<(NodeId Author, long Start, long End)>? acknowledged = null;
        foreach (var key in peerState.Ids.Keys)
        {
            if (peerState.StreamAcked.TryGetValue(key.Author, out var acked) &&
                key.End <= acked)
            {
                (acknowledged ??= new List<(NodeId Author, long Start, long End)>()).Add(key);
            }
        }

        if (acknowledged is null) return;
        foreach (var key in acknowledged)
            peerState.Ids.Remove(key);
    }

    private static bool HasContiguousOutboxRange(List<OutboxEntry>? entries, NodeId author,
        long current, long requested) =>
        requested <= current || ContiguousOutboxEnd(entries, author, current, requested) == requested;

    private static long ContiguousOutboxEnd(List<OutboxEntry>? entries, NodeId author,
        long current, long requested)
    {
        if (requested <= current) return current;
        if (entries is null) return current;
        long next = current + 1;
        for (int i = 0; i < entries.Count && next <= requested; i++)
        {
            var entry = entries[i];
            if (entry.AuthorNodeId != author || entry.Seq < next) continue;
            if (entry.Seq != next) break;
            next++;
        }
        return next - 1;
    }


    /// Snapshot = all live records + tombstones + ghosts (§ summary 'снапшот').
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>BuildSnapshotInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public List<Batch> BuildSnapshot(int namespaceId)
    {
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta) return BuildSnapshotInternal(namespaceId);
    }

    internal List<Batch> BuildSnapshotInternal(int namespaceId)
    {
        ThrowIfDisposed();
        ValidateNamespaceId(namespaceId);
        var batches = new List<Batch>();
        var nsStore = Ns(namespaceId);
        foreach (var (rid, rec) in nsStore.OrderBy(kv => kv.Key.ToString()))
        {
            var entries = new List<OutboxEntry>();
            foreach (var (fid, fs) in rec.State.OrderBy(kv => kv.Key))
            {
                var op = new OpFieldWrite { FieldId = fid, KeyEpoch = fs.KeyEpoch, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical, Origin = 1, AuthorNodeId = fs.AuthorNodeId, Value = fs.Value.ToArray() };
                entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = BatchId.From(Guid.NewGuid().ToByteArray()), RecordId = rid, OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOpFull(op), AuthorNodeId = fs.AuthorNodeId, CreatedAt = 0 });
            }
            foreach (var (fid, fs) in rec.FieldTombs.OrderBy(kv => kv.Key))
            {
                var op = new OpFieldTomb { FieldId = fid, KeyEpoch = fs.KeyEpoch, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical, Origin = 1, AuthorNodeId = fs.AuthorNodeId };
                entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = BatchId.From(Guid.NewGuid().ToByteArray()), RecordId = rid, OpKind = OpsConst.TagFieldTomb, Payload = Ops.EncodeOpFull(op), AuthorNodeId = fs.AuthorNodeId, CreatedAt = 0 });
            }
            if (rec.LifecycleHlc is not null)
            {
                var lcAuthor = rec.LifecycleAuthor ?? NodeId;
                var op = new OpRecordTombstone { RecordId = rid, Wall = rec.LifecycleHlc.Wall, Logical = rec.LifecycleHlc.Logical, Origin = 1, AuthorNodeId = lcAuthor };
                if (rec.LifecycleValue == Model.Alive)
                {
                    // SPEC HARDENING: snapshots must carry an explicit REVIVE lifecycle op
                    // so the receiver un-ghosts historical fields (§7.4 У4).
                    var opw = new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = rec.LifecycleHlc.Wall, Logical = rec.LifecycleHlc.Logical, Origin = 1, AuthorNodeId = lcAuthor, Value = new[] { Model.Alive } };
                    entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = BatchId.From(Guid.NewGuid().ToByteArray()), RecordId = rid, OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOpFull(opw), AuthorNodeId = opw.AuthorNodeId!.Value, CreatedAt = 0 });
                }
                else
                    entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = BatchId.From(Guid.NewGuid().ToByteArray()), RecordId = rid, OpKind = OpsConst.TagRecordTomb, Payload = Ops.EncodeOpFull(op), AuthorNodeId = op.AuthorNodeId!.Value, CreatedAt = 0 });
            }
            if (entries.Count > 0)
                batches.Add(new Batch(namespaceId, 0, 1, 0x02, 0, NodeId, Epoch, 0, 0,
                                      BatchId.From(Guid.NewGuid().ToByteArray()), Zero128,
                                      ComputeRecordsRoot(entries), Zero128, entries));
        }
        foreach (var b in batches)
            b.BatchDigest = ComputeBatchDigest(b.BatchId, b.NamespaceId, b.IssuerNodeId,
                b.IssuerEpoch, b.CursorStart, b.CursorEnd, b.RecordsRoot, b.PrevBatchDigest);
        return batches;
    }

    /// Apply snapshot ops directly (not through cursor machinery).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ApplySnapshotInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int ApplySnapshot(Batch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        int applied;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            ThrowIfDisposed();
            applied = ApplySnapshotInternal(batch);
        }
        RaiseSnapshotApplied(batch.NamespaceId, applied);
        return applied;
    }

    internal int ApplySnapshotInternal(Batch batch)
    {
        ThrowIfDisposed();
        // Snapshot batches use the same canonical records array as DELTA_BATCH.
        // Validate provenance for the complete snapshot before mutating storage.
        var snapshotChangeIds = new HashSet<BatchId>();
        foreach (var entry in batch.Entries)
            if (entry.ChangeId.IsZero || !snapshotChangeIds.Add(entry.ChangeId))
                throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);

        var snapshotRoot = ComputeRecordsRoot(batch.Entries);
        if (batch.RecordsRoot != snapshotRoot)
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        var snapshotDigest = ComputeBatchDigest(batch.BatchId, batch.NamespaceId, batch.IssuerNodeId,
            batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd, batch.RecordsRoot, batch.PrevBatchDigest);
        if (batch.BatchDigest != snapshotDigest)
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);

        ValidateNamespaceId(batch.NamespaceId);
        bool hadNamespace = Storage.TryGetValue(batch.NamespaceId, out var nsStore);
        nsStore ??= new Dictionary<RecordId, Record>();
        var overlays = new Dictionary<RecordId, Record>();
        int applied = 0;

        // Decode and apply into copy-on-write records. A malformed late entry must
        // not leave earlier snapshot entries visible in the live namespace.
        var snapshotTargets = new HashSet<(RecordId RecordId, ushort FieldId)>();
        foreach (var e in batch.Entries)
        {
            var op = Ops.DecodeOp(e.Payload).Op;
            ValidateEntryOperation(e, op, batch.BatchId);
            ushort targetFieldId = op switch
            {
                OpFieldWrite fieldWrite => fieldWrite.FieldId,
                OpFieldTomb fieldTomb when fieldTomb.FieldId != Model.LifecycleFieldId => fieldTomb.FieldId,
                OpRecordTombstone => Model.LifecycleFieldId,
                _ => throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId),
            };
            if (!snapshotTargets.Add((e.RecordId, targetFieldId)))
                throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
            var author = GetAuthor(op);
            if (author is null)
                throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);

            if (!overlays.TryGetValue(e.RecordId, out var rec))
            {
                rec = nsStore.TryGetValue(e.RecordId, out var existing)
                    ? Clone(existing)
                    : new Record(e.RecordId, (ushort)batch.NamespaceId);
                overlays.Add(e.RecordId, rec);
            }

            if (ApplyOpInternal(rec, op, author.Value)) applied++;
        }

        int newRecordCount = 0;
        foreach (var recordId in overlays.Keys)
            if (!nsStore.ContainsKey(recordId)) newRecordCount++;
        Limits?.EnsureCanApplySnapshot(this, newRecordCount);

        // Reserve all destination capacity before publication. For a new namespace,
        // build the complete store off-node and publish it with one dictionary insert;
        // never expose an empty/partially populated namespace if allocation fails.
        if (overlays.Count > 0)
        {
            if (hadNamespace)
            {
                nsStore.EnsureCapacity(nsStore.Count + overlays.Count);
                foreach (var (recordId, record) in overlays)
                    nsStore[recordId] = record;
            }
            else
            {
                var newStore = new Dictionary<RecordId, Record>(overlays.Count);
                foreach (var (recordId, record) in overlays)
                    newStore.Add(recordId, record);
                Storage.EnsureCapacity(Storage.Count + 1);
                Storage.Add(batch.NamespaceId, newStore);
            }
        }
        return applied;
    }

}
