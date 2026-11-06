// Alias maps, session epoch, durable identity (§4 / errata E-1/E-5).
// Partial of Node — extracted from Node.cs (behavior unchanged).

namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    // ------------------------------------------------------------------ aliases
    /// <summary>Take a stable copy of the durable author registry under the metadata gate.</summary>
    internal KeyValuePair<NodeId, int>[] DurableAuthorsSnapshot()
    {
        lock (_lockMeta) return DurableAuthors.ToArray();
    }

    /// §4.4 HELLO carries the sender's incarnation epoch; the receiver stores it as the
    /// per-session watermark used by STEP 0.5 (errata E-2/E-3). A NEW session resets the
    /// peer's watermark first (§4.3: reconnect = fresh exchange), then raises it.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>RegisterSessionEpochInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public void RegisterSessionEpoch(NodeId peer, int epoch)
    {
        lock (_lockMeta) RegisterSessionEpochInternal(peer, epoch);
    }

    internal void RegisterSessionEpochInternal(NodeId peer, int epoch)
    {
        foreach (var k in KnownEpochs.Keys.Where(k => k.Item1 == peer).ToList())
            KnownEpochs.Remove(k);
        KnownEpochs[(peer, 0)] = epoch;
    }

    /// Durable observation of an author's incarnation epoch (§4.4). Used only as fallback
    /// metadata (_author_epoch) — never as a regression gate (errata E-2).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>LearnAuthorEpochInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public void LearnAuthorEpoch(NodeId author, int epoch)
    {
        lock (_lockMeta) LearnAuthorEpochInternal(author, epoch);
    }

    internal void LearnAuthorEpochInternal(NodeId author, int epoch)
    {
        if (author.IsZero) return;
        if (!KnownEpochs.TryGetValue((author, -1), out var cur) || epoch > cur)
            KnownEpochs[(author, -1)] = epoch;
    }

    /// <summary>Return the highest known epoch for an author, if any.</summary>
    public int? KnownAuthorEpoch(NodeId author)
    {
        lock (_lockMeta)
            return KnownEpochs.TryGetValue((author, -1), out var v) ? v : null;
    }

    /// <summary>Return the session epoch for a peer in a namespace, if any.</summary>
    public int? SessionEpoch(NodeId peer, int ns)
    {
        lock (_lockMeta)
        {
            int? e = KnownEpochs.TryGetValue((peer, ns), out var v1) ? v1 : null;
            int? d = KnownEpochs.TryGetValue((peer, 0), out var v0) ? v0 : null;
            if (e is null && d is null) return null;
            return Math.Max(e ?? 0, d ?? 0);
        }
    }

    /// §4.3: ALIAS_MAP belongs to a SESSION — every new connection performs a fresh
    /// exchange, so per-peer maps must be REPLACED (not merged).
    /// BUGFIX (implementation finding): a new session must also drop the per-session
    /// receive-side state of the previous incarnation — stream cursors (PeerStates),
    /// chain checkpoints (LastBatchDigest / LastStreamDigest) and the cached last
    /// accepted stream author. Otherwise a re-handshake against the same nodeId with a
    /// bumped epoch inherits stale cursor/chain expectations and rejects the first batch
    /// of the new incarnation with SEQ_GAP (§4.4, errata E-2/E-3: watermark keyed per
    /// SESSION; reconnect = fresh exchange).
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ResetSessionInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public void ResetSession(NodeId peer)
    {
        lock (_lockMeta) ResetSessionInternal(peer, resetReceiveState: true);
    }

    /// <summary>
    /// Start a fresh alias session while retaining peer replication cursors when the
    /// peer incarnation is unchanged. A changed HELLO epoch invalidates receive-side
    /// cursors and chain checkpoints from the previous incarnation.
    /// </summary>
    public void ResetSession(NodeId peer, int newEpoch)
    {
        lock (_lockMeta)
        {
            bool incarnationChanged =
                KnownEpochs.TryGetValue((peer, 0), out int previousEpoch) &&
                previousEpoch != newEpoch;
            ResetSessionInternal(peer, resetReceiveState: incarnationChanged);
        }
    }

    internal void ResetSessionInternal(NodeId peer) =>
        ResetSessionInternal(peer, resetReceiveState: true);

    internal void ResetSessionInternal(NodeId peer, bool resetReceiveState)
    {
        AliasToNode.Remove(peer);
        NodeToAlias.Remove(peer);
        if (!resetReceiveState) return;

        foreach (var k in PeerStates.Keys.Where(k => k.Item1 == peer).ToList())
            PeerStates.Remove(k);
        foreach (var k in LastBatchDigest.Keys.Where(k => k.Issuer == peer).ToList())
            LastBatchDigest.Remove(k);
        foreach (var k in LastStreamDigest.Keys.Where(k => k.Peer == peer || k.Item1.Issuer == peer).ToList())
            LastStreamDigest.Remove(k);
        foreach (var k in LastStreamAuthor.Keys.Where(k => k.Item1 == peer).ToList())
            LastStreamAuthor.Remove(k);
    }

    /// <summary>
    /// Atomically publish a completed peer session. Validation happens before any
    /// prior session state is changed; reset, epoch registration and alias publication
    /// then run under one metadata gate so readers cannot observe a half-committed session.
    /// </summary>
    internal void CommitSession(NodeId peer, int peerEpoch, IReadOnlyList<AliasMapEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        lock (_lockMeta)
        {
            var aliases = new HashSet<int>();
            var authors = new HashSet<NodeId>();
            bool hasLocal = false;
            bool hasPeer = false;

            foreach (var entry in entries)
            {
                if (entry.Alias <= 0 || entry.NodeId.IsZero || entry.Epoch < 0 ||
                    !aliases.Add(entry.Alias) || !authors.Add(entry.NodeId))
                    throw new FrameException("FRAME_REJECTED");

                if (entry.NodeId == NodeId && entry.Epoch == Epoch) hasLocal = true;
                if (entry.NodeId == peer && entry.Epoch == peerEpoch) hasPeer = true;
            }

            if (!hasLocal || !hasPeer)
                throw new FrameException("FRAME_REJECTED");

            bool incarnationChanged =
                KnownEpochs.TryGetValue((peer, 0), out int previousEpoch) &&
                previousEpoch != peerEpoch;

            ResetSessionInternal(peer, resetReceiveState: incarnationChanged);
            RegisterSessionEpochInternal(peer, peerEpoch);
            foreach (var entry in entries)
                RegisterAliasInternal(peer, entry.Alias, entry.NodeId, entry.Epoch);
        }
    }

    /// Errata E-10 (scenario 23 relay): modelled incremental ALIAS_MAP update (§4.3/§12.7):
    /// register one entry in BOTH peers' copies of the session map under the SAME alias
    /// when possible. Returns the alias valid on both ends, or None when no collision-free
    /// number exists — caller then fails closed with UNKNOWN_ALIAS [А3] instead of guessing.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>EnsureAliasInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int? EnsureAlias(Node peer, NodeId nodeId, int epoch)
    {
        ArgumentNullException.ThrowIfNull(peer);

        // Never block while acquiring another node's metadata gate: two nodes may
        // negotiate aliases in opposite directions while each holds its own gate.
        // A transient conflict is fail-closed (caller may retry on the next exchange).
        lock (_lockMeta)
        {
            if (!Monitor.TryEnter(peer._lockMeta))
                return null;

            try
            {
                return EnsureAliasInternal(peer, nodeId, epoch);
            }
            finally
            {
                Monitor.Exit(peer._lockMeta);
            }
        }
    }

    internal int? EnsureAliasInternal(Node peer, NodeId nodeId, int epoch)
    {
        var fwdA = AliasToNode.TryGetValue(peer.NodeId, out var fa) ? fa : new Dictionary<int, (NodeId Id, int Epoch)>();
        var revA = NodeToAlias.TryGetValue(peer.NodeId, out var ra) ? ra : new Dictionary<(NodeId Id, int Epoch), int>();
        var durable = (nodeId, epoch);
        if (revA.TryGetValue(durable, out var existing)) return existing;
        // is the peer's copy already bound to this author?
        Dictionary<int, (NodeId Id, int Epoch)>? fwdB = peer.AliasToNode.TryGetValue(NodeId, out var ob) ? ob : null;
        if (fwdB is not null)
        {
            foreach (var (al, dur) in fwdB)
            {
                if (dur.Id == nodeId && dur.Item2 == epoch)
                {
                    // peer knows it under `al`; can we mirror the same number?
                    if (!fwdA.ContainsKey(al))
                    {
                        RegisterAlias(peer.NodeId, al, nodeId, epoch);
                        return al;
                    }
                    var cur = fwdA[al];
                    if (cur.Id == nodeId) return al;  // same author, older epoch → rotate
                    return null;                                              // collision — fail closed
                }
            }
        }
        // Errata E-10 follow-up (scenario 23): when BOTH sides already hold a binding for
        // this author but disagree on the alias NUMBER, the canonical number wins
        // deterministically: the LOWER of the two proposals.
        if (fwdB is not null)
        {
            foreach (var (al, dur) in fwdB)
            {
                if (dur.Id == nodeId && !(dur.Item2 == epoch))
                {
                    int? ownAl = null;
                    foreach (var (d2, a2) in revA)
                        if (d2.Id == nodeId) { ownAl = a2; break; }
                    int canon = ownAl is not null ? Math.Min(al, ownAl.Value) : al;
                    // rotate our older binding to the canonical number when free
                    if (ownAl is not null && ownAl.Value != canon && !fwdA.ContainsKey(canon))
                    {
                        var oldDur = revA.FirstOrDefault(kv => kv.Value == ownAl.Value).Key;
                        AliasToNode[peer.NodeId].Remove(ownAl.Value);
                        NodeToAlias[peer.NodeId].Remove(oldDur);
                        AliasToNode[peer.NodeId][canon] = durable;
                        NodeToAlias[peer.NodeId][durable] = canon;
                    }
                    if (NodeToAlias.TryGetValue(peer.NodeId, out var revNow))
                        if (revNow.TryGetValue(durable, out var got)) return got;
                    return ownAl;
                }
            }
        }
        var used = new HashSet<int>(fwdA.Keys);
        if (fwdB is not null) foreach (var k in fwdB.Keys) used.Add(k);
        int nxt = 1;
        while (used.Contains(nxt)) nxt++;
        // BUGFIX (persistence resume): a resumed node (NodePersistence.Load) keeps the E-11
        // registry KEYS but its PeersByNid values are null placeholders — there is no live
        // handle to negotiate with. The two endpoints of the session still hold their OWN
        // durable copies of the shared ALIAS_MAP (§4.3), so before allocating a brand-new
        // number, adopt an existing binding for this author from either copy iff it is
        // collision-free on BOTH ends ([А3] never guess). Without this, every post-restart
        // re-handshake wiped the persisted maps and reallocated aliases 1/2, which broke
        // wire-stability of already-signed outbox batches whose ops carry origin=2
        // (ApplyBatch then failed closed with UNKNOWN_ALIAS).
        if (peer.PeersByNid is null || !peer.PeersByNid.TryGetValue(NodeId, out var liveHandle) || liveHandle is null)
        {
            foreach (var cand in EnumerateCandidateAliases(fwdA, fwdB, nodeId))
            {
                bool freeA = !fwdA.TryGetValue(cand, out var occA) || occA.Id == nodeId;
                bool freeB = fwdB is null || !fwdB.TryGetValue(cand, out var occB) || occB.Id == nodeId;
                if (!freeA || !freeB) continue;   // occupied by another author — [А3] skip
                try
                {
                    if (TryRegisterAliasPairInternal(peer, cand, nodeId, epoch))
                        return cand;
                    // The candidate was not safe on both sides; try a fresh collision-free alias.
                }
                catch (SyncException) { /* reverse-map conflict — fall through to fresh allocation */ }
            }
        }
        if (!TryRegisterAliasPairInternal(peer, nxt, nodeId, epoch))
            return null;
        return nxt;
    }

    /// <summary>
    /// Register a negotiated alias on both ends. If the peer rejects its half,
    /// roll back the local half so the session cannot retain a one-sided binding.
    /// Caller holds this node's metadata gate; RegisterAlias acquires the peer gate.
    /// </summary>
    private bool TryRegisterAliasPairInternal(Node peer, int alias, NodeId nodeId, int epoch)
    {
        bool hadDurable = DurableAuthors.TryGetValue(nodeId, out int priorDurable);
        try
        {
            RegisterAliasInternal(peer.NodeId, alias, nodeId, epoch);
        }
        catch (SyncException)
        {
            RemoveEmptyAliasMapsInternal(peer.NodeId);
            return false;
        }

        try
        {
            peer.RegisterAlias(NodeId, alias, nodeId, epoch);
            return true;
        }
        catch (SyncException)
        {
            // The first registration succeeded, the second did not. Undo only the
            // exact binding inserted above; preserve any pre-existing durable epoch.
            if (AliasToNode.TryGetValue(peer.NodeId, out var fwd) &&
                fwd.TryGetValue(alias, out var binding) &&
                binding.Id == nodeId && binding.Epoch == epoch)
                fwd.Remove(alias);

            if (NodeToAlias.TryGetValue(peer.NodeId, out var rev) &&
                rev.TryGetValue((nodeId, epoch), out var reverseAlias) &&
                reverseAlias == alias)
                rev.Remove((nodeId, epoch));

            if (hadDurable) DurableAuthors[nodeId] = priorDurable;
            else DurableAuthors.Remove(nodeId);
            RemoveEmptyAliasMapsInternal(peer.NodeId);

            // RegisterAliasInternal on the peer creates maps before validating
            // conflicts. Remove only those now-empty containers, under its own gate.
            lock (peer._lockMeta)
                peer.RemoveEmptyAliasMapsInternal(NodeId);
            return false;
        }
        catch
        {
            // Unexpected failures must also leave no one-sided alias binding.
            if (AliasToNode.TryGetValue(peer.NodeId, out var fwd) &&
                fwd.TryGetValue(alias, out var binding) &&
                binding.Id == nodeId && binding.Epoch == epoch)
                fwd.Remove(alias);

            if (NodeToAlias.TryGetValue(peer.NodeId, out var rev) &&
                rev.TryGetValue((nodeId, epoch), out var reverseAlias) &&
                reverseAlias == alias)
                rev.Remove((nodeId, epoch));

            if (hadDurable) DurableAuthors[nodeId] = priorDurable;
            else DurableAuthors.Remove(nodeId);
            RemoveEmptyAliasMapsInternal(peer.NodeId);
            lock (peer._lockMeta)
                peer.RemoveEmptyAliasMapsInternal(NodeId);
            throw;
        }
    }

    private void RemoveEmptyAliasMapsInternal(NodeId peer)
    {
        if (AliasToNode.TryGetValue(peer, out var fwd) && fwd.Count == 0)
            AliasToNode.Remove(peer);
        if (NodeToAlias.TryGetValue(peer, out var rev) && rev.Count == 0)
            NodeToAlias.Remove(peer);
    }

    private static IEnumerable<int> EnumerateCandidateAliases(
        Dictionary<int, (NodeId Id, int Epoch)> fwdA, Dictionary<int, (NodeId Id, int Epoch)>? fwdB, byte[] nodeId)
    {
        var seen = new HashSet<int>();
        foreach (var (al, dur) in fwdA)
            if (dur.Id == nodeId && seen.Add(al)) yield return al;
        if (fwdB is not null)
            foreach (var (al, dur) in fwdB)
                if (dur.Id == nodeId && seen.Add(al)) yield return al;
    }

    /// ALIAS_MAP entry for session with `peer` (§4.3). Alias exists only in session.
    /// BUGFIX (errata, Python port): reverse map keyed by the DURABLE pair (nodeId, epoch)
    /// — §4.4 lets a post-crash node keep its nodeId and bump only the epoch.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>RegisterAliasInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public void RegisterAlias(NodeId peer, int alias, NodeId nodeId, int epoch)
    {
        lock (_lockMeta) RegisterAliasInternal(peer, alias, nodeId, epoch);
    }

    internal void RegisterAliasInternal(NodeId peer, int alias, NodeId nodeId, int epoch)
    {
        var m = AliasToNode.TryGetValue(peer, out var mm) ? mm : (AliasToNode[peer] = new());
        // BUGFIX (port): inner reverse map MUST use the BytesIntEq content comparer — the default
        // tuple comparer falls back to byte[] REFERENCE equality for nodeId, so re-creating
        // a decoded copy of the same author bytes produced fresh array instances that never
        // matched existing keys (AliasOf misses → spurious UNKNOWN_ALIAS / stale bindings).
        var rev = NodeToAlias.TryGetValue(peer, out var rr) ? rr : (NodeToAlias[peer] = new());
        var durable = (nodeId, epoch);
        // forbid reuse of an alias for another (nodeId, epoch) within a session
        if (m.TryGetValue(alias, out var occ) && !(occ.Id == nodeId && occ.Item2 == epoch))
            throw new SyncException("UNKNOWN_ALIAS");
        if (rev.TryGetValue(durable, out var occAl) && occAl != alias)
            throw new SyncException("UNKNOWN_ALIAS");
        m[alias] = durable;
        rev[durable] = alias;
        // Errata E-9: remember the binding DURABLY (survives reset_session).
        if (!DurableAuthors.TryGetValue(nodeId, out var cur) || epoch > cur)
            DurableAuthors[nodeId] = epoch;
    }


    /// <summary>
    /// Apply a complete session alias map atomically. Validate every forward and reverse
    /// binding before mutating either map or the durable-author registry.
    /// </summary>
    internal void ApplyAliasMapAtomic(NodeId peer, IReadOnlyList<AliasMapEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        lock (_lockMeta)
        {
            var aliases = new HashSet<int>();
            var authors = new HashSet<NodeId>();
            var fwd = AliasToNode.TryGetValue(peer, out var currentFwd)
                ? currentFwd : null;
            var rev = NodeToAlias.TryGetValue(peer, out var currentRev)
                ? currentRev : null;

            foreach (var entry in entries)
            {
                int alias = entry.Alias;
                var durable = (entry.NodeId, entry.Epoch);
                if (alias <= 0 || entry.NodeId.IsZero || entry.Epoch < 0 ||
                    !aliases.Add(alias) || !authors.Add(entry.NodeId))
                    throw new FrameException("FRAME_REJECTED");

                if (fwd is not null && fwd.TryGetValue(alias, out var owner) &&
                    (owner.Id != entry.NodeId || owner.Epoch != entry.Epoch))
                    throw new SyncException("UNKNOWN_ALIAS");

                if (rev is not null && rev.TryGetValue(durable, out int existingAlias) &&
                    existingAlias != alias)
                    throw new SyncException("UNKNOWN_ALIAS");
            }

            // No operation below can encounter a binding conflict: the complete set
            // was checked against both maps while holding the same metadata gate.
            foreach (var entry in entries)
                RegisterAliasInternal(peer, entry.Alias, entry.NodeId, entry.Epoch);
        }
    }

    /// Resolve a wire alias to the DURABLE author identity.
    /// SPEC CORRECTION (errata E-1): the durable author is the bare b128 nodeId (§4.6/§5.4).
    /// Errata E-12: on a miss, consult the peer's copy of THIS session and adopt its
    /// binding iff collision-free; an identity neither copy knows still fails closed [А3].
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>ResolveAliasInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public NodeId ResolveAlias(NodeId peer, int alias)
    {
        lock (_lockMeta) return ResolveAliasInternal(peer, alias);
    }

    internal NodeId ResolveAliasInternal(NodeId peer, int alias)
    {
        var entry = AliasToNode.TryGetValue(peer, out var m) && m.TryGetValue(alias, out var e) ? e : (default, int.MinValue);
        bool have = entry.Item2 != int.MinValue;
        if (!have)
        {
            Node? other = null;
            bool attached = false;
            if (PeersByNid is not null && PeersByNid.TryGetValue(peer, out var o))
            {
                other = o;
                attached = o is not null;
            }
            if (other is not null)
            {
                var theirFwd = other.AliasToNode.TryGetValue(NodeId, out var tf) ? tf : new Dictionary<int, (NodeId Id, int Epoch)>();
                if (theirFwd.TryGetValue(alias, out var durThere))
                {
                    try { RegisterAlias(peer, alias, durThere.Id, durThere.Epoch); return durThere.Id; } // E-1: bare nodeId
                    catch (SyncException) { throw new SyncException("UNKNOWN_ALIAS"); } // genuine conflict — never guess
                }
                // BUGFIX (persistence resume, symmetric E-12): the wire alias is missing from
                // OUR copy of the session map (a resumed node can lose per-peer maps while
                // already-signed outbox batches still carry those aliases). The sender just
                // resolved the SAME alias successfully in ITS own copy — consult the sender's
                // forward map for this session and adopt its binding iff collision-free.
                // §4.3 makes both copies of a session table identical, so this is knowledge
                // exchange, not guessing [А3]; an alias unknown on BOTH ends still fails closed.
                if (attached)
                {
                    var theirMapForUs = other.AliasToNode.TryGetValue(NodeId, out var tm) ? tm : null;
                    if (theirMapForUs is not null && theirMapForUs.TryGetValue(alias, out var durTheir))
                    {
                        try { RegisterAlias(peer, alias, durTheir.Id, durTheir.Epoch); return durTheir.Id; }
                        catch (SyncException) { /* real conflict — fail closed below */ }
                    }
                }
            }
            throw new SyncException("UNKNOWN_ALIAS");  // [А3]/[И3] never guess
        }
        return entry.Id.IsZero && entry.Epoch == int.MinValue ? throw new SyncException("UNKNOWN_ALIAS") : entry.Id; // E-1: bare nodeId
    }

    /// Canonical durable author identity: nodeId || epoch(u32 LE), 20 bytes.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>DurableIdInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public byte[] DurableId(NodeId? nodeId = null, int? epoch = null)
    {
        lock (_lockMeta) return DurableIdInternal(nodeId, epoch);
    }

    /// <summary>Write the 20-byte durable identity (nodeId || epoch u32 LE) into <paramref name="dest"/>.</summary>
    public void WriteDurableId(Span<byte> dest, NodeId? nodeId = null, int? epoch = null)
    {
        if (dest.Length < 20) throw new ArgumentException("need 20 bytes", nameof(dest));
        lock (_lockMeta)
        {
            var n = nodeId ?? NodeId;
            var e = epoch ?? Epoch;
            n.WriteTo(dest[..16]);
            BitConverter.TryWriteBytes(dest[16..], (uint)e);
        }
    }

    internal byte[] DurableIdInternal(NodeId? nodeId = null, int? epoch = null)
    {
        var r = new byte[20];
        WriteDurableId(r, nodeId, epoch);
        return r;
    }

    /// Reverse lookup keyed by the bare durable nodeId (errata E-1), with the collision
    /// guard of errata E-5 (see Python port docstring): ambiguous alias → None, never guess.
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>AliasOfInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    public int? AliasOf(NodeId peer, NodeId nodeId, int? epoch = null)
    {
        lock (_lockMeta) return AliasOfInternal(peer, nodeId, epoch);
    }

    internal int? AliasOfInternal(NodeId peer, NodeId nodeId, int? epoch = null)
    {
        // BUGFIX (port): the fallback map MUST use the byte-content comparer too — with the
        // default ReferenceEqualityComparer an empty miss-map behaved inconsistently.
        if (!NodeToAlias.TryGetValue(peer, out var rev)) rev = EmptyRevAlias;
        // SPEC CORRECTION (errata E-1, Python port `alias_of`): the `epoch` argument is kept
        // for API compatibility ONLY; identity per §4.6 is the bare nodeId. The previous port
        // short-circuited on the exact (nodeId, epoch) pair, which made a stale registration
        // Nid(6)@epoch-1 win over the live session binding Nid(6)@epoch-5 — the reference
        // ignores `epoch` entirely and lets first-registration-wins + [А3] collision guard decide.
        // Errata E-1/E-5 (Python port `alias_of`): first registration wins among entries
        // sharing the nodeId; then the collision guard checks whether THAT alias belongs
        // to a different durable author in the forward map → None (never guess [А3]).
        // BUGFIX (port): the previous variant collected ALL candidate aliases and rejected
        // each against the forward map, which wrongly failed closed when the wanted nodeId
        // itself legitimately owns the colliding slot (e.g. two authors registered into
        // the same alias — the second registration is refused upstream, so here alias 50
        // resolves to Nid(5)'s binding, and Nid(6) has no binding at all → null).
        int? found = null;
        foreach (var (dur, a) in rev)
            if (dur.Id == nodeId && found is null) found = a;
        if (found is null) return null;
        if (!AliasToNode.TryGetValue(peer, out var fwd)) fwd = EmptyAliasMap;
        // collision check: does this alias belong to a DIFFERENT author in the forward map?
        if (fwd.TryGetValue(found.Value, out var owner) && !(owner.Id == nodeId))
            return null;      // ambiguous alias — never guess [А3]
        return found;
    }

    // ------------------------------------------------------------------- local writes
    // BUGFIX (port): inner record maps MUST use byte-content equality (BytesEq). The default
    // ReferenceEqualityComparer made every recordId lookup a reference miss — the exact
    // cause of the KeyNotFoundException storm in tests that read node.Storage[ns][rid].
    /// <summary>Thread-safe entry point (production hardening): takes the node gate
    /// and delegates to <c>NsInternal</c>. See <see cref="EnterAllGates"/>.</summary>
    internal Dictionary<RecordId, Record> Ns(int namespaceId)
    {
        lock (_lockStorage) return NsInternal(namespaceId);
    }

    internal Dictionary<RecordId, Record> NsInternal(int namespaceId)
    { return Storage.TryGetValue(namespaceId, out var s) ? s : (Storage[namespaceId] = new Dictionary<RecordId, Record>()); }

}
