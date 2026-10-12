// Node engine: storage, outbox/inbox, alias maps, batches, applyBatch (§13.4),
// relay with alias translation (§13.6), snapshots (§12.18), echo suppression [E2],
// idempotency-before-chain-check [S7].
// Port of binsync/node.py — all SPEC HARDENING / errata notes preserved.
using Blake3;

namespace BinSync;

public class SyncException : Exception
{
    public string CodeName { get; }
    public ushort Code => Frames.ErrorCodes.TryGetValue(CodeName, out var c) ? c : (ushort)0;
    public byte[]? BatchId { get; }
    public SyncException(string codeName, byte[]? batchId = null) : base(codeName)
    { CodeName = codeName; BatchId = batchId; }
}

public sealed class OutboxEntry
{
    public int NamespaceId;
    public long Seq;               // monotonic per (authorNodeId, namespaceId) [П1]
    public byte[] ChangeId = Array.Empty<byte>();   // b128 idempotency key
    public byte[] RecordId = Array.Empty<byte>();
    public byte OpKind;
    public byte[] Payload = Array.Empty<byte>();    // canonical encoded op (compact mode)
    public byte[] AuthorNodeId = Array.Empty<byte>();
    public long CreatedAt;
    public List<byte[]> CausalDeps = new();
}

public sealed class Batch
{
    public int NamespaceId, SchemaId, SchemaVersion;
    public int MergePolicy, Flags;
    public byte[] IssuerNodeId = Array.Empty<byte>();
    public int IssuerEpoch;
    public long CursorStart, CursorEnd;
    public byte[] BatchId = Node.Zero128, PrevBatchDigest = Node.Zero128;
    public byte[] RecordsRoot = Node.Zero128, BatchDigest = Node.Zero128;
    public List<OutboxEntry> Entries = new();      // decoded ops kept for retransmission/ACK
    /// Errata E-4: durable author of this batch, resolved on the receiver at apply time.
    public byte[]? AuthorStreamNodeId = null;

    public Batch() { }
    public Batch(int ns, int schemaId, int schemaVer, int policy, int flags,
                 byte[] issuer, int issuerEpoch, long cstart, long cend,
                 byte[] batchId, byte[] prev, byte[] root, byte[] digest,
                 List<OutboxEntry> entries)
    {
        NamespaceId = ns; SchemaId = schemaId; SchemaVersion = schemaVer;
        MergePolicy = policy; Flags = flags; IssuerNodeId = issuer; IssuerEpoch = issuerEpoch;
        CursorStart = cstart; CursorEnd = cend; BatchId = batchId; PrevBatchDigest = prev;
        RecordsRoot = root; BatchDigest = digest; Entries = entries;
    }

    public byte[] EncodeRecords() => Node.EncodeRecordsArray(Entries);

    public List<object> Ops()
    {
        var outp = new List<object>();
        foreach (var e in Entries) outp.Add(BinSync.Ops.DecodeOp(e.Payload).Op);
        return outp;
    }

    /// §13.6.1 [Р3]: a batch carries exactly one author — the stream key.
    public byte[] AuthorStream() =>
        Entries.Count > 0 ? Entries[0].AuthorNodeId : Node.Zero128;

    /// Errata E-4: on the RECEIVER entries carry the ISSUER's nodeId as placeholder
    /// author; the durable author is resolved from op.origin at apply time and cached
    /// in AuthorStreamNodeId. On the SENDER side the per-author stream key is authoritative.
    public byte[] DurableAuthor => AuthorStreamNodeId ?? AuthorStream();
}

public sealed class PeerState
{
    public byte[] PeerNodeId;
    public int NamespaceId;
    public long LastPushedSeq = 0;
    public long LastReceivedSeq = 0;
    // SPEC CORRECTION (errata E-3): receive cursor keyed per SESSION stream
    // (senderPeer, senderEpoch, ns, authorStream) — see Python port for full rationale.
    public long LastAckedSeq = 0;
    public readonly Dictionary<(byte[], int, int, byte[]), long> ReceivedStreams = new(new BA4());
    // SPEC CORRECTION (errata E-4): push watermark keyed by the AUTHOR STREAM.
    public readonly Dictionary<byte[], long> StreamAcked = new(new BA());
    public bool InitialSyncComplete = false;
    public int MembershipEpoch = 0;
    public long LastSeen = 0;
    // Deterministic batchId cache keyed by (stream, cursorStart, cursorEnd) (§10.2.2 replay).
    internal readonly Dictionary<(byte[], long, long), (byte[] Id, byte[] Prev)> Ids = new(new BAT());

    public PeerState(byte[] peer, int ns) { PeerNodeId = peer; NamespaceId = ns; }
    public long AckedFor(byte[] author) => StreamAcked.TryGetValue(author, out var v) ? v : 0;
}

internal sealed class BA : IEqualityComparer<byte[]>
{
    public bool Equals(byte[]? x, byte[]? y) => x is null ? y is null : y is not null && x.AsSpan().SequenceEqual(y);
    public int GetHashCode(byte[] t) { unchecked { int h = 17; foreach (byte b in t) h = h * 31 + b; return h; } }
}

internal sealed class BAT : IEqualityComparer<(byte[], long, long)>
{
    public bool Equals((byte[], long, long) x, (byte[], long, long) y) =>
        new BA().Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    public int GetHashCode((byte[], long, long) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), t.Item2.GetHashCode(), t.Item3.GetHashCode());
}

internal sealed class BA4 : IEqualityComparer<(byte[], int, int, byte[])>
{
    public bool Equals((byte[], int, int, byte[]) x, (byte[], int, int, byte[]) y) =>
        new BA().Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3 && new BA().Equals(x.Item4, y.Item4);
    public int GetHashCode((byte[], int, int, byte[]) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), t.Item2, t.Item3, new BA().GetHashCode(t.Item4));
}

internal sealed class BA2 : IEqualityComparer<(byte[], byte[])>
{
    public bool Equals((byte[], byte[]) x, (byte[], byte[]) y) =>
        new BA().Equals(x.Item1, y.Item1) && new BA().Equals(x.Item2, y.Item2);
    public int GetHashCode((byte[], byte[]) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), new BA().GetHashCode(t.Item2));
}

// Mixed-key comparers for KnownEpochs: keys are (peer|author, ns|-1|0) — the second
// component is an INT discriminator, so the tuple comparer must be (bytes, int).
internal sealed class BAI2 : IEqualityComparer<(byte[], int)>
{
    public bool Equals((byte[], int) x, (byte[], int) y) =>
        new BA().Equals(x.Item1, y.Item1) && x.Item2 == y.Item2;
    public int GetHashCode((byte[], int) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), t.Item2);
}

internal sealed class BAI3 : IEqualityComparer<(byte[], int, int)>
{
    public bool Equals((byte[], int, int) x, (byte[], int, int) y) =>
        new BA().Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    public int GetHashCode((byte[], int, int) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), t.Item2, t.Item3);
}

internal sealed class BA2I2 : IEqualityComparer<((byte[], int), (byte[], int))>
{
    static readonly BAI2 C = new();
    public bool Equals(((byte[], int), (byte[], int)) x, ((byte[], int), (byte[], int)) y) =>
        C.Equals(x.Item1, y.Item1) && C.Equals(x.Item2, y.Item2);
    public int GetHashCode(((byte[], int), (byte[], int)) t) =>
        HashCode.Combine(C.GetHashCode(t.Item1), C.GetHashCode(t.Item2));
}

internal sealed class BA3 : IEqualityComparer<(byte[], byte[], byte[])>
{
    public bool Equals((byte[], byte[], byte[]) x, (byte[], byte[], byte[]) y) =>
        new BA().Equals(x.Item1, y.Item1) && new BA().Equals(x.Item2, y.Item2) && new BA().Equals(x.Item3, y.Item3);
    public int GetHashCode((byte[], byte[], byte[]) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), new BA().GetHashCode(t.Item2), new BA().GetHashCode(t.Item3));
}

internal sealed class BAI4 : IEqualityComparer<(byte[], int, int)>
{
    public bool Equals((byte[], int, int) x, (byte[], int, int) y) =>
        new BA().Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3;
    public int GetHashCode((byte[], int, int) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), t.Item2, t.Item3);
}

internal sealed class BAI4B : IEqualityComparer<((byte[], int, int), byte[])>
{
    public bool Equals(((byte[], int, int), byte[]) x, ((byte[], int, int), byte[]) y) =>
        new BAI4().Equals(x.Item1, y.Item1) && new BA().Equals(x.Item2, y.Item2);
    public int GetHashCode(((byte[], int, int), byte[]) t) =>
        HashCode.Combine(new BAI4().GetHashCode(t.Item1), new BA().GetHashCode(t.Item2));
}

internal sealed class BAI5 : IEqualityComparer<(byte[], int, int, byte[])>
{
    public bool Equals((byte[], int, int, byte[]) x, (byte[], int, int, byte[]) y) =>
        new BA().Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && x.Item3 == y.Item3 && new BA().Equals(x.Item4, y.Item4);
    public int GetHashCode((byte[], int, int, byte[]) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), t.Item2, t.Item3, new BA().GetHashCode(t.Item4));
}

internal sealed class BAPairBytes : IEqualityComparer<(byte[], byte[])>
{
    public bool Equals((byte[], byte[]) x, (byte[], byte[]) y) =>
        new BA().Equals(x.Item1, y.Item1) && new BA().Equals(x.Item2, y.Item2);
    public int GetHashCode((byte[], byte[]) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), new BA().GetHashCode(t.Item2));
}

internal sealed class BAIntBytesInt : IEqualityComparer<(int, byte[], int)>
{
    public bool Equals((int, byte[], int) x, (int, byte[], int) y) =>
        x.Item1 == y.Item1 && new BA().Equals(x.Item2, y.Item2) && x.Item3 == y.Item3;
    public int GetHashCode((int, byte[], int) t) =>
        HashCode.Combine(t.Item1, new BA().GetHashCode(t.Item2), t.Item3);
}

public sealed class SeenKeyComparer : IEqualityComparer<(byte[], int, byte[])>
{
    public bool Equals((byte[], int, byte[]) x, (byte[], int, byte[]) y) =>
        new BA().Equals(x.Item1, y.Item1) && x.Item2 == y.Item2 && new BA().Equals(x.Item3, y.Item3);
    public int GetHashCode((byte[], int, byte[]) t) =>
        HashCode.Combine(new BA().GetHashCode(t.Item1), t.Item2, new BA().GetHashCode(t.Item3));
}

public sealed class Node
{
    public static readonly byte[] Zero128 = new byte[16];

    public byte[] NodeId;
    public int Epoch;
    // BUGFIX (implementation finding): the HLC must start cold (§5.2) — see Python port note.
    public HlcClock Clock;
    // storage: namespaceId -> recordId -> Record
    public Dictionary<int, Dictionary<byte[], Record>> Storage = new();
    // outbox: namespaceId -> list[OutboxEntry] (per-author seq counters inside)
    public Dictionary<int, List<OutboxEntry>> Outbox = new();
    internal readonly Dictionary<(byte[], int), long> _outboxSeq = new(new BAI2());   // (author, ns) -> seq
    // inbox idempotency: set of (issuerNodeId, ns, issuerEpoch, batchId)
    public readonly HashSet<(byte[], int, int, byte[])> Inbox = new(new BAI5());
    // batch digest chain checkpoints: (issuerNodeId, ns, epoch) -> last batchDigest
    public readonly Dictionary<(byte[], int, int), byte[]> LastBatchDigest = new(new BAI4());
    // BUGFIX: per-STREAM chain for the ISSUER side: ((issuer, ns, epoch), targetPeer)
    // -> last sent batchDigest (§12.17.2 hardening).
    public readonly Dictionary<((byte[], int, int), byte[]), byte[]> LastStreamDigest = new(new BAI4B());
    // alias maps per session peer: peerNodeId -> {alias -> (nodeId, epoch)} and reverse
    public Dictionary<byte[], Dictionary<int, (byte[], int)>> AliasToNode = new(new BA());
    public Dictionary<byte[], Dictionary<(byte[], int), int>> NodeToAlias = new(new BA());
    // Errata E-9 (transport): DURABLE author knowledge — every (nodeId, epoch) ever
    // resolved through ANY ALIAS_MAP. Used ONLY for session-table construction, never
    // for resolution [А3].
    public Dictionary<byte[], int> DurableAuthors = new(new BA());   // nodeId -> max known epoch
    public Dictionary<(byte[], int), PeerState> PeerStates = new(new BAI2());
    // Errata E-10 (scenario 23): provenance seen-set — (senderPeer, ns, changeId)
    // (E-13 follow-up 2: channel-scoped). Relay eligibility decided by FIRST sighting.
    public readonly HashSet<(byte[], int, byte[])> SeenChanges = new(new SeenKeyComparer());
    // Errata E-11: per-(senderPeer, ns) cache of the durable author stream last accepted.
    public readonly Dictionary<(byte[], int), byte[]> LastStreamAuthor = new(new BAI2());
    // known_epochs: mixed keys — (peer, ns) session watermarks, (author, -1) durable
    // author epochs, (peer, 0) HELLO epoch. See register_session_epoch / learn_author_epoch.
    public Dictionary<(byte[], int), int> KnownEpochs = new(new BAI2());
    public readonly HashSet<byte[]> Quarantined = new(new BA());       // zombie nodes requiring resync
    public List<string> ErrorsOut = new();            // observable error log for tests
    public List<byte[]> AcksOut = new();              // ACKed batchIds
    public readonly HashSet<int> ForbiddenNamespaces = new();
    public Dictionary<int, int> SchemaVersions = new();  // namespaceId -> supported schemaVersion
    // Harness handle to peer Node objects (live connection handles in a real stack).
    public Dictionary<byte[], Node>? PeersByNid = null;

    public Node(byte[]? nodeId = null, int epoch = 1, Func<long>? nowFn = null,
                long maxSkewUs = 60_000_000)
    {
        NodeId = nodeId ?? Guid.NewGuid().ToByteArray();
        if (NodeId.Length != 16) throw new InvalidOperationException("nodeId must be 16 bytes");
        Epoch = epoch;
        Clock = new HlcClock(nowFn, maxSkewUs);
    }

    // ------------------------------------------------------------------ aliases
    /// §4.4 HELLO carries the sender's incarnation epoch; the receiver stores it as the
    /// per-session watermark used by STEP 0.5 (errata E-2/E-3). A NEW session resets the
    /// peer's watermark first (§4.3: reconnect = fresh exchange), then raises it.
    public void RegisterSessionEpoch(byte[] peer, int epoch)
    {
        foreach (var k in KnownEpochs.Keys.Where(k => k.Item1.AsSpan().SequenceEqual(peer)).ToList())
            KnownEpochs.Remove(k);
        KnownEpochs[(peer, 0)] = epoch;
    }

    /// Durable observation of an author's incarnation epoch (§4.4). Used only as fallback
    /// metadata (_author_epoch) — never as a regression gate (errata E-2).
    public void LearnAuthorEpoch(byte[] author, int epoch)
    {
        if (author.Length != 16) return;
        if (!KnownEpochs.TryGetValue((author, -1), out var cur) || epoch > cur)
            KnownEpochs[(author, -1)] = epoch;
    }

    public int? KnownAuthorEpoch(byte[] author) =>
        KnownEpochs.TryGetValue((author, -1), out var v) ? v : null;

    public int? SessionEpoch(byte[] peer, int ns)
    {
        int? e = KnownEpochs.TryGetValue((peer, ns), out var v1) ? v1 : null;
        int? d = KnownEpochs.TryGetValue((peer, 0), out var v0) ? v0 : null;
        if (e is null && d is null) return null;
        return Math.Max(e ?? 0, d ?? 0);
    }

    /// §4.3: ALIAS_MAP belongs to a SESSION — every new connection performs a fresh
    /// exchange, so per-peer maps must be REPLACED (not merged).
    public void ResetSession(byte[] peer)
    {
        AliasToNode.Remove(peer);
        NodeToAlias.Remove(peer);
    }

    /// Errata E-10 (scenario 23 relay): modelled incremental ALIAS_MAP update (§4.3/§12.7):
    /// register one entry in BOTH peers' copies of the session map under the SAME alias
    /// when possible. Returns the alias valid on both ends, or None when no collision-free
    /// number exists — caller then fails closed with UNKNOWN_ALIAS [А3] instead of guessing.
    public int? EnsureAlias(Node peer, byte[] nodeId, int epoch)
    {
        var fwdA = AliasToNode.TryGetValue(peer.NodeId, out var fa) ? fa : new Dictionary<int, (byte[], int)>();
        var revA = NodeToAlias.TryGetValue(peer.NodeId, out var ra) ? ra : new Dictionary<(byte[], int), int>();
        var durable = (nodeId, epoch);
        if (revA.TryGetValue(durable, out var existing)) return existing;
        // is the peer's copy already bound to this author?
        Dictionary<int, (byte[], int)>? fwdB = peer.AliasToNode.TryGetValue(NodeId, out var ob) ? ob : null;
        if (fwdB is not null)
        {
            foreach (var (al, dur) in fwdB)
            {
                if (dur.Item1.AsSpan().SequenceEqual(nodeId) && dur.Item2 == epoch)
                {
                    // peer knows it under `al`; can we mirror the same number?
                    if (!fwdA.ContainsKey(al))
                    {
                        RegisterAlias(peer.NodeId, al, nodeId, epoch);
                        return al;
                    }
                    var cur = fwdA[al];
                    if (cur.Item1.AsSpan().SequenceEqual(nodeId)) return al;  // same author, older epoch → rotate
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
                if (dur.Item1.AsSpan().SequenceEqual(nodeId) && !(dur.Item2 == epoch))
                {
                    int? ownAl = null;
                    foreach (var (d2, a2) in revA)
                        if (d2.Item1.AsSpan().SequenceEqual(nodeId)) { ownAl = a2; break; }
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
        try
        {
            RegisterAlias(peer.NodeId, nxt, nodeId, epoch);
            peer.RegisterAlias(NodeId, nxt, nodeId, epoch);
        }
        catch (SyncException) { return null; }
        return nxt;
    }

    /// ALIAS_MAP entry for session with `peer` (§4.3). Alias exists only in session.
    /// BUGFIX (errata, Python port): reverse map keyed by the DURABLE pair (nodeId, epoch)
    /// — §4.4 lets a post-crash node keep its nodeId and bump only the epoch.
    public void RegisterAlias(byte[] peer, int alias, byte[] nodeId, int epoch)
    {
        var m = AliasToNode.TryGetValue(peer, out var mm) ? mm : (AliasToNode[peer] = new()) ;
        var rev = NodeToAlias.TryGetValue(peer, out var rr) ? rr : (NodeToAlias[peer] = new());
        var durable = (nodeId, epoch);
        // forbid reuse of an alias for another (nodeId, epoch) within a session
        if (m.TryGetValue(alias, out var occ) && !(occ.Item1.AsSpan().SequenceEqual(nodeId) && occ.Item2 == epoch))
            throw new SyncException("UNKNOWN_ALIAS");
        if (rev.TryGetValue(durable, out var occAl) && occAl != alias)
            throw new SyncException("UNKNOWN_ALIAS");
        m[alias] = durable;
        rev[durable] = alias;
        // Errata E-9: remember the binding DURABLY (survives reset_session).
        if (!DurableAuthors.TryGetValue(nodeId, out var cur) || epoch > cur)
            DurableAuthors[nodeId] = epoch;
    }

    /// Resolve a wire alias to the DURABLE author identity.
    /// SPEC CORRECTION (errata E-1): the durable author is the bare b128 nodeId (§4.6/§5.4).
    /// Errata E-12: on a miss, consult the peer's copy of THIS session and adopt its
    /// binding iff collision-free; an identity neither copy knows still fails closed [А3].
    public byte[] ResolveAlias(byte[] peer, int alias)
    {
        var entry = AliasToNode.TryGetValue(peer, out var m) && m.TryGetValue(alias, out var e) ? e : ((byte[])null!, int.MinValue);
        bool have = !(entry.Item1 is null) || entry.Item2 != int.MinValue;
        if (!have)
        {
            var other = PeersByNid is not null && PeersByNid.TryGetValue(peer, out var o) ? o : null;
            if (other is not null)
            {
                var theirFwd = other.AliasToNode.TryGetValue(NodeId, out var tf) ? tf : new Dictionary<int, (byte[], int)>();
                if (theirFwd.TryGetValue(alias, out var durThere))
                {
                    try { RegisterAlias(peer, alias, durThere.Item1, durThere.Item2); return durThere.Item1; } // E-1: bare nodeId
                    catch (SyncException) { throw new SyncException("UNKNOWN_ALIAS"); } // genuine conflict — never guess
                }
            }
            throw new SyncException("UNKNOWN_ALIAS");  // [А3]/[И3] never guess
        }
        return entry.Item1;   // SPEC CORRECTION (errata E-1): bare nodeId
    }

    /// Canonical durable author identity: nodeId || epoch(u32 LE), 20 bytes.
    public byte[] DurableId(byte[]? nodeId = null, int? epoch = null)
    {
        var n = nodeId ?? NodeId;
        var e = epoch ?? Epoch;
        if (n.Length != 16) return n;   // tolerate an already-pair identity passed in
        var r = new byte[20];
        Array.Copy(n, r, 16);
        BitConverter.GetBytes((uint)e).CopyTo(r, 16);
        return r;
    }

    /// Reverse lookup keyed by the bare durable nodeId (errata E-1), with the collision
    /// guard of errata E-5 (see Python port docstring): ambiguous alias → None, never guess.
    public int? AliasOf(byte[] peer, byte[] nodeId, int? epoch = null)
    {
        var rev = NodeToAlias.TryGetValue(peer, out var r) ? r : new Dictionary<(byte[], int), int>();
        if (epoch is not null && rev.TryGetValue((nodeId, epoch.Value), out var exact)) return exact;
        int? found = null;
        foreach (var (dur, a) in rev)
            if (dur.Item1.AsSpan().SequenceEqual(nodeId)) { found ??= a; }
        if (found is null) return null;
        // collision check: does this alias belong to a DIFFERENT author in the forward map?
        var fwd = AliasToNode.TryGetValue(peer, out var f) ? f : new Dictionary<int, (byte[], int)>();
        if (fwd.TryGetValue(found.Value, out var owner) &&
            !owner.Item1.AsSpan().SequenceEqual(nodeId))
            return null;      // ambiguous alias — never guess [А3]
        return found;
    }

    // ------------------------------------------------------------------- local writes
    // BUGFIX (port): inner record maps MUST use byte-content equality (BA). The default
    // ReferenceEqualityComparer made every recordId lookup a reference miss — the exact
    // cause of the KeyNotFoundException storm in tests that read node.Storage[ns][rid].
    public Dictionary<byte[], Record> Ns(int namespaceId) =>
        Storage.TryGetValue(namespaceId, out var s) ? s : (Storage[namespaceId] = new(new BA()));

    /// §10.1: `seq` is monotonic per (authorNodeId, namespaceId).
    public long NextOutboxSeq(byte[] author, int namespaceId)
    {
        var key = (author, namespaceId);
        long s = (_outboxSeq.TryGetValue(key, out var v) ? v : 0) + 1;
        _outboxSeq[key] = s;
        return s;
    }

    /// Current durable (wall, logical, author, value|"TOMB") of one field, straight from storage.
    public (ulong Wall, uint Logical, byte[] Author, object Val)? SeenState(int ns, byte[] recordId, int fieldId)
    {
        if (!Storage.TryGetValue(ns, out var store)) return null;
        if (!store.TryGetValue(recordId, out var rec)) return null;
        FieldState? fs = rec.State.TryGetValue((ushort)fieldId, out var w) ? w :
                         (rec.FieldTombs.TryGetValue((ushort)fieldId, out var t) ? t : null);
        if (fs is null) return null;
        bool tomb = rec.FieldTombs.ContainsKey((ushort)fieldId);
        return (fs.Timestamp.Wall, fs.Timestamp.Logical, fs.AuthorNodeId, tomb ? "TOMB" : fs.Value);
    }

    /// Append a LOCAL write to outbox ONLY if merge reported changed (echo [E2]).
    /// Errata E-13 (scenario 37): restored-echo suppression — see Python port docstring.
    public OutboxEntry? PushOutbox(int ns, byte[] recordId, object op, byte[] author, byte[]? peer = null)
    {
        if (!author.AsSpan().SequenceEqual(NodeId))
            throw new InvalidOperationException("local push only; relay path enqueues raw payloads");
        // Errata E-13 follow-up 3: suppress ONLY when OUR OWN queued rival carried
        // byte-identical durable state to what this write re-established [Ш2].
        ushort? fid = op switch { OpFieldWrite fw => fw.FieldId, OpFieldTomb ft => ft.FieldId, _ => (ushort?)null };
        if (fid is not null && op is not OpRecordTombstone)
        {
            var st = SeenState(ns, recordId, fid.Value);
            if (st is not null)
            {
                var (wall, logical, sAuthor, val) = st.Value;
                if (sAuthor.AsSpan().SequenceEqual(NodeId))
                {
                    bool sameState = op switch
                    {
                        OpFieldWrite fw => val is byte[] vb && vb.AsSpan().SequenceEqual(fw.Value)
                                           && wall == fw.Wall && logical == fw.Logical,
                        OpFieldTomb ft => (val is string sv) && sv == "TOMB"
                                          && wall == ft.Wall && logical == ft.Logical,
                        _ => false,
                    };
                    if (sameState)
                    {
                        var list = Outbox.TryGetValue(ns, out var l) ? l : new List<OutboxEntry>();
                        for (int i = list.Count - 1; i >= 0; i--)
                        {
                            var q = list[i];
                            if (!q.RecordId.AsSpan().SequenceEqual(recordId) ||
                                !q.AuthorNodeId.AsSpan().SequenceEqual(NodeId)) continue;
                            object qop;
                            try { qop = Ops.DecodeOp(q.Payload).Op; }
                            catch { break; }
                            ushort? qfid = qop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
                            if (qfid != fid) continue;
                            bool qSame = TagOf(qop) == TagOf(op) && WallOf(qop) == WallOf(op) &&
                                         LogicalOf(qop) == LogicalOf(op) &&
                                         ValueOf(qop).AsSpan().SequenceEqual(ValueOf(op));
                            if (qSame) return null;   // E-13: restored echo of a queued winner
                            break;                    // newest own queued op differs → push
                        }
                    }
                }
            }
        }
        SetOrigin(op, 0);   // neutral; CreateDeltaBatch rewrites it per target session
        var payload = Ops.EncodeOp(op, fullAuthor: false);
        var entry = new OutboxEntry
        {
            NamespaceId = ns,
            Seq = NextOutboxSeq(author, ns),
            ChangeId = Guid.NewGuid().ToByteArray(),
            RecordId = recordId,
            OpKind = TagOf(op),
            Payload = payload,
            AuthorNodeId = author,
            CreatedAt = (long)Clock.SendTick().Wall,
        };
        if (!Outbox.TryGetValue(ns, out var ol)) Outbox[ns] = ol = new();
        ol.Add(entry);
        // Errata E-13 follow-up: register our own changeId under our OWN channel key so
        // apply_batch's relay rule treats a direct echo as already-seen (see Python port).
        SeenChanges.Add((NodeId, ns, entry.ChangeId));
        return entry;
    }

    private static byte TagOf(object op) => op switch
    { OpFieldWrite => OpsConst.TagFieldWrite, OpFieldTomb => OpsConst.TagFieldTomb, OpRecordTombstone => OpsConst.TagRecordTomb, _ => throw new ArgumentException() };
    private static ulong WallOf(object op) => op switch
    { OpFieldWrite x => x.Wall, OpFieldTomb x => x.Wall, OpRecordTombstone x => x.Wall, _ => 0 };
    private static uint LogicalOf(object op) => op switch
    { OpFieldWrite x => x.Logical, OpFieldTomb x => x.Logical, OpRecordTombstone x => x.Logical, _ => 0 };
    private static byte[] ValueOf(object op) => op is OpFieldWrite fw ? fw.Value : Array.Empty<byte>();
    private static void SetOrigin(object op, ushort v)
    {
        switch (op)
        {
            case OpFieldWrite x: x.Origin = v; break;
            case OpFieldTomb x: x.Origin = v; break;
            case OpRecordTombstone x: x.Origin = v; break;
        }
    }

    /// Local FieldWrite. Returns true if state changed (and outbox grew).
    public bool WriteField(int namespaceId, byte[] recordId, int fieldId, byte[] value, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        var fs = new FieldState(value, h, NodeId);
        // BUGFIX (implementation finding): `origin` is overwritten per target session in
        // CreateDeltaBatch (§13.6.2 [R4]); neutral slot here, like TombField/DeleteRecord.
        var op = new OpFieldWrite { FieldId = (ushort)fieldId, KeyEpoch = 0, Wall = h.Wall, Logical = h.Logical, Origin = 0, AuthorNodeId = NodeId, Value = value };
        bool changed = Merge.ApplyFieldWrite(rec, (ushort)fieldId, fs);
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    public bool TombField(int namespaceId, byte[] recordId, int fieldId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        var fs = new FieldState(Array.Empty<byte>(), h, NodeId);
        var op = new OpFieldTomb { FieldId = (ushort)fieldId, KeyEpoch = 0, Wall = h.Wall, Logical = h.Logical, Origin = 1, AuthorNodeId = NodeId };
        bool changed = Merge.ApplyFieldTomb(rec, (ushort)fieldId, fs);
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    /// Lifecycle → dead (delete-wins).
    public bool DeleteRecord(int namespaceId, byte[] recordId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        bool changed = Merge.ApplyLifecycle(rec, h, NodeId, Model.Dead);
        var op = new OpRecordTombstone { RecordId = recordId, Wall = h.Wall, Logical = h.Logical, Origin = 1, AuthorNodeId = NodeId };
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    /// Undelete only via explicit lifecycle write value=0x00 (§7.4).
    public bool UndeleteRecord(int namespaceId, byte[] recordId, Hlc? hlc = null)
    {
        var store = Ns(namespaceId);
        if (!store.TryGetValue(recordId, out var rec)) store[recordId] = rec = new Record(recordId, (ushort)namespaceId);
        var h = hlc ?? Clock.SendTick();
        bool changed = Merge.ApplyLifecycle(rec, h, NodeId, Model.Alive);
        // encode as FieldWrite on reserved field 0x0000 with value 0x00
        var op = new OpFieldWrite { FieldId = Model.LifecycleFieldId, KeyEpoch = 0, Wall = h.Wall, Logical = h.Logical, Origin = 1, AuthorNodeId = NodeId, Value = new[] { Model.Alive } };
        if (changed) PushOutbox(namespaceId, recordId, op, NodeId);
        else
            // SPEC HARDENING: propagate the no-op revive anyway ([П7]/anti-entropy; scenario 38 step 5).
            PushOutbox(namespaceId, recordId, op, NodeId);
        return changed;
    }

    // --------------------------------------------------------------------- batching
    /// BUGFIX (scenario 23): a relay must stamp ops of a THIRD-PARTY author with that
    /// author's own epoch, not the relay's self.epoch (see Python port docstring).
    public int AuthorEpoch(byte[] author)
    {
        foreach (var m in AliasToNode.Values)
            foreach (var (nid, ep) in m.Values)
                if (nid.AsSpan().SequenceEqual(author)) return ep;
        if (author.AsSpan().SequenceEqual(NodeId)) return Epoch;
        return KnownEpochs.TryGetValue((author, -1), out var ke) ? ke : Epoch;
    }

    /// Build DELTA_BATCH from outbox beyond peer's per-stream watermark.
    /// §13.6.1 [Р3]: NO coalescing across authors — one author per batch.
    /// §13.6.2 [R4]: translate each op's `origin` into the alias agreed for THIS session.
    /// SPEC HARDENING: unknown author ⇒ deterministic UNKNOWN_ALIAS rejection (never a
    /// silent skip that would hole the seq stream). Full rationale in the Python port.
    public Batch? CreateDeltaBatch(byte[] targetPeer, int namespaceId,
                                   byte[]? filterAuthorNodeId = null,
                                   int schemaId = 0, int schemaVersion = 1)
    {
        var pkey = (targetPeer, namespaceId);
        if (!PeerStates.TryGetValue(pkey, out var ps)) PeerStates[pkey] = ps = new PeerState(targetPeer, namespaceId);
        long wm;
        byte[] stream;
        List<OutboxEntry> pending;
        var all = Outbox.TryGetValue(namespaceId, out var ob) ? ob : new List<OutboxEntry>();
        if (filterAuthorNodeId is not null)
        {
            stream = filterAuthorNodeId;
            wm = ps.AckedFor(stream);
            pending = all.Where(e => e.AuthorNodeId.AsSpan().SequenceEqual(stream) && e.Seq > wm).ToList();
            // Errata E-4: gap normalisation also applies on the explicit-filter path.
            if (pending.Count > 0)
            {
                long gap = pending[0].Seq - (wm + 1);
                if (gap > 0) foreach (var e in pending) e.Seq -= gap;
            }
        }
        else
        {
            // pick the CONTIGUOUS stream first (first un-acked seq == acked+1), earliest
            // outbox position; gapped stream as fallback (tail renumbered). BUGFIX/HARDENING
            // (scenario 42 livelock) — see Python port comment.
            var firstPending = new Dictionary<byte[], long>(new BA());
            foreach (var e in all)
            {
                long w = ps.AckedFor(e.AuthorNodeId);   // Errata E-4: per-author watermark
                if (e.Seq > w && (!firstPending.TryGetValue(e.AuthorNodeId, out var cur) || e.Seq < cur))
                    firstPending[e.AuthorNodeId] = e.Seq;
            }
            stream = Array.Empty<byte>();
            bool found = false;
            foreach (var kv in firstPending)
                if (kv.Value == ps.AckedFor(kv.Key) + 1) { stream = kv.Key; found = true; break; }
            if (!found && firstPending.Count > 0) { stream = firstPending.Keys.First(); found = true; }
            if (!found) return null;
            wm = ps.AckedFor(stream);
            pending = all.Where(e => e.AuthorNodeId.AsSpan().SequenceEqual(stream) && e.Seq > wm).ToList();
        }
        if (pending.Count == 0) return null;
        // BUGFIX: cursorStart = first still-un-ACKed seq of THIS stream, not lastPushedSeq+1.
        // SPEC HARDENING (§10.5): close gaps deterministically by renumbering the un-acked
        // tail so it starts exactly at lastAckedSeq+1 ([П2]/[S2]).
        {
            long gap = pending[0].Seq - (wm + 1);   // Errata E-4: per-author watermark `wm`
            if (gap > 0) foreach (var e in pending) e.Seq -= gap;
        }
        long start = pending[0].Seq, end = pending[^1].Seq;
        var translated = new List<OutboxEntry>();
        foreach (var e in pending)
        {
            int? alias = AliasOf(targetPeer, e.AuthorNodeId, Epoch);
            if (alias is null && e.AuthorNodeId.AsSpan().SequenceEqual(NodeId))
                alias = 1;   // BUGFIX: self-entry fallback keeps [А1] intact
            if (alias is null)
            {
                // Errata E-11 (scenario 23): try incremental ALIAS_MAP negotiation before failing closed.
                var peerNode = PeersByNid is not null && PeersByNid.TryGetValue(targetPeer, out var pn) ? pn : null;
                if (peerNode is not null && !e.AuthorNodeId.AsSpan().SequenceEqual(NodeId))
                {
                    int epTry = KnownAuthorEpoch(e.AuthorNodeId) ?? Epoch;
                    alias = EnsureAlias(peerNode, e.AuthorNodeId, epTry);
                }
            }
            if (alias is null)
                throw new SyncException("UNKNOWN_ALIAS");   // [R4'] + Errata E-11
            var op = Ops.DecodeOp(e.Payload).Op;
            SetOrigin(op, (ushort)alias.Value);   // ПЕРЕЗАПИСЬ of session alias (§13.6.2)
            var newPayload = Ops.EncodeOp(op, fullAuthor: false);
            translated.Add(new OutboxEntry
            {
                NamespaceId = e.NamespaceId, Seq = e.Seq, ChangeId = e.ChangeId,
                RecordId = e.RecordId, OpKind = e.OpKind, Payload = newPayload,
                AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt,
            });
        }
        if (translated.Count == 0) return null;
        var recordsRoot = ComputeRecordsRoot(translated);
        // BUGFIX (scenario 23): stamp with the epoch LEARNED for the author in the target
        // session (ALIAS_MAP entry / per-session watermark), not self.epoch.
        // BUGFIX (errata E-6 follow-up): stream key is `stream`, not a stale loop variable.
        int ep = Epoch;
        byte[] author = stream;
        if (!author.AsSpan().SequenceEqual(NodeId))
        {
            var m = AliasToNode.TryGetValue(targetPeer, out var am) ? am : new Dictionary<int, (byte[], int)>();
            bool hit = false;
            foreach (var (alias_, (nid2, ep2)) in m)
                if (nid2.AsSpan().SequenceEqual(author)) { ep = ep2; hit = true; break; }
            if (!hit)
                foreach (var kv in KnownEpochs)
                    if (kv.Key.Item2 >= 0 && kv.Key.Item1.AsSpan().SequenceEqual(targetPeer))
                        ep = Math.Max(ep, kv.Value);
        }
        var batch = new Batch(namespaceId, schemaId, schemaVersion, 0x02, 0,   // 0x02 = LWW_FIELD
                              NodeId, ep, start, end, Zero128, Zero128, recordsRoot, Zero128, translated);
        ps.LastPushedSeq = Math.Max(ps.LastPushedSeq, end);
        // BUGFIX (implementation finding): a freshly built batch must be fully signed —
        // sign_batch_for is idempotent w.r.t. the id cache; the per-stream chain checkpoint
        // advances only after successful delivery (AdvanceStream).
        return SignBatchFor(batch, targetPeer);
    }


    /// Bind batchId + prevBatchDigest + batchDigest at WIRE time for one stream.
    /// SPEC HARDENING (§12.17.2 / [Т3]): the chain is PER STREAM ((issuer, ns, epoch), targetPeer).
    /// Deterministic replay: the SAME (stream, cursor span) always yields the SAME batchId
    /// and byte-identical digest (§10.2.2 step 3, [П7]).
    public Batch SignBatchFor(Batch batch, byte[] targetPeer)
    {
        // SPEC CORRECTION (errata E-2 follow-up): the stream chain is keyed by the ACTUAL
        // stamped issuerEpoch of this batch (author's epoch when relaying).
        var chainKey = (NodeId, batch.NamespaceId, batch.IssuerEpoch);
        var streamKey = (chainKey, targetPeer);
        var ps = PeerStates[(targetPeer, batch.NamespaceId)];
        // Errata E-4: the seq numbering is per AUTHOR STREAM — cache/determinism key
        // must be the resolved durable author (receiver side), falling back to the
        // issuer's nodeId on the sender side where AuthorStreamNodeId is not yet set.
        byte[] authorStream = batch.AuthorStreamNodeId ?? batch.AuthorStream();
        var key = (authorStream, batch.CursorStart, batch.CursorEnd);
        if (!ps.Ids.TryGetValue(key, out var cached))
        {
            cached = (Guid.NewGuid().ToByteArray(),
                      LastStreamDigest.TryGetValue(streamKey, out var p) ? p : Zero128);
            ps.Ids[key] = cached;
        }
        var (batchId, prev) = cached;
        batch.BatchId = batchId;
        batch.PrevBatchDigest = prev;
        batch.BatchDigest = ComputeBatchDigest(batchId, batch.NamespaceId, batch.IssuerNodeId,
            batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd, batch.RecordsRoot, prev);
        return batch;
    }

    /// Called after the frame was successfully handed to targetPeer's transport: the
    /// stream chain checkpoint moves to this batch's digest.
    public void AdvanceStream(byte[] targetPeer, int namespaceId, Batch batch) =>
        LastStreamDigest[((NodeId, namespaceId, batch.IssuerEpoch), targetPeer)] = batch.BatchDigest;

    /// DELTA_BATCH wire layout (§12.17).
    public byte[] EncodeBatchPayload(Batch batch)
    {
        var records = batch.EncodeRecords();
        var w = new Writer();
        w.U16((ushort)batch.NamespaceId).U16((ushort)batch.SchemaId).U16((ushort)batch.SchemaVersion);
        w.U8((byte)batch.MergePolicy).U8((byte)batch.Flags);
        w.B128(batch.IssuerNodeId).U32((uint)batch.IssuerEpoch);
        w.U64((ulong)batch.CursorStart).U64((ulong)batch.CursorEnd);
        w.Varint((ulong)batch.Entries.Count);
        w.B128(batch.BatchId).B128(batch.PrevBatchDigest);
        w.B128(batch.RecordsRoot).B128(batch.BatchDigest);
        w.Raw(records);
        return w.Value();
    }

    public Batch DecodeBatchPayload(byte[] data)
    {
        var r = new Reader(data);
        int ns = r.U16();
        int schemaId = r.U16();
        int schemaVer = r.U16();
        byte policy = r.U8();
        byte flags = r.U8();
        byte[] issuer = r.B128();
        int epoch = (int)r.U32();
        long cstart = (long)r.U64();
        long cend = (long)r.U64();
        int count = (int)r.Varint();
        byte[] batchId = r.B128();
        byte[] prev = r.B128();
        byte[] root = r.B128();
        byte[] digest = r.B128();
        byte[] recordsData = r.Raw(data.Length - r.Pos);
        var rawEntries = DecodeRecordsArray(recordsData);
        if (rawEntries.Count != count) throw new SyncException("BATCH_TRANSACTION_FAILED");
        var entries = new List<OutboxEntry>();
        foreach (var (rid, kind, payload) in rawEntries)
            entries.Add(new OutboxEntry
            {
                NamespaceId = ns, Seq = 0, ChangeId = Zero128, RecordId = rid,
                OpKind = kind, Payload = payload, AuthorNodeId = issuer, CreatedAt = 0,
            });
        return new Batch(ns, schemaId, schemaVer, policy, flags, issuer, epoch,
                         cstart, cend, batchId, prev, root, digest, entries);
    }

    public BatchResult ApplyBatch(Batch batch, byte[] senderPeer)
    {
        var result = new BatchResult(batch.BatchId);
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
            return result;
        }
        // STEP 2: cursor check [S2]/[П2] — errata E-3: keyed per SESSION stream.
        var pkey = (batch.IssuerNodeId, batch.NamespaceId);
        if (!PeerStates.TryGetValue(pkey, out var ps)) PeerStates[pkey] = ps = new PeerState(batch.IssuerNodeId, batch.NamespaceId);
        // Errata E-4: resolve the batch's AUTHOR STREAM durably and key the receive cursor
        // per stream — §13.6.1 [Р3] makes seq numbering per-author.
        byte[] streamAuthor;
        try
        {
            object? op0 = batch.Entries.Count > 0 ? Ops.DecodeOp(batch.Entries[0].Payload).Op : null;
            streamAuthor = op0 is not null ? ResolveAlias(senderPeer, GetOrigin(op0)) : batch.IssuerNodeId;
        }
        catch (SyncException) { streamAuthor = batch.IssuerNodeId; }
        batch.AuthorStreamNodeId = streamAuthor;
        LastStreamAuthor[(senderPeer, batch.NamespaceId)] = streamAuthor;   // E-11
        var streamKey = (senderPeer, batch.IssuerEpoch, batch.NamespaceId, streamAuthor);
        long expected = (ps.ReceivedStreams.TryGetValue(streamKey, out var ex) ? ex : 0) + 1;
        if (batch.CursorStart != expected)
            throw new SyncException("SEQ_GAP", batch.BatchId);
        // STEP 3: batch digest chain (§12.17.2, [Т3])
        byte[] expectPrev = LastBatchDigest.TryGetValue(chain, out var ep2) ? ep2 : Zero128;
        if (!batch.PrevBatchDigest.AsSpan().SequenceEqual(expectPrev))
            throw new SyncException("BATCH_CHAIN_BROKEN", batch.BatchId);
        var root = ComputeRecordsRoot(batch.Entries);
        if (!batch.RecordsRoot.AsSpan().SequenceEqual(root))
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        var recomputed = ComputeBatchDigest(batch.BatchId, batch.NamespaceId, batch.IssuerNodeId,
            batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd, batch.RecordsRoot, batch.PrevBatchDigest);
        if (!batch.BatchDigest.AsSpan().SequenceEqual(recomputed))
            throw new SyncException("BATCH_TRANSACTION_FAILED", batch.BatchId);
        // STEP 4: atomic application [T1] — all-or-nothing on a copy-on-write staging
        var nsStore = Ns(batch.NamespaceId);
        var staged = new Dictionary<byte[], Record>(new BA());
        foreach (var (rid, rec) in nsStore) staged[rid] = Clone(rec);
        int applied = 0;
        var changedIdx = new List<int>();   // indices of entries that actually changed state
        for (int idx = 0; idx < batch.Entries.Count; idx++)
        {
            var e = batch.Entries[idx];
            var op = Ops.DecodeOp(e.Payload).Op;
            var opAuthor = ResolveAlias(senderPeer, GetOrigin(op));   // [А2]; unknown → raise
            // clock skew clamp on incoming HLC wall (§5.3)
            long now = Clock.Now();
            if ((long)WallOf(op) > now + Clock.MaxSkewUs)
                throw new SyncException("CLOCK_DRIFT", batch.BatchId);
            byte[] recId = op is OpRecordTombstone rt ? rt.RecordId : e.RecordId;
            if (!staged.TryGetValue(recId, out var rec)) staged[recId] = rec = new Record(e.RecordId, (ushort)batch.NamespaceId);
            bool changed = ApplyOp(rec, op, opAuthor);
            if (changed) { applied++; changedIdx.Add(idx); }
        }
        // commit atomically (rollback path: exceptions above leave storage untouched [Т1])
        Storage[batch.NamespaceId] = staged;
        Inbox.Add(key);
        ps.LastReceivedSeq = batch.CursorEnd;   // [П3]/[Т2] atomic cursor jump (compat copy)
        ps.ReceivedStreams[streamKey] = batch.CursorEnd;   // errata E-3 per-session cursor
        LastBatchDigest[chain] = batch.BatchDigest;
        // SPEC CORRECTION (errata E-2): track epochs PER SESSION (sender_peer, ns); the
        // commit path only ever RAISES the watermark for batches that pass every gate.
        int knownS = KnownEpochs.TryGetValue(sesKey, out var ks) ? ks : 0;
        KnownEpochs[sesKey] = Math.Max(knownS, batch.IssuerEpoch);
        AcksOut.Add(batch.BatchId);
        result.Applied = applied;
        result.Acked = true;
        // Relay [Р1, Р2]: ONLY entries that actually changed state are appended to OUR
        // outbox under the ORIGINAL authorNodeId (SPEC HARDENING §8.3 [E2] + Errata E-10:
        // provenance-based relay eligibility — FULL rationale in the Python port).
        if (!batch.IssuerNodeId.AsSpan().SequenceEqual(NodeId))
        {
            var batchOps = batch.Ops();
            for (int idx = 0; idx < batch.Entries.Count; idx++)
            {
                var e = batch.Entries[idx];
                byte[] cid = e.ChangeId.AsSpan().SequenceEqual(Zero128) ? Guid.NewGuid().ToByteArray() : e.ChangeId;
                var ck = (senderPeer, batch.NamespaceId, cid);
                bool known = SeenChanges.Contains(ck);
                SeenChanges.Add(ck);
                if (!known && !changedIdx.Contains(idx))
                {
                    bool relayOk = true;
                    var rop = batchOps[idx];
                    ushort? rfid = rop switch { OpFieldWrite x => x.FieldId, OpFieldTomb x => x.FieldId, _ => (ushort?)null };
                    if (rfid is not null)
                    {
                        byte[]? rAuthor = null;
                        try { rAuthor = ResolveAlias(senderPeer, GetOrigin(rop)); }
                        catch (SyncException) { }
                        if (rAuthor is not null && rAuthor.AsSpan().SequenceEqual(NodeId) &&
                            SeenState(batch.NamespaceId, e.RecordId, rfid.Value) is not null &&
                            OwnQueueHolds(batch.NamespaceId, e.RecordId, rop))
                            relayOk = false;   // own durable state echoed back [E2]/[Ш2]
                    }
                    if (relayOk) changedIdx.Add(idx);     // new provenance ⇒ relay even as loser
                }
            }
            foreach (var idx in changedIdx)
            {
                var e = batch.Entries[idx];
                var op = batchOps[idx];
                if (GetAuthor(op) is not null) continue;   // full-author mode keeps durable author
                var resolved = ResolveAlias(senderPeer, GetOrigin(op));
                // BUGFIX (scenario 23): the relay keeps the op payload VERBATIM (§13.5 [Р1]/[И4]).
                var entry = new OutboxEntry
                {
                    NamespaceId = e.NamespaceId != 0 ? e.NamespaceId : batch.NamespaceId,
                    Seq = NextOutboxSeq(resolved, batch.NamespaceId),
                    ChangeId = e.ChangeId.AsSpan().SequenceEqual(Zero128) ? Guid.NewGuid().ToByteArray() : e.ChangeId,
                    RecordId = e.RecordId,
                    OpKind = e.OpKind,
                    Payload = e.Payload,
                    AuthorNodeId = resolved,
                    CreatedAt = (long)WallOf(op),   // BUGFIX: stamp the OP's own HLC wall time
                };
                if (!Outbox.TryGetValue(batch.NamespaceId, out var ol)) Outbox[batch.NamespaceId] = ol = new();
                ol.Add(entry);
                // Errata E-10/E-11: propagate the third-party binding learned through THIS
                // session to our OTHER live sessions AND mirror it into the sender's copy
                // (incremental ALIAS_MAP update, §4.3/§12.7). FULL comment in Python port.
                int? epm = null;
                var smap = AliasToNode.TryGetValue(senderPeer, out var ss) ? ss : new Dictionary<int, (byte[], int)>();
                foreach (var (_al, (n, epv)) in smap)
                    if (n.AsSpan().SequenceEqual(resolved)) { epm = epv; break; }
                if (epm is not null)
                {
                    foreach (var otherPeerKv in AliasToNode.Keys.ToList())
                    {
                        var otherPeer = PeersByNid is not null && PeersByNid.TryGetValue(otherPeerKv, out var on) ? on : null;
                        if (otherPeer is not null && !otherPeerKv.AsSpan().SequenceEqual(senderPeer))
                            EnsureAlias(otherPeer, resolved, epm.Value);
                    }
                    var senderNode = PeersByNid is not null && PeersByNid.TryGetValue(senderPeer, out var sn) ? sn : null;
                    if (senderNode is not null && !resolved.AsSpan().SequenceEqual(NodeId))
                        EnsureAlias(senderNode, resolved, epm.Value);
                }
            }
        }
        return result;
    }

    /// Errata E-13 follow-up 4 (scenario 37): does OUR OWN un-ACKed queue already carry a
    /// byte-equal winner for (recordId, fieldId)? If yes, the ring merely mirrors our own
    /// state back — enqueueing again would duplicate propagation [И2].
    private bool OwnQueueHolds(int ns, byte[] rid, object qop)
    {
        var list = Outbox.TryGetValue(ns, out var l) ? l : new List<OutboxEntry>();
        foreach (var q in list)
        {
            if (!q.RecordId.AsSpan().SequenceEqual(rid) || !q.AuthorNodeId.AsSpan().SequenceEqual(NodeId)) continue;
            object sOp;
            try { sOp = Ops.DecodeOp(q.Payload).Op; } catch { continue; }
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
    private static byte[]? GetAuthor(object op) => op switch
    { OpFieldWrite x => x.AuthorNodeId, OpFieldTomb x => x.AuthorNodeId, OpRecordTombstone x => x.AuthorNodeId, _ => null };

    /// Apply one resolved op to a staged record. Echo suppression happens inside
    /// MergeField: identical (hlc, author, value) → changed=False → nothing queued.
    public bool ApplyOp(Record rec, object op, byte[] author)
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
    public void ProcessAck(byte[] peerNode, int namespaceId, long ackedSeq, byte[]? authorStream = null)
    {
        if (!PeerStates.TryGetValue((peerNode, namespaceId), out var ps)) return;
        if (authorStream is not null)
            ps.StreamAcked[authorStream] = Math.Max(ps.StreamAcked.TryGetValue(authorStream, out var cur) ? cur : 0, ackedSeq);
        else
        {
            var entries = Outbox.TryGetValue(namespaceId, out var es) ? es : new List<OutboxEntry>();
            var heads = new Dictionary<byte[], long>(new BA());
            foreach (var e in entries)
                if (!heads.TryGetValue(e.AuthorNodeId, out var cur) || e.Seq < cur)
                    heads[e.AuthorNodeId] = e.Seq;
            foreach (var (author, firstSeq) in heads)
                if (firstSeq <= ackedSeq)
                    ps.StreamAcked[author] = Math.Max(ps.StreamAcked.TryGetValue(author, out var c2) ? c2 : 0, ackedSeq);
        }
        // legacy mirror: monotone scalar = min over known streams' watermarks.
        if (ps.StreamAcked.Count > 0)
            ps.LastAckedSeq = ps.StreamAcked.Values.Min();
    }

    /// Snapshot = all live records + tombstones + ghosts (§ summary 'снапшот').
    public List<Batch> BuildSnapshot(int namespaceId)
    {
        var batches = new List<Batch>();
        var nsStore = Ns(namespaceId);
        foreach (var (rid, rec) in nsStore.OrderBy(kv => Convert.ToHexString(kv.Key)))
        {
            var entries = new List<OutboxEntry>();
            foreach (var (fid, fs) in rec.State.OrderBy(kv => kv.Key))
            {
                var op = new OpFieldWrite { FieldId = fid, KeyEpoch = fs.KeyEpoch, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical, Origin = 1, AuthorNodeId = fs.AuthorNodeId, Value = fs.Value };
                entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid, OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOpFull(op), AuthorNodeId = fs.AuthorNodeId, CreatedAt = 0 });
            }
            foreach (var (fid, fs) in rec.FieldTombs.OrderBy(kv => kv.Key))
            {
                var op = new OpFieldTomb { FieldId = fid, KeyEpoch = fs.KeyEpoch, Wall = fs.Timestamp.Wall, Logical = fs.Timestamp.Logical, Origin = 1, AuthorNodeId = fs.AuthorNodeId };
                entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid, OpKind = OpsConst.TagFieldTomb, Payload = Ops.EncodeOpFull(op), AuthorNodeId = fs.AuthorNodeId, CreatedAt = 0 });
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
                    entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid, OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOpFull(opw), AuthorNodeId = opw.AuthorNodeId!, CreatedAt = 0 });
                }
                else
                    entries.Add(new OutboxEntry { NamespaceId = namespaceId, Seq = 0, ChangeId = Zero128, RecordId = rid, OpKind = OpsConst.TagRecordTomb, Payload = Ops.EncodeOpFull(op), AuthorNodeId = op.AuthorNodeId!, CreatedAt = 0 });
            }
            if (entries.Count > 0)
                batches.Add(new Batch(namespaceId, 0, 1, 0x02, 0, NodeId, Epoch, 0, 0,
                                      Guid.NewGuid().ToByteArray(), Zero128,
                                      ComputeRecordsRoot(entries), Zero128, entries));
        }
        foreach (var b in batches)
            b.BatchDigest = ComputeBatchDigest(b.BatchId, b.NamespaceId, b.IssuerNodeId,
                b.IssuerEpoch, b.CursorStart, b.CursorEnd, b.RecordsRoot, b.PrevBatchDigest);
        return batches;
    }

    /// Apply snapshot ops directly (not through cursor machinery).
    public int ApplySnapshot(Batch batch, byte[] senderPeer)
    {
        int applied = 0;
        foreach (var e in batch.Entries)
        {
            var op = Ops.DecodeOp(e.Payload).Op;
            var author = GetAuthor(op)!;
            var nsStore = Ns(batch.NamespaceId);
            if (!nsStore.TryGetValue(e.RecordId, out var rec)) nsStore[e.RecordId] = rec = new Record(e.RecordId, (ushort)batch.NamespaceId);
            if (ApplyOp(rec, op, author)) applied++;
        }
        return applied;
    }

    // ---------------------------------------------------------------------- helpers
    public byte[]? RecordDigestOf(int namespaceId, byte[] recordId) =>
        Storage.TryGetValue(namespaceId, out var s) && s.TryGetValue(recordId, out var rec) ? Digests.RecordDigest(rec) : null;

    public Dictionary<byte[], byte[]> AllRecordDigests(int namespaceId)
    {
        var d = new Dictionary<byte[], byte[]>(new BA());
        if (Storage.TryGetValue(namespaceId, out var s))
            foreach (var (rid, rec) in s) d[rid] = Digests.RecordDigest(rec);
        return d;
    }

    public int OutboxSize(int namespaceId) => Outbox.TryGetValue(namespaceId, out var l) ? l.Count : 0;

    internal static Record Clone(Record rec)
    {
        var r = new Record(rec.RecordId, rec.NamespaceId)
        {
            State = new(rec.State),
            LifecycleHlc = rec.LifecycleHlc,
            LifecycleAuthor = rec.LifecycleAuthor,
            LifecycleValue = rec.LifecycleValue,
            FieldTombs = new(rec.FieldTombs),
        };
        return r;
    }

    /// Canonical records array: count varint, then per entry:
    /// recordId b128 | opKind u8 | payloadLen varint | payload.
    public static byte[] EncodeRecordsArray(List<OutboxEntry> entries)
    {
        var w = new Writer();
        w.Varint((ulong)entries.Count);
        foreach (var e in entries)
            w.B128(e.RecordId).U8(e.OpKind).Varint((ulong)e.Payload.Length).Raw(e.Payload);
        return w.Value();
    }

    public static List<(byte[] Rid, byte Kind, byte[] Payload)> DecodeRecordsArray(byte[] data)
    {
        var r = new Reader(data);
        int n = (int)r.Varint();
        var outp = new List<(byte[], byte, byte[])>();
        for (int i = 0; i < n; i++)
        {
            byte[] rid = r.B128();
            byte kind = r.U8();
            byte[] payload = r.Bytes();
            outp.Add((rid, kind, payload));
        }
        return outp;
    }

    /// §12.17.2 batchDigest = BLAKE3-128(domainTag || batchId || namespaceId || ...)
    public static byte[] ComputeBatchDigest(byte[] batchId, int namespaceId, byte[] issuerNode,
        int issuerEpoch, long cursorStart, long cursorEnd, byte[] recordsRoot, byte[] prevBatchDigest)
    {
        var w = new Writer();
        w.Raw(Domains.DomainBatch).B128(batchId).U16((ushort)namespaceId).B128(issuerNode);
        w.U32((uint)issuerEpoch).U64((ulong)cursorStart).U64((ulong)cursorEnd);
        w.B128(recordsRoot).B128(prevBatchDigest);
        return Digests.Blake3(w.Value(), 16);
    }

    /// recordsRoot = BLAKE3-128 over the canonical records array.
    public static byte[] ComputeRecordsRoot(List<OutboxEntry> entries) =>
        Digests.Blake3(Domains.Cat(Domains.DomainRecords, EncodeRecordsArray(entries)), 16);
}

public sealed class BatchResult
{
    public byte[] BatchId;
    public int Applied = 0;
    public bool Acked = false;
    public bool Duplicate = false;
    public BatchResult(byte[] batchId) { BatchId = batchId; }
}
