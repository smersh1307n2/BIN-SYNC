// Production hardening (Шаг 3): atomic save/load of the FULL BinSync.Net node state.
//
// WAL-style durability contract:
//  * Integrity — the serialized body is covered by a domain-tagged BLAKE3 digest
//    ("BIN-SYNC/NODESTATE/v1", in-tree Blake3Core, zero dependencies). Any bit flip,
//    truncation or version mismatch makes Load throw InvalidDataException.
//  * Atomicity — the image goes to a temp file in the same directory, fsync'd, then
//    File.Move(overwrite) renames it over the target; a crash can never leave a
//    half-written state file.
//  * Consistency — SerializeBody holds the node's re-entrant gate (_gate) so the
//    image is a point-in-time snapshot even while other threads mutate the node.
//
// Persisted state mirrors every field of BinSync.Node:
//  identity (NodeId, Epoch), HLC clock (LastWall/LastLogical), Storage (ns -> recordId
//  -> Record with FieldState/HLC/ghost lifecycle + FieldTombs), Outbox per namespace,
//  per-(author,ns) outbox seq counters, Inbox idempotency set, LastBatchDigest and
//  LastStreamDigest chain checkpoints, AliasToNode/NodeToAlias, DurableAuthors,
//  PeerStates (cursors + ReceivedStreams/StreamAcked/Ids caches), SeenChanges,
//  LastStreamAuthor, KnownEpochs, Quarantined, ForbiddenNamespaces, SchemaVersions.
//
// Wire format: big-endian header (magic/version/bodyLen/digest), LE canonical body
// written with the core Writer/Reader codec (varint-prefixed byte keys keep content
// ordering deterministic via explicit sorting).
namespace BinSync;

public static class NodePersistence
{
    /// <summary>Digest tag mandated by the spec.</summary>
    public const string StateTag = "BIN-SYNC/NODESTATE/v1";

    private const uint Magic = 0x42534E53; // 'B''S''N''S'
    private const ushort Version = 1;
    private const int DigestLen = 32;

    // ---------------------------------------------------------------- Save

    /// <summary>Atomically persist <paramref name="node"/> to <paramref name="path"/>.</summary>
    public static void Save(Node node, string path)
    {
        if (node is null) throw new ArgumentNullException(nameof(node));
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path required.", nameof(path));

        var body = SerializeBody(node);
        var digest = Blake3Core.TaggedHash(StateTag, body);

        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            WriteBE(w, Magic);
            WriteBE(w, Version);
            WriteBE(w, (ulong)body.Length);
            w.Write(digest);
            w.Write(body);
            w.Flush();
        }
        var image = ms.ToArray();

        var dir = Path.GetDirectoryName(Path.GetFullPath(path))!;
        Directory.CreateDirectory(dir);
        var tmp = Path.Combine(dir, $".{Path.GetFileName(path)}.tmp-{Guid.NewGuid():N}");
        try
        {
            using (var fs = new FileStream(tmp, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                                           4096, FileOptions.WriteThrough))
            {
                fs.Write(image);
                fs.Flush(flushToDisk: true);   // fsync before rename
            }
            File.Move(tmp, path, overwrite: true); // atomic on POSIX; rename semantics on Windows
        }
        finally
        {
            if (File.Exists(tmp)) { try { File.Delete(tmp); } catch { /* best effort */ } }
        }
    }

    // ---------------------------------------------------------------- Load

    /// <summary>Load a node previously saved with <see cref="Save"/>. Throws InvalidDataException on any corruption.</summary>
    public static Node Load(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path required.", nameof(path));
        return LoadFromBytes(File.ReadAllBytes(path));
    }

    /// <summary>In-memory variant of <see cref="Load"/> (same integrity rules).</summary>
    public static Node LoadFromBytes(byte[] bytes)
    {
        if (bytes.Length < 4 + 2 + 8 + DigestLen)
            throw new InvalidDataException("State file truncated.");
        using var ms = new MemoryStream(bytes, writable: false);
        using var r = new BinaryReader(ms, System.Text.Encoding.UTF8);

        if (ReadBEU32(r) != Magic)
            throw new InvalidDataException("Not a BinSync node-state file (bad magic).");
        var ver = ReadBEU16(r);
        if (ver != Version)
            throw new InvalidDataException($"Unsupported state version {ver} (expected {Version}).");

        var bodyLen = ReadBEU64(r);
        if (bodyLen > (ulong)(bytes.Length - 14 - DigestLen))
            throw new InvalidDataException("State file truncated (body length out of bounds).");
        var storedDigest = r.ReadBytes(DigestLen);
        var body = r.ReadBytes((int)bodyLen);
        if (body.Length != (int)bodyLen)
            throw new InvalidDataException("State file truncated mid-body.");

        var actual = Blake3Core.TaggedHash(StateTag, body);
        if (!actual.AsSpan().SequenceEqual(storedDigest))
            throw new InvalidDataException("State digest mismatch — file corrupted (tagged BLAKE3 " + StateTag + ").");

        return DeserializeBody(body);
    }

    // ------------------------------------------------------------- serialization

    private static byte[] SerializeBody(Node n)
    {
        lock (n.Gate) // point-in-time image under the re-entrant gate
        {
            var w = new Writer();
            w.B128(n.NodeId);
            w.Varint((ulong)n.Epoch);

            // --- HLC clock (§5.2: cold boot allowed — zeros round-trip)
            w.U64(n.Clock.LastWall);
            w.U32(n.Clock.LastLogical);

            // --- storage: ns -> recordId -> Record
            w.Varint((ulong)n.Storage.Count);
            foreach (var (ns, recs) in n.Storage.OrderBy(kv => kv.Key))
            {
                w.Varint((ulong)ns);
                w.Varint((ulong)recs.Count);
                foreach (var (rid, rec) in recs.OrderBy(kv => kv.Key, BytesOrd))
                    WriteRecord(w, rid, rec);
            }

            // --- outbox: ns -> entries
            w.Varint((ulong)n.Outbox.Count);
            foreach (var (ns, list) in n.Outbox.OrderBy(kv => kv.Key))
            {
                w.Varint((ulong)ns);
                w.Varint((ulong)list.Count);
                foreach (var e in list) WriteOutboxEntry(w, e);
            }

            // --- per-(author, ns) outbox seq counters [П1]
            var seqMap = GetField<Dictionary<(byte[], int), long>>(n, "_outboxSeq");
            w.Varint((ulong)seqMap.Count);
            foreach (var ((author, ns), seq) in seqMap.OrderBy(kv => kv.Key.Item1, BytesOrd).ThenBy(kv => kv.Key.Item2))
            { w.B128(author); w.Varint((ulong)ns); w.U64((ulong)seq); }

            // --- inbox idempotency set (issuer, ns, epoch, batchId)
            w.Varint((ulong)n.Inbox.Count);
            foreach (var (issuer, ns, ep, bid) in n.Inbox.OrderBy(k => k.Item1, BytesOrd)
                                                         .ThenBy(k => k.Item2).ThenBy(k => k.Item3)
                                                         .ThenBy(k => k.Item4, BytesOrd))
            { w.B128(issuer); w.Varint((ulong)ns); w.Varint((ulong)ep); w.B128(bid); }

            // --- batch-digest chain checkpoints: (issuer, ns, epoch) -> digest
            w.Varint((ulong)n.LastBatchDigest.Count);
            foreach (var ((issuer, ns, ep), dig) in n.LastBatchDigest.OrderBy(k => k.Key.Item1, BytesOrd)
                                                                     .ThenBy(k => k.Key.Item2).ThenBy(k => k.Key.Item3))
            { w.B128(issuer); w.Varint((ulong)ns); w.Varint((ulong)ep); w.B128(dig); }

            // --- per-stream issuer checkpoints: ((issuer, ns, ep), peer) -> digest (§12.17.2)
            w.Varint((ulong)n.LastStreamDigest.Count);
            foreach (var (((issuer, ns, ep), peer), dig) in n.LastStreamDigest
                       .OrderBy(k => k.Key.Item1.Item1, BytesOrd).ThenBy(k => k.Key.Item1.Item2)
                       .ThenBy(k => k.Key.Item1.Item3).ThenBy(k => k.Key.Item2, BytesOrd))
            { w.B128(issuer); w.Varint((ulong)ns); w.Varint((ulong)ep); w.B128(peer); w.B128(dig); }

            // --- aliases per session peer
            w.Varint((ulong)n.AliasToNode.Count);
            foreach (var (peer, map) in n.AliasToNode.OrderBy(k => k.Key, BytesOrd))
            {
                w.B128(peer);
                w.Varint((ulong)map.Count);
                foreach (var (alias, (nid, ep)) in map.OrderBy(k => k.Key))
                { w.Varint((ulong)alias); w.B128(nid); w.Varint((ulong)ep); }
            }
            w.Varint((ulong)n.NodeToAlias.Count);
            foreach (var (peer, map) in n.NodeToAlias.OrderBy(k => k.Key, BytesOrd))
            {
                w.B128(peer);
                w.Varint((ulong)map.Count);
                foreach (var ((nid, ep), alias) in map.OrderBy(k => k.Key.Item1, BytesOrd).ThenBy(k => k.Key.Item2))
                { w.B128(nid); w.Varint((ulong)ep); w.Varint((ulong)alias); }
            }

            // --- durable authors (Errata E-9)
            w.Varint((ulong)n.DurableAuthors.Count);
            foreach (var (nid, ep) in n.DurableAuthors.OrderBy(k => k.Key, BytesOrd))
            { w.B128(nid); w.Varint((ulong)ep); }

            // --- peer states
            w.Varint((ulong)n.PeerStates.Count);
            foreach (var ((peer, ns), ps) in n.PeerStates.OrderBy(k => k.Key.Item1, BytesOrd).ThenBy(k => k.Key.Item2))
            {
                w.B128(peer); w.Varint((ulong)ns);
                w.U64((ulong)ps.LastPushedSeq); w.U64((ulong)ps.LastReceivedSeq); w.U64((ulong)ps.LastAckedSeq);
                w.U8(ps.InitialSyncComplete ? (byte)1 : (byte)0);
                w.Varint((ulong)ps.MembershipEpoch); w.U64((ulong)ps.LastSeen);
                w.Varint((ulong)ps.ReceivedStreams.Count);
                foreach (var ((issuer, rns, rep, bid), seq) in ps.ReceivedStreams
                           .OrderBy(k => k.Key.Item1, BytesOrd).ThenBy(k => k.Key.Item2).ThenBy(k => k.Key.Item3)
                           .ThenBy(k => k.Key.Item4, BytesOrd))
                { w.B128(issuer); w.Varint((ulong)rns); w.Varint((ulong)rep); w.B128(bid); w.U64((ulong)seq); }
                w.Varint((ulong)ps.StreamAcked.Count);
                foreach (var (author, seq) in ps.StreamAcked.OrderBy(k => k.Key, BytesOrd))
                { w.B128(author); w.U64((ulong)seq); }
                w.Varint((ulong)ps.Ids.Count);
                foreach (var ((author, start, end), (id, prev)) in ps.Ids
                           .OrderBy(k => k.Key.Item1, BytesOrd).ThenBy(k => k.Key.Item2).ThenBy(k => k.Key.Item3))
                { w.B128(author); w.U64((ulong)start); w.U64((ulong)end); w.B128(id); w.B128(prev); }
            }

            // --- provenance seen-set (Errata E-10/E-13)
            w.Varint((ulong)n.SeenChanges.Count);
            foreach (var (peer, ns, cid) in n.SeenChanges.OrderBy(k => k.Item1, BytesOrd).ThenBy(k => k.Item2)
                                                         .ThenBy(k => k.Item3, BytesOrd))
            { w.B128(peer); w.Varint((ulong)ns); w.B128(cid); }

            // --- last accepted stream author (Errata E-11)
            w.Varint((ulong)n.LastStreamAuthor.Count);
            foreach (var ((peer, ns), author) in n.LastStreamAuthor.OrderBy(k => k.Key.Item1, BytesOrd).ThenBy(k => k.Key.Item2))
            { w.B128(peer); w.Varint((ulong)ns); w.B128(author); }

            // --- known epochs (mixed-key watermarks)
            w.Varint((ulong)n.KnownEpochs.Count);
            foreach (var ((key, ns), ep) in n.KnownEpochs.OrderBy(k => k.Key.Item1, BytesOrd).ThenBy(k => k.Key.Item2))
            { w.B128(key); w.Varint((ulong)(ns - int.MinValue)); w.Varint((ulong)ep); }

            // --- quarantine / policy sets
            w.Varint((ulong)n.Quarantined.Count);
            foreach (var nid in n.Quarantined.OrderBy(k => k, BytesOrd)) w.B128(nid);
            w.Varint((ulong)n.ForbiddenNamespaces.Count);
            foreach (var fns in n.ForbiddenNamespaces.OrderBy(v => v)) w.Varint((ulong)fns);
            w.Varint((ulong)n.SchemaVersions.Count);
            foreach (var (svNs, sv) in n.SchemaVersions.OrderBy(kv => kv.Key))
            { w.Varint((ulong)svNs); w.Varint((ulong)sv); }

            // --- session peer ids (E-11 registry). Only the KEY (peer nodeId) is serialized;
            // Node.PeersByNid maps nodeId -> live Node object and is not part of the durable
            // state image. Load() rebuilds the registry keys from these ids + AliasToNode keys.
            var peers = n.PeersByNid is null ? new List<byte[]>() : n.PeersByNid.Keys.ToList();
            w.Varint((ulong)peers.Count);
            foreach (var p in peers.OrderBy(k => k, BytesOrd)) w.B128(p);

            return w.Value();
        }
    }

    private static Node DeserializeBody(byte[] body)
    {
        var r = new Reader(body);
        var nodeId = r.B128();
        var epoch = (int)r.Varint();
        var n = new Node(nodeId, epoch);
        n.Epoch = epoch;

        n.Clock.LastWall = r.U64();
        n.Clock.LastLogical = r.U32();

        int nsCount = (int)r.Varint();
        for (int i = 0; i < nsCount; i++)
        {
            var ns = (int)r.Varint();
            int recCount = (int)r.Varint();
            var map = new Dictionary<byte[], Record>(new BA());
            for (int j = 0; j < recCount; j++)
            {
                var (rid, rec) = ReadRecord(r);
                map[rid] = rec;
            }
            n.Storage[ns] = map;
        }

        int outNsCount = (int)r.Varint();
        for (int i = 0; i < outNsCount; i++)
        {
            var ns = (int)r.Varint();
            int cnt = (int)r.Varint();
            var list = new List<OutboxEntry>(cnt);
            for (int j = 0; j < cnt; j++) list.Add(ReadOutboxEntry(r));
            n.Outbox[ns] = list;
        }

        var seqMap = GetField<Dictionary<(byte[], int), long>>(n, "_outboxSeq");
        int seqCount = (int)r.Varint();
        for (int i = 0; i < seqCount; i++)
        {
            var author = r.B128(); var ns = (int)r.Varint(); var seq = (long)r.U64();
            seqMap[(author, ns)] = seq;
        }

        int inboxCount = (int)r.Varint();
        for (int i = 0; i < inboxCount; i++)
        {
            var issuer = r.B128(); var ns = (int)r.Varint(); var ep = (int)r.Varint(); var bid = r.B128();
            n.Inbox.Add((issuer, ns, ep, bid));
        }

        int lbdCount = (int)r.Varint();
        for (int i = 0; i < lbdCount; i++)
        {
            var issuer = r.B128(); var ns = (int)r.Varint(); var ep = (int)r.Varint(); var dig = r.B128();
            n.LastBatchDigest[(issuer, ns, ep)] = dig;
        }

        int lsCount = (int)r.Varint();
        for (int i = 0; i < lsCount; i++)
        {
            var issuer = r.B128(); var ns = (int)r.Varint(); var ep = (int)r.Varint();
            var peer = r.B128(); var dig = r.B128();
            n.LastStreamDigest[((issuer, ns, ep), peer)] = dig;
        }

        int a2nCount = (int)r.Varint();
        for (int i = 0; i < a2nCount; i++)
        {
            var peer = r.B128();
            int cnt = (int)r.Varint();
            var map = new Dictionary<int, (byte[], int)>();
            for (int j = 0; j < cnt; j++)
            {
                var alias = (int)r.Varint(); var nid = r.B128(); var ep = (int)r.Varint();
                map[alias] = (nid, ep);
            }
            n.AliasToNode[peer] = map;
        }
        int n2aCount = (int)r.Varint();
        for (int i = 0; i < n2aCount; i++)
        {
            var peer = r.B128();
            int cnt = (int)r.Varint();
            var map = new Dictionary<(byte[], int), int>(new BAI2b());
            for (int j = 0; j < cnt; j++)
            {
                var nid = r.B128(); var ep = (int)r.Varint(); var alias = (int)r.Varint();
                map[(nid, ep)] = alias;
            }
            n.NodeToAlias[peer] = map;
        }

        int daCount = (int)r.Varint();
        for (int i = 0; i < daCount; i++)
        {
            var nid = r.B128(); var ep = (int)r.Varint();
            n.DurableAuthors[nid] = ep;
        }

        int psCount = (int)r.Varint();
        for (int i = 0; i < psCount; i++)
        {
            var peer = r.B128(); var ns = (int)r.Varint();
            var ps = new PeerState(peer, ns)
            {
                LastPushedSeq = (long)r.U64(),
                LastReceivedSeq = (long)r.U64(),
                LastAckedSeq = (long)r.U64(),
                InitialSyncComplete = r.U8() != 0,
                MembershipEpoch = (int)r.Varint(),
                LastSeen = (long)r.U64(),
            };
            int rsCount = (int)r.Varint();
            for (int j = 0; j < rsCount; j++)
            {
                var issuer = r.B128(); var rns = (int)r.Varint(); var rep = (int)r.Varint();
                var bid = r.B128(); var seq = (long)r.U64();
                ps.ReceivedStreams[(issuer, rns, rep, bid)] = seq;
            }
            int saCount = (int)r.Varint();
            for (int j = 0; j < saCount; j++)
            {
                var author = r.B128(); var seq = (long)r.U64();
                ps.StreamAcked[author] = seq;
            }
            int idsCount = (int)r.Varint();
            for (int j = 0; j < idsCount; j++)
            {
                var author = r.B128(); var start = (long)r.U64(); var end = (long)r.U64();
                var id = r.B128(); var prev = r.B128();
                ps.Ids[(author, start, end)] = (id, prev);
            }
            n.PeerStates[(peer, ns)] = ps;
        }

        int seenCount = (int)r.Varint();
        for (int i = 0; i < seenCount; i++)
        {
            var peer = r.B128(); var ns = (int)r.Varint(); var cid = r.B128();
            n.SeenChanges.Add((peer, ns, cid));
        }

        int lsaCount = (int)r.Varint();
        for (int i = 0; i < lsaCount; i++)
        {
            var peer = r.B128(); var ns = (int)r.Varint(); var author = r.B128();
            n.LastStreamAuthor[(peer, ns)] = author;
        }

        int keCount = (int)r.Varint();
        for (int i = 0; i < keCount; i++)
        {
            var key = r.B128();
            var ns = (int)((long)r.Varint() + int.MinValue);   // zig: stored as ns - int.MinValue
            var ep = (int)r.Varint();
            n.KnownEpochs[(key, ns)] = ep;
        }

        int qCount = (int)r.Varint();
        for (int i = 0; i < qCount; i++) n.Quarantined.Add(r.B128());
        int fCount = (int)r.Varint();
        for (int i = 0; i < fCount; i++) n.ForbiddenNamespaces.Add((int)r.Varint());
        int svCount = (int)r.Varint();
        for (int i = 0; i < svCount; i++)
        {
            var svNs = (int)r.Varint(); var sv = (int)r.Varint();
            n.SchemaVersions[svNs] = sv;
        }

        // --- session peer ids: rebuild the E-11 registry KEYS. The Node objects themselves
        // are live handles and cannot be deserialized; a resumed application re-registers
        // peers via Transport.MakeSession / AttachPeer, which merge into this map.
        // PRODUCTION FIX: placeholder entries must NOT shadow live handles — TryAdd kept a
        // null value for a peer that MakeSession registers later, so ResolveAlias's errata
        // E-12 "consult the peer's copy of the session" path silently died after restart
        // (UNKNOWN_ALIAS on the first post-restart SyncRound). AttachPeer-style semantics:
        // only fill slots that are absent or currently null.
        int peerCount = (int)r.Varint();
        var peerIds = new List<byte[]>(peerCount);
        for (int i = 0; i < peerCount; i++) peerIds.Add(r.B128());
        if (peerIds.Count > 0 || n.AliasToNode.Count > 0)
        {
            n.PeersByNid ??= new(new ByteArrEq());
            // NOTE: `existing is null` must be a plain reference check — Node derives from
            // MarshalByRefObject so its == operator does NOT test for null.
            foreach (var p in peerIds.Concat(n.AliasToNode.Keys))
                if (!n.PeersByNid.TryGetValue(p, out var existing) || ReferenceEquals(existing, null))
                    n.PeersByNid[p] = null!;
        }

        if (!r.Eof())
            throw new InvalidDataException("Trailing bytes in node-state body.");
        return n;
    }

    // ----------------------------------------------------------- record helpers

    private static void WriteRecord(Writer w, byte[] rid, Record rec)
    {
        w.B128(rid);
        w.Varint((ulong)rec.NamespaceId);
        w.U8(rec.LifecycleValue);
        w.U8(rec.LifecycleHlc is null ? (byte)0 : (byte)1);
        if (rec.LifecycleHlc is not null)
        {
            w.U64(rec.LifecycleHlc.Wall); w.U32(rec.LifecycleHlc.Logical);
            w.B128(rec.LifecycleAuthor ?? new byte[16]);
        }
        w.Varint((ulong)rec.State.Count);
        foreach (var (fid, fs) in rec.State.OrderBy(kv => kv.Key))
            WriteFieldState(w, fid, fs);
        w.Varint((ulong)rec.FieldTombs.Count);
        foreach (var (fid, fs) in rec.FieldTombs.OrderBy(kv => kv.Key))
            WriteFieldState(w, fid, fs);
    }

    private static (byte[], Record) ReadRecord(Reader r)
    {
        var rid = r.B128();
        var ns = (ushort)r.Varint();
        var rec = new Record(rid, ns) { LifecycleValue = r.U8() };
        if (r.U8() != 0)
        {
            var wall = r.U64(); var logical = r.U32(); var author = r.B128();
            rec.LifecycleHlc = new Hlc(wall, logical);
            rec.LifecycleAuthor = author;
        }
        int fCount = (int)r.Varint();
        for (int i = 0; i < fCount; i++)
        {
            var (fid, fs) = ReadFieldState(r);
            rec.State[fid] = fs;
        }
        int tCount = (int)r.Varint();
        for (int i = 0; i < tCount; i++)
        {
            var (fid, fs) = ReadFieldState(r);
            rec.FieldTombs[fid] = fs;
        }
        return (rid, rec);
    }

    private static void WriteFieldState(Writer w, ushort fid, FieldState fs)
    {
        w.U16(fid);
        w.U64(fs.Timestamp.Wall); w.U32(fs.Timestamp.Logical);
        w.B128(fs.AuthorNodeId);
        w.U16(fs.KeyEpoch);
        w.Bytes(fs.Value);
    }

    private static (ushort, FieldState) ReadFieldState(Reader r)
    {
        var fid = r.U16();
        var wall = r.U64(); var logical = r.U32();
        var author = r.B128();
        var keyEpoch = r.U16();
        var value = r.Bytes();
        return (fid, new FieldState(value, new Hlc(wall, logical), author, keyEpoch));
    }

    private static void WriteOutboxEntry(Writer w, OutboxEntry e)
    {
        w.Varint((ulong)e.NamespaceId);
        w.U64((ulong)e.Seq);
        w.B128(e.ChangeId);
        w.B128(e.RecordId);
        w.U8(e.OpKind);
        // The payload is a compact-mode op encoding; its value length is a varint, so
        // 0x00..0x7F bytes at the tail are ambiguous with an empty author-id slot. Store
        // the exact byte length explicitly and read it back raw (length-prefixed).
        w.Varint((ulong)e.Payload.Length);
        w.Raw(e.Payload);
        w.B128(e.AuthorNodeId);
        w.U64((ulong)e.CreatedAt);
        w.Varint((ulong)e.CausalDeps.Count);
        foreach (var d in e.CausalDeps) w.B128(d);
    }

    private static OutboxEntry ReadOutboxEntry(Reader r)
    {
        var e = new OutboxEntry
        {
            NamespaceId = (int)r.Varint(),
            Seq = (long)r.U64(),
            ChangeId = r.B128(),
            RecordId = r.B128(),
            OpKind = r.U8(),
            Payload = r.Raw((int)r.Varint()),
            AuthorNodeId = r.B128(),
            CreatedAt = (long)r.U64(),
        };
        int depCount = (int)r.Varint();
        for (int i = 0; i < depCount; i++) e.CausalDeps.Add(r.B128());
        return e;
    }

    // ------------------------------------------------------------- plumbing

    private static readonly Comparer<byte[]> BytesOrd = Comparer<byte[]>
        .Create((a, b) => HlcOps.BytesCompare(a, b));

    public static T GetField<T>(object obj, string name)
    {
        var f = obj.GetType().GetField(name, System.Reflection.BindingFlags.Instance |
                                            System.Reflection.BindingFlags.NonPublic);
        if (f is null) throw new InvalidOperationException($"Field '{name}' not found on {obj.GetType().Name}");
        return (T)f.GetValue(obj)!;
    }

    private static void WriteBE(BinaryWriter w, uint v)
    { Span<byte> s = stackalloc byte[4]; System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(s, v); w.Write(s); }
    private static void WriteBE(BinaryWriter w, ushort v)
    { Span<byte> s = stackalloc byte[2]; System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(s, v); w.Write(s); }
    private static void WriteBE(BinaryWriter w, ulong v)
    { Span<byte> s = stackalloc byte[8]; System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(s, v); w.Write(s); }
    private static uint ReadBEU32(BinaryReader r)
    { var b = r.ReadBytes(4); return System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(b); }
    private static ushort ReadBEU16(BinaryReader r)
    { var b = r.ReadBytes(2); return System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(b); }
    private static ulong ReadBEU64(BinaryReader r)
    { var b = r.ReadBytes(8); return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(b); }
}

/// <summary>Content-equality comparer for (byte[], int) keys used by NodeToAlias inner maps.</summary>
internal sealed class BAI2b : IEqualityComparer<(byte[], int)>
{
    public bool Equals((byte[], int) x, (byte[], int) y) =>
        x.Item2 == y.Item2 && (x.Item1 is null ? y.Item1 is null : y.Item1 is not null && x.Item1.AsSpan().SequenceEqual(y.Item1));
    public int GetHashCode((byte[], int) t)
    {
        unchecked
        {
            int h = 19;
            if (t.Item1 is not null) foreach (byte b in t.Item1) h = h * 31 + b;
            return h * 31 + t.Item2;
        }
    }
}
