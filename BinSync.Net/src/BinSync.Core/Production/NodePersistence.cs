// Production hardening (Шаг 3): atomic save/load of the FULL BinSync.Net node state.
//
// WAL-style durability contract:
//  * Integrity — the serialized body is covered by a domain-tagged BLAKE3 digest
//    ("BIN-SYNC/NODESTATE/v8", Blake3Core). Any bit flip,
//    truncation or version mismatch makes Load throw InvalidDataException.
//  * Atomicity — the image goes to a temp file in the same directory, fsync'd, then
//    File.Move(overwrite) renames it over the target; a crash can never leave a
//    half-written state file.
//  * Consistency — SerializeBody holds Storage → Outbox → Meta gates so the
//    image is a point-in-time snapshot even while other threads mutate the node.
//
// Persisted state mirrors every field of BinSync.Node:
//  identity (NodeId, Epoch), HLC clock (LastWall/LastLogical), Storage (ns -> recordId
//  -> Record with FieldState/HLC/ghost lifecycle + FieldTombs), Outbox per namespace,
//  per-(author,ns) outbox seq counters, unified batch receipts (key -> durable author), LastBatchDigest and
//  LastStreamDigest chain checkpoints, AliasToNode/NodeToAlias, DurableAuthors,
//  PeerStates (cursors + ReceivedStreams/StreamAcked/StreamPushed/Ids caches),
//  local SeenChanges WAL replay markers, GlobalChangeLedger (namespace + ChangeId -> canonical semantic fingerprint), LastStreamAuthor, KnownEpochs, Quarantined, ForbiddenNamespaces, SchemaVersions.
//
// Wire format: big-endian header (magic/version/bodyLen/digest), LE canonical body
// written with the core Writer/Reader codec (varint-prefixed byte keys keep content
// ordering deterministic via explicit sorting).
namespace BinSync;

/// <summary>Serialize and restore a Node to durable storage.</summary>
public static class NodePersistence
{
    /// <summary>Digest tag mandated by the spec.</summary>
    public const string StateTag = "BIN-SYNC/NODESTATE/v8";
    private const string Version7StateTag = "BIN-SYNC/NODESTATE/v7";
    private const string Version6StateTag = "BIN-SYNC/NODESTATE/v6";
    private const string Version5StateTag = "BIN-SYNC/NODESTATE/v5";
    private const string Version4StateTag = "BIN-SYNC/NODESTATE/v4";
    private const string Version3StateTag = "BIN-SYNC/NODESTATE/v3";
    private const string LegacyVersion3StateTag = "BIN-SYNC/NODESTATE/v2";

    private const uint Magic = 0x42534E53; // 'B''S''N''S'
    private const ushort Version = 8;
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
        if (ver != Version && ver != 7 && ver != 6 && ver != 5 && ver != 4 && ver != 3)
            throw new InvalidDataException($"Unsupported state version {ver} (expected {Version}, 7, 6, 5, 4 or legacy 3).");

        var bodyLen = ReadBEU64(r);
        if (bodyLen != (ulong)(bytes.Length - 14 - DigestLen))
            throw new InvalidDataException("State file length does not match the declared body length.");
        var storedDigest = r.ReadBytes(DigestLen);
        var body = r.ReadBytes((int)bodyLen);
        if (body.Length != (int)bodyLen)
            throw new InvalidDataException("State file truncated mid-body.");

        bool validDigest;
        if (ver == Version)
        {
            var actual = Blake3Core.TaggedHash(StateTag, body);
            validDigest = actual.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 7)
        {
            var v7 = Blake3Core.TaggedHash(Version7StateTag, body);
            validDigest = v7.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 6)
        {
            var v6 = Blake3Core.TaggedHash(Version6StateTag, body);
            validDigest = v6.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 5)
        {
            var v5 = Blake3Core.TaggedHash(Version5StateTag, body);
            validDigest = v5.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 4)
        {
            var v4 = Blake3Core.TaggedHash(Version4StateTag, body);
            validDigest = v4.AsSpan().SequenceEqual(storedDigest);
        }
        else
        {
            var v3 = Blake3Core.TaggedHash(Version3StateTag, body);
            var legacy = Blake3Core.TaggedHash(LegacyVersion3StateTag, body);
            validDigest = v3.AsSpan().SequenceEqual(storedDigest) || legacy.AsSpan().SequenceEqual(storedDigest);
        }
        if (!validDigest)
            throw new InvalidDataException("State digest mismatch — file corrupted (tagged BLAKE3 " + StateTag + ").");

        return DeserializeBody(body, ver);
    }

    // ------------------------------------------------------------- serialization

    internal static Node CloneForWalReplay(Node node)
    {
        ArgumentNullException.ThrowIfNull(node);
        var body = SerializeBody(node);
        var clone = DeserializeBody(body, Version, node.Clock.NowFn, node.Clock.MaxSkewUs, node.Limits);
        if (clone.PeersByNid is not null)
            foreach (var peerId in clone.PeersByNid.Keys.ToArray())
                clone.PeersByNid[peerId] = null!;
        clone.ErrorsOut.AddRange(node.ErrorsOut);
        clone.AcksOut.AddRange(node.AcksOut);
        return clone;
    }

    private static byte[] SerializeBody(Node n)
    {
        using (n.EnterAllGates()) // point-in-time image under all domain gates
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
                foreach (var (rid, rec) in recs.OrderBy(kv => kv.Key.ToString()))
                    WriteRecord(w, rid.ToArray(), rec);
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
            var seqMap = n.OutboxSeqRef;
            w.Varint((ulong)seqMap.Count);
            foreach (var ((author, ns), seq) in seqMap.OrderBy(kv => kv.Key.Item1.ToString()).ThenBy(kv => kv.Key.Item2))
            { w.B128(author); w.Varint((ulong)ns); w.U64((ulong)seq); }

            // --- unified accepted-batch receipt: issuer/ns/epoch/batchId -> durable author
            w.Varint((ulong)n.InboxAuthors.Count);
            foreach (var (key, author) in n.InboxAuthors.OrderBy(k => k.Key.Issuer.Lo).ThenBy(k => k.Key.Issuer.Hi)
                                                        .ThenBy(k => k.Key.Ns).ThenBy(k => k.Key.Epoch)
                                                        .ThenBy(k => k.Key.BatchId.Lo).ThenBy(k => k.Key.BatchId.Hi))
            {
                w.B128(key.Issuer); w.Varint((ulong)key.Ns); w.Varint((ulong)key.Epoch);
                w.B128(key.BatchId); w.B128(author.DurableAuthor);
                byte receiptFlags = (byte)((author.HasCursorEnd ? 1 : 0) |
                                           (author.HasFingerprint ? 2 : 0));
                w.U8(receiptFlags);
                if (author.HasCursorEnd) w.U64((ulong)author.CursorEnd);
                if (author.HasFingerprint) w.B128(author.Fingerprint);
                w.Varint(author.AppliedAt);
                w.Varint(author.ExpiresAt);
            }

            // --- batch-digest chain checkpoints: (issuer, ns, epoch) -> digest
            w.Varint((ulong)n.LastBatchDigest.Count);
            foreach (var ((issuer, ns, ep), dig) in n.LastBatchDigest.OrderBy(k => k.Key.Issuer.Lo).ThenBy(k => k.Key.Issuer.Hi)
                                                                     .ThenBy(k => k.Key.Ns).ThenBy(k => k.Key.Epoch))
            { w.B128(issuer); w.Varint((ulong)ns); w.Varint((ulong)ep); w.B128(dig); }

            // --- per-stream issuer checkpoints: ((issuer, ns, ep), peer) -> digest (§12.17.2)
            w.Varint((ulong)n.LastStreamDigest.Count);
            foreach (var (((issuer, ns, ep), peer), dig) in n.LastStreamDigest
                       .OrderBy(k => k.Key.Item1.Issuer.Lo).ThenBy(k => k.Key.Item1.Issuer.Hi)
                       .ThenBy(k => k.Key.Item1.Ns).ThenBy(k => k.Key.Item1.Epoch)
                       .ThenBy(k => k.Key.Peer.Lo).ThenBy(k => k.Key.Peer.Hi))
            { w.B128(issuer); w.Varint((ulong)ns); w.Varint((ulong)ep); w.B128(peer); w.B128(dig); }

            // --- aliases per session peer
            w.Varint((ulong)n.AliasToNode.Count);
            foreach (var (peer, map) in n.AliasToNode.OrderBy(k => k.Key.ToString()))
            {
                w.B128(peer.ToArray());
                w.Varint((ulong)map.Count);
                foreach (var (alias, (nid, ep)) in map.OrderBy(k => k.Key))
                { w.Varint((ulong)alias); w.B128(nid.ToArray()); w.Varint((ulong)ep); }
            }
            w.Varint((ulong)n.NodeToAlias.Count);
            foreach (var (peer, map) in n.NodeToAlias.OrderBy(k => k.Key.ToString()))
            {
                w.B128(peer.ToArray());
                w.Varint((ulong)map.Count);
                foreach (var ((nid, ep), alias) in map.OrderBy(k => k.Key.Id.ToString()).ThenBy(k => k.Key.Epoch))
                { w.B128(nid.ToArray()); w.Varint((ulong)ep); w.Varint((ulong)alias); }
            }

            // --- durable authors (Errata E-9)
            w.Varint((ulong)n.DurableAuthors.Count);
            foreach (var (nid, ep) in n.DurableAuthors.OrderBy(k => k.Key.ToString()))
            { w.B128(nid.ToArray()); w.Varint((ulong)ep); }

            // --- peer states
            w.Varint((ulong)n.PeerStates.Count);
            foreach (var ((peer, ns), ps) in n.PeerStates.OrderBy(k => k.Key.Item1.ToString()).ThenBy(k => k.Key.Item2))
            {
                w.B128(peer); w.Varint((ulong)ns);
                w.U64((ulong)ps.LastPushedSeq); w.U64((ulong)ps.LastReceivedSeq); w.U64((ulong)ps.LastAckedSeq);
                w.U8(ps.InitialSyncComplete ? (byte)1 : (byte)0);
                w.Varint((ulong)ps.MembershipEpoch); w.U64((ulong)ps.LastSeen);
                w.Varint((ulong)ps.ReceivedStreams.Count);
                foreach (var ((issuer, rns, rep, bid), seq) in ps.ReceivedStreams
                           .OrderBy(k => k.Key.Peer.Lo).ThenBy(k => k.Key.Peer.Hi).ThenBy(k => k.Key.Epoch).ThenBy(k => k.Key.Ns)
                           .ThenBy(k => k.Key.Author.Lo).ThenBy(k => k.Key.Author.Hi))
                { w.B128(issuer); w.Varint((ulong)rns); w.Varint((ulong)rep); w.B128(bid); w.U64((ulong)seq); }
                w.Varint((ulong)ps.StreamAcked.Count);
                foreach (var (author, seq) in ps.StreamAcked.OrderBy(k => k.Key.ToString()))
                { w.B128(author.ToArray()); w.U64((ulong)seq); }
                w.Varint((ulong)ps.StreamPushed.Count);
                foreach (var (author, seq) in ps.StreamPushed.OrderBy(k => k.Key.ToString()))
                { w.B128(author.ToArray()); w.U64((ulong)seq); }
                w.Varint((ulong)ps.StreamOffsets.Count);
                foreach (var (author, offset) in ps.StreamOffsets.OrderBy(k => k.Key.ToString()))
                { w.B128(author.ToArray()); w.U64((ulong)offset); }
                w.Varint((ulong)ps.Ids.Count);
                foreach (var ((author, start, end), (id, prev)) in ps.Ids
                           .OrderBy(k => k.Key.Author.Lo).ThenBy(k => k.Key.Author.Hi)
                           .ThenBy(k => k.Key.Start).ThenBy(k => k.Key.End))
                { w.B128(author); w.U64((ulong)start); w.U64((ulong)end); w.B128(id); w.B128(prev); }
            }

            // --- provenance seen-set (Errata E-10/E-13)
            w.Varint((ulong)n.SeenChanges.Count);
            foreach (var (peer, ns, cid) in n.SeenChanges.OrderBy(k => k.Peer.Lo).ThenBy(k => k.Peer.Hi).ThenBy(k => k.Ns)
                                                         .ThenBy(k => k.ChangeId.Lo).ThenBy(k => k.ChangeId.Hi))
            { w.B128(peer); w.Varint((ulong)ns); w.B128(cid); }

            // --- route-independent global ChangeId fingerprints
            w.Varint((ulong)n.GlobalChangeLedger.Count);
            foreach (var (key, value) in n.GlobalChangeLedger
                         .OrderBy(k => k.Key.Ns)
                         .ThenBy(k => k.Key.ChangeId.Lo).ThenBy(k => k.Key.ChangeId.Hi))
            {
                w.Varint((ulong)key.Ns);
                w.B128(key.ChangeId);
                w.U8(value.IsKnown ? (byte)1 : (byte)0);
                if (value.IsKnown) w.B128(value.Fingerprint);
            }

            // --- last accepted stream author (Errata E-11)
            w.Varint((ulong)n.LastStreamAuthor.Count);
            foreach (var ((peer, ns), author) in n.LastStreamAuthor.OrderBy(k => k.Key.Item1.ToString()).ThenBy(k => k.Key.Item2))
            { w.B128(peer); w.Varint((ulong)ns); w.B128(author); }

            // --- known epochs (mixed-key watermarks)
            w.Varint((ulong)n.KnownEpochs.Count);
            foreach (var ((key, ns), ep) in n.KnownEpochs.OrderBy(k => k.Key.Item1.ToString()).ThenBy(k => k.Key.Item2))
            { w.B128(key); w.Varint((ulong)((long)ns - int.MinValue)); w.Varint((ulong)ep); }

            // --- peer-wide HELLO incarnation epochs (separate from namespace watermarks)
            w.Varint((ulong)n.PeerSessionEpochs.Count);
            foreach (var (peer, ep) in n.PeerSessionEpochs.OrderBy(k => k.Key.ToString()))
            { w.B128(peer); w.Varint((ulong)ep); }

            // --- quarantine / policy sets
            w.Varint((ulong)n.Quarantined.Count);
            foreach (var nid in n.Quarantined.OrderBy(k => k.ToString())) w.B128(nid.ToArray());
            w.Varint((ulong)n.ForbiddenNamespaces.Count);
            foreach (var fns in n.ForbiddenNamespaces.OrderBy(v => v)) w.Varint((ulong)fns);
            w.Varint((ulong)n.SchemaVersions.Count);
            foreach (var (svNs, sv) in n.SchemaVersions.OrderBy(kv => kv.Key))
            { w.Varint((ulong)svNs); w.Varint((ulong)sv); }

            // --- session peer ids (E-11 registry). Only the KEY (peer nodeId) is serialized;
            // Node.PeersByNid maps nodeId -> live Node object and is not part of the durable
            // state image. Load() rebuilds the registry keys from these ids + AliasToNode keys.
            var peers = n.PeersByNid is null ? new List<NodeId>() : n.PeersByNid.Keys.ToList();
            w.Varint((ulong)peers.Count);
            foreach (var p in peers.OrderBy(k => k.ToString())) w.B128(p.ToArray());

            return w.Value();
        }
    }

    private static Node DeserializeBody(byte[] body, ushort version,
        Func<long>? nowFn = null, long maxSkewUs = 60_000_000, ResourceLimits? limits = null)
    {
        var r = new Reader(body);
        var nodeId = r.B128();
        var epoch = (int)r.Varint();
        var n = new Node(new NodeOptions
        {
            NodeId = nodeId,
            Epoch = epoch,
            NowFn = nowFn,
            MaxSkewUs = maxSkewUs,
            Limits = limits,
        });
        n.Epoch = epoch;

        n.Clock.LastWall = r.U64();
        n.Clock.LastLogical = r.U32();

        int nsCount = (int)r.Varint();
        for (int i = 0; i < nsCount; i++)
        {
            var ns = (int)r.Varint();
            int recCount = (int)r.Varint();
            var map = new Dictionary<RecordId, Record>();
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

        var seqMap = GetField<Dictionary<(NodeId, int), long>>(n, "_outboxSeq");
        int seqCount = (int)r.Varint();
        for (int i = 0; i < seqCount; i++)
        {
            var author = r.B128(); var ns = (int)r.Varint(); var seq = (long)r.U64();
            seqMap[((NodeId)author, ns)] = seq;
        }

        HashSet<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)>? legacyInbox = null;
        if (version >= 7)
        {
            int receiptCount = (int)r.Varint();
            for (int i = 0; i < receiptCount; i++)
            {
                var issuer = NodeId.From(r.B128());
                var ns = (int)r.Varint();
                var ep = (int)r.Varint();
                var batchId = BatchId.From(r.B128());
                var author = NodeId.From(r.B128());
                InboxReceipt receipt;
                if (version >= 8)
                {
                    byte receiptFlags = r.U8();
                    if (receiptFlags > 3 || ((receiptFlags & 2) != 0 && (receiptFlags & 1) == 0))
                        throw new InvalidDataException("Invalid persisted receipt flags.");
                    bool hasCursorEnd = (receiptFlags & 1) != 0;
                    long cursorEnd = hasCursorEnd ? (long)r.U64() : 0;
                    bool hasFingerprint = (receiptFlags & 2) != 0;
                    var fingerprint = hasFingerprint ? Digest16.From(r.B128()) : default;
                    var appliedAt = r.Varint();
                    var expiresAt = r.Varint();
                    if (cursorEnd < 0 || (expiresAt != 0 && expiresAt < appliedAt))
                        throw new InvalidDataException("Invalid persisted receipt cursor or expiry.");
                    receipt = new InboxReceipt(author, cursorEnd, hasCursorEnd,
                        fingerprint, hasFingerprint, appliedAt, expiresAt);
                }
                else
                    receipt = new InboxReceipt(author, cursorEnd: 0, hasCursorEnd: false,
                        fingerprint: default, hasFingerprint: false, appliedAt: 0, expiresAt: 0);
                var key = (issuer, ns, ep, batchId);
                if (issuer.IsZero || batchId.IsZero || author.IsZero ||
                    (uint)ns > ushort.MaxValue || ep < 0 || !n.InboxAuthors.TryAdd(key, receipt))
                    throw new InvalidDataException("Invalid or duplicate persisted batch receipt.");
            }
        }
        else
        {
            // v3-v6 stored the receipt key set and durable-author map separately.
            // Keep the old set only long enough to validate exact migration parity.
            legacyInbox = new HashSet<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)>();
            int inboxCount = (int)r.Varint();
            for (int i = 0; i < inboxCount; i++)
            {
                var issuer = NodeId.From(r.B128()); var ns = (int)r.Varint();
                var ep = (int)r.Varint(); var batchId = BatchId.From(r.B128());
                if (!legacyInbox.Add((issuer, ns, ep, batchId)))
                    throw new InvalidDataException("Duplicate legacy inbox receipt key.");
            }
            int inboxAuthorCount = (int)r.Varint();
            for (int i = 0; i < inboxAuthorCount; i++)
            {
                var issuer = NodeId.From(r.B128()); var ns = (int)r.Varint();
                var ep = (int)r.Varint(); var batchId = BatchId.From(r.B128());
                var author = NodeId.From(r.B128());
                var legacyReceipt = new InboxReceipt(author, cursorEnd: 0, hasCursorEnd: false,
                    fingerprint: default, hasFingerprint: false, appliedAt: 0, expiresAt: 0);
                if (!n.InboxAuthors.TryAdd((issuer, ns, ep, batchId), legacyReceipt))
                    throw new InvalidDataException("Duplicate legacy inbox author key.");
            }
        }

        int lbdCount = (int)r.Varint();
        for (int i = 0; i < lbdCount; i++)
        {
            var issuer = r.B128(); var ns = (int)r.Varint(); var ep = (int)r.Varint(); var dig = r.B128();
            n.LastBatchDigest[((NodeId)issuer, ns, ep)] = Digest16.From(dig);
        }

        int lsCount = (int)r.Varint();
        for (int i = 0; i < lsCount; i++)
        {
            var issuer = r.B128(); var ns = (int)r.Varint(); var ep = (int)r.Varint();
            var peer = r.B128(); var dig = r.B128();
            n.LastStreamDigest[(((NodeId)issuer, ns, ep), (NodeId)peer)] = Digest16.From(dig);
        }

        int a2nCount = (int)r.Varint();
        for (int i = 0; i < a2nCount; i++)
        {
            var peer = r.B128();
            int cnt = (int)r.Varint();
            var map = new Dictionary<int, (NodeId Id, int Epoch)>();
            for (int j = 0; j < cnt; j++)
            {
                var alias = (int)r.Varint(); var nid = r.B128(); var ep = (int)r.Varint();
                map[alias] = ((NodeId)nid, ep);
            }
            n.AliasToNode[(NodeId)peer] = map;
        }
        int n2aCount = (int)r.Varint();
        for (int i = 0; i < n2aCount; i++)
        {
            var peer = r.B128();
            int cnt = (int)r.Varint();
            var map = new Dictionary<(NodeId Id, int Epoch), int>();
            for (int j = 0; j < cnt; j++)
            {
                var nid = r.B128(); var ep = (int)r.Varint(); var alias = (int)r.Varint();
                map[((NodeId)nid, ep)] = alias;
            }
            n.NodeToAlias[(NodeId)peer] = map;
        }

        int daCount = (int)r.Varint();
        for (int i = 0; i < daCount; i++)
        {
            var nid = r.B128(); var ep = (int)r.Varint();
            n.DurableAuthors[(NodeId)nid] = ep;
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
                ps.ReceivedStreams[((NodeId)issuer, rns, rep, (NodeId)bid)] = seq;
            }
            int saCount = (int)r.Varint();
            for (int j = 0; j < saCount; j++)
            {
                var author = r.B128(); var seq = (long)r.U64();
                ps.StreamAcked[(NodeId)author] = seq;
            }
            int spCount = (int)r.Varint();
            for (int j = 0; j < spCount; j++)
            {
                var author = r.B128(); var seq = (long)r.U64();
                ps.StreamPushed[(NodeId)author] = seq;
            }
            if (version >= 4)
            {
                int offsetCount = (int)r.Varint();
                for (int j = 0; j < offsetCount; j++)
                {
                    var author = r.B128(); var offset = (long)r.U64();
                    ps.StreamOffsets[(NodeId)author] = offset;
                }
            }
            int idsCount = (int)r.Varint();
            for (int j = 0; j < idsCount; j++)
            {
                var author = r.B128(); var start = (long)r.U64(); var end = (long)r.U64();
                var id = r.B128(); var prev = r.B128();
                ps.Ids[((NodeId)author, start, end)] = (BatchId.From(id), Digest16.From(prev));
            }
            n.PeerStates[((NodeId)peer, ns)] = ps;
        }

        int seenCount = (int)r.Varint();
        var persistedSeen = new List<(NodeId Peer, int Ns, BatchId ChangeId)>(seenCount);
        for (int i = 0; i < seenCount; i++)
        {
            var peer = NodeId.From(r.B128());
            var ns = (int)r.Varint();
            var changeId = BatchId.From(r.B128());
            persistedSeen.Add((peer, ns, changeId));
        }
        // Only local WAL replay markers remain in SeenChanges. Remote provenance
        // is now covered by the global ChangeId ledger and need not be duplicated
        // once per sender peer.
        foreach (var (peer, ns, changeId) in persistedSeen)
            if (peer == nodeId)
                n.SeenChanges.Add((peer, ns, changeId));

        if (version >= 6)
        {
            int ledgerCount = (int)r.Varint();
            for (int i = 0; i < ledgerCount; i++)
            {
                int ns = (int)r.Varint();
                var changeId = BatchId.From(r.B128());
                byte known = r.U8();
                if (known > 1 || changeId.IsZero || (uint)ns > ushort.MaxValue)
                    throw new InvalidDataException("Invalid persisted global ChangeId ledger entry.");
                var value = known == 1
                    ? new ChangeLedgerValue(Digest16.From(r.B128()), isKnown: true)
                    : new ChangeLedgerValue(default, isKnown: false);
                if (!n.GlobalChangeLedger.TryAdd((ns, changeId), value))
                    throw new InvalidDataException("Duplicate persisted global ChangeId ledger key.");
            }
        }
        else
        {
            // v3-v5 only had route-scoped SeenChanges and no semantic fingerprint.
            // Preserve every historical identity conservatively as an explicit
            // unknown marker before dropping redundant remote peer provenance.
            foreach (var (_, ns, changeId) in persistedSeen)
                n.GlobalChangeLedger.TryAdd((ns, changeId),
                    new ChangeLedgerValue(default, isKnown: false));
        }

        if (version < 6)
        {
            // A retained legacy Outbox payload is sufficient to reconstruct its
            // canonical fingerprint. Only compacted or deferred-value entries stay unknown.
            foreach (var (ns, entries) in n.Outbox)
            {
                foreach (var entry in entries)
                {
                    if (entry.ChangeId.IsZero) continue;
                    var key = (ns, entry.ChangeId);
                    if (!n.GlobalChangeLedger.ContainsKey(key))
                        n.GlobalChangeLedger.Add(key, new ChangeLedgerValue(default, isKnown: false));
                    if (!ChangeIdentity.TryFingerprint(entry, out var fingerprint))
                        continue; // keep the explicit unknown marker for deferred payloads
                    if (n.GlobalChangeLedger.TryGetValue(key, out var oldValue) &&
                        oldValue.IsKnown && oldValue.Fingerprint != fingerprint)
                        throw new InvalidDataException("Legacy Outbox contains conflicting payloads for one ChangeId.");
                    n.GlobalChangeLedger[key] = new ChangeLedgerValue(fingerprint, isKnown: true);
                }
            }
        }

        int lsaCount = (int)r.Varint();
        for (int i = 0; i < lsaCount; i++)
        {
            var peer = r.B128(); var ns = (int)r.Varint(); var author = r.B128();
            n.LastStreamAuthor[((NodeId)peer, ns)] = NodeId.From(author);
        }

        int keCount = (int)r.Varint();
        for (int i = 0; i < keCount; i++)
        {
            var key = r.B128();
            var ns = (int)((long)r.Varint() + int.MinValue);   // zig: stored as ns - int.MinValue
            var ep = (int)r.Varint();
            n.KnownEpochs[((NodeId)key, ns)] = ep;
        }

        if (version >= 5)
        {
            int peerEpochCount = (int)r.Varint();
            for (int i = 0; i < peerEpochCount; i++)
            {
                var peer = NodeId.From(r.B128());
                var peerEpoch = (int)r.Varint();
                if (peer.IsZero || peerEpoch < 0)
                    throw new InvalidDataException("Invalid persisted peer session epoch.");
                n.PeerSessionEpochs[peer] = peerEpoch;
            }
        }
        else
        {
            // v3/v4 stored HELLO epochs in the namespace-0 slot. Preserve that
            // effective legacy baseline while retaining the slot as namespace 0.
            foreach (var ((peer, ns), peerEpoch) in n.KnownEpochs)
                if (ns == 0 && !peer.IsZero)
                    n.PeerSessionEpochs[peer] = peerEpoch;
        }

        int qCount = (int)r.Varint();
        for (int i = 0; i < qCount; i++) n.Quarantined.Add((NodeId)r.B128());
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
            n.PeersByNid ??= new Dictionary<NodeId, Node>();
            // NOTE: `existing is null` must be a plain reference check — Node derives from
            // MarshalByRefObject so its == operator does NOT test for null.
            foreach (var p in peerIds.Select(x => (NodeId)x).Concat(n.AliasToNode.Keys))
                if (!n.PeersByNid.TryGetValue(p, out var existing) || ReferenceEquals(existing, null))
                    n.PeersByNid[p] = null!;
        }

        if (!r.Eof())
            throw new InvalidDataException("Trailing bytes in node-state body.");

        // v3-v6 carried duplicate receipt keys. Require exact key parity during
        // migration; v7 stores each key and its ACK author in one atomic map entry.
        if (legacyInbox is not null)
        {
            if (n.InboxAuthors.Count != legacyInbox.Count)
                throw new InvalidDataException("Legacy inbox author map does not match the inbox.");
            foreach (var inboxKey in legacyInbox)
            {
                if (!n.InboxAuthors.TryGetValue(inboxKey, out var receipt) || receipt.DurableAuthor.IsZero)
                    throw new InvalidDataException("Legacy inbox receipt is missing its durable author.");
            }
            foreach (var (inboxKey, receipt) in n.InboxAuthors)
            {
                if (!legacyInbox.Contains(inboxKey) || receipt.DurableAuthor.IsZero)
                    throw new InvalidDataException("Legacy inbox author map contains an orphan or zero author.");
            }
        }
        else
        {
            foreach (var (key, receipt) in n.InboxAuthors)
                if (key.Issuer.IsZero || key.BatchId.IsZero || receipt.DurableAuthor.IsZero ||
                    (uint)key.Ns > ushort.MaxValue || key.Epoch < 0 ||
                    (receipt.HasCursorEnd && receipt.CursorEnd < 0) ||
                    (receipt.HasFingerprint && !receipt.HasCursorEnd) ||
                    (receipt.ExpiresAt != 0 && receipt.ExpiresAt < receipt.AppliedAt))
                    throw new InvalidDataException("Invalid persisted batch receipt.");
        }

        foreach (var ps in n.PeerStates.Values)
        {
            foreach (var (author, acked) in ps.StreamAcked)
            {
                if (author.IsZero || acked < 0 ||
                    !ps.StreamPushed.TryGetValue(author, out var pushed) || pushed < acked)
                    throw new InvalidDataException("Invalid persisted ACK/pushed watermark.");
            }
            foreach (var (author, pushed) in ps.StreamPushed)
            {
                if (author.IsZero || pushed < 0)
                    throw new InvalidDataException("Invalid persisted pushed watermark.");
            }
            foreach (var (author, offset) in ps.StreamOffsets)
            {
                if (author.IsZero || offset < 0 ||
                    (ps.StreamAcked.TryGetValue(author, out var acked) && acked > long.MaxValue - offset) ||
                    (ps.StreamPushed.TryGetValue(author, out var pushed) && pushed > long.MaxValue - offset))
                    throw new InvalidDataException("Invalid persisted stream offset.");
            }
        }

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
            w.B128(rec.LifecycleAuthor ?? default);
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
            rec.LifecycleAuthor = NodeId.From(author);
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
        return (fid, new FieldState(value, new Hlc(wall, logical), NodeId.From(author), keyEpoch));
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
        w.Raw(e.Payload.Span);
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
            ChangeId = BatchId.From(r.B128()),
            RecordId = RecordId.From(r.B128()),
            OpKind = r.U8(),
            Payload = r.Raw((int)r.Varint()),
            AuthorNodeId = NodeId.From(r.B128()),
            CreatedAt = (long)r.U64(),
        };
        int depCount = (int)r.Varint();
        for (int i = 0; i < depCount; i++) e.CausalDeps.Add(BatchId.From(r.B128()));
        return e;
    }

    // ------------------------------------------------------------- plumbing

    private static readonly Comparer<byte[]> BytesOrd = Comparer<byte[]>
        .Create((a, b) => HlcOps.BytesCompare(a, b));

    /// <summary>Read a private/public field by name via reflection (persistence helpers).</summary>
    internal static T GetField<T>(object obj, string name)
    {
        var f = obj.GetType().GetField(name, System.Reflection.BindingFlags.Instance |
                                            System.Reflection.BindingFlags.NonPublic);
        if (f is null) throw new InvalidOperationException($"Field '{name}' not found on {obj.GetType().Name}");
        return (T)f.GetValue(obj)!;
    }

    private static void WriteBE(BinaryWriter w, uint v)
    {
        Span<byte> s = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(s, v);
        w.Write(s);
    }

    private static void WriteBE(BinaryWriter w, ushort v)
    {
        Span<byte> s = stackalloc byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(s, v);
        w.Write(s);
    }

    private static void WriteBE(BinaryWriter w, ulong v)
    {
        Span<byte> s = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(s, v);
        w.Write(s);
    }

    private static uint ReadBEU32(BinaryReader r)
    {
        var b = r.ReadBytes(4);
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(b);
    }

    private static ushort ReadBEU16(BinaryReader r)
    {
        var b = r.ReadBytes(2);
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(b);
    }

    private static ulong ReadBEU64(BinaryReader r)
    {
        var b = r.ReadBytes(8);
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(b);
    }
}

