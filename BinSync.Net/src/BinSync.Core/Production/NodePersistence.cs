// Production hardening (Шаг 3): atomic save/load of the FULL BinSync.Net node state.
//
// WAL-style durability contract:
//  * Integrity — the serialized body is covered by a domain-tagged BLAKE3 digest
//    ("BIN-SYNC/NODESTATE/v15", Blake3Core). Any bit flip,
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
//  LastStreamDigest chain checkpoints, AliasToNode (reverse NodeToAlias is rebuilt on load), DurableAuthors,
//  PeerStates (compact per-author progress, ReceivedStreams and replay Ids caches),
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
    public const string StateTag = "BIN-SYNC/NODESTATE/v16";
    private const string Version14StateTag = "BIN-SYNC/NODESTATE/v14";
    private const string Version13StateTag = "BIN-SYNC/NODESTATE/v13";
    private const string Version12StateTag = "BIN-SYNC/NODESTATE/v12";
    private const string Version11StateTag = "BIN-SYNC/NODESTATE/v11";
    private const string Version10StateTag = "BIN-SYNC/NODESTATE/v10";
    private const string Version9StateTag = "BIN-SYNC/NODESTATE/v9";
    private const string Version8StateTag = "BIN-SYNC/NODESTATE/v8";
    private const string Version7StateTag = "BIN-SYNC/NODESTATE/v7";
    private const string Version6StateTag = "BIN-SYNC/NODESTATE/v6";
    private const string Version5StateTag = "BIN-SYNC/NODESTATE/v5";
    private const string Version4StateTag = "BIN-SYNC/NODESTATE/v4";
    private const string Version3StateTag = "BIN-SYNC/NODESTATE/v3";
    private const string LegacyVersion3StateTag = "BIN-SYNC/NODESTATE/v2";

    private const uint Magic = 0x42534E53; // 'B''S''N''S'
    private const ushort Version = 16;
    private const int DigestLen = 32;

    // ---------------------------------------------------------------- Save

    /// <summary>Atomically persist <paramref name="node"/> to <paramref name="path"/>.</summary>
    public static void Save(Node node, string path)
    {
        if (node is null) throw new ArgumentNullException(nameof(node));
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path required.", nameof(path));

        var image = SerializeToBytes(node);

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

    /// <summary>
    /// Serialize the complete Node checkpoint into an owned, integrity-protected image.
    /// This is the same v15 image written by Save and can be stored atomically in a
    /// caller-owned StorageDatabase or another durable byte store.
    /// </summary>
    public static byte[] SerializeToBytes(Node node)
    {
        ArgumentNullException.ThrowIfNull(node);
        var body = SerializeBody(node);
        var digest = Blake3Core.TaggedHash(StateTag, body);

        using var ms = new MemoryStream();
        using (var writer = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            WriteBE(writer, Magic);
            WriteBE(writer, Version);
            WriteBE(writer, (ulong)body.Length);
            writer.Write(digest);
            writer.Write(body);
            writer.Flush();
        }
        return ms.ToArray();
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
            throw new InvalidDataException("Unsupported state version " + ver + "; BIN-SYNC v3 requires state version " + Version + ". Recreate the node store.");

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
        else if (ver == 14)
        {
            var v14 = Blake3Core.TaggedHash(Version14StateTag, body);
            validDigest = v14.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 13)
        {
            var v13 = Blake3Core.TaggedHash(Version13StateTag, body);
            validDigest = v13.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 12)
        {
            var v12 = Blake3Core.TaggedHash(Version12StateTag, body);
            validDigest = v12.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 11)
        {
            var v11 = Blake3Core.TaggedHash(Version11StateTag, body);
            validDigest = v11.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 10)
        {
            var v10 = Blake3Core.TaggedHash(Version10StateTag, body);
            validDigest = v10.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 9)
        {
            var v9 = Blake3Core.TaggedHash(Version9StateTag, body);
            validDigest = v9.AsSpan().SequenceEqual(storedDigest);
        }
        else if (ver == 8)
        {
            var v8 = Blake3Core.TaggedHash(Version8StateTag, body);
            validDigest = v8.AsSpan().SequenceEqual(storedDigest);
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
                WriteOutboxEntries(w, ns, list);
            }

            // --- per-(author, ns) outbox seq counters [П1]
            var seqMap = n.OutboxSeqRef;
            w.Varint((ulong)seqMap.Count);
            foreach (var ((author, ns), seq) in seqMap.OrderBy(kv => kv.Key.Item1.ToString()).ThenBy(kv => kv.Key.Item2))
            { w.B128(author); w.Varint((ulong)ns); WriteNonNegativeVarint(w, seq, "Outbox sequence counter"); }

            // --- accepted-batch receipts grouped by issuer/namespace/epoch.
            // Use author subgroups only when removing repeated 16-byte authors beats
            // the extra subgroup-count varints; singleton-heavy sessions stay flat.
            var receiptGroups = n.InboxAuthors
                .GroupBy(k => (k.Key.Issuer, k.Key.Ns, k.Key.Epoch))
                .OrderBy(g => g.Key.Issuer.Lo).ThenBy(g => g.Key.Issuer.Hi)
                .ThenBy(g => g.Key.Ns).ThenBy(g => g.Key.Epoch).ToList();
            w.Varint((ulong)receiptGroups.Count);
            foreach (var group in receiptGroups)
            {
                w.B128(group.Key.Issuer);
                w.Varint((ulong)group.Key.Ns);
                w.Varint((ulong)group.Key.Epoch);
                var groupReceipts = group.ToArray();
                var authorGroups = groupReceipts.GroupBy(k => k.Value.DurableAuthor)
                    .OrderBy(g => g.Key.Lo).ThenBy(g => g.Key.Hi)
                    .Select(g => new
                    {
                        Author = g.Key,
                        Receipts = g.OrderBy(k => k.Key.BatchId.Lo)
                            .ThenBy(k => k.Key.BatchId.Hi).ToArray()
                    }).ToArray();
                int flatCountBytes = Codec.VarintLength((ulong)groupReceipts.Length);
                int groupedCountBytes = Codec.VarintLength((ulong)authorGroups.Length);
                foreach (var authorGroup in authorGroups)
                    groupedCountBytes += Codec.VarintLength((ulong)authorGroup.Receipts.Length);
                long repeatedAuthorBytes = 16L * (groupReceipts.Length - authorGroups.Length);
                bool useAuthorGroups = repeatedAuthorBytes > groupedCountBytes - flatCountBytes + 1;

                w.U8(useAuthorGroups ? (byte)1 : (byte)0);
                if (!useAuthorGroups)
                {
                    var receipts = groupReceipts.OrderBy(k => k.Key.BatchId.Lo)
                        .ThenBy(k => k.Key.BatchId.Hi).ToArray();
                    w.Varint((ulong)receipts.Length);
                    foreach (var (key, receipt) in receipts)
                    {
                        w.B128(key.BatchId); w.B128(receipt.DurableAuthor);
                        WriteReceiptValue(w, receipt);
                    }
                }
                else
                {
                    w.Varint((ulong)authorGroups.Length);
                    foreach (var authorGroup in authorGroups)
                    {
                        w.B128(authorGroup.Author);
                        w.Varint((ulong)authorGroup.Receipts.Length);
                        foreach (var (key, receipt) in authorGroup.Receipts)
                        {
                            w.B128(key.BatchId);
                            WriteReceiptValue(w, receipt);
                        }
                    }
                }
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
            // NodeToAlias is a deterministic reverse index of AliasToNode. Keep it in memory
            // for O(1) reverse lookup, but do not persist the same bindings twice. v13 rebuilds it.

            // --- durable authors (Errata E-9)
            w.Varint((ulong)n.DurableAuthors.Count);
            foreach (var (nid, ep) in n.DurableAuthors.OrderBy(k => k.Key.ToString()))
            { w.B128(nid.ToArray()); w.Varint((ulong)ep); }

            // --- peer states
            w.Varint((ulong)n.PeerStates.Count);
            foreach (var ((peer, ns), ps) in n.PeerStates.OrderBy(k => k.Key.Item1.ToString()).ThenBy(k => k.Key.Item2))
            {
                w.B128(peer); w.Varint((ulong)ns);
                WriteNonNegativeVarint(w, ps.LastPushedSeq, "peer pushed sequence");
                WriteNonNegativeVarint(w, ps.LastReceivedSeq, "peer received sequence");
                w.U8(ps.InitialSyncComplete ? (byte)1 : (byte)0);
                w.Varint((ulong)ps.MembershipEpoch); WriteNonNegativeVarint(w, ps.LastSeen, "peer last-seen time");
                w.Varint((ulong)ps.ReceivedStreams.Count);
                foreach (var ((streamPeer, epoch, author), seq) in ps.ReceivedStreams
                           .OrderBy(k => k.Key.Peer.Lo).ThenBy(k => k.Key.Peer.Hi)
                           .ThenBy(k => k.Key.Epoch).ThenBy(k => k.Key.Author.Lo).ThenBy(k => k.Key.Author.Hi))
                { w.B128(streamPeer); w.Varint((ulong)epoch); w.B128(author); WriteNonNegativeVarint(w, seq, "received stream cursor"); }
                w.Varint((ulong)ps.AuthorProgress.Count);
                foreach (var (author, progress) in ps.AuthorProgress.OrderBy(k => k.Key.ToString()))
                {
                    w.B128(author);
                    w.U8(progress.Flags);
                    if (progress.HasAcked) WriteNonNegativeVarint(w, progress.Acked, "author ACK watermark");
                    if (progress.HasPushed) WriteNonNegativeVarint(w, progress.Pushed, "author pushed watermark");
                    if (progress.HasOffset) WriteNonNegativeVarint(w, progress.Offset, "author stream offset");
                }
                w.Varint((ulong)ps.Ids.Count);
                foreach (var ((author, start, end), (id, prev)) in ps.Ids
                           .OrderBy(k => k.Key.Author.Lo).ThenBy(k => k.Key.Author.Hi)
                           .ThenBy(k => k.Key.Start).ThenBy(k => k.Key.End))
                { w.B128(author); WriteNonNegativeVarint(w, start, "replay range start"); WriteNonNegativeVarint(w, end, "replay range end"); w.B128(id); w.B128(prev); }
            }

            // --- local WAL replay markers; NodeId is implicit in this state image.
            w.Varint((ulong)n.SeenChanges.Count);
            foreach (var (ns, cid) in n.SeenChanges.OrderBy(k => k.Ns)
                                                   .ThenBy(k => k.ChangeId.Lo).ThenBy(k => k.ChangeId.Hi))
            { w.Varint((ulong)ns); w.B128(cid); }

            // --- route-independent ChangeId fingerprints grouped by namespace.
            // Namespace is repeated once per group, not once per operation.
            var ledgerGroups = n.GlobalChangeLedger.GroupBy(k => k.Key.Ns)
                .OrderBy(g => g.Key).ToList();
            w.Varint((ulong)ledgerGroups.Count);
            foreach (var group in ledgerGroups)
            {
                w.Varint((ulong)group.Key);
                var entries = group.OrderBy(k => k.Key.ChangeId.Lo)
                    .ThenBy(k => k.Key.ChangeId.Hi).ToArray();
                w.Varint((ulong)entries.Length);
                foreach (var (key, value) in entries)
                {
                    w.B128(key.ChangeId);
                    w.U8(value.IsKnown ? (byte)1 : (byte)0);
                    if (value.IsKnown) w.B128(value.Fingerprint);
                }
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
        var epoch = ReadInt32Varint(r);
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

        int nsCount = ReadCount(r, 2, "namespace");
        for (int i = 0; i < nsCount; i++)
        {
            var ns = ReadNamespaceId(r);
            int recCount = ReadCount(r, 21, "record");
            var map = new Dictionary<RecordId, Record>();
            for (int j = 0; j < recCount; j++)
            {
                var (rid, rec) = ReadRecord(r);
                map[rid] = rec;
            }
            n.Storage[ns] = map;
        }

        int outNsCount = ReadCount(r, 2, "outbox namespace");
        for (int i = 0; i < outNsCount; i++)
        {
            var ns = ReadNamespaceId(r);
            int cnt = ReadCount(r, version >= 15 ? 52 : version >= 14 ? 59 : 68, "outbox entry");
            n.Outbox[ns] = version >= 14
                ? ReadOutboxEntries(r, ns, cnt, version)
                : ReadLegacyOutboxEntries(r, cnt);
        }

        var seqMap = GetField<Dictionary<(NodeId, int), long>>(n, "_outboxSeq");
        int seqCount = ReadCount(r, version >= 15 ? 18 : 25, "outbox sequence");
        for (int i = 0; i < seqCount; i++)
        {
            var author = r.B128(); var ns = ReadNamespaceId(r);
            var seq = version >= 15 ? ReadInt64Varint(r, "Outbox sequence counter") : (long)r.U64();
            seqMap[((NodeId)author, ns)] = seq;
        }

        HashSet<(NodeId Issuer, int Ns, int Epoch, BatchId BatchId)>? legacyInbox = null;
        if (version >= 12)
        {
            int groupCount = ReadCount(r, 20, "receipt group");
            var receiptGroupKeys = new HashSet<(NodeId Issuer, int Ns, int Epoch)>();
            for (int gi = 0; gi < groupCount; gi++)
            {
                var issuer = NodeId.From(r.B128());
                var ns = ReadNamespaceId(r);
                var ep = ReadInt32Varint(r);
                byte layout = r.U8();
                if (issuer.IsZero || !receiptGroupKeys.Add((issuer, ns, ep)))
                    throw new InvalidDataException("Invalid or duplicate persisted receipt group.");

                if (layout == 0)
                {
                    int receiptCount = ReadCount(r, version >= 15 ? 34 : 35, "flat batch receipt");
                    if (receiptCount == 0)
                        throw new InvalidDataException("Empty persisted receipt group.");
                    for (int i = 0; i < receiptCount; i++)
                    {
                        var batchId = BatchId.From(r.B128());
                        var author = NodeId.From(r.B128());
                        var receipt = ReadReceiptValue(r, author, batchId, version);
                        if (!n.InboxAuthors.TryAdd((issuer, ns, ep, batchId), receipt))
                            throw new InvalidDataException("Duplicate persisted batch receipt.");
                    }
                }
                else if (layout == 1)
                {
                    int authorGroupCount = ReadCount(r, 17, "receipt author group");
                    if (authorGroupCount == 0)
                        throw new InvalidDataException("Empty persisted receipt author groups.");
                    var receiptAuthors = new HashSet<NodeId>();
                    for (int ai = 0; ai < authorGroupCount; ai++)
                    {
                        var author = NodeId.From(r.B128());
                        int receiptCount = ReadCount(r, version >= 15 ? 18 : 19, "batch receipt");
                        if (author.IsZero || receiptCount == 0 || !receiptAuthors.Add(author))
                            throw new InvalidDataException("Invalid or duplicate persisted receipt author group.");
                        for (int i = 0; i < receiptCount; i++)
                        {
                            var batchId = BatchId.From(r.B128());
                            var receipt = ReadReceiptValue(r, author, batchId, version);
                            if (!n.InboxAuthors.TryAdd((issuer, ns, ep, batchId), receipt))
                                throw new InvalidDataException("Duplicate persisted batch receipt.");
                        }
                    }
                }
                else
                    throw new InvalidDataException("Unknown persisted receipt group layout.");
            }
        }
        else if (version >= 10)
        {
            int groupCount = ReadCount(r, 19, "receipt group");
            var receiptGroupKeys = new HashSet<(NodeId Issuer, int Ns, int Epoch)>();
            for (int gi = 0; gi < groupCount; gi++)
            {
                var issuer = NodeId.From(r.B128());
                var ns = ReadNamespaceId(r);
                var ep = ReadInt32Varint(r);
                int receiptCount = ReadCount(r, 35, "batch receipt");
                if (receiptCount == 0 || issuer.IsZero ||
                    !receiptGroupKeys.Add((issuer, ns, ep)))
                    throw new InvalidDataException("Invalid or duplicate persisted receipt group.");
                for (int i = 0; i < receiptCount; i++)
                {
                    var batchId = BatchId.From(r.B128());
                    var author = NodeId.From(r.B128());
                    byte receiptFlags = r.U8();
                    if (receiptFlags > 3 || ((receiptFlags & 2) != 0 && (receiptFlags & 1) == 0))
                        throw new InvalidDataException("Invalid persisted receipt flags.");
                    bool hasCursorEnd = (receiptFlags & 1) != 0;
                    long cursorEnd = hasCursorEnd ? (long)r.Varint() : 0;
                    bool hasFingerprint = (receiptFlags & 2) != 0;
                    var fingerprint = hasFingerprint ? Digest16.From(r.B128()) : default;
                    var appliedAt = r.Varint();
                    var expiresAt = r.Varint();
                    if (batchId.IsZero || author.IsZero || cursorEnd < 0 ||
                        (expiresAt != 0 && expiresAt < appliedAt))
                        throw new InvalidDataException("Invalid persisted receipt cursor or expiry.");
                    var receipt = new InboxReceipt(author, cursorEnd, hasCursorEnd,
                        fingerprint, hasFingerprint, appliedAt, expiresAt);
                    if (!n.InboxAuthors.TryAdd((issuer, ns, ep, batchId), receipt))
                        throw new InvalidDataException("Duplicate persisted batch receipt.");
                }
            }
        }
        else if (version >= 7)
        {
            int receiptCount = ReadCount(r, 50, "legacy receipt");
            for (int i = 0; i < receiptCount; i++)
            {
                var issuer = NodeId.From(r.B128());
                var ns = ReadNamespaceId(r);
                var ep = ReadInt32Varint(r);
                var batchId = BatchId.From(r.B128());
                var author = NodeId.From(r.B128());
                InboxReceipt receipt;
                if (version >= 8)
                {
                    byte receiptFlags = r.U8();
                    if (receiptFlags > 3 || ((receiptFlags & 2) != 0 && (receiptFlags & 1) == 0))
                        throw new InvalidDataException("Invalid persisted receipt flags.");
                    bool hasCursorEnd = (receiptFlags & 1) != 0;
                    long cursorEnd = hasCursorEnd ? (long)r.Varint() : 0;
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
            int inboxCount = ReadCount(r, 34, "legacy inbox key");
            for (int i = 0; i < inboxCount; i++)
            {
                var issuer = NodeId.From(r.B128()); var ns = ReadNamespaceId(r);
                var ep = ReadInt32Varint(r); var batchId = BatchId.From(r.B128());
                if (!legacyInbox.Add((issuer, ns, ep, batchId)))
                    throw new InvalidDataException("Duplicate legacy inbox receipt key.");
            }
            int inboxAuthorCount = ReadCount(r, 50, "legacy inbox author");
            for (int i = 0; i < inboxAuthorCount; i++)
            {
                var issuer = NodeId.From(r.B128()); var ns = ReadNamespaceId(r);
                var ep = ReadInt32Varint(r); var batchId = BatchId.From(r.B128());
                var author = NodeId.From(r.B128());
                var legacyReceipt = new InboxReceipt(author, cursorEnd: 0, hasCursorEnd: false,
                    fingerprint: default, hasFingerprint: false, appliedAt: 0, expiresAt: 0);
                if (!n.InboxAuthors.TryAdd((issuer, ns, ep, batchId), legacyReceipt))
                    throw new InvalidDataException("Duplicate legacy inbox author key.");
            }
        }

        int lbdCount = ReadCount(r, 34, "last batch digest");
        for (int i = 0; i < lbdCount; i++)
        {
            var issuer = r.B128(); var ns = ReadNamespaceId(r); var ep = ReadInt32Varint(r); var dig = r.B128();
            n.LastBatchDigest[((NodeId)issuer, ns, ep)] = Digest16.From(dig);
        }

        int lsCount = ReadCount(r, 50, "last stream digest");
        for (int i = 0; i < lsCount; i++)
        {
            var issuer = r.B128(); var ns = ReadNamespaceId(r); var ep = ReadInt32Varint(r);
            var peer = r.B128(); var dig = r.B128();
            n.LastStreamDigest[(((NodeId)issuer, ns, ep), (NodeId)peer)] = Digest16.From(dig);
        }

        int a2nCount = ReadCount(r, 17, "alias peer");
        for (int i = 0; i < a2nCount; i++)
        {
            var peer = r.B128();
            int cnt = ReadCount(r, 18, "peer alias");
            var map = new Dictionary<int, (NodeId Id, int Epoch)>();
            for (int j = 0; j < cnt; j++)
            {
                var alias = ReadInt32Varint(r); var nid = r.B128(); var ep = ReadInt32Varint(r);
                map[alias] = ((NodeId)nid, ep);
            }
            n.AliasToNode[(NodeId)peer] = map;
        }
        if (version < 13)
        {
            int n2aCount = ReadCount(r, 17, "node alias peer");
            for (int i = 0; i < n2aCount; i++)
            {
                var peer = r.B128();
                int cnt = ReadCount(r, 18, "node alias");
                var map = new Dictionary<(NodeId Id, int Epoch), int>();
                for (int j = 0; j < cnt; j++)
                {
                    var nid = r.B128(); var ep = ReadInt32Varint(r); var alias = ReadInt32Varint(r);
                    map[((NodeId)nid, ep)] = alias;
                }
                n.NodeToAlias[(NodeId)peer] = map;
            }
        }
        else
        {
            // The forward map is authoritative in v13. Rebuild and validate the
            // in-memory reverse index rather than persisting duplicate bindings.
            foreach (var (peer, forward) in n.AliasToNode)
            {
                if (peer.IsZero)
                    throw new InvalidDataException("Zero peer in persisted alias map.");
                var reverse = new Dictionary<(NodeId Id, int Epoch), int>(forward.Count);
                foreach (var (alias, binding) in forward)
                {
                    if (alias <= 0 || binding.Id.IsZero || binding.Epoch < 0 ||
                        !reverse.TryAdd(binding, alias))
                        throw new InvalidDataException("Invalid or conflicting persisted alias binding.");
                }
                n.NodeToAlias.Add(peer, reverse);
            }
        }

        int daCount = ReadCount(r, 17, "durable author");
        for (int i = 0; i < daCount; i++)
        {
            var nid = r.B128(); var ep = ReadInt32Varint(r);
            n.DurableAuthors[(NodeId)nid] = ep;
        }

        int psCount = ReadCount(r, version >= 15 ? 25 : 50, "peer state");
        for (int i = 0; i < psCount; i++)
        {
            var peer = r.B128(); var ns = ReadNamespaceId(r);
            long lastPushedSeq, lastReceivedSeq, lastAckedSeq;
            if (version >= 15)
            {
                lastPushedSeq = ReadInt64Varint(r, "peer pushed sequence");
                lastReceivedSeq = ReadInt64Varint(r, "peer received sequence");
                lastAckedSeq = 0; // v15 derives this compatibility mirror from per-author progress
            }
            else
            {
                lastPushedSeq = (long)r.U64();
                lastReceivedSeq = (long)r.U64();
                lastAckedSeq = (long)r.U64();
            }
            bool initialSyncComplete = r.U8() != 0;
            int membershipEpoch = ReadInt32Varint(r);
            long lastSeen = version >= 15
                ? ReadInt64Varint(r, "peer last-seen time")
                : (long)r.U64();
            var ps = new PeerState(peer, ns)
            {
                LastPushedSeq = lastPushedSeq,
                LastReceivedSeq = lastReceivedSeq,
                LastAckedSeq = lastAckedSeq,
                InitialSyncComplete = initialSyncComplete,
                MembershipEpoch = membershipEpoch,
                LastSeen = lastSeen,
            };
            int rsCount = ReadCount(r, version >= 15 ? 34 : 41, "received stream");
            for (int j = 0; j < rsCount; j++)
            {
                NodeId streamPeer;
                int streamEpoch;
                NodeId streamAuthor;
                long seq;
                if (version >= 9)
                {
                    streamPeer = NodeId.From(r.B128());
                    streamEpoch = ReadInt32Varint(r);
                    streamAuthor = NodeId.From(r.B128());
                    seq = version >= 15 ? ReadInt64Varint(r, "received stream cursor") : (long)r.U64();
                }
                else
                {
                    streamPeer = NodeId.From(r.B128());
                    var rns = ReadNamespaceId(r);
                    streamEpoch = ReadInt32Varint(r);
                    streamAuthor = NodeId.From(r.B128());
                    seq = (long)r.U64();
                    if (rns != ns)
                        throw new InvalidDataException("Received-stream namespace does not match its PeerState.");
                }
                if (streamPeer.IsZero || streamEpoch < 0 || streamAuthor.IsZero || seq < 0 ||
                    !ps.ReceivedStreams.TryAdd((streamPeer, streamEpoch, streamAuthor), seq))
                    throw new InvalidDataException("Invalid or duplicate persisted received-stream cursor.");
            }

            if (version >= 9)
            {
                int progressCount = ReadCount(r, 17, "peer author progress");
                for (int j = 0; j < progressCount; j++)
                {
                    var author = NodeId.From(r.B128());
                    byte flags = r.U8();
                    if (author.IsZero || flags == 0 || (flags & ~7) != 0)
                        throw new InvalidDataException("Invalid persisted peer author-progress flags.");
                    var progress = new PeerAuthorProgress { Flags = flags };
                    if ((flags & PeerAuthorProgress.AckedFlag) != 0) progress.Acked = version >= 15 ? ReadInt64Varint(r, "author ACK watermark") : (long)r.U64();
                    if ((flags & PeerAuthorProgress.PushedFlag) != 0) progress.Pushed = version >= 15 ? ReadInt64Varint(r, "author pushed watermark") : (long)r.U64();
                    if ((flags & PeerAuthorProgress.OffsetFlag) != 0) progress.Offset = version >= 15 ? ReadInt64Varint(r, "author stream offset") : (long)r.U64();
                    if ((progress.HasAcked && progress.Acked < 0) ||
                        (progress.HasPushed && progress.Pushed < 0) ||
                        (progress.HasOffset && progress.Offset < 0) ||
                        !ps.AuthorProgress.TryAdd(author, progress))
                        throw new InvalidDataException("Invalid or duplicate persisted peer author progress.");
                }
                if (version >= 15 && ps.StreamAckedCount > 0)
                    ps.LastAckedSeq = ps.MinStreamAcked;
            }
            else
            {
                int saCount = ReadCount(r, 24, "legacy stream ACK");
                for (int j = 0; j < saCount; j++)
                {
                    var author = NodeId.From(r.B128()); var seq = (long)r.U64();
                    if (author.IsZero) throw new InvalidDataException("Zero author in persisted ACK watermark.");
                    ps.SetStreamAcked(author, seq);
                }
                int spCount = ReadCount(r, 24, "legacy stream pushed watermark");
                for (int j = 0; j < spCount; j++)
                {
                    var author = NodeId.From(r.B128()); var seq = (long)r.U64();
                    if (author.IsZero) throw new InvalidDataException("Zero author in persisted pushed watermark.");
                    ps.SetStreamPushed(author, seq);
                }
                if (version >= 4)
                {
                    int offsetCount = ReadCount(r, 24, "legacy stream offset");
                    for (int j = 0; j < offsetCount; j++)
                    {
                        var author = NodeId.From(r.B128()); var offset = (long)r.U64();
                        if (author.IsZero) throw new InvalidDataException("Zero author in persisted stream offset.");
                        ps.SetStreamOffset(author, offset);
                    }
                }
            }
            int idsCount = ReadCount(r, version >= 15 ? 50 : 64, "peer batch replay mapping");
            for (int j = 0; j < idsCount; j++)
            {
                var author = r.B128();
                var start = version >= 15 ? ReadInt64Varint(r, "replay range start") : (long)r.U64();
                var end = version >= 15 ? ReadInt64Varint(r, "replay range end") : (long)r.U64();
                var id = r.B128(); var prev = r.B128();
                ps.Ids[((NodeId)author, start, end)] = (BatchId.From(id), Digest16.From(prev));
            }
            n.PeerStates[((NodeId)peer, ns)] = ps;
        }

        int seenCount = ReadCount(r, version >= 11 ? 17 : 33, "local WAL replay marker");
        List<(NodeId Peer, int Ns, BatchId ChangeId)>? persistedLegacySeen = null;
        if (version >= 11)
        {
            for (int i = 0; i < seenCount; i++)
            {
                int ns = ReadNamespaceId(r);
                var changeId = BatchId.From(r.B128());
                if ((uint)ns > ushort.MaxValue || changeId.IsZero ||
                    !n.SeenChanges.Add((ns, changeId)))
                    throw new InvalidDataException("Invalid or duplicate persisted local WAL replay marker.");
            }
        }
        else
        {
            persistedLegacySeen = new List<(NodeId Peer, int Ns, BatchId ChangeId)>(seenCount);
            for (int i = 0; i < seenCount; i++)
            {
                var peer = NodeId.From(r.B128());
                var ns = ReadNamespaceId(r);
                var changeId = BatchId.From(r.B128());
                persistedLegacySeen.Add((peer, ns, changeId));
            }
            // Legacy route-scoped rows are folded into global unknown markers below;
            // only rows belonging to this node remain local WAL replay markers.
            foreach (var (peer, ns, changeId) in persistedLegacySeen)
                if (peer == nodeId && (uint)ns <= ushort.MaxValue && !changeId.IsZero)
                    n.SeenChanges.Add((ns, changeId));
        }

        if (version >= 10)
        {
            int groupCount = ReadCount(r, 2, "ChangeId namespace group");
            var ledgerNamespaces = new HashSet<int>();
            for (int gi = 0; gi < groupCount; gi++)
            {
                int ns = ReadNamespaceId(r);
                int entryCount = ReadCount(r, 17, "ChangeId ledger entry");
                if ((uint)ns > ushort.MaxValue || entryCount == 0 || !ledgerNamespaces.Add(ns))
                    throw new InvalidDataException("Invalid or duplicate persisted ChangeId namespace group.");
                for (int i = 0; i < entryCount; i++)
                {
                    var changeId = BatchId.From(r.B128());
                    byte known = r.U8();
                    if (known > 1 || changeId.IsZero)
                        throw new InvalidDataException("Invalid persisted global ChangeId ledger entry.");
                    var value = ReadChangeLedgerValue(r, known);
                    if (!n.GlobalChangeLedger.TryAdd((ns, changeId), value))
                        throw new InvalidDataException("Duplicate persisted global ChangeId ledger key.");
                }
            }
        }
        else if (version >= 6)
        {
            int ledgerCount = ReadCount(r, 18, "global ChangeId ledger entry");
            for (int i = 0; i < ledgerCount; i++)
            {
                int ns = ReadNamespaceId(r);
                var changeId = BatchId.From(r.B128());
                byte known = r.U8();
                if (known > 1 || changeId.IsZero || (uint)ns > ushort.MaxValue)
                    throw new InvalidDataException("Invalid persisted global ChangeId ledger entry.");
                var value = ReadChangeLedgerValue(r, known);
                if (!n.GlobalChangeLedger.TryAdd((ns, changeId), value))
                    throw new InvalidDataException("Duplicate persisted global ChangeId ledger key.");
            }
        }
        else
        {
            // v3-v5 only had route-scoped SeenChanges and no semantic fingerprint.
            // Preserve every historical identity conservatively as an explicit
            // unknown marker before dropping redundant remote peer provenance.
            foreach (var (_, ns, changeId) in persistedLegacySeen!)
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

        int lsaCount = ReadCount(r, 33, "last stream author");
        for (int i = 0; i < lsaCount; i++)
        {
            var peer = r.B128(); var ns = ReadNamespaceId(r); var author = r.B128();
            n.LastStreamAuthor[((NodeId)peer, ns)] = NodeId.From(author);
        }

        int keCount = ReadCount(r, 17, "known epoch");
        for (int i = 0; i < keCount; i++)
        {
            var key = r.B128();
            var ns = (int)((long)r.Varint() + int.MinValue);   // zig: stored as ns - int.MinValue
            var ep = ReadInt32Varint(r);
            n.KnownEpochs[((NodeId)key, ns)] = ep;
        }

        if (version >= 5)
        {
            int peerEpochCount = ReadCount(r, 17, "peer session epoch");
            for (int i = 0; i < peerEpochCount; i++)
            {
                var peer = NodeId.From(r.B128());
                var peerEpoch = ReadInt32Varint(r);
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

        int qCount = ReadCount(r, 16, "quarantined peer");
        for (int i = 0; i < qCount; i++) n.Quarantined.Add((NodeId)r.B128());
        int fCount = ReadCount(r, 1, "forbidden namespace");
        for (int i = 0; i < fCount; i++) n.ForbiddenNamespaces.Add(ReadInt32Varint(r));
        int svCount = ReadCount(r, 2, "schema version");
        for (int i = 0; i < svCount; i++)
        {
            var svNs = ReadNamespaceId(r); var sv = ReadInt32Varint(r);
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
        int peerCount = ReadCount(r, 16, "peer registry entry");
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
            foreach (var ((streamPeer, streamEpoch, streamAuthor), seq) in ps.ReceivedStreams)
                if (streamPeer.IsZero || streamEpoch < 0 || streamAuthor.IsZero || seq < 0)
                    throw new InvalidDataException("Invalid persisted received-stream cursor.");

            foreach (var (author, progress) in ps.AuthorProgress)
            {
                if (author.IsZero || progress.Flags == 0 || (progress.Flags & ~7) != 0 ||
                    (progress.HasAcked && (!progress.HasPushed || progress.Acked < 0 ||
                                           progress.Pushed < progress.Acked)) ||
                    (progress.HasPushed && progress.Pushed < 0) ||
                    (progress.HasOffset && progress.Offset < 0) ||
                    (progress.HasOffset && progress.HasAcked && progress.Acked > long.MaxValue - progress.Offset) ||
                    (progress.HasOffset && progress.HasPushed && progress.Pushed > long.MaxValue - progress.Offset))
                    throw new InvalidDataException("Invalid persisted peer author progress.");
            }
        }

        return n;
    }

    private static ChangeLedgerValue ReadChangeLedgerValue(Reader reader, byte known)
    {
        if (known == 0) return new ChangeLedgerValue(default, isKnown: false);
        var fingerprint = Digest16.From(reader.B128());
        if (fingerprint.IsZero)
            throw new InvalidDataException("Known ChangeId fingerprint cannot be zero.");
        return new ChangeLedgerValue(fingerprint, isKnown: true);
    }

    private static InboxReceipt ReadReceiptValue(Reader reader, NodeId author, BatchId batchId, ushort version)
    {
        byte flags = reader.U8();
        byte allowedFlags = version >= 15 ? (byte)7 : (byte)3;
        if (author.IsZero || batchId.IsZero || flags > allowedFlags ||
            ((flags & 2) != 0 && (flags & 1) == 0))
            throw new InvalidDataException("Invalid persisted receipt identity or flags.");
        bool hasCursorEnd = (flags & 1) != 0;
        long cursorEnd = hasCursorEnd ? (long)reader.Varint() : 0;
        bool hasFingerprint = (flags & 2) != 0;
        var fingerprint = hasFingerprint ? Digest16.From(reader.B128()) : default;
        var appliedAt = reader.Varint();
        bool hasExpiry = version >= 15 && (flags & 4) != 0;
        var expiresAt = version >= 15 ? (hasExpiry ? reader.Varint() : 0UL) : reader.Varint();
        if (cursorEnd < 0 || (expiresAt != 0 && expiresAt < appliedAt))
            throw new InvalidDataException("Invalid persisted receipt cursor or expiry.");
        return new InboxReceipt(author, cursorEnd, hasCursorEnd,
            fingerprint, hasFingerprint, appliedAt, expiresAt);
    }

    private static void WriteReceiptValue(Writer writer, InboxReceipt receipt)
    {
        bool hasExpiry = receipt.ExpiresAt != 0;
        byte flags = (byte)((receipt.HasCursorEnd ? 1 : 0) |
                            (receipt.HasFingerprint ? 2 : 0) |
                            (hasExpiry ? 4 : 0));
        writer.U8(flags);
        if (receipt.HasCursorEnd) writer.Varint((ulong)receipt.CursorEnd);
        if (receipt.HasFingerprint) writer.B128(receipt.Fingerprint);
        writer.Varint(receipt.AppliedAt);
        if (hasExpiry) writer.Varint(receipt.ExpiresAt);
    }

    private static int ReadInt32Varint(Reader reader)
    {
        ulong raw = reader.Varint();
        if (raw > int.MaxValue)
            throw new InvalidDataException("Persisted integer exceeds Int32 range.");
        return (int)raw;
    }

    private static int ReadNamespaceId(Reader reader)
    {
        ulong raw = reader.Varint();
        if (raw > ushort.MaxValue)
            throw new InvalidDataException("Persisted namespace id exceeds UInt16 range.");
        return (int)raw;
    }

    private static int ReadCount(Reader reader, int minimumEntryBytes, string section)
    {
        ulong raw = reader.Varint();
        if (minimumEntryBytes <= 0 || raw > int.MaxValue ||
            raw > (ulong)(reader.Remaining / minimumEntryBytes))
            throw new InvalidDataException($"Invalid persisted {section} count.");
        return (int)raw;
    }

    private static int ReadByteLength(Reader reader, string section)
    {
        ulong raw = reader.Varint();
        if (raw > int.MaxValue || raw > (ulong)reader.Remaining)
            throw new InvalidDataException($"Invalid persisted {section} length.");
        return (int)raw;
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
        int fCount = ReadCount(r, 33, "record field");
        for (int i = 0; i < fCount; i++)
        {
            var (fid, fs) = ReadFieldState(r);
            rec.State[fid] = fs;
        }
        int tCount = ReadCount(r, 33, "record field tombstone");
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

    private static void WriteOutboxEntries(Writer w, int namespaceId, List<OutboxEntry> entries)
    {
        w.Varint((ulong)entries.Count);
        // Keep original Outbox order while storing each author once per namespace.
        var authors = new List<NodeId>();
        var authorIndexes = new Dictionary<NodeId, int>();
        foreach (var entry in entries)
        {
            if (entry.NamespaceId != namespaceId || entry.AuthorNodeId.IsZero ||
                entry.Seq < 0 || entry.CreatedAt < 0)
                throw new InvalidOperationException("Invalid Outbox entry metadata for compact persistence.");
            if (!authorIndexes.ContainsKey(entry.AuthorNodeId))
            {
                authorIndexes.Add(entry.AuthorNodeId, authors.Count);
                authors.Add(entry.AuthorNodeId);
            }
        }
        w.Varint((ulong)authors.Count);
        foreach (var author in authors) w.B128(author);
        foreach (var entry in entries)
        {
            var payload = entry.Payload.Span;
            if (payload.IsEmpty)
                throw new InvalidOperationException("Outbox operation payload cannot be empty.");
            byte opKind = payload[0];
            if (opKind != entry.OpKind ||
                (opKind != OpsConst.TagFieldWrite && opKind != OpsConst.TagFieldTomb &&
                 opKind != OpsConst.TagRecordTomb))
                throw new InvalidOperationException("Outbox operation kind does not match its canonical payload.");
            bool recordTombstone = opKind == OpsConst.TagRecordTomb;
            if (recordTombstone &&
                (payload.Length < 31 || !payload.Slice(1, 16).SequenceEqual(entry.RecordId.ToArray())))
                throw new InvalidOperationException("Record tombstone identity does not match its payload.");
            w.Varint((ulong)authorIndexes[entry.AuthorNodeId]);
            w.Varint((ulong)entry.Seq);
            w.B128(entry.ChangeId);
            if (entry.OriginAuthorEpoch == 0 || entry.OriginSequence == 0)
                throw new InvalidOperationException("Outbox origin provenance is required.");
            w.U32(entry.OriginAuthorEpoch);
            w.U64(entry.OriginSequence);
            w.Varint((ulong)payload.Length);
            w.Raw(payload);
            if (!recordTombstone) w.B128(entry.RecordId);
            WriteNonNegativeVarint(w, entry.CreatedAt, "Outbox CreatedAt");
            w.Varint((ulong)entry.CausalDeps.Count);
            foreach (var dependency in entry.CausalDeps) w.B128(dependency);
        }
    }

    private static List<OutboxEntry> ReadOutboxEntries(Reader r, int namespaceId, int count, ushort version)
    {
        int authorCount = ReadCount(r, 16, "outbox author");
        if ((count == 0) != (authorCount == 0) || authorCount > count)
            throw new InvalidDataException("Invalid compact Outbox author table.");
        var authors = new NodeId[authorCount];
        var uniqueAuthors = new HashSet<NodeId>();
        for (int i = 0; i < authorCount; i++)
        {
            authors[i] = NodeId.From(r.B128());
            if (authors[i].IsZero || !uniqueAuthors.Add(authors[i]))
                throw new InvalidDataException("Invalid or duplicate compact Outbox author.");
        }
        var entries = new List<OutboxEntry>(count);
        for (int i = 0; i < count; i++)
        {
            int authorIndex = ReadInt32Varint(r);
            if ((uint)authorIndex >= (uint)authors.Length)
                throw new InvalidDataException("Compact Outbox author index is out of range.");
            long sequence = ReadInt64Varint(r, "Outbox sequence");
            var changeId = BatchId.From(r.B128());
            uint originEpoch = r.U32();
            ulong originSequence = r.U64();
            if (originEpoch == 0 || originSequence == 0)
                throw new InvalidDataException("Persisted Outbox origin provenance is invalid.");
            byte[] payload = r.Raw(ReadByteLength(r, "Outbox payload"));
            if (payload.Length == 0)
                throw new InvalidDataException("Empty compact Outbox operation payload.");
            byte opKind = payload[0];
            if (opKind != OpsConst.TagFieldWrite && opKind != OpsConst.TagFieldTomb &&
                opKind != OpsConst.TagRecordTomb)
                throw new InvalidDataException("Unknown compact Outbox operation kind.");
            RecordId recordId;
            if (opKind == OpsConst.TagRecordTomb)
            {
                if (payload.Length < 31)
                    throw new InvalidDataException("Truncated record-tombstone Outbox payload.");
                recordId = RecordId.From(payload.AsSpan(1, 16));
            }
            else
                recordId = RecordId.From(r.B128());
            long createdAt = version >= 15 ? ReadInt64Varint(r, "Outbox CreatedAt") : (long)r.U64();
            int dependencyCount = ReadCount(r, 16, "causal dependency");
            var entry = new OutboxEntry
            {
                NamespaceId = namespaceId, Seq = sequence, ChangeId = changeId,
                RecordId = recordId, OpKind = opKind, Payload = payload,
                OriginAuthorEpoch = originEpoch, OriginSequence = originSequence,
                AuthorNodeId = authors[authorIndex], CreatedAt = createdAt,
            };
            for (int di = 0; di < dependencyCount; di++)
                entry.CausalDeps.Add(BatchId.From(r.B128()));
            entries.Add(entry);
        }
        return entries;
    }

    private static List<OutboxEntry> ReadLegacyOutboxEntries(Reader r, int count)
    {
        var entries = new List<OutboxEntry>(count);
        for (int i = 0; i < count; i++) entries.Add(ReadOutboxEntry(r));
        return entries;
    }

    private static void WriteNonNegativeVarint(Writer writer, long value, string section)
    {
        if (value < 0)
            throw new InvalidOperationException($"Cannot persist negative {section}.");
        writer.Varint((ulong)value);
    }

    private static long ReadInt64Varint(Reader reader, string section)
    {
        ulong raw = reader.Varint();
        if (raw > long.MaxValue)
            throw new InvalidDataException($"Persisted {section} exceeds Int64 range.");
        return (long)raw;
    }

    private static void WriteOutboxEntry(Writer w, OutboxEntry e)
    {
        w.Varint((ulong)e.NamespaceId);
        w.U64((ulong)e.Seq);
        w.B128(e.ChangeId);
        w.U32(e.OriginAuthorEpoch);
        w.U64(e.OriginSequence);
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
            NamespaceId = ReadNamespaceId(r),
            Seq = (long)r.U64(),
            ChangeId = BatchId.From(r.B128()),
            OriginAuthorEpoch = r.U32(),
            OriginSequence = r.U64(),
            RecordId = RecordId.From(r.B128()),
            OpKind = r.U8(),
            Payload = r.Raw(ReadByteLength(r, "outbox payload")),
            AuthorNodeId = NodeId.From(r.B128()),
            CreatedAt = (long)r.U64(),
        };
        int depCount = ReadCount(r, 16, "causal dependency");
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

