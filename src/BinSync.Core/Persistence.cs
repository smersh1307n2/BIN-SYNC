using System.Buffers.Binary;

namespace BinSync.Core;

/// <summary>
/// WAL-style save/load of a node's full state (§ production hardening, Шаг 1).
///
/// Persisted data: records, outbox (per-author seq + pending batches), inbox cursors,
/// batchDigest checkpoints, ALIAS_MAP, peer cursors, replay batchId cache/digests and the
/// HLC clock.
///
/// Integrity: the whole body is covered by a BLAKE3-256 digest tagged with "BIN-SYNC/NODESTATE/v1".
/// Any corruption (bit flip, truncated file, wrong version) makes <see cref="Load"/> throw
/// <see cref="InvalidDataException"/>.
///
/// Atomicity: the image is written to a temporary file in the same directory, flushed to disk
/// (fsync), and then moved over the target with File.Move(temp, target, overwrite: true) — so a
/// crash can never leave a half-written state file.
///
/// Wire format: binary, big-endian.
/// </summary>
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
        var digest = Blake3.TaggedHash(StateTag, body);

        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            w.Write(Magic);          // 4 bytes, BE
            w.Write(Version);        // 2 bytes, BE
            WriteBE(w, (ulong)body.Length);
            w.Write(digest);         // 32 bytes
            w.Write(body);
            w.Flush();
        }
        var image = ms.ToArray();

        var dir = Path.GetDirectoryName(Path.GetFullPath(path))!;
        Directory.CreateDirectory(dir);
        var tmp = Path.Combine(dir, $".{Path.GetFileName(path)}.tmp-{Guid.NewGuid():N}");
        try
        {
            // Flush the temp file fully to disk before renaming over the target.
            using (var fs = new FileStream(tmp, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                                           4096, FileOptions.WriteThrough))
            {
                fs.Write(image);
                fs.Flush(flushToDisk: true);
            }
            File.Move(tmp, path, overwrite: true); // atomic on POSIX; documented rename semantics on Windows
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
        var bytes = File.ReadAllBytes(path);
        return LoadFromBytes(bytes);
    }

    /// <summary>In-memory variant of <see cref="Load"/> (same integrity rules).</summary>
    public static Node LoadFromBytes(ReadOnlySpan<byte> bytes)
    {
        using var ms = new MemoryStream(bytes.ToArray(), writable: false);
        using var r = new BinaryReader(ms, System.Text.Encoding.UTF8);

        if (bytes.Length < 4 + 2 + 8 + DigestLen)
            throw new InvalidDataException("State file truncated.");

        if (r.ReadUInt32() != Magic)
            throw new InvalidDataException("Not a BinSync node-state file (bad magic).");
        var ver = r.ReadUInt16();
        if (ver != Version)
            throw new InvalidDataException($"Unsupported state version {ver} (expected {Version}).");

        var bodyLen = ReadBE(r);
        if (bodyLen > (ulong)(bytes.Length - 14 - DigestLen))
            throw new InvalidDataException("State file truncated (body length out of bounds).");
        var storedDigest = r.ReadBytes(DigestLen);
        var body = r.ReadBytes((int)bodyLen);
        if (body.Length != (int)bodyLen)
            throw new InvalidDataException("State file truncated mid-body.");

        var actual = Blake3.TaggedHash(StateTag, body);
        if (!actual.AsSpan().SequenceEqual(storedDigest))
            throw new InvalidDataException("State digest mismatch — file corrupted (tagged BLAKE3 " + StateTag + ").");

        return DeserializeBody(body);
    }

    // ------------------------------------------------------------- serialization

    private static byte[] SerializeBody(Node n)
    {
        lock (n.Gate) // hold the node gate for a consistent point-in-time image
        {
            using var ms = new MemoryStream();
            using var w = new BinaryWriter(ms, System.Text.Encoding.UTF8);

            WriteStr(w, n.Id);

            // --- HLC clock
            WriteBE(w, (ulong)n.Clock.PhysicalUs);
            WriteBE(w, n.Clock.Logical);

            // --- known authors
            WriteBE(w, (ulong)n.KnownAuthorsRef.Count);
            foreach (var a in n.KnownAuthorsRef) WriteStr(w, a);

            // --- records
            WriteBE(w, (ulong)n.RecordsRef.Count);
            foreach (var (id, rec) in n.RecordsRef.OrderBy(k => k.Key, StringComparer.Ordinal))
            {
                WriteStr(w, id);
                w.Write((byte)rec.Status);
                WriteBE(w, (ulong)rec.Tombstones.Count);
                foreach (var (a, t) in rec.Tombstones.OrderBy(k => k.Key, StringComparer.Ordinal))
                { WriteStr(w, a); WriteBE(w, (ulong)t); }
                WriteBE(w, (ulong)rec.Fields.Count);
                foreach (var (f, cell) in rec.Fields.OrderBy(k => k.Key, StringComparer.Ordinal))
                {
                    WriteStr(w, f);
                    var v = cell.Current;
                    w.Write(v is not null);
                    if (v is not null)
                    {
                        WriteStr(w, v.Author);
                        WriteBE(w, (ulong)v.TsUs);
                        WriteBytes(w, v.Value);
                    }
                }
            }

            // --- ALIAS_MAP
            WriteBE(w, (ulong)n.AliasMapRef.Count);
            foreach (var (a, id) in n.AliasMapRef.OrderBy(k => k.Key, StringComparer.Ordinal))
            { WriteStr(w, a); WriteStr(w, id); }

            // --- outbox: per-author seq counters
            WriteBE(w, (ulong)n.OutboxSeqRef.Count);
            foreach (var (a, s) in n.OutboxSeqRef.OrderBy(k => k.Key, StringComparer.Ordinal))
            { WriteStr(w, a); WriteBE(w, (ulong)s); }

            // --- outbox: pending batches
            WriteBE(w, (ulong)n.OutboxRef.Count);
            foreach (var (author, q) in n.OutboxRef.OrderBy(k => k.Key, StringComparer.Ordinal))
            {
                WriteStr(w, author);
                WriteBE(w, (ulong)q.Count);
                foreach (var b in q) WriteBatch(w, b);
            }

            // --- inbox cursors (peer cursors)
            WriteBE(w, (ulong)n.InboxCursorsRef.Count);
            foreach (var (p, s) in n.InboxCursorsRef.OrderBy(k => k.Key, StringComparer.Ordinal))
            { WriteStr(w, p); WriteBE(w, (ulong)s); }

            // --- batchDigest checkpoints
            WriteBE(w, (ulong)n.CheckpointsRef.Count);
            foreach (var (seq, bid, dig) in n.CheckpointsRef)
            { WriteBE(w, (ulong)seq); WriteStr(w, bid); WriteStr(w, dig); }

            // --- replay cache: applied batchIds + cached digests
            WriteBE(w, (ulong)n.AppliedBatchIdsRef.Count);
            foreach (var bid in n.AppliedBatchIdsRef.OrderBy(s => s, StringComparer.Ordinal))
                WriteStr(w, bid);
            WriteBE(w, (ulong)n.ReplayDigestCacheRef.Count);
            foreach (var (bid, dig) in n.ReplayDigestCacheRef.OrderBy(k => k.Key, StringComparer.Ordinal))
            { WriteStr(w, bid); WriteStr(w, dig); }

            w.Flush();
            return ms.ToArray();
        }
    }

    private static Node DeserializeBody(byte[] body)
    {
        using var ms = new MemoryStream(body, writable: false);
        using var r = new BinaryReader(ms, System.Text.Encoding.UTF8);

        var nodeId = ReadStr(r);
        var node = new Node(nodeId);

        node.Clock.PhysicalUs = (long)ReadBE(r);
        node.Clock.Logical = (uint)ReadBE(r);

        var na = (int)ReadBE(r);
        for (int i = 0; i < na; i++) node.KnownAuthorsRef.Add(ReadStr(r));

        var nr = (int)ReadBE(r);
        for (int i = 0; i < nr; i++)
        {
            var id = ReadStr(r);
            var status = (RecordStatus)r.ReadByte();
            var rec = new Record { Id = id, Status = status };
            var nt = (int)ReadBE(r);
            for (int j = 0; j < nt; j++)
            {
                var a = ReadStr(r);
                var t = (long)ReadBE(r);
                rec.Tombstones[a] = t;
            }
            var nf = (int)ReadBE(r);
            for (int j = 0; j < nf; j++)
            {
                var f = ReadStr(r);
                bool has = r.ReadBoolean();
                var cell = new FieldCell();
                if (has)
                {
                    var author = ReadStr(r);
                    var ts = (long)ReadBE(r);
                    var val = ReadBytes(r);
                    cell.Merge(new FieldValue { Author = author, TsUs = ts, Value = val });
                }
                rec.Fields[f] = cell;
            }
            node.RecordsRef[id] = rec;
        }

        var nal = (int)ReadBE(r);
        for (int i = 0; i < nal; i++)
        {
            var a = ReadStr(r);
            var id = ReadStr(r);
            node.AliasMapRef[a] = id;
        }

        var nseq = (int)ReadBE(r);
        for (int i = 0; i < nseq; i++)
        {
            var a = ReadStr(r);
            node.OutboxSeqRef[a] = (long)ReadBE(r);
        }

        var nob = (int)ReadBE(r);
        for (int i = 0; i < nob; i++)
        {
            var author = ReadStr(r);
            var cnt = (int)ReadBE(r);
            var q = new Queue<DeltaBatch>();
            for (int j = 0; j < cnt; j++) q.Enqueue(ReadBatch(r));
            node.OutboxRef[author] = q;
        }

        var nic = (int)ReadBE(r);
        for (int i = 0; i < nic; i++)
        {
            var p = ReadStr(r);
            node.InboxCursorsRef[p] = (long)ReadBE(r);
        }

        var ncp = (int)ReadBE(r);
        for (int i = 0; i < ncp; i++)
        {
            var seq = (long)ReadBE(r);
            var bid = ReadStr(r);
            var dig = ReadStr(r);
            node.CheckpointsRef.Add((seq, bid, dig));
        }

        var nab = (int)ReadBE(r);
        for (int i = 0; i < nab; i++) node.AppliedBatchIdsRef.Add(ReadStr(r));

        var nrd = (int)ReadBE(r);
        for (int i = 0; i < nrd; i++)
        {
            var bid = ReadStr(r);
            var dig = ReadStr(r);
            node.ReplayDigestCacheRef[bid] = dig;
        }

        if (ms.Position != ms.Length)
            throw new InvalidDataException("Trailing garbage in state body.");
        return node;
    }

    private static void WriteBatch(BinaryWriter w, DeltaBatch b)
    {
        WriteStr(w, b.BatchId);
        WriteStr(w, b.Peer);
        WriteBE(w, (ulong)b.Seq);
        WriteBE(w, (ulong)b.DepSeq);
        WriteBE(w, (ulong)b.HlcTs);
        WriteBE(w, b.HlcLog);
        WriteStr(w, b.HlcNode);
        WriteStr(w, b.Digest);
        WriteBE(w, (ulong)b.Ops.Count);
        foreach (var o in b.Ops)
        {
            WriteStr(w, o.RecId); WriteStr(w, o.Field);
            WriteStr(w, o.Value.Author); WriteBE(w, (ulong)o.Value.TsUs);
            WriteBytes(w, o.Value.Value);
        }
        WriteBE(w, (ulong)b.Tombs.Count);
        foreach (var t in b.Tombs) { WriteStr(w, t.RecId); WriteStr(w, t.Author); WriteBE(w, (ulong)t.TsUs); }
        WriteBE(w, (ulong)b.AliasRenames.Count);
        foreach (var (rid, al) in b.AliasRenames) { WriteStr(w, rid); WriteStr(w, al); }
    }

    private static DeltaBatch ReadBatch(BinaryReader r)
    {
        var b = new DeltaBatch
        {
            BatchId = ReadStr(r),
            Peer = ReadStr(r),
            Seq = (long)ReadBE(r),
            DepSeq = (long)ReadBE(r),
            HlcTs = (long)ReadBE(r),
            HlcLog = (uint)ReadBE(r),
            HlcNode = ReadStr(r),
            Digest = ReadStr(r),
        };
        int no = (int)ReadBE(r);
        for (int i = 0; i < no; i++)
        {
            var rid = ReadStr(r);
            var fld = ReadStr(r);
            var author = ReadStr(r);
            var ts = (long)ReadBE(r);
            var val = ReadBytes(r);
            b.Ops.Add(new FieldOp(rid, fld, new FieldValue { Author = author, TsUs = ts, Value = val }));
        }
        int nt = (int)ReadBE(r);
        for (int i = 0; i < nt; i++)
        {
            var rid = ReadStr(r);
            var author = ReadStr(r);
            var ts = (long)ReadBE(r);
            b.Tombs.Add(new TombOp(rid, author, ts));
        }
        int na = (int)ReadBE(r);
        for (int i = 0; i < na; i++)
        {
            var rid = ReadStr(r);
            var al = ReadStr(r);
            b.AliasRenames.Add((rid, al));
        }
        return b;
    }

    // ---------------------------------------------------------- BE primitives

    internal static void WriteBE(BinaryWriter w, ulong v)
    {
        Span<byte> buf = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buf, v);
        w.Write(buf);
    }

    internal static ulong ReadBE(BinaryReader r)
    {
        Span<byte> buf = stackalloc byte[8];
        var b = r.ReadBytes(8);
        if (b.Length != 8) throw new InvalidDataException("Truncated 64-bit value — corrupt state.");
        b.AsSpan().CopyTo(buf);
        return BinaryPrimitives.ReadUInt64BigEndian(buf);
    }

    internal static void WriteStr(BinaryWriter w, string s)
    {
        var b = System.Text.Encoding.UTF8.GetBytes(s);
        WriteBE(w, (ulong)b.Length);
        w.Write(b);
    }

    internal static string ReadStr(BinaryReader r)
    {
        var len = ReadBE(r);
        if (len > (ulong)r.BaseStream.Length - (ulong)r.BaseStream.Position)
            throw new InvalidDataException("String length out of bounds — corrupt state.");
        return System.Text.Encoding.UTF8.GetString(r.ReadBytes((int)len));
    }

    internal static void WriteBytes(BinaryWriter w, byte[] b)
    {
        WriteBE(w, (ulong)b.Length);
        w.Write(b);
    }

    internal static byte[] ReadBytes(BinaryReader r)
    {
        var len = ReadBE(r);
        if (len > (ulong)r.BaseStream.Length - (ulong)r.BaseStream.Position)
            throw new InvalidDataException("Byte-array length out of bounds — corrupt state.");
        var b = r.ReadBytes((int)len);
        if (b.Length != (int)len) throw new InvalidDataException("Truncated byte array.");
        return b;
    }
}
