// Append-only write-ahead log for incremental durability between full snapshots.
// Format: magic + version, then length-prefixed records (type + payload).
// Crash recovery: Load snapshot (optional) then Wal.Replay onto the node.
// Note: B128 is fixed 16-byte identity; field values use varint-length Bytes().
namespace BinSync;

/// <summary>WAL record type tags.</summary>
public enum WalRecordType : byte
{
    Checkpoint = 0x02,
    LocalMutation = 0x03,
}

/// <summary>
/// Simple durable WAL between periodic <see cref="NodePersistence.Save"/> checkpoints.
/// </summary>
public sealed class Wal : IDisposable
{
    /// <summary>WAL format identifier string written at the start of a log segment.</summary>
    public const string Tag = "BIN-SYNC/WAL/v2";
    private const uint Magic = 0x4253574C; // 'B''S''W''L'
    private const ushort Version = 2;
    private const int RecordDigestLen = 16;
    private const string RecordDigestTag = "BIN-SYNC/WAL-RECORD/v1";

    private readonly string _path;
    private FileStream? _fs;
    private readonly object _lock = new();
    private long _sequence;

    /// <summary>Filesystem path of the write-ahead log.</summary>
    public string Path => _path;
    /// <summary>Monotonic sequence of the last appended WAL record.</summary>
    public long Sequence => _sequence;

    private Wal(string path, FileStream fs, long sequence)
    {
        _path = path;
        _fs = fs;
        _sequence = sequence;
    }

    /// <summary>Open or create a write-ahead log at the given path.</summary>
    public static Wal Open(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path required.", nameof(path));
        var dir = System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(path))!;
        Directory.CreateDirectory(dir);

        var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read,
            4096, FileOptions.WriteThrough);
        try
        {
        long seq = 0;
        if (fs.Length == 0)
        {
            WriteHeader(fs);
        }
        else
        {
            fs.Position = 0;
            using (var br = new BinaryReader(fs, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                if (ReadBEU32(br) != Magic)
                    throw new InvalidDataException("Not a BinSync WAL (bad magic).");
                var version = ReadBEU16(br);
                if (version != Version)
                    throw new InvalidDataException($"Unsupported WAL version {version}; WAL v2 is required.");
            }
            long validEnd = 6;
            while (validEnd + 4 <= fs.Length)
            {
                fs.Position = validEnd;
                int len = ReadBEInt32(fs);
                if (len < 1)
                    throw new InvalidDataException("Invalid WAL record length.");
                const int trailerLength = RecordDigestLen;
                if (fs.Position + len + trailerLength > fs.Length)
                    break;
                var payload = new byte[len];
                int read = fs.Read(payload, 0, len);
                if (read != len) break;
                {
                    var storedDigest = new byte[RecordDigestLen];
                    if (fs.Read(storedDigest, 0, storedDigest.Length) != storedDigest.Length) break;
                    VerifyRecordDigest(payload, storedDigest);
                }
                ValidateRecord(payload);
                validEnd = fs.Position;
                seq++;
            }

            // Also discard a torn length prefix (< 4 bytes) at EOF. Appending after
            // an incomplete tail would make every later record unreachable to replay.
            if (validEnd != fs.Length)
            {
                fs.SetLength(validEnd);
                fs.Flush(flushToDisk: true);
            }
            fs.Position = fs.Length;
        }
        return new Wal(path, fs, seq);
        }
        catch
        {
            fs.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Append one complete local mutation, including its exact durable Outbox identity.
    /// The caller publishes the corresponding in-memory state only after this returns.
    /// </summary>
    internal long AppendLocalMutation(OutboxEntry entry, bool applyToStorage)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var w = new Writer();
        w.U8((byte)WalRecordType.LocalMutation);
        w.U8(applyToStorage ? (byte)1 : (byte)0);
        w.Varint((ulong)entry.NamespaceId);
        w.U64((ulong)entry.Seq);
        w.B128(entry.ChangeId);
        w.B128(entry.RecordId);
        w.U8(entry.OpKind);
        w.Varint((ulong)entry.Payload.Length);
        w.Raw(entry.Payload.Span);
        w.B128(entry.AuthorNodeId);
        w.U64((ulong)entry.CreatedAt);
        w.Varint((ulong)entry.CausalDeps.Count);
        foreach (var dep in entry.CausalDeps) w.B128(dep);
        var payload = w.Value();
        ValidateRecord(payload);
        return AppendRaw(payload);
    }

    /// <summary>Append a checkpoint marker and return its sequence.</summary>
    public long AppendCheckpoint()
    {
        var w = new Writer();
        w.U8((byte)WalRecordType.Checkpoint);
        w.U64((ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return AppendRaw(w.Value());
    }

    private long AppendRaw(byte[] payload)
    {
        lock (_lock)
        {
            if (_fs is null) throw new ObjectDisposedException(nameof(Wal));
            Span<byte> lenBuf = stackalloc byte[4];
            WriteBEInt32(lenBuf, payload.Length);
            _fs.Write(lenBuf);
            _fs.Write(payload);
            _fs.Write(ComputeRecordDigest(payload));
            _fs.Flush(flushToDisk: true);
            _sequence++;
            return _sequence;
        }
    }

    /// <summary>Replay WAL records into a node; returns the number of entries applied.</summary>
    public int Replay(Node node)
    {
        ArgumentNullException.ThrowIfNull(node);
        // Match writer/checkpoint lock order and prevent WAL → Node inversion.
        using (node.EnterAllGates())
        lock (_lock)
        {
            if (_fs is null) throw new ObjectDisposedException(nameof(Wal));
            long end = _fs.Length;
            long pos = 6;
            var records = new List<byte[]>();
            while (pos + 4 <= end)
            {
                _fs.Position = pos;
                int len = ReadBEInt32(_fs);
                if (len < 1)
                    throw new InvalidDataException("Invalid WAL record length.");
                const int trailerLength = RecordDigestLen;
                if (pos + 4L + len + trailerLength > end)
                    break;
                var payload = new byte[len];
                int read = _fs.Read(payload, 0, len);
                if (read != len) break;
                {
                    var storedDigest = new byte[RecordDigestLen];
                    if (_fs.Read(storedDigest, 0, storedDigest.Length) != storedDigest.Length) break;
                    VerifyRecordDigest(payload, storedDigest);
                }
                ValidateRecord(payload);
                pos = _fs.Position;
                records.Add(payload);
            }

            int applied = 0;
            foreach (var payload in records)
                applied += ApplyRecord(node, payload);
            _fs.Position = end;
            return applied;
        }
    }

    /// <summary>Open a WAL file and replay it into a node.</summary>
    public static int ReplayFile(string path, Node node)
    {
        ArgumentNullException.ThrowIfNull(node);
        // Acquire Node gates before observing the file so the replay sees a stable
        // WAL boundary and cannot race an attached local mutation or checkpoint.
        using (node.EnterAllGates())
        {
        if (!File.Exists(path)) return 0;
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (fs.Length < 6) return 0;
        using var br = new BinaryReader(fs, System.Text.Encoding.UTF8, leaveOpen: true);
        if (ReadBEU32(br) != Magic) throw new InvalidDataException("Not a BinSync WAL.");
        var version = ReadBEU16(br);
        if (version != Version)
            throw new InvalidDataException($"Unsupported WAL version {version}; WAL v2 is required.");
        var records = new List<byte[]>();
        while (fs.Position + 4 <= fs.Length)
        {
            int len = ReadBEInt32(fs);
            if (len < 1)
                throw new InvalidDataException("Invalid WAL record length.");
            const int trailerLength = RecordDigestLen;
            if (fs.Position + len + trailerLength > fs.Length)
                break;
            var payload = br.ReadBytes(len);
            if (payload.Length != len) break;
            {
                var storedDigest = br.ReadBytes(RecordDigestLen);
                if (storedDigest.Length != RecordDigestLen) break;
                VerifyRecordDigest(payload, storedDigest);
            }
            ValidateRecord(payload);
            records.Add(payload);
        }

        // Do not mutate the target node until every complete record has passed
        // structural validation. This prevents partial replay on malformed tails.
        int applied = 0;
        foreach (var payload in records)
            applied += ApplyRecord(node, payload);
        return applied;
        }
    }

    /// <summary>Truncate the WAL after a successful checkpoint.</summary>
    public void Truncate()
    {
        lock (_lock)
        {
            if (_fs is null) throw new ObjectDisposedException(nameof(Wal));
            _fs.SetLength(0);
            WriteHeader(_fs);
            _fs.Flush(flushToDisk: true);
            _sequence = 0;
        }
    }

    /// <summary>Checkpoint node state to a snapshot file and truncate the WAL.</summary>
    public void Checkpoint(Node node, string snapshotPath)
    {
        ArgumentNullException.ThrowIfNull(node);
        // Match the global mutation lock order: Node gates → WAL lock.
        // Local writes hold Storage/Outbox while appending to WAL; taking the WAL
        // lock first here would deadlock against a writer waiting for that lock.
        using (node.EnterAllGates())
        {
            lock (_lock)
            {
                if (_fs is null) throw new ObjectDisposedException(nameof(Wal));
                // The node state cannot change, and no WAL append can interleave,
                // between the snapshot and truncation.
                NodePersistence.Save(node, snapshotPath);
                AppendCheckpoint();
                Truncate();
            }
        }
    }

    private static byte[] ComputeRecordDigest(byte[] payload)
        => Blake3Core.TaggedHash(RecordDigestTag, payload).AsSpan(0, RecordDigestLen).ToArray();

    private static void VerifyRecordDigest(byte[] payload, byte[] storedDigest)
    {
        var actual = ComputeRecordDigest(payload);
        if (!actual.AsSpan().SequenceEqual(storedDigest))
            throw new InvalidDataException("WAL record digest mismatch.");
    }

    // Validate framing and the complete payload before any replay mutation.
    // A complete but malformed record is corruption, not a torn tail.
    private static void ValidateRecord(byte[] payload)
    {
        var r = new Reader(payload);
        var type = (WalRecordType)r.U8();
        switch (type)
        {
            case WalRecordType.Checkpoint:
                _ = r.U64();
                break;
            case WalRecordType.LocalMutation:
                {
                    var (_, entry) = ReadLocalMutation(r);
                    if (entry.Seq <= 0 || entry.ChangeId.IsZero || entry.AuthorNodeId.IsZero || entry.CreatedAt < 0)
                        throw new InvalidDataException("Invalid WAL local-mutation metadata.");
                    var decoded = Ops.DecodeOp(entry.Payload).Op;
                    byte expectedKind = decoded switch
                    {
                        OpFieldWrite => OpsConst.TagFieldWrite,
                        OpFieldTomb => OpsConst.TagFieldTomb,
                        OpRecordTombstone => OpsConst.TagRecordTomb,
                        _ => throw new InvalidDataException("Unsupported WAL local-mutation operation.")
                    };
                    if (entry.OpKind != expectedKind)
                        throw new InvalidDataException("WAL local-mutation operation tag mismatch.");
                    if (decoded is OpRecordTombstone tombstone &&
                        !RecordId.From(tombstone.RecordId).Equals(entry.RecordId))
                        throw new InvalidDataException("WAL record-tombstone identity does not match its Outbox entry.");
                    break;
                }
            default:
                throw new InvalidDataException($"Unknown WAL record type 0x{(byte)type:X2}.");
        }

        if (!r.Eof())
            throw new InvalidDataException("WAL record contains trailing bytes.");
    }

    private static int ApplyRecord(Node node, byte[] payload)
    {
        var r = new Reader(payload);
        var type = (WalRecordType)r.U8();
        switch (type)
        {
            case WalRecordType.Checkpoint:
                return 0;
            case WalRecordType.LocalMutation:
                {
                    var (applyToStorage, entry) = ReadLocalMutation(r);
                    return node.ReplayLocalMutation(entry, applyToStorage) ? 1 : 0;
                }
            default:
                throw new InvalidDataException($"Unknown WAL record type 0x{(byte)type:X2}.");
        }
    }

    private static (bool ApplyToStorage, OutboxEntry Entry) ReadLocalMutation(Reader r)
    {
        byte applyFlag = r.U8();
        if (applyFlag > 1) throw new InvalidDataException("Invalid WAL local-mutation flags.");
        ulong ns = r.Varint();
        ulong seq = r.U64();
        var changeId = BatchId.From(r.B128());
        var recordId = RecordId.From(r.B128());
        byte opKind = r.U8();
        ulong payloadLength = r.Varint();
        if (ns > int.MaxValue || seq > long.MaxValue || payloadLength > int.MaxValue)
            throw new InvalidDataException("WAL local-mutation field is out of range.");
        var payload = r.Raw((int)payloadLength);
        var author = NodeId.From(r.B128());
        ulong createdAt = r.U64();
        if (createdAt > long.MaxValue)
            throw new InvalidDataException("WAL local-mutation creation time is out of range.");
        ulong depCount = r.Varint();
        if (depCount > 1_000_000)
            throw new InvalidDataException("WAL local-mutation dependency count is out of range.");
        var entry = new OutboxEntry
        {
            NamespaceId = (int)ns,
            Seq = (long)seq,
            ChangeId = changeId,
            RecordId = recordId,
            OpKind = opKind,
            Payload = payload,
            AuthorNodeId = author,
            CreatedAt = (long)createdAt,
        };
        for (ulong i = 0; i < depCount; i++) entry.CausalDeps.Add(BatchId.From(r.B128()));
        return (applyFlag != 0, entry);
    }

    private static void WriteHeader(FileStream fs)
    {
        Span<byte> hdr = stackalloc byte[6];
        WriteBEU32(hdr, Magic);
        WriteBEU16(hdr.Slice(4), Version);
        fs.Write(hdr);
        fs.Flush(flushToDisk: true);
    }

    private static void WriteBEU32(Span<byte> s, uint v)
    {
        s[0] = (byte)(v >> 24); s[1] = (byte)(v >> 16); s[2] = (byte)(v >> 8); s[3] = (byte)v;
    }
    private static void WriteBEU16(Span<byte> s, ushort v)
    {
        s[0] = (byte)(v >> 8); s[1] = (byte)v;
    }
    private static void WriteBEInt32(Span<byte> s, int v) => WriteBEU32(s, unchecked((uint)v));

    private static uint ReadBEU32(BinaryReader r)
        => ((uint)r.ReadByte() << 24) | ((uint)r.ReadByte() << 16) | ((uint)r.ReadByte() << 8) | r.ReadByte();
    private static ushort ReadBEU16(BinaryReader r)
        => (ushort)((r.ReadByte() << 8) | r.ReadByte());
    private static int ReadBEInt32(Stream s)
    {
        Span<byte> b = stackalloc byte[4];
        int n = s.Read(b);
        if (n != 4) throw new EndOfStreamException();
        return (int)(((uint)b[0] << 24) | ((uint)b[1] << 16) | ((uint)b[2] << 8) | b[3]);
    }

    /// <summary>Release acquired resources.</summary>
    public void Dispose()
    {
        lock (_lock)
        {
            _fs?.Dispose();
            _fs = null;
        }
    }
}
