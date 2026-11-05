// Append-only write-ahead log for incremental durability between full snapshots.
// Format: magic + version, then length-prefixed records (type + payload).
// Crash recovery: Load snapshot (optional) then Wal.Replay onto the node.
// Note: B128 is fixed 16-byte identity; field values use varint-length Bytes().
namespace BinSync;

/// <summary>WAL record type tags.</summary>
public enum WalRecordType : byte
{
    FieldWrite = 0x01,
    Checkpoint = 0x02,
}

/// <summary>
/// Simple durable WAL between periodic <see cref="NodePersistence.Save"/> checkpoints.
/// </summary>
public sealed class Wal : IDisposable
{
    /// <summary>WAL format identifier string written at the start of a log segment.</summary>
    public const string Tag = "BIN-SYNC/WAL/v1";
    private const uint Magic = 0x4253574C; // 'B''S''W''L'
    private const ushort Version = 1;

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
                var ver = ReadBEU16(br);
                if (ver != Version)
                    throw new InvalidDataException($"Unsupported WAL version {ver}.");
            }
            long validEnd = 6;
            while (validEnd + 4 <= fs.Length)
            {
                fs.Position = validEnd;
                int len = ReadBEInt32(fs);
                if (len < 1)
                    throw new InvalidDataException("Invalid WAL record length.");
                if (fs.Position + len > fs.Length)
                    break; // Torn final record: discard it before accepting new appends.
                var payload = new byte[len];
                int read = fs.Read(payload, 0, len);
                if (read != len) break;
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

    /// <summary>Append a field-write record to the WAL and return its sequence.</summary>
    public long AppendFieldWrite(
        int namespaceId, byte[] recordId, ushort fieldId, byte[] value,
        Hlc hlc, byte[] authorNodeId)
    {
        var w = new Writer();
        w.U8((byte)WalRecordType.FieldWrite);
        w.Varint((ulong)namespaceId);
        w.B128(Codec.CanonicalB128(recordId));
        w.U16(fieldId);
        w.Bytes(value ?? Array.Empty<byte>()); // variable-length payload, not B128
        w.U64(hlc.Wall);
        w.U32(hlc.Logical);
        w.B128(Codec.CanonicalB128(authorNodeId));
        return AppendRaw(w.Value());
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
            _fs.Flush(flushToDisk: true);
            _sequence++;
            return _sequence;
        }
    }

    /// <summary>Replay WAL records into a node; returns the number of entries applied.</summary>
    public int Replay(Node node)
    {
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
                if (len < 1 || pos + 4L + len > end)
                    throw new InvalidDataException("WAL contains an incomplete record.");
                var payload = new byte[len];
                int read = _fs.Read(payload, 0, len);
                if (read != len)
                    throw new InvalidDataException("WAL record is truncated.");
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
        if (!File.Exists(path)) return 0;
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (fs.Length < 6) return 0;
        using var br = new BinaryReader(fs, System.Text.Encoding.UTF8, leaveOpen: true);
        if (ReadBEU32(br) != Magic) throw new InvalidDataException("Not a BinSync WAL.");
        if (ReadBEU16(br) != Version) throw new InvalidDataException("Unsupported WAL version.");
        var records = new List<byte[]>();
        while (fs.Position + 4 <= fs.Length)
        {
            int len = ReadBEInt32(fs);
            if (len < 1 || fs.Position + len > fs.Length)
                throw new InvalidDataException("WAL contains an incomplete record.");
            var payload = br.ReadBytes(len);
            if (payload.Length != len)
                throw new InvalidDataException("WAL record is truncated.");
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
        lock (_lock)
        {
            if (_fs is null) throw new ObjectDisposedException(nameof(Wal));
            // Prevent appends between snapshot creation and WAL truncation.
            NodePersistence.Save(node, snapshotPath);
            AppendCheckpoint();
            Truncate();
        }
    }

    // Validate framing and the complete payload before any replay mutation.
    // A complete but malformed record is corruption, not a torn tail.
    private static void ValidateRecord(byte[] payload)
    {
        var r = new Reader(payload);
        var type = (WalRecordType)r.U8();
        switch (type)
        {
            case WalRecordType.FieldWrite:
                {
                    ulong ns = r.Varint();
                    if (ns > int.MaxValue)
                        throw new InvalidDataException("WAL namespace id is out of range.");
                    _ = r.B128();
                    _ = r.U16();
                    _ = r.Bytes();
                    _ = r.U64();
                    _ = r.U32();
                    _ = r.B128();
                    break;
                }
            case WalRecordType.Checkpoint:
                _ = r.U64();
                break;
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
            case WalRecordType.FieldWrite:
                {
                    int ns = (int)r.Varint();
                    var rid = r.B128();
                    ushort fid = r.U16();
                    var value = r.Bytes();
                    ulong wall = r.U64();
                    uint logical = r.U32();
                    var author = r.B128();
                    node.ApplyRemoteField(ns, rid, fid, value, new Hlc(wall, logical), author);
                    return 1;
                }
            case WalRecordType.Checkpoint:
                return 0;
            default:
                throw new InvalidDataException($"Unknown WAL record type 0x{(byte)type:X2}.");
        }
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
