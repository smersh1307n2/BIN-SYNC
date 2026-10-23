// Append-only write-ahead log for incremental durability between full snapshots.
// Format: magic + version, then length-prefixed records (type + payload).
// Crash recovery: Load snapshot (optional) then Wal.Replay onto the node.
namespace BinSync;

/// <summary>Record types stored in the WAL.</summary>
public enum WalRecordType : byte
{
    /// <summary>Field write with original author/HLC (same as ApplyRemoteField).</summary>
    FieldWrite = 0x01,
    /// <summary>Marks that a full NodePersistence snapshot was taken; safe to truncate before this.</summary>
    Checkpoint = 0x02,
}

/// <summary>
/// Simple durable WAL. Not a full database journal — enough to survive process crash
/// between periodic <see cref="NodePersistence.Save"/> checkpoints.
/// </summary>
public sealed class Wal : IDisposable
{
    public const string Tag = "BIN-SYNC/WAL/v1";
    private const uint Magic = 0x4253574C; // 'B''S''W''L'
    private const ushort Version = 1;

    private readonly string _path;
    private FileStream? _fs;
    private readonly object _lock = new();
    private long _sequence;

    public string Path => _path;
    public long Sequence => _sequence;

    private Wal(string path, FileStream fs, long sequence)
    {
        _path = path;
        _fs = fs;
        _sequence = sequence;
    }

    /// <summary>Create or open a WAL file for append.</summary>
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
            fs.Position = 6;
            while (fs.Position + 5 <= fs.Length)
            {
                int len = ReadBEInt32(fs);
                if (len < 1 || fs.Position + len > fs.Length) break;
                fs.Position += len;
                seq++;
            }
            fs.Position = fs.Length;
        }
        return new Wal(path, fs, seq);
    }

    /// <summary>Append a field write (author/HLC preserved for LWW).</summary>
    public long AppendFieldWrite(
        int namespaceId, byte[] recordId, ushort fieldId, byte[] value,
        Hlc hlc, byte[] authorNodeId)
    {
        var w = new Writer();
        w.U8((byte)WalRecordType.FieldWrite);
        w.Varint((ulong)namespaceId);
        w.B128(Codec.CanonicalB128(recordId));
        w.U16(fieldId);
        w.B128(value ?? Array.Empty<byte>());
        w.U64(hlc.Wall);
        w.U32(hlc.Logical);
        w.B128(Codec.CanonicalB128(authorNodeId));
        return AppendRaw(w.Value());
    }

    /// <summary>Append checkpoint marker after a successful full snapshot.</summary>
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

    /// <summary>Replay all records onto node. Returns field writes applied.</summary>
    public int Replay(Node node)
    {
        lock (_lock)
        {
            if (_fs is null) throw new ObjectDisposedException(nameof(Wal));
            long end = _fs.Length;
            long pos = 6;
            int applied = 0;
            while (pos + 5 <= end)
            {
                _fs.Position = pos;
                int len = ReadBEInt32(_fs);
                if (len < 1 || pos + 4 + len > end) break;
                var payload = new byte[len];
                int read = _fs.Read(payload, 0, len);
                if (read != len) break;
                pos = _fs.Position;
                applied += ApplyRecord(node, payload);
            }
            _fs.Position = end;
            return applied;
        }
    }

    /// <summary>Read-only replay from a closed file path.</summary>
    public static int ReplayFile(string path, Node node)
    {
        if (!File.Exists(path)) return 0;
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (fs.Length < 6) return 0;
        using var br = new BinaryReader(fs, System.Text.Encoding.UTF8, leaveOpen: true);
        if (ReadBEU32(br) != Magic) throw new InvalidDataException("Not a BinSync WAL.");
        if (ReadBEU16(br) != Version) throw new InvalidDataException("Unsupported WAL version.");
        int applied = 0;
        while (fs.Position + 5 <= fs.Length)
        {
            int len = ReadBEInt32(fs);
            if (len < 1 || fs.Position + len > fs.Length) break;
            var payload = br.ReadBytes(len);
            if (payload.Length != len) break;
            applied += ApplyRecord(node, payload);
        }
        return applied;
    }

    /// <summary>Truncate WAL after checkpoint (keeps header only).</summary>
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

    /// <summary>Save full snapshot, append checkpoint, truncate WAL.</summary>
    public void Checkpoint(Node node, string snapshotPath)
    {
        NodePersistence.Save(node, snapshotPath);
        AppendCheckpoint();
        Truncate();
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
                var value = r.B128();
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

    public void Dispose()
    {
        lock (_lock)
        {
            _fs?.Dispose();
            _fs = null;
        }
    }
}
