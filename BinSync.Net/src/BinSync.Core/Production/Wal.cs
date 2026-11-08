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
    RemoteBatch = 0x04,
}

/// <summary>
/// Simple durable WAL between periodic <see cref="NodePersistence.Save"/> checkpoints.
/// </summary>
public sealed class Wal : IDisposable
{
    /// <summary>WAL format identifier string written at the start of a log segment.</summary>
    public const string Tag = "BIN-SYNC/WAL/v3";
    private const uint Magic = 0x4253574C; // 'B''S''W''L'
    private const ushort Version = 3;
    private const int RecordDigestLen = 16;
    private const string RecordDigestTag = "BIN-SYNC/WAL-RECORD/v2";

    private readonly string _path;
    private FileStream? _fs;
    private readonly object _lock = new();
    private long _sequence;
    // An I/O error during append/truncate has an indeterminate durability outcome.
    // The instance must be reopened and replayed before it can accept more work.
    private bool _faulted;

    /// <summary>Filesystem path of the write-ahead log.</summary>
    public string Path => _path;
    /// <summary>Monotonic sequence of the last appended WAL record.</summary>
    public long Sequence { get { lock (_lock) return _sequence; } }

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
                    throw new InvalidDataException($"Unsupported WAL version {version}; BIN-SYNC v3 requires WAL v3.");
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
                fs.ReadExactly(payload);
                {
                    var storedDigest = new byte[RecordDigestLen];
                    fs.ReadExactly(storedDigest);
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
        if (entry.OriginAuthorEpoch == 0 || entry.OriginSequence == 0)
            throw new InvalidDataException("WAL local mutation requires immutable Origin provenance.");
        w.U32(entry.OriginAuthorEpoch).U64(entry.OriginSequence);
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

    /// <summary>
    /// Append a validated incoming batch before its effects are published or ACKed.
    /// The exact wire payload and receiving peer identity are retained for replay.
    /// </summary>
    internal long AppendRemoteBatch(NodeId senderPeer, int senderEpoch,
        IReadOnlyList<AliasMapEntry> aliases, ReadOnlySpan<byte> batchPayload)
    {
        if (senderPeer.IsZero) throw new ArgumentException("Sender peer identity must be non-zero.", nameof(senderPeer));
        if (senderEpoch < 0) throw new ArgumentOutOfRangeException(nameof(senderEpoch));
        ArgumentNullException.ThrowIfNull(aliases);
        if (batchPayload.IsEmpty) throw new ArgumentException("Batch payload must not be empty.", nameof(batchPayload));
        var w = new Writer();
        w.U8((byte)WalRecordType.RemoteBatch);
        w.B128(senderPeer).U32((uint)senderEpoch);
        w.Varint((ulong)aliases.Count);
        foreach (var alias in aliases)
            w.U16(alias.Alias).B128(alias.NodeId).U32((uint)alias.Epoch);
        w.Varint((ulong)batchPayload.Length);
        w.Raw(batchPayload);
        var payload = w.Value();
        ValidateRecord(payload);
        return AppendRaw(payload);
    }

    /// <summary>Append a checkpoint marker and return its sequence.</summary>
    internal long AppendCheckpoint()
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
            if (_faulted)
                throw new InvalidOperationException("WAL is faulted after an earlier I/O error; reopen and replay it before further use.");
            try
            {
                Span<byte> lenBuf = stackalloc byte[4];
                WriteBEInt32(lenBuf, payload.Length);
                _fs.Write(lenBuf);
                _fs.Write(payload);
                _fs.Write(ComputeRecordDigest(payload));
                _fs.Flush(flushToDisk: true);
                _sequence++;
                return _sequence;
            }
            catch
            {
                // Flush failures are ambiguous: the complete record may already be
                // durable. Never append another record behind an uncertain outcome.
                _faulted = true;
                throw;
            }
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
            if (_faulted)
                throw new InvalidOperationException("WAL is faulted after an earlier I/O error; reopen it before replay.");
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
                _fs.ReadExactly(payload);
                {
                    var storedDigest = new byte[RecordDigestLen];
                    _fs.ReadExactly(storedDigest);
                    VerifyRecordDigest(payload, storedDigest);
                }
                ValidateRecord(payload);
                pos = _fs.Position;
                records.Add(payload);
            }

            int applied = ReplayRecordsAtomically(node, records);
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
        if (fs.Length == 0) return 0;
        if (fs.Length < 6)
            throw new InvalidDataException("WAL header is truncated.");
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
            var payload = new byte[len];
            fs.ReadExactly(payload);
            {
                var storedDigest = new byte[RecordDigestLen];
                fs.ReadExactly(storedDigest);
                VerifyRecordDigest(payload, storedDigest);
            }
            ValidateRecord(payload);
            records.Add(payload);
        }

        // Replay on an isolated state image. A late semantic failure must not
        // publish any earlier local or remote WAL effects to the target node.
        return ReplayRecordsAtomically(node, records);
        }
    }

    /// <summary>Truncate the WAL after a successful checkpoint.</summary>
    internal void Truncate()
    {
        lock (_lock)
        {
            if (_fs is null) throw new ObjectDisposedException(nameof(Wal));
            if (_faulted)
                throw new InvalidOperationException("WAL is faulted after an earlier I/O error; reopen and replay it before truncating.");
            try
            {
                // Never truncate the active WAL in place: a crash between SetLength(0)
                // and a complete header write could destroy the only recovery log.
                // Build a fresh v2 segment beside it, fsync it, then atomically replace.
                var tempPath = _path + ".reset-" + Guid.NewGuid().ToString("N");
                try
                {
                    using (var temp = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write,
                        FileShare.None, 4096, FileOptions.WriteThrough))
                        WriteHeader(temp);

                    _fs.Dispose();
                    _fs = null;
                    File.Move(tempPath, _path, overwrite: true);
                    _fs = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
                        4096, FileOptions.WriteThrough);
                    _fs.Position = _fs.Length;
                    _sequence = 0;
                }
                finally
                {
                    if (File.Exists(tempPath))
                    {
                        try { File.Delete(tempPath); } catch { /* best effort */ }
                    }
                }
            }
            catch
            {
                _faulted = true;
                throw;
            }
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
                if (_faulted)
                    throw new InvalidOperationException("WAL is faulted after an earlier I/O error; reopen it before checkpointing.");
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
            case WalRecordType.RemoteBatch:
                {
                    var remote = ReadRemoteBatch(r);
                    if (remote.SenderPeer.IsZero || remote.BatchPayload.Length == 0)
                        throw new InvalidDataException("Invalid WAL remote-batch metadata.");
                    if (remote.Aliases.Count > 0 &&
                        !remote.Aliases.Any(x => x.NodeId == remote.SenderPeer && x.Epoch == remote.SenderEpoch))
                        throw new InvalidDataException("WAL remote-batch session map does not identify its sender.");
                    CausalWire.ValidatePayload(remote.BatchPayload);
                    break;
                }
            case WalRecordType.LocalMutation:
                {
                    var (_, entry) = ReadLocalMutation(r);
                    if (entry.Seq <= 0 || entry.ChangeId.IsZero || entry.AuthorNodeId.IsZero || entry.CreatedAt < 0 || entry.OriginAuthorEpoch == 0 || entry.OriginSequence == 0)
                        throw new InvalidDataException("Invalid WAL local-mutation metadata.");
                    var decoded = Ops.DecodeOp(entry.Payload, fullAuthor: false).Op;
                    byte expectedKind = decoded switch
                    {
                        OpFieldWrite => OpsConst.TagFieldWrite,
                        OpFieldTomb => OpsConst.TagFieldTomb,
                        OpRecordTombstone => OpsConst.TagRecordTomb,
                        _ => throw new InvalidDataException("Unsupported WAL local-mutation operation.")
                    };
                    if (entry.OpKind != expectedKind)
                        throw new InvalidDataException("WAL local-mutation operation tag mismatch.");
                    ushort origin = decoded switch
                    {
                        OpFieldWrite x => x.Origin,
                        OpFieldTomb x => x.Origin,
                        OpRecordTombstone x => x.Origin,
                        _ => 0
                    };
                    if (origin != 1)
                        throw new InvalidDataException("WAL local-mutation payload must use local origin 1.");
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

    // Validate local Outbox continuity for the whole WAL before applying any
    // mutation. Structural frame validation alone cannot catch a later sequence gap.
    private static void ValidateReplayLocalMutationIdentities(Node node, IReadOnlyList<byte[]> records)
    {
        var initialSeen = new HashSet<(int Ns, BatchId ChangeId)>(node.SeenChanges);
        var seenInWal = new HashSet<(int Ns, BatchId ChangeId)>();
        foreach (var payload in records)
        {
            var reader = new Reader(payload);
            if ((WalRecordType)reader.U8() != WalRecordType.LocalMutation)
                continue;

            var (_, entry) = ReadLocalMutation(reader);
            if (entry.AuthorNodeId != node.NodeId)
                throw new InvalidDataException("WAL local mutation belongs to a different node identity.");

            var key = (entry.NamespaceId, entry.ChangeId);
            if (initialSeen.Contains(key)) continue; // already represented by the loaded snapshot
            if (!seenInWal.Add(key))
                throw new InvalidDataException("WAL contains a duplicate local ChangeId.");
        }
    }

    private static int ReplayRecordsAtomically(Node node, IReadOnlyList<byte[]> records)
    {
        ValidateReplayLocalMutationIdentities(node, records);
        using var staged = NodePersistence.CloneForWalReplay(node);
        int applied = 0;
        foreach (var payload in records)
            applied += ApplyRecord(staged, payload);
        node.CommitWalReplayFrom(staged);
        return applied;
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
            case WalRecordType.RemoteBatch:
                {
                    var remote = ReadRemoteBatch(r);
                    return node.ReplayRemoteBatch(remote.SenderPeer, remote.SenderEpoch,
                        remote.Aliases, remote.BatchPayload) ? 1 : 0;
                }
            default:
                throw new InvalidDataException($"Unknown WAL record type 0x{(byte)type:X2}.");
        }
    }

    private static (NodeId SenderPeer, int SenderEpoch, List<AliasMapEntry> Aliases, byte[] BatchPayload)
        ReadRemoteBatch(Reader r)
    {
        var senderPeer = NodeId.From(r.B128());
        ulong epochValue = r.U32();
        if (epochValue > int.MaxValue)
            throw new InvalidDataException("Invalid WAL remote-batch sender epoch.");
        int aliasCount = checked((int)r.Varint());
        if (aliasCount > ushort.MaxValue)
            throw new InvalidDataException("WAL remote-batch alias map is too large.");
        var aliases = new List<AliasMapEntry>(aliasCount);
        var aliasIds = new HashSet<ushort>();
        var nodeIds = new HashSet<NodeId>();
        for (int i = 0; i < aliasCount; i++)
        {
            ushort alias = r.U16();
            var nodeId = NodeId.From(r.B128());
            ulong aliasEpochValue = r.U32();
            if (alias == 0 || nodeId.IsZero || aliasEpochValue > int.MaxValue ||
                !aliasIds.Add(alias) || !nodeIds.Add(nodeId))
                throw new InvalidDataException("Invalid WAL remote-batch alias map.");
            aliases.Add(new AliasMapEntry(alias, nodeId, (int)aliasEpochValue));
        }
        var batchPayload = r.BytesMemory().ToArray();
        return (senderPeer, (int)epochValue, aliases, batchPayload);
    }

    private static (bool ApplyToStorage, OutboxEntry Entry) ReadLocalMutation(Reader r)
    {
        byte applyFlag = r.U8();
        if (applyFlag > 1) throw new InvalidDataException("Invalid WAL local-mutation flags.");
        ulong ns = r.Varint();
        ulong seq = r.U64();
        var changeId = BatchId.From(r.B128());
        uint originAuthorEpoch = r.U32();
        ulong originSequence = r.U64();
        var recordId = RecordId.From(r.B128());
        byte opKind = r.U8();
        ulong payloadLength = r.Varint();
        if (ns > int.MaxValue || seq > long.MaxValue || originAuthorEpoch == 0 || originSequence == 0 || payloadLength > int.MaxValue)
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
            OriginAuthorEpoch = originAuthorEpoch,
            OriginSequence = originSequence,
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
        s.ReadExactly(b);
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
