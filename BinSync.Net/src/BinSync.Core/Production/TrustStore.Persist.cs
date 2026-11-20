// Durable TOFU / PreShared pin file (§15.6). Separate from NODESTATE so
// identity keys survive even when the node snapshot is rotated.
namespace BinSync;

/// <summary>Pinned peer keys, CA roots, and batch signature verification.</summary>
public sealed partial class TrustStore
{
    internal (TrustMode DefaultMode,
        List<(byte[] NodeId, byte[] PublicKey, TrustMode Mode, bool Revoked, long PinnedAt)> Peers,
        List<byte[]> CaRoots) Snapshot()
    {
        lock (_gate)
        {
            var peers = new List<(byte[] NodeId, byte[] PublicKey, TrustMode Mode, bool Revoked, long PinnedAt)>(_peers.Count);
            foreach (var peer in _peers.Values)
            {
                peers.Add((
                    NodeId: peer.NodeId.ToArray(),
                    PublicKey: peer.PublicKey.ToArray(),
                    Mode: peer.Mode,
                    Revoked: peer.Revoked,
                    PinnedAt: peer.PinnedAtUnixMs));
            }
            peers.Sort((left, right) =>
                StringComparer.Ordinal.Compare(Convert.ToHexString(left.NodeId), Convert.ToHexString(right.NodeId)));
            var roots = _caRoots
                .Select(hex => Convert.FromHexString(hex))
                .OrderBy(Convert.ToHexString, StringComparer.Ordinal)
                .ToList();
            return (_defaultMode, peers, roots);
        }
    }

    internal void Restore(
        TrustMode mode,
        IEnumerable<(byte[] NodeId, byte[] PublicKey, TrustMode Mode, bool Revoked, long PinnedAt)> peers,
        IEnumerable<byte[]> caRoots)
    {
        lock (_gate)
        {
            DefaultMode = mode;
            _peers.Clear();
            _caRoots.Clear();
            foreach (var p in peers)
            {
                var id = NodeId.From(p.NodeId);
                _peers[Key(id)] = new TrustedPeer
                {
                    NodeId = id,
                    PublicKey = p.PublicKey.ToArray(),
                    Mode = p.Mode,
                    Revoked = p.Revoked,
                    PinnedAtUnixMs = p.PinnedAt,
                };
            }
            foreach (var id in caRoots)
                _caRoots.Add(Key(NodeId.From(id)));
        }
    }
}

/// <summary>Atomic trust-pin file. Magic <c>TRST</c>, version 1.</summary>
public static class TrustPersistence
{
    /// <summary>File or segment magic number identifying the format.</summary>
    public const uint Magic = 0x54525354; // 'TRST'
    /// <summary>Format version for this on-disk or wire structure.</summary>
    public const ushort Version = 1;

    /// <summary>Serialize the object to a durable byte encoding.</summary>
    public static byte[] Serialize(TrustStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        var w = new Writer();
        var snapshot = store.Snapshot();
        w.U32(Magic).U16(Version).U8((byte)snapshot.DefaultMode);
        var peers = snapshot.Peers;
        w.Varint((ulong)peers.Count);
        foreach (var p in peers)
        {
            w.B128(Codec.CanonicalB128(p.NodeId));
            w.Raw(p.PublicKey);
            w.U8((byte)p.Mode);
            w.U8(p.Revoked ? (byte)1 : (byte)0);
            w.U64(unchecked((ulong)p.PinnedAt));
        }
        var roots = snapshot.CaRoots;
        w.Varint((ulong)roots.Count);
        foreach (var id in roots)
            w.B128(Codec.CanonicalB128(id));
        return w.Value();
    }

    /// <summary>Deserialize an instance from a durable byte encoding.</summary>
    public static TrustStore Deserialize(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var r = new Reader(data);
        if (r.U32() != Magic)
            throw new InvalidDataException("Not a BIN-SYNC trust file.");
        var ver = r.U16();
        if (ver != Version)
            throw new InvalidDataException($"Unsupported trust file version {ver}.");
        var mode = ReadTrustMode(r.U8(), "default trust mode");
        int peerCount = ReadBoundedCount(r, 58, "peer");
        var peers = new List<(byte[], byte[], TrustMode, bool, long)>(peerCount);
        var peerIds = new HashSet<NodeId>();
        for (int i = 0; i < peerCount; i++)
        {
            var id = NodeId.From(r.B128());
            if (id.IsZero || !peerIds.Add(id))
                throw new InvalidDataException("Trust file contains a duplicate peer identity.");
            var pk = r.Raw(BatchSignature.PublicKeySize);
            var peerMode = ReadTrustMode(r.U8(), "peer trust mode");
            byte revokedFlag = r.U8();
            if (revokedFlag > 1)
                throw new InvalidDataException("Trust file contains an invalid revoked flag.");
            ulong rawPinnedAt = r.U64();
            if (rawPinnedAt > long.MaxValue)
                throw new InvalidDataException("Trust file contains an invalid pin timestamp.");
            peers.Add((id.ToArray(), pk, peerMode, revokedFlag == 1, (long)rawPinnedAt));
        }

        int rootCount = ReadBoundedCount(r, 16, "CA root");
        var roots = new List<byte[]>(rootCount);
        var rootIds = new HashSet<NodeId>();
        for (int i = 0; i < rootCount; i++)
        {
            var id = NodeId.From(r.B128());
            if (id.IsZero || !rootIds.Add(id))
                throw new InvalidDataException("Trust file contains a duplicate CA root.");
            roots.Add(id.ToArray());
        }
        if (!r.Eof())
            throw new InvalidDataException("Trust file contains trailing bytes.");

        var store = new TrustStore { DefaultMode = mode };
        store.Restore(mode, peers, roots);
        return store;
    }

    private static TrustMode ReadTrustMode(byte raw, string description)
    {
        var mode = (TrustMode)raw;
        if (mode is not (TrustMode.Tofu or TrustMode.PreShared or TrustMode.CertificateAuthority))
            throw new InvalidDataException($"Trust file contains an invalid {description}.");
        return mode;
    }

    private static int ReadBoundedCount(Reader reader, int minimumEntrySize, string description)
    {
        ulong count = reader.Varint();
        if (count > int.MaxValue || count > (ulong)(reader.Remaining / minimumEntrySize))
            throw new InvalidDataException($"Trust file {description} count exceeds the remaining data.");
        return (int)count;
    }

    /// <summary>Write the object to a file path on disk.</summary>
    public static void Save(TrustStore store, string path)
    {
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path required.", nameof(path));
        var image = Serialize(store);
        var dir = Path.GetDirectoryName(Path.GetFullPath(path))!;
        Directory.CreateDirectory(dir);
        var tmp = Path.Combine(dir, $".{Path.GetFileName(path)}.tmp-{Guid.NewGuid():N}");
        try
        {
            using (var stream = new FileStream(
                tmp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
            {
                stream.Write(image);
                stream.Flush(flushToDisk: true);
            }
            File.Move(tmp, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(tmp)) { try { File.Delete(tmp); } catch { /* best effort */ } }
        }
    }

    /// <summary>Load an instance from a file path on disk.</summary>
    public static TrustStore Load(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path required.", nameof(path));
        return Deserialize(File.ReadAllBytes(path));
    }
}
