// Durable TOFU / PreShared pin file (§15.6). Separate from NODESTATE so
// identity keys survive even when the node snapshot is rotated.
namespace BinSync;

public sealed partial class TrustStore
{
    internal List<(byte[] NodeId, byte[] PublicKey, TrustMode Mode, bool Revoked, long PinnedAt)> SnapshotPeers()
    {
        lock (_gate)
        {
            return _peers.Values
                .Select(p => (p.NodeId, p.PublicKey, p.Mode, p.Revoked, p.PinnedAtUnixMs))
                .OrderBy(p => Convert.ToHexString(p.NodeId), StringComparer.Ordinal)
                .ToList();
        }
    }

    internal List<byte[]> SnapshotCaRoots()
    {
        lock (_gate)
        {
            return _caRoots
                .Select(hex => Convert.FromHexString(hex))
                .OrderBy(Convert.ToHexString, StringComparer.Ordinal)
                .ToList();
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
                var id = Codec.CanonicalB128(p.NodeId);
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
                _caRoots.Add(Key(id));
        }
    }
}

/// <summary>Atomic trust-pin file. Magic <c>TRST</c>, version 1.</summary>
public static class TrustPersistence
{
    public const uint Magic = 0x54525354; // 'TRST'
    public const ushort Version = 1;

    public static byte[] Serialize(TrustStore store)
    {
        var w = new Writer();
        w.U32(Magic).U16(Version).U8((byte)store.DefaultMode);
        var peers = store.SnapshotPeers();
        w.Varint((ulong)peers.Count);
        foreach (var p in peers)
        {
            w.B128(Codec.CanonicalB128(p.NodeId));
            w.Raw(p.PublicKey);
            w.U8((byte)p.Mode);
            w.U8(p.Revoked ? (byte)1 : (byte)0);
            w.U64(unchecked((ulong)p.PinnedAt));
        }
        var roots = store.SnapshotCaRoots();
        w.Varint((ulong)roots.Count);
        foreach (var id in roots)
            w.B128(Codec.CanonicalB128(id));
        return w.Value();
    }

    public static TrustStore Deserialize(byte[] data)
    {
        var r = new Reader(data);
        if (r.U32() != Magic)
            throw new InvalidDataException("Not a BIN-SYNC trust file.");
        var ver = r.U16();
        if (ver != Version)
            throw new InvalidDataException($"Unsupported trust file version {ver}.");
        var mode = (TrustMode)r.U8();
        int n = (int)r.Varint();
        var peers = new List<(byte[], byte[], TrustMode, bool, long)>(n);
        for (int i = 0; i < n; i++)
        {
            var id = r.B128();
            var pk = r.Raw(BatchSignature.PublicKeySize);
            var pm = (TrustMode)r.U8();
            bool revoked = r.U8() != 0;
            long pinned = unchecked((long)r.U64());
            peers.Add((id, pk, pm, revoked, pinned));
        }
        int c = (int)r.Varint();
        var roots = new List<byte[]>(c);
        for (int i = 0; i < c; i++)
            roots.Add(r.B128());
        var store = new TrustStore { DefaultMode = mode };
        store.Restore(mode, peers, roots);
        return store;
    }

    public static void Save(TrustStore store, string path)
    {
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path required.", nameof(path));
        var image = Serialize(store);
        var dir = Path.GetDirectoryName(Path.GetFullPath(path))!;
        Directory.CreateDirectory(dir);
        var tmp = Path.Combine(dir, $".{Path.GetFileName(path)}.tmp-{Guid.NewGuid():N}");
        try
        {
            File.WriteAllBytes(tmp, image);
            File.Move(tmp, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(tmp)) { try { File.Delete(tmp); } catch { /* best effort */ } }
        }
    }

    public static TrustStore Load(string path) => Deserialize(File.ReadAllBytes(path));
}
