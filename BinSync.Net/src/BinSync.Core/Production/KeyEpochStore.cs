// Tracks per-(namespace, node) E2EE key epochs; applies verified KEY_ROTATION notices (§14.9).
namespace BinSync;

/// <summary>
/// Mutable registry of the highest accepted key epoch per publisher nodeId within a namespace.
/// Epochs only advance after a valid Ed25519 proof under the publisher's identity public key.
/// </summary>
public sealed class KeyEpochStore
{
    // namespaceId → nodeId-hex → epoch
    private readonly Dictionary<int, Dictionary<string, ushort>> _epochs = new();

    /// <summary>Trusted identity public keys (32-byte Ed25519) keyed by nodeId hex.</summary>
    private readonly Dictionary<string, byte[]> _identityKeys = new();

    public void TrustIdentity(ReadOnlySpan<byte> nodeId, ReadOnlySpan<byte> publicKey)
    {
        if (publicKey.Length != BatchSignature.PublicKeySize)
            throw new ArgumentException($"public key must be {BatchSignature.PublicKeySize} bytes");
        _identityKeys[Hex(nodeId)] = publicKey.ToArray();
    }

    public bool TryGetIdentity(ReadOnlySpan<byte> nodeId, out byte[] publicKey) =>
        _identityKeys.TryGetValue(Hex(nodeId), out publicKey!);

    /// <summary>Current known key epoch for a publisher (0 if never rotated).</summary>
    public ushort GetEpoch(int namespaceId, ReadOnlySpan<byte> nodeId)
    {
        if (!_epochs.TryGetValue(namespaceId, out var map)) return 0;
        return map.TryGetValue(Hex(nodeId), out var e) ? e : (ushort)0;
    }

    /// <summary>
    /// Apply a KEY_ROTATION notice. Returns true when the local epoch advanced.
    /// Rejects: missing trust, bad proof, non-increasing epoch, nodeId mismatch vs trusted set.
    /// </summary>
    public bool TryApply(KeyRotationNotice notice)
    {
        if (notice.NewKeyEpoch <= notice.OldKeyEpoch) return false;
        var hex = Hex(notice.NodeId);
        if (!_identityKeys.TryGetValue(hex, out var pub))
            return false; // UNKNOWN identity → drop (caller may surface UNKNOWN_KEY_EPOCH)
        if (!KeyRotation.VerifyProof(notice, pub))
            return false;

        if (!_epochs.TryGetValue(notice.NamespaceId, out var map))
        {
            map = new Dictionary<string, ushort>();
            _epochs[notice.NamespaceId] = map;
        }
        ushort current = map.TryGetValue(hex, out var cur) ? cur : (ushort)0;
        // Accept only when old matches current (or current is 0 and old is 0) and new advances.
        if (current != 0 && notice.OldKeyEpoch != current)
            return false;
        if (notice.NewKeyEpoch <= current)
            return false;
        map[hex] = notice.NewKeyEpoch;
        return true;
    }

    /// <summary>Force-set epoch without proof (bootstrap / tests). Prefer <see cref="TryApply"/>.</summary>
    public void SeedEpoch(int namespaceId, ReadOnlySpan<byte> nodeId, ushort epoch)
    {
        if (!_epochs.TryGetValue(namespaceId, out var map))
        {
            map = new Dictionary<string, ushort>();
            _epochs[namespaceId] = map;
        }
        map[Hex(nodeId)] = epoch;
    }

    static string Hex(ReadOnlySpan<byte> id) => Convert.ToHexString(id);
}
