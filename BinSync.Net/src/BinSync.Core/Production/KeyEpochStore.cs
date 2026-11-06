// Tracks per-(namespace, node) E2EE key epochs; applies verified KEY_ROTATION notices (§14.9).
namespace BinSync;

/// <summary>
/// Mutable registry of the highest accepted key epoch per publisher nodeId within a namespace.
/// Epochs only advance after a valid Ed25519 proof under the publisher's identity public key.
/// </summary>
public sealed class KeyEpochStore
{
    // namespaceId → nodeId-hex → epoch
    private readonly object _gate = new();
    private readonly Dictionary<int, Dictionary<string, ushort>> _epochs = new();

    /// <summary>Trusted identity public keys (32-byte Ed25519) keyed by nodeId hex.</summary>
    private readonly Dictionary<string, byte[]> _identityKeys = new();

    /// <summary>Store or update trusted public-key material for a node id.</summary>
    public void TrustIdentity(NodeId nodeId, ReadOnlySpan<byte> publicKey)
    {
        if (publicKey.Length != BatchSignature.PublicKeySize)
            throw new ArgumentException($"public key must be {BatchSignature.PublicKeySize} bytes");
        lock (_gate) _identityKeys[Hex(nodeId)] = publicKey.ToArray();
    }

    /// <summary>Try to load stored public identity material for a node id.</summary>
    public bool TryGetIdentity(NodeId nodeId, out byte[] publicKey)
    {
        lock (_gate)
        {
            if (_identityKeys.TryGetValue(Hex(nodeId), out var stored))
            {
                publicKey = stored.ToArray();
                return true;
            }
            publicKey = Array.Empty<byte>();
            return false;
        }
    }

    /// <summary>Current known key epoch for a publisher (0 if never rotated).</summary>
    public ushort GetEpoch(int namespaceId, NodeId nodeId)
    {
        lock (_gate)
        {
            if (!_epochs.TryGetValue(namespaceId, out var map)) return 0;
            return map.TryGetValue(Hex(nodeId), out var e) ? e : (ushort)0;
        }
    }

    /// <summary>
    /// Apply a KEY_ROTATION notice. Returns true when the local epoch advanced.
    /// Rejects: missing trust, bad proof, non-increasing epoch, nodeId mismatch vs trusted set.
    /// </summary>
    public bool TryApply(KeyRotationNotice notice)
    {
        lock (_gate)
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
            if (current != 0 && notice.OldKeyEpoch != current)
                return false;
            if (notice.NewKeyEpoch <= current)
                return false;
            map[hex] = notice.NewKeyEpoch;
            return true;
        }
    }

    /// <summary>Force-set epoch without proof (bootstrap / tests). Prefer <see cref="TryApply"/>.</summary>
    public void SeedEpoch(int namespaceId, NodeId nodeId, ushort epoch)
    {
        lock (_gate)
        {
            if (!_epochs.TryGetValue(namespaceId, out var map))
            {
                map = new Dictionary<string, ushort>();
                _epochs[namespaceId] = map;
            }
            map[Hex(nodeId)] = epoch;
        }
    }

    static string Hex(NodeId id) => Convert.ToHexString(id.ToArray());
}
