// Trust model §15.6: TOFU, pre-shared allow-list, simple CA allow-list, revoke.
namespace BinSync;

/// <summary>How a peer key is trusted (pin, CA-signed, …).</summary>
public enum TrustMode : byte
{
    Tofu = 1,
    PreShared = 2,
    CertificateAuthority = 3,
}

/// <summary>One pinned peer entry in the trust store.</summary>
public sealed class TrustedPeer
{
    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public byte[] NodeId { get; init; } = Array.Empty<byte>();
    /// <summary>Ed25519 public key bytes pinned for this trusted peer.</summary>
    public byte[] PublicKey { get; init; } = Array.Empty<byte>();
    /// <summary>Trust mode controlling how signatures from this peer are validated.</summary>
    public TrustMode Mode { get; init; }
    /// <summary>True when this peer entry has been revoked.</summary>
    public bool Revoked { get; set; }
    /// <summary>Unix time in milliseconds when the peer key was pinned.</summary>
    public long PinnedAtUnixMs { get; init; }
}

/// <summary>Pinned peer keys, CA roots, and batch signature verification.</summary>
public sealed partial class TrustStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, TrustedPeer> _peers = new(StringComparer.Ordinal);
    private readonly HashSet<string> _caRoots = new(StringComparer.Ordinal);

    /// <summary>Default trust mode applied when pinning new peers.</summary>
    public TrustMode DefaultMode { get; set; } = TrustMode.Tofu;

    /// <summary>Number of peers currently present in the trust store.</summary>
    public int PeerCount
    {
        get { lock (_gate) return _peers.Count; }
    }

    static string Key(byte[] nodeId) => Convert.ToHexString(Codec.CanonicalB128(nodeId));

    /// <summary>Pin a peer public key under the given trust mode.</summary>
    public void Allow(byte[] nodeId, byte[] publicKey, TrustMode mode = TrustMode.PreShared)
    {
        if (publicKey.Length != BatchSignature.PublicKeySize)
            throw new ArgumentException("public key must be 32 bytes");
        var id = Codec.CanonicalB128(nodeId);
        lock (_gate)
        {
            _peers[Key(id)] = new TrustedPeer
            {
                NodeId = id,
                PublicKey = publicKey.ToArray(),
                Mode = mode,
                Revoked = false,
                PinnedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            };
        }
    }

    /// <summary>Mark a node id as a certificate-authority root.</summary>
    public void AddCaRoot(byte[] caNodeId)
    {
        lock (_gate) _caRoots.Add(Key(caNodeId));
    }

    /// <summary>True when the node is listed as a certificate-authority root.</summary>
    public bool IsCaRoot(byte[] nodeId)
    {
        lock (_gate) return _caRoots.Contains(Key(nodeId));
    }

    /// <summary>Revoke trust for a node id (signatures will no longer verify).</summary>
    public void Revoke(byte[] nodeId)
    {
        lock (_gate)
        {
            if (_peers.TryGetValue(Key(nodeId), out var p))
                p.Revoked = true;
        }
    }

    /// <summary>Try to get trusted-peer metadata for a node id.</summary>
    public bool TryGet(byte[] nodeId, out TrustedPeer? peer)
    {
        lock (_gate)
            return _peers.TryGetValue(Key(nodeId), out peer);
    }

    /// <summary>Resolve the public key to use for a node, optionally checking a candidate.</summary>
    public byte[]? ResolvePublicKey(NodeId nodeId, byte[]? candidatePublicKey = null) =>
        ResolvePublicKey(nodeId.ToArray(), candidatePublicKey);

    public byte[]? ResolvePublicKey(byte[] nodeId, byte[]? candidatePublicKey = null)
    {
        lock (_gate)
        {
            var k = Key(nodeId);
            if (_peers.TryGetValue(k, out var existing))
            {
                if (existing.Revoked) return null;
                if (candidatePublicKey is { Length: BatchSignature.PublicKeySize } &&
                    !existing.PublicKey.AsSpan().SequenceEqual(candidatePublicKey))
                    return null;
                return existing.PublicKey;
            }

            if (DefaultMode == TrustMode.Tofu &&
                candidatePublicKey is { Length: BatchSignature.PublicKeySize })
            {
                var id = Codec.CanonicalB128(nodeId);
                _peers[k] = new TrustedPeer
                {
                    NodeId = id,
                    PublicKey = candidatePublicKey.ToArray(),
                    Mode = TrustMode.Tofu,
                    Revoked = false,
                    PinnedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                };
                return candidatePublicKey;
            }

            return null;
        }
    }

    /// <summary>Verify the batch signature or throw if the peer is untrusted/invalid.</summary>
    public void VerifyBatchOrThrow(
        Batch batch, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey = null)
    {
        var pk = ResolvePublicKey(batch.IssuerNodeId, candidatePublicKey);
        if (pk is null)
            throw new SyncException("TRUST_REVOKED", batch.BatchId);
        if (!BatchSignature.VerifyBatch(pk, batch, signature))
            throw new SyncException("SIGNATURE_INVALID", batch.BatchId);
    }

    /// <summary>Verify the batch signature with the trust store; returns true if accepted.</summary>
    public bool TryVerifyBatch(Batch batch, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey = null)
    {
        try
        {
            VerifyBatchOrThrow(batch, signature, candidatePublicKey);
            return true;
        }
        catch (SyncException)
        {
            return false;
        }
    }
}
