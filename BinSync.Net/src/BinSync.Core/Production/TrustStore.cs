// Trust model §15.6: TOFU, pre-shared allow-list, simple CA allow-list, revoke.
namespace BinSync;

/// <summary>TrustMode type used by the BIN-SYNC engine.</summary>
public enum TrustMode : byte
{
    Tofu = 1,
    PreShared = 2,
    CertificateAuthority = 3,
}

/// <summary>TrustedPeer type used by the BIN-SYNC engine.</summary>
public sealed class TrustedPeer
{
    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public byte[] NodeId { get; init; } = Array.Empty<byte>();
    /// <summary>Gets the public key.</summary>
    public byte[] PublicKey { get; init; } = Array.Empty<byte>();
    /// <summary>Gets the mode.</summary>
    public TrustMode Mode { get; init; }
    /// <summary>Gets or sets the revoked.</summary>
    public bool Revoked { get; set; }
    /// <summary>Gets the pinned at unix ms.</summary>
    public long PinnedAtUnixMs { get; init; }
}

/// <summary>TrustStore type used by the BIN-SYNC engine.</summary>
public sealed partial class TrustStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, TrustedPeer> _peers = new(StringComparer.Ordinal);
    private readonly HashSet<string> _caRoots = new(StringComparer.Ordinal);

    /// <summary>Gets or sets the default mode.</summary>
    public TrustMode DefaultMode { get; set; } = TrustMode.Tofu;

    /// <summary>PeerCount: peer count.</summary>
    public int PeerCount
    {
        get { lock (_gate) return _peers.Count; }
    }

    static string Key(byte[] nodeId) => Convert.ToHexString(Codec.CanonicalB128(nodeId));

    /// <summary>Allow: allow.</summary>
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

    /// <summary>AddCaRoot: add ca root.</summary>
    public void AddCaRoot(byte[] caNodeId)
    {
        lock (_gate) _caRoots.Add(Key(caNodeId));
    }

    /// <summary>Indicates whether ca root.</summary>
    public bool IsCaRoot(byte[] nodeId)
    {
        lock (_gate) return _caRoots.Contains(Key(nodeId));
    }

    /// <summary>Revoke: revoke.</summary>
    public void Revoke(byte[] nodeId)
    {
        lock (_gate)
        {
            if (_peers.TryGetValue(Key(nodeId), out var p))
                p.Revoked = true;
        }
    }

    /// <summary>Try to get; returns whether the operation succeeded.</summary>
    public bool TryGet(byte[] nodeId, out TrustedPeer? peer)
    {
        lock (_gate)
            return _peers.TryGetValue(Key(nodeId), out peer);
    }

    /// <summary>ResolvePublicKey: resolve public key.</summary>
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

    /// <summary>VerifyBatchOrThrow: verify batch or throw.</summary>
    public void VerifyBatchOrThrow(
        Batch batch, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey = null)
    {
        var pk = ResolvePublicKey(batch.IssuerNodeId, candidatePublicKey);
        if (pk is null)
            throw new SyncException("TRUST_REVOKED", batch.BatchId);
        if (!BatchSignature.VerifyBatch(pk, batch, signature))
            throw new SyncException("SIGNATURE_INVALID", batch.BatchId);
    }

    /// <summary>Try to verify batch; returns whether the operation succeeded.</summary>
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
