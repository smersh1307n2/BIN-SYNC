// Trust model §15.6: TOFU, pre-shared allow-list, simple CA allow-list, revoke.
namespace BinSync;

/// <summary>How a peer identity key was established.</summary>
public enum TrustMode : byte
{
    /// <summary>First-use: first observed key is pinned forever.</summary>
    Tofu = 1,
    /// <summary>Only keys explicitly added via <see cref="TrustStore.Allow"/>.</summary>
    PreShared = 2,
    /// <summary>Keys signed / listed under a trusted root id (allow-list of CA node ids).</summary>
    CertificateAuthority = 3,
}

/// <summary>One pinned peer public key.</summary>
public sealed class TrustedPeer
{
    public byte[] NodeId { get; init; } = Array.Empty<byte>();
    public byte[] PublicKey { get; init; } = Array.Empty<byte>();
    public TrustMode Mode { get; init; }
    public bool Revoked { get; set; }
    public long PinnedAtUnixMs { get; init; }
}

/// <summary>
/// Thread-safe trust database for Ed25519 peer keys (§15.6).
/// Opt-in: attach to <see cref="Node.Trust"/> and call <see cref="VerifyBatchOrThrow"/>.
/// </summary>
public sealed class TrustStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, TrustedPeer> _peers = new(StringComparer.Ordinal);
    private readonly HashSet<string> _caRoots = new(StringComparer.Ordinal); // nodeId hex of CA roots

    public TrustMode DefaultMode { get; set; } = TrustMode.Tofu;

    public int PeerCount
    {
        get { lock (_gate) return _peers.Count; }
    }

    static string Key(byte[] nodeId) => Convert.ToHexString(Codec.CanonicalB128(nodeId));

    /// <summary>Pre-share or CA-enrol a peer key (also used after TOFU pin).</summary>
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

    public void AddCaRoot(byte[] caNodeId)
    {
        lock (_gate) _caRoots.Add(Key(caNodeId));
    }

    public bool IsCaRoot(byte[] nodeId)
    {
        lock (_gate) return _caRoots.Contains(Key(nodeId));
    }

    public void Revoke(byte[] nodeId)
    {
        lock (_gate)
        {
            if (_peers.TryGetValue(Key(nodeId), out var p))
                p.Revoked = true;
        }
    }

    public bool TryGet(byte[] nodeId, out TrustedPeer? peer)
    {
        lock (_gate)
            return _peers.TryGetValue(Key(nodeId), out peer);
    }

    /// <summary>
    /// Resolve key for <paramref name="nodeId"/>.
    /// TOFU: first observed <paramref name="candidatePublicKey"/> is pinned.
    /// PreShared: must already be in store.
    /// Returns null if untrusted / revoked.
    /// </summary>
    public byte[]? ResolvePublicKey(byte[] nodeId, byte[]? candidatePublicKey = null)
    {
        lock (_gate)
        {
            var k = Key(nodeId);
            if (_peers.TryGetValue(k, out var existing))
            {
                if (existing.Revoked) return null;
                // Key substitution attempt under TOFU / pin
                if (candidatePublicKey is { Length: BatchSignature.PublicKeySize } &&
                    !existing.PublicKey.AsSpan().SequenceEqual(candidatePublicKey))
                    return null; // TRUST_REVOKED / pin mismatch
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

    /// <summary>
    /// Verify a batch signature under this trust store.
    /// Throws <see cref="SyncException"/> with TRUST_REVOKED or SIGNATURE_INVALID.
    /// </summary>
    public void VerifyBatchOrThrow(
        Batch batch, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey = null)
    {
        var pk = ResolvePublicKey(batch.IssuerNodeId, candidatePublicKey);
        if (pk is null)
            throw new SyncException("TRUST_REVOKED", batch.BatchId);
        if (!BatchSignature.VerifyBatch(pk, batch, signature))
            throw new SyncException("SIGNATURE_INVALID", batch.BatchId);
    }

    /// <summary>Non-throwing verify.</summary>
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
