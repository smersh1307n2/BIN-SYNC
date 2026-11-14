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
    public NodeId NodeId { get; init; }
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
    private TrustMode _defaultMode = TrustMode.Tofu;

    /// <summary>Default trust mode applied when pinning new peers.</summary>
    public TrustMode DefaultMode
    {
        get
        {
            lock (_gate) return _defaultMode;
        }
        set
        {
            if (value is not (TrustMode.Tofu or TrustMode.PreShared or TrustMode.CertificateAuthority))
                throw new ArgumentOutOfRangeException(nameof(value));
            lock (_gate) _defaultMode = value;
        }
    }

    /// <summary>Number of peers currently present in the trust store.</summary>
    public int PeerCount
    {
        get { lock (_gate) return _peers.Count; }
    }

    static string Key(NodeId nodeId) => Convert.ToHexString(nodeId.ToArray());

    /// <summary>Pin a peer public key under the given trust mode.</summary>
    public void Allow(NodeId nodeId, byte[] publicKey, TrustMode mode = TrustMode.PreShared)
    {
        if (nodeId.IsZero) throw new ArgumentException("Peer identity must be non-zero.", nameof(nodeId));
        ArgumentNullException.ThrowIfNull(publicKey);
        if (mode is not (TrustMode.Tofu or TrustMode.PreShared or TrustMode.CertificateAuthority))
            throw new ArgumentOutOfRangeException(nameof(mode));
        if (publicKey.Length != BatchSignature.PublicKeySize)
            throw new ArgumentException("public key must be 32 bytes");
        var id = nodeId;
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
    public void AddCaRoot(NodeId caNodeId)
    {
        if (caNodeId.IsZero) throw new ArgumentException("CA identity must be non-zero.", nameof(caNodeId));
        lock (_gate) _caRoots.Add(Key(caNodeId));
    }

    /// <summary>True when the node is listed as a certificate-authority root.</summary>
    public bool IsCaRoot(NodeId nodeId)
    {
        lock (_gate) return _caRoots.Contains(Key(nodeId));
    }

    /// <summary>Revoke trust for a node id (signatures will no longer verify).</summary>
    public void Revoke(NodeId nodeId)
    {
        lock (_gate)
        {
            if (_peers.TryGetValue(Key(nodeId), out var p))
                p.Revoked = true;
        }
    }

    /// <summary>Try to get detached trusted-peer metadata for a node id.</summary>
    public bool TryGet(NodeId nodeId, out TrustedPeer? peer)
    {
        lock (_gate)
        {
            if (_peers.TryGetValue(Key(nodeId), out var stored))
            {
                peer = ClonePeer(stored);
                return true;
            }
            peer = null;
            return false;
        }
    }

    private static TrustedPeer ClonePeer(TrustedPeer peer) => new()
    {
        NodeId = peer.NodeId,
        PublicKey = peer.PublicKey.ToArray(),
        Mode = peer.Mode,
        Revoked = peer.Revoked,
        PinnedAtUnixMs = peer.PinnedAtUnixMs,
    };

    /// <summary>
    /// Resolve a pinned key or return an unpinned TOFU candidate. A candidate is not
    /// persisted until <see cref="VerifyBatchOrThrow"/> verifies a valid signature.
    /// </summary>
    public byte[]? ResolvePublicKey(NodeId nodeId, byte[]? candidatePublicKey = null)
    {
        if (nodeId.IsZero) return null;
        if (candidatePublicKey is not null &&
            candidatePublicKey.Length != BatchSignature.PublicKeySize)
            return null;

        lock (_gate)
        {
            var k = Key(nodeId);
            if (_peers.TryGetValue(k, out var existing))
            {
                if (existing.Revoked) return null;
                if (candidatePublicKey is not null &&
                    !existing.PublicKey.AsSpan().SequenceEqual(candidatePublicKey))
                    return null;
                return existing.PublicKey.ToArray();
            }

            return _defaultMode == TrustMode.Tofu
                ? candidatePublicKey?.ToArray()
                : null;
        }
    }

    /// <summary>Verify the batch signature or throw if the peer is untrusted/invalid.</summary>
    public void VerifyBatchOrThrow(
        Batch batch, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey = null)
    {
        ArgumentNullException.ThrowIfNull(batch);
        var ownedSignature = signature.ToArray();
        var ownedCandidate = candidatePublicKey?.ToArray();
        lock (_gate)
            VerifyBatchOrThrowLocked(batch, ownedSignature, ownedCandidate);
    }

    /// <summary>
    /// Verify a batch and execute its commit while trust changes are excluded.
    /// The callback is synchronous and is intended for the Node's already-gated
    /// internal apply path; it must not invoke application callbacks.
    /// </summary>
    internal T VerifyBatchAndExecute<T>(
        Batch batch, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey, Func<T> execute)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(execute);
        var ownedSignature = signature.ToArray();
        var ownedCandidate = candidatePublicKey?.ToArray();
        lock (_gate)
        {
            VerifyBatchOrThrowLocked(batch, ownedSignature, ownedCandidate);
            return execute();
        }
    }

    private void VerifyBatchOrThrowLocked(Batch batch, byte[] signature, byte[]? candidatePublicKey)
    {
        var issuer = batch.IssuerNodeId;
        var batchId = batch.BatchId;
        var payload = BatchSignature.BuildSignPayload(
            batch.NamespaceId, batch.IssuerEpoch, issuer, batchId, batch.BatchDigest);
        var publicKey = ResolvePublicKey(issuer, candidatePublicKey);
        if (publicKey is null)
            throw new SyncException("TRUST_REVOKED", batchId);
        if (!BatchSignature.Verify(publicKey, payload, signature))
            throw new SyncException("SIGNATURE_INVALID", batchId);

        // TOFU must prove possession before first-use pinning. Keep the trust lock
        // through verification and publication so concurrent revocation/replacement
        // is ordered either before or after the accepted batch.
        var key = Key(issuer);
        if (_peers.TryGetValue(key, out var existing))
        {
            if (existing.Revoked || !existing.PublicKey.AsSpan().SequenceEqual(publicKey))
                throw new SyncException("TRUST_REVOKED", batchId);
            return;
        }

        if (_defaultMode != TrustMode.Tofu || candidatePublicKey is null ||
            !candidatePublicKey.AsSpan().SequenceEqual(publicKey))
            throw new SyncException("TRUST_REVOKED", batchId);

        _peers[key] = new TrustedPeer
        {
            NodeId = issuer,
            PublicKey = publicKey.ToArray(),
            Mode = TrustMode.Tofu,
            Revoked = false,
            PinnedAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };
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
