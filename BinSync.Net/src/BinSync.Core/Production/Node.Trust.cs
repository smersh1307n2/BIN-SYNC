// Opt-in trust surface on Node (§15.6).
namespace BinSync;

/// <summary>Sync node: storage, outbox, alias maps, create/apply delta batches. Identity is fixed at construction.</summary>
public sealed partial class Node
{
    /// <summary>
    /// When set, callers may use <see cref="VerifyAndApplyBatch(Batch, NodeId, ReadOnlySpan{byte}, byte[])"/> before apply.
    /// Plain <see cref="ApplyBatch"/> does not consult trust (backward compatible).
    /// </summary>
    private TrustStore? _trust;

    /// <summary>Optional thread-safe trust store used by signature-gated operations.</summary>
    public TrustStore? Trust
    {
        get { lock (_lockMeta) return _trust; }
        set { lock (_lockMeta) _trust = value; }
    }

    /// <summary>
    /// Ensure <see cref="Trust"/> exists (TOFU by default) and return it.
    /// </summary>
    public TrustStore EnsureTrust(TrustMode mode = TrustMode.Tofu)
    {
        lock (_lockMeta)
        {
            Trust ??= new TrustStore { DefaultMode = mode };
            return Trust;
        }
    }

    /// <summary>
    /// Verify signature then apply. Requires <see cref="Trust"/>.
    /// </summary>
    public BatchResult VerifyAndApplyBatch(
        Batch batch, NodeId senderPeer, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey = null)
    {
        ArgumentNullException.ThrowIfNull(batch);
        var ownedBatch = CloneBatchForPublicRead(batch);
        var ownedSignature = signature.ToArray();
        var ownedCandidateKey = candidatePublicKey?.ToArray();
        BatchResult result;
        lock (_lockStorage) lock (_lockOutbox) lock (_lockMeta)
        {
            var store = Trust ?? throw new InvalidOperationException("Trust store not configured");
            // Keep Node and trust gates through publication: trust replacement, peer
            // revocation, and the accepted batch have one linearized order.
            result = store.VerifyBatchAndExecute(
                ownedBatch,
                ownedSignature,
                ownedCandidateKey,
                () => ApplyBatchInternal(ownedBatch, senderPeer));
        }
        if (!result.Duplicate || result.Applied > 0)
            RaiseBatchApplied(result, senderPeer);
        return result;
    }

    /// <summary>Pin or pre-share a peer key.</summary>
    public void TrustPeer(NodeId nodeId, byte[] publicKey, TrustMode mode = TrustMode.PreShared)
    {
        lock (_lockMeta)
        {
            EnsureTrust(mode == TrustMode.Tofu ? TrustMode.Tofu : mode)
                .Allow(nodeId, publicKey, mode);
        }
    }

    /// <summary>Revoke a previously trusted peer.</summary>
    public void RevokeTrust(NodeId nodeId)
    {
        lock (_lockMeta) Trust?.Revoke(nodeId);
    }

    /// <summary>Persist <see cref="Trust"/> (creating a TOFU store if missing).</summary>
    public void SaveTrust(string path)
    {
        lock (_lockMeta)
            TrustPersistence.Save(EnsureTrust(), path);
    }

    /// <summary>Replace <see cref="Trust"/> with a file previously written by <see cref="SaveTrust"/>.</summary>
    public TrustStore LoadTrust(string path)
    {
        var loaded = TrustPersistence.Load(path);
        lock (_lockMeta)
        {
            Trust = loaded;
            return Trust;
        }
    }
}
