// Opt-in trust surface on Node (§15.6).
namespace BinSync;

public sealed partial class Node
{
    /// <summary>
    /// When set, callers may use <see cref="VerifyAndApplyBatch"/> before apply.
    /// Plain <see cref="ApplyBatch"/> does not consult trust (backward compatible).
    /// </summary>
    public TrustStore? Trust { get; set; }

    /// <summary>
    /// Ensure <see cref="Trust"/> exists (TOFU by default) and return it.
    /// </summary>
    public TrustStore EnsureTrust(TrustMode mode = TrustMode.Tofu)
    {
        lock (_gate)
        {
            Trust ??= new TrustStore { DefaultMode = mode };
            return Trust;
        }
    }

    /// <summary>
    /// Verify signature then apply. Requires <see cref="Trust"/>.
    /// </summary>
    public BatchResult VerifyAndApplyBatch(
        Batch batch, byte[] senderPeer, ReadOnlySpan<byte> signature, byte[]? candidatePublicKey = null)
    {
        lock (_gate)
        {
            var store = Trust ?? throw new InvalidOperationException("Trust store not configured");
            store.VerifyBatchOrThrow(batch, signature, candidatePublicKey);
            return ApplyBatchInternal(batch, senderPeer);
        }
    }

    /// <summary>Pin or pre-share a peer key.</summary>
    public void TrustPeer(byte[] nodeId, byte[] publicKey, TrustMode mode = TrustMode.PreShared)
    {
        lock (_gate)
        {
            EnsureTrust(mode == TrustMode.Tofu ? TrustMode.Tofu : mode)
                .Allow(nodeId, publicKey, mode);
        }
    }

    public void RevokeTrust(byte[] nodeId)
    {
        lock (_gate) Trust?.Revoke(nodeId);
    }

    /// <summary>Persist <see cref="Trust"/> (creating a TOFU store if missing).</summary>
    public void SaveTrust(string path)
    {
        lock (_gate)
            TrustPersistence.Save(EnsureTrust(), path);
    }

    /// <summary>Replace <see cref="Trust"/> with a file previously written by <see cref="SaveTrust"/>.</summary>
    public TrustStore LoadTrust(string path)
    {
        var loaded = TrustPersistence.Load(path);
        lock (_gate)
        {
            Trust = loaded;
            return Trust;
        }
    }
}
