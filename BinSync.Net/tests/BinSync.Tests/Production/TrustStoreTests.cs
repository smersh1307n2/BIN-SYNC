// §15.6 TOFU / PreShared / revoke + signed batch gate.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TrustStoreTests
{
    static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    static Batch SampleBatch(byte issuerTail = 1) =>
        new(7, 0, 1, 0x02, 0, Id(issuerTail), 1, 1, 1,
            Id(0xAA), Node.Zero128, Node.Zero128, Id(0xDD),
            new List<OutboxEntry>());

    [Fact]
    public void Tofu_pins_first_key_only_after_valid_signature()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var node = NodeId.From(Id(1));
        var batch = SampleBatch(1);

        Assert.Equal(pub, store.ResolvePublicKey(node, pub));
        Assert.Equal(0, store.PeerCount); // lookup alone must not establish trust
        store.VerifyBatchOrThrow(batch, BatchSignature.SignBatch(seed, batch), pub);

        Assert.Equal(1, store.PeerCount);
        Assert.True(store.TryGet(node, out var pinned));
        Assert.Equal(pub, pinned!.PublicKey);
        Assert.Equal(pub, store.ResolvePublicKey(node, pub));
    }

    [Fact]
    public void Tofu_rejects_key_substitution_after_verified_first_use()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (seed1, pub1) = BatchSignature.GenerateKeyPair();
        var (seed2, pub2) = BatchSignature.GenerateKeyPair();
        var node = NodeId.From(Id(2));
        var batch = SampleBatch(2);
        store.VerifyBatchOrThrow(batch, BatchSignature.SignBatch(seed1, batch), pub1);

        Assert.Null(store.ResolvePublicKey(node, pub2));
        Assert.Throws<SyncException>(() =>
            store.VerifyBatchOrThrow(batch, BatchSignature.SignBatch(seed2, batch), pub2));
        Assert.Equal(pub1, store.ResolvePublicKey(node, pub1));
    }

    [Fact]
    public void Trust_store_rejects_zero_identities_null_keys_and_unknown_modes()
    {
        var store = new TrustStore();
        var (_, publicKey) = BatchSignature.GenerateKeyPair();
        var peer = NodeId.From(Id(8));

        Assert.Throws<ArgumentOutOfRangeException>(() => store.DefaultMode = (TrustMode)0xFF);
        Assert.Throws<ArgumentException>(() => store.Allow(default, publicKey));
        Assert.Throws<ArgumentNullException>(() => store.Allow(peer, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => store.Allow(peer, publicKey, (TrustMode)0xFF));
        Assert.Throws<ArgumentException>(() => store.AddCaRoot(default));
        Assert.Null(store.ResolvePublicKey(default, publicKey));
        Assert.Equal(0, store.PeerCount);
    }

    [Fact]
    public void Tofu_invalid_signature_does_not_poison_peer_identity()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (seed, attackerKey) = BatchSignature.GenerateKeyPair();
        var batch = SampleBatch(6);
        var invalid = BatchSignature.SignBatch(seed, batch);
        invalid[0] ^= 0x80;

        Assert.Throws<SyncException>(() => store.VerifyBatchOrThrow(batch, invalid, attackerKey));
        Assert.Equal(0, store.PeerCount);
        Assert.False(store.TryGet(batch.IssuerNodeId, out _));
    }

    [Fact]
    public void PreShared_requires_explicit_allow()
    {
        var store = new TrustStore { DefaultMode = TrustMode.PreShared };
        var (_, pub) = BatchSignature.GenerateKeyPair();
        Assert.Null(store.ResolvePublicKey(Id(3), pub));
        store.Allow(Id(3), pub, TrustMode.PreShared);
        Assert.Equal(pub, store.ResolvePublicKey(Id(3), pub));
    }

    [Fact]
    public void Trust_change_events_run_outside_node_locks()
    {
        using var node = Fix.MakeNode(915);
        var peer = NodeId.From(Id(0x15));
        var (_, publicKey) = BatchSignature.GenerateKeyPair();
        int trusted = 0, revoked = 0, probeRecord = 915;

        node.TrustChanged += (changedPeer, isTrusted) =>
        {
            Assert.Equal(peer, changedPeer);
            var probeId = Fix.Rid(Interlocked.Increment(ref probeRecord));
            var write = Task.Run(() => node.WriteField(7, probeId, 0x101, new byte[] { 1 }));
            Assert.True(write.Wait(TimeSpan.FromSeconds(3)), "Trust event ran while node locks were held.");
            Assert.True(write.Result);
            if (isTrusted) trusted++;
            else revoked++;
        };

        node.TrustPeer(peer, publicKey, TrustMode.PreShared);
        node.RevokeTrust(peer);
        Assert.Equal(1, trusted);
        Assert.Equal(1, revoked);
    }

    [Fact]
    public void Revoke_blocks_verify()
    {
        var store = new TrustStore { DefaultMode = TrustMode.PreShared };
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        store.Allow(Id(4), pub);
        var batch = SampleBatch(4);
        // Fix issuer to match allowed node
        batch = new Batch(7, 0, 1, 0x02, 0, Id(4), 1, 1, 1,
            Id(0xAA), Node.Zero128, Node.Zero128, Id(0xDD),
            new List<OutboxEntry>());
        var sig = BatchSignature.SignBatch(seed, batch);
        Assert.True(store.TryVerifyBatch(batch, sig, pub));

        store.Revoke(Id(4));
        var ex = Assert.Throws<SyncException>(() => store.VerifyBatchOrThrow(batch, sig, pub));
        Assert.Equal("TRUST_REVOKED", ex.CodeName);
    }

    [Fact]
    public void Bad_signature_is_SIGNATURE_INVALID()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var batch = new Batch(7, 0, 1, 0x02, 0, Id(5), 1, 1, 1,
            Id(0xAA), Node.Zero128, Node.Zero128, Id(0xDD),
            new List<OutboxEntry>());
        var sig = BatchSignature.SignBatch(seed, batch);
        sig[0] ^= 0xFF;
        var ex = Assert.Throws<SyncException>(() => store.VerifyBatchOrThrow(batch, sig, pub));
        Assert.Equal("SIGNATURE_INVALID", ex.CodeName);
    }

    [Fact]
    public void Trust_revocation_waits_until_verified_batch_commit_finishes()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (seed, publicKey) = BatchSignature.GenerateKeyPair();
        var batch = SampleBatch(7);
        var signature = BatchSignature.SignBatch(seed, batch);
        using var verifying = new ManualResetEventSlim();
        using var releaseCommit = new ManualResetEventSlim();
        using var revokeStarted = new ManualResetEventSlim();
        using var revokeCompleted = new ManualResetEventSlim();
        Exception? verifyError = null;
        var verifyThread = new Thread(() =>
        {
            try
            {
                store.VerifyBatchAndExecute(batch, signature, publicKey, () =>
                {
                    verifying.Set();
                    releaseCommit.Wait();
                    return true;
                });
            }
            catch (Exception ex) { verifyError = ex; }
        }) { IsBackground = true };
        var revokeThread = new Thread(() =>
        {
            revokeStarted.Set();
            store.Revoke(batch.IssuerNodeId);
            revokeCompleted.Set();
        }) { IsBackground = true };

        verifyThread.Start();
        try
        {
            Assert.True(verifying.Wait(TimeSpan.FromSeconds(3)));
            revokeThread.Start();
            Assert.True(revokeStarted.Wait(TimeSpan.FromSeconds(3)));
            Assert.False(revokeCompleted.Wait(TimeSpan.FromMilliseconds(100)));
        }
        finally
        {
            releaseCommit.Set();
        }

        Assert.True(verifyThread.Join(TimeSpan.FromSeconds(3)));
        Assert.True(revokeThread.Join(TimeSpan.FromSeconds(3)));
        Assert.Null(verifyError);
        Assert.Null(store.ResolvePublicKey(batch.IssuerNodeId, publicKey));
    }

    [Fact]
    public void Node_TrustPeer_and_VerifyAndApply_empty_batch()
    {
        var n = Fix.MakeNode(901);
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var issuer = Id(9);
        n.TrustPeer(issuer, pub, TrustMode.PreShared);

        var batch = new Batch(1, 0, 1, 0x02, 0, issuer, 1, 1, 1,
            Fix.Nid(0xAB), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry>());
        var sig = BatchSignature.SignBatch(seed, batch);

        // Empty batch may still hit engine seq paths — only assert trust does not throw TRUST_*
        try
        {
            n.VerifyAndApplyBatch(batch, issuer, sig, pub);
        }
        catch (SyncException ex)
        {
            Assert.DoesNotContain("TRUST", ex.CodeName);
            Assert.DoesNotContain("SIGNATURE", ex.CodeName);
        }
    }

    [Fact]
    public void Error_codes_registered()
    {
        Assert.Equal(0x0013, Frames.ErrorCodes["SIGNATURE_INVALID"]);
        Assert.Equal(0x0014, Frames.ErrorCodes["TRUST_REVOKED"]);
    }

    [Fact]
    public void CaRoot_tracking()
    {
        var store = new TrustStore { DefaultMode = TrustMode.CertificateAuthority };
        var ca = Id(0xCA);
        store.AddCaRoot(ca);
        Assert.True(store.IsCaRoot(ca));
        Assert.False(store.IsCaRoot(Id(1)));
    }
}
