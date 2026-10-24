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
    public void Tofu_pins_first_key()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var node = Id(1);
        var resolved = store.ResolvePublicKey(node, pub);
        Assert.NotNull(resolved);
        Assert.Equal(pub, resolved);
        Assert.Equal(1, store.PeerCount);

        // Same key again OK
        Assert.Equal(pub, store.ResolvePublicKey(node, pub));
    }

    [Fact]
    public void Tofu_rejects_key_substitution()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (_, pub1) = BatchSignature.GenerateKeyPair();
        var (_, pub2) = BatchSignature.GenerateKeyPair();
        var node = Id(2);
        Assert.NotNull(store.ResolvePublicKey(node, pub1));
        Assert.Null(store.ResolvePublicKey(node, pub2)); // pin mismatch
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
