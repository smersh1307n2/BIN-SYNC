// E2EE foundation tests (§14.5, [E1]).
using System.Security.Cryptography;
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class E2eeTests
{
    static byte[] Rid(byte b)
    {
        var a = new byte[16];
        a[0] = b;
        return a;
    }

    static byte[] Author(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Nonce_is_12_bytes_and_deterministic()
    {
        var key = E2ee.GenerateNamespaceKey();
        var n1 = E2ee.DeriveNonce(key, Rid(1), 0x10, 1000, 2, Author(7), keyEpoch: 1);
        var n2 = E2ee.DeriveNonce(key, Rid(1), 0x10, 1000, 2, Author(7), keyEpoch: 1);
        Assert.Equal(12, n1.Length);
        Assert.Equal(n1, n2);
    }

    [Fact]
    public void Nonce_differs_when_any_context_changes_E1()
    {
        var key = E2ee.GenerateNamespaceKey();
        var baseN = E2ee.DeriveNonce(key, Rid(1), 0x10, 1000, 2, Author(7), 1);

        Assert.NotEqual(baseN, E2ee.DeriveNonce(key, Rid(2), 0x10, 1000, 2, Author(7), 1));
        Assert.NotEqual(baseN, E2ee.DeriveNonce(key, Rid(1), 0x11, 1000, 2, Author(7), 1));
        Assert.NotEqual(baseN, E2ee.DeriveNonce(key, Rid(1), 0x10, 1001, 2, Author(7), 1));
        Assert.NotEqual(baseN, E2ee.DeriveNonce(key, Rid(1), 0x10, 1000, 3, Author(7), 1));
        Assert.NotEqual(baseN, E2ee.DeriveNonce(key, Rid(1), 0x10, 1000, 2, Author(8), 1));
        Assert.NotEqual(baseN, E2ee.DeriveNonce(key, Rid(1), 0x10, 1000, 2, Author(7), 2));
    }

    [Fact]
    public void Encrypt_decrypt_roundtrip()
    {
        var key = E2ee.GenerateNamespaceKey();
        var nonce = E2ee.DeriveNonce(key, Rid(1), 1, 50, 0, Author(1), 0);
        var plain = System.Text.Encoding.UTF8.GetBytes("secret-payload");
        var sealed_ = E2ee.Encrypt(key, nonce, plain);
        Assert.True(sealed_.Length >= plain.Length + 16);
        var opened = E2ee.Decrypt(key, nonce, sealed_);
        Assert.Equal(plain, opened);
    }

    [Fact]
    public void SealField_OpenField_roundtrip()
    {
        var key = E2ee.GenerateNamespaceKey();
        var rid = Rid(9);
        var author = Author(3);
        var plain = new byte[] { 1, 2, 3, 4, 5 };
        var sealed_ = E2ee.SealField(key, rid, 0x42, 999, 1, author, keyEpoch: 5, plain);
        var opened = E2ee.OpenField(key, rid, 0x42, 999, 1, author, 5, sealed_);
        Assert.Equal(plain, opened);
    }

    [Fact]
    public void Tampered_ciphertext_fails_auth()
    {
        var key = E2ee.GenerateNamespaceKey();
        var nonce = E2ee.DeriveNonce(key, Rid(1), 1, 1, 0, Author(1), 0);
        var sealed_ = E2ee.Encrypt(key, nonce, new byte[] { 0xAA });
        sealed_[0] ^= 0xFF;
        Assert.ThrowsAny<CryptographicException>(() => E2ee.Decrypt(key, nonce, sealed_));
    }

    [Fact]
    public void Wrong_key_fails_auth()
    {
        var key1 = E2ee.GenerateNamespaceKey();
        var key2 = E2ee.GenerateNamespaceKey();
        var nonce = E2ee.DeriveNonce(key1, Rid(1), 1, 1, 0, Author(1), 0);
        var sealed_ = E2ee.Encrypt(key1, nonce, new byte[] { 0x01, 0x02 });
        Assert.ThrowsAny<CryptographicException>(() => E2ee.Decrypt(key2, nonce, sealed_));
    }

    [Fact]
    public void Blind_merge_sees_only_ciphertext_bytes()
    {
        // Server-side LWW compares ciphertext as opaque bytes; same HLC rules apply.
        var key = E2ee.GenerateNamespaceKey();
        var a = Author(1);
        var b = Author(2);
        var rid = Rid(1);
        var c1 = E2ee.SealField(key, rid, 1, hlcWall: 10, hlcLogical: 0, a, 0, new byte[] { 1 });
        var c2 = E2ee.SealField(key, rid, 1, hlcWall: 20, hlcLogical: 0, b, 0, new byte[] { 2 });

        var fs1 = new FieldState(c1, new Hlc(10, 0), a);
        var fs2 = new FieldState(c2, new Hlc(20, 0), b);
        var (winner, changed) = Merge.MergeFieldWithPolicy(fs1, fs2, MergePolicy.LwwField);
        Assert.True(changed);
        Assert.Equal(c2, winner!.Value); // higher HLC wins; value remains ciphertext
    }
}
