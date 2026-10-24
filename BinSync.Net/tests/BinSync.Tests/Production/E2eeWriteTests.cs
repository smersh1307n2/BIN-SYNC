// §14.5 opt-in WriteFieldSealed / ReadFieldOpen.
using System.Security.Cryptography;
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class E2eeWriteTests
{
    static byte[] Rid(byte b)
    {
        var a = new byte[16];
        a[0] = b;
        return a;
    }

    [Fact]
    public void WriteFieldSealed_stores_ciphertext_not_plaintext()
    {
        var n = Fix.MakeNode(200);
        var key = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key, keyEpoch: 0);

        var rid = Rid(1);
        var plain = System.Text.Encoding.UTF8.GetBytes("top-secret");
        Assert.True(n.WriteFieldSealed(1, rid, 0x100, plain));

        var fs = n.Storage[1][rid].State[0x100];
        Assert.NotEqual(plain, fs.Value);
        Assert.True(fs.Value.Length >= plain.Length + E2ee.TagSize);
    }

    [Fact]
    public void ReadFieldOpen_roundtrip()
    {
        var n = Fix.MakeNode(201);
        var key = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key);

        var rid = Rid(2);
        var plain = new byte[] { 9, 8, 7, 6 };
        n.WriteFieldSealed(1, rid, 0x200, plain);
        var opened = n.ReadFieldOpen(1, rid, 0x200);
        Assert.Equal(plain, opened);
    }

    [Fact]
    public void WriteFieldSealed_without_key_throws()
    {
        var n = Fix.MakeNode(202);
        n.E2eeKeys = new E2eeKeyRing();
        Assert.Throws<InvalidOperationException>(() =>
            n.WriteFieldSealed(1, Rid(1), 1, new byte[] { 1 }));
    }

    [Fact]
    public void WriteFieldSealed_without_ring_throws()
    {
        var n = Fix.MakeNode(203);
        Assert.Null(n.E2eeKeys);
        Assert.Throws<InvalidOperationException>(() =>
            n.WriteFieldSealed(1, Rid(1), 1, new byte[] { 1 }));
    }

    [Fact]
    public void Plain_WriteField_unaffected_when_ring_present()
    {
        var n = Fix.MakeNode(204);
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey());
        var rid = Rid(3);
        var plain = new byte[] { 0xDE, 0xAD };
        Assert.True(n.WriteField(1, rid, 0x300, plain));
        Assert.Equal(plain, n.Storage[1][rid].State[0x300].Value);
    }

    [Fact]
    public void KeyEpoch_recorded_on_FieldState()
    {
        var n = Fix.MakeNode(205);
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey(), keyEpoch: 3);
        n.WriteFieldSealed(1, Rid(4), 0x400, new byte[] { 1 });
        Assert.Equal(3, n.Storage[1][Rid(4)].State[0x400].KeyEpoch);
    }

    [Fact]
    public void AdvanceEpoch_changes_nonce_context()
    {
        var n = Fix.MakeNode(206);
        var key = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key, keyEpoch: 0);
        var rid = Rid(5);
        n.WriteFieldSealed(1, rid, 0x500, new byte[] { 1, 2 });
        var ct0 = n.Storage[1][rid].State[0x500].Value.ToArray();

        n.E2eeKeys.AdvanceEpoch(1, 1);
        var h = new Hlc(n.Clock.SendTick().Wall + 1000, 0);
        n.WriteFieldSealed(1, rid, 0x501, new byte[] { 1, 2 }, hlc: h);
        var ct1 = n.Storage[1][rid].State[0x501].Value;
        Assert.NotEqual(ct0, ct1);
        Assert.Equal(1, n.Storage[1][rid].State[0x501].KeyEpoch);
    }

    [Fact]
    public void Wrong_key_cannot_open()
    {
        var n = Fix.MakeNode(207);
        var key1 = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key1);
        n.WriteFieldSealed(1, Rid(6), 0x600, new byte[] { 0xAA });

        n.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey());
        Assert.ThrowsAny<CryptographicException>(() =>
            n.ReadFieldOpen(1, Rid(6), 0x600));
    }

    [Fact]
    public void Shared_key_opens_peer_ciphertext()
    {
        var key = E2ee.GenerateNamespaceKey();
        var a = Fix.MakeNode(208);
        a.E2eeKeys = new E2eeKeyRing();
        a.E2eeKeys.Set(1, key);

        var rid = Rid(7);
        var plain = System.Text.Encoding.UTF8.GetBytes("shared-secret");
        a.WriteFieldSealed(1, rid, 0x700, plain);
        var fs = a.Storage[1][rid].State[0x700];

        var opened = E2ee.OpenField(
            key, rid, 0x700, fs.Timestamp.Wall, fs.Timestamp.Logical,
            fs.AuthorNodeId, fs.KeyEpoch, fs.Value);
        Assert.Equal(plain, opened);
    }
}
