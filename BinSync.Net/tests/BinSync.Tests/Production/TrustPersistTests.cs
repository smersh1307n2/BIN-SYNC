// Durable trust pins + OUTDATED_WATERMARK gate.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class TrustPersistTests
{
    static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void Trust_file_roundtrip_keeps_tofu_pin_and_revoke()
    {
        var store = new TrustStore { DefaultMode = TrustMode.Tofu };
        var (_, pub) = BatchSignature.GenerateKeyPair();
        store.Allow(Id(7), pub, TrustMode.Tofu);
        store.Revoke(Id(7));
        store.AddCaRoot(Id(0xCA));

        var path = Path.Combine(Path.GetTempPath(), $"binsync-trust-{Guid.NewGuid():N}.bin");
        try
        {
            TrustPersistence.Save(store, path);
            var back = TrustPersistence.Load(path);
            Assert.Equal(TrustMode.Tofu, back.DefaultMode);
            Assert.True(back.IsCaRoot(Id(0xCA)));
            Assert.Null(back.ResolvePublicKey(Id(7), pub));
            Assert.True(back.TryGet(Id(7), out var peer));
            Assert.True(peer!.Revoked);
            Assert.Equal(pub, peer.PublicKey);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Node_SaveTrust_LoadTrust()
    {
        var n = Fix.MakeNode(910);
        var (_, pub) = BatchSignature.GenerateKeyPair();
        n.TrustPeer(Id(3), pub, TrustMode.PreShared);
        var path = Path.Combine(Path.GetTempPath(), $"binsync-trust-{Guid.NewGuid():N}.bin");
        try
        {
            n.SaveTrust(path);
            var n2 = Fix.MakeNode(911);
            n2.LoadTrust(path);
            Assert.Equal(pub, n2.Trust!.ResolvePublicKey(Id(3)));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Trust_file_rejects_trailing_bytes_and_impossible_entry_counts()
    {
        var valid = TrustPersistence.Serialize(new TrustStore());
        Assert.Throws<InvalidDataException>(() =>
            TrustPersistence.Deserialize(valid.Concat(new byte[] { 0 }).ToArray()));

        var impossibleCount = new Writer()
            .U32(TrustPersistence.Magic)
            .U16(TrustPersistence.Version)
            .U8((byte)TrustMode.Tofu)
            .Varint(1)
            .Value();
        Assert.Throws<InvalidDataException>(() =>
            TrustPersistence.Deserialize(impossibleCount));
    }

    [Fact]
    public void Trust_file_rejects_invalid_modes_and_flags()
    {
        var invalidDefaultMode = new Writer()
            .U32(TrustPersistence.Magic)
            .U16(TrustPersistence.Version)
            .U8(0xFF)
            .Varint(0)
            .Varint(0)
            .Value();
        Assert.Throws<InvalidDataException>(() =>
            TrustPersistence.Deserialize(invalidDefaultMode));

        var store = new TrustStore();
        var (_, publicKey) = BatchSignature.GenerateKeyPair();
        store.Allow(NodeId.From(Id(8)), publicKey);
        var invalidPeerMode = TrustPersistence.Serialize(store);
        invalidPeerMode[56] = 0xFF;
        Assert.Throws<InvalidDataException>(() =>
            TrustPersistence.Deserialize(invalidPeerMode));

        var invalidRevokedFlag = TrustPersistence.Serialize(store);
        invalidRevokedFlag[57] = 2;
        Assert.Throws<InvalidDataException>(() =>
            TrustPersistence.Deserialize(invalidRevokedFlag));
    }

    [Fact]
    public void OutdatedWatermark_throws()
    {
        var ex = Assert.Throws<SyncException>(() => WatermarkGate.RequireCurrent(10, 100));
        Assert.Equal("OUTDATED_WATERMARK", ex.CodeName);
        Assert.Equal(0x000B, ex.Code);
        WatermarkGate.RequireCurrent(100, 100);
        WatermarkGate.RequireCurrent(0, 0);
    }
}
