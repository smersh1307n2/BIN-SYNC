// §14.9 KEY_ROTATION: KeyEpochStore + TcpSync exchange.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class KeyRotationTests
{
    static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void KeyEpochStore_applies_verified_rotation()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var nodeId = Id(0x10);
        var store = new KeyEpochStore();
        store.TrustIdentity(nodeId, pub);

        Assert.Equal(0, store.GetEpoch(1, nodeId));

        var n1 = KeyRotation.Create(1, 0, 1, nodeId, seed);
        Assert.True(store.TryApply(n1));
        Assert.Equal(1, store.GetEpoch(1, nodeId));

        var n2 = KeyRotation.Create(1, 1, 2, nodeId, seed);
        Assert.True(store.TryApply(n2));
        Assert.Equal(2, store.GetEpoch(1, nodeId));
    }

    [Fact]
    public void KeyEpochStore_rejects_untrusted_publisher()
    {
        var (seed, _) = BatchSignature.GenerateKeyPair();
        var store = new KeyEpochStore();
        // no TrustIdentity
        var notice = KeyRotation.Create(1, 0, 1, Id(1), seed);
        Assert.False(store.TryApply(notice));
    }

    [Fact]
    public void KeyEpochStore_rejects_bad_proof()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var nodeId = Id(2);
        var store = new KeyEpochStore();
        store.TrustIdentity(nodeId, pub);
        var notice = KeyRotation.Create(1, 0, 1, nodeId, seed);
        notice.Proof[3] ^= 0xAA;
        Assert.False(store.TryApply(notice));
        Assert.Equal(0, store.GetEpoch(1, nodeId));
    }

    [Fact]
    public void KeyEpochStore_rejects_epoch_skip_mismatch()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var nodeId = Id(3);
        var store = new KeyEpochStore();
        store.TrustIdentity(nodeId, pub);
        Assert.True(store.TryApply(KeyRotation.Create(1, 0, 1, nodeId, seed)));
        // old=0 but current is already 1 → reject
        Assert.False(store.TryApply(KeyRotation.Create(1, 0, 2, nodeId, seed)));
        Assert.Equal(1, store.GetEpoch(1, nodeId));
    }

    [Fact]
    public void KeyEpochStore_rejects_stale_or_equal_new()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var nodeId = Id(4);
        var store = new KeyEpochStore();
        store.TrustIdentity(nodeId, pub);
        store.SeedEpoch(1, nodeId, 5);
        Assert.False(store.TryApply(KeyRotation.Create(1, 5, 5, nodeId, seed)));
        Assert.False(store.TryApply(KeyRotation.Create(1, 5, 4, nodeId, seed)));
        Assert.Equal(5, store.GetEpoch(1, nodeId));
    }

    [Fact]
    public async Task Tcp_ExchangeKeyRotation_advances_peer_epoch()
    {
        var (seedA, pubA) = BatchSignature.GenerateKeyPair();
        var (seedB, pubB) = BatchSignature.GenerateKeyPair();
        var idA = Id(0xA1);
        var idB = Id(0xB2);

        var storeA = new KeyEpochStore();
        var storeB = new KeyEpochStore();
        // Each side trusts the other's identity
        storeA.TrustIdentity(idB, pubB);
        storeB.TrustIdentity(idA, pubA);

        var noticeFromA = KeyRotation.Create(1, 0, 1, idA, seedA);
        var noticeFromB = KeyRotation.Create(1, 0, 1, idB, seedB);

        var nodeA = Fix.MakeNode(110);
        var nodeB = Fix.MakeNode(111);

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(nodeB);
        await using var client = await TcpSyncSession.ConnectAsync(nodeA, "127.0.0.1", listener.Port);
        await using var server = await accept;

        // Initiator (A) sends its rotation first; acceptor (B) replies with its own.
        // Custom two-step: A send → B recv+apply → B send → A recv+apply
        var serverTask = Task.Run(async () =>
        {
            bool applied = await server.ReceiveKeyRotationAsync(storeB);
            await server.SendKeyRotationAsync(noticeFromB);
            return applied;
        });
        await client.SendKeyRotationAsync(noticeFromA);
        bool appliedOnA = await client.ReceiveKeyRotationAsync(storeA);
        bool appliedOnB = await serverTask;

        Assert.True(appliedOnB); // B applied A's rotation
        Assert.True(appliedOnA); // A applied B's rotation
        Assert.Equal(1, storeB.GetEpoch(1, idA));
        Assert.Equal(1, storeA.GetEpoch(1, idB));
    }

    [Fact]
    public async Task Tcp_KEY_ROTATION_frame_rejected_when_type_wrong()
    {
        var a = Fix.MakeNode(112);
        var b = Fix.MakeNode(113);
        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        // Send a PING instead of KEY_ROTATION
        var ping = FrameCodec.BuildFrame(0x0F, 0, 1, Array.Empty<byte>());
        // Use reflection-free path: send via public Push won't work; use Receive expecting KEY_ROTATION
        // Push raw through a second connection pattern — send GC then expect KEY_ROTATION fail on peer
        await client.SendGcNoticeAsync(a, 1);
        var store = new KeyEpochStore();
        await Assert.ThrowsAsync<FrameException>(async () =>
            await server.ReceiveKeyRotationAsync(store));
    }
}
