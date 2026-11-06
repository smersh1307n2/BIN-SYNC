// §14.9 KEY_ROTATION: KeyEpochStore + TcpSync exchange.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class KeyRotationTests
{
    static NodeId Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return NodeId.From(a);
    }

    [Fact]
    public void KeyEpochStore_applies_verified_rotation()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var nodeId = Id(0x10);
        var store = new KeyEpochStore();
        store.TrustIdentity(nodeId, pub);
        Assert.True(store.TryGetIdentity(nodeId, out var exposedKey));
        exposedKey[0] ^= 0xFF;
        Assert.True(store.TryGetIdentity(nodeId, out var storedKey));
        Assert.Equal(pub, storedKey);

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
    public void KeyRotation_rejects_malformed_proof_lengths_and_trailing_bytes()
    {
        var (seed, _) = BatchSignature.GenerateKeyPair();
        var notice = KeyRotation.Create(1, 0, 1, Id(6), seed);
        var payload = KeyRotation.Encode(notice);
        Assert.Throws<InvalidDataException>(() => KeyRotation.Decode(payload.Append((byte)0).ToArray()));

        var wrongLength = payload.ToArray();
        wrongLength[22] = 63;
        wrongLength[23] = 0;
        Assert.Throws<InvalidDataException>(() => KeyRotation.Decode(wrongLength));

        var invalidNotice = new KeyRotationNotice
        {
            NamespaceId = 1,
            OldKeyEpoch = 0,
            NewKeyEpoch = 1,
            NodeId = Id(6),
            Proof = new byte[63],
        };
        Assert.Throws<ArgumentException>(() => KeyRotation.Encode(invalidNotice));
    }

    [Fact]
    public void Node_ApplyKeyRotation_RaisesDetachedNotice()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var nodeId = Id(7);
        var store = new KeyEpochStore();
        store.TrustIdentity(nodeId, pub);
        var notice = KeyRotation.Create(1, 0, 1, nodeId, seed);
        var originalProof = notice.Proof.ToArray();
        using var node = Fix.MakeNode(114);
        node.KeyRotationApplied += applied => applied.Proof[0] ^= 0xFF;

        Assert.True(node.ApplyKeyRotation(notice, store));
        Assert.Equal(originalProof, notice.Proof);
        Assert.Equal(1, store.GetEpoch(1, nodeId));
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
        await using var client = await TcpSync.ConnectAsync(nodeA, "127.0.0.1", listener.Port);
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
    public async Task Tcp_KEY_ROTATION_wait_preserves_other_frame_types_for_their_receivers()
    {
        var a = Fix.MakeNode(112);
        var b = Fix.MakeNode(113);
        var (seedA, pubA) = BatchSignature.GenerateKeyPair();
        var idA = Id(0xA1);

        var store = new KeyEpochStore();
        store.TrustIdentity(idA, pubA);
        var notice = KeyRotation.Create(1, 0, 1, idA, seedA);

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        // A valid GC_NOTICE must not be consumed by the KEY_ROTATION waiter.
        await client.SendGcNoticeAsync(a, 1);
        var keyRotationTask = server.ReceiveKeyRotationAsync(store);

        await client.SendKeyRotationAsync(notice);
        Assert.True(await keyRotationTask.WaitAsync(TimeSpan.FromSeconds(3)));
        Assert.Equal(1, store.GetEpoch(1, idA));

        // The earlier unrelated frame remains queued and is delivered to its own receiver.
        _ = await server.ReceiveGcNoticeAsync(b).WaitAsync(TimeSpan.FromSeconds(3));
    }
}
