// §14.8 Ed25519 batch signatures + §14.9 KEY_ROTATION.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class BatchSignatureTests
{
    static byte[] Id(byte b)
    {
        var a = new byte[16];
        a[15] = b;
        return a;
    }

    [Fact]
    public void KeyPair_sizes()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        Assert.Equal(32, seed.Length);
        Assert.Equal(32, pub.Length);
    }

    [Fact]
    public void Sign_verify_roundtrip()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var msg = System.Text.Encoding.UTF8.GetBytes("hello-binsync");
        var sig = BatchSignature.Sign(seed, msg);
        Assert.Equal(64, sig.Length);
        Assert.True(BatchSignature.Verify(pub, msg, sig));
    }

    [Fact]
    public void Tampered_message_fails()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var msg = new byte[] { 1, 2, 3 };
        var sig = BatchSignature.Sign(seed, msg);
        msg[0] ^= 0xFF;
        Assert.False(BatchSignature.Verify(pub, msg, sig));
    }

    [Fact]
    public void Wrong_key_fails()
    {
        var (seed1, _) = BatchSignature.GenerateKeyPair();
        var (_, pub2) = BatchSignature.GenerateKeyPair();
        var msg = new byte[] { 9 };
        var sig = BatchSignature.Sign(seed1, msg);
        Assert.False(BatchSignature.Verify(pub2, msg, sig));
    }

    [Fact]
    public void SignBatch_uses_canonical_payload()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var batch = new Batch(
            ns: 7, schemaId: 0, schemaVer: 1, policy: 0x02, flags: 0,
            issuer: Id(1), issuerEpoch: 3,
            cstart: 1, cend: 1,
            batchId: Id(0xAA), prev: Node.Zero128,
            root: Node.Zero128, digest: Id(0xDD),
            entries: new List<OutboxEntry>());
        var sig = BatchSignature.SignBatch(seed, batch);
        Assert.True(BatchSignature.VerifyBatch(pub, batch, sig));

        // Mutate digest → must fail
        batch.BatchDigest = Id(0xEE);
        Assert.False(BatchSignature.VerifyBatch(pub, batch, sig));
    }

    [Fact]
    public void KeyRotation_encode_decode_roundtrip()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var notice = KeyRotation.Create(
            namespaceId: 1, oldKeyEpoch: 2, newKeyEpoch: 3,
            nodeId: Id(0x42), identitySeed: seed);
        var bytes = KeyRotation.Encode(notice);
        var decoded = KeyRotation.Decode(bytes);
        Assert.Equal(1, decoded.NamespaceId);
        Assert.Equal(2, decoded.OldKeyEpoch);
        Assert.Equal(3, decoded.NewKeyEpoch);
        Assert.Equal(Id(0x42), decoded.NodeId);
        Assert.True(KeyRotation.VerifyProof(decoded, pub));
    }

    [Fact]
    public void KeyRotation_bad_proof_fails()
    {
        var (seed, pub) = BatchSignature.GenerateKeyPair();
        var notice = KeyRotation.Create(1, 0, 1, Id(1), seed);
        notice.Proof[0] ^= 0xFF;
        Assert.False(KeyRotation.VerifyProof(notice, pub));
    }

    [Fact]
    public void KeyRotation_rejects_non_increasing_epoch()
    {
        Assert.ThrowsAny<ArgumentException>(() =>
            KeyRotation.Encode(new KeyRotationNotice
            {
                NamespaceId = 1,
                OldKeyEpoch = 5,
                NewKeyEpoch = 5,
                NodeId = Id(1),
                Proof = new byte[64],
            }));
    }

    [Fact]
    public void KeyRotation_frame_type_matches_spec()
    {
        Assert.Equal(0x11, KeyRotation.FrameType);
        Assert.Equal("KEY_ROTATION", Frames.FrameTypes[0x11]);
        var (seed, _) = BatchSignature.GenerateKeyPair();
        var notice = KeyRotation.Create(1, 0, 1, Id(1), seed);
        var frame = KeyRotation.BuildFrame(notice, frameSeq: 1);
        var (parsed, _) = FrameCodec.ParseFrame(frame);
        Assert.Equal(KeyRotation.FrameType, parsed.FrameType);
        var again = KeyRotation.Decode(parsed.Payload);
        Assert.Equal(notice.NewKeyEpoch, again.NewKeyEpoch);
    }
}
