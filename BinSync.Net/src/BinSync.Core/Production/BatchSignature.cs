// Batch authenticity §14.8: Ed25519 over canonical batch sign payload.
// FLAG_SIGNED (0x02) on the wire is set by the embedder when attaching a signature.
// Crypto backend: vendored RFC 8032 from ZtSharp Crypto/External (Bouncy Castle MIT subset).
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using BinSync.Crypto.Ed25519;

namespace BinSync;

/// <summary>Ed25519 helpers for signing / verifying batch authenticity material (§14.8).</summary>
public static class BatchSignature
{
    public const string SignDomain = "BIN-SYNC/BATCH-SIGN/v1";
    public const int PublicKeySize = 32;
    public const int SeedSize = 32;
    public const int SignatureSize = 64;

    public static (byte[] Seed, byte[] PublicKey) GenerateKeyPair()
    {
        var seed = new byte[SeedSize];
        RandomNumberGenerator.Fill(seed);
        var pub = new byte[PublicKeySize];
        Ed25519.GeneratePublicKey(seed, 0, pub, 0);
        return (seed, pub);
    }

    public static byte[] BuildSignPayload(
        int namespaceId,
        int issuerEpoch,
        ReadOnlySpan<byte> issuerNodeId,
        ReadOnlySpan<byte> batchId,
        ReadOnlySpan<byte> batchDigest)
    {
        var domain = Encoding.UTF8.GetBytes(SignDomain);
        int len = domain.Length + 4 + 4 + issuerNodeId.Length + batchId.Length + batchDigest.Length;
        var buf = new byte[len];
        int o = 0;
        domain.CopyTo(buf.AsSpan(o)); o += domain.Length;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), namespaceId); o += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), issuerEpoch); o += 4;
        issuerNodeId.CopyTo(buf.AsSpan(o)); o += issuerNodeId.Length;
        batchId.CopyTo(buf.AsSpan(o)); o += batchId.Length;
        batchDigest.CopyTo(buf.AsSpan(o));
        return buf;
    }

    public static byte[] BuildSignPayload(Batch batch) =>
        BuildSignPayload(batch.NamespaceId, batch.IssuerEpoch, batch.IssuerNodeId, batch.BatchId, batch.BatchDigest);

    public static byte[] Sign(ReadOnlySpan<byte> privateSeed, ReadOnlySpan<byte> message)
    {
        if (privateSeed.Length != SeedSize)
            throw new ArgumentException($"Ed25519 seed must be {SeedSize} bytes", nameof(privateSeed));
        var sk = privateSeed.ToArray();
        var pk = new byte[PublicKeySize];
        Ed25519.GeneratePublicKey(sk, 0, pk, 0);
        var msg = message.ToArray();
        var sig = new byte[SignatureSize];
        Ed25519.Sign(sk, 0, pk, 0, msg, 0, msg.Length, sig, 0);
        return sig;
    }

    public static bool Verify(ReadOnlySpan<byte> publicKey, ReadOnlySpan<byte> message, ReadOnlySpan<byte> signature)
    {
        if (publicKey.Length != PublicKeySize || signature.Length != SignatureSize)
            return false;
        try
        {
            var pk = publicKey.ToArray();
            var msg = message.ToArray();
            var sig = signature.ToArray();
            return Ed25519.Verify(sig, 0, pk, 0, msg, 0, msg.Length);
        }
        catch (Exception)
        {
            return false;
        }
    }

    public static byte[] SignBatch(ReadOnlySpan<byte> privateSeed, Batch batch) =>
        Sign(privateSeed, BuildSignPayload(batch));

    public static bool VerifyBatch(ReadOnlySpan<byte> publicKey, Batch batch, ReadOnlySpan<byte> signature) =>
        Verify(publicKey, BuildSignPayload(batch), signature);
}
