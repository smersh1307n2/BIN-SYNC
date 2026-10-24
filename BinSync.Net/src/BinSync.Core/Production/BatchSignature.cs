// Batch authenticity §14.8: Ed25519 over canonical batch sign payload.
// FLAG_SIGNED (0x02) on the wire is set by the embedder when attaching a signature.
// Crypto backend: BouncyCastle.Cryptography (temporary). Same API as ZtSharp's
// Crypto/External/Ed25519 (MIT BC subset) — swap planned without changing this surface.
using System.Buffers.Binary;
using System.Text;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Signers;
using Org.BouncyCastle.Security;

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
        var gen = new Ed25519KeyPairGenerator();
        gen.Init(new Ed25519KeyGenerationParameters(new SecureRandom()));
        var kp = gen.GenerateKeyPair();
        var priv = (Ed25519PrivateKeyParameters)kp.Private;
        var pub = (Ed25519PublicKeyParameters)kp.Public;
        return (priv.GetEncoded(), pub.GetEncoded());
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
        var priv = new Ed25519PrivateKeyParameters(privateSeed.ToArray(), 0);
        var signer = new Ed25519Signer();
        signer.Init(true, priv);
        signer.BlockUpdate(message.ToArray(), 0, message.Length);
        return signer.GenerateSignature();
    }

    public static bool Verify(ReadOnlySpan<byte> publicKey, ReadOnlySpan<byte> message, ReadOnlySpan<byte> signature)
    {
        if (publicKey.Length != PublicKeySize || signature.Length != SignatureSize)
            return false;
        try
        {
            var pub = new Ed25519PublicKeyParameters(publicKey.ToArray(), 0);
            var verifier = new Ed25519Signer();
            verifier.Init(false, pub);
            verifier.BlockUpdate(message.ToArray(), 0, message.Length);
            return verifier.VerifySignature(signature.ToArray());
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
