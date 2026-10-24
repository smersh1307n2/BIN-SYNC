// Batch authenticity §14.8: Ed25519 over canonical batch sign payload.
// Uses vendored RFC 8032 implementation (ZtSharp/Bouncy Castle MIT subset) — no NuGet.
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

    /// <summary>Generate a fresh Ed25519 key pair (32-byte seed + 32-byte public key).</summary>
    public static (byte[] Seed, byte[] PublicKey) GenerateKeyPair()
    {
        var seed = new byte[SeedSize];
        RandomNumberGenerator.Fill(seed);
        var pub = new byte[PublicKeySize];
        Ed25519.GeneratePublicKey(seed, 0, pub, 0);
        return (seed, pub);
    }

    /// <summary>
    /// Canonical message for batch authenticity:
    /// domain || namespaceId LE32 || issuerEpoch LE32 || issuerNodeId || batchId || batchDigest.
    /// </summary>
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

    /// <summary>Sign message with Ed25519 private seed (32 bytes). Returns 64-byte signature.</summary>
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

    /// <summary>Verify Ed25519 signature. Returns false on length mismatch or invalid signature.</summary>
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
