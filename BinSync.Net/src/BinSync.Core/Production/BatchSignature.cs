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
    /// <summary>Constant sign domain.</summary>
    public const string SignDomain = "BIN-SYNC/BATCH-SIGN/v1";
    /// <summary>Constant public key size.</summary>
    public const int PublicKeySize = 32;
    /// <summary>Constant seed size.</summary>
    public const int SeedSize = 32;
    /// <summary>Constant signature size.</summary>
    public const int SignatureSize = 64;

    /// <summary>Generate a new Ed25519 key pair (seed + public key).</summary>
    public static (byte[] Seed, byte[] PublicKey) GenerateKeyPair()
    {
        var seed = new byte[SeedSize];
        RandomNumberGenerator.Fill(seed);
        var pub = new byte[PublicKeySize];
        Ed25519.GeneratePublicKey(seed, 0, pub, 0);
        return (seed, pub);
    }

    /// <summary>Build the canonical sign payload from typed batch identity values.</summary>
    public static byte[] BuildSignPayload(
        int namespaceId,
        int issuerEpoch,
        NodeId issuerNodeId,
        BatchId batchId,
        Digest16 batchDigest)
    {
        var domain = Encoding.UTF8.GetBytes(SignDomain);
        var buf = new byte[domain.Length + 8 + 16 + 16 + 16];
        int o = 0;
        domain.CopyTo(buf.AsSpan(o)); o += domain.Length;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), namespaceId); o += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(o), issuerEpoch); o += 4;
        issuerNodeId.WriteTo(buf.AsSpan(o, 16)); o += 16;
        batchId.WriteTo(buf.AsSpan(o, 16)); o += 16;
        batchDigest.WriteTo(buf.AsSpan(o, 16));
        return buf;
    }

    /// <summary>Build sign payload from a batch's typed identity values.</summary>
    public static byte[] BuildSignPayload(Batch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        return BuildSignPayload(batch.NamespaceId, batch.IssuerEpoch,
            batch.IssuerNodeId, batch.BatchId, batch.BatchDigest);
    }

    /// <summary>Sign a message with an Ed25519 private seed.</summary>
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

    /// <summary>Verify  and throw on failure when required.</summary>
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

    /// <summary>Sign the canonical batch digest with an Ed25519 private seed.</summary>
    public static byte[] SignBatch(ReadOnlySpan<byte> privateSeed, Batch batch) =>
        Sign(privateSeed, BuildSignPayload(batch));

    /// <summary>Verify batch and throw on failure when required.</summary>
    public static bool VerifyBatch(ReadOnlySpan<byte> publicKey, Batch batch, ReadOnlySpan<byte> signature) =>
        Verify(publicKey, BuildSignPayload(batch), signature);
}
