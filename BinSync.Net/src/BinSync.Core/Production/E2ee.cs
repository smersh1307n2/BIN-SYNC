// E2EE / Zero-Knowledge Sync foundation (§14.5).
// Blind merge still uses open HLC + authorNodeId; value is ciphertext on the wire.
// Not auto-wired into Node.WriteField — embedders encrypt before write / decrypt after read.
using System.Security.Cryptography;
using System.Text;

namespace BinSync;

/// <summary>E2EE helpers: deterministic nonce (§14.5) and ChaCha20-Poly1305 seal/open.</summary>
public static class E2ee
{
    public const string NonceDomain = "BIN-SYNC/E2EE/nonce/v1";
    public const int KeySize = 32;
    public const int NonceSize = 12;
    public const int TagSize = 16;

    /// <summary>
    /// nonce = KDF-SHA256(namespaceKey, domain, recordId || fieldId || wall || logical || author || keyEpoch)[0..11]
    /// Invariant [E1]: unique per (record, field, time, author, keyEpoch).
    /// </summary>
    public static byte[] DeriveNonce(
        ReadOnlySpan<byte> namespaceKey,
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ulong hlcWall,
        uint hlcLogical,
        ReadOnlySpan<byte> authorNodeId,
        ushort keyEpoch)
    {
        // Material: domain || recordId || fieldId LE || wall LE || logical LE || author || keyEpoch LE
        var domain = Encoding.UTF8.GetBytes(NonceDomain);
        int len = domain.Length + recordId.Length + 2 + 8 + 4 + authorNodeId.Length + 2;
        Span<byte> material = len <= 512 ? stackalloc byte[len] : new byte[len];
        int o = 0;
        domain.CopyTo(material[o..]); o += domain.Length;
        recordId.CopyTo(material[o..]); o += recordId.Length;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(material[o..], fieldId); o += 2;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(material[o..], hlcWall); o += 8;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(material[o..], hlcLogical); o += 4;
        authorNodeId.CopyTo(material[o..]); o += authorNodeId.Length;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(material[o..], keyEpoch);

        // HMAC-SHA256(namespaceKey, material) → take first 12 bytes as nonce
        var mac = HMACSHA256.HashData(namespaceKey, material);
        var nonce = new byte[NonceSize];
        Array.Copy(mac, 0, nonce, 0, NonceSize);
        return nonce;
    }

    /// <summary>
    /// Seal plaintext → ciphertext || 16-byte Poly1305 tag (ChaCha20-Poly1305).
    /// </summary>
    public static byte[] Encrypt(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> plaintext)
    {
        if (key.Length != KeySize)
            throw new ArgumentException($"E2EE key must be {KeySize} bytes", nameof(key));
        if (nonce.Length != NonceSize)
            throw new ArgumentException($"E2EE nonce must be {NonceSize} bytes", nameof(nonce));

        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[TagSize];
        using var aead = new ChaCha20Poly1305(key);
        aead.Encrypt(nonce, plaintext, ciphertext, tag);

        var result = new byte[ciphertext.Length + TagSize];
        ciphertext.CopyTo(result.AsSpan(0, ciphertext.Length));
        tag.CopyTo(result.AsSpan(ciphertext.Length));
        return result;
    }

    /// <summary>
    /// Open ciphertext||tag → plaintext. Throws <see cref="CryptographicException"/> on auth failure.
    /// </summary>
    public static byte[] Decrypt(ReadOnlySpan<byte> key, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> sealedValue)
    {
        if (key.Length != KeySize)
            throw new ArgumentException($"E2EE key must be {KeySize} bytes", nameof(key));
        if (nonce.Length != NonceSize)
            throw new ArgumentException($"E2EE nonce must be {NonceSize} bytes", nameof(nonce));
        if (sealedValue.Length < TagSize)
            throw new CryptographicException("E2EE sealed value too short");

        int ctLen = sealedValue.Length - TagSize;
        var ciphertext = sealedValue[..ctLen];
        var tag = sealedValue[ctLen..];
        var plaintext = new byte[ctLen];
        using var aead = new ChaCha20Poly1305(key);
        aead.Decrypt(nonce, ciphertext, tag, plaintext);
        return plaintext;
    }

    /// <summary>
    /// Convenience: derive nonce from field context, encrypt plaintext under <paramref name="namespaceKey"/>.
    /// </summary>
    public static byte[] SealField(
        ReadOnlySpan<byte> namespaceKey,
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ulong hlcWall,
        uint hlcLogical,
        ReadOnlySpan<byte> authorNodeId,
        ushort keyEpoch,
        ReadOnlySpan<byte> plaintext)
    {
        var nonce = DeriveNonce(namespaceKey, recordId, fieldId, hlcWall, hlcLogical, authorNodeId, keyEpoch);
        return Encrypt(namespaceKey, nonce, plaintext);
    }

    /// <summary>Inverse of <see cref="SealField"/>.</summary>
    public static byte[] OpenField(
        ReadOnlySpan<byte> namespaceKey,
        ReadOnlySpan<byte> recordId,
        ushort fieldId,
        ulong hlcWall,
        uint hlcLogical,
        ReadOnlySpan<byte> authorNodeId,
        ushort keyEpoch,
        ReadOnlySpan<byte> sealedValue)
    {
        var nonce = DeriveNonce(namespaceKey, recordId, fieldId, hlcWall, hlcLogical, authorNodeId, keyEpoch);
        return Decrypt(namespaceKey, nonce, sealedValue);
    }

    /// <summary>Generate a random 32-byte namespace key.</summary>
    public static byte[] GenerateNamespaceKey()
    {
        var key = new byte[KeySize];
        RandomNumberGenerator.Fill(key);
        return key;
    }
}
