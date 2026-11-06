// KEY_ROTATION frame payload (§14.9, type 0x11).
// Spec is brief: "frame with epoch proof". We encode a minimal, self-describing payload;
// proof is an Ed25519 signature over the rotation message under the node's identity key.
using System.Buffers.Binary;
using System.Text;

namespace BinSync;

/// <summary>Decoded KEY_ROTATION payload.</summary>
public sealed class KeyRotationNotice
{
    /// <summary>Logical namespace this entity belongs to.</summary>
    public ushort NamespaceId { get; init; }
    /// <summary>Key epoch being rotated away from.</summary>
    public ushort OldKeyEpoch { get; init; }
    /// <summary>Key epoch being rotated to.</summary>
    public ushort NewKeyEpoch { get; init; }
    /// <summary>Stable 16-byte node identity; set only in the constructor.</summary>
    public NodeId NodeId { get; init; }
    /// <summary>Ed25519 signature (64 bytes) over <see cref="KeyRotation.BuildProofMessage"/>.</summary>
    public byte[] Proof { get; init; } = Array.Empty<byte>();
}

/// <summary>Encode / decode KEY_ROTATION (frame type 0x11) and verify epoch proofs.</summary>
public static class KeyRotation
{
    /// <summary>Wire frame type byte (e.g. DELTA_BATCH, ACK).</summary>
    public const byte FrameType = 0x11;
    /// <summary>Constant proof domain.</summary>
    public const string ProofDomain = "BIN-SYNC/KEY-ROTATION/v1";

    /// <summary>
    /// Wire layout (little-endian):
    /// namespaceId u16 | oldEpoch u16 | newEpoch u16 | nodeId[16] | proofLen u16 | proof[proofLen].
    /// </summary>
    public static byte[] Encode(KeyRotationNotice n)
    {
        ArgumentNullException.ThrowIfNull(n);
        if (n.NewKeyEpoch <= n.OldKeyEpoch)
            throw new ArgumentException("newKeyEpoch must be greater than oldKeyEpoch");
        var proof = n.Proof?.ToArray() ?? throw new ArgumentException("KEY_ROTATION proof is required", nameof(n));
        if (proof.Length != BatchSignature.SignatureSize)
            throw new ArgumentException($"KEY_ROTATION proof must be {BatchSignature.SignatureSize} bytes", nameof(n));

        int proofLen = proof.Length;
        var buf = new byte[2 + 2 + 2 + 16 + 2 + proofLen];
        int o = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), n.NamespaceId); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), n.OldKeyEpoch); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), n.NewKeyEpoch); o += 2;
        n.NodeId.WriteTo(buf.AsSpan(o)); o += 16;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), (ushort)proofLen); o += 2;
        proof.CopyTo(buf.AsSpan(o));
        return buf;
    }

    /// <summary>Decode  from canonical wire bytes.</summary>
    public static KeyRotationNotice Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 2 + 2 + 2 + 16 + 2)
            throw new InvalidDataException("KEY_ROTATION payload too short");
        int o = 0;
        ushort ns = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        ushort oldE = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        ushort newE = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        var nodeId = NodeId.From(payload.Slice(o, 16)); o += 16;
        ushort proofLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        if (proofLen != BatchSignature.SignatureSize || payload.Length != o + proofLen)
            throw new InvalidDataException("KEY_ROTATION proof length or payload size is invalid");
        if (newE <= oldE)
            throw new InvalidDataException("KEY_ROTATION epoch must advance");
        var proof = payload.Slice(o, proofLen).ToArray();
        return new KeyRotationNotice
        {
            NamespaceId = ns,
            OldKeyEpoch = oldE,
            NewKeyEpoch = newE,
            NodeId = nodeId,
            Proof = proof,
        };
    }

    /// <summary>Canonical message covered by the rotation proof.</summary>
    public static byte[] BuildProofMessage(ushort namespaceId, ushort oldEpoch, ushort newEpoch, NodeId nodeId)
    {
        var domain = Encoding.UTF8.GetBytes(ProofDomain);
        var buf = new byte[domain.Length + 2 + 2 + 2 + 16];
        int o = 0;
        domain.CopyTo(buf.AsSpan(o)); o += domain.Length;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), namespaceId); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), oldEpoch); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), newEpoch); o += 2;
        nodeId.WriteTo(buf.AsSpan(o, 16));
        return buf;
    }

    /// <summary>Create a signed KEY_ROTATION notice under the node's identity seed.</summary>
    public static KeyRotationNotice Create(
        ushort namespaceId,
        ushort oldKeyEpoch,
        ushort newKeyEpoch,
        NodeId nodeId,
        ReadOnlySpan<byte> identitySeed)
    {
        // Creation signs the supplied transition; policy validation remains in Encode/TryApply.
        var msg = BuildProofMessage(namespaceId, oldKeyEpoch, newKeyEpoch, nodeId);
        var proof = BatchSignature.Sign(identitySeed, msg);
        return new KeyRotationNotice
        {
            NamespaceId = namespaceId,
            OldKeyEpoch = oldKeyEpoch,
            NewKeyEpoch = newKeyEpoch,
            NodeId = nodeId,
            Proof = proof,
        };
    }

    /// <summary>Verify the notice's Ed25519 proof against the publisher's public key.</summary>
    public static bool VerifyProof(KeyRotationNotice n, ReadOnlySpan<byte> publisherPublicKey)
    {
        if (n is null || n.NewKeyEpoch <= n.OldKeyEpoch ||
            n.Proof is null || n.Proof.Length != BatchSignature.SignatureSize) return false;
        var msg = BuildProofMessage(n.NamespaceId, n.OldKeyEpoch, n.NewKeyEpoch, n.NodeId);
        return BatchSignature.Verify(publisherPublicKey, msg, n.Proof);
    }

    /// <summary>Wrap payload in a protocol frame (control stream).</summary>
    public static byte[] BuildFrame(KeyRotationNotice n, ulong frameSeq, uint streamId = 0) =>
        FrameCodec.BuildFrame(FrameType, streamId, frameSeq, Encode(n));
}
