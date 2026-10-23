// KEY_ROTATION frame payload (§14.9, type 0x11).
// Spec is brief: "frame with epoch proof". We encode a minimal, self-describing payload;
// proof is an Ed25519 signature over the rotation message under the node's identity key.
using System.Buffers.Binary;
using System.Text;

namespace BinSync;

/// <summary>Decoded KEY_ROTATION payload.</summary>
public sealed class KeyRotationNotice
{
    public ushort NamespaceId { get; init; }
    public ushort OldKeyEpoch { get; init; }
    public ushort NewKeyEpoch { get; init; }
    public byte[] NodeId { get; init; } = Array.Empty<byte>();
    /// <summary>Ed25519 signature (64 bytes) over <see cref="KeyRotation.BuildProofMessage"/>.</summary>
    public byte[] Proof { get; init; } = Array.Empty<byte>();
}

/// <summary>Encode / decode KEY_ROTATION (frame type 0x11) and verify epoch proofs.</summary>
public static class KeyRotation
{
    public const byte FrameType = 0x11;
    public const string ProofDomain = "BIN-SYNC/KEY-ROTATION/v1";

    /// <summary>
    /// Wire layout (little-endian):
    /// namespaceId u16 | oldEpoch u16 | newEpoch u16 | nodeId[16] | proofLen u16 | proof[proofLen].
    /// </summary>
    public static byte[] Encode(KeyRotationNotice n)
    {
        if (n.NodeId.Length != 16)
            throw new ArgumentException("nodeId must be 16 bytes", nameof(n));
        if (n.NewKeyEpoch <= n.OldKeyEpoch)
            throw new ArgumentException("newKeyEpoch must be greater than oldKeyEpoch");

        int proofLen = n.Proof.Length;
        var buf = new byte[2 + 2 + 2 + 16 + 2 + proofLen];
        int o = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), n.NamespaceId); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), n.OldKeyEpoch); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), n.NewKeyEpoch); o += 2;
        n.NodeId.CopyTo(buf.AsSpan(o)); o += 16;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), (ushort)proofLen); o += 2;
        n.Proof.CopyTo(buf.AsSpan(o));
        return buf;
    }

    public static KeyRotationNotice Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 2 + 2 + 2 + 16 + 2)
            throw new ArgumentException("KEY_ROTATION payload too short");
        int o = 0;
        ushort ns = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        ushort oldE = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        ushort newE = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        var nodeId = payload.Slice(o, 16).ToArray(); o += 16;
        ushort proofLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        if (payload.Length < o + proofLen)
            throw new ArgumentException("KEY_ROTATION proof truncated");
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
    public static byte[] BuildProofMessage(ushort namespaceId, ushort oldEpoch, ushort newEpoch, ReadOnlySpan<byte> nodeId)
    {
        var domain = Encoding.UTF8.GetBytes(ProofDomain);
        var buf = new byte[domain.Length + 2 + 2 + 2 + nodeId.Length];
        int o = 0;
        domain.CopyTo(buf.AsSpan(o)); o += domain.Length;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), namespaceId); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), oldEpoch); o += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), newEpoch); o += 2;
        nodeId.CopyTo(buf.AsSpan(o));
        return buf;
    }

    /// <summary>Create a signed KEY_ROTATION notice under the node's identity seed.</summary>
    public static KeyRotationNotice Create(
        ushort namespaceId,
        ushort oldKeyEpoch,
        ushort newKeyEpoch,
        ReadOnlySpan<byte> nodeId,
        ReadOnlySpan<byte> identitySeed)
    {
        var msg = BuildProofMessage(namespaceId, oldKeyEpoch, newKeyEpoch, nodeId);
        var proof = BatchSignature.Sign(identitySeed, msg);
        return new KeyRotationNotice
        {
            NamespaceId = namespaceId,
            OldKeyEpoch = oldKeyEpoch,
            NewKeyEpoch = newKeyEpoch,
            NodeId = nodeId.ToArray(),
            Proof = proof,
        };
    }

    /// <summary>Verify the notice's Ed25519 proof against the publisher's public key.</summary>
    public static bool VerifyProof(KeyRotationNotice n, ReadOnlySpan<byte> publisherPublicKey)
    {
        if (n.NewKeyEpoch <= n.OldKeyEpoch) return false;
        var msg = BuildProofMessage(n.NamespaceId, n.OldKeyEpoch, n.NewKeyEpoch, n.NodeId);
        return BatchSignature.Verify(publisherPublicKey, msg, n.Proof);
    }

    /// <summary>Wrap payload in a protocol frame (control stream).</summary>
    public static byte[] BuildFrame(KeyRotationNotice n, ulong frameSeq, uint streamId = 0) =>
        FrameCodec.BuildFrame(FrameType, streamId, frameSeq, Encode(n));
}
