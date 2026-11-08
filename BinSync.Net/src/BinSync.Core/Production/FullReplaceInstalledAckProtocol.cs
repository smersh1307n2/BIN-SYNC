using System.Security.Cryptography;
using System.Text;

namespace BinSync;

public sealed class FullReplaceInstalledAck
{
    public int NamespaceId { get; init; }
    public ulong RosterGeneration { get; init; }
    public byte[] RosterDigest { get; init; } = Array.Empty<byte>();
    public Guid Generation { get; init; }
    public byte[] Digest { get; init; } = Array.Empty<byte>();
    public NodeId MemberNodeId { get; init; }
    public uint MemberTransportEpoch { get; init; }
    public bool Installed { get; init; }
    public byte[] Signature { get; init; } = Array.Empty<byte>();
}

/// <summary>Canonical FRA1 member-signed proof of durable FULL_REPLACE installation.</summary>
public static class FullReplaceInstalledAckProtocol
{
    public const string SignDomain = "BIN-SYNC/FULL-REPLACE-INSTALLED-ACK/v1";
    private static ReadOnlySpan<byte> Magic => "FRA1"u8;

    public static byte[] Sign(FullReplaceInstalledAck acknowledgement, ReadOnlySpan<byte> memberSeed)
    {
        Validate(acknowledgement);
        byte[] payload = EncodePayload(acknowledgement);
        byte[] signature = BatchSignature.Sign(memberSeed, BuildSigningPayload(payload));
        byte[] encoded = new byte[payload.Length + BatchSignature.SignatureSize];
        payload.CopyTo(encoded, 0);
        signature.CopyTo(encoded, payload.Length);
        return encoded;
    }

    public static FullReplaceInstalledAck DecodeAndVerify(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate roster)
    {
        ArgumentNullException.ThrowIfNull(roster);
        NamespaceRosterProtocol.Validate(roster);
        const int payloadLength = 4 + 2 + 8 + 32 + 16 + 32 + 16 + 4 + 1;
        if (encoded.Length != payloadLength + BatchSignature.SignatureSize)
            throw new InvalidDataException("FRA1 acknowledgement length is invalid.");

        byte[] payload = encoded[..payloadLength].ToArray();
        byte[] signature = encoded[payloadLength..].ToArray();
        var reader = new Reader(payload);
        if (!reader.Raw(4).AsSpan().SequenceEqual(Magic))
            throw new InvalidDataException("FRA1 magic is invalid.");
        var acknowledgement = new FullReplaceInstalledAck
        {
            NamespaceId = reader.U16(),
            RosterGeneration = reader.U64(),
            RosterDigest = reader.Raw(NamespaceRosterProtocol.DigestSize),
            Generation = new Guid(reader.Raw(16), bigEndian: true),
            Digest = reader.Raw(NamespaceRosterProtocol.DigestSize),
            MemberNodeId = NodeId.From(reader.B128()),
            MemberTransportEpoch = reader.U32(),
            Installed = reader.U8() switch
            {
                0 => false,
                1 => true,
                _ => throw new InvalidDataException("FRA1 installed flag is invalid.")
            },
            Signature = signature
        };
        if (!reader.Eof())
            throw new InvalidDataException("FRA1 contains trailing bytes.");
        Validate(acknowledgement);
        ValidateRosterBinding(acknowledgement, roster);

        NamespaceRosterMember member = OriginFrontierProtocol.FindMember(roster, acknowledgement.MemberNodeId);
        if (member.Status == NamespaceMemberStatus.Retired ||
            member.TransportPeerEpoch != acknowledgement.MemberTransportEpoch)
            throw new InvalidDataException("FRA1 member is retired or its transport epoch does not match the roster.");
        if (!BatchSignature.Verify(member.PublicKey, BuildSigningPayload(payload), signature))
            throw new CryptographicException("FRA1 member signature is invalid.");
        return acknowledgement;
    }

    public static byte[] EncodePayload(FullReplaceInstalledAck acknowledgement)
    {
        Validate(acknowledgement);
        var writer = new Writer();
        writer.Raw(Magic);
        writer.U16(checked((ushort)acknowledgement.NamespaceId));
        writer.U64(acknowledgement.RosterGeneration);
        writer.Raw(acknowledgement.RosterDigest);
        Span<byte> generation = stackalloc byte[16];
        acknowledgement.Generation.TryWriteBytes(generation, bigEndian: true, out _);
        writer.Raw(generation);
        writer.Raw(acknowledgement.Digest);
        writer.B128(acknowledgement.MemberNodeId);
        writer.U32(acknowledgement.MemberTransportEpoch);
        writer.U8(acknowledgement.Installed ? (byte)1 : (byte)0);
        return writer.Value();
    }

    public static void Validate(FullReplaceInstalledAck acknowledgement)
    {
        ArgumentNullException.ThrowIfNull(acknowledgement);
        if (acknowledgement.NamespaceId <= 0 || acknowledgement.NamespaceId > ushort.MaxValue ||
            acknowledgement.RosterGeneration == 0 ||
            acknowledgement.RosterDigest is null ||
            acknowledgement.RosterDigest.Length != NamespaceRosterProtocol.DigestSize)
            throw new ArgumentException("FRA1 namespace/roster identity is invalid.", nameof(acknowledgement));
        if (acknowledgement.Generation == Guid.Empty ||
            acknowledgement.Digest is null ||
            acknowledgement.Digest.Length != NamespaceRosterProtocol.DigestSize)
            throw new ArgumentException("FRA1 FULL_REPLACE generation/digest is invalid.", nameof(acknowledgement));
        if (acknowledgement.MemberNodeId.IsZero || acknowledgement.MemberTransportEpoch == 0)
            throw new ArgumentException("FRA1 member identity and transport epoch must be non-zero.", nameof(acknowledgement));
        if (!acknowledgement.Installed)
            throw new ArgumentException("FRA1 is only an installed-success acknowledgement.", nameof(acknowledgement));
        if (acknowledgement.Signature is not null && acknowledgement.Signature.Length != 0 &&
            acknowledgement.Signature.Length != BatchSignature.SignatureSize)
            throw new ArgumentException("FRA1 signature must be empty or 64 bytes.", nameof(acknowledgement));
    }

    private static void ValidateRosterBinding(
        FullReplaceInstalledAck acknowledgement, NamespaceRosterCertificate roster)
    {
        if (acknowledgement.NamespaceId != roster.NamespaceId ||
            acknowledgement.RosterGeneration != roster.RosterGeneration ||
            !acknowledgement.RosterDigest.AsSpan().SequenceEqual(NamespaceRosterProtocol.ComputeDigest(roster)))
            throw new InvalidDataException("FRA1 is not bound to the accepted namespace roster.");
    }

    private static byte[] BuildSigningPayload(ReadOnlySpan<byte> payload)
    {
        byte[] domain = Encoding.UTF8.GetBytes(SignDomain);
        byte[] result = new byte[domain.Length + payload.Length];
        domain.CopyTo(result, 0);
        payload.CopyTo(result.AsSpan(domain.Length));
        return result;
    }
}
