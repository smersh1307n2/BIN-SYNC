using System.Security.Cryptography;
using System.Text;

namespace BinSync;

public enum NamespaceMemberStatus : byte
{
    Joining = 0,
    Active = 1,
    Retired = 2
}

public sealed class NamespaceRosterMember
{
    public NodeId NodeId { get; init; }
    public byte[] PublicKey { get; init; } = Array.Empty<byte>();
    public uint TransportPeerEpoch { get; init; }
    public NamespaceMemberStatus Status { get; init; }
    public uint RetiredThroughEpoch { get; init; }
}

public sealed class NamespaceRetiredOriginEpoch
{
    public NodeId OriginAuthorNodeId { get; init; }
    public uint OriginAuthorEpoch { get; init; }
    public ulong FinalIssuedOriginSequence { get; init; }
    public ulong MaxIssuedHlcWall { get; init; }
    public uint MaxIssuedHlcLogical { get; init; }
}

public sealed class NamespaceRosterCertificate
{
    public int NamespaceId { get; init; }
    public ulong RosterGeneration { get; init; }
    public byte[] PreviousRosterDigest { get; init; } = Array.Empty<byte>();
    public NodeId AuthorityNodeId { get; init; }
    public IReadOnlyList<NamespaceRosterMember> Members { get; init; } = Array.Empty<NamespaceRosterMember>();
    public IReadOnlyList<NamespaceRetiredOriginEpoch> RetiredOriginEpochs { get; init; } = Array.Empty<NamespaceRetiredOriginEpoch>();
    public byte[] Signature { get; init; } = Array.Empty<byte>();
}

/// <summary>
/// Canonical NRS2 signed namespace roster. Only a pinned PreShared authority key
/// can authorize a roster; TOFU, CA-only and revoked keys are deliberately rejected.
/// </summary>
public static class NamespaceRosterProtocol
{
    public const int DigestSize = 32;
    public const int MaxMembers = 4096;
    public const int MaxRetiredOriginEpochs = 16384;
    public const string SignDomain = "BIN-SYNC/NAMESPACE-ROSTER/v2";
    private static ReadOnlySpan<byte> Magic => "NRS2"u8;

    public static byte[] Sign(NamespaceRosterCertificate roster, ReadOnlySpan<byte> authoritySeed)
    {
        Validate(roster);
        byte[] payload = EncodePayload(roster);
        byte[] signingPayload = BuildSigningPayload(payload);
        byte[] signature = BatchSignature.Sign(authoritySeed, signingPayload);
        byte[] encoded = new byte[payload.Length + BatchSignature.SignatureSize];
        payload.CopyTo(encoded, 0);
        signature.CopyTo(encoded, payload.Length);
        return encoded;
    }

    public static NamespaceRosterCertificate DecodeAndVerify(ReadOnlySpan<byte> encoded, TrustStore trustStore)
    {
        ArgumentNullException.ThrowIfNull(trustStore);
        if (encoded.Length < 4 + 2 + 8 + DigestSize + 16 + 2 + BatchSignature.SignatureSize)
            throw new InvalidDataException("NRS2 roster is truncated.");

        int payloadLength = encoded.Length - BatchSignature.SignatureSize;
        byte[] payload = encoded[..payloadLength].ToArray();
        byte[] signature = encoded[payloadLength..].ToArray();
        var reader = new Reader(payload);
        if (!reader.Raw(4).AsSpan().SequenceEqual(Magic))
            throw new InvalidDataException("NRS2 roster magic is invalid.");

        int namespaceId = reader.U16();
        ulong generation = reader.U64();
        byte[] previousDigest = reader.Raw(DigestSize);
        NodeId authority = NodeId.From(reader.B128());
        int count = reader.U16();
        if (count > MaxMembers || reader.Remaining < 2 || count > (reader.Remaining - 2) / 57)
            throw new InvalidDataException("NRS2 member count exceeds the bounded payload.");

        var members = new List<NamespaceRosterMember>(count);
        for (int i = 0; i < count; i++)
        {
            NodeId memberId = NodeId.From(reader.B128());
            byte[] publicKey = reader.Raw(BatchSignature.PublicKeySize);
            uint peerEpoch = reader.U32();
            var status = (NamespaceMemberStatus)reader.U8();
            uint retiredThrough = reader.U32();
            members.Add(new NamespaceRosterMember
            {
                NodeId = memberId,
                PublicKey = publicKey,
                TransportPeerEpoch = peerEpoch,
                Status = status,
                RetiredThroughEpoch = retiredThrough
            });
        }

        int retiredOriginCount = reader.U16();
        if (retiredOriginCount > MaxRetiredOriginEpochs || retiredOriginCount > reader.Remaining / 40)
            throw new InvalidDataException("NRS2 retired-origin count exceeds the bounded payload.");
        var retiredOrigins = new List<NamespaceRetiredOriginEpoch>(retiredOriginCount);
        for (int i = 0; i < retiredOriginCount; i++)
        {
            retiredOrigins.Add(new NamespaceRetiredOriginEpoch
            {
                OriginAuthorNodeId = NodeId.From(reader.B128()),
                OriginAuthorEpoch = reader.U32(),
                FinalIssuedOriginSequence = reader.U64(),
                MaxIssuedHlcWall = reader.U64(),
                MaxIssuedHlcLogical = reader.U32()
            });
        }
        if (!reader.Eof())
            throw new InvalidDataException("NRS2 roster contains trailing payload bytes.");

        var roster = new NamespaceRosterCertificate
        {
            NamespaceId = namespaceId,
            RosterGeneration = generation,
            PreviousRosterDigest = previousDigest,
            AuthorityNodeId = authority,
            Members = members,
            RetiredOriginEpochs = retiredOrigins,
            Signature = signature
        };
        Validate(roster);

        if (!trustStore.TryGet(authority, out TrustedPeer? trusted) ||
            trusted is null || trusted.Revoked || trusted.Mode != TrustMode.PreShared ||
            trusted.PublicKey.Length != BatchSignature.PublicKeySize)
            throw new CryptographicException("Namespace authority is not pinned with active PreShared trust.");

        if (!BatchSignature.Verify(trusted.PublicKey, BuildSigningPayload(payload), signature))
            throw new CryptographicException("NRS2 namespace roster signature is invalid.");

        return roster;
    }

    public static void VerifyOrThrow(NamespaceRosterCertificate roster, TrustStore trustStore)
    {
        ArgumentNullException.ThrowIfNull(trustStore);
        Validate(roster);
        if (!trustStore.TryGet(roster.AuthorityNodeId, out TrustedPeer? trusted) ||
            trusted is null || trusted.Revoked || trusted.Mode != TrustMode.PreShared ||
            trusted.PublicKey.Length != BatchSignature.PublicKeySize)
            throw new CryptographicException("Namespace authority is not pinned with active PreShared trust.");
        byte[] payload = EncodePayload(roster);
        if (roster.Signature is null || roster.Signature.Length != BatchSignature.SignatureSize ||
            !BatchSignature.Verify(trusted.PublicKey, BuildSigningPayload(payload), roster.Signature))
            throw new CryptographicException("NRS2 namespace roster signature is invalid.");
    }

    public static byte[] ComputeDigest(NamespaceRosterCertificate roster)
    {
        Validate(roster);
        return SHA256.HashData(BuildSigningPayload(EncodePayload(roster)));
    }

    public static byte[] EncodePayload(NamespaceRosterCertificate roster)
    {
        Validate(roster);
        var writer = new Writer();
        writer.Raw(Magic);
        writer.U16(checked((ushort)roster.NamespaceId));
        writer.U64(roster.RosterGeneration);
        writer.Raw(roster.PreviousRosterDigest);
        writer.B128(roster.AuthorityNodeId);
        writer.U16(checked((ushort)roster.Members.Count));
        foreach (NamespaceRosterMember member in roster.Members)
        {
            writer.B128(member.NodeId);
            writer.Raw(member.PublicKey);
            writer.U32(member.TransportPeerEpoch);
            writer.U8((byte)member.Status);
            writer.U32(member.RetiredThroughEpoch);
        }
        writer.U16(checked((ushort)roster.RetiredOriginEpochs.Count));
        foreach (NamespaceRetiredOriginEpoch retired in roster.RetiredOriginEpochs)
        {
            writer.B128(retired.OriginAuthorNodeId);
            writer.U32(retired.OriginAuthorEpoch);
            writer.U64(retired.FinalIssuedOriginSequence);
            writer.U64(retired.MaxIssuedHlcWall);
            writer.U32(retired.MaxIssuedHlcLogical);
        }
        return writer.Value();
    }

    public static void Validate(NamespaceRosterCertificate roster)
    {
        ArgumentNullException.ThrowIfNull(roster);
        if (roster.NamespaceId <= 0 || roster.NamespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(roster), "NamespaceId must fit a non-zero u16.");
        if (roster.RosterGeneration == 0)
            throw new ArgumentException("RosterGeneration must be non-zero.", nameof(roster));
        if (roster.AuthorityNodeId.IsZero)
            throw new ArgumentException("Namespace authority identity must be non-zero.", nameof(roster));
        if (roster.PreviousRosterDigest is null || roster.PreviousRosterDigest.Length != DigestSize)
            throw new ArgumentException("PreviousRosterDigest must be 32 bytes.", nameof(roster));
        if (roster.RosterGeneration == 1 && !IsZero(roster.PreviousRosterDigest))
            throw new ArgumentException("The first roster generation must use a zero previous digest.", nameof(roster));
        if (roster.RosterGeneration > 1 && IsZero(roster.PreviousRosterDigest))
            throw new ArgumentException("A later roster generation must bind its previous digest.", nameof(roster));
        ArgumentNullException.ThrowIfNull(roster.Members);
        if (roster.Members.Count is 0 or > MaxMembers)
            throw new ArgumentOutOfRangeException(nameof(roster), $"Roster member count must be 1..{MaxMembers}.");

        NamespaceRosterMember? previous = null;
        foreach (NamespaceRosterMember member in roster.Members)
        {
            ArgumentNullException.ThrowIfNull(member);
            if (member.NodeId.IsZero)
                throw new ArgumentException("Roster member NodeId must be non-zero.", nameof(roster));
            if (member.PublicKey is null || member.PublicKey.Length != BatchSignature.PublicKeySize || IsZero(member.PublicKey))
                throw new ArgumentException("Roster member public key must be a non-zero 32-byte Ed25519 key.", nameof(roster));
            if (member.Status is not (NamespaceMemberStatus.Joining or NamespaceMemberStatus.Active or NamespaceMemberStatus.Retired))
                throw new ArgumentException("Roster member status is invalid.", nameof(roster));

            if (member.Status is NamespaceMemberStatus.Joining or NamespaceMemberStatus.Active)
            {
                if (member.TransportPeerEpoch == 0 || member.RetiredThroughEpoch != 0)
                    throw new ArgumentException("Joining/Active members require a transport epoch and zero retired fence.", nameof(roster));
            }
            else if (member.RetiredThroughEpoch == 0)
            {
                throw new ArgumentException("Retired members require a durable RetiredThroughEpoch.", nameof(roster));
            }

            if (previous is not null && CompareNodeIds(previous.NodeId, member.NodeId) >= 0)
                throw new ArgumentException("Roster members must be strictly sorted by NodeId with no duplicates.", nameof(roster));
            previous = member;
        }

        ArgumentNullException.ThrowIfNull(roster.RetiredOriginEpochs);
        if (roster.RetiredOriginEpochs.Count > MaxRetiredOriginEpochs)
            throw new ArgumentOutOfRangeException(nameof(roster), $"Retired-origin count exceeds {MaxRetiredOriginEpochs}.");
        NamespaceRetiredOriginEpoch? previousRetired = null;
        foreach (NamespaceRetiredOriginEpoch retired in roster.RetiredOriginEpochs)
        {
            ArgumentNullException.ThrowIfNull(retired);
            if (retired.OriginAuthorNodeId.IsZero || retired.OriginAuthorEpoch == 0 ||
                retired.FinalIssuedOriginSequence == 0)
                throw new ArgumentException("Retired-origin closure requires author, epoch and final issued sequence.", nameof(roster));
            NamespaceRosterMember? authorMember = roster.Members.FirstOrDefault(member =>
                member.NodeId == retired.OriginAuthorNodeId);
            if (authorMember is null)
                throw new ArgumentException("Retired-origin closure must reference a retained roster member identity.", nameof(roster));
            if (previousRetired is not null)
            {
                int authorOrder = CompareNodeIds(previousRetired.OriginAuthorNodeId, retired.OriginAuthorNodeId);
                if (authorOrder > 0 || (authorOrder == 0 &&
                    previousRetired.OriginAuthorEpoch >= retired.OriginAuthorEpoch))
                    throw new ArgumentException("Retired-origin closures must be strictly sorted and unique by author/epoch.", nameof(roster));
            }
            previousRetired = retired;
        }

        if (roster.Signature is null || (roster.Signature.Length != 0 &&
            roster.Signature.Length != BatchSignature.SignatureSize))
            throw new ArgumentException("Roster signature must be empty or 64 bytes.", nameof(roster));
    }

    private static byte[] BuildSigningPayload(ReadOnlySpan<byte> payload)
    {
        byte[] domain = Encoding.UTF8.GetBytes(SignDomain);
        byte[] result = new byte[domain.Length + payload.Length];
        domain.CopyTo(result, 0);
        payload.CopyTo(result.AsSpan(domain.Length));
        return result;
    }

    private static int CompareNodeIds(NodeId left, NodeId right)
    {
        Span<byte> a = stackalloc byte[16];
        Span<byte> b = stackalloc byte[16];
        left.WriteTo(a);
        right.WriteTo(b);
        return a.SequenceCompareTo(b);
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte b in value) aggregate |= b;
        return aggregate == 0;
    }
}
