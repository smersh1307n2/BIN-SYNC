using BinSync;
using Xunit;

namespace BinSync.Tests;

public sealed class NamespaceRosterProtocolTests
{
    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static NamespaceRosterCertificate CreateRoster(byte[] authoritySeed, byte[] authorityKey, byte[] memberKey)
        => new()
        {
            NamespaceId = 7,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = NodeId.From(Id(1)),
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = NodeId.From(Id(1)),
                    PublicKey = authorityKey,
                    TransportPeerEpoch = 4,
                    Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = NodeId.From(Id(2)),
                    PublicKey = memberKey,
                    TransportPeerEpoch = 9,
                    Status = NamespaceMemberStatus.Joining
                }
            }
        };

    [Fact]
    public void NRS1_sign_verify_roundtrips_canonical_roster_and_digest()
    {
        var (authoritySeed, authorityKey) = BatchSignature.GenerateKeyPair();
        var (_, memberKey) = BatchSignature.GenerateKeyPair();
        NamespaceRosterCertificate roster = CreateRoster(authoritySeed, authorityKey, memberKey);
        byte[] encoded = NamespaceRosterProtocol.Sign(roster, authoritySeed);
        var trust = new TrustStore();
        trust.Allow(roster.AuthorityNodeId, authorityKey, TrustMode.PreShared);

        NamespaceRosterCertificate decoded = NamespaceRosterProtocol.DecodeAndVerify(encoded, trust);

        Assert.Equal(roster.NamespaceId, decoded.NamespaceId);
        Assert.Equal(roster.RosterGeneration, decoded.RosterGeneration);
        Assert.Equal(roster.AuthorityNodeId, decoded.AuthorityNodeId);
        Assert.Equal(2, decoded.Members.Count);
        Assert.Equal(NamespaceMemberStatus.Joining, decoded.Members[1].Status);
        Assert.Equal(NamespaceRosterProtocol.ComputeDigest(roster),
            NamespaceRosterProtocol.ComputeDigest(decoded));
        Assert.Equal(encoded, NamespaceRosterProtocol.Sign(decoded, authoritySeed));
    }

    [Fact]
    public void NRS1_rejects_tampered_signature_and_unpinned_or_revoked_authority()
    {
        var (authoritySeed, authorityKey) = BatchSignature.GenerateKeyPair();
        var (_, memberKey) = BatchSignature.GenerateKeyPair();
        var roster = CreateRoster(authoritySeed, authorityKey, memberKey);
        byte[] encoded = NamespaceRosterProtocol.Sign(roster, authoritySeed);

        var unpinned = new TrustStore();
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(
            () => NamespaceRosterProtocol.DecodeAndVerify(encoded, unpinned));

        var tofu = new TrustStore();
        tofu.Allow(roster.AuthorityNodeId, authorityKey, TrustMode.Tofu);
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(
            () => NamespaceRosterProtocol.DecodeAndVerify(encoded, tofu));

        var revoked = new TrustStore();
        revoked.Allow(roster.AuthorityNodeId, authorityKey, TrustMode.PreShared);
        revoked.Revoke(roster.AuthorityNodeId);
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(
            () => NamespaceRosterProtocol.DecodeAndVerify(encoded, revoked));

        var trusted = new TrustStore();
        trusted.Allow(roster.AuthorityNodeId, authorityKey, TrustMode.PreShared);
        encoded[^1] ^= 0x80;
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(
            () => NamespaceRosterProtocol.DecodeAndVerify(encoded, trusted));
    }

    [Fact]
    public void NRS1_rejects_unsorted_duplicate_and_invalid_retired_members()
    {
        var (authoritySeed, authorityKey) = BatchSignature.GenerateKeyPair();
        var (_, memberKey) = BatchSignature.GenerateKeyPair();
        var roster = CreateRoster(authoritySeed, authorityKey, memberKey);

        var unsorted = new NamespaceRosterCertificate
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            PreviousRosterDigest = roster.PreviousRosterDigest,
            AuthorityNodeId = roster.AuthorityNodeId,
            Members = roster.Members.Reverse().ToArray()
        };
        Assert.Throws<ArgumentException>(() => NamespaceRosterProtocol.Sign(unsorted, authoritySeed));

        var duplicate = new NamespaceRosterCertificate
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            PreviousRosterDigest = roster.PreviousRosterDigest,
            AuthorityNodeId = roster.AuthorityNodeId,
            Members = new[] { roster.Members[0], roster.Members[0] }
        };
        Assert.Throws<ArgumentException>(() => NamespaceRosterProtocol.Sign(duplicate, authoritySeed));

        var retired = new NamespaceRosterCertificate
        {
            NamespaceId = 7,
            RosterGeneration = 2,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = roster.AuthorityNodeId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = roster.AuthorityNodeId,
                    PublicKey = authorityKey,
                    Status = NamespaceMemberStatus.Retired,
                    RetiredThroughEpoch = 0
                }
            }
        };
        Assert.Throws<ArgumentException>(() => NamespaceRosterProtocol.Sign(retired, authoritySeed));
    }

    [Fact]
    public void NRS1_rejects_generation_chain_without_previous_digest()
    {
        var (authoritySeed, authorityKey) = BatchSignature.GenerateKeyPair();
        var (_, memberKey) = BatchSignature.GenerateKeyPair();
        var roster = CreateRoster(authoritySeed, authorityKey, memberKey);
        var next = new NamespaceRosterCertificate
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = 2,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = roster.AuthorityNodeId,
            Members = roster.Members
        };
        Assert.Throws<ArgumentException>(() => NamespaceRosterProtocol.Sign(next, authoritySeed));
    }
    [Fact]
    public void V31_capabilities_and_frame_ids_are_reserved_without_collisions()
    {
        Assert.Equal(1u << 3, (uint)SessionCapabilities.NamespaceRosterV31);
        Assert.Equal(1u << 4, (uint)SessionCapabilities.OriginFrontierV31);
        Assert.Equal(1u << 5, (uint)SessionCapabilities.FullReplaceV31);
        Assert.Equal("NAMESPACE_ROSTER", Frames.FrameTypes[0x14]);
        Assert.Equal("ORIGIN_FRONTIER", Frames.FrameTypes[0x15]);
        Assert.Equal("RETENTION_CHECKPOINT", Frames.FrameTypes[0x16]);
        Assert.Equal("FULL_REPLACE_BEGIN", Frames.FrameTypes[0x17]);
        Assert.Equal("FULL_REPLACE_CHUNK", Frames.FrameTypes[0x18]);
        Assert.Equal("FULL_REPLACE_COMMIT", Frames.FrameTypes[0x19]);
        Assert.Equal("FULL_REPLACE_ACK", Frames.FrameTypes[0x1A]);
        Assert.Equal("FULL_REPLACE_ABORT", Frames.FrameTypes[0x1B]);
    }

}
