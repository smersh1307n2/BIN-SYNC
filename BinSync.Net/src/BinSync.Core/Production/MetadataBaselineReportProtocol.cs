using System.Security.Cryptography;
using System.Text;

namespace BinSync;

public enum MetadataBaselineFeedKind : byte
{
    Outbound = 1,
    Relay = 2
}

public sealed class MetadataBaselineFeedEntry
{
    public NodeId TargetPeer { get; init; }
    public uint TargetTransportEpoch { get; init; }
    public NodeId AuthorStream { get; init; }
    public MetadataBaselineFeedKind FeedKind { get; init; }
    public uint IssuerEpoch { get; init; }
    public ulong AbsoluteFeedOffset { get; init; }
    public byte[] DigestAnchor { get; init; } = new byte[16];
}

public sealed class MetadataBaselineReceiveEntry
{
    public NodeId SenderPeer { get; init; }
    public uint SenderTransportEpoch { get; init; }
    public NodeId AuthorStream { get; init; }
    public uint IssuerEpoch { get; init; }
    public ulong ReceiveCursor { get; init; }
    public byte[] LastBatchDigest { get; init; } = new byte[16];
}

/// <summary>
/// One member's own Metadata cut associated with an installed FULL_REPLACE generation.
/// Reports are member-signed: an authority cannot invent another member's local feed
/// cursors or receive digest chains.
/// </summary>
public sealed class MetadataBaselineReport
{
    public int NamespaceId { get; init; }
    public ulong RosterGeneration { get; init; }
    public byte[] RosterDigest { get; init; } = Array.Empty<byte>();
    public Guid FullReplaceGeneration { get; init; }
    public byte[] SnapshotDigest { get; init; } = Array.Empty<byte>();
    public int SnapshotEntryCount { get; init; }
    public ulong ProviderChangeFeedWatermark { get; init; }
    public ulong HlcWallFloor { get; init; }
    public uint HlcLogicalFloor { get; init; }
    public NodeId MemberNodeId { get; init; }
    public uint MemberTransportEpoch { get; init; }
    public ulong ReportSequence { get; init; }
    public IReadOnlyList<MetadataBaselineFeedEntry> FeedEntries { get; init; } =
        Array.Empty<MetadataBaselineFeedEntry>();
    public IReadOnlyList<MetadataBaselineReceiveEntry> ReceiveEntries { get; init; } =
        Array.Empty<MetadataBaselineReceiveEntry>();
    public byte[] Signature { get; init; } = Array.Empty<byte>();
}

/// <summary>Canonical member-signed MBR1 FULL_REPLACE Metadata baseline evidence.</summary>
public static class MetadataBaselineReportProtocol
{
    public const int MaxEntries = 4096;
    public const int DigestSize = 32;
    public const int ChainDigestSize = 16;
    public const string SignDomain = "BIN-SYNC/METADATA-FULL-REPLACE-BASELINE/v1";
    private static ReadOnlySpan<byte> Magic => "MBR1"u8;

    public static byte[] Sign(MetadataBaselineReport report, ReadOnlySpan<byte> memberSeed)
    {
        Validate(report);
        byte[] payload = EncodePayload(report);
        byte[] signature = BatchSignature.Sign(memberSeed, BuildSigningPayload(payload));
        byte[] encoded = new byte[payload.Length + BatchSignature.SignatureSize];
        payload.CopyTo(encoded, 0);
        signature.CopyTo(encoded, payload.Length);
        return encoded;
    }

    public static MetadataBaselineReport DecodeAndVerify(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster,
        Guid expectedGeneration, ReadOnlySpan<byte> expectedSnapshotDigest)
    {
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        NamespaceRosterProtocol.Validate(acceptedRoster);
        if (expectedGeneration == Guid.Empty || expectedSnapshotDigest.Length != DigestSize)
            throw new ArgumentException("Expected FULL_REPLACE generation and 32-byte snapshot digest are required.");
        if (encoded.Length < 4 + 2 + 8 + 32 + 16 + 32 + 4 + 8 + 8 + 4 + 16 + 4 + 8 + 2 + 2 + 64)
            throw new InvalidDataException("MBR1 baseline report is truncated.");

        int payloadLength = encoded.Length - BatchSignature.SignatureSize;
        byte[] payload = encoded[..payloadLength].ToArray();
        byte[] signature = encoded[payloadLength..].ToArray();
        using var stream = new MemoryStream(payload, writable: false);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false);
        if (!ReadExact(reader, 4).AsSpan().SequenceEqual(Magic))
            throw new InvalidDataException("MBR1 magic is invalid.");

        int namespaceId = reader.ReadUInt16();
        ulong rosterGeneration = reader.ReadUInt64();
        byte[] rosterDigest = ReadExact(reader, DigestSize);
        Guid generation = new(ReadExact(reader, 16), bigEndian: true);
        byte[] snapshotDigest = ReadExact(reader, DigestSize);
        int snapshotEntryCount = reader.ReadInt32();
        ulong providerWatermark = reader.ReadUInt64();
        ulong hlcWall = reader.ReadUInt64();
        uint hlcLogical = reader.ReadUInt32();
        NodeId member = NodeId.From(ReadExact(reader, 16));
        uint memberEpoch = reader.ReadUInt32();
        ulong reportSequence = reader.ReadUInt64();

        int feedCount = reader.ReadUInt16();
        if (feedCount > MaxEntries || feedCount > (stream.Length - stream.Position - 2) / 61)
            throw new InvalidDataException("MBR1 feed entry count exceeds the bounded payload.");
        var feeds = new MetadataBaselineFeedEntry[feedCount];
        for (int i = 0; i < feedCount; i++)
        {
            feeds[i] = new MetadataBaselineFeedEntry
            {
                TargetPeer = NodeId.From(ReadExact(reader, 16)),
                TargetTransportEpoch = reader.ReadUInt32(),
                AuthorStream = NodeId.From(ReadExact(reader, 16)),
                FeedKind = (MetadataBaselineFeedKind)reader.ReadByte(),
                IssuerEpoch = reader.ReadUInt32(),
                AbsoluteFeedOffset = reader.ReadUInt64(),
                DigestAnchor = ReadExact(reader, ChainDigestSize)
            };
        }

        int receiveCount = reader.ReadUInt16();
        if (receiveCount > MaxEntries || receiveCount > (stream.Length - stream.Position) / 60)
            throw new InvalidDataException("MBR1 receive entry count exceeds the bounded payload.");
        var receives = new MetadataBaselineReceiveEntry[receiveCount];
        for (int i = 0; i < receiveCount; i++)
        {
            receives[i] = new MetadataBaselineReceiveEntry
            {
                SenderPeer = NodeId.From(ReadExact(reader, 16)),
                SenderTransportEpoch = reader.ReadUInt32(),
                AuthorStream = NodeId.From(ReadExact(reader, 16)),
                IssuerEpoch = reader.ReadUInt32(),
                ReceiveCursor = reader.ReadUInt64(),
                LastBatchDigest = ReadExact(reader, ChainDigestSize)
            };
        }
        if (stream.Position != stream.Length)
            throw new InvalidDataException("MBR1 contains trailing payload bytes.");

        var report = new MetadataBaselineReport
        {
            NamespaceId = namespaceId,
            RosterGeneration = rosterGeneration,
            RosterDigest = rosterDigest,
            FullReplaceGeneration = generation,
            SnapshotDigest = snapshotDigest,
            SnapshotEntryCount = snapshotEntryCount,
            ProviderChangeFeedWatermark = providerWatermark,
            HlcWallFloor = hlcWall,
            HlcLogicalFloor = hlcLogical,
            MemberNodeId = member,
            MemberTransportEpoch = memberEpoch,
            ReportSequence = reportSequence,
            FeedEntries = feeds,
            ReceiveEntries = receives,
            Signature = signature
        };
        Validate(report);
        ValidateRosterBinding(report, acceptedRoster);
        if (report.FullReplaceGeneration != expectedGeneration ||
            !CryptographicOperations.FixedTimeEquals(report.SnapshotDigest, expectedSnapshotDigest))
            throw new InvalidDataException("MBR1 report is bound to a different FULL_REPLACE generation or snapshot digest.");

        NamespaceRosterMember rosterMember = FindMember(acceptedRoster, member);
        if (rosterMember.Status != NamespaceMemberStatus.Active ||
            rosterMember.TransportPeerEpoch != memberEpoch)
            throw new InvalidDataException("MBR1 member is not Active at the reported transport epoch.");
        if (!BatchSignature.Verify(rosterMember.PublicKey, BuildSigningPayload(payload), signature))
            throw new CryptographicException("MBR1 member signature is invalid.");
        return report;
    }

    public static byte[] EncodePayload(MetadataBaselineReport report)
    {
        Validate(report);
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        writer.Write(Magic);
        writer.Write(checked((ushort)report.NamespaceId));
        writer.Write(report.RosterGeneration);
        writer.Write(report.RosterDigest);
        Span<byte> generation = stackalloc byte[16];
        report.FullReplaceGeneration.TryWriteBytes(generation, bigEndian: true, out _);
        writer.Write(generation);
        writer.Write(report.SnapshotDigest);
        writer.Write(report.SnapshotEntryCount);
        writer.Write(report.ProviderChangeFeedWatermark);
        writer.Write(report.HlcWallFloor);
        writer.Write(report.HlcLogicalFloor);
        writer.Write(report.MemberNodeId.ToArray());
        writer.Write(report.MemberTransportEpoch);
        writer.Write(report.ReportSequence);
        writer.Write(checked((ushort)report.FeedEntries.Count));
        foreach (MetadataBaselineFeedEntry entry in report.FeedEntries)
        {
            writer.Write(entry.TargetPeer.ToArray());
            writer.Write(entry.TargetTransportEpoch);
            writer.Write(entry.AuthorStream.ToArray());
            writer.Write((byte)entry.FeedKind);
            writer.Write(entry.IssuerEpoch);
            writer.Write(entry.AbsoluteFeedOffset);
            writer.Write(entry.DigestAnchor);
        }
        writer.Write(checked((ushort)report.ReceiveEntries.Count));
        foreach (MetadataBaselineReceiveEntry entry in report.ReceiveEntries)
        {
            writer.Write(entry.SenderPeer.ToArray());
            writer.Write(entry.SenderTransportEpoch);
            writer.Write(entry.AuthorStream.ToArray());
            writer.Write(entry.IssuerEpoch);
            writer.Write(entry.ReceiveCursor);
            writer.Write(entry.LastBatchDigest);
        }
        writer.Flush();
        return stream.ToArray();
    }

    public static void Validate(MetadataBaselineReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (report.NamespaceId <= 0 || report.NamespaceId > ushort.MaxValue ||
            report.RosterGeneration == 0 || report.RosterDigest is null || report.RosterDigest.Length != DigestSize)
            throw new ArgumentException("MBR1 namespace and roster identity are invalid.", nameof(report));
        if (report.FullReplaceGeneration == Guid.Empty || report.SnapshotDigest is null ||
            report.SnapshotDigest.Length != DigestSize || report.SnapshotEntryCount < 0)
            throw new ArgumentException("MBR1 FULL_REPLACE snapshot identity is invalid.", nameof(report));
        if (report.MemberNodeId.IsZero || report.MemberTransportEpoch == 0 || report.ReportSequence == 0)
            throw new ArgumentException("MBR1 member identity, transport epoch and report sequence must be non-zero.", nameof(report));
        if (report.FeedEntries is null || report.ReceiveEntries is null ||
            report.FeedEntries.Count > MaxEntries || report.ReceiveEntries.Count > MaxEntries)
            throw new ArgumentOutOfRangeException(nameof(report), "MBR1 feed/receive entry count exceeds its bound.");

        MetadataBaselineFeedEntry? previousFeed = null;
        foreach (MetadataBaselineFeedEntry entry in report.FeedEntries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            if (entry.TargetPeer.IsZero || entry.TargetPeer == report.MemberNodeId ||
                entry.TargetTransportEpoch == 0 || entry.AuthorStream.IsZero || entry.IssuerEpoch == 0 ||
                entry.FeedKind is not (MetadataBaselineFeedKind.Outbound or MetadataBaselineFeedKind.Relay) ||
                entry.DigestAnchor is null || entry.DigestAnchor.Length != ChainDigestSize)
                throw new ArgumentException("MBR1 feed entry identity or digest is invalid.", nameof(report));
            if (previousFeed is not null && CompareFeed(previousFeed, entry) >= 0)
                throw new ArgumentException("MBR1 feed entries must be strictly sorted and unique by peer/epoch/author.", nameof(report));
            previousFeed = entry;
        }

        MetadataBaselineReceiveEntry? previousReceive = null;
        foreach (MetadataBaselineReceiveEntry entry in report.ReceiveEntries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            if (entry.SenderPeer.IsZero || entry.SenderPeer == report.MemberNodeId ||
                entry.SenderTransportEpoch == 0 || entry.AuthorStream.IsZero || entry.IssuerEpoch == 0 ||
                entry.LastBatchDigest is null || entry.LastBatchDigest.Length != ChainDigestSize)
                throw new ArgumentException("MBR1 receive entry identity or digest is invalid.", nameof(report));
            if (entry.ReceiveCursor == 0 && !IsZero(entry.LastBatchDigest))
                throw new ArgumentException("An empty receive stream must use the zero digest anchor.", nameof(report));
            if (previousReceive is not null && CompareReceive(previousReceive, entry) >= 0)
                throw new ArgumentException("MBR1 receive entries must be strictly sorted and unique.", nameof(report));
            previousReceive = entry;
        }
        if (report.Signature is null || (report.Signature.Length != 0 &&
            report.Signature.Length != BatchSignature.SignatureSize))
            throw new ArgumentException("MBR1 signature must be empty or 64 bytes.", nameof(report));
    }

    private static void ValidateRosterBinding(MetadataBaselineReport report, NamespaceRosterCertificate roster)
    {
        if (report.NamespaceId != roster.NamespaceId ||
            report.RosterGeneration != roster.RosterGeneration ||
            !CryptographicOperations.FixedTimeEquals(report.RosterDigest, NamespaceRosterProtocol.ComputeDigest(roster)))
            throw new InvalidDataException("MBR1 report is not bound to the accepted namespace roster.");
    }

    private static NamespaceRosterMember FindMember(NamespaceRosterCertificate roster, NodeId nodeId)
        => roster.Members.FirstOrDefault(member => member.NodeId == nodeId)
            ?? throw new InvalidDataException("MBR1 signer is absent from the accepted roster.");

    private static int CompareFeed(MetadataBaselineFeedEntry left, MetadataBaselineFeedEntry right)
    {
        int result = CompareNode(left.TargetPeer, right.TargetPeer);
        if (result != 0) return result;
        result = left.TargetTransportEpoch.CompareTo(right.TargetTransportEpoch);
        if (result != 0) return result;
        result = CompareNode(left.AuthorStream, right.AuthorStream);
        return result != 0 ? result : ((byte)left.FeedKind).CompareTo((byte)right.FeedKind);
    }

    private static int CompareReceive(MetadataBaselineReceiveEntry left, MetadataBaselineReceiveEntry right)
    {
        int result = CompareNode(left.SenderPeer, right.SenderPeer);
        if (result != 0) return result;
        result = left.SenderTransportEpoch.CompareTo(right.SenderTransportEpoch);
        if (result != 0) return result;
        result = CompareNode(left.AuthorStream, right.AuthorStream);
        return result != 0 ? result : left.IssuerEpoch.CompareTo(right.IssuerEpoch);
    }

    private static int CompareNode(NodeId left, NodeId right)
        => left.ToArray().AsSpan().SequenceCompareTo(right.ToArray());

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        foreach (byte item in value) if (item != 0) return false;
        return true;
    }

    private static byte[] BuildSigningPayload(ReadOnlySpan<byte> payload)
    {
        byte[] domain = Encoding.UTF8.GetBytes(SignDomain);
        byte[] result = new byte[domain.Length + payload.Length];
        domain.CopyTo(result, 0);
        payload.CopyTo(result.AsSpan(domain.Length));
        return result;
    }

    private static byte[] ReadExact(BinaryReader reader, int length)
    {
        byte[] bytes = reader.ReadBytes(length);
        if (bytes.Length != length) throw new InvalidDataException("MBR1 payload is truncated.");
        return bytes;
    }
}
