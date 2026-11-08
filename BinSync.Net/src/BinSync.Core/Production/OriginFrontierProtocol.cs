using System.Security.Cryptography;
using System.Text;

namespace BinSync;

public sealed class AppliedOriginFrontierEntry
{
    public NodeId OriginAuthorNodeId { get; init; }
    public uint OriginAuthorEpoch { get; init; }
    public ulong ContiguousAppliedOriginSequence { get; init; }
}

public sealed class IssuedOriginFrontierEntry
{
    public uint OriginAuthorEpoch { get; init; }
    public ulong IssuedOriginSequence { get; init; }
    public ulong MaxIssuedHlcWall { get; init; }
    public uint MaxIssuedHlcLogical { get; init; }
}

public sealed class AppliedOriginFrontierReport
{
    public int NamespaceId { get; init; }
    public ulong RosterGeneration { get; init; }
    public byte[] RosterDigest { get; init; } = Array.Empty<byte>();
    public NodeId MemberNodeId { get; init; }
    public uint MemberTransportEpoch { get; init; }
    public ulong ReportSequence { get; init; }
    public bool FinalForAuthor { get; init; }
    public ulong UnpublishedDraftCount { get; init; }
    public ulong MaxUnpublishedDraftHlcWall { get; init; }
    public uint MaxUnpublishedDraftHlcLogical { get; init; }
    public IReadOnlyList<AppliedOriginFrontierEntry> Entries { get; init; } = Array.Empty<AppliedOriginFrontierEntry>();
    /// <summary>Final issued high-watermarks for this report's own author identity, one per epoch.</summary>
    public IReadOnlyList<IssuedOriginFrontierEntry> IssuedOrigins { get; init; } = Array.Empty<IssuedOriginFrontierEntry>();
    public byte[] Signature { get; init; } = Array.Empty<byte>();
}

public sealed class RetentionFrontierEntry
{
    public NodeId OriginAuthorNodeId { get; init; }
    public uint OriginAuthorEpoch { get; init; }
    public ulong SafeAppliedOriginSequence { get; init; }
}

public sealed class NamespaceRetentionCheckpoint
{
    public int NamespaceId { get; init; }
    public ulong RosterGeneration { get; init; }
    public byte[] RosterDigest { get; init; } = Array.Empty<byte>();
    public ulong CheckpointSequence { get; init; }
    public byte[] PreviousCheckpointDigest { get; init; } = Array.Empty<byte>();
    public IReadOnlyList<RetentionFrontierEntry> SafeFrontier { get; init; } = Array.Empty<RetentionFrontierEntry>();
    public Guid? FullReplaceGeneration { get; init; }
    public byte[] FullReplaceDigest { get; init; } = Array.Empty<byte>();
    public ulong FullReplaceHlcWallFloor { get; init; }
    public uint FullReplaceHlcLogicalFloor { get; init; }
    public byte[] Signature { get; init; } = Array.Empty<byte>();
}

/// <summary>Canonical OFR2 member-signed applied-origin frontier reports.</summary>
public static class OriginFrontierProtocol
{
    public const int MaxEntries = 4096;
    public const string SignDomain = "BIN-SYNC/APPLIED-ORIGIN-FRONTIER/v2";
    private static ReadOnlySpan<byte> Magic => "OFR2"u8;

    public static byte[] Sign(AppliedOriginFrontierReport report, ReadOnlySpan<byte> memberSeed)
    {
        Validate(report);
        byte[] payload = EncodePayload(report);
        byte[] signature = BatchSignature.Sign(memberSeed, BuildSigningPayload(payload));
        byte[] encoded = new byte[payload.Length + BatchSignature.SignatureSize];
        payload.CopyTo(encoded, 0);
        signature.CopyTo(encoded, payload.Length);
        return encoded;
    }

    public static AppliedOriginFrontierReport DecodeAndVerify(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate roster)
    {
        ArgumentNullException.ThrowIfNull(roster);
        NamespaceRosterProtocol.Validate(roster);
        if (encoded.Length < 4 + 2 + 8 + 32 + 16 + 4 + 8 + 1 + 8 + 8 + 4 + 2 + 2 + 64)
            throw new InvalidDataException("OFR2 frontier report is truncated.");
        int payloadLength = encoded.Length - BatchSignature.SignatureSize;
        byte[] payload = encoded[..payloadLength].ToArray();
        byte[] signature = encoded[payloadLength..].ToArray();
        var reader = new Reader(payload);
        if (!reader.Raw(4).AsSpan().SequenceEqual(Magic))
            throw new InvalidDataException("OFR2 magic is invalid.");

        int namespaceId = reader.U16();
        ulong rosterGeneration = reader.U64();
        byte[] rosterDigest = reader.Raw(NamespaceRosterProtocol.DigestSize);
        NodeId memberId = NodeId.From(reader.B128());
        uint memberEpoch = reader.U32();
        ulong reportSequence = reader.U64();
        int count = reader.U16();
        if (count > MaxEntries || reader.Remaining < 2 || count > (reader.Remaining - 2) / 28)
            throw new InvalidDataException("OFR2 entry count exceeds the bounded payload.");

        var entries = new List<AppliedOriginFrontierEntry>(count);
        for (int i = 0; i < count; i++)
        {
            entries.Add(new AppliedOriginFrontierEntry
            {
                OriginAuthorNodeId = NodeId.From(reader.B128()),
                OriginAuthorEpoch = reader.U32(),
                ContiguousAppliedOriginSequence = reader.U64()
            });
        }
        int issuedCount = reader.U16();
        if (issuedCount > MaxEntries || issuedCount > reader.Remaining / 24)
            throw new InvalidDataException("OFR2 issued-origin count exceeds the bounded payload.");
        var issuedOrigins = new List<IssuedOriginFrontierEntry>(issuedCount);
        for (int i = 0; i < issuedCount; i++)
        {
            issuedOrigins.Add(new IssuedOriginFrontierEntry
            {
                OriginAuthorEpoch = reader.U32(),
                IssuedOriginSequence = reader.U64(),
                MaxIssuedHlcWall = reader.U64(),
                MaxIssuedHlcLogical = reader.U32()
            });
        }
        if (!reader.Eof())
            throw new InvalidDataException("OFR2 contains trailing payload bytes.");

        var report = new AppliedOriginFrontierReport
        {
            NamespaceId = namespaceId,
            RosterGeneration = rosterGeneration,
            RosterDigest = rosterDigest,
            MemberNodeId = memberId,
            MemberTransportEpoch = memberEpoch,
            ReportSequence = reportSequence,
            Entries = entries,
            IssuedOrigins = issuedOrigins,
            Signature = signature
        };
        Validate(report);
        ValidateRosterBinding(report, roster);

        NamespaceRosterMember member = FindMember(roster, memberId);
        if (member.Status != NamespaceMemberStatus.Active ||
            member.TransportPeerEpoch != memberEpoch)
            throw new InvalidDataException("OFR2 member is not Active at the reported transport epoch.");
        if (!BatchSignature.Verify(member.PublicKey, BuildSigningPayload(payload), signature))
            throw new CryptographicException("OFR2 member signature is invalid.");
        return report;
    }

    public static bool Verify(AppliedOriginFrontierReport report, NamespaceRosterCertificate roster)
    {
        try
        {
            Validate(report);
            NamespaceRosterProtocol.Validate(roster);
            ValidateRosterBinding(report, roster);
            NamespaceRosterMember member = FindMember(roster, report.MemberNodeId);
            if (member.Status != NamespaceMemberStatus.Active ||
                member.TransportPeerEpoch != report.MemberTransportEpoch ||
                report.Signature.Length != BatchSignature.SignatureSize)
                return false;
            return BatchSignature.Verify(member.PublicKey,
                BuildSigningPayload(EncodePayload(report)), report.Signature);
        }
        catch (Exception exception) when (exception is ArgumentException or InvalidDataException)
        {
            return false;
        }
    }

    public static byte[] EncodePayload(AppliedOriginFrontierReport report)
    {
        Validate(report);
        var writer = new Writer();
        writer.Raw(Magic);
        writer.U16(checked((ushort)report.NamespaceId));
        writer.U64(report.RosterGeneration);
        writer.Raw(report.RosterDigest);
        writer.B128(report.MemberNodeId);
        writer.U32(report.MemberTransportEpoch);
        writer.U64(report.ReportSequence);
        writer.U8(report.FinalForAuthor ? (byte)1 : (byte)0);
        writer.U64(report.UnpublishedDraftCount);
        writer.U64(report.MaxUnpublishedDraftHlcWall);
        writer.U32(report.MaxUnpublishedDraftHlcLogical);
        writer.U16(checked((ushort)report.Entries.Count));
        foreach (AppliedOriginFrontierEntry entry in report.Entries)
        {
            writer.B128(entry.OriginAuthorNodeId);
            writer.U32(entry.OriginAuthorEpoch);
            writer.U64(entry.ContiguousAppliedOriginSequence);
        }
        writer.U16(checked((ushort)report.IssuedOrigins.Count));
        foreach (IssuedOriginFrontierEntry issued in report.IssuedOrigins)
        {
            writer.U32(issued.OriginAuthorEpoch);
            writer.U64(issued.IssuedOriginSequence);
            writer.U64(issued.MaxIssuedHlcWall);
            writer.U32(issued.MaxIssuedHlcLogical);
        }
        return writer.Value();
    }

    public static void Validate(AppliedOriginFrontierReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (report.NamespaceId <= 0 || report.NamespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(report), "NamespaceId must fit a non-zero u16.");
        if (report.RosterGeneration == 0 || report.RosterDigest is null ||
            report.RosterDigest.Length != NamespaceRosterProtocol.DigestSize)
            throw new ArgumentException("OFR2 requires a roster generation and 32-byte roster digest.", nameof(report));
        if (report.MemberNodeId.IsZero || report.MemberTransportEpoch == 0 || report.ReportSequence == 0)
            throw new ArgumentException("OFR2 member identity, transport epoch and report sequence must be non-zero.", nameof(report));
        ArgumentNullException.ThrowIfNull(report.Entries);
        if (report.Entries.Count > MaxEntries)
            throw new ArgumentOutOfRangeException(nameof(report), $"OFR2 entry count exceeds {MaxEntries}.");
        AppliedOriginFrontierEntry? previous = null;
        foreach (AppliedOriginFrontierEntry entry in report.Entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            if (entry.OriginAuthorNodeId.IsZero || entry.OriginAuthorEpoch == 0 ||
                entry.ContiguousAppliedOriginSequence == 0)
                throw new ArgumentException("OFR2 entries require non-zero author, epoch and contiguous sequence.", nameof(report));
            if (previous is not null && CompareOrigin(previous.OriginAuthorNodeId, previous.OriginAuthorEpoch,
                    entry.OriginAuthorNodeId, entry.OriginAuthorEpoch) >= 0)
                throw new ArgumentException("OFR2 entries must be strictly sorted and unique by origin author/epoch.", nameof(report));
            previous = entry;
        }
        ArgumentNullException.ThrowIfNull(report.IssuedOrigins);
        if (report.IssuedOrigins.Count > MaxEntries)
            throw new ArgumentOutOfRangeException(nameof(report), $"OFR2 issued-origin count exceeds {MaxEntries}.");
        uint previousIssuedEpoch = 0;
        foreach (IssuedOriginFrontierEntry issued in report.IssuedOrigins)
        {
            ArgumentNullException.ThrowIfNull(issued);
            if (issued.OriginAuthorEpoch == 0 || issued.IssuedOriginSequence == 0 ||
                issued.OriginAuthorEpoch <= previousIssuedEpoch)
                throw new ArgumentException("OFR2 issued-origin epochs must be non-zero, strictly sorted and unique.", nameof(report));
            AppliedOriginFrontierEntry? ownApplied = report.Entries.FirstOrDefault(entry =>
                entry.OriginAuthorNodeId == report.MemberNodeId &&
                entry.OriginAuthorEpoch == issued.OriginAuthorEpoch);
            if (ownApplied is null ||
                ownApplied.ContiguousAppliedOriginSequence < issued.IssuedOriginSequence)
                throw new ArgumentException("An author-issued watermark cannot exceed the member's own contiguous applied frontier.", nameof(report));
            previousIssuedEpoch = issued.OriginAuthorEpoch;
        }
        foreach (AppliedOriginFrontierEntry ownApplied in report.Entries.Where(entry =>
            entry.OriginAuthorNodeId == report.MemberNodeId))
        {
            IssuedOriginFrontierEntry? issued = report.IssuedOrigins.FirstOrDefault(entry =>
                entry.OriginAuthorEpoch == ownApplied.OriginAuthorEpoch);
            if (issued is null || issued.IssuedOriginSequence < ownApplied.ContiguousAppliedOriginSequence)
                throw new ArgumentException("A member report must include the issued watermark for each locally authored applied epoch.", nameof(report));
        }
        if (report.Signature is null || (report.Signature.Length != 0 &&
            report.Signature.Length != BatchSignature.SignatureSize))
            throw new ArgumentException("OFR2 signature must be empty or 64 bytes.", nameof(report));
    }

    internal static int CompareOrigin(NodeId leftId, uint leftEpoch, NodeId rightId, uint rightEpoch)
    {
        Span<byte> left = stackalloc byte[16];
        Span<byte> right = stackalloc byte[16];
        leftId.WriteTo(left);
        rightId.WriteTo(right);
        int order = left.SequenceCompareTo(right);
        return order != 0 ? order : leftEpoch.CompareTo(rightEpoch);
    }

    internal static NamespaceRosterMember FindMember(NamespaceRosterCertificate roster, NodeId memberId)
        => roster.Members.FirstOrDefault(m => m.NodeId == memberId)
           ?? throw new InvalidDataException("Member is not present in the accepted namespace roster.");

    private static void ValidateRosterBinding(AppliedOriginFrontierReport report, NamespaceRosterCertificate roster)
    {
        if (report.NamespaceId != roster.NamespaceId ||
            report.RosterGeneration != roster.RosterGeneration ||
            !report.RosterDigest.AsSpan().SequenceEqual(NamespaceRosterProtocol.ComputeDigest(roster)))
            throw new InvalidDataException("OFR2 is not bound to the accepted namespace roster generation/digest.");
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

/// <summary>
/// Canonical RCP2 authority-signed namespace minimum. A missing member/report/author
/// contributes zero; callers must compute the checkpoint from verified reports.
/// </summary>
public static class NamespaceRetentionCheckpointProtocol
{
    public const int MaxEntries = 4096;
    public const string SignDomain = "BIN-SYNC/RETENTION-CHECKPOINT/v2";
    private static ReadOnlySpan<byte> Magic => "RCP2"u8;

    public static NamespaceRetentionCheckpoint CreateFromReports(
        NamespaceRosterCertificate roster,
        ulong checkpointSequence,
        ReadOnlySpan<byte> previousCheckpointDigest,
        IReadOnlyList<AppliedOriginFrontierReport> reports,
        Guid? fullReplaceGeneration = null,
        ReadOnlySpan<byte> fullReplaceDigest = default,
        ulong fullReplaceHlcWallFloor = 0,
        uint fullReplaceHlcLogicalFloor = 0)
    {
        ArgumentNullException.ThrowIfNull(roster);
        ArgumentNullException.ThrowIfNull(reports);
        NamespaceRosterProtocol.Validate(roster);
        var active = roster.Members.Where(m => m.Status == NamespaceMemberStatus.Active).ToArray();
        var byMember = new Dictionary<NodeId, AppliedOriginFrontierReport>();
        foreach (AppliedOriginFrontierReport report in reports)
        {
            if (!OriginFrontierProtocol.Verify(report, roster))
                throw new InvalidDataException("Cannot compute retention checkpoint from an invalid frontier report.");
            if (!byMember.TryAdd(report.MemberNodeId, report))
                throw new InvalidDataException("Duplicate frontier report for one roster member.");
        }

        var origins = new SortedDictionary<OriginKey, (ulong Sequence, ulong HlcWall, uint HlcLogical)>();
        foreach (NamespaceRetiredOriginEpoch closure in roster.RetiredOriginEpochs)
        {
            origins.Add(new OriginKey(closure.OriginAuthorNodeId, closure.OriginAuthorEpoch),
                (closure.FinalIssuedOriginSequence, closure.MaxIssuedHlcWall, closure.MaxIssuedHlcLogical));
        }

        foreach (AppliedOriginFrontierReport report in byMember.Values)
        {
            foreach (IssuedOriginFrontierEntry issued in report.IssuedOrigins)
            {
                var key = new OriginKey(report.MemberNodeId, issued.OriginAuthorEpoch);
                var value = (issued.IssuedOriginSequence, issued.MaxIssuedHlcWall, issued.MaxIssuedHlcLogical);
                if (origins.TryGetValue(key, out var closureValue))
                {
                    if (closureValue != value)
                        throw new InvalidDataException("Active member issued-origin report conflicts with its authority-signed retired-epoch closure.");
                }
                else
                {
                    origins.Add(key, value);
                }
            }
        }

        if (fullReplaceGeneration.HasValue)
        {
            foreach (NamespaceRosterMember member in active)
            {
                if (!byMember.TryGetValue(member.NodeId, out AppliedOriginFrontierReport? report) ||
                    report.UnpublishedDraftCount != 0)
                    throw new InvalidDataException("FULL_REPLACE retention checkpoint requires every Active member to report zero unpublished local drafts.");
            }
            ulong requiredHlcWall = 0;
            uint requiredHlcLogical = 0;
            foreach (var issued in origins.Values)
            {
                if (issued.HlcWall > requiredHlcWall ||
                    (issued.HlcWall == requiredHlcWall && issued.HlcLogical > requiredHlcLogical))
                {
                    requiredHlcWall = issued.HlcWall;
                    requiredHlcLogical = issued.HlcLogical;
                }
            }
            if (fullReplaceHlcWallFloor < requiredHlcWall ||
                (fullReplaceHlcWallFloor == requiredHlcWall && fullReplaceHlcLogicalFloor < requiredHlcLogical))
                throw new InvalidDataException("FULL_REPLACE HLC floor does not cover every signed author-issued epoch high-watermark.");
        }

        var safe = new List<RetentionFrontierEntry>(origins.Count);
        foreach (var pair in origins)
        {
            bool allActiveApplied = true;
            foreach (NamespaceRosterMember member in active)
            {
                if (!byMember.TryGetValue(member.NodeId, out AppliedOriginFrontierReport? report))
                {
                    allActiveApplied = false;
                    break;
                }
                AppliedOriginFrontierEntry? applied = report.Entries.FirstOrDefault(entry =>
                    entry.OriginAuthorNodeId == pair.Key.Author &&
                    entry.OriginAuthorEpoch == pair.Key.Epoch);
                if (applied is null || applied.ContiguousAppliedOriginSequence < pair.Value.Sequence)
                {
                    allActiveApplied = false;
                    break;
                }
            }
            if (allActiveApplied && pair.Value.Sequence != 0)
            {
                safe.Add(new RetentionFrontierEntry
                {
                    OriginAuthorNodeId = pair.Key.Author,
                    OriginAuthorEpoch = pair.Key.Epoch,
                    SafeAppliedOriginSequence = pair.Value.Sequence
                });
            }
        }

        var checkpoint = new NamespaceRetentionCheckpoint
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            CheckpointSequence = checkpointSequence,
            PreviousCheckpointDigest = previousCheckpointDigest.ToArray(),
            SafeFrontier = safe,
            FullReplaceGeneration = fullReplaceGeneration,
            FullReplaceDigest = fullReplaceGeneration.HasValue ? fullReplaceDigest.ToArray() : Array.Empty<byte>(),
            FullReplaceHlcWallFloor = fullReplaceHlcWallFloor,
            FullReplaceHlcLogicalFloor = fullReplaceHlcLogicalFloor
        };
        Validate(checkpoint);
        return checkpoint;
    }

    public static byte[] Sign(NamespaceRetentionCheckpoint checkpoint, ReadOnlySpan<byte> authoritySeed)
    {
        Validate(checkpoint);
        byte[] payload = EncodePayload(checkpoint);
        byte[] signature = BatchSignature.Sign(authoritySeed, BuildSigningPayload(payload));
        byte[] encoded = new byte[payload.Length + BatchSignature.SignatureSize];
        payload.CopyTo(encoded, 0);
        signature.CopyTo(encoded, payload.Length);
        return encoded;
    }

    public static NamespaceRetentionCheckpoint DecodeAndVerify(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate roster, TrustStore trustStore)
    {
        ArgumentNullException.ThrowIfNull(roster);
        ArgumentNullException.ThrowIfNull(trustStore);
        NamespaceRosterProtocol.Validate(roster);
        if (encoded.Length < 4 + 2 + 8 + 32 + 8 + 32 + 2 + 1 + 64)
            throw new InvalidDataException("RCP1 checkpoint is truncated.");
        int payloadLength = encoded.Length - BatchSignature.SignatureSize;
        byte[] payload = encoded[..payloadLength].ToArray();
        byte[] signature = encoded[payloadLength..].ToArray();
        var reader = new Reader(payload);
        if (!reader.Raw(4).AsSpan().SequenceEqual(Magic))
            throw new InvalidDataException("RCP1 magic is invalid.");
        int namespaceId = reader.U16();
        ulong rosterGeneration = reader.U64();
        byte[] rosterDigest = reader.Raw(NamespaceRosterProtocol.DigestSize);
        ulong checkpointSequence = reader.U64();
        byte[] previousDigest = reader.Raw(NamespaceRosterProtocol.DigestSize);
        int count = reader.U16();
        if (count > MaxEntries || count > reader.Remaining / 28)
            throw new InvalidDataException("RCP1 frontier count exceeds the bounded payload.");
        var frontier = new List<RetentionFrontierEntry>(count);
        for (int i = 0; i < count; i++)
        {
            frontier.Add(new RetentionFrontierEntry
            {
                OriginAuthorNodeId = NodeId.From(reader.B128()),
                OriginAuthorEpoch = reader.U32(),
                SafeAppliedOriginSequence = reader.U64()
            });
        }

        byte hasGeneration = reader.U8();
        if (hasGeneration > 1)
            throw new InvalidDataException("RCP1 FULL_REPLACE generation flag is invalid.");
        Guid? generation = null;
        byte[] fullReplaceDigest = Array.Empty<byte>();
        ulong fullReplaceHlcWallFloor = 0;
        uint fullReplaceHlcLogicalFloor = 0;
        if (hasGeneration == 1)
        {
            generation = new Guid(reader.Raw(16), bigEndian: true);
            fullReplaceDigest = reader.Raw(NamespaceRosterProtocol.DigestSize);
            fullReplaceHlcWallFloor = reader.U64();
            fullReplaceHlcLogicalFloor = reader.U32();
        }
        if (!reader.Eof())
            throw new InvalidDataException("RCP1 contains trailing payload bytes.");

        var checkpoint = new NamespaceRetentionCheckpoint
        {
            NamespaceId = namespaceId,
            RosterGeneration = rosterGeneration,
            RosterDigest = rosterDigest,
            CheckpointSequence = checkpointSequence,
            PreviousCheckpointDigest = previousDigest,
            SafeFrontier = frontier,
            FullReplaceGeneration = generation,
            FullReplaceDigest = fullReplaceDigest,
            FullReplaceHlcWallFloor = fullReplaceHlcWallFloor,
            FullReplaceHlcLogicalFloor = fullReplaceHlcLogicalFloor,
            Signature = signature
        };
        Validate(checkpoint);
        if (checkpoint.NamespaceId != roster.NamespaceId ||
            checkpoint.RosterGeneration != roster.RosterGeneration ||
            !checkpoint.RosterDigest.AsSpan().SequenceEqual(NamespaceRosterProtocol.ComputeDigest(roster)))
            throw new InvalidDataException("RCP1 is not bound to the accepted namespace roster.");

        if (!trustStore.TryGet(roster.AuthorityNodeId, out TrustedPeer? trusted) ||
            trusted is null || trusted.Revoked || trusted.Mode != TrustMode.PreShared ||
            trusted.PublicKey.Length != BatchSignature.PublicKeySize)
            throw new CryptographicException("Namespace authority is not pinned with active PreShared trust.");
        if (!BatchSignature.Verify(trusted.PublicKey, BuildSigningPayload(payload), signature))
            throw new CryptographicException("RCP1 authority signature is invalid.");
        return checkpoint;
    }

    public static byte[] EncodePayload(NamespaceRetentionCheckpoint checkpoint)
    {
        Validate(checkpoint);
        var writer = new Writer();
        writer.Raw(Magic);
        writer.U16(checked((ushort)checkpoint.NamespaceId));
        writer.U64(checkpoint.RosterGeneration);
        writer.Raw(checkpoint.RosterDigest);
        writer.U64(checkpoint.CheckpointSequence);
        writer.Raw(checkpoint.PreviousCheckpointDigest);
        writer.U16(checked((ushort)checkpoint.SafeFrontier.Count));
        foreach (RetentionFrontierEntry entry in checkpoint.SafeFrontier)
        {
            writer.B128(entry.OriginAuthorNodeId);
            writer.U32(entry.OriginAuthorEpoch);
            writer.U64(entry.SafeAppliedOriginSequence);
        }
        writer.U8(checkpoint.FullReplaceGeneration.HasValue ? (byte)1 : (byte)0);
        if (checkpoint.FullReplaceGeneration.HasValue)
        {
            Span<byte> guid = stackalloc byte[16];
            checkpoint.FullReplaceGeneration.Value.TryWriteBytes(guid, bigEndian: true, out _);
            writer.Raw(guid);
            writer.Raw(checkpoint.FullReplaceDigest);
            writer.U64(checkpoint.FullReplaceHlcWallFloor);
            writer.U32(checkpoint.FullReplaceHlcLogicalFloor);
        }
        return writer.Value();
    }

    public static byte[] ComputeDigest(NamespaceRetentionCheckpoint checkpoint)
        => SHA256.HashData(BuildSigningPayload(EncodePayload(checkpoint)));

    public static void Validate(NamespaceRetentionCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        if (checkpoint.NamespaceId <= 0 || checkpoint.NamespaceId > ushort.MaxValue ||
            checkpoint.RosterGeneration == 0 || checkpoint.RosterDigest is null ||
            checkpoint.RosterDigest.Length != NamespaceRosterProtocol.DigestSize)
            throw new ArgumentException("RCP1 namespace/roster identity is invalid.", nameof(checkpoint));
        if (checkpoint.CheckpointSequence == 0 ||
            checkpoint.PreviousCheckpointDigest is null ||
            checkpoint.PreviousCheckpointDigest.Length != NamespaceRosterProtocol.DigestSize)
            throw new ArgumentException("RCP1 checkpoint sequence/digest is invalid.", nameof(checkpoint));
        if (checkpoint.CheckpointSequence == 1 && !IsZero(checkpoint.PreviousCheckpointDigest))
            throw new ArgumentException("The first RCP1 checkpoint must use a zero previous digest.", nameof(checkpoint));
        if (checkpoint.CheckpointSequence > 1 && IsZero(checkpoint.PreviousCheckpointDigest))
            throw new ArgumentException("A later RCP1 checkpoint must bind its previous digest.", nameof(checkpoint));
        ArgumentNullException.ThrowIfNull(checkpoint.SafeFrontier);
        if (checkpoint.SafeFrontier.Count > MaxEntries)
            throw new ArgumentOutOfRangeException(nameof(checkpoint), $"RCP1 frontier count exceeds {MaxEntries}.");
        RetentionFrontierEntry? previous = null;
        foreach (RetentionFrontierEntry entry in checkpoint.SafeFrontier)
        {
            ArgumentNullException.ThrowIfNull(entry);
            if (entry.OriginAuthorNodeId.IsZero || entry.OriginAuthorEpoch == 0 ||
                entry.SafeAppliedOriginSequence == 0)
                throw new ArgumentException("RCP1 entries require non-zero author, epoch and safe sequence.", nameof(checkpoint));
            if (previous is not null && OriginFrontierProtocol.CompareOrigin(
                    previous.OriginAuthorNodeId, previous.OriginAuthorEpoch,
                    entry.OriginAuthorNodeId, entry.OriginAuthorEpoch) >= 0)
                throw new ArgumentException("RCP1 entries must be strictly sorted and unique.", nameof(checkpoint));
            previous = entry;
        }
        if (checkpoint.FullReplaceGeneration.HasValue)
        {
            if (checkpoint.FullReplaceGeneration.Value == Guid.Empty ||
                checkpoint.FullReplaceDigest is null ||
                checkpoint.FullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize)
                throw new ArgumentException("RCP1 FULL_REPLACE binding requires a non-empty generation and 32-byte digest.", nameof(checkpoint));
        }
        else if (checkpoint.FullReplaceDigest is { Length: > 0 } ||
            checkpoint.FullReplaceHlcWallFloor != 0 || checkpoint.FullReplaceHlcLogicalFloor != 0)
        {
            throw new ArgumentException("RCP2 digest/HLC floor cannot be present without a FULL_REPLACE generation.", nameof(checkpoint));
        }
        if (checkpoint.Signature is null || (checkpoint.Signature.Length != 0 &&
            checkpoint.Signature.Length != BatchSignature.SignatureSize))
            throw new ArgumentException("RCP1 signature must be empty or 64 bytes.", nameof(checkpoint));
    }

    private static byte[] BuildSigningPayload(ReadOnlySpan<byte> payload)
    {
        byte[] domain = Encoding.UTF8.GetBytes(SignDomain);
        byte[] result = new byte[domain.Length + payload.Length];
        domain.CopyTo(result, 0);
        payload.CopyTo(result.AsSpan(domain.Length));
        return result;
    }

    private static bool IsZero(ReadOnlySpan<byte> value)
    {
        byte aggregate = 0;
        foreach (byte b in value) aggregate |= b;
        return aggregate == 0;
    }

    private readonly record struct OriginKey(NodeId Author, uint Epoch) : IComparable<OriginKey>
    {
        public int CompareTo(OriginKey other)
            => OriginFrontierProtocol.CompareOrigin(Author, Epoch, other.Author, other.Epoch);
    }
}
