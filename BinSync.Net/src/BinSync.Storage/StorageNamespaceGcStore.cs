using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using BinSync;
using Fld1.Storage.Engine;

namespace BinSync.Storage;

/// <summary>Explicit opt-in switches for the three independent physical-GC domains.</summary>
/// <remarks>Every switch defaults to false; enabling a switch never bypasses signed proof validation.</remarks>
public sealed record StorageNamespaceGcFeatureOptions(
    bool EnableTombstoneGc = false,
    bool EnableChangeIdGc = false,
    bool EnableReceiptGc = false);

/// <summary>
/// Durable signed namespace roster/frontier/checkpoint/installation state.
/// It shares the Storage transaction gate with StorageMetadataStore but uses
/// a separate key prefix, so it never duplicates or rewrites ChangeId rows.
/// </summary>
public sealed class StorageNamespaceGcStore : IMetadataNamespaceGcStore
{
    private const byte RosterKind = 1;
    private const byte FrontierKind = 2;
    private const byte CheckpointKind = 3;
    private const byte FullReplaceAckKind = 4;
    private const byte CurrentRosterKind = 5;
    private const byte BaselineReportKind = 6;
    private const int RootLength = 49;
    private readonly StorageDatabase _storage;
    private readonly object _gate;
    private readonly byte[] _root;
    private readonly byte[] _identityKey;
    private readonly byte[] _identityValue;
    private readonly StorageNamespaceGcFeatureOptions _featureOptions;

    public StorageNamespaceGcStore(
        StorageDatabase storage, string scopeId, NodeId localReplica,
        StorageNamespaceGcFeatureOptions? featureOptions = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _featureOptions = featureOptions ?? new StorageNamespaceGcFeatureOptions();
        ArgumentException.ThrowIfNullOrWhiteSpace(scopeId);
        if (localReplica.IsZero) throw new ArgumentException("Local replica identity must be non-zero.", nameof(localReplica));
        _gate = StorageDatabaseTransactionGate.For(storage);
        byte[] scope = Encoding.UTF8.GetBytes(scopeId);
        byte[] hash = SHA256.HashData(scope);
        _root = new byte[RootLength];
        _root[0] = 0x47; // namespace-GC metadata domain, distinct from the main Metadata Store (0x70)
        hash.CopyTo(_root, 1);
        localReplica.WriteTo(_root.AsSpan(33));
        _identityKey = Key(0, ReadOnlySpan<byte>.Empty);
        _identityValue = new byte[4 + 4 + scope.Length + 16];
        "NGC1"u8.CopyTo(_identityValue);
        BinaryPrimitives.WriteInt32LittleEndian(_identityValue.AsSpan(4), scope.Length);
        scope.CopyTo(_identityValue, 8);
        localReplica.WriteTo(_identityValue.AsSpan(8 + scope.Length));
    }

    internal bool IsBoundTo(StorageDatabase storage) => ReferenceEquals(_storage, storage);

    internal bool IsBoundTo(StorageDatabase storage, string scopeId, NodeId localReplica)
    {
        if (!ReferenceEquals(_storage, storage) ||
            string.IsNullOrWhiteSpace(scopeId) || localReplica.IsZero)
            return false;

        int scopeLength = Encoding.UTF8.GetByteCount(scopeId);
        if (_identityValue.Length != checked(8 + scopeLength + 16) ||
            !_identityValue.AsSpan(0, 4).SequenceEqual("NGC1"u8) ||
            BinaryPrimitives.ReadInt32LittleEndian(_identityValue.AsSpan(4)) != scopeLength)
            return false;

        byte[] scope = Encoding.UTF8.GetBytes(scopeId);
        if (!_identityValue.AsSpan(8, scopeLength).SequenceEqual(scope))
            return false;

        Span<byte> local = stackalloc byte[16];
        localReplica.WriteTo(local);
        return _identityValue.AsSpan(8 + scopeLength, 16).SequenceEqual(local);
    }

    /// <summary>Whether the caller may run proof-gated physical tombstone compaction.</summary>
    public bool IsTombstoneGcProofEnabled => _featureOptions.EnableTombstoneGc;

    /// <summary>Whether the caller may run proof-gated ChangeId ledger compaction.</summary>
    public bool IsChangeIdGcProofEnabled => _featureOptions.EnableChangeIdGc;

    /// <summary>Whether the caller may run proof-gated receipt compaction.</summary>
    public bool IsReceiptGcProofEnabled => _featureOptions.EnableReceiptGc;

    public NamespaceRosterCertificate AcceptNamespaceRoster(ReadOnlySpan<byte> encoded, TrustStore trustStore)
    {
        ArgumentNullException.ThrowIfNull(trustStore);
        byte[] owned = encoded.ToArray();
        NamespaceRosterCertificate candidate = NamespaceRosterProtocol.DecodeAndVerify(owned, trustStore);
        lock (_gate)
        {
            if (TryReadCurrentRosterCore(trustStore, candidate.NamespaceId, out NamespaceRosterCertificate? prior) && prior is not null)
            {
                byte[] priorDigest = NamespaceRosterProtocol.ComputeDigest(prior);
                if (candidate.AuthorityNodeId != prior.AuthorityNodeId)
                    throw new InvalidDataException("Namespace authority identity cannot change inside an existing namespace.");
                byte[] candidateDigest = NamespaceRosterProtocol.ComputeDigest(candidate);
                if (candidate.RosterGeneration < prior.RosterGeneration)
                    throw new InvalidDataException("Namespace roster rollback is not allowed.");
                if (candidate.RosterGeneration == prior.RosterGeneration)
                {
                    if (!candidateDigest.AsSpan().SequenceEqual(priorDigest))
                        throw new InvalidDataException("Namespace roster generation fork detected.");
                    return prior;
                }
                if (candidate.RosterGeneration == prior.RosterGeneration + 1 &&
                    !candidate.PreviousRosterDigest.AsSpan().SequenceEqual(priorDigest))
                    throw new InvalidDataException("Next namespace roster does not link to the accepted previous digest.");
                bool skippedGenerations = candidate.RosterGeneration - prior.RosterGeneration > 1;
                ValidateMembershipTransition(prior, candidate, trustStore, skippedGenerations);
            }

            byte[] rosterKey = RosterKey(candidate.NamespaceId, candidate.RosterGeneration);
            byte[] pointerKey = CurrentRosterKey(candidate.NamespaceId);
            byte[] digest = NamespaceRosterProtocol.ComputeDigest(candidate);
            byte[] pointer = new byte[8 + NamespaceRosterProtocol.DigestSize];
            BinaryPrimitives.WriteUInt64LittleEndian(pointer, candidate.RosterGeneration);
            digest.CopyTo(pointer, 8);
            WriteRows((rosterKey, owned), (pointerKey, pointer));
            return candidate;
        }
    }

    public bool TryReadNamespaceRoster(int namespaceId, TrustStore trustStore, out NamespaceRosterCertificate? roster)
    {
        ArgumentNullException.ThrowIfNull(trustStore);
        if (namespaceId <= 0 || namespaceId > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(namespaceId));
        lock (_gate) return TryReadCurrentRosterCore(trustStore, namespaceId, out roster);
    }

    public AppliedOriginFrontierReport AcceptOriginFrontier(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster)
    {
        byte[] owned = encoded.ToArray();
        AppliedOriginFrontierReport report = OriginFrontierProtocol.DecodeAndVerify(owned, acceptedRoster);
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            byte[] key = FrontierKey(report.NamespaceId, report.RosterGeneration, report.MemberNodeId);
            byte[]? priorBytes = ReadValue(key);
            if (priorBytes is not null)
            {
                AppliedOriginFrontierReport prior = OriginFrontierProtocol.DecodeAndVerify(priorBytes, acceptedRoster);
                if (prior.FinalForAuthor && report.ReportSequence > prior.ReportSequence)
                    throw new InvalidDataException("A final author frontier is terminal; no later origin report is accepted.");
                if (report.ReportSequence < prior.ReportSequence)
                    throw new InvalidDataException("Applied-origin frontier report sequence regressed.");
                if (report.ReportSequence == prior.ReportSequence)
                {
                    if (!owned.AsSpan().SequenceEqual(priorBytes))
                        throw new InvalidDataException("Applied-origin frontier report sequence fork detected.");
                    return prior;
                }

                var nextEntries = report.Entries.ToDictionary(
                    entry => (entry.OriginAuthorNodeId, entry.OriginAuthorEpoch),
                    entry => entry.ContiguousAppliedOriginSequence);
                foreach (AppliedOriginFrontierEntry oldEntry in prior.Entries)
                {
                    if (!nextEntries.TryGetValue(
                            (oldEntry.OriginAuthorNodeId, oldEntry.OriginAuthorEpoch), out ulong nextSequence) ||
                        nextSequence < oldEntry.ContiguousAppliedOriginSequence)
                        throw new InvalidDataException("Applied-origin frontier regressed or dropped a previously reported author epoch.");
                }

                // Issued-origin maxima are GC authority too: keeping the applied
                // vector monotonic is insufficient if a member can lower its issued
                // sequence/HLC claim in a later signed report.
                var nextIssuedByEpoch = report.IssuedOrigins.ToDictionary(
                    entry => entry.OriginAuthorEpoch);
                foreach (IssuedOriginFrontierEntry oldIssued in prior.IssuedOrigins)
                {
                    if (!nextIssuedByEpoch.TryGetValue(oldIssued.OriginAuthorEpoch,
                            out IssuedOriginFrontierEntry? nextIssued) ||
                        nextIssued.IssuedOriginSequence < oldIssued.IssuedOriginSequence ||
                        nextIssued.MaxIssuedHlcWall < oldIssued.MaxIssuedHlcWall ||
                        (nextIssued.MaxIssuedHlcWall == oldIssued.MaxIssuedHlcWall &&
                         nextIssued.MaxIssuedHlcLogical < oldIssued.MaxIssuedHlcLogical))
                        throw new InvalidDataException(
                            "Issued-origin sequence or HLC maximum regressed or dropped a previously reported author epoch.");
                }
            }
            WriteRows((key, owned));
            return report;
        }
    }

    public IReadOnlyList<AppliedOriginFrontierReport> ReadOriginFrontiers(NamespaceRosterCertificate acceptedRoster)
    {
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            return ReadOriginFrontiersCore(acceptedRoster);
        }
    }

    public NamespaceRetentionCheckpoint AcceptRetentionCheckpoint(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster, TrustStore trustStore)
    {
        byte[] owned = encoded.ToArray();
        NamespaceRetentionCheckpoint checkpoint =
            NamespaceRetentionCheckpointProtocol.DecodeAndVerify(owned, acceptedRoster, trustStore);
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            ValidateCheckpointFrontier(checkpoint, acceptedRoster);
            if (checkpoint.FullReplaceGeneration.HasValue &&
                (!TryGetCommonInstalledHlcFloorCore(acceptedRoster,
                    checkpoint.FullReplaceGeneration.Value, checkpoint.FullReplaceDigest,
                    out ulong installedWall, out uint installedLogical) ||
                 installedWall != checkpoint.FullReplaceHlcWallFloor ||
                 installedLogical != checkpoint.FullReplaceHlcLogicalFloor))
                throw new InvalidDataException("RCP2 FULL_REPLACE generation/HLC floor must match every Active member's signed FRA2 ACK.");

            byte[] key = CheckpointKey(checkpoint.NamespaceId, checkpoint.RosterGeneration);
            byte[]? priorBytes = ReadValue(key);
            if (priorBytes is not null)
            {
                NamespaceRetentionCheckpoint prior =
                    NamespaceRetentionCheckpointProtocol.DecodeAndVerify(priorBytes, acceptedRoster, trustStore);
                if (checkpoint.CheckpointSequence < prior.CheckpointSequence)
                    throw new InvalidDataException("Retention checkpoint sequence regressed.");
                if (checkpoint.CheckpointSequence == prior.CheckpointSequence)
                {
                    if (!NamespaceRetentionCheckpointProtocol.ComputeDigest(checkpoint)
                        .AsSpan().SequenceEqual(NamespaceRetentionCheckpointProtocol.ComputeDigest(prior)))
                        throw new InvalidDataException("Retention checkpoint sequence fork detected.");
                    return prior;
                }
                var nextFrontier = checkpoint.SafeFrontier.ToDictionary(
                    entry => (entry.OriginAuthorNodeId, entry.OriginAuthorEpoch),
                    entry => entry.SafeAppliedOriginSequence);
                foreach (RetentionFrontierEntry oldEntry in prior.SafeFrontier)
                {
                    if (!nextFrontier.TryGetValue(
                            (oldEntry.OriginAuthorNodeId, oldEntry.OriginAuthorEpoch), out ulong nextSequence) ||
                        nextSequence < oldEntry.SafeAppliedOriginSequence)
                        throw new InvalidDataException("Retention checkpoint safe frontier regressed.");
                }
                if (checkpoint.CheckpointSequence == prior.CheckpointSequence + 1 &&
                    !checkpoint.PreviousCheckpointDigest.AsSpan()
                        .SequenceEqual(NamespaceRetentionCheckpointProtocol.ComputeDigest(prior)))
                    throw new InvalidDataException("Retention checkpoint does not link to the accepted previous checkpoint.");
            }

            WriteRows((key, owned));
            return checkpoint;
        }
    }

    public bool TryReadRetentionCheckpoint(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        out NamespaceRetentionCheckpoint? checkpoint)
    {
        ArgumentNullException.ThrowIfNull(trustStore);
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            checkpoint = TryReadRetentionCheckpointCore(acceptedRoster, trustStore);
            return checkpoint is not null;
        }
    }

    public FullReplaceInstalledAck AcceptFullReplaceInstalledAck(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster)
    {
        byte[] owned = encoded.ToArray();
        FullReplaceInstalledAck acknowledgement =
            FullReplaceInstalledAckProtocol.DecodeAndVerify(owned, acceptedRoster);
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            byte[] key = FullReplaceAckKey(acknowledgement.NamespaceId,
                acknowledgement.RosterGeneration, acknowledgement.Generation, acknowledgement.MemberNodeId);
            byte[]? prior = ReadValue(key);
            if (prior is not null)
            {
                if (!prior.AsSpan().SequenceEqual(owned))
                    throw new InvalidDataException("Member equivocated about FULL_REPLACE installation for one roster generation.");
                return acknowledgement;
            }
            WriteRows((key, owned));
            return acknowledgement;
        }
    }

    public MetadataBaselineReport AcceptMetadataBaselineReport(
        ReadOnlySpan<byte> encoded, NamespaceRosterCertificate acceptedRoster,
        Guid expectedGeneration, ReadOnlySpan<byte> expectedSnapshotDigest)
    {
        byte[] owned = encoded.ToArray();
        MetadataBaselineReport report = MetadataBaselineReportProtocol.DecodeAndVerify(
            owned, acceptedRoster, expectedGeneration, expectedSnapshotDigest);
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            byte[] ackBytes = ReadValue(FullReplaceAckKey(
                report.NamespaceId, report.RosterGeneration, expectedGeneration, report.MemberNodeId))
                ?? throw new InvalidDataException("MBR1 cannot be accepted before the member's durable FULL_REPLACE installation ACK.");
            FullReplaceInstalledAck ack = FullReplaceInstalledAckProtocol.DecodeAndVerify(ackBytes, acceptedRoster);
            if (!ack.Installed || ack.MemberTransportEpoch != report.MemberTransportEpoch ||
                ack.Generation != expectedGeneration ||
                !ack.Digest.AsSpan().SequenceEqual(expectedSnapshotDigest) ||
                ack.HlcWallFloor != report.HlcWallFloor || ack.HlcLogicalFloor != report.HlcLogicalFloor)
                throw new InvalidDataException("MBR1 report does not match the member's exact installed FULL_REPLACE ACK.");

            byte[] key = BaselineReportKey(report.NamespaceId, report.RosterGeneration, report.MemberNodeId);
            byte[]? priorBytes = ReadValue(key);
            if (priorBytes is not null)
            {
                if (priorBytes.Length < 94 + BatchSignature.SignatureSize ||
                    !priorBytes.AsSpan(0, 4).SequenceEqual("MBR1"u8))
                    throw new InvalidDataException("Stored MBR1 report is corrupt.");
                Guid priorGeneration = new(priorBytes.AsSpan(46, 16), bigEndian: true);
                byte[] priorSnapshotDigest = priorBytes.AsSpan(62, MetadataBaselineReportProtocol.DigestSize).ToArray();
                MetadataBaselineReport prior = MetadataBaselineReportProtocol.DecodeAndVerify(
                    priorBytes, acceptedRoster, priorGeneration, priorSnapshotDigest);
                if (!key.AsSpan().SequenceEqual(BaselineReportKey(
                        prior.NamespaceId, prior.RosterGeneration, prior.MemberNodeId)))
                    throw new InvalidDataException("Stored MBR1 report key/value identity mismatch.");

                if (report.ReportSequence < prior.ReportSequence)
                    throw new InvalidDataException("MBR1 report sequence regressed.");
                if (report.ReportSequence == prior.ReportSequence)
                {
                    if (!owned.AsSpan().SequenceEqual(priorBytes))
                        throw new InvalidDataException("MBR1 report sequence fork detected.");
                    return prior;
                }
                ValidateBaselineReportProgress(prior, report);
            }

            WriteRows((key, owned));
            return report;
        }
    }

    public IReadOnlyList<MetadataBaselineReport> ReadMetadataBaselineReports(
        NamespaceRosterCertificate acceptedRoster, Guid generation, ReadOnlySpan<byte> snapshotDigest)
    {
        if (generation == Guid.Empty || snapshotDigest.Length != MetadataBaselineReportProtocol.DigestSize)
            throw new ArgumentException("A FULL_REPLACE generation and 32-byte snapshot digest are required.");
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            byte[] prefix = BaselineReportPrefix(acceptedRoster.NamespaceId, acceptedRoster.RosterGeneration);
            var rows = Scan(prefix);
            var reports = new List<MetadataBaselineReport>(rows.Count);
            foreach (StorageEntry row in rows)
            {
                if (row.Value.Length < 118 + BatchSignature.SignatureSize ||
                    !row.Value.AsSpan(0, 4).SequenceEqual("MBR1"u8))
                    throw new InvalidDataException("Stored MBR1 report is corrupt.");
                Guid storedGeneration = new(row.Value.AsSpan(46, 16), bigEndian: true);
                byte[] storedDigest = row.Value.AsSpan(62, MetadataBaselineReportProtocol.DigestSize).ToArray();
                MetadataBaselineReport report = MetadataBaselineReportProtocol.DecodeAndVerify(
                    row.Value, acceptedRoster, storedGeneration, storedDigest);
                if (row.Key.Length != prefix.Length + 16 ||
                    !row.Key.AsSpan(prefix.Length).SequenceEqual(report.MemberNodeId.ToArray()))
                    throw new InvalidDataException("MBR1 report key/value identity mismatch.");

                byte[] ackBytes = ReadValue(FullReplaceAckKey(
                    report.NamespaceId, report.RosterGeneration, report.FullReplaceGeneration, report.MemberNodeId))
                    ?? throw new InvalidDataException("Stored MBR1 report has no durable FULL_REPLACE installation ACK.");
                FullReplaceInstalledAck ack = FullReplaceInstalledAckProtocol.DecodeAndVerify(ackBytes, acceptedRoster);
                if (!ack.Installed || ack.MemberTransportEpoch != report.MemberTransportEpoch ||
                    ack.Generation != report.FullReplaceGeneration ||
                    !ack.Digest.AsSpan().SequenceEqual(report.SnapshotDigest) ||
                    ack.HlcWallFloor != report.HlcWallFloor || ack.HlcLogicalFloor != report.HlcLogicalFloor)
                    throw new InvalidDataException("Stored MBR1 report no longer matches its installed FULL_REPLACE ACK.");

                if (storedGeneration == generation && storedDigest.AsSpan().SequenceEqual(snapshotDigest))
                    reports.Add(report);
            }
            reports.Sort((a, b) => CompareNodeIds(a.MemberNodeId, b.MemberNodeId));
            return reports;
        }
    }

    private static void ValidateBaselineReportProgress(
        MetadataBaselineReport prior, MetadataBaselineReport next)
    {
        if (next.ProviderChangeFeedWatermark < prior.ProviderChangeFeedWatermark ||
            next.HlcWallFloor < prior.HlcWallFloor ||
            (next.HlcWallFloor == prior.HlcWallFloor && next.HlcLogicalFloor < prior.HlcLogicalFloor))
            throw new InvalidDataException("MBR1 provider watermark or HLC floor regressed.");

        var nextFeeds = next.FeedEntries.ToDictionary(
            entry => (entry.TargetPeer, entry.TargetTransportEpoch, entry.AuthorStream));
        foreach (MetadataBaselineFeedEntry oldEntry in prior.FeedEntries)
        {
            if (!nextFeeds.TryGetValue(
                    (oldEntry.TargetPeer, oldEntry.TargetTransportEpoch, oldEntry.AuthorStream),
                    out MetadataBaselineFeedEntry? current) ||
                current.FeedKind != oldEntry.FeedKind ||
                current.IssuerEpoch < oldEntry.IssuerEpoch ||
                current.AbsoluteFeedOffset < oldEntry.AbsoluteFeedOffset ||
                (current.IssuerEpoch > oldEntry.IssuerEpoch &&
                 current.DigestAnchor.Any(value => value != 0)))
                throw new InvalidDataException("MBR1 feed vector regressed, dropped a stream, or changed an equal-offset digest anchor.");
        }

        var nextReceives = next.ReceiveEntries.ToDictionary(
            entry => (entry.SenderPeer, entry.SenderTransportEpoch, entry.AuthorStream, entry.IssuerEpoch));
        foreach (MetadataBaselineReceiveEntry oldEntry in prior.ReceiveEntries)
        {
            if (!nextReceives.TryGetValue(
                    (oldEntry.SenderPeer, oldEntry.SenderTransportEpoch, oldEntry.AuthorStream, oldEntry.IssuerEpoch),
                    out MetadataBaselineReceiveEntry? current) ||
                current.ReceiveCursor < oldEntry.ReceiveCursor ||
                (current.ReceiveCursor == oldEntry.ReceiveCursor &&
                 !current.LastBatchDigest.AsSpan().SequenceEqual(oldEntry.LastBatchDigest)))
                throw new InvalidDataException("MBR1 receive vector regressed, dropped a stream, or changed an equal-cursor digest anchor.");
        }
    }

    public IReadOnlyList<FullReplaceInstalledAck> ReadFullReplaceInstalledAcks(
        NamespaceRosterCertificate acceptedRoster, Guid generation)
    {
        if (generation == Guid.Empty) throw new ArgumentException("FULL_REPLACE generation must be non-empty.", nameof(generation));
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            byte[] prefix = FullReplaceAckPrefix(acceptedRoster.NamespaceId, acceptedRoster.RosterGeneration, generation);
            var rows = Scan(prefix);
            var result = new List<FullReplaceInstalledAck>(rows.Count);
            foreach (StorageEntry row in rows)
            {
                FullReplaceInstalledAck ack = FullReplaceInstalledAckProtocol.DecodeAndVerify(row.Value, acceptedRoster);
                if (row.Key.Length != prefix.Length + 16 ||
                    !row.Key.AsSpan(prefix.Length).SequenceEqual(ack.MemberNodeId.ToArray()) ||
                    ack.Generation != generation)
                    throw new InvalidDataException("FULL_REPLACE ACK key/value identity mismatch.");
                result.Add(ack);
            }
            result.Sort((a, b) => CompareNodeIds(a.MemberNodeId, b.MemberNodeId));
            return result;
        }
    }

    public bool AreAllActiveMembersInstalled(
        NamespaceRosterCertificate acceptedRoster, Guid generation, ReadOnlySpan<byte> digest)
    {
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            return AreAllActiveMembersInstalledCore(acceptedRoster, generation, digest);
        }
    }

    public bool IsOriginCoveredByRetentionCheckpoint(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        NodeId originAuthor, uint originEpoch, ulong originSequence)
    {
        if (originAuthor.IsZero || originEpoch == 0 || originSequence == 0) return false;
        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            NamespaceRetentionCheckpoint? checkpoint = TryReadRetentionCheckpointCore(acceptedRoster, trustStore);
            if (checkpoint is null) return false;
            RetentionFrontierEntry? safe = checkpoint.SafeFrontier.FirstOrDefault(entry =>
                entry.OriginAuthorNodeId == originAuthor && entry.OriginAuthorEpoch == originEpoch);
            return safe is not null && originSequence <= safe.SafeAppliedOriginSequence;
        }
    }

    internal TombstoneGcProofSnapshot? TryCreateTombstoneGcProofSnapshot(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        Guid fullReplaceGeneration, ReadOnlySpan<byte> fullReplaceDigest)
    {
        ArgumentNullException.ThrowIfNull(acceptedRoster);
        ArgumentNullException.ThrowIfNull(trustStore);
        if (fullReplaceGeneration == Guid.Empty ||
            fullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize)
            return null;

        lock (_gate)
        {
            EnsureCurrentRoster(acceptedRoster);
            NamespaceRetentionCheckpoint? checkpoint =
                TryReadRetentionCheckpointCore(acceptedRoster, trustStore);
            if (checkpoint is null ||
                checkpoint.FullReplaceGeneration != fullReplaceGeneration ||
                checkpoint.FullReplaceDigest is null ||
                checkpoint.FullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize ||
                !checkpoint.FullReplaceDigest.AsSpan().SequenceEqual(fullReplaceDigest) ||
                !TryGetCommonInstalledHlcFloorCore(
                    acceptedRoster, fullReplaceGeneration, fullReplaceDigest,
                    out ulong installedWall, out uint installedLogical) ||
                installedWall != checkpoint.FullReplaceHlcWallFloor ||
                installedLogical != checkpoint.FullReplaceHlcLogicalFloor)
                return null;

            NamespaceRosterMember[] activeMembers = acceptedRoster.Members
                .Where(member => member.Status == NamespaceMemberStatus.Active).ToArray();
            if (activeMembers.Length == 0)
                return null;

            IReadOnlyList<AppliedOriginFrontierReport> reports =
                ReadOriginFrontiersCore(acceptedRoster);
            var reportsByMember = reports.ToDictionary(report => report.MemberNodeId);
            foreach (NamespaceRosterMember member in activeMembers)
            {
                if (!reportsByMember.TryGetValue(member.NodeId, out AppliedOriginFrontierReport? report) ||
                    report.UnpublishedDraftCount != 0)
                    return null;
            }

            var proofs = new Dictionary<(NodeId Author, uint Epoch), TombstoneGcOriginProof>();
            foreach (NamespaceRetiredOriginEpoch closure in acceptedRoster.RetiredOriginEpochs)
            {
                proofs[(closure.OriginAuthorNodeId, closure.OriginAuthorEpoch)] =
                    new TombstoneGcOriginProof(
                        closure.FinalIssuedOriginSequence,
                        closure.MaxIssuedHlcWall,
                        closure.MaxIssuedHlcLogical,
                        0,
                        false);
            }

            foreach (NamespaceRosterMember member in activeMembers)
            {
                AppliedOriginFrontierReport report = reportsByMember[member.NodeId];
                foreach (IssuedOriginFrontierEntry issued in report.IssuedOrigins)
                {
                    var key = (member.NodeId, issued.OriginAuthorEpoch);
                    if (!proofs.ContainsKey(key))
                        proofs.Add(key, new TombstoneGcOriginProof(
                            issued.IssuedOriginSequence,
                            issued.MaxIssuedHlcWall,
                            issued.MaxIssuedHlcLogical,
                            0,
                            false));
                }
            }

            var safeFrontiers = checkpoint.SafeFrontier.ToDictionary(
                entry => (entry.OriginAuthorNodeId, entry.OriginAuthorEpoch),
                entry => entry.SafeAppliedOriginSequence);
            var appliedByMember = new Dictionary<NodeId, Dictionary<(NodeId Author, uint Epoch), ulong>>();
            foreach (NamespaceRosterMember member in activeMembers)
            {
                AppliedOriginFrontierReport report = reportsByMember[member.NodeId];
                var applied = new Dictionary<(NodeId Author, uint Epoch), ulong>();
                foreach (AppliedOriginFrontierEntry entry in report.Entries)
                    applied[(entry.OriginAuthorNodeId, entry.OriginAuthorEpoch)] =
                        entry.ContiguousAppliedOriginSequence;
                appliedByMember.Add(member.NodeId, applied);
            }

            foreach (var pair in proofs.ToArray())
            {
                ulong safe = safeFrontiers.TryGetValue(pair.Key, out ulong value) ? value : 0;
                bool allApplied = true;
                foreach (NamespaceRosterMember member in activeMembers)
                {
                    if (!appliedByMember[member.NodeId].TryGetValue(pair.Key, out ulong applied) ||
                        applied < pair.Value.FinalIssuedSequence)
                    {
                        allApplied = false;
                        break;
                    }
                }
                proofs[pair.Key] = pair.Value with
                {
                    SafeAppliedSequence = safe,
                    AllActiveMembersApplied = allApplied
                };
            }

            return new TombstoneGcProofSnapshot(
                checkpoint.FullReplaceHlcWallFloor,
                checkpoint.FullReplaceHlcLogicalFloor,
                proofs);
        }
    }

    public bool HasProtocolTombstoneGcProof(
        NamespaceRosterCertificate acceptedRoster, TrustStore trustStore,
        NodeId originAuthor, uint originEpoch, ulong originSequence,
        ulong tombstoneHlcWall, uint tombstoneHlcLogical,
        Guid fullReplaceGeneration, ReadOnlySpan<byte> fullReplaceDigest)
    {
        TombstoneGcProofSnapshot? snapshot = TryCreateTombstoneGcProofSnapshot(
            acceptedRoster, trustStore, fullReplaceGeneration, fullReplaceDigest);
        return snapshot is not null && snapshot.HasProof(
            originAuthor, originEpoch, originSequence, tombstoneHlcWall, tombstoneHlcLogical);
    }

    private bool TryReadCurrentRosterCore(TrustStore trustStore, int namespaceId, out NamespaceRosterCertificate? roster)
    {
        roster = null;
        byte[]? pointer = ReadValue(CurrentRosterKey(namespaceId));
        if (pointer is null)
        {
            Span<byte> namespacePrefix = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(namespacePrefix, checked((ushort)namespaceId));
            if (Scan(Key(RosterKind, namespacePrefix)).Count != 0)
                throw new InvalidDataException("Namespace roster certificates exist without a current-roster pointer.");
            return false;
        }
        if (pointer.Length != 8 + NamespaceRosterProtocol.DigestSize)
            throw new InvalidDataException("Current namespace roster pointer is corrupt.");
        ulong generation = BinaryPrimitives.ReadUInt64LittleEndian(pointer);
        byte[]? encoded = ReadValue(RosterKey(namespaceId, generation));
        if (encoded is null)
            throw new InvalidDataException("Current namespace roster certificate is missing.");
        roster = NamespaceRosterProtocol.DecodeAndVerify(encoded, trustStore);
        if (roster.NamespaceId != namespaceId || roster.RosterGeneration != generation ||
            !NamespaceRosterProtocol.ComputeDigest(roster).AsSpan().SequenceEqual(pointer.AsSpan(8)))
            throw new InvalidDataException("Current namespace roster pointer/certificate mismatch.");
        return true;
    }

    private void ValidateMembershipTransition(
        NamespaceRosterCertificate prior, NamespaceRosterCertificate candidate,
        TrustStore trustStore, bool skippedGenerations)
    {
        var incoming = candidate.Members.ToDictionary(member => member.NodeId);
        foreach (NamespaceRosterMember oldMember in prior.Members)
        {
            if (!incoming.TryGetValue(oldMember.NodeId, out NamespaceRosterMember? next))
                throw new InvalidDataException("A roster update must retain removed members as explicit Retired entries.");
            if (!oldMember.PublicKey.AsSpan().SequenceEqual(next.PublicKey))
                throw new InvalidDataException("Namespace member key rotation requires a separate authenticated key-rotation transition.");

            switch (oldMember.Status)
            {
                case NamespaceMemberStatus.Active:
                    if (next.Status == NamespaceMemberStatus.Joining)
                    {
                        if (!skippedGenerations ||
                            next.TransportPeerEpoch <= oldMember.TransportPeerEpoch)
                            throw new InvalidDataException("An Active member can return to Joining only across a signed roster-generation gap and with an advanced transport epoch.");
                        // The pinned authority attests that the omitted generations
                        // contain retirement and any required rejoin preconditions.
                        ValidatePreservedRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                    }
                    if (next.Status == NamespaceMemberStatus.Active &&
                        next.TransportPeerEpoch < oldMember.TransportPeerEpoch)
                        throw new InvalidDataException("Active member transport epoch regressed.");
                    if (next.Status == NamespaceMemberStatus.Active)
                        ValidatePreservedRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                    if (next.Status == NamespaceMemberStatus.Retired)
                    {
                        if (next.RetiredThroughEpoch < oldMember.TransportPeerEpoch)
                            throw new InvalidDataException("RetiredThroughEpoch does not cover the previous Active epoch.");
                        if (skippedGenerations)
                            ValidatePreservedRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                        else
                            ValidateRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                    }
                    break;

                case NamespaceMemberStatus.Joining:
                    if (next.Status == NamespaceMemberStatus.Active)
                    {
                        if (skippedGenerations)
                        {
                            if (next.TransportPeerEpoch < oldMember.TransportPeerEpoch)
                                throw new InvalidDataException("Joining member transport epoch regressed across a roster-generation gap.");
                            // The pinned authority signature attests that the omitted
                            // roster generations contain the FULL_REPLACE installed ACK.
                        }
                        else if (next.TransportPeerEpoch != oldMember.TransportPeerEpoch ||
                                 !HasInstalledAckForMember(prior, oldMember, trustStore))
                        {
                            throw new InvalidDataException("Joining member cannot become Active before ACKing the current FULL_REPLACE generation.");
                        }
                        ValidatePreservedRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                    }
                    else if (next.Status == NamespaceMemberStatus.Joining &&
                        next.TransportPeerEpoch < oldMember.TransportPeerEpoch)
                    {
                        throw new InvalidDataException("Joining member transport epoch regressed.");
                    }
                    else if (next.Status == NamespaceMemberStatus.Joining)
                    {
                        ValidatePreservedRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                    }
                    else if (next.Status == NamespaceMemberStatus.Retired)
                    {
                        if (next.RetiredThroughEpoch < oldMember.TransportPeerEpoch)
                            throw new InvalidDataException("RetiredThroughEpoch does not cover the previous Joining epoch.");
                        ValidatePreservedRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                    }
                    break;

                case NamespaceMemberStatus.Retired:
                    if (next.Status == NamespaceMemberStatus.Active)
                    {
                        if (!skippedGenerations ||
                            next.TransportPeerEpoch <= oldMember.RetiredThroughEpoch)
                            throw new InvalidDataException("A Retired member may become Active only across a signed roster-generation gap, after advancing beyond its retired epoch fence.");
                        // The pinned authority attests that the omitted generations
                        // contain Joining, FULL_REPLACE installation and promotion.
                    }
                    if (next.Status == NamespaceMemberStatus.Joining &&
                        next.TransportPeerEpoch <= oldMember.RetiredThroughEpoch)
                        throw new InvalidDataException("Rejoining member must advance beyond its retired transport epoch fence.");
                    if (next.Status == NamespaceMemberStatus.Retired &&
                        next.RetiredThroughEpoch < oldMember.RetiredThroughEpoch)
                        throw new InvalidDataException("Retired transport epoch fence regressed.");
                    ValidatePreservedRetiredOriginClosure(prior, candidate, oldMember.NodeId);
                    break;
            }
        }

        foreach (NamespaceRosterMember member in candidate.Members)
        {
            if (!prior.Members.Any(old => old.NodeId == member.NodeId))
            {
                if (!skippedGenerations && member.Status != NamespaceMemberStatus.Joining)
                    throw new InvalidDataException("A new namespace member must enter as Joining, never directly as Active.");
                if (!skippedGenerations &&
                    candidate.RetiredOriginEpochs.Any(entry => entry.OriginAuthorNodeId == member.NodeId))
                    throw new InvalidDataException("A new Joining member cannot arrive with a retired-origin closure.");
                // Across a generation gap, the pinned authority attests the complete
                // omitted Joining/FULL_REPLACE/retirement transitions. The signed
                // candidate remains the complete current roster; known prior fences
                // and retired-origin closures above are still enforced.
            }
        }
    }

    private void ValidateRetiredOriginClosure(
        NamespaceRosterCertificate prior, NamespaceRosterCertificate candidate, NodeId author)
    {
        AppliedOriginFrontierReport report = ReadOriginFrontiersCore(prior)
            .FirstOrDefault(item => item.MemberNodeId == author)
            ?? throw new InvalidDataException("An Active author must publish a signed issued-origin report before retirement.");
        if (!report.FinalForAuthor || report.UnpublishedDraftCount != 0)
            throw new InvalidDataException("An Active author must publish a final, draft-free signed report before retirement.");

        NamespaceRetiredOriginEpoch[] expected = report.IssuedOrigins
            .Select(entry => new NamespaceRetiredOriginEpoch
            {
                OriginAuthorNodeId = author,
                OriginAuthorEpoch = entry.OriginAuthorEpoch,
                FinalIssuedOriginSequence = entry.IssuedOriginSequence,
                MaxIssuedHlcWall = entry.MaxIssuedHlcWall,
                MaxIssuedHlcLogical = entry.MaxIssuedHlcLogical
            }).ToArray();
        NamespaceRetiredOriginEpoch[] actual = candidate.RetiredOriginEpochs
            .Where(entry => entry.OriginAuthorNodeId == author)
            .ToArray();
        if (actual.Length != expected.Length)
            throw new InvalidDataException("Retiring an author requires a roster closure for every signed issued author epoch.");
        for (int i = 0; i < expected.Length; i++)
        {
            if (actual[i].OriginAuthorEpoch != expected[i].OriginAuthorEpoch ||
                actual[i].FinalIssuedOriginSequence != expected[i].FinalIssuedOriginSequence ||
                actual[i].MaxIssuedHlcWall != expected[i].MaxIssuedHlcWall ||
                actual[i].MaxIssuedHlcLogical != expected[i].MaxIssuedHlcLogical)
                throw new InvalidDataException("Retired-origin closure does not match the author's latest signed issued-origin report.");
        }
    }

    private static void ValidatePreservedRetiredOriginClosure(
        NamespaceRosterCertificate prior, NamespaceRosterCertificate candidate, NodeId author)
    {
        NamespaceRetiredOriginEpoch[] oldEntries = prior.RetiredOriginEpochs
            .Where(entry => entry.OriginAuthorNodeId == author).ToArray();
        NamespaceRetiredOriginEpoch[] newEntries = candidate.RetiredOriginEpochs
            .Where(entry => entry.OriginAuthorNodeId == author).ToArray();
        if (oldEntries.Length != newEntries.Length)
            throw new InvalidDataException("A retired author's closed epochs cannot be removed or added after retirement.");
        for (int i = 0; i < oldEntries.Length; i++)
        {
            if (oldEntries[i].OriginAuthorEpoch != newEntries[i].OriginAuthorEpoch ||
                oldEntries[i].FinalIssuedOriginSequence != newEntries[i].FinalIssuedOriginSequence ||
                oldEntries[i].MaxIssuedHlcWall != newEntries[i].MaxIssuedHlcWall ||
                oldEntries[i].MaxIssuedHlcLogical != newEntries[i].MaxIssuedHlcLogical)
                throw new InvalidDataException("A retired author's closure is immutable across roster generations.");
        }
    }

    private bool HasInstalledAckForMember(
        NamespaceRosterCertificate priorRoster, NamespaceRosterMember member, TrustStore trustStore)
    {
        NamespaceRetentionCheckpoint? checkpoint = TryReadRetentionCheckpointCore(priorRoster, trustStore);
        if (checkpoint?.FullReplaceGeneration is not Guid generation ||
            checkpoint.FullReplaceDigest.Length != NamespaceRosterProtocol.DigestSize)
            return false;
        byte[]? encoded = ReadValue(FullReplaceAckKey(priorRoster.NamespaceId,
            priorRoster.RosterGeneration, generation, member.NodeId));
        if (encoded is null) return false;
        FullReplaceInstalledAck ack = FullReplaceInstalledAckProtocol.DecodeAndVerify(encoded, priorRoster);
        return ack.MemberNodeId == member.NodeId &&
            ack.MemberTransportEpoch == member.TransportPeerEpoch &&
            ack.Generation == generation &&
            ack.Digest.AsSpan().SequenceEqual(checkpoint.FullReplaceDigest);
    }

    private NamespaceRetentionCheckpoint? TryReadRetentionCheckpointCore(
        NamespaceRosterCertificate roster, TrustStore trustStore)
    {
        byte[]? encoded = ReadValue(CheckpointKey(roster.NamespaceId, roster.RosterGeneration));
        if (encoded is null) return null;
        NamespaceRetentionCheckpoint checkpoint =
            NamespaceRetentionCheckpointProtocol.DecodeAndVerify(encoded, roster, trustStore);
        ValidateCheckpointFrontier(checkpoint, roster);
        if (checkpoint.FullReplaceGeneration.HasValue &&
            (!TryGetCommonInstalledHlcFloorCore(roster,
                checkpoint.FullReplaceGeneration.Value, checkpoint.FullReplaceDigest,
                out ulong installedWall, out uint installedLogical) ||
             installedWall != checkpoint.FullReplaceHlcWallFloor ||
             installedLogical != checkpoint.FullReplaceHlcLogicalFloor))
            throw new InvalidDataException("Stored RCP2 checkpoint HLC floor does not match all Active FRA2 installation ACKs.");
        return checkpoint;
    }

    private void ValidateCheckpointFrontier(
        NamespaceRetentionCheckpoint checkpoint, NamespaceRosterCertificate roster)
    {
        IReadOnlyList<AppliedOriginFrontierReport> reports = ReadOriginFrontiersCore(roster);
        var current = NamespaceRetentionCheckpointProtocol.CreateFromReports(
            roster, checkpoint.CheckpointSequence, checkpoint.PreviousCheckpointDigest, reports,
            checkpoint.FullReplaceGeneration, checkpoint.FullReplaceDigest,
            checkpoint.FullReplaceHlcWallFloor, checkpoint.FullReplaceHlcLogicalFloor);
        var currentMap = current.SafeFrontier.ToDictionary(
            entry => (entry.OriginAuthorNodeId, entry.OriginAuthorEpoch),
            entry => entry.SafeAppliedOriginSequence);
        foreach (RetentionFrontierEntry entry in checkpoint.SafeFrontier)
        {
            if (!currentMap.TryGetValue((entry.OriginAuthorNodeId, entry.OriginAuthorEpoch), out ulong maximum) ||
                entry.SafeAppliedOriginSequence > maximum)
                throw new InvalidDataException("Retention checkpoint overclaims an active-member applied frontier.");
        }
    }

    private IReadOnlyList<AppliedOriginFrontierReport> ReadOriginFrontiersCore(NamespaceRosterCertificate roster)
    {
        byte[] prefix = FrontierPrefix(roster.NamespaceId, roster.RosterGeneration);
        var rows = Scan(prefix);
        var reports = new List<AppliedOriginFrontierReport>(rows.Count);
        foreach (StorageEntry row in rows)
        {
            AppliedOriginFrontierReport report = OriginFrontierProtocol.DecodeAndVerify(row.Value, roster);
            if (row.Key.Length != prefix.Length + 16 ||
                !row.Key.AsSpan(prefix.Length).SequenceEqual(report.MemberNodeId.ToArray()))
                throw new InvalidDataException("Applied-origin frontier key/value identity mismatch.");
            reports.Add(report);
        }
        reports.Sort((a, b) => CompareNodeIds(a.MemberNodeId, b.MemberNodeId));
        return reports;
    }

    private bool TryGetCommonInstalledHlcFloorCore(
        NamespaceRosterCertificate roster, Guid generation, ReadOnlySpan<byte> digest,
        out ulong wallFloor, out uint logicalFloor)
    {
        wallFloor = 0;
        logicalFloor = 0;
        if (generation == Guid.Empty || digest.Length != NamespaceRosterProtocol.DigestSize)
            return false;
        NamespaceRosterMember[] active = roster.Members
            .Where(member => member.Status == NamespaceMemberStatus.Active).ToArray();
        if (active.Length == 0) return false;

        bool first = true;
        foreach (NamespaceRosterMember member in active)
        {
            byte[]? encoded = ReadValue(FullReplaceAckKey(
                roster.NamespaceId, roster.RosterGeneration, generation, member.NodeId));
            if (encoded is null) return false;
            FullReplaceInstalledAck ack = FullReplaceInstalledAckProtocol.DecodeAndVerify(encoded, roster);
            if (!ack.Installed || ack.MemberNodeId != member.NodeId ||
                ack.MemberTransportEpoch != member.TransportPeerEpoch ||
                ack.Generation != generation || !ack.Digest.AsSpan().SequenceEqual(digest))
                return false;
            if (first)
            {
                wallFloor = ack.HlcWallFloor;
                logicalFloor = ack.HlcLogicalFloor;
                first = false;
            }
            else if (ack.HlcWallFloor != wallFloor || ack.HlcLogicalFloor != logicalFloor)
            {
                return false;
            }
        }
        return true;
    }

    private bool AreAllActiveMembersInstalledCore(
        NamespaceRosterCertificate roster, Guid generation, ReadOnlySpan<byte> digest)
    {
        if (generation == Guid.Empty || digest.Length != NamespaceRosterProtocol.DigestSize)
            return false;
        NamespaceRosterMember[] active = roster.Members
            .Where(member => member.Status == NamespaceMemberStatus.Active).ToArray();
        if (active.Length == 0) return false;
        foreach (NamespaceRosterMember member in active)
        {
            byte[]? encoded = ReadValue(FullReplaceAckKey(roster.NamespaceId,
                roster.RosterGeneration, generation, member.NodeId));
            if (encoded is null) return false;
            FullReplaceInstalledAck ack = FullReplaceInstalledAckProtocol.DecodeAndVerify(encoded, roster);
            if (ack.MemberTransportEpoch != member.TransportPeerEpoch ||
                ack.Generation != generation ||
                !ack.Digest.AsSpan().SequenceEqual(digest))
                return false;
        }
        return true;
    }

    private void EnsureCurrentRoster(NamespaceRosterCertificate roster)
    {
        NamespaceRosterProtocol.Validate(roster);
        byte[]? pointer = ReadValue(CurrentRosterKey(roster.NamespaceId));
        if (pointer is null || pointer.Length != 8 + NamespaceRosterProtocol.DigestSize ||
            BinaryPrimitives.ReadUInt64LittleEndian(pointer) != roster.RosterGeneration ||
            !pointer.AsSpan(8).SequenceEqual(NamespaceRosterProtocol.ComputeDigest(roster)))
            throw new InvalidDataException("Operation is not bound to the currently accepted namespace roster.");
    }

    private byte[]? ReadValue(byte[] key)
    {
        EnsureIdentity();
        using var snapshot = _storage.OpenSnapshot();
        return snapshot.TryGet(key, out byte[]? value) ? value : null;
    }

    private IReadOnlyList<StorageEntry> Scan(byte[] prefix)
    {
        EnsureIdentity();
        using var snapshot = _storage.OpenSnapshot();
        return snapshot.ScanPrefix(prefix, ReadOnlySpan<byte>.Empty, 8192);
    }

    private void EnsureIdentity()
    {
        using var snapshot = _storage.OpenSnapshot();
        if (snapshot.TryGet(_identityKey, out byte[]? identity))
        {
            if (identity is null || !identity.AsSpan().SequenceEqual(_identityValue))
                throw new InvalidDataException("Namespace GC storage identity mismatch.");
            return;
        }
        if (snapshot.ScanPrefix(_root, ReadOnlySpan<byte>.Empty, 1).Count != 0)
            throw new InvalidDataException("Namespace GC storage rows exist without their identity record.");
    }

    private void WriteRows(params (byte[] Key, byte[] Value)[] rows)
    {
        EnsureIdentity();
        using var batch = _storage.BeginWriteBatch();
        using (var snapshot = _storage.OpenSnapshot())
        {
            if (!snapshot.TryGet(_identityKey, out _))
                batch.Put(_identityKey, _identityValue);
        }
        foreach (var row in rows) batch.Put(row.Key, row.Value);
        batch.Commit();
    }

    private byte[] CurrentRosterKey(int namespaceId)
    {
        Span<byte> suffix = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        return Key(CurrentRosterKind, suffix);
    }

    private byte[] RosterKey(int namespaceId, ulong generation)
        => Key(RosterKind, NamespaceGenerationSuffix(namespaceId, generation));

    private byte[] FrontierKey(int namespaceId, ulong generation, NodeId member)
    {
        byte[] suffix = new byte[2 + 8 + 16];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        BinaryPrimitives.WriteUInt64LittleEndian(suffix.AsSpan(2), generation);
        member.WriteTo(suffix.AsSpan(10));
        return Key(FrontierKind, suffix);
    }

    private byte[] FrontierPrefix(int namespaceId, ulong generation)
        => Key(FrontierKind, NamespaceGenerationSuffix(namespaceId, generation));

    private byte[] CheckpointKey(int namespaceId, ulong generation)
        => Key(CheckpointKind, NamespaceGenerationSuffix(namespaceId, generation));

    private byte[] BaselineReportKey(int namespaceId, ulong rosterGeneration, NodeId member)
    {
        byte[] suffix = new byte[2 + 8 + 16];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        BinaryPrimitives.WriteUInt64LittleEndian(suffix.AsSpan(2), rosterGeneration);
        member.WriteTo(suffix.AsSpan(10));
        return Key(BaselineReportKind, suffix);
    }

    private byte[] BaselineReportPrefix(int namespaceId, ulong rosterGeneration)
        => Key(BaselineReportKind, NamespaceGenerationSuffix(namespaceId, rosterGeneration));

    private byte[] FullReplaceAckKey(int namespaceId, ulong rosterGeneration, Guid generation, NodeId member)
    {
        byte[] suffix = new byte[2 + 8 + 16 + 16];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        BinaryPrimitives.WriteUInt64LittleEndian(suffix.AsSpan(2), rosterGeneration);
        generation.TryWriteBytes(suffix.AsSpan(10, 16), bigEndian: true, out _);
        member.WriteTo(suffix.AsSpan(26));
        return Key(FullReplaceAckKind, suffix);
    }

    private byte[] FullReplaceAckPrefix(int namespaceId, ulong rosterGeneration, Guid generation)
    {
        byte[] suffix = new byte[2 + 8 + 16];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        BinaryPrimitives.WriteUInt64LittleEndian(suffix.AsSpan(2), rosterGeneration);
        generation.TryWriteBytes(suffix.AsSpan(10, 16), bigEndian: true, out _);
        return Key(FullReplaceAckKind, suffix);
    }

    private static byte[] NamespaceGenerationSuffix(int namespaceId, ulong generation)
    {
        byte[] suffix = new byte[10];
        BinaryPrimitives.WriteUInt16LittleEndian(suffix, checked((ushort)namespaceId));
        BinaryPrimitives.WriteUInt64LittleEndian(suffix.AsSpan(2), generation);
        return suffix;
    }

    private byte[] Key(byte kind, ReadOnlySpan<byte> suffix)
    {
        byte[] key = new byte[_root.Length + 1 + suffix.Length];
        _root.CopyTo(key, 0);
        key[_root.Length] = kind;
        suffix.CopyTo(key.AsSpan(_root.Length + 1));
        return key;
    }

    private static int CompareNodeIds(NodeId left, NodeId right)
    {
        Span<byte> a = stackalloc byte[16];
        Span<byte> b = stackalloc byte[16];
        left.WriteTo(a);
        right.WriteTo(b);
        return a.SequenceCompareTo(b);
    }
}
