using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageNamespaceGcStoreTests
{
    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private static NamespaceRosterCertificate Roster(
        int generation, byte[] previousDigest,
        (byte[] Seed, byte[] PublicKey)[] keys,
        NamespaceMemberStatus[]? statuses = null)
    {
        statuses ??= Enumerable.Repeat(NamespaceMemberStatus.Active, keys.Length).ToArray();
        var members = new NamespaceRosterMember[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            members[i] = new NamespaceRosterMember
            {
                NodeId = NodeId.From(Id((byte)(i + 1))),
                PublicKey = keys[i].PublicKey,
                TransportPeerEpoch = 10u + (uint)i,
                Status = statuses[i],
                RetiredThroughEpoch = statuses[i] == NamespaceMemberStatus.Retired ? 9u : 0u
            };
        }
        return new NamespaceRosterCertificate
        {
            NamespaceId = 19,
            RosterGeneration = (ulong)generation,
            PreviousRosterDigest = previousDigest,
            AuthorityNodeId = NodeId.From(Id(1)),
            Members = members
        };
    }

    private static AppliedOriginFrontierReport Report(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys, ulong reportSequence, ulong appliedSequence)
        => new()
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            MemberNodeId = roster.Members[memberIndex].NodeId,
            MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
            ReportSequence = reportSequence,
            Entries = new[]
            {
                new AppliedOriginFrontierEntry
                {
                    OriginAuthorNodeId = NodeId.From(Id(2)),
                    OriginAuthorEpoch = 3,
                    ContiguousAppliedOriginSequence = appliedSequence
                }
            },
            IssuedOrigins = memberIndex == 1
                ? new[]
                {
                    new IssuedOriginFrontierEntry
                    {
                        OriginAuthorEpoch = 3,
                        IssuedOriginSequence = appliedSequence,
                        MaxIssuedHlcWall = 100,
                        MaxIssuedHlcLogical = 2
                    }
                }
                : Array.Empty<IssuedOriginFrontierEntry>()
        };

    private static byte[] SignedReport(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys, ulong reportSequence, ulong appliedSequence)
        => OriginFrontierProtocol.Sign(Report(roster, memberIndex, keys, reportSequence, appliedSequence),
            keys[memberIndex].Seed);

    private static byte[] SignedAck(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys, Guid generation, byte[] digest)
        => FullReplaceInstalledAckProtocol.Sign(new FullReplaceInstalledAck
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            Generation = generation,
            Digest = digest,
            MemberNodeId = roster.Members[memberIndex].NodeId,
            MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
            HlcWallFloor = 100,
            HlcLogicalFloor = 2,
            Installed = true
        }, keys[memberIndex].Seed);

    private static byte[] SignedBaselineReport(
        NamespaceRosterCertificate roster, int memberIndex,
        (byte[] Seed, byte[] PublicKey)[] keys, Guid generation, byte[] snapshotDigest,
        ulong reportSequence, ulong feedOffset, ulong receiveCursor, byte anchor, uint issuerEpoch = 7)
    {
        int peerIndex = memberIndex == 0 ? 1 : 0;
        byte[] feedDigest = Enumerable.Repeat(anchor, MetadataBaselineReportProtocol.ChainDigestSize).ToArray();
        byte[] receiveDigest = Enumerable.Repeat((byte)(anchor + 1), MetadataBaselineReportProtocol.ChainDigestSize).ToArray();
        return MetadataBaselineReportProtocol.Sign(new MetadataBaselineReport
        {
            NamespaceId = roster.NamespaceId,
            RosterGeneration = roster.RosterGeneration,
            RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
            FullReplaceGeneration = generation,
            SnapshotDigest = snapshotDigest,
            SnapshotEntryCount = 4,
            ProviderChangeFeedWatermark = 40,
            HlcWallFloor = 100,
            HlcLogicalFloor = 2,
            MemberNodeId = roster.Members[memberIndex].NodeId,
            MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
            ReportSequence = reportSequence,
            FeedEntries = new[]
            {
                new MetadataBaselineFeedEntry
                {
                    TargetPeer = roster.Members[peerIndex].NodeId,
                    TargetTransportEpoch = roster.Members[peerIndex].TransportPeerEpoch,
                    AuthorStream = NodeId.From(Id(0x31)),
                    FeedKind = MetadataBaselineFeedKind.Outbound,
                    IssuerEpoch = issuerEpoch,
                    AbsoluteFeedOffset = feedOffset,
                    DigestAnchor = feedOffset == 0 ? new byte[MetadataBaselineReportProtocol.ChainDigestSize] : feedDigest
                }
            },
            ReceiveEntries = new[]
            {
                new MetadataBaselineReceiveEntry
                {
                    SenderPeer = roster.Members[peerIndex].NodeId,
                    SenderTransportEpoch = roster.Members[peerIndex].TransportPeerEpoch,
                    AuthorStream = NodeId.From(Id(0x32)),
                    IssuerEpoch = 8,
                    ReceiveCursor = receiveCursor,
                    LastBatchDigest = receiveCursor == 0 ? new byte[MetadataBaselineReportProtocol.ChainDigestSize] : receiveDigest
                }
            }
        }, keys[memberIndex].Seed);
    }

    private static void AcceptInstalledAcks(
        StorageNamespaceGcStore gc, NamespaceRosterCertificate roster,
        (byte[] Seed, byte[] PublicKey)[] keys, Guid generation, byte[] digest)
    {
        for (int i = 0; i < keys.Length; i++)
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, i, keys, generation, digest), roster);
    }

    [Fact]
    public void Receipt_gc_requires_retired_transport_fence_signed_origin_horizon_and_empty_host_references()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-receipt-gc-proof-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        const string scope = "receipt-gc-proof";
        const int namespaceId = 19;
        NodeId local = NodeId.From(Id(0x7F));
        NodeId sender = NodeId.From(Id(2));
        Guid generation = Guid.Parse("22345678-1234-5678-9abc-def012345678");
        byte[] replaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("receipt GC baseline"));
        var compactedBatches = new List<MetadataIncomingBatch>();

        IApplicationGcReferenceScanner Scanner(bool hasReceiptReference)
            => new DelegateApplicationGcReferenceScanner((_, _, _) =>
                new DelegateApplicationGcReferenceScan(
                    new MemoryStream(),
                    new ApplicationDraftHorizon(0, 0, 0),
                    (_, _, _, _) => hasReceiptReference,
                    _ => false,
                    (_, _) => false));

        try
        {
            using (var storage = StorageDatabase.Open(directory, 910))
            using (var metadata = new StorageMetadataStore(storage))
            {
                var gc = new StorageNamespaceGcStore(
                    storage, scope, local,
                    new StorageNamespaceGcFeatureOptions(EnableReceiptGc: true));
                NamespaceRosterCertificate roster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(Roster(1, new byte[32], keys), keys[0].Seed), trust);
                metadata.AttachNamespaceGcGuard(gc, trust, scope, local);

                metadata.RegisterInterestedPeer(scope, local, sender, 1, namespaceId);
                var batch = new MetadataIncomingBatch
                {
                    ScopeId = scope,
                    LocalReplica = local,
                    SenderPeer = sender,
                    SenderEpoch = 3,
                    TransportPeerEpoch = 1,
                    AuthorStreamNodeId = sender,
                    NamespaceId = namespaceId,
                    BatchId = BatchId.From(Id(0x60)),
                    CursorStart = 1,
                    CursorEnd = 1,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat((byte)0x61, 16).ToArray(),
                    HeaderFingerprint = Enumerable.Repeat((byte)0x62, 16).ToArray(),
                    Operations = new[]
                    {
                        new MetadataOperation
                        {
                            AuthorNodeId = sender,
                            ChangeId = BatchId.From(Id(0x63)),
                            OriginAuthorEpoch = 3,
                            OriginSequence = 50,
                            Fingerprint = Enumerable.Repeat((byte)0x64, 16).ToArray(),
                            Payload = new byte[] { 0x65 }
                        }
                    }
                };
                Assert.False(metadata.AcceptIncomingBatch(batch).IsDuplicate);
                compactedBatches.Add(batch);

                // Populate a short contiguous receive chain so the receipt collector
                // must honor its page limit and resume across repeated calls.
                byte[] previousDigest = batch.BatchDigest.ToArray();
                for (byte index = 0; index < 2; index++)
                {
                    byte cursor = (byte)(index + 2);
                    var nextBatch = new MetadataIncomingBatch
                    {
                        ScopeId = scope,
                        LocalReplica = local,
                        SenderPeer = sender,
                        SenderEpoch = 3,
                        TransportPeerEpoch = 1,
                        AuthorStreamNodeId = sender,
                        NamespaceId = namespaceId,
                        BatchId = BatchId.From(Id((byte)(0x70 + index))),
                        CursorStart = cursor,
                        CursorEnd = cursor,
                        PreviousBatchDigest = previousDigest,
                        BatchDigest = Enumerable.Repeat((byte)(0x71 + index), 16).ToArray(),
                        HeaderFingerprint = Enumerable.Repeat((byte)(0x73 + index), 16).ToArray(),
                        Operations = new[]
                        {
                            new MetadataOperation
                            {
                                AuthorNodeId = sender,
                                ChangeId = BatchId.From(Id((byte)(0x75 + index))),
                                OriginAuthorEpoch = 3,
                                OriginSequence = (ulong)(50 + cursor - 1),
                                Fingerprint = Enumerable.Repeat((byte)(0x77 + index), 16).ToArray(),
                                Payload = new[] { (byte)(0x79 + index) }
                            }
                        }
                    };
                    Assert.False(metadata.AcceptIncomingBatch(nextBatch).IsDuplicate);
                    compactedBatches.Add(nextBatch);
                    previousDigest = nextBatch.BatchDigest.ToArray();
                }

                // A newer authenticated transport incarnation is registered and
                // completes its baseline before the old epoch is durably fenced.
                metadata.RegisterInterestedPeer(scope, local, sender, 2, namespaceId);
                metadata.CompletePeerInitialSync(scope, local, sender, 2, namespaceId,
                    new[] { new MetadataPeerStreamBaseline(local, 0, 3) });
                metadata.RetireInterestedPeer(scope, local, sender, 1, 2, namespaceId);

                // A transport retirement fence alone is insufficient: the origin
                // must also be covered by the signed all-active-member checkpoint.
                Assert.Equal(0, metadata.CompactReceipts(
                    scope, local, namespaceId, 16, () => new MemoryStream(), Scanner(false)));

                var report0 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
                var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
                gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, replaceDigest), roster);
                gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 1, keys, generation, replaceDigest), roster);
                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    roster, 1, new byte[32], new[] { report0, report1 }, generation, replaceDigest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

                // Even with both signed proof domains satisfied, a host-side
                // reference snapshot must veto receipt deletion.
                Assert.Equal(0, metadata.CompactReceipts(
                    scope, local, namespaceId, 16, () => new MemoryStream(), Scanner(true)));

                Assert.Equal(1, metadata.CompactReceipts(
                    scope, local, namespaceId, 1, () => new MemoryStream(), Scanner(false)));
                Assert.Equal(1, metadata.CompactReceipts(
                    scope, local, namespaceId, 1, () => new MemoryStream(), Scanner(false)));
                Assert.Equal(1, metadata.CompactReceipts(
                    scope, local, namespaceId, 1, () => new MemoryStream(), Scanner(false)));
                Assert.Equal(0, metadata.CompactReceipts(
                    scope, local, namespaceId, 1, () => new MemoryStream(), Scanner(false)));
            }

            // The receipt deletion is durable. Reopening the same Storage database
            // must not resurrect it or make the same retired receipt collectible again.
            using var reopenedStorage = StorageDatabase.Open(directory, 910);
            using var reopenedMetadata = new StorageMetadataStore(reopenedStorage);
            var reopenedGc = new StorageNamespaceGcStore(
                reopenedStorage, scope, local,
                new StorageNamespaceGcFeatureOptions(EnableReceiptGc: true));
            reopenedMetadata.AttachNamespaceGcGuard(reopenedGc, trust, scope, local);
            Assert.Equal(0, reopenedMetadata.CompactReceipts(
                scope, local, namespaceId, 16, () => new MemoryStream(), Scanner(false)));
            // The receipt row is gone, but the durable receive cursor/digest chain
            // must still reject the old batch after restart instead of treating it as new.
            Assert.Equal(3, compactedBatches.Count);
            foreach (MetadataIncomingBatch compactedBatch in compactedBatches)
                Assert.Throws<InvalidDataException>(() =>
                    reopenedMetadata.AcceptIncomingBatch(compactedBatch));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Theory]
    [InlineData(NamespaceMemberStatus.Active, 12u, 0u, 11u, 11u, true)]
    [InlineData(NamespaceMemberStatus.Active, 12u, 0u, 12u, 12u, false)]
    [InlineData(NamespaceMemberStatus.Retired, 10u, 9u, 9u, 9u, true)]
    [InlineData(NamespaceMemberStatus.Retired, 10u, 9u, 10u, 10u, false)]
    public void Receipt_gc_requires_both_durable_and_current_roster_transport_epoch_fences(
        NamespaceMemberStatus status, uint currentTransportEpoch, uint rosterRetiredThrough,
        uint receiptTransportEpoch, uint durableRetiredThrough, bool expected)
    {
        var member = new NamespaceRosterMember
        {
            NodeId = NodeId.From(Id(2)),
            PublicKey = BatchSignature.GenerateKeyPair().PublicKey,
            TransportPeerEpoch = currentTransportEpoch,
            Status = status,
            RetiredThroughEpoch = rosterRetiredThrough
        };

        Assert.Equal(expected, StorageMetadataStore.IsReceiptTransportEpochFenced(
            member, receiptTransportEpoch, durableRetiredThrough));
    }

    [Fact]
    public void Physical_gc_domains_are_disabled_by_default_and_require_explicit_options()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-gc-flags-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 19);
            NodeId local = NodeId.From(Id(0x7F));
            var defaults = new StorageNamespaceGcStore(storage, "gc-flags", local);
            Assert.False(defaults.IsTombstoneGcProofEnabled);
            Assert.False(defaults.IsChangeIdGcProofEnabled);
            Assert.False(defaults.IsReceiptGcProofEnabled);

            var enabled = new StorageNamespaceGcStore(
                storage, "gc-flags", local,
                new StorageNamespaceGcFeatureOptions(
                    EnableTombstoneGc: true,
                    EnableChangeIdGc: true,
                    EnableReceiptGc: true));
            Assert.True(enabled.IsTombstoneGcProofEnabled);
            Assert.True(enabled.IsChangeIdGcProofEnabled);
            Assert.True(enabled.IsReceiptGcProofEnabled);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    private sealed class RecordingTombstoneGc : IAsyncProviderTombstoneGc
    {
        private readonly IReadOnlyList<ProviderMutation> _candidates;

        public RecordingTombstoneGc(IReadOnlyList<ProviderMutation> candidates)
            => _candidates = candidates;

        public int ReadCalls { get; private set; }
        public int CompactCalls { get; private set; }
        public IReadOnlyList<ProviderMutation>? Approved { get; private set; }

        public ValueTask<IReadOnlyList<ProviderMutation>> ReadTombstoneGcCandidatesAsync(
            int limit, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ReadCalls++;
            return ValueTask.FromResult<IReadOnlyList<ProviderMutation>>(_candidates.Take(limit).ToArray());
        }

        public ValueTask<int> CompactTombstonesAsync(
            IReadOnlyList<ProviderMutation> approvedCandidates,
            int limit, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CompactCalls++;
            Approved = approvedCandidates.ToArray();
            return ValueTask.FromResult(Approved.Count);
        }
    }

    [Theory]
    [InlineData(true, 90UL, 1)]
    [InlineData(true, 91UL, 0)]
    [InlineData(false, 90UL, 0)]
    public async Task Async_tombstone_gc_coordinator_requires_opt_in_and_signed_safe_horizon(
        bool enabled, ulong originSequence, int expectedCompacted)
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-async-tombstone-gc-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        const string scope = "async-tombstone-gc-coordinator";
        const int namespaceId = 19;
        Guid generation = Guid.Parse("12345678-1234-5678-9abc-def012345678");
        byte[] replaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("authoritative snapshot"));
        try
        {
            using var storage = StorageDatabase.Open(directory, 909);
            using var metadata = new StorageMetadataStore(storage);
            using var node = new NodeBuilder().WithNodeId(Id(99)).WithEpoch(1).Build();
            var gc = new StorageNamespaceGcStore(
                storage, scope, node.NodeId,
                new StorageNamespaceGcFeatureOptions(EnableTombstoneGc: enabled));
            var roster = gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(Roster(1, new byte[32], keys), keys[0].Seed), trust);
            var report0 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
            var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, replaceDigest), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 1, keys, generation, replaceDigest), roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[] { report0, report1 }, generation, replaceDigest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

            var candidate = new ProviderMutation
            {
                RecordId = RecordId.From(Id(0x55)),
                FieldId = 7,
                IsTombstone = true,
                IsRecordTombstone = false,
                HlcWall = 100,
                HlcLogical = 2,
                AuthorNodeId = NodeId.From(Id(2)),
                KeyEpoch = 0,
                Value = Array.Empty<byte>(),
                ChangeId = BatchId.From(Id(0x56)),
                OriginAuthorEpoch = 3,
                OriginSequence = originSequence
            };
            var provider = new RecordingTombstoneGc(new[] { candidate });
            var coordinator = new AsyncStorageSyncCoordinator(node, metadata, scope, namespaceId);
            coordinator.AttachNamespaceGcGuard(gc, trust);

            int compacted = await coordinator.CompactTombstonesAsync(provider, 16);

            Assert.Equal(expectedCompacted, compacted);
            Assert.Equal(enabled ? 1 : 0, provider.ReadCalls);
            Assert.Equal(expectedCompacted == 1 ? 1 : 0, provider.CompactCalls);
            if (expectedCompacted == 1)
                Assert.Equal(candidate, Assert.Single(provider.Approved!));
            else
                Assert.Null(provider.Approved);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Signed_roster_frontiers_acks_and_checkpoint_survive_restart_and_prove_only_the_safe_horizon()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-gc-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);
        byte[] rosterBytes = NamespaceRosterProtocol.Sign(roster, keys[0].Seed);
        Guid generation = Guid.Parse("12345678-1234-5678-9abc-def012345678");
        byte[] replaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("authoritative snapshot"));

        try
        {
            using (var storage = StorageDatabase.Open(directory, 901))
            {
                var gc = new StorageNamespaceGcStore(storage, "fld-db/main", NodeId.From(Id(99)));
                roster = gc.AcceptNamespaceRoster(rosterBytes, trust);

                var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
                var report2 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
                gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, replaceDigest), roster);
                gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 1, keys, generation, replaceDigest), roster);

                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    roster, 1, new byte[32], new[] { report1, report2 }, generation, replaceDigest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
                byte[] checkpointBytes = NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed);
                gc.AcceptRetentionCheckpoint(checkpointBytes, roster, trust);

                Assert.True(gc.AreAllActiveMembersInstalled(roster, generation, replaceDigest));
                Assert.True(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, NodeId.From(Id(2)), 3, 90));
                Assert.False(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, NodeId.From(Id(2)), 3, 91));
                Assert.True(gc.HasProtocolTombstoneGcProof(
                    roster, trust, NodeId.From(Id(2)), 3, 90, 0, 0, generation, replaceDigest));
                Assert.False(gc.HasProtocolTombstoneGcProof(
                    roster, trust, NodeId.From(Id(2)), 3, 91, 0, 0, generation, replaceDigest));
            }

            using (var reopened = StorageDatabase.Open(directory, 901))
            {
                var gc = new StorageNamespaceGcStore(reopened, "fld-db/main", NodeId.From(Id(99)));
                Assert.True(gc.TryReadNamespaceRoster(19, trust, out var restoredRoster));
                Assert.NotNull(restoredRoster);
                Assert.Equal(1UL, restoredRoster!.RosterGeneration);
                Assert.True(gc.TryReadRetentionCheckpoint(restoredRoster, trust, out var restoredCheckpoint));
                Assert.NotNull(restoredCheckpoint);
                Assert.Equal(2, gc.ReadOriginFrontiers(restoredRoster).Count);
                Assert.Equal(2, gc.ReadFullReplaceInstalledAcks(restoredRoster, generation).Count);
                Assert.True(gc.HasProtocolTombstoneGcProof(
                    restoredRoster, trust, NodeId.From(Id(2)), 3, 90, 0, 0, generation, replaceDigest));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Tombstone_gc_proof_rejects_hlc_and_full_replace_identity_overclaims()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-tombstone-proof-overclaim-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);
        Guid generation = Guid.Parse("12345678-1234-5678-9abc-def012345679");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("tombstone proof snapshot"));

        try
        {
            using var storage = StorageDatabase.Open(directory, 903);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/tombstone-overclaim", NodeId.From(Id(99)));
            roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
            var report0 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
            var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, digest), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 1, keys, generation, digest), roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[] { report0, report1 }, generation, digest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

            NodeId origin = NodeId.From(Id(2));
            Assert.True(gc.HasProtocolTombstoneGcProof(
                roster, trust, origin, 3, 90, 100, 2, generation, digest));
            Assert.False(gc.HasProtocolTombstoneGcProof(
                roster, trust, origin, 3, 90, 101, 0, generation, digest));
            Assert.False(gc.HasProtocolTombstoneGcProof(
                roster, trust, origin, 3, 90, 100, 3, generation, digest));
            Assert.False(gc.HasProtocolTombstoneGcProof(
                roster, trust, origin, 3, 90, 100, 2,
                Guid.Parse("12345678-1234-5678-9abc-def012345680"), digest));
            Assert.False(gc.HasProtocolTombstoneGcProof(
                roster, trust, origin, 3, 90, 100, 2, generation,
                SHA256.HashData(Encoding.UTF8.GetBytes("different snapshot"))));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Tombstone_horizon_waits_for_offline_active_member_and_requires_member_bound_reports()
    {
        string directory = Path.Combine(
            Path.GetTempPath(), "binsync-tombstone-offline-member-" + Guid.NewGuid().ToString("N"));
        var keys = new[]
        {
            BatchSignature.GenerateKeyPair(),
            BatchSignature.GenerateKeyPair(),
            BatchSignature.GenerateKeyPair()
        };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        NamespaceRosterCertificate roster = Roster(1, new byte[32], keys);
        Guid generation = Guid.Parse("cccccccc-dddd-eeee-ffff-000000000042");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("same full replace for every route"));
        NodeId origin = NodeId.From(Id(2));
        const uint originEpoch = 7;
        const ulong finalIssued = 42;

        AppliedOriginFrontierReport MemberReport(int memberIndex, ulong reportSequence, ulong applied)
            => new()
            {
                NamespaceId = roster.NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = roster.Members[memberIndex].NodeId,
                MemberTransportEpoch = roster.Members[memberIndex].TransportPeerEpoch,
                ReportSequence = reportSequence,
                Entries = new[]
                {
                    new AppliedOriginFrontierEntry
                    {
                        OriginAuthorNodeId = origin,
                        OriginAuthorEpoch = originEpoch,
                        ContiguousAppliedOriginSequence = applied
                    }
                },
                IssuedOrigins = memberIndex == 1
                    ? new[]
                    {
                        new IssuedOriginFrontierEntry
                        {
                            OriginAuthorEpoch = originEpoch,
                            IssuedOriginSequence = finalIssued,
                            MaxIssuedHlcWall = 100,
                            MaxIssuedHlcLogical = 2
                        }
                    }
                    : Array.Empty<IssuedOriginFrontierEntry>()
            };

        try
        {
            using (var storage = StorageDatabase.Open(directory, 906))
            {
                var gc = new StorageNamespaceGcStore(
                    storage, "fld-db/tombstone-offline-member", NodeId.From(Id(99)));
                roster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);

                var reports = new[]
                {
                    gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                        MemberReport(0, 1, finalIssued), keys[0].Seed), roster),
                    gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                        MemberReport(1, 1, finalIssued), keys[1].Seed), roster),
                    gc.AcceptOriginFrontier(OriginFrontierProtocol.Sign(
                        MemberReport(2, 1, finalIssued - 1), keys[2].Seed), roster)
                };

                for (int i = 0; i < keys.Length; i++)
                    gc.AcceptFullReplaceInstalledAck(SignedAck(roster, i, keys, generation, digest), roster);

                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    roster, 1, new byte[32], reports, generation, digest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

                Assert.True(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, origin, originEpoch, finalIssued - 1));
                Assert.False(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, origin, originEpoch, finalIssued));
                Assert.False(gc.HasProtocolTombstoneGcProof(
                    roster, trust, origin, originEpoch, finalIssued, 100, 2, generation, digest));

                // A report forwarded over another route cannot impersonate the offline member:
                // the signed member identity and transport epoch remain bound to its roster key.
                AppliedOriginFrontierReport forged = MemberReport(2, 2, finalIssued);
                byte[] forgedBytes = OriginFrontierProtocol.Sign(forged, keys[0].Seed);
                Assert.Throws<System.Security.Cryptography.CryptographicException>(() =>
                    gc.AcceptOriginFrontier(forgedBytes, roster));
            }

            // Reopen must preserve the lagging member's durable frontier and keep the
            // tombstone horizon closed while that active member is offline.
            using (var reopened = StorageDatabase.Open(directory, 906))
            {
                var gc = new StorageNamespaceGcStore(
                    reopened, "fld-db/tombstone-offline-member", NodeId.From(Id(99)));
                Assert.True(gc.TryReadNamespaceRoster(19, trust, out NamespaceRosterCertificate? restored));
                Assert.NotNull(restored);
                Assert.False(gc.HasProtocolTombstoneGcProof(
                    restored!, trust, origin, originEpoch, finalIssued, 100, 2, generation, digest));

                var recoveredReports = gc.ReadOriginFrontiers(restored!);
                AppliedOriginFrontierReport caughtUp = MemberReport(2, 2, finalIssued);
                gc.AcceptOriginFrontier(
                    OriginFrontierProtocol.Sign(caughtUp, keys[2].Seed), restored!);

                var nextReports = gc.ReadOriginFrontiers(restored!);
                var nextCheckpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    restored!, 2, NamespaceRetentionCheckpointProtocol.ComputeDigest(
                        NamespaceRetentionCheckpointProtocol.CreateFromReports(
                            restored!, 1, new byte[32], recoveredReports, generation, digest,
                            fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2)),
                    nextReports, generation, digest,
                    fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(nextCheckpoint, keys[0].Seed),
                    restored!, trust);

                Assert.True(gc.HasProtocolTombstoneGcProof(
                    restored!, trust, origin, originEpoch, finalIssued, 100, 2, generation, digest));
                Assert.Equal(finalIssued, nextReports.Single(
                    report => report.MemberNodeId == restored!.Members[2].NodeId)
                    .Entries.Single().ContiguousAppliedOriginSequence);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Tombstone_gc_proof_requires_every_active_member_installed_ack()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-tombstone-proof-missing-ack-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);
        Guid generation = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("one member missing ack"));

        try
        {
            using var storage = StorageDatabase.Open(directory, 904);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/tombstone-missing-ack", NodeId.From(Id(99)));
            roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
            var report0 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
            var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, digest), roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[] { report0, report1 }, generation, digest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
            Assert.Throws<InvalidDataException>(() => gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust));

            Assert.False(gc.HasProtocolTombstoneGcProof(
                roster, trust, NodeId.From(Id(2)), 3, 90, 100, 2, generation, digest));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Tombstone_gc_proof_requires_all_active_replicas_to_apply_the_final_issued_prefix()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-tombstone-proof-incomplete-applied-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);
        Guid generation = Guid.Parse("bbbbbbbb-cccc-dddd-eeee-ffffffffffff");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("incomplete applied prefix"));

        try
        {
            using var storage = StorageDatabase.Open(directory, 905);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/tombstone-incomplete-applied", NodeId.From(Id(99)));
            roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
            NodeId origin = NodeId.From(Id(1));
            var member0Report = new AppliedOriginFrontierReport
            {
                NamespaceId = roster.NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = roster.Members[0].NodeId,
                MemberTransportEpoch = roster.Members[0].TransportPeerEpoch,
                ReportSequence = 1,
                Entries = new[]
                {
                    new AppliedOriginFrontierEntry
                    {
                        OriginAuthorNodeId = origin,
                        OriginAuthorEpoch = 3,
                        ContiguousAppliedOriginSequence = 100
                    }
                },
                IssuedOrigins = new[]
                {
                    new IssuedOriginFrontierEntry
                    {
                        OriginAuthorEpoch = 3,
                        IssuedOriginSequence = 90,
                        MaxIssuedHlcWall = 100,
                        MaxIssuedHlcLogical = 2
                    }
                }
            };
            var member1Report = new AppliedOriginFrontierReport
            {
                NamespaceId = roster.NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = roster.Members[1].NodeId,
                MemberTransportEpoch = roster.Members[1].TransportPeerEpoch,
                ReportSequence = 1,
                Entries = new[]
                {
                    new AppliedOriginFrontierEntry
                    {
                        OriginAuthorNodeId = origin,
                        OriginAuthorEpoch = 3,
                        ContiguousAppliedOriginSequence = 80
                    }
                }
            };
            var report0 = gc.AcceptOriginFrontier(
                OriginFrontierProtocol.Sign(member0Report, keys[0].Seed), roster);
            var report1 = gc.AcceptOriginFrontier(
                OriginFrontierProtocol.Sign(member1Report, keys[1].Seed), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, digest), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 1, keys, generation, digest), roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[] { report0, report1 }, generation, digest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

            Assert.True(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, origin, 3, 80));
            Assert.False(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, origin, 3, 90));
            Assert.False(gc.HasProtocolTombstoneGcProof(
                roster, trust, origin, 3, 80, 100, 2, generation, digest));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Joining_member_cannot_become_active_until_current_full_replace_ack_is_durable()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-join-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster1 = Roster(1, new byte[32], keys,
            new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Joining });
        Guid generation = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        byte[] replaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("join baseline"));

        try
        {
            using var storage = StorageDatabase.Open(directory, 902);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/join", NodeId.From(Id(99)));
            roster1 = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster1, keys[0].Seed), trust);
            var report = gc.AcceptOriginFrontier(SignedReport(roster1, 0, keys, 1, 50), roster1);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster1, 0, keys, generation, replaceDigest), roster1);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster1, 1, new byte[32], new[] { report }, generation, replaceDigest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster1, trust);

            var roster2 = Roster(2, NamespaceRosterProtocol.ComputeDigest(roster1), keys,
                new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Active });
            byte[] roster2Bytes = NamespaceRosterProtocol.Sign(roster2, keys[0].Seed);
            Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(roster2Bytes, trust));

            gc.AcceptFullReplaceInstalledAck(SignedAck(roster1, 1, keys, generation, replaceDigest), roster1);
            var accepted = gc.AcceptNamespaceRoster(roster2Bytes, trust);
            Assert.Equal(2UL, accepted.RosterGeneration);
            Assert.True(gc.TryReadNamespaceRoster(19, trust, out var current));
            Assert.Equal(2UL, current!.RosterGeneration);
            Assert.Empty(gc.ReadOriginFrontiers(current));
            Assert.False(gc.AreAllActiveMembersInstalled(current, generation, replaceDigest));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_retention_checkpoint_rejects_missing_active_member_frontier_report()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = Roster(1, new byte[32], keys);
        var report0 = OriginFrontierProtocol.Sign(Report(roster, 0, keys, 1, 100), keys[0].Seed);
        Guid generation = Guid.Parse("cccccccc-dddd-eeee-ffff-000000000001");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("missing active report"));

        Assert.Throws<InvalidDataException>(() =>
            NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[]
                {
                    OriginFrontierProtocol.DecodeAndVerify(report0, roster)
                },
                generation, digest, fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2));
    }

    [Fact]
    public void Full_replace_retention_checkpoint_rejects_active_member_with_unpublished_drafts()
    {
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var roster = Roster(1, new byte[32], keys);
        AppliedOriginFrontierReport clean = Report(roster, 0, keys, 1, 100);
        AppliedOriginFrontierReport draftBase = Report(roster, 1, keys, 1, 90);
        var draftful = new AppliedOriginFrontierReport
        {
            NamespaceId = draftBase.NamespaceId,
            RosterGeneration = draftBase.RosterGeneration,
            RosterDigest = draftBase.RosterDigest,
            MemberNodeId = draftBase.MemberNodeId,
            MemberTransportEpoch = draftBase.MemberTransportEpoch,
            ReportSequence = draftBase.ReportSequence,
            UnpublishedDraftCount = 1,
            MaxUnpublishedDraftHlcWall = 200,
            Entries = draftBase.Entries,
            IssuedOrigins = draftBase.IssuedOrigins
        };
        var verified = new[]
        {
            OriginFrontierProtocol.DecodeAndVerify(
                OriginFrontierProtocol.Sign(clean, keys[0].Seed), roster),
            OriginFrontierProtocol.DecodeAndVerify(
                OriginFrontierProtocol.Sign(draftful, keys[1].Seed), roster)
        };
        Guid generation = Guid.Parse("cccccccc-dddd-eeee-ffff-000000000002");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("draftful active member"));

        Assert.Throws<InvalidDataException>(() =>
            NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], verified, generation, digest,
                fullReplaceHlcWallFloor: 200, fullReplaceHlcLogicalFloor: 2));
    }

    [Fact]
    public void Tombstone_gc_proof_requires_signed_issued_origin_maxima()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-tombstone-proof-missing-issued-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);
        Guid generation = Guid.Parse("cccccccc-dddd-eeee-ffff-000000000003");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("missing issued origin"));

        try
        {
            using var storage = StorageDatabase.Open(directory, 906);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/tombstone-missing-issued", NodeId.From(Id(99)));
            roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
            var report0 = new AppliedOriginFrontierReport
            {
                NamespaceId = roster.NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = roster.Members[0].NodeId,
                MemberTransportEpoch = roster.Members[0].TransportPeerEpoch,
                ReportSequence = 1,
                Entries = new[]
                {
                    new AppliedOriginFrontierEntry
                    {
                        OriginAuthorNodeId = NodeId.From(Id(2)),
                        OriginAuthorEpoch = 3,
                        ContiguousAppliedOriginSequence = 90
                    }
                }
            };
            var report1 = new AppliedOriginFrontierReport
            {
                NamespaceId = roster.NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = roster.Members[1].NodeId,
                MemberTransportEpoch = roster.Members[1].TransportPeerEpoch,
                ReportSequence = 1,
                Entries = new[]
                {
                    new AppliedOriginFrontierEntry
                    {
                        OriginAuthorNodeId = NodeId.From(Id(1)),
                        OriginAuthorEpoch = 3,
                        ContiguousAppliedOriginSequence = 90
                    }
                }
            };
            var accepted0 = gc.AcceptOriginFrontier(
                OriginFrontierProtocol.Sign(report0, keys[0].Seed), roster);
            var accepted1 = gc.AcceptOriginFrontier(
                OriginFrontierProtocol.Sign(report1, keys[1].Seed), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 0, keys, generation, digest), roster);
            gc.AcceptFullReplaceInstalledAck(SignedAck(roster, 1, keys, generation, digest), roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[] { accepted0, accepted1 }, generation, digest,
                fullReplaceHlcWallFloor: 100, fullReplaceHlcLogicalFloor: 2);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

            Assert.False(gc.HasProtocolTombstoneGcProof(
                roster, trust, NodeId.From(Id(2)), 3, 90, 100, 2, generation, digest));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Offline_rejoin_accepts_signed_roster_gap_but_resets_every_gc_horizon()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-rejoin-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        NodeId authority = NodeId.From(Id(1));
        NodeId rejoining = NodeId.From(Id(2));
        trust.Allow(authority, keys[0].PublicKey, TrustMode.PreShared);
        Guid generation = Guid.Parse("bbbbbbbb-cccc-dddd-eeee-ffffffffffff");
        byte[] replaceDigest = SHA256.HashData(Encoding.UTF8.GetBytes("pre-offline installed snapshot"));
        var closure = new[]
        {
            new NamespaceRetiredOriginEpoch
            {
                OriginAuthorNodeId = rejoining,
                OriginAuthorEpoch = 3,
                FinalIssuedOriginSequence = 40,
                MaxIssuedHlcWall = 100,
                MaxIssuedHlcLogical = 2
            }
        };

        NamespaceRosterCertificate MakeRoster(
            ulong rosterGeneration, byte[] previousDigest,
            NamespaceMemberStatus rejoiningStatus, uint rejoiningEpoch, uint retiredThrough)
            => new()
            {
                NamespaceId = 19,
                RosterGeneration = rosterGeneration,
                PreviousRosterDigest = previousDigest,
                AuthorityNodeId = authority,
                Members = new[]
                {
                    new NamespaceRosterMember
                    {
                        NodeId = authority,
                        PublicKey = keys[0].PublicKey,
                        TransportPeerEpoch = 10,
                        Status = NamespaceMemberStatus.Active
                    },
                    new NamespaceRosterMember
                    {
                        NodeId = rejoining,
                        PublicKey = keys[1].PublicKey,
                        TransportPeerEpoch = rejoiningEpoch,
                        Status = rejoiningStatus,
                        RetiredThroughEpoch = retiredThrough
                    }
                },
                RetiredOriginEpochs = closure
            };

        try
        {
            using (var storage = StorageDatabase.Open(directory, 904))
            {
                var gc = new StorageNamespaceGcStore(storage, "fld-db/offline-rejoin", NodeId.From(Id(99)));
                NamespaceRosterCertificate retiredRoster = MakeRoster(
                    1, new byte[32], NamespaceMemberStatus.Retired, 11, 11);
                retiredRoster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(retiredRoster, keys[0].Seed), trust);

                AppliedOriginFrontierReport oldReport = gc.AcceptOriginFrontier(
                    SignedReport(retiredRoster, 0, keys, 1, 100), retiredRoster);
                var oldCheckpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    retiredRoster, 1, new byte[32], new[] { oldReport });
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(oldCheckpoint, keys[0].Seed), retiredRoster, trust);
                gc.AcceptFullReplaceInstalledAck(
                    SignedAck(retiredRoster, 0, keys, generation, replaceDigest), retiredRoster);

                Assert.True(gc.IsOriginCoveredByRetentionCheckpoint(
                    retiredRoster, trust, rejoining, 3, 40));
                Assert.True(gc.AreAllActiveMembersInstalled(retiredRoster, generation, replaceDigest));

                // A consecutive Retired -> Active transition is forbidden even with
                // a newer transport epoch; the authority must attest the skipped
                // Joining/FULL_REPLACE/promotion history in a higher full roster.
                NamespaceRosterCertificate consecutive = MakeRoster(
                    2, NamespaceRosterProtocol.ComputeDigest(retiredRoster),
                    NamespaceMemberStatus.Active, 12, 0);
                Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(consecutive, keys[0].Seed), trust));

                // Across a generation gap the retired fence still applies: equality
                // is not a new transport incarnation and cannot reactivate the member.
                NamespaceRosterCertificate staleEpochGap = MakeRoster(
                    4, SHA256.HashData("omitted roster chain"u8),
                    NamespaceMemberStatus.Active, 11, 0);
                Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(staleEpochGap, keys[0].Seed), trust));

                NamespaceRosterCertificate staleJoiningGap = MakeRoster(
                    4, SHA256.HashData("omitted roster chain"u8),
                    NamespaceMemberStatus.Joining, 11, 0);
                Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(staleJoiningGap, keys[0].Seed), trust));

                // Retired -> Joining requires an epoch strictly beyond the retired
                // transport fence. The next signed generation gap may attest the
                // omitted FULL_REPLACE promotion and activate that same epoch.
                NamespaceRosterCertificate rejoiningJoin = MakeRoster(
                    4, SHA256.HashData("omitted roster chain"u8),
                    NamespaceMemberStatus.Joining, 12, 0);
                rejoiningJoin = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(rejoiningJoin, keys[0].Seed), trust);
                NamespaceRosterCertificate regressedPromotion = MakeRoster(
                    6, NamespaceRosterProtocol.ComputeDigest(rejoiningJoin),
                    NamespaceMemberStatus.Active, 11, 0);
                Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(regressedPromotion, keys[0].Seed), trust));
                NamespaceRosterCertificate rejoined = MakeRoster(
                    6, NamespaceRosterProtocol.ComputeDigest(rejoiningJoin),
                    NamespaceMemberStatus.Active, 12, 0);
                rejoined = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(rejoined, keys[0].Seed), trust);

                // Old generation reports/checkpoints/ACKs are not inherited by the
                // rejoined generation. Current evidence must be re-established.
                Assert.Empty(gc.ReadOriginFrontiers(rejoined));
                Assert.False(gc.TryReadRetentionCheckpoint(rejoined, trust, out _));
                Assert.Empty(gc.ReadFullReplaceInstalledAcks(rejoined, generation));
                Assert.False(gc.IsOriginCoveredByRetentionCheckpoint(rejoined, trust, rejoining, 3, 40));
                Assert.False(gc.AreAllActiveMembersInstalled(rejoined, generation, replaceDigest));
                Assert.False(gc.HasProtocolTombstoneGcProof(
                    rejoined, trust, rejoining, 3, 40, 100, 2, generation, replaceDigest));

                // Active -> Joining is accepted only across a signed generation gap
                // with a strictly advanced transport epoch. In the same skipped
                // roster, a newly seen member may already be Retired.
                NamespaceRosterCertificate staleActiveJoin = MakeRoster(
                    8, NamespaceRosterProtocol.ComputeDigest(rejoined),
                    NamespaceMemberStatus.Joining, 12, 0);
                Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(staleActiveJoin, keys[0].Seed), trust));

                NodeId newRetiredMember = NodeId.From(Id(3));
                var newRetiredKey = BatchSignature.GenerateKeyPair();
                NamespaceRosterCertificate activeToJoiningWithRetiredJoiner = new()
                {
                    NamespaceId = 19,
                    RosterGeneration = 8,
                    PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(rejoined),
                    AuthorityNodeId = authority,
                    Members = new[]
                    {
                        new NamespaceRosterMember
                        {
                            NodeId = authority,
                            PublicKey = keys[0].PublicKey,
                            TransportPeerEpoch = 10,
                            Status = NamespaceMemberStatus.Active
                        },
                        new NamespaceRosterMember
                        {
                            NodeId = rejoining,
                            PublicKey = keys[1].PublicKey,
                            TransportPeerEpoch = 13,
                            Status = NamespaceMemberStatus.Joining
                        },
                        new NamespaceRosterMember
                        {
                            NodeId = newRetiredMember,
                            PublicKey = newRetiredKey.PublicKey,
                            TransportPeerEpoch = 5,
                            Status = NamespaceMemberStatus.Retired,
                            RetiredThroughEpoch = 5
                        }
                    },
                    RetiredOriginEpochs = closure
                };
                activeToJoiningWithRetiredJoiner = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(activeToJoiningWithRetiredJoiner, keys[0].Seed), trust);

                // A later signed gap may attest Joining -> Active and Retired ->
                // Active, but neither transition may regress its transport epoch.
                NamespaceRosterCertificate allActive = new()
                {
                    NamespaceId = 19,
                    RosterGeneration = 10,
                    PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(activeToJoiningWithRetiredJoiner),
                    AuthorityNodeId = authority,
                    Members = new[]
                    {
                        new NamespaceRosterMember
                        {
                            NodeId = authority,
                            PublicKey = keys[0].PublicKey,
                            TransportPeerEpoch = 10,
                            Status = NamespaceMemberStatus.Active
                        },
                        new NamespaceRosterMember
                        {
                            NodeId = rejoining,
                            PublicKey = keys[1].PublicKey,
                            TransportPeerEpoch = 13,
                            Status = NamespaceMemberStatus.Active
                        },
                        new NamespaceRosterMember
                        {
                            NodeId = newRetiredMember,
                            PublicKey = newRetiredKey.PublicKey,
                            TransportPeerEpoch = 6,
                            Status = NamespaceMemberStatus.Active
                        }
                    },
                    RetiredOriginEpochs = closure
                };
                allActive = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(allActive, keys[0].Seed), trust);

                // Retired-origin closures are immutable even across signed gaps;
                // member key identity is likewise immutable without a dedicated
                // authenticated key-rotation transition.
                NamespaceRosterCertificate deletedClosure = new()
                {
                    NamespaceId = 19,
                    RosterGeneration = 12,
                    PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(allActive),
                    AuthorityNodeId = authority,
                    Members = allActive.Members,
                    RetiredOriginEpochs = Array.Empty<NamespaceRetiredOriginEpoch>()
                };
                Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(deletedClosure, keys[0].Seed), trust));

                NamespaceRosterCertificate rotatedKey = new()
                {
                    NamespaceId = 19,
                    RosterGeneration = 12,
                    PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(allActive),
                    AuthorityNodeId = authority,
                    Members = new[]
                    {
                        allActive.Members[0],
                        new NamespaceRosterMember
                        {
                            NodeId = rejoining,
                            PublicKey = BatchSignature.GenerateKeyPair().PublicKey,
                            TransportPeerEpoch = 13,
                            Status = NamespaceMemberStatus.Active
                        },
                        allActive.Members[2]
                    },
                    RetiredOriginEpochs = closure
                };
                Assert.Throws<InvalidDataException>(() => gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(rotatedKey, keys[0].Seed), trust));

                Assert.Empty(gc.ReadOriginFrontiers(allActive));
                Assert.False(gc.TryReadRetentionCheckpoint(allActive, trust, out _));
                Assert.Empty(gc.ReadFullReplaceInstalledAcks(allActive, generation));
                Assert.False(gc.IsOriginCoveredByRetentionCheckpoint(allActive, trust, rejoining, 3, 40));
                Assert.False(gc.AreAllActiveMembersInstalled(allActive, generation, replaceDigest));
                Assert.False(gc.HasProtocolTombstoneGcProof(
                    allActive, trust, rejoining, 3, 40, 100, 2, generation, replaceDigest));
            }

            using (var reopened = StorageDatabase.Open(directory, 904))
            {
                var gc = new StorageNamespaceGcStore(reopened, "fld-db/offline-rejoin", NodeId.From(Id(99)));
                Assert.True(gc.TryReadNamespaceRoster(19, trust, out var current));
                Assert.NotNull(current);
                Assert.Equal(10UL, current!.RosterGeneration);
                Assert.Empty(gc.ReadOriginFrontiers(current));
                Assert.False(gc.TryReadRetentionCheckpoint(current, trust, out _));
                Assert.False(gc.AreAllActiveMembersInstalled(current, generation, replaceDigest));
                Assert.False(gc.IsOriginCoveredByRetentionCheckpoint(current, trust, rejoining, 3, 40));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Member_baseline_reports_require_installed_ack_and_survive_reopen()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-store-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        NamespaceRosterCertificate roster = Roster(1, new byte[32], keys);
        Guid generation = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("mbr1 snapshot"));
        byte[] member0 = SignedBaselineReport(roster, 0, keys, generation, digest, 1, 10, 11, 0x21);
        byte[] member1 = SignedBaselineReport(roster, 1, keys, generation, digest, 1, 20, 21, 0x41);

        try
        {
            using (var storage = StorageDatabase.Open(directory, 905))
            {
                var gc = new StorageNamespaceGcStore(storage, "fld-db/mbr1", NodeId.From(Id(99)));
                roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
                Assert.Throws<InvalidDataException>(() =>
                    gc.AcceptMetadataBaselineReport(member0, roster, generation, digest));

                AcceptInstalledAcks(gc, roster, keys, generation, digest);
                var accepted0 = gc.AcceptMetadataBaselineReport(member0, roster, generation, digest);
                Assert.Equal(roster.Members[0].NodeId, accepted0.MemberNodeId);
                Assert.Equal(member0, MetadataBaselineReportProtocol.Sign(accepted0, keys[0].Seed));
                gc.AcceptMetadataBaselineReport(member0, roster, generation, digest);
                gc.AcceptMetadataBaselineReport(member1, roster, generation, digest);

                var reports = gc.ReadMetadataBaselineReports(roster, generation, digest);
                Assert.Equal(2, reports.Count);
                Assert.Equal(new[] { roster.Members[0].NodeId, roster.Members[1].NodeId },
                    reports.Select(report => report.MemberNodeId).ToArray());
            }

            using (var reopened = StorageDatabase.Open(directory, 905))
            {
                var gc = new StorageNamespaceGcStore(reopened, "fld-db/mbr1", NodeId.From(Id(99)));
                Assert.True(gc.TryReadNamespaceRoster(19, trust, out var restoredRoster));
                Assert.NotNull(restoredRoster);
                var reports = gc.ReadMetadataBaselineReports(restoredRoster!, generation, digest);
                Assert.Equal(2, reports.Count);
                Assert.Equal(1UL, reports[0].ReportSequence);
                Assert.Equal(10UL, reports[0].FeedEntries[0].AbsoluteFeedOffset);
                Assert.Equal(11UL, reports[0].ReceiveEntries[0].ReceiveCursor);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Member_baseline_reports_reject_sequence_forks_and_cursor_regressions()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-mbr1-fence-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        NamespaceRosterCertificate roster = Roster(1, new byte[32], keys);
        Guid generation = Guid.Parse("bbbbbbbb-cccc-dddd-eeee-ffffffffffff");
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes("mbr1 monotonic snapshot"));

        try
        {
            using var storage = StorageDatabase.Open(directory, 906);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/mbr1-fence", NodeId.From(Id(99)));
            roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);
            AcceptInstalledAcks(gc, roster, keys, generation, digest);
            byte[] initial = SignedBaselineReport(roster, 0, keys, generation, digest, 1, 10, 10, 0x31);
            gc.AcceptMetadataBaselineReport(initial, roster, generation, digest);

            byte[] fork = SignedBaselineReport(roster, 0, keys, generation, digest, 1, 10, 10, 0x51);
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptMetadataBaselineReport(fork, roster, generation, digest));

            byte[] feedRegression = SignedBaselineReport(roster, 0, keys, generation, digest, 2, 9, 11, 0x31);
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptMetadataBaselineReport(feedRegression, roster, generation, digest));

            byte[] receiveRegression = SignedBaselineReport(roster, 0, keys, generation, digest, 2, 11, 9, 0x31);
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptMetadataBaselineReport(receiveRegression, roster, generation, digest));

            byte[] equalCursorDigestFork = SignedBaselineReport(roster, 0, keys, generation, digest, 2, 10, 10, 0x71);
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptMetadataBaselineReport(equalCursorDigestFork, roster, generation, digest));

            byte[] epochChangeWithOldDigest = SignedBaselineReport(
                roster, 0, keys, generation, digest, 2, 11, 11, 0x31, issuerEpoch: 8);
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptMetadataBaselineReport(epochChangeWithOldDigest, roster, generation, digest));

            byte[] epochChangeWithResetDigest = SignedBaselineReport(
                roster, 0, keys, generation, digest, 2, 11, 11, 0x00, issuerEpoch: 8);
            gc.AcceptMetadataBaselineReport(epochChangeWithResetDigest, roster, generation, digest);

            var only = Assert.Single(gc.ReadMetadataBaselineReports(roster, generation, digest));
            Assert.Equal(2UL, only.ReportSequence);
            Assert.Equal(8U, only.FeedEntries[0].IssuerEpoch);
            Assert.Equal(11UL, only.FeedEntries[0].AbsoluteFeedOffset);
            Assert.Equal(11UL, only.ReceiveEntries[0].ReceiveCursor);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Storage_rejects_frontier_regression_checkpoint_overclaim_and_roster_fork()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-ns-fence-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        var roster = Roster(1, new byte[32], keys);

        try
        {
            using var storage = StorageDatabase.Open(directory, 903);
            var gc = new StorageNamespaceGcStore(storage, "fld-db/fence", NodeId.From(Id(99)));
            roster = gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, keys[0].Seed), trust);

            gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
            gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 2, 89), roster));

            var reports = gc.ReadOriginFrontiers(roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], reports);
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);

            var overclaim = new NamespaceRetentionCheckpoint
            {
                NamespaceId = roster.NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                CheckpointSequence = 2,
                PreviousCheckpointDigest = NamespaceRetentionCheckpointProtocol.ComputeDigest(checkpoint),
                SafeFrontier = new[]
                {
                    new RetentionFrontierEntry
                    {
                        OriginAuthorNodeId = NodeId.From(Id(9)),
                        OriginAuthorEpoch = 3,
                        SafeAppliedOriginSequence = 100
                    }
                }
            };
            Assert.Throws<InvalidDataException>(() => gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(overclaim, keys[0].Seed), roster, trust));

            var fork = Roster(1, new byte[32], keys,
                new[] { NamespaceMemberStatus.Active, NamespaceMemberStatus.Joining });
            Assert.Throws<InvalidDataException>(() =>
                gc.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(fork, keys[0].Seed), trust));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Change_id_gc_provider_scan_retains_unpublished_feed_identities_across_reopen_and_releases_only_acknowledged_entries()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-changeid-provider-gc-" + Guid.NewGuid().ToString("N"));
        NodeId local = NodeId.From(Id(0x71));
        BatchId first = BatchId.From(Id(0x72));
        BatchId second = BatchId.From(Id(0x73));

        try
        {
            using (var storage = StorageDatabase.Open(directory, 911))
            using (var provider = new StorageDataProvider(storage))
            {
                provider.AppendLocalChange(new ProviderMutation
                {
                    RecordId = RecordId.From(Id(0x74)),
                    FieldId = 1,
                    HlcWall = 10,
                    AuthorNodeId = local,
                    OriginAuthorEpoch = 3,
                    Value = new byte[] { 0xA1 },
                    ChangeId = first
                });
                provider.AppendLocalChange(new ProviderMutation
                {
                    RecordId = RecordId.From(Id(0x74)),
                    FieldId = 2,
                    HlcWall = 11,
                    AuthorNodeId = local,
                    OriginAuthorEpoch = 3,
                    Value = new byte[] { 0xA2 },
                    ChangeId = second
                });

                Assert.True(StorageMetadataStore.TryCollectProviderChangeIdsForGc(
                    provider, provider.LocalChangeHighWatermark, out HashSet<BatchId> live));
                Assert.Equal(new HashSet<BatchId> { first, second }, live);
            }

            using (var storage = StorageDatabase.Open(directory, 911))
            using (var provider = new StorageDataProvider(storage))
            {
                // Acknowledgement is legal only after the complete source prefix was
                // scanned. Once publication has consumed the feed entries, those rows
                // are no longer unpublished host references to the ChangeId ledger.
                ProviderChangePage page = provider.ReadLocalChanges(0, 16);
                Assert.Equal(2UL, page.ScannedThroughSequence);
                Assert.Equal(new[] { first, second }, page.Changes.Select(change => change.ChangeId));
                provider.AcknowledgeLocalChanges(2);

                Assert.True(StorageMetadataStore.TryCollectProviderChangeIdsForGc(
                    provider, provider.LocalChangeHighWatermark, out HashSet<BatchId> live));
                Assert.Empty(live);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Change_id_gc_keeps_pushed_peer_reservation_across_reopen_until_acknowledged()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-changeid-reservation-reopen-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        const string scope = "changeid-reservation-reopen";
        const int namespaceId = 19;
        NodeId local = NodeId.From(Id(0x7F));
        NodeId sender = NodeId.From(Id(2));
        NodeId downstream = NodeId.From(Id(3));
        BatchId changeId = BatchId.From(Id(0x74));
        var reservation = new MetadataOutboundBatchReservation
        {
            BatchId = BatchId.From(Id(0x75)),
            IssuerEpoch = 3,
            CursorStart = 1,
            CursorEnd = 1,
            PreviousBatchDigest = new byte[16],
            BatchDigest = Enumerable.Repeat((byte)0x76, 16).ToArray(),
            Payload = new byte[] { 0x77 }
        };

        IApplicationGcReferenceScanner Scanner()
            => new DelegateApplicationGcReferenceScanner((_, _, _) =>
                new DelegateApplicationGcReferenceScan(
                    new MemoryStream(),
                    new ApplicationDraftHorizon(0, 0, 0),
                    (_, _, _, _) => false,
                    _ => false,
                    (_, _) => false));

        try
        {
            using (var storage = StorageDatabase.Open(directory, 913))
            using (var metadata = new StorageMetadataStore(storage))
            using (var provider = new StorageDataProvider(storage))
            {
                var gc = new StorageNamespaceGcStore(
                    storage, scope, local,
                    new StorageNamespaceGcFeatureOptions(EnableChangeIdGc: true));
                NamespaceRosterCertificate roster = gc.AcceptNamespaceRoster(
                    NamespaceRosterProtocol.Sign(Roster(1, new byte[32], keys), keys[0].Seed), trust);
                metadata.AttachNamespaceGcGuard(gc, trust, scope, local);

                metadata.RegisterInterestedPeer(scope, local, sender, 1, namespaceId);
                var incoming = new MetadataIncomingBatch
                {
                    ScopeId = scope,
                    LocalReplica = local,
                    SenderPeer = sender,
                    SenderEpoch = 3,
                    TransportPeerEpoch = 1,
                    AuthorStreamNodeId = sender,
                    NamespaceId = namespaceId,
                    BatchId = BatchId.From(Id(0x78)),
                    CursorStart = 1,
                    CursorEnd = 1,
                    PreviousBatchDigest = new byte[16],
                    BatchDigest = Enumerable.Repeat((byte)0x79, 16).ToArray(),
                    HeaderFingerprint = Enumerable.Repeat((byte)0x7A, 16).ToArray(),
                    Operations = new[]
                    {
                        new MetadataOperation
                        {
                            AuthorNodeId = sender,
                            ChangeId = changeId,
                            OriginAuthorEpoch = 3,
                            OriginSequence = 50,
                            Fingerprint = Enumerable.Repeat((byte)0x7B, 16).ToArray(),
                            Payload = new byte[] { 0x7C },
                            RelayEligible = false
                        }
                    }
                };
                Assert.False(metadata.AcceptIncomingBatch(incoming).IsDuplicate);

                var report0 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
                var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
                var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                    roster, 1, new byte[32], new[] { report0, report1 });
                gc.AcceptRetentionCheckpoint(
                    NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);
                Assert.True(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, sender, 3, 50));

                metadata.RegisterInterestedPeer(scope, local, downstream, 1, namespaceId);
                metadata.CompletePeerInitialSync(scope, local, downstream, 1, namespaceId,
                    new[] { new MetadataPeerStreamBaseline(sender, 0, 3) });
                metadata.ReserveOutboundBatch(scope, local, downstream, 1,
                    namespaceId, sender, reservation);
                metadata.MarkOutboundBatchPushed(scope, local, downstream, 1,
                    namespaceId, sender, reservation.BatchId);

                Assert.Equal(0, metadata.CompactChangeLedger(
                    scope, local, namespaceId, provider,
                    16, () => new MemoryStream(), Scanner()));
            }

            // The immutable reservation and its pushed cursor must survive a
            // process/store restart; the signed origin checkpoint is durable too.
            using (var reopenedStorage = StorageDatabase.Open(directory, 913))
            using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
            using (var reopenedProvider = new StorageDataProvider(reopenedStorage))
            {
                var reopenedGc = new StorageNamespaceGcStore(
                    reopenedStorage, scope, local,
                    new StorageNamespaceGcFeatureOptions(EnableChangeIdGc: true));
                reopenedMetadata.AttachNamespaceGcGuard(reopenedGc, trust, scope, local);
                var state = reopenedMetadata.GetPeerStreamState(
                    scope, local, downstream, 1, namespaceId, sender);
                Assert.Equal(0UL, state.AckedCursor);
                Assert.Equal(1UL, state.PushedCursor);
                Assert.NotNull(state.PendingBatch);
                Assert.True(reopenedMetadata.TryGetChangeIdentity(
                    scope, local, namespaceId, changeId, out _, out _, out _));
                Assert.Equal(0, reopenedMetadata.CompactChangeLedger(
                    scope, local, namespaceId, reopenedProvider,
                    16, () => new MemoryStream(), Scanner()));

                reopenedMetadata.AcknowledgePeerBatch(scope, local, downstream, 1,
                    namespaceId, sender, reservation.BatchId, 1, reservation.BatchDigest.ToArray());
                Assert.Equal(1UL, reopenedMetadata.GetPeerStreamState(
                    scope, local, downstream, 1, namespaceId, sender).AckedCursor);
                Assert.Equal(1, reopenedMetadata.CompactChangeLedger(
                    scope, local, namespaceId, reopenedProvider,
                    16, () => new MemoryStream(), Scanner()));
                Assert.False(reopenedMetadata.TryGetChangeIdentity(
                    scope, local, namespaceId, changeId, out _, out _, out _));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Change_id_gc_requires_signed_origin_horizon_and_empty_external_references_and_keeps_replay_fence()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-changeid-gc-proof-" + Guid.NewGuid().ToString("N"));
        var keys = new[] { BatchSignature.GenerateKeyPair(), BatchSignature.GenerateKeyPair() };
        var trust = new TrustStore();
        trust.Allow(NodeId.From(Id(1)), keys[0].PublicKey, TrustMode.PreShared);
        const string scope = "changeid-gc-proof";
        const int namespaceId = 19;
        NodeId local = NodeId.From(Id(0x7F));
        NodeId sender = NodeId.From(Id(2));
        BatchId changeId = BatchId.From(Id(0x67));
        BatchId secondChangeId = BatchId.From(Id(0x68));
        BatchId thirdChangeId = BatchId.From(Id(0x69));
        var changeIds = new HashSet<BatchId> { changeId, secondChangeId, thirdChangeId };
        MetadataIncomingBatch? staleReplay = null;

        IApplicationGcReferenceScanner Scanner(bool hasChangeIdReference)
            => new DelegateApplicationGcReferenceScanner((_, _, _) =>
                new DelegateApplicationGcReferenceScan(
                    new MemoryStream(),
                    new ApplicationDraftHorizon(0, 0, 0),
                    (_, _, _, _) => false,
                    id => hasChangeIdReference && changeIds.Contains(id),
                    (_, _) => false));

        try
        {
            using (var storage = StorageDatabase.Open(directory, 912))
            using (var metadata = new StorageMetadataStore(storage))
            using (var provider = new StorageDataProvider(storage))
            {
            var gc = new StorageNamespaceGcStore(
                storage, scope, local,
                new StorageNamespaceGcFeatureOptions(EnableChangeIdGc: true));
            NamespaceRosterCertificate roster = gc.AcceptNamespaceRoster(
                NamespaceRosterProtocol.Sign(Roster(1, new byte[32], keys), keys[0].Seed), trust);
            metadata.AttachNamespaceGcGuard(gc, trust, scope, local);

            metadata.RegisterInterestedPeer(scope, local, sender, 1, namespaceId);
            var incoming = new MetadataIncomingBatch
            {
                ScopeId = scope,
                LocalReplica = local,
                SenderPeer = sender,
                SenderEpoch = 3,
                TransportPeerEpoch = 1,
                AuthorStreamNodeId = sender,
                NamespaceId = namespaceId,
                BatchId = BatchId.From(Id(0x66)),
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)0x68, 16).ToArray(),
                HeaderFingerprint = Enumerable.Repeat((byte)0x69, 16).ToArray(),
                Operations = new[]
                {
                    new MetadataOperation
                    {
                        AuthorNodeId = sender,
                        ChangeId = changeId,
                        OriginAuthorEpoch = 3,
                        OriginSequence = 50,
                        Fingerprint = Enumerable.Repeat((byte)0x6A, 16).ToArray(),
                        Payload = new byte[] { 0x6B },
                        RelayEligible = true
                    },
                    new MetadataOperation
                    {
                        AuthorNodeId = sender,
                        ChangeId = secondChangeId,
                        OriginAuthorEpoch = 3,
                        OriginSequence = 51,
                        Fingerprint = Enumerable.Repeat((byte)0x6F, 16).ToArray(),
                        Payload = new byte[] { 0x70 },
                        RelayEligible = false
                    },
                    new MetadataOperation
                    {
                        AuthorNodeId = sender,
                        ChangeId = thirdChangeId,
                        OriginAuthorEpoch = 3,
                        OriginSequence = 52,
                        Fingerprint = Enumerable.Repeat((byte)0x71, 16).ToArray(),
                        Payload = new byte[] { 0x72 },
                        RelayEligible = false
                    }
                }
            };
            Assert.False(metadata.AcceptIncomingBatch(incoming).IsDuplicate);
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, namespaceId, changeId, out _, out OperationOrigin origin, out _));
            Assert.Equal(sender, origin.AuthorNodeId);
            Assert.Equal(3U, origin.AuthorEpoch);
            Assert.Equal(50UL, origin.Sequence);
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, namespaceId, secondChangeId, out _, out OperationOrigin secondOrigin, out _));
            Assert.Equal(51UL, secondOrigin.Sequence);
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, namespaceId, thirdChangeId, out _, out OperationOrigin thirdOrigin, out _));
            Assert.Equal(52UL, thirdOrigin.Sequence);

            // The signed all-active-member horizon is the first independent gate.
            var report0 = gc.AcceptOriginFrontier(SignedReport(roster, 0, keys, 1, 100), roster);
            var report1 = gc.AcceptOriginFrontier(SignedReport(roster, 1, keys, 1, 90), roster);
            var checkpoint = NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[32], new[] { report0, report1 });
            gc.AcceptRetentionCheckpoint(
                NamespaceRetentionCheckpointProtocol.Sign(checkpoint, keys[0].Seed), roster, trust);
            Assert.True(gc.IsOriginCoveredByRetentionCheckpoint(roster, trust, sender, 3, 50));

            // An interested downstream peer receives the immutable relay stream.
            // Its pushed-but-unacknowledged reservation is an independent namespace
            // GC barrier, even though the origin horizon is already signed.
            NodeId downstream = NodeId.From(Id(3));
            metadata.RegisterInterestedPeer(scope, local, downstream, 1, namespaceId);
            metadata.CompletePeerInitialSync(scope, local, downstream, 1, namespaceId,
                new[] { new MetadataPeerStreamBaseline(sender, 0, 3) });
            var reservation = new MetadataOutboundBatchReservation
            {
                BatchId = BatchId.From(Id(0x70)),
                IssuerEpoch = 3,
                CursorStart = 1,
                CursorEnd = 1,
                PreviousBatchDigest = new byte[16],
                BatchDigest = Enumerable.Repeat((byte)0x71, 16).ToArray(),
                Payload = new byte[] { 0x72 }
            };
            metadata.ReserveOutboundBatch(scope, local, downstream, 1,
                namespaceId, sender, reservation);
            metadata.MarkOutboundBatchPushed(scope, local, downstream, 1,
                namespaceId, sender, reservation.BatchId);
            var reservedState = metadata.GetPeerStreamState(
                scope, local, downstream, 1, namespaceId, sender);
            Assert.Equal(0UL, reservedState.AckedCursor);
            Assert.Equal(1UL, reservedState.PushedCursor);
            Assert.NotNull(reservedState.PendingBatch);
            Assert.Equal(0, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                16, () => new MemoryStream(), Scanner(hasChangeIdReference: false)));

            metadata.AcknowledgePeerBatch(scope, local, downstream, 1,
                namespaceId, sender, reservation.BatchId, 1, reservation.BatchDigest.ToArray());
            Assert.Equal(1UL, metadata.GetPeerStreamState(
                scope, local, downstream, 1, namespaceId, sender).AckedCursor);

            // Relay payload remains an independent live reference after peer ACK.
            // It is released only when the relay consumer ACKs and bounded
            // payload compaction completes.
            Assert.Equal(0, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                16, () => new MemoryStream(), Scanner(hasChangeIdReference: false)));
            metadata.AcknowledgeRelay(scope, local, namespaceId, sender, 1);
            Assert.Equal(1, metadata.CompactRelayThroughBounded(
                scope, local, namespaceId, sender, 1, maxEntries: 16));

            // A provider snapshot pins the exact source generation and its ChangeId
            // provenance. GC must fail closed while the snapshot handle is alive,
            // even if the application scanner reports no external reference.
            ProviderSnapshotHandle providerSnapshot = provider.OpenSnapshot();
            Assert.Equal(0, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                16, () => new MemoryStream(), Scanner(hasChangeIdReference: false)));
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, namespaceId, changeId, out _, out _, out _));
            provider.ReleaseSnapshot(providerSnapshot.Token);

            // A host-owned immutable snapshot reference must also block reclamation
            // after the signed causal horizon has advanced.
            Assert.Equal(0, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                16, () => new MemoryStream(), Scanner(hasChangeIdReference: true)));
            Assert.True(metadata.TryGetChangeIdentity(
                scope, local, namespaceId, changeId, out _, out _, out _));

            // Once the host confirms no reference remains, the ledger row can be
            // removed. The signed checkpoint remains the durable stale-replay fence.
            // Reclaim a single ledger identity per durable transaction. Repeated
            // bounded calls must make monotonic progress without skipping candidates.
            int CountLiveChangeIds() => changeIds.Count(id =>
                metadata.TryGetChangeIdentity(scope, local, namespaceId, id, out _, out _, out _));
            Assert.Equal(1, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                1, () => new MemoryStream(), Scanner(hasChangeIdReference: false)));
            Assert.Equal(2, CountLiveChangeIds());
            Assert.Equal(1, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                1, () => new MemoryStream(), Scanner(hasChangeIdReference: false)));
            Assert.Equal(1, CountLiveChangeIds());
            Assert.Equal(1, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                1, () => new MemoryStream(), Scanner(hasChangeIdReference: false)));
            Assert.Equal(0, CountLiveChangeIds());
            Assert.Equal(0, metadata.CompactChangeLedger(
                scope, local, namespaceId, provider,
                1, () => new MemoryStream(), Scanner(hasChangeIdReference: false)));

            staleReplay = new MetadataIncomingBatch
            {
                ScopeId = scope,
                LocalReplica = local,
                SenderPeer = sender,
                SenderEpoch = 3,
                TransportPeerEpoch = 1,
                AuthorStreamNodeId = sender,
                NamespaceId = namespaceId,
                BatchId = BatchId.From(Id(0x6C)),
                CursorStart = 2,
                CursorEnd = 2,
                PreviousBatchDigest = incoming.BatchDigest.ToArray(),
                BatchDigest = Enumerable.Repeat((byte)0x6D, 16).ToArray(),
                HeaderFingerprint = Enumerable.Repeat((byte)0x6E, 16).ToArray(),
                Operations = incoming.Operations
            };
            // The replay fence must outlive the process-local metadata objects.
            // Reopen the same durable Storage partition before attempting the stale operation.
            }

            using (var reopenedStorage = StorageDatabase.Open(directory, 912))
            using (var reopenedMetadata = new StorageMetadataStore(reopenedStorage))
            {
                var reopenedGc = new StorageNamespaceGcStore(
                    reopenedStorage, scope, local,
                    new StorageNamespaceGcFeatureOptions(EnableChangeIdGc: true));
                reopenedMetadata.AttachNamespaceGcGuard(reopenedGc, trust, scope, local);
                Assert.False(reopenedMetadata.TryGetChangeIdentity(
                    scope, local, namespaceId, changeId, out _, out _, out _));
                Assert.Throws<InvalidDataException>(() =>
                    reopenedMetadata.AcceptIncomingBatch(staleReplay!));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }

    }


}
