using System.Buffers.Binary;
using BinSync;
using BinSync.Storage;
using BenchmarkDotNet.Attributes;
using Fld1.Storage.Engine;

namespace BinSync.Bench;

/// <summary>
/// Measures the durable ChangeId-GC scan and bounded-delete path against a
/// growing namespace. Each iteration builds a real Storage Metadata partition,
/// signs a v3.1 all-member origin horizon, and seeds incoming ChangeId rows.
/// The benchmark invocation itself performs exactly one bounded compaction page.
/// </summary>
[MemoryDiagnoser]
[InvocationCount(1)]
[UnrollFactor(1)]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MetadataChangeIdGcBench
{
    private const string Scope = "metadata-changeid-gc-bench";
    private const int NamespaceId = 19;
    private const int BatchSize = 256;
    private const int GcPageLimit = 4096;
    private static readonly NodeId Local = NodeId.From(IdBytes(0x7F));
    private static readonly NodeId Authority = NodeId.From(IdBytes(1));
    private static readonly NodeId Author = NodeId.From(IdBytes(2));

    private StorageDatabase _storage = null!;
    private StorageMetadataStore _metadata = null!;
    private StorageDataProvider _provider = null!;
    private DelegateApplicationGcReferenceScanner _scanner = null!;
    private int _expectedReclaimed;
    private string _directory = string.Empty;

    [Params(1_000, 10_000, 50_000)]
    public int ExistingChangeIds { get; set; }

    [Params(16, 4_096)]
    public int EligibleChangeIds { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(),
            "binsync-changeid-gc-bench-" + Guid.NewGuid().ToString("N"));
        _storage = StorageDatabase.Open(_directory, 713);
        _metadata = new StorageMetadataStore(_storage);
        _provider = new StorageDataProvider(_storage);

        var authorityKeys = BatchSignature.GenerateKeyPair();
        var authorKeys = BatchSignature.GenerateKeyPair();
        var trust = new TrustStore();
        trust.Allow(Authority, authorityKeys.PublicKey, TrustMode.PreShared);
        var gcStore = new StorageNamespaceGcStore(
            _storage, Scope, Local,
            new StorageNamespaceGcFeatureOptions(EnableChangeIdGc: true));

        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = NamespaceId,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[NamespaceRosterProtocol.DigestSize],
            AuthorityNodeId = Authority,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = Authority,
                    PublicKey = authorityKeys.PublicKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                },
                new NamespaceRosterMember
                {
                    NodeId = Author,
                    PublicKey = authorKeys.PublicKey,
                    TransportPeerEpoch = 1,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        NamespaceRosterCertificate roster = gcStore.AcceptNamespaceRoster(
            NamespaceRosterProtocol.Sign(unsignedRoster, authorityKeys.Seed), trust);
        _metadata.AttachNamespaceGcGuard(gcStore, trust, Scope, Local);

        _metadata.RegisterInterestedPeer(Scope, Local, Author, 1, NamespaceId);
        SeedIncomingChangeIds();

        ulong safeFrontier = (ulong)Math.Min(EligibleChangeIds, ExistingChangeIds);
        AppliedOriginFrontierReport authorityReport = gcStore.AcceptOriginFrontier(
            OriginFrontierProtocol.Sign(new AppliedOriginFrontierReport
            {
                NamespaceId = NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = Authority,
                MemberTransportEpoch = 1,
                ReportSequence = 1,
                Entries = new[]
                {
                    new AppliedOriginFrontierEntry
                    {
                        OriginAuthorNodeId = Author,
                        OriginAuthorEpoch = 1,
                        ContiguousAppliedOriginSequence = safeFrontier
                    }
                }
            }, authorityKeys.Seed), roster);
        AppliedOriginFrontierReport authorReport = gcStore.AcceptOriginFrontier(
            OriginFrontierProtocol.Sign(new AppliedOriginFrontierReport
            {
                NamespaceId = NamespaceId,
                RosterGeneration = roster.RosterGeneration,
                RosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                MemberNodeId = Author,
                MemberTransportEpoch = 1,
                ReportSequence = 1,
                Entries = new[]
                {
                    new AppliedOriginFrontierEntry
                    {
                        OriginAuthorNodeId = Author,
                        OriginAuthorEpoch = 1,
                        ContiguousAppliedOriginSequence = safeFrontier
                    }
                },
                IssuedOrigins = new[]
                {
                    new IssuedOriginFrontierEntry
                    {
                        OriginAuthorEpoch = 1,
                        IssuedOriginSequence = (ulong)ExistingChangeIds,
                        MaxIssuedHlcWall = 100,
                        MaxIssuedHlcLogical = 1
                    }
                }
            }, authorKeys.Seed), roster);

        NamespaceRetentionCheckpoint checkpoint =
            NamespaceRetentionCheckpointProtocol.CreateFromReports(
                roster, 1, new byte[NamespaceRosterProtocol.DigestSize],
                new[] { authorityReport, authorReport });
        gcStore.AcceptRetentionCheckpoint(
            NamespaceRetentionCheckpointProtocol.Sign(checkpoint, authorityKeys.Seed),
            roster, trust);

        _scanner = new DelegateApplicationGcReferenceScanner((_, _, _) =>
            new DelegateApplicationGcReferenceScan(
                new MemoryStream(),
                new ApplicationDraftHorizon(0, 0, 0),
                (_, _, _, _) => false,
                _ => false,
                (_, _) => false));

        _expectedReclaimed = Math.Min(
            Math.Min(EligibleChangeIds, ExistingChangeIds), GcPageLimit);
        if (!_metadata.TryGetChangeIdentity(
                Scope, Local, NamespaceId, BatchId.From(IdBytes(100_001)),
                out _, out OperationOrigin firstOrigin, out _) ||
            firstOrigin.Sequence != 1)
            throw new InvalidOperationException("ChangeId GC benchmark setup did not persist the expected provenance.");
        if (!gcStore.IsOriginCoveredByRetentionCheckpoint(
                roster, trust, Author, 1, safeFrontier))
            throw new InvalidOperationException("ChangeId GC benchmark setup did not publish the expected signed horizon.");
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _metadata.Dispose();
        _provider.Dispose();
        _storage.Dispose();
        try { Directory.Delete(_directory, recursive: true); } catch { }
    }

    [Benchmark(Description = "Bounded ChangeId GC page / namespace cardinality and eligible-prefix density")]
    public int CompactBoundedChangeIdPage()
    {
        int reclaimed = _metadata.CompactChangeLedger(
            Scope, Local, NamespaceId, _provider, GcPageLimit,
            static () => new MemoryStream(), _scanner);
        if (reclaimed != _expectedReclaimed)
            throw new InvalidOperationException(
                $"ChangeId GC reclaimed {reclaimed}; expected {_expectedReclaimed}.");
        return reclaimed;
    }

    private void SeedIncomingChangeIds()
    {
        byte[] previousDigest = new byte[16];
        int batchIndex = 0;
        for (int start = 0; start < ExistingChangeIds; start += BatchSize)
        {
            int count = Math.Min(BatchSize, ExistingChangeIds - start);
            var operations = new MetadataOperation[count];
            for (int i = 0; i < count; i++)
            {
                int sequence = start + i + 1;
                operations[i] = new MetadataOperation
                {
                    AuthorNodeId = Author,
                    ChangeId = BatchId.From(IdBytes(100_000 + sequence)),
                    OriginAuthorEpoch = 1,
                    OriginSequence = (ulong)sequence,
                    Fingerprint = IdBytes(200_000 + sequence),
                    Payload = BitConverter.GetBytes(sequence),
                    RelayEligible = false
                };
            }

            byte[] batchDigest = IdBytes(400_000 + batchIndex);
            var batch = new MetadataIncomingBatch
            {
                ScopeId = Scope,
                LocalReplica = Local,
                SenderPeer = Author,
                SenderEpoch = 1,
                TransportPeerEpoch = 1,
                AuthorStreamNodeId = Author,
                NamespaceId = NamespaceId,
                BatchId = BatchId.From(IdBytes(300_000 + batchIndex)),
                CursorStart = (ulong)start + 1,
                CursorEnd = (ulong)(start + count),
                PreviousBatchDigest = previousDigest,
                BatchDigest = batchDigest,
                HeaderFingerprint = IdBytes(500_000 + batchIndex),
                Operations = operations
            };
            _metadata.AcceptIncomingBatch(batch);
            previousDigest = batchDigest;
            batchIndex++;
        }
    }

    private static byte[] IdBytes(long value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }
}
