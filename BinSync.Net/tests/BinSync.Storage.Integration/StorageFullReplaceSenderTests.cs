using System.Security.Cryptography;
using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageFullReplaceSenderTests
{
    private sealed class CallbackLease : IDisposable
    {
        private Action? _release;
        public CallbackLease(Action release) => _release = release;
        public void Dispose() => Interlocked.Exchange(ref _release, null)?.Invoke();
    }

    private static byte[] Id(byte value)
    {
        byte[] bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    private sealed class DurableSnapshotProvider : IDataProvider, IProviderDurableSnapshotStore, IProviderCommitGuard
    {
        private const byte SnapshotPrefix = 0xF0;
        private readonly StorageDataProvider _inner;
        private readonly StorageDatabase _storage;

        public DurableSnapshotProvider(StorageDatabase storage)
        {
            _storage = storage;
            _inner = new StorageDataProvider(storage);
        }

        public RecordId ResolveRecordId(ReadOnlyMemory<byte> sourceItemKey) =>
            _inner.ResolveRecordId(sourceItemKey);
        public ushort ResolveFieldId(ReadOnlyMemory<byte> sourceFieldKey) =>
            _inner.ResolveFieldId(sourceFieldKey);
        public void AppendLocalChange(ProviderMutation mutation) => _inner.AppendLocalChange(mutation);
        public ProviderChangePage ReadLocalChanges(ulong afterSequence, int limit) =>
            _inner.ReadLocalChanges(afterSequence, limit);
        public void AcknowledgeLocalChanges(ulong safeThroughSequence) =>
            _inner.AcknowledgeLocalChanges(safeThroughSequence);
        public ulong LocalChangeHighWatermark => _inner.LocalChangeHighWatermark;
        public IDisposable AcquireCommitGuard() => _inner.AcquireCommitGuard();
        public ProviderApplyReceipt ApplyRemoteBatch(Guid applyToken, IReadOnlyList<ProviderMutation> mutations) =>
            _inner.ApplyRemoteBatch(applyToken, mutations);
        public bool TryReadCurrent(RecordId recordId, ushort fieldId, out ProviderMutation? mutation) =>
            _inner.TryReadCurrent(recordId, fieldId, out mutation);

        public ProviderSnapshotHandle OpenSnapshot()
        {
            ProviderSnapshotHandle source = _inner.OpenSnapshot();
            try
            {
                var entries = new List<ProviderMutation>();
                int afterIndex = 0;
                while (true)
                {
                    ProviderSnapshotPage page = _inner.ReadSnapshot(source.Token, afterIndex, 512);
                    if (page.Entries.Count > 512 || page.NextIndex < afterIndex ||
                        page.NextIndex != afterIndex + page.Entries.Count)
                        throw new InvalidDataException("Inner provider returned a non-contiguous snapshot page.");
                    entries.AddRange(page.Entries);
                    if (page.IsComplete) break;
                    if (page.NextIndex <= afterIndex)
                        throw new InvalidDataException("Inner provider snapshot cursor did not advance.");
                    afterIndex = page.NextIndex;
                }

                byte[] value = EncodeSnapshot(source.ChangeFeedWatermark, entries);
                using var batch = _storage.BeginWriteBatch();
                batch.Put(SnapshotKey(source.Token), value);
                batch.Commit();
                return source;
            }
            finally
            {
                _inner.ReleaseSnapshot(source.Token);
            }
        }

        public ProviderSnapshotPage ReadSnapshot(Guid snapshotToken, int afterIndex, int limit)
        {
            if (afterIndex < 0) throw new ArgumentOutOfRangeException(nameof(afterIndex));
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            ProviderMutation[] entries = ReadSnapshotData(snapshotToken, out _);
            if (afterIndex > entries.Length)
                throw new ArgumentOutOfRangeException(nameof(afterIndex));
            ProviderMutation[] page = entries.Skip(afterIndex).Take(limit).ToArray();
            int next = checked(afterIndex + page.Length);
            return new ProviderSnapshotPage(page, next, next == entries.Length);
        }

        public void ReleaseSnapshot(Guid snapshotToken)
        {
            using var batch = _storage.BeginWriteBatch();
            batch.Delete(SnapshotKey(snapshotToken));
            batch.Commit();
        }

        public bool HasPersistedSnapshot(Guid snapshotToken)
        {
            using var snapshot = _storage.OpenSnapshot();
            return snapshot.TryGet(SnapshotKey(snapshotToken), out _);
        }

        public bool TryGetPersistedSnapshotWatermark(Guid snapshotToken, out ulong changeFeedWatermark)
        {
            using var snapshot = _storage.OpenSnapshot();
            if (!snapshot.TryGet(SnapshotKey(snapshotToken), out byte[]? value) || value is null)
            {
                changeFeedWatermark = 0;
                return false;
            }
            changeFeedWatermark = DecodeSnapshot(value).Watermark;
            return true;
        }

        public void PruneSnapshotsExcept(IReadOnlySet<Guid> activeSnapshotTokens)
        {
            ArgumentNullException.ThrowIfNull(activeSnapshotTokens);
            using var snapshot = _storage.OpenSnapshot();
            IReadOnlyList<StorageEntry> rows = snapshot.ScanPrefix(
                new[] { SnapshotPrefix }, ReadOnlySpan<byte>.Empty, 4096);
            byte[][] staleKeys = rows
                .Where(row => row.Key.Length == 17 &&
                    !activeSnapshotTokens.Contains(new Guid(row.Key.AsSpan(1, 16))))
                .Select(row => row.Key)
                .ToArray();
            if (staleKeys.Length == 0) return;
            using var batch = _storage.BeginWriteBatch();
            foreach (byte[] key in staleKeys) batch.Delete(key);
            batch.Commit();
        }

        public void Dispose() => _inner.Dispose();

        private ProviderMutation[] ReadSnapshotData(Guid token, out ulong watermark)
        {
            using var snapshot = _storage.OpenSnapshot();
            if (!snapshot.TryGet(SnapshotKey(token), out byte[]? value) || value is null)
                throw new KeyNotFoundException("Durable provider snapshot token was not found.");
            var decoded = DecodeSnapshot(value);
            watermark = decoded.Watermark;
            return decoded.Entries;
        }

        private static byte[] SnapshotKey(Guid token)
        {
            byte[] key = new byte[17];
            key[0] = SnapshotPrefix;
            token.ToByteArray().CopyTo(key, 1);
            return key;
        }

        private static byte[] EncodeSnapshot(ulong watermark, IReadOnlyList<ProviderMutation> entries)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
            writer.Write(watermark);
            writer.Write(entries.Count);
            foreach (ProviderMutation entry in entries)
            {
                byte[] encoded = ProviderMutationCodec.Encode(entry);
                writer.Write(encoded.Length);
                writer.Write(encoded);
            }
            writer.Flush();
            return stream.ToArray();
        }

        private static (ulong Watermark, ProviderMutation[] Entries) DecodeSnapshot(byte[] value)
        {
            using var stream = new MemoryStream(value, writable: false);
            using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
            ulong watermark = reader.ReadUInt64();
            int count = reader.ReadInt32();
            if (count < 0 || count > 1_000_000)
                throw new InvalidDataException("Durable test snapshot entry count is invalid.");
            var entries = new ProviderMutation[count];
            for (int i = 0; i < count; i++)
            {
                int length = reader.ReadInt32();
                if (length < ProviderMutationCodec.FixedEncodedLength || length > stream.Length - stream.Position)
                    throw new InvalidDataException("Durable test snapshot mutation length is invalid.");
                byte[] encoded = reader.ReadBytes(length);
                if (encoded.Length != length)
                    throw new InvalidDataException("Durable test snapshot mutation is truncated.");
                entries[i] = ProviderMutationCodec.Decode(encoded);
            }
            if (stream.Position != stream.Length)
                throw new InvalidDataException("Durable test snapshot contains trailing bytes.");
            return (watermark, entries);
        }
    }

    private static (TrustStore Trust, NamespaceRosterCertificate Roster, byte[] Seed, NodeId Authority)
        CreateAuthority()
    {
        var (seed, publicKey) = BatchSignature.GenerateKeyPair();
        NodeId authority = NodeId.From(Id(1));
        var trust = new TrustStore();
        trust.Allow(authority, publicKey, TrustMode.PreShared);
        var unsigned = new NamespaceRosterCertificate
        {
            NamespaceId = 23,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = authority,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = authority,
                    PublicKey = publicKey,
                    TransportPeerEpoch = 7,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        NamespaceRosterCertificate roster = NamespaceRosterProtocol.DecodeAndVerify(
            NamespaceRosterProtocol.Sign(unsigned, seed), trust);
        return (trust, roster, seed, authority);
    }

    [Fact]
    public void Source_snapshot_preparation_requires_and_holds_typed_full_replace_commit_guard()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-full-replace-admission-" + Guid.NewGuid().ToString("N"));
        const string scope = "full-replace-source-admission";
        const int partition = 904;
        var (seed, publicKey) = BatchSignature.GenerateKeyPair();
        NodeId localId = NodeId.From(Id(44));
        var trust = new TrustStore();
        trust.Allow(localId, publicKey, TrustMode.PreShared);
        var unsignedRoster = new NamespaceRosterCertificate
        {
            NamespaceId = 23,
            RosterGeneration = 1,
            PreviousRosterDigest = new byte[32],
            AuthorityNodeId = localId,
            Members = new[]
            {
                new NamespaceRosterMember
                {
                    NodeId = localId,
                    PublicKey = publicKey,
                    TransportPeerEpoch = 7,
                    Status = NamespaceMemberStatus.Active
                }
            }
        };
        NamespaceRosterCertificate roster = NamespaceRosterProtocol.DecodeAndVerify(
            NamespaceRosterProtocol.Sign(unsignedRoster, seed), trust);

        try
        {
            using var storage = StorageDatabase.Open(directory, partition);
            using var provider = new DurableSnapshotProvider(storage);
            using var metadata = new StorageMetadataStore(storage);
            using var node = new Node(localId);
            var gcStore = new StorageNamespaceGcStore(storage, scope, localId);
            _ = gcStore.AcceptNamespaceRoster(NamespaceRosterProtocol.Sign(roster, seed), trust);
            var coordinator = new StorageSyncCoordinator(node, provider, 23, metadata, scope);
            coordinator.AttachNamespaceGcGuard(gcStore, trust);

            int acquired = 0;
            int released = 0;
            using PreparedFullReplaceSnapshot prepared =
                StorageFullReplaceSender.PrepareSnapshotAndPersistUnderCommitGuard(
                    provider, trust, roster, localId,
                    Guid.Parse("90400000-0000-0000-0000-000000000001"), seed,
                    metadata, scope, localId, coordinator,
                    () =>
                    {
                        Interlocked.Increment(ref acquired);
                        return new CallbackLease(() => Interlocked.Increment(ref released));
                    },
                    baselineReportSequence: 1);

            Assert.Equal(23, prepared.NamespaceId);
            Assert.Equal(0UL, prepared.SourceChangeFeedWatermark);
            Assert.Equal(1, acquired);
            Assert.Equal(1, released);
            MetadataBaselineReport localBaseline = Assert.Single(
                gcStore.ReadMetadataBaselineReports(roster, prepared.Generation, prepared.Digest));
            Assert.Equal(localId, localBaseline.MemberNodeId);
            Assert.Equal(1UL, localBaseline.ReportSequence);
            Assert.Equal(prepared.SourceChangeFeedWatermark, localBaseline.ProviderChangeFeedWatermark);
            Assert.Equal(prepared.ExpectedEntryCount, localBaseline.SnapshotEntryCount);
            ulong baselineCommit = storage.LastCommitSeq;
            FullReplaceInstalledAck localAck = Assert.Single(
                gcStore.ReadFullReplaceInstalledAcks(roster, prepared.Generation));
            MetadataBaselineReport replayedBaseline = metadata.AcceptLocalFullReplaceAndCreateBaselineReport(
                scope, localId, roster, localAck, prepared.ExpectedEntryCount,
                prepared.SourceChangeFeedWatermark, localBaseline.ReportSequence, seed);
            Assert.Equal(
                MetadataBaselineReportProtocol.EncodeEnvelope(localBaseline),
                MetadataBaselineReportProtocol.EncodeEnvelope(replayedBaseline));
            Assert.Equal(baselineCommit, storage.LastCommitSeq);
            Assert.True(metadata.TryGetPreparedFullReplaceSnapshot(
                scope, localId, 23, prepared.Generation, out PreparedFullReplaceSnapshotState? state));
            Assert.NotNull(state);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Prepared_sender_snapshot_survives_provider_and_metadata_reopen_and_rejects_stale_roster()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-full-replace-sender-" + Guid.NewGuid().ToString("N"));
        const string scope = "full-replace-sender-restart";
        const int partition = 905;
        var (trust, roster, authoritySeed, authority) = CreateAuthority();
        Guid generation = Guid.Parse("90500000-0000-0000-0000-000000000001");
        Guid snapshotToken;
        byte[] digest;
        ulong watermark;
        ulong hlcWall;
        uint hlcLogical;

        try
        {
            using (var storage = StorageDatabase.Open(directory, partition))
            using (var provider = new DurableSnapshotProvider(storage))
            using (var metadata = new StorageMetadataStore(storage))
            {
                provider.AppendLocalChange(new ProviderMutation
                {
                    RecordId = RecordId.From(Guid.Parse("90500000-0000-0000-0000-000000000010").ToByteArray()),
                    FieldId = 0x0923,
                    AuthorNodeId = authority,
                    OriginAuthorEpoch = 1,
                    HlcWall = 1200,
                    HlcLogical = 3,
                    Value = "sender-snapshot-row"u8.ToArray()
                });

                using PreparedFullReplaceSnapshot prepared = StorageFullReplaceSender.PrepareSnapshotAndPersist(
                    provider, trust, roster, authority, generation, authoritySeed,
                    metadata, scope, authority);
                Assert.Equal(23, prepared.NamespaceId);
                Assert.Equal(1, prepared.ExpectedEntryCount);
                Assert.Equal(1UL, prepared.SourceChangeFeedWatermark);
                Assert.Equal(1200UL, prepared.HlcWallFloor);
                Assert.Equal(3U, prepared.HlcLogicalFloor);
                digest = prepared.Digest;
                watermark = prepared.SourceChangeFeedWatermark;
                hlcWall = prepared.HlcWallFloor;
                hlcLogical = prepared.HlcLogicalFloor;

                Assert.True(metadata.TryGetPreparedFullReplaceSnapshot(
                    scope, authority, 23, generation, out PreparedFullReplaceSnapshotState? state));
                Assert.NotNull(state);
                snapshotToken = state!.SnapshotToken;
                Assert.Equal(generation, state.Generation);
                Assert.Equal(watermark, state.SourceFeedWatermark);
                Assert.True(((IProviderDurableSnapshotStore)provider)
                    .TryGetPersistedSnapshotWatermark(snapshotToken, out ulong persistedWatermark));
                Assert.Equal(watermark, persistedWatermark);
                Assert.True(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));

                // Persisted Dispose detaches only the process-local handle. The
                // provider generation and sender manifest remain durable for restart.
                prepared.Dispose();
                Assert.True(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));
            }

            using (var reopened = StorageDatabase.Open(directory, partition))
            using (var provider = new DurableSnapshotProvider(reopened))
            using (var metadata = new StorageMetadataStore(reopened))
            {
                var staleRosterUnsigned = new NamespaceRosterCertificate
                {
                    NamespaceId = roster.NamespaceId,
                    RosterGeneration = 2,
                    PreviousRosterDigest = NamespaceRosterProtocol.ComputeDigest(roster),
                    AuthorityNodeId = authority,
                    Members = roster.Members
                };
                NamespaceRosterCertificate staleRoster = NamespaceRosterProtocol.DecodeAndVerify(
                    NamespaceRosterProtocol.Sign(staleRosterUnsigned, authoritySeed), trust);

                Assert.Throws<InvalidOperationException>(() =>
                    StorageFullReplaceSender.RestorePreparedSnapshot(
                        provider, metadata, scope, authority, 23, generation,
                        trust, staleRoster, authority, authoritySeed));
                Assert.True(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));

                using PreparedFullReplaceSnapshot restored = StorageFullReplaceSender.RestorePreparedSnapshot(
                    provider, metadata, scope, authority, 23, generation,
                    trust, roster, authority, authoritySeed);
                Assert.Equal(generation, restored.Generation);
                Assert.Equal(23, restored.NamespaceId);
                Assert.Equal(1, restored.ExpectedEntryCount);
                Assert.Equal(watermark, restored.SourceChangeFeedWatermark);
                Assert.Equal(hlcWall, restored.HlcWallFloor);
                Assert.Equal(hlcLogical, restored.HlcLogicalFloor);
                Assert.Equal(digest, restored.Digest);

                ProviderSnapshotPage page = provider.ReadSnapshot(snapshotToken, 0, 8);
                var row = Assert.Single(page.Entries);
                Assert.Equal("sender-snapshot-row", Encoding.UTF8.GetString(row.Value));
                Assert.True(page.IsComplete);

                StorageFullReplaceSender.ReleasePreparedSnapshot(
                    restored, provider, metadata, scope, authority);
                Assert.False(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(snapshotToken));
                Assert.False(metadata.TryGetPreparedFullReplaceSnapshot(
                    scope, authority, 23, generation, out _));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Prune_unreferenced_sender_snapshot_repairs_release_before_metadata_cleanup_crash_cut()
    {
        string directory = Path.Combine(Path.GetTempPath(), "binsync-full-replace-prune-" + Guid.NewGuid().ToString("N"));
        const string scope = "full-replace-sender-prune";
        const int partition = 906;
        var (trust, roster, authoritySeed, authority) = CreateAuthority();
        Guid generation = Guid.Parse("90600000-0000-0000-0000-000000000001");

        try
        {
            using var storage = StorageDatabase.Open(directory, partition);
            using var provider = new DurableSnapshotProvider(storage);
            using var metadata = new StorageMetadataStore(storage);
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = RecordId.From(Guid.Parse("90600000-0000-0000-0000-000000000010").ToByteArray()),
                FieldId = 0x0923,
                AuthorNodeId = authority,
                OriginAuthorEpoch = 1,
                HlcWall = 1300,
                Value = "orphaned-sender-snapshot"u8.ToArray()
            });

            using PreparedFullReplaceSnapshot prepared = StorageFullReplaceSender.PrepareSnapshotAndPersist(
                provider, trust, roster, authority, generation, authoritySeed,
                metadata, scope, authority);
            Assert.True(metadata.TryGetPreparedFullReplaceSnapshot(
                scope, authority, 23, generation, out PreparedFullReplaceSnapshotState? state));
            Assert.NotNull(state);
            Guid token = state!.SnapshotToken;
            prepared.Dispose();

            // Simulate a crash after the provider token is released but before the
            // Metadata Store sender-progress row is deleted.
            provider.ReleaseSnapshot(token);
            Assert.True(metadata.TryGetPreparedFullReplaceSnapshot(
                scope, authority, 23, generation, out _));
            Assert.False(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(token));

            StorageFullReplaceSender.PruneUnreferencedPreparedSnapshots(
                provider, metadata, scope, authority, 23);

            Assert.False(metadata.TryGetPreparedFullReplaceSnapshot(
                scope, authority, 23, generation, out _));
            Assert.False(((IProviderDurableSnapshotStore)provider).HasPersistedSnapshot(token));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }
}
