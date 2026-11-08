using System.Diagnostics;
using BinSync;
using Fld1.Storage.Engine;
using BinSync.DataProvider.Conformance;
using Xunit;

namespace BinSync.Storage.Integration;

public sealed class StorageDataProviderConformanceTests : DataProviderConformanceSuite
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "binsync-storage-provider-" + Guid.NewGuid().ToString("N"));

    protected override IDataProviderConformanceAdapter CreateProvider()
        => new StorageDataProviderConformanceAdapter(
            Path.Combine(_root, Guid.NewGuid().ToString("N")),
            partitionId: 77);

    [Fact]
    public void Local_feed_mappings_remote_receipts_and_current_state_survive_restart()
    {
        string directory = Path.Combine(_root, "restart");
        Directory.CreateDirectory(directory);
        var itemKey = new byte[] { 0xA1, 0xB2, 0xC3 };
        var fieldKey = new byte[] { 0x10, 0x20 };
        RecordId recordId;
        ushort fieldId;
        var token = Guid.Parse("00000000-0000-0000-0000-000000000301");
        var remote = Mutation(7, 3, 90, 2);

        try
        {
            using (var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 78, deleteOnDispose: false))
            {
                recordId = provider.ResolveRecordId(itemKey);
                fieldId = provider.ResolveFieldId(fieldKey);
                Assert.True(fieldId >= 0x0100);
                provider.AppendLocalChange(new ProviderMutation
                {
                    RecordId = recordId,
                    FieldId = fieldId,
                    AuthorNodeId = Node(1),
                    HlcWall = 100,
                    HlcLogical = 0,
                    KeyEpoch = 0,
                    Value = new byte[] { 42 }
                });
                provider.ApplyRemoteBatch(token, new[] { remote });
            }

            using var reopened = new StorageDataProviderConformanceAdapter(directory, partitionId: 78, deleteOnDispose: false);
            Assert.Equal(recordId, reopened.ResolveRecordId(itemKey));
            Assert.Equal(fieldId, reopened.ResolveFieldId(fieldKey));
            Assert.Single(reopened.ReadLocalChanges(0, 10).Changes);
            Assert.True(reopened.TryReadCurrent(remote.RecordId, remote.FieldId, out var current));
            Assert.Equal(remote.Value, current!.Value);
            Assert.True(reopened.ApplyRemoteBatch(token, new[] { remote }).IsDuplicate);
            Assert.Equal(1UL, reopened.LocalChangeHighWatermark);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Local_feed_change_identity_survives_reopen_and_is_removed_on_ack()
    {
        string directory = Path.Combine(_root, "feed-change-identity");
        Directory.CreateDirectory(directory);
        BatchId changeId;
        try
        {
            using (var provider = new StorageDataProvider(directory, partitionId: 82, deleteOnDispose: false))
            {
                provider.AppendLocalChange(Mutation(9, 5, 60, 1));
                ProviderChange change = Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
                Assert.False(change.ChangeId.IsZero);
                changeId = change.ChangeId;
            }

            using var reopened = new StorageDataProvider(directory, partitionId: 82, deleteOnDispose: false);
            ProviderChange replayed = Assert.Single(reopened.ReadLocalChanges(0, 10).Changes);
            Assert.Equal(changeId, replayed.ChangeId);
            Assert.Equal(1UL, replayed.Sequence);

            reopened.AcknowledgeLocalChanges(replayed.Sequence);
            Assert.Empty(reopened.ReadLocalChanges(0, 10).Changes);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Legacy_local_feed_row_gets_stable_identity_on_first_read()
    {
        string directory = Path.Combine(_root, "legacy-feed-identity");
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, partitionId: 83);
            using var provider = new StorageDataProvider(storage);
            provider.AppendLocalChange(Mutation(10, 6, 70, 1));

            byte[] feedKey = [0x20, 0, 0, 0, 0, 0, 0, 0, 1];
            Assert.True(storage.TryGet(feedKey, out var encoded));
            byte[] legacyValue = encoded![..^16];
            using (var batch = storage.BeginWriteBatch())
            {
                batch.Put(feedKey, legacyValue);
                batch.Commit();
            }

            ProviderChange first = Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
            Assert.False(first.ChangeId.IsZero);
            ProviderChange replayed = Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
            Assert.Equal(first.ChangeId, replayed.ChangeId);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Acknowledged_feed_cleanup_does_not_remove_current_array_state()
    {
        string directory = Path.Combine(_root, "ack");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 79, deleteOnDispose: false);
            var mutation = Mutation(8, 4, 55, 1);
            provider.AppendLocalChange(mutation);
            _ = provider.ReadLocalChanges(0, 10);
            provider.AcknowledgeLocalChanges(1);
            Assert.Empty(provider.ReadLocalChanges(0, 10).Changes);
            Assert.True(provider.TryReadCurrent(mutation.RecordId, mutation.FieldId, out var current));
            Assert.Equal(mutation.Value, current!.Value);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Source_feed_retry_is_idempotent_and_same_version_equivocation_fails_closed()
    {
        string directory = Path.Combine(_root, "source-retry");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 81, deleteOnDispose: false);
            var mutation = new ProviderMutation
            {
                RecordId = RecordId.From(new byte[] { 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }),
                FieldId = 0x0101,
                AuthorNodeId = NodeId.From(new byte[] { 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }),
                HlcWall = 500,
                HlcLogical = 2,
                Value = "stable"u8.ToArray()
            };

            provider.AppendLocalChange(mutation);
            provider.AppendLocalChange(mutation);
            Assert.Equal(1UL, provider.LocalChangeHighWatermark);
            Assert.Single(provider.ReadLocalChanges(0, 10).Changes);

            var equivocation = new ProviderMutation
            {
                RecordId = mutation.RecordId,
                FieldId = mutation.FieldId,
                AuthorNodeId = mutation.AuthorNodeId,
                HlcWall = mutation.HlcWall,
                HlcLogical = mutation.HlcLogical,
                Value = "conflict"u8.ToArray()
            };
            Assert.Throws<InvalidDataException>(() => provider.AppendLocalChange(equivocation));
            Assert.Equal(1UL, provider.LocalChangeHighWatermark);
            Assert.True(provider.TryReadCurrent(mutation.RecordId, mutation.FieldId, out var current));
            Assert.Equal("stable", System.Text.Encoding.UTF8.GetString(current!.Value));

            Guid firstToken = Guid.Parse("00000000-0000-0000-0000-000000000501");
            Guid conflictingToken = Guid.Parse("00000000-0000-0000-0000-000000000502");
            provider.ApplyRemoteBatch(firstToken, new[] { mutation });
            Assert.Throws<InvalidDataException>(() => provider.ApplyRemoteBatch(conflictingToken, new[] { equivocation }));
            Assert.False(provider.ApplyRemoteBatch(conflictingToken, new[] { mutation }).IsDuplicate);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Out_of_order_local_feed_keeps_current_value_at_the_lww_winner()
    {
        string directory = Path.Combine(_root, "lww-order");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProviderConformanceAdapter(directory, partitionId: 80, deleteOnDispose: false);
            RecordId record = RecordId.From(new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
            NodeId author = NodeId.From(new byte[] { 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record, FieldId = 0x0101, AuthorNodeId = author,
                HlcWall = 200, Value = "newer"u8.ToArray()
            });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record, FieldId = 0x0101, AuthorNodeId = author,
                HlcWall = 100, Value = "older"u8.ToArray()
            });

            Assert.True(provider.TryReadCurrent(record, 0x0101, out var current));
            Assert.Equal("newer", System.Text.Encoding.UTF8.GetString(current!.Value));
            Assert.Equal(2, provider.ReadLocalChanges(0, 10).Changes.Count);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Record_tombstone_hides_older_fields_but_newer_ghost_field_survives_restart_and_snapshot()
    {
        string directory = Path.Combine(_root, "record-tombstone");
        Directory.CreateDirectory(directory);
        RecordId record = RecordId.From(Guid.Parse("f1000000-0000-0000-0000-000000000001").ToByteArray());
        NodeId author = NodeId.From(Guid.Parse("f2000000-0000-0000-0000-000000000002").ToByteArray());

        using (var provider = new StorageDataProvider(directory, 82))
        {
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0101,
                AuthorNodeId = author,
                HlcWall = 1000,
                Value = "before-delete"u8.ToArray()
            });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = Model.LifecycleFieldId,
                IsRecordTombstone = true,
                AuthorNodeId = author,
                HlcWall = 2000,
                Value = Array.Empty<byte>()
            });

            Assert.False(provider.TryReadCurrent(record, 0x0101, out _));

            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0101,
                AuthorNodeId = author,
                HlcWall = 3000,
                Value = "newer-ghost"u8.ToArray()
            });
            Assert.True(provider.TryReadCurrent(record, 0x0101, out var visible));
            Assert.Equal("newer-ghost", System.Text.Encoding.UTF8.GetString(visible!.Value));

            var snapshot = provider.OpenSnapshot();
            try
            {
                var page = provider.ReadSnapshot(snapshot.Token, 0, 10);
                Assert.True(page.IsComplete);
                Assert.Contains(page.Entries, entry => entry.IsRecordTombstone);
            }
            finally
            {
                provider.ReleaseSnapshot(snapshot.Token);
            }
        }

        using (var reopened = new StorageDataProvider(directory, 82))
        {
            Assert.True(reopened.TryReadCurrent(record, 0x0101, out var visible));
            Assert.Equal("newer-ghost", System.Text.Encoding.UTF8.GetString(visible!.Value));
            Assert.Equal(3, reopened.ReadLocalChanges(0, 10).Changes.Count);
        }

        try { Directory.Delete(directory, recursive: true); } catch { }
    }


    [Fact]
    public void Metadata_store_owned_publication_cutover_marker_survives_reopen()
    {
        string directory = Path.Combine(_root, "publication-cutover-marker");
        Directory.CreateDirectory(directory);
        try
        {
            using (var provider = new StorageDataProvider(directory, 83))
            {
                provider.EnableMetadataStoreOwnedPublication(17);
                Assert.True(provider.TryGetMetadataStoreOwnedPublicationCutover(out ulong cutover));
                Assert.Equal(17UL, cutover);
            }

            using (var reopened = new StorageDataProvider(directory, 83))
            {
                Assert.True(reopened.TryGetMetadataStoreOwnedPublicationCutover(out ulong cutover));
                Assert.Equal(17UL, cutover);
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Metadata_store_owned_publication_cutover_rejects_pending_local_feed()
    {
        string directory = Path.Combine(_root, "publication-cutover-pending-feed");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProvider(directory, 84);
            provider.AppendLocalChange(Mutation(11, 7, 110, 1));

            Assert.Throws<InvalidOperationException>(() =>
                provider.EnableMetadataStoreOwnedPublication(0));
            Assert.False(provider.TryGetMetadataStoreOwnedPublicationCutover(out _));
            Assert.Single(provider.ReadLocalChanges(0, 10).Changes);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Provider_and_metadata_adapters_share_one_storage_transaction_gate()
    {
        string directory = Path.Combine(_root, "shared-storage-transaction-gate");
        Directory.CreateDirectory(directory);
        try
        {
            using var storage = StorageDatabase.Open(directory, 85);
            using var provider = new StorageDataProvider(storage);
            using var metadata = new StorageMetadataStore(storage);
            NodeId local = Node(85);
            const string scope = "shared-storage-transaction-gate";

            Task providerWrites = Task.Run(() => Parallel.For(0, 64, i =>
            {
                byte[] identity = new byte[16];
                identity[14] = (byte)(i >> 8);
                identity[15] = (byte)(i + 1);
                provider.AppendLocalChange(new ProviderMutation
                {
                    RecordId = RecordId.From(identity),
                    FieldId = 0x0830,
                    AuthorNodeId = local,
                    HlcWall = checked((ulong)(10_000 + i)),
                    Value = new[] { checked((byte)i) }
                });
            }));

            Task metadataWrites = Task.Run(() => Parallel.For(0, 64, i =>
            {
                byte[] identity = new byte[16];
                identity[14] = 0x80;
                identity[15] = (byte)(i + 1);
                metadata.FreezeOutbound(scope, local, 1, local, new[]
                {
                    new MetadataFrozenOperation
                    {
                        ChangeId = BatchId.From(identity),
                        Fingerprint = identity.ToArray(),
                        Payload = new[] { checked((byte)i) }
                    }
                });
            }));

            await Task.WhenAll(providerWrites, metadataWrites);
            Assert.Equal(64UL, provider.LocalChangeHighWatermark);
            Assert.Equal(64UL, metadata.GetOutboundHighWatermark(scope, local, 1, local));
            Assert.Equal(128UL, storage.LastCommitSeq);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Storage_database_rejects_a_second_writer_for_the_same_partition()
    {
        string directory = Path.Combine(_root, "single-writer-fence");
        Directory.CreateDirectory(directory);
        try
        {
            using (var first = StorageDatabase.Open(directory, 86))
            {
                Assert.Throws<IOException>(() => StorageDatabase.Open(directory, 86));

                using var batch = first.BeginWriteBatch();
                batch.Put(new byte[] { 1 }, new byte[] { 2 });
                batch.Commit();
                Assert.True(first.TryGet(new byte[] { 1 }, out var value));
                Assert.Equal(new byte[] { 2 }, value);
            }

            using var reopened = StorageDatabase.Open(directory, 86);
            Assert.True(reopened.TryGet(new byte[] { 1 }, out var reopenedValue));
            Assert.Equal(new byte[] { 2 }, reopenedValue);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Wal_writer_fence_recovers_an_abandoned_mutex_before_wal_recovery()
    {
        string directory = Path.Combine(_root, "abandoned-wal-writer-fence");
        Directory.CreateDirectory(directory);
        const ulong partitionId = 87;
        string fullPath = Path.GetFullPath(directory);
        if (OperatingSystem.IsWindows() || OperatingSystem.IsMacOS())
            fullPath = fullPath.ToUpperInvariant();
        string identity = fullPath + "|" + partitionId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        string digest = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(identity)));
        string mutexName = OperatingSystem.IsWindows()
            ? "Global\\Fld1Storage_" + digest
            : "Fld1Storage_" + digest;

        try
        {
            using var abandoned = new Mutex(initiallyOwned: false, mutexName);
            using var acquired = new ManualResetEventSlim(false);
            var owner = new Thread(() =>
            {
                abandoned.WaitOne();
                acquired.Set();
                // Deliberately exit without ReleaseMutex: the OS must mark the
                // named mutex abandoned and transfer ownership to the next opener.
            });
            owner.Start();
            Assert.True(acquired.Wait(TimeSpan.FromSeconds(5)));
            owner.Join();

            using var storage = StorageDatabase.Open(directory, partitionId);
            using var batch = storage.BeginWriteBatch();
            batch.Put(new byte[] { 7 }, new byte[] { 8 });
            batch.Commit();
            Assert.True(storage.TryGet(new byte[] { 7 }, out var value));
            Assert.Equal(new byte[] { 8 }, value);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Wal_writer_fence_rejects_second_process_and_recovers_after_owner_termination()
    {
        string directory = Path.Combine(_root, "cross-process-wal-writer-fence");
        Directory.CreateDirectory(directory);
        string readyPath = Path.Combine(directory, "child-ready");
        string? projectDirectory = AppContext.BaseDirectory;
        while (projectDirectory is not null &&
               !File.Exists(Path.Combine(projectDirectory, "BinSync.Storage.Integration.csproj")))
            projectDirectory = Directory.GetParent(projectDirectory)?.FullName;
        Assert.NotNull(projectDirectory);

        string configuration = Directory.GetParent(AppContext.BaseDirectory.TrimEnd(
            Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))?.Name ?? "Release";
        var start = new ProcessStartInfo
        {
            FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet",
            WorkingDirectory = projectDirectory!,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        start.ArgumentList.Add("test");
        start.ArgumentList.Add(Path.Combine(projectDirectory!, "BinSync.Storage.Integration.csproj"));
        start.ArgumentList.Add("--no-build");
        start.ArgumentList.Add("--no-restore");
        start.ArgumentList.Add("--configuration");
        start.ArgumentList.Add(configuration);
        start.ArgumentList.Add("--filter");
        start.ArgumentList.Add("FullyQualifiedName~Wal_writer_fence_child_process_probe");
        start.Environment["BIN_SYNC_WAL_FENCE_PROBE_DIR"] = directory;
        start.Environment["BIN_SYNC_WAL_FENCE_PROBE_READY"] = readyPath;

        try
        {
            using var child = Process.Start(start);
            Assert.NotNull(child);
            Task<string> stdout = child!.StandardOutput.ReadToEndAsync();
            Task<string> stderr = child.StandardError.ReadToEndAsync();

            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!File.Exists(readyPath) && !child.HasExited && DateTime.UtcNow < deadline)
                await Task.Delay(25);
            if (!File.Exists(readyPath))
            {
                if (!child.HasExited) child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                Assert.True(File.Exists(readyPath), $"Child process did not acquire WAL fence. stdout={await stdout} stderr={await stderr}");
            }

            Assert.Throws<IOException>(() => StorageDatabase.Open(directory, 88));
            try
            {
                await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (TimeoutException)
            {
                child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }

            using var recovered = StorageDatabase.Open(directory, 88);
            Assert.True(recovered.TryGet(new byte[] { 0x11 }, out var value));
            Assert.Equal(new byte[] { 0x22 }, value);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Wal_writer_fence_child_process_probe()
    {
        string? directory = Environment.GetEnvironmentVariable("BIN_SYNC_WAL_FENCE_PROBE_DIR");
        string? readyPath = Environment.GetEnvironmentVariable("BIN_SYNC_WAL_FENCE_PROBE_READY");
        if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(readyPath))
            return;

        using var storage = StorageDatabase.Open(directory, 88);
        using (var batch = storage.BeginWriteBatch())
        {
            batch.Put(new byte[] { 0x11 }, new byte[] { 0x22 });
            batch.Commit();
        }
        File.WriteAllText(readyPath, "ready");
        Environment.Exit(0);
    }

    [Fact]
    public void Wal_writer_fence_rejects_symlink_alias_of_same_partition()
    {
        if (OperatingSystem.IsWindows())
            return; // Windows symlink creation may require Developer Mode or elevated privileges.

        string physical = Path.Combine(_root, "physical-wal-directory");
        string alias = Path.Combine(_root, "wal-directory-alias");
        Directory.CreateDirectory(physical);
        Directory.CreateSymbolicLink(alias, physical);
        Assert.NotNull(new DirectoryInfo(alias).LinkTarget);
        Assert.NotNull(new DirectoryInfo(alias).ResolveLinkTarget(returnFinalTarget: true));

        using var first = StorageDatabase.Open(physical, 89);
        Assert.Throws<IOException>(() => StorageDatabase.Open(alias, 89));
    }

    [Fact]
    public void Wal_writer_fence_rejects_case_alias_on_case_insensitive_filesystems()
    {
        string directory = Path.Combine(_root, "case-alias-wal-directory");
        Directory.CreateDirectory(directory);
        string alias = directory.ToUpperInvariant();
        if (!Directory.Exists(alias) || string.Equals(directory, alias, StringComparison.Ordinal))
            return;

        using var first = StorageDatabase.Open(directory, 90);
        Assert.Throws<IOException>(() => StorageDatabase.Open(alias, 90));
    }

}