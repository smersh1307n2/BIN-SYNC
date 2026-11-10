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
    public void Open_snapshot_authorizes_skipping_its_exact_feed_watermark_only_while_held()
    {
        string directory = Path.Combine(_root, "snapshot-feed-barrier");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProvider(directory, partitionId: 91, deleteOnDispose: true);
            provider.AppendLocalChange(Mutation(1, 1, 100, 1));

            var snapshot = provider.OpenSnapshot();
            Assert.Equal(1UL, snapshot.ChangeFeedWatermark);
            Assert.Empty(provider.ReadLocalChanges(snapshot.ChangeFeedWatermark, 10).Changes);
            Assert.Throws<InvalidDataException>(() =>
                provider.ReadLocalChanges(snapshot.ChangeFeedWatermark + 1, 10));

            provider.ReleaseSnapshot(snapshot.Token);
            Assert.Throws<InvalidDataException>(() =>
                provider.ReadLocalChanges(snapshot.ChangeFeedWatermark + 1, 10));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

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
                    OriginAuthorEpoch = 1,
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
    public void Legacy_local_feed_row_without_change_id_fails_closed()
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

            Assert.Throws<InvalidDataException>(() => provider.ReadLocalChanges(0, 10));
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
                OriginAuthorEpoch = 1,
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
                    OriginAuthorEpoch = 1,
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
                    OriginAuthorEpoch = 1,
                HlcWall = 200, Value = "newer"u8.ToArray()
            });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record, FieldId = 0x0101, AuthorNodeId = author,
                    OriginAuthorEpoch = 1,
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
                    OriginAuthorEpoch = 1,
                HlcWall = 1000,
                Value = "before-delete"u8.ToArray()
            });
            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = Model.LifecycleFieldId,
                IsRecordTombstone = true,
                AuthorNodeId = author,
                    OriginAuthorEpoch = 1,
                HlcWall = 2000,
                Value = Array.Empty<byte>()
            });

            Assert.False(provider.TryReadCurrent(record, 0x0101, out _));

            provider.AppendLocalChange(new ProviderMutation
            {
                RecordId = record,
                FieldId = 0x0101,
                AuthorNodeId = author,
                    OriginAuthorEpoch = 1,
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
                    OriginAuthorEpoch = 1,
                    HlcWall = checked((ulong)(10_000 + i)),
                    Value = new[] { checked((byte)i) }
                });
            }));

            object metadataSequenceGate = new();
            ulong nextOriginSequence = 0;
            Task metadataWrites = Task.Run(() => Parallel.For(0, 64, i =>
            {
                byte[] identity = new byte[16];
                identity[14] = 0x80;
                identity[15] = (byte)(i + 1);
                lock (metadataSequenceGate)
                {
                    ulong originSequence = ++nextOriginSequence;
                    metadata.FreezeOutbound(scope, local, 1, local, new[]
                    {
                        new MetadataFrozenOperation
                        {
                            ChangeId = BatchId.From(identity),
                            Fingerprint = identity.ToArray(),
                            Payload = new[] { checked((byte)i) },
                            OriginAuthorEpoch = 1,
                            OriginSequence = originSequence
                        }
                    });
                }
            }));

            await Task.WhenAll(providerWrites, metadataWrites);
            Assert.Equal(64UL, provider.LocalChangeHighWatermark);
            Assert.Equal(64UL, metadata.GetOutboundHighWatermark(scope, local, 1, local));
            // BSP3 marker + active-generation pointer are committed before concurrent writes.\n            Assert.Equal(129UL, storage.LastCommitSeq);
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
    public async Task Wal_writer_fence_can_be_released_from_a_different_thread()
    {
        string directory = Path.Combine(_root, "cross-thread-wal-writer-fence-release");
        Directory.CreateDirectory(directory);
        const ulong partitionId = 87;

        try
        {
            var storage = StorageDatabase.Open(directory, partitionId);
            using (var batch = storage.BeginWriteBatch())
            {
                batch.Put(new byte[] { 7 }, new byte[] { 8 });
                batch.Commit();
            }

            // The named Mutex is owned by the process-wide manager thread. Disposal
            // from this caller thread must enqueue release to that owner and permit the next writer.
            await Task.Run(storage.Dispose);

            using var reopened = StorageDatabase.Open(directory, partitionId);
            Assert.True(reopened.TryGet(new byte[] { 7 }, out var value));
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
        string releasePath = Path.Combine(directory, "child-release");
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
        start.Environment["BIN_SYNC_WAL_FENCE_PROBE_RELEASE"] = releasePath;

        Process? child = null;
        try
        {
            child = Process.Start(start);
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
            File.WriteAllText(releasePath, "release");
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
            try { File.WriteAllText(releasePath, "release"); } catch { }
            if (child is not null)
            {
                try { if (!child.HasExited) child.Kill(entireProcessTree: true); } catch { }
                try { await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
                child.Dispose();
            }
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public async Task Wal_writer_fence_recovers_after_owner_process_is_killed()
    {
        string directory = Path.Combine(_root, "killed-wal-writer-process");
        Directory.CreateDirectory(directory);
        string readyPath = Path.Combine(directory, "child-ready");
        string ownerPidPath = Path.Combine(directory, "child-owner-pid");
        string releasePath = Path.Combine(directory, "never-release");
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
        start.Environment["BIN_SYNC_WAL_FENCE_PROBE_RELEASE"] = releasePath;
        start.Environment["BIN_SYNC_WAL_FENCE_PROBE_PID"] = ownerPidPath;

        Process? child = null;
        try
        {
            child = Process.Start(start);
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
                Assert.True(File.Exists(readyPath),
                    $"Child process did not acquire WAL fence. stdout={await stdout} stderr={await stderr}");
            }

            Assert.Throws<IOException>(() => StorageDatabase.Open(directory, 88));

            // dotnet test launches a separate testhost. Kill the actual process
            // that owns StorageDatabase first, then reap the dotnet test parent.
            int ownerPid = int.Parse(File.ReadAllText(ownerPidPath));
            try
            {
                using var owner = Process.GetProcessById(ownerPid);
                owner.Kill(entireProcessTree: true);
                await owner.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (ArgumentException)
            {
                // The testhost may have exited between reading the PID and opening it.
            }

            if (!child.HasExited)
                child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));

            StorageDatabase? recovered = null;
            Exception? lastError = null;
            DateTime recoveryDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (recovered is null && DateTime.UtcNow < recoveryDeadline)
            {
                try { recovered = StorageDatabase.Open(directory, 88); }
                catch (IOException ex)
                {
                    lastError = ex;
                    await Task.Delay(50);
                }
            }

            Assert.True(recovered is not null, $"WAL fence was not released after owner PID {ownerPid} exited. {lastError}");
            using (recovered!)
            {
                Assert.True(recovered!.TryGet(new byte[] { 0x11 }, out var value));
                Assert.Equal(new byte[] { 0x22 }, value);
            }
        }
        finally
        {
            try { File.WriteAllText(releasePath, "release"); } catch { }
            if (child is not null)
            {
                try { if (!child.HasExited) child.Kill(entireProcessTree: true); } catch { }
                try { await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
                child.Dispose();
            }
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Wal_writer_fence_child_process_probe()
    {
        string? directory = Environment.GetEnvironmentVariable("BIN_SYNC_WAL_FENCE_PROBE_DIR");
        string? readyPath = Environment.GetEnvironmentVariable("BIN_SYNC_WAL_FENCE_PROBE_READY");
        string? releasePath = Environment.GetEnvironmentVariable("BIN_SYNC_WAL_FENCE_PROBE_RELEASE");
        string? ownerPidPath = Environment.GetEnvironmentVariable("BIN_SYNC_WAL_FENCE_PROBE_PID");
        if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(readyPath) ||
            string.IsNullOrWhiteSpace(releasePath))
            return;

        using var storage = StorageDatabase.Open(directory, 88);
        using (var batch = storage.BeginWriteBatch())
        {
            batch.Put(new byte[] { 0x11 }, new byte[] { 0x22 });
            batch.Commit();
        }
        if (!string.IsNullOrWhiteSpace(ownerPidPath))
            File.WriteAllText(ownerPidPath, Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        File.WriteAllText(readyPath, "ready");
        while (!File.Exists(releasePath))
            Thread.Sleep(10);
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


    [Fact]
    public void Provider_v3_marker_is_written_and_legacy_unmarked_rows_fail_closed()
    {
        string directory = Path.Combine(_root, "provider-v3-marker");
        Directory.CreateDirectory(directory);
        try
        {
            using (var provider = new StorageDataProvider(directory, partitionId: 92, deleteOnDispose: false))
            {
                provider.AppendLocalChange(Mutation(1, 1, 100, 1));
            }

            using (var storage = StorageDatabase.Open(directory, 92))
            {
                Assert.True(storage.TryGet(new byte[] { 0x25 }, out var marker));
                Assert.Equal(new byte[] { 0x42, 0x53, 0x50, 0x33 }, marker);
            }

            string legacyDirectory = Path.Combine(_root, "provider-unmarked-legacy");
            Directory.CreateDirectory(legacyDirectory);
            using (var storage = StorageDatabase.Open(legacyDirectory, 93))
            {
                using var batch = storage.BeginWriteBatch();
                batch.Put(new byte[] { 0x21 }, new byte[8]);
                batch.Commit();
            }
            Assert.Throws<InvalidDataException>(() =>
                new StorageDataProvider(legacyDirectory, partitionId: 93, deleteOnDispose: true));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
            try { Directory.Delete(Path.Combine(_root, "provider-unmarked-legacy"), recursive: true); } catch { }
        }
    }

    [Fact]
    public void Provider_v3_feed_rejects_mutation_rows_without_stable_change_id()
    {
        string directory = Path.Combine(_root, "provider-v3-feed-identity");
        Directory.CreateDirectory(directory);
        try
        {
            using (var provider = new StorageDataProvider(directory, partitionId: 94, deleteOnDispose: false))
                provider.AppendLocalChange(Mutation(1, 1, 100, 1));

            using (var storage = StorageDatabase.Open(directory, 94))
            {
                byte[] key = new byte[] { 0x20, 0, 0, 0, 0, 0, 0, 0, 1 };
                Assert.True(storage.TryGet(key, out var value));
                Assert.NotNull(value);
                Assert.True(value!.Length >= 69);
                using var batch = storage.BeginWriteBatch();
                batch.Put(key, value.AsSpan(0, value.Length - 16));
                batch.Commit();
            }

            using var reopened = new StorageDataProvider(directory, partitionId: 94, deleteOnDispose: false);
            Assert.Throws<InvalidDataException>(() => reopened.ReadLocalChanges(0, 10));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Full_replace_stages_inactive_generation_and_switches_atomically()
    {
        string directory = Path.Combine(_root, "provider-full-replace");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProvider(directory, partitionId: 95, deleteOnDispose: false);
            ProviderMutation prior = Mutation(101, 1, 100, 1);
            ProviderMutation replacement = Mutation(102, 2, 200, 2);
            provider.AppendLocalChange(prior);
            ProviderSnapshotHandle heldSnapshot = provider.OpenSnapshot();

            Guid generation = Guid.Parse("95000000-0000-0000-0000-000000000001");
            byte[] digest = StorageDataProvider.ComputeFullReplaceDigest(generation, 17, new[] { replacement });
            provider.BeginFullReplace(generation, 17, 1, digest);
            provider.StageFullReplacePage(generation, 0, new[] { replacement });

            Assert.True(provider.TryReadCurrent(prior.RecordId, prior.FieldId, out _));
            Assert.False(provider.TryReadCurrent(replacement.RecordId, replacement.FieldId, out _));

            provider.CommitFullReplace(generation);
            Assert.False(provider.TryReadCurrent(prior.RecordId, prior.FieldId, out _));
            Assert.True(provider.TryReadCurrent(replacement.RecordId, replacement.FieldId, out var current));
            Assert.Equal(replacement.Value, current!.Value);

            ProviderSnapshotPage heldPage = provider.ReadSnapshot(heldSnapshot.Token, 0, 10);
            Assert.Single(heldPage.Entries);
            Assert.Equal(prior.RecordId, heldPage.Entries[0].RecordId);

            provider.BeginFullReplace(generation, 17, 1, digest);
            provider.CommitFullReplace(generation);
            provider.ReleaseSnapshot(heldSnapshot.Token);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Full_replace_digest_mismatch_never_changes_active_generation()
    {
        string directory = Path.Combine(_root, "provider-full-replace-mismatch");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProvider(directory, partitionId: 96, deleteOnDispose: false);
            ProviderMutation prior = Mutation(103, 1, 100, 1);
            ProviderMutation replacement = Mutation(104, 2, 200, 2);
            provider.AppendLocalChange(prior);

            Guid generation = Guid.Parse("96000000-0000-0000-0000-000000000001");
            byte[] digest = StorageDataProvider.ComputeFullReplaceDigest(generation, 19, new[] { replacement });
            digest[0] ^= 0x80;
            provider.BeginFullReplace(generation, 19, 1, digest);
            provider.StageFullReplacePage(generation, 0, new[] { replacement });

            Assert.Throws<InvalidDataException>(() => provider.CommitFullReplace(generation));
            Assert.True(provider.TryReadCurrent(prior.RecordId, prior.FieldId, out _));
            Assert.False(provider.TryReadCurrent(replacement.RecordId, replacement.FieldId, out _));
            Assert.True(provider.AbortFullReplace(generation, 10));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Full_replace_staging_cannot_overwrite_a_newer_active_generation()
    {
        string directory = Path.Combine(_root, "provider-full-replace-stale-base");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProvider(directory, partitionId: 97, deleteOnDispose: false);
            ProviderMutation first = Mutation(105, 1, 100, 1);
            ProviderMutation second = Mutation(106, 2, 200, 2);
            Guid staleGeneration = Guid.Parse("97000000-0000-0000-0000-000000000001");
            Guid currentGeneration = Guid.Parse("97000000-0000-0000-0000-000000000002");

            byte[] staleDigest = StorageDataProvider.ComputeFullReplaceDigest(staleGeneration, 20, new[] { first });
            provider.BeginFullReplace(staleGeneration, 20, 1, staleDigest);
            provider.StageFullReplacePage(staleGeneration, 0, new[] { first });

            byte[] currentDigest = StorageDataProvider.ComputeFullReplaceDigest(currentGeneration, 21, new[] { second });
            provider.BeginFullReplace(currentGeneration, 21, 1, currentDigest);
            provider.StageFullReplacePage(currentGeneration, 0, new[] { second });
            provider.CommitFullReplace(currentGeneration);

            Assert.Throws<InvalidOperationException>(() => provider.CommitFullReplace(staleGeneration));
            Assert.True(provider.TryReadCurrent(second.RecordId, second.FieldId, out _));
            Assert.False(provider.TryReadCurrent(first.RecordId, first.FieldId, out _));
            Assert.True(provider.AbortFullReplace(staleGeneration, 10));
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Full_replace_retains_old_generations_until_safe_gc_proof()
    {
        string directory = Path.Combine(_root, "provider-generation-reclaim");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProvider(directory, partitionId: 98, deleteOnDispose: false);
            ProviderMutation prior = Mutation(107, 1, 100, 1);
            ProviderMutation replacement = Mutation(108, 2, 200, 2);
            provider.AppendLocalChange(prior);
            ProviderSnapshotHandle heldSnapshot = provider.OpenSnapshot();

            Guid generation = Guid.Parse("98000000-0000-0000-0000-000000000001");
            byte[] digest = StorageDataProvider.ComputeFullReplaceDigest(generation, 22, new[] { replacement });
            provider.BeginFullReplace(generation, 22, 1, digest);
            provider.StageFullReplacePage(generation, 0, new[] { replacement });
            provider.CommitFullReplace(generation);

            ProviderMutation latest = Mutation(109, 3, 230, 3);
            Guid latestGeneration = Guid.Parse("98000000-0000-0000-0000-000000000002");
            byte[] latestDigest = StorageDataProvider.ComputeFullReplaceDigest(latestGeneration, 23, new[] { latest });
            provider.BeginFullReplace(latestGeneration, 23, 1, latestDigest);
            provider.StageFullReplacePage(latestGeneration, 0, new[] { latest });
            provider.CommitFullReplace(latestGeneration);

            ProviderSnapshotPage heldPage = provider.ReadSnapshot(heldSnapshot.Token, 0, 10);
            Assert.Single(heldPage.Entries);
            Assert.Equal(prior.RecordId, heldPage.Entries[0].RecordId);
            Assert.False(provider.TryReadCurrent(prior.RecordId, prior.FieldId, out _));
            Assert.False(provider.TryReadCurrent(replacement.RecordId, replacement.FieldId, out _));
            Assert.True(provider.TryReadCurrent(latest.RecordId, latest.FieldId, out _));

            Assert.Throws<InvalidDataException>(() =>
                provider.BeginFullReplace(generation, 22, 1, digest));
            provider.ReleaseSnapshot(heldSnapshot.Token);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Full_replace_staging_resumes_after_provider_restart()
    {
        string directory = Path.Combine(_root, "provider-full-replace-restart");
        Directory.CreateDirectory(directory);
        try
        {
            ProviderMutation first = Mutation(110, 1, 100, 1);
            ProviderMutation second = Mutation(110, 2, 101, 1);
            Guid generation = Guid.Parse("99000000-0000-0000-0000-000000000001");
            byte[] digest = StorageDataProvider.ComputeFullReplaceDigest(generation, 24, new[] { first, second });

            using (var provider = new StorageDataProvider(directory, partitionId: 99, deleteOnDispose: false))
            {
                provider.BeginFullReplace(generation, 24, 2, digest);
                provider.StageFullReplacePage(generation, 0, new[] { first });
                Assert.False(provider.TryReadCurrent(first.RecordId, first.FieldId, out _));
            }

            using (var reopened = new StorageDataProvider(directory, partitionId: 99, deleteOnDispose: false))
            {
                reopened.BeginFullReplace(generation, 24, 2, digest);
                reopened.StageFullReplacePage(generation, 1, new[] { second });
                reopened.CommitFullReplace(generation);
                Assert.True(reopened.TryReadCurrent(first.RecordId, first.FieldId, out _));
                Assert.True(reopened.TryReadCurrent(second.RecordId, second.FieldId, out _));
            }
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }


    [Fact]
    public void Full_replace_digest_builder_streams_canonical_pages_without_materializing()
    {
        ProviderMutation first = Mutation(120, 1, 100, 1);
        ProviderMutation second = Mutation(120, 2, 101, 1);
        ProviderMutation third = Mutation(121, 1, 102, 1);
        Guid generation = Guid.Parse("9a000000-0000-0000-0000-000000000001");
        const ulong watermark = 25;
        byte[] expected = StorageDataProvider.ComputeFullReplaceDigest(
            generation, watermark, new[] { first, second, third });

        using var builder = new ProviderFullReplaceDigestBuilder(generation, watermark);
        builder.AppendPage(new[] { first, second });
        builder.AppendPage(new[] { third });
        Assert.Equal(3, builder.EntryCount);
        Assert.Equal(expected, builder.Complete());
        Assert.Throws<InvalidOperationException>(() => builder.AppendPage(new[] { third }));

        using var invalid = new ProviderFullReplaceDigestBuilder(
            Guid.Parse("9a000000-0000-0000-0000-000000000002"), watermark);
        Assert.Throws<InvalidDataException>(() => invalid.AppendPage(new[] { second, first }));
        Assert.Throws<InvalidOperationException>(() => invalid.Complete());
    }


    [Fact]
    public void Full_replace_preserves_record_and_field_tombstones()
    {
        string directory = Path.Combine(_root, "provider-full-replace-tombstones");
        Directory.CreateDirectory(directory);
        try
        {
            using var provider = new StorageDataProvider(directory, partitionId: 100, deleteOnDispose: false);
            ProviderMutation hiddenField = Mutation(130, 1, 50, 1);
            ProviderMutation recordTombstone = new()
            {
                RecordId = hiddenField.RecordId,
                FieldId = Model.LifecycleFieldId,
                IsRecordTombstone = true,
                HlcWall = 200,
                HlcLogical = 1,
                AuthorNodeId = Node(1),
                KeyEpoch = 0,
                Value = Array.Empty<byte>()
            };
            ProviderMutation fieldTombstone = new()
            {
                RecordId = Mutation(131, 2, 160, 1).RecordId,
                FieldId = 2,
                IsTombstone = true,
                HlcWall = 250,
                HlcLogical = 1,
                AuthorNodeId = Node(1),
                KeyEpoch = 0,
                Value = Array.Empty<byte>()
            };
            Guid generation = Guid.Parse("9b000000-0000-0000-0000-000000000001");
            ProviderMutation[] image = { recordTombstone, hiddenField, fieldTombstone };
            byte[] digest = StorageDataProvider.ComputeFullReplaceDigest(generation, 26, image);

            provider.BeginFullReplace(generation, 26, image.Length, digest);
            provider.StageFullReplacePage(generation, 0, image);
            provider.CommitFullReplace(generation);

            Assert.False(provider.TryReadCurrent(hiddenField.RecordId, hiddenField.FieldId, out _));
            Assert.True(provider.TryReadCurrent(fieldTombstone.RecordId, fieldTombstone.FieldId, out var current));
            Assert.True(current!.IsTombstone);
            Assert.False(current.IsRecordTombstone);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

}