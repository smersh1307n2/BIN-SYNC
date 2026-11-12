using System.Text;
using BinSync;
using BinSync.Storage;
using Fld1.Storage.Engine;
using Xunit;

namespace BinSync.Storage.Integration;

/// <summary>
/// Real process-death coverage for the async coordinator's durable receive path.
/// Each test kills the child at a distinct durability boundary, then reopens the
/// same Storage partition and proves recovery or duplicate replay before ACK.
/// </summary>
public sealed class AsyncCoordinatorProcessCrashTests
{
    private const ulong PartitionId = 481;
    private const int NamespaceId = 1;
    private const byte SenderSeed = 131;
    private const byte ReceiverSeed = 132;
    private static readonly RecordId Record =
        RecordId.From(Guid.Parse("8b000000-0000-0000-0000-00000000008b").ToByteArray());

    private sealed class BlockBeforeTransportAck : IPeerAwareSyncTransport
    {
        private readonly ISyncTransport _inner;
        private readonly Action _onAck;
        private NodeId _peerNodeId;

        public BlockBeforeTransportAck(ISyncTransport inner, Action onAck)
        {
            _inner = inner;
            _onAck = onAck;
        }

        public NodeId PeerNodeId
        {
            get => _peerNodeId;
            set
            {
                _peerNodeId = value;
                if (_inner is IPeerAwareSyncTransport aware)
                    aware.PeerNodeId = value;
            }
        }

        public SyncTransportCapabilities Capabilities => _inner.Capabilities;
        public int? MaxPacketSize => _inner.MaxPacketSize;

        public ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
        {
            var (frame, _) = FrameCodec.ParseFrame(packet.ToArray());
            if (frame.FrameType == 0x0B)
            {
                _onAck();
                throw new IOException("Process terminated at the durable Metadata-finalized / transport-ACK boundary.");
            }
            return _inner.SendAsync(packet, ct);
        }

        public ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default) =>
            _inner.ReceiveAsync(ct);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private sealed class StorageAsyncRemoteBatchStore : IAsyncProviderRemoteBatchCommitStore
    {
        private readonly IDataProvider _provider;
        private readonly Action? _beforeCommit;
        private readonly Action? _afterCommit;

        public StorageAsyncRemoteBatchStore(
            IDataProvider provider, Action? afterCommit = null, Action? beforeCommit = null)
        {
            _provider = provider;
            _afterCommit = afterCommit;
            _beforeCommit = beforeCommit;
        }

        public ValueTask<ProviderApplyReceipt> ApplyRemoteBatchAsync(
            ProviderRemoteBatchCommit batch, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _beforeCommit?.Invoke();
            ProviderApplyReceipt applied = _provider.ApplyRemoteBatch(batch.ApplyToken, batch.Mutations);
            _afterCommit?.Invoke();
            return ValueTask.FromResult(new ProviderApplyReceipt(
                batch.ApplyToken, batch.BatchDigest.ToArray(), applied.IsDuplicate));
        }
    }

    [Theory]
    [InlineData("after-pending-apply")]
    [InlineData("after-node-checkpoint")]
    [InlineData("after-provider-commit")]
    [InlineData("after-metadata-finalized-before-ack")]
    public async Task Async_coordinator_process_death_at_receive_cut_preserves_durable_state(string crashCut)
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "binsync-async-coordinator-crash-" + Guid.NewGuid().ToString("N"));
        string readyPath = Path.Combine(directory, "ready");
        string pidPath = Path.Combine(directory, "owner-pid");
        Directory.CreateDirectory(directory);

        string? projectDirectory = AppContext.BaseDirectory;
        while (projectDirectory is not null &&
               !File.Exists(Path.Combine(projectDirectory, "BinSync.Storage.Integration.csproj")))
            projectDirectory = Directory.GetParent(projectDirectory)?.FullName;
        Assert.NotNull(projectDirectory);
        string configuration = Directory.GetParent(AppContext.BaseDirectory.TrimEnd(
            Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))?.Name ?? "Release";

        var start = new System.Diagnostics.ProcessStartInfo
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
        start.ArgumentList.Add("FullyQualifiedName~Async_coordinator_process_kill_child_probe");
        start.ArgumentList.Add("--results-directory");
        start.ArgumentList.Add(Path.Combine(directory, "child-results"));
        start.Environment["BIN_SYNC_ASYNC_CRASH_DIR"] = directory;
        start.Environment["BIN_SYNC_ASYNC_CRASH_READY"] = readyPath;
        start.Environment["BIN_SYNC_ASYNC_CRASH_PID"] = pidPath;
        start.Environment["BIN_SYNC_ASYNC_CRASH_CUT"] = crashCut;

        System.Diagnostics.Process? child = null;
        try
        {
            child = System.Diagnostics.Process.Start(start);
            Assert.NotNull(child);
            Task<string> stdout = child!.StandardOutput.ReadToEndAsync();
            Task<string> stderr = child.StandardError.ReadToEndAsync();
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(45);
            while (!File.Exists(readyPath) && !child.HasExited && DateTime.UtcNow < deadline)
                await Task.Delay(25);
            if (!File.Exists(readyPath))
            {
                if (!child.HasExited) child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                Assert.True(File.Exists(readyPath),
                    $"Async coordinator child did not reach durable provider commit. stdout={await stdout} stderr={await stderr}");
            }

            int ownerPid = int.Parse(File.ReadAllText(pidPath), System.Globalization.CultureInfo.InvariantCulture);
            using (var owner = System.Diagnostics.Process.GetProcessById(ownerPid))
            {
                owner.Kill(entireProcessTree: true);
                await owner.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }
            if (!child.HasExited) child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));

            StorageDatabase? reopened = null;
            Exception? reopenError = null;
            DateTime reopenDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (reopened is null && DateTime.UtcNow < reopenDeadline)
            {
                try { reopened = StorageDatabase.Open(directory, PartitionId); }
                catch (IOException ex) { reopenError = ex; await Task.Delay(50); }
            }
            Assert.NotNull(reopened);
            using var storage = reopened!;
            using var provider = new StorageDataProvider(storage);
            using var metadata = new StorageMetadataStore(storage);
            using var receiver = StorageNodePersistence.Load(storage, NewNode(ReceiverSeed).NodeId);
            using var senderIdentity = NewNode(SenderSeed);

            bool afterMetadataFinalize = crashCut == "after-metadata-finalized-before-ack";
            IReadOnlyList<MetadataPendingApplyIntent> pendingBeforeRecovery =
                metadata.ReadPendingApplyIntents("async-process-crash", receiver.NodeId);
            if (afterMetadataFinalize) Assert.Empty(pendingBeforeRecovery);
            else Assert.Single(pendingBeforeRecovery);
            bool nodeCheckpointed = receiver.TryGetField(NamespaceId, Record, 0x088B, out var checkpointed);
            Assert.Equal(crashCut != "after-pending-apply", nodeCheckpointed);
            if (nodeCheckpointed)
                Assert.Equal("async-provider-commit", Encoding.UTF8.GetString(checkpointed!.Value));
            bool providerCommitted = provider.TryReadCurrent(Record, 0x088B, out var providerValue);
            Assert.Equal(crashCut is "after-provider-commit" or "after-metadata-finalized-before-ack", providerCommitted);
            if (providerCommitted)
                Assert.Equal("async-provider-commit", Encoding.UTF8.GetString(providerValue!.Value));
            Assert.Equal(afterMetadataFinalize ? 1UL : 0UL, metadata.GetReceiveCursor(
                "async-process-crash", receiver.NodeId, senderIdentity.NodeId, 1, NamespaceId));

            var coordinator = new AsyncStorageSyncCoordinator(
                receiver, metadata, "async-process-crash", NamespaceId);
            var remote = new StorageAsyncRemoteBatchStore(provider);
            if (afterMetadataFinalize)
            {
                using var sender = StorageNodePersistence.Load(storage, senderIdentity.NodeId);
                // This cut is deliberately before the receiver emits transport ACK.
                // The sender must therefore retain an unadvanced ACK cursor. Exact
                // immutable-batch replay is covered by the lost-ACK integration tests;
                // this process-kill test isolates the finalized-but-unacknowledged cut.
                Assert.True(!sender.PeerStates.TryGetValue((receiver.NodeId, NamespaceId), out var senderPeerState) ||
                    senderPeerState.AckedFor(sender.NodeId) == 0L);
            }
            else
            {
                Assert.Equal(1, await coordinator.RecoverPendingAppliesAsync(
                    remote, node => StorageNodePersistence.Save(storage, node)));
            }
            Assert.Empty(metadata.ReadPendingApplyIntents("async-process-crash", receiver.NodeId));
            Assert.Equal(1UL, metadata.GetReceiveCursor(
                "async-process-crash", receiver.NodeId, senderIdentity.NodeId, 1, NamespaceId));
            Assert.True(provider.TryReadCurrent(Record, 0x088B, out var recoveredProvider));
            Assert.Equal("async-provider-commit", Encoding.UTF8.GetString(recoveredProvider!.Value));
            Assert.True(receiver.TryGetField(NamespaceId, Record, 0x088B, out var recoveredNode));
            Assert.Equal("async-provider-commit", Encoding.UTF8.GetString(recoveredNode!.Value));
        }
        finally
        {
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
    public async Task Async_coordinator_process_kill_child_probe()
    {
        string? directory = Environment.GetEnvironmentVariable("BIN_SYNC_ASYNC_CRASH_DIR");
        string? readyPath = Environment.GetEnvironmentVariable("BIN_SYNC_ASYNC_CRASH_READY");
        string? pidPath = Environment.GetEnvironmentVariable("BIN_SYNC_ASYNC_CRASH_PID");
        string? crashCut = Environment.GetEnvironmentVariable("BIN_SYNC_ASYNC_CRASH_CUT");
        if (string.IsNullOrWhiteSpace(directory) ||
            string.IsNullOrWhiteSpace(readyPath) ||
            string.IsNullOrWhiteSpace(pidPath) ||
            string.IsNullOrWhiteSpace(crashCut))
            return;

        using var storage = StorageDatabase.Open(directory, PartitionId);
        using var provider = new StorageDataProvider(storage);
        using var metadata = new StorageMetadataStore(storage);
        using var sender = NewNode(SenderSeed);
        using var receiver = NewNode(ReceiverSeed);
        Action signalAndBlock = () =>
        {
            File.WriteAllText(pidPath, Environment.ProcessId.ToString(
                System.Globalization.CultureInfo.InvariantCulture));
            File.WriteAllText(readyPath, "reached-" + crashCut);
            Thread.Sleep(Timeout.Infinite);
        };
        var (toSender, toReceiver) = PipeTransport.CreatePair();
        ISyncTransport inboundTransport = crashCut == "after-metadata-finalized-before-ack"
            ? new BlockBeforeTransportAck(toReceiver, signalAndBlock)
            : toReceiver;
        Task<SyncSession> acceptTask = SyncSession.AcceptAsync(receiver, inboundTransport);
        Task<SyncSession> connectTask = SyncSession.ConnectAsync(sender, toSender);
        await using var acceptor = await acceptTask;
        await using var initiator = await connectTask;
        StorageNodePersistence.Save(storage, receiver);

        Assert.True(sender.WriteField(NamespaceId, Record, 0x088B,
            Encoding.UTF8.GetBytes("async-provider-commit"), new Hlc(8800, 0)));
        StorageNodePersistence.Save(storage, sender);
        var coordinator = new AsyncStorageSyncCoordinator(
            receiver, metadata, "async-process-crash", NamespaceId);
        Task<BatchResult> receive;
        if (crashCut == "after-pending-apply")
        {
            coordinator.RegisterInterestedPeer(acceptor, NamespaceId);
            receive = acceptor.ReceiveBatchAsync(receiver,
                beforeApply: null,
                beforeApplyWithOperationDecisions: (batch, payload, peer) =>
                {
                    StorageRemoteApplyPlan plan = coordinator.BeginRemoteBatchBeforeNodeApply(
                        batch, payload.ToArray(), peer, checked((uint)acceptor.PeerEpoch));
                    signalAndBlock();
                    return plan.ApplyOperations;
                },
                beforeAck: null,
                receiveStateOwnedByMetadataStore: true);
        }
        else
        {
            var remote = crashCut switch
            {
                "after-node-checkpoint" =>
                    new StorageAsyncRemoteBatchStore(provider, beforeCommit: signalAndBlock),
                "after-provider-commit" =>
                    new StorageAsyncRemoteBatchStore(provider, afterCommit: signalAndBlock),
                // The fourth cut must pass provider commit and Metadata finalization;
                // BlockBeforeTransportAck is the only crash barrier for this case.
                "after-metadata-finalized-before-ack" =>
                    new StorageAsyncRemoteBatchStore(provider),
                _ => throw new InvalidOperationException($"Unknown crash cut: {crashCut}")
            };
            receive = coordinator.ReceiveBatchWithAsyncProviderBeforeAckAsync(
                acceptor, remote, node => StorageNodePersistence.Save(storage, node));
        }
        Task<bool> send = initiator.PushBatchAsync(sender, NamespaceId);
        await Task.WhenAll(receive, send).WaitAsync(TimeSpan.FromSeconds(30));
    }

    private static Node NewNode(byte seed)
    {
        byte[] id = new byte[16];
        id[0] = seed;
        id[15] = (byte)(seed ^ 0xA5);
        return new NodeBuilder().WithNodeId(id).WithEpoch(1).Build();
    }
}
