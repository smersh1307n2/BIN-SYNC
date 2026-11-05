// SNAPSHOT_CHUNK round-trip + AutoReconcile over TCP.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class SnapshotTests
{
    [Fact]
    public void Snapshot_encode_decode_roundtrip()
    {
        var a = Fix.MakeNode(51);
        var rid = Fix.Rid(70);
        a.WriteField(1, rid, 0x100, new byte[] { 3, 4 });
        var snap = Reconcile.BuildSnapshot(a, 1);
        Assert.NotEmpty(snap.Records);
        var back = Reconcile.DecodeSnapshotChunk(Reconcile.EncodeSnapshotChunk(snap));
        Assert.Equal(snap.NamespaceId, back.NamespaceId);
        Assert.Equal(snap.Records.Count, back.Records.Count);
        Assert.True(Fix.Eq(rid, back.Records[0].RecordId));
        Assert.Equal(new byte[] { 3, 4 }, back.Records[0].Fields[0].Value);
        Assert.Equal(snap.Records[0].HasLifecycle, back.Records[0].HasLifecycle);
    }

    [Fact]
    public void ApplySnapshot_merges_into_empty_node()
    {
        var a = Fix.MakeNode(52);
        var b = Fix.MakeNode(53);
        var rid = Fix.Rid(71);
        a.WriteField(1, rid, 0x100, new byte[] { 9 });
        var snap = Reconcile.BuildSnapshot(a, 1);
        int n = Reconcile.ApplySnapshot(b, snap);
        Assert.True(n >= 1);
        Assert.Equal(new byte[] { 9 }, b.Storage[1][rid].State[0x100].Value);
    }

    [Fact]
    public void Snapshot_roundtrip_and_apply_preserve_field_and_record_tombstones()
    {
        var source = Fix.MakeNode(58);
        var target = Fix.MakeNode(59);
        var rid = Fix.Rid(74);
        source.WriteField(1, rid, 0x100, new byte[] { 1 });
        source.TombField(1, rid, 0x100);
        source.DeleteRecord(1, rid);

        var encoded = Reconcile.EncodeSnapshotChunk(Reconcile.BuildSnapshot(source, 1));
        var decoded = Reconcile.DecodeSnapshotChunk(encoded);
        Assert.Contains(decoded.Records[0].Fields, f => f.IsTombstone && f.FieldId == 0x100);
        Assert.True(decoded.Records[0].HasLifecycle);
        Reconcile.ApplySnapshot(target, decoded);

        Assert.True(target.Storage[1][rid].HasFieldTombs);
        Assert.Equal(Model.Dead, target.Storage[1][rid].LifecycleValue);
    }

    [Fact]
    public void Snapshot_chunks_cover_the_complete_record_set()
    {
        var source = Fix.MakeNode(60);
        for (int i = 0; i < 5; i++)
            source.WriteField(1, Fix.Rid(100 + i), 0x100, new byte[] { (byte)i });

        var chunks = Reconcile.BuildSnapshotChunks(source, 1, maxRecords: 2);
        Assert.Equal(3, chunks.Count);
        Assert.Equal(new uint[] { 0, 1, 2 }, chunks.Select(c => c.ChunkIndex));
        Assert.All(chunks, c => Assert.Equal(3u, c.ChunkCount));
        Assert.Equal(5, chunks.Sum(c => c.Records.Count));
        var target = Fix.MakeNode(61);
        Assert.Throws<InvalidOperationException>(() => Reconcile.ApplySnapshot(target, chunks[0]));
        Assert.Empty(target.Storage);
        Assert.True(Reconcile.ApplySnapshotChunks(target, chunks) > 0);
        Assert.Equal(5, target.GetRecordCount(1));
    }

    [Fact]
    public async Task Tcp_Snapshot_transfer_fills_missing_record()
    {
        var a = Fix.MakeNode(54);
        var b = Fix.MakeNode(55);
        var rid = Fix.Rid(72);
        b.WriteField(1, rid, 0x100, new byte[] { 7 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var send = server.SendSnapshotAsync(b, 1);
        var applied = await client.ReceiveSnapshotAsync(a);
        await send;
        Assert.True(applied >= 1);
        Assert.Equal(new byte[] { 7 }, a.Storage[1][rid].State[0x100].Value);
    }

    [Fact]
    public async Task Concurrent_snapshot_transfers_are_serialized_as_complete_sequences()
    {
        var source = Fix.MakeNode(80);
        var target = Fix.MakeNode(81);
        var firstId = Fix.Rid(800);
        var secondId = Fix.Rid(801);
        source.WriteField(1, firstId, 0x100, new byte[9 * 1024 * 1024]);
        source.WriteField(1, secondId, 0x100, new byte[9 * 1024 * 1024]);

        var (senderPipe, receiverPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(target, receiverPipe);
        await using var sender = await SyncSession.ConnectAsync(source, senderPipe);
        await using var receiver = await acceptTask;

        var receiveOne = receiver.ReceiveSnapshotAsync(target);
        var receiveTwo = receiver.ReceiveSnapshotAsync(target);
        var sendOne = sender.SendSnapshotAsync(source, 1);
        var sendTwo = sender.SendSnapshotAsync(source, 1);

        var applied = await Task.WhenAll(receiveOne, receiveTwo).WaitAsync(TimeSpan.FromSeconds(15));
        await Task.WhenAll(sendOne, sendTwo).WaitAsync(TimeSpan.FromSeconds(15));

        Assert.All(applied, count => Assert.Equal(2, count));
        Assert.Equal(2, target.GetRecordCount(1));
        Assert.Equal(9 * 1024 * 1024, target.Storage[1][firstId].State[0x100].Value.Length);
        Assert.Equal(9 * 1024 * 1024, target.Storage[1][secondId].State[0x100].Value.Length);
    }

    [Fact]
    public async Task Tcp_AutoReconcile_converges_divergent_field()
    {
        var a = Fix.MakeNode(56);
        var b = Fix.MakeNode(57);
        var rid = Fix.Rid(73);
        a.WriteField(1, rid, 0x100, new byte[] { 1 });
        b.WriteField(1, rid, 0x100, new byte[] { 2 });
        var onlyA = Fix.Rid(75);
        var onlyB = Fix.Rid(76);
        a.WriteField(1, onlyA, 0x100, new byte[] { 0xA1 });
        b.WriteField(1, onlyB, 0x100, new byte[] { 0xB1 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.AutoReconcileAsync(b, 1, initiate: false);
        var (prefixes, fields) = await client.AutoReconcileAsync(a, 1, initiate: true);
        await serverTask;

        Assert.True(prefixes >= 1);
        Assert.True(fields > 0);
        Assert.True(a.Storage[1].ContainsKey(onlyB));
        Assert.True(b.Storage[1].ContainsKey(onlyA));
        Assert.True(a.PeerStates[(b.NodeId, 1)].InitialSyncComplete);
        Assert.True(b.PeerStates[(a.NodeId, 1)].InitialSyncComplete);
    }

    [Fact]
    public async Task AutoReconcile_does_not_commit_baseline_after_only_one_direction()
    {
        var a = Fix.MakeNode(62);
        var b = Fix.MakeNode(63);
        a.WriteField(1, Fix.Rid(201), 0x100, new byte[] { 0xA1 });
        b.WriteField(1, Fix.Rid(202), 0x100, new byte[] { 0xB2 });

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        // Complete only the first direction: A requests B's divergent snapshots.
        var serverDigestTask = server.ExchangeStateDigestAsync(b, 1, initiate: false);
        var (_, _, diverged) = await client.ExchangeStateDigestAsync(a, 1, initiate: true);
        await serverDigestTask;

        foreach (var prefix in diverged)
        {
            var needTask = server.ReceiveNeedRangeAsync();
            await client.SendNeedRangeAsync(1, prefix);
            var need = await needTask;
            var sendTask = server.FulfillNeedRangeWithSnapshotAsync(b, need);
            await client.ReceiveSnapshotAsync(a);
            await sendTask;
        }

        await client.SendNeedRangeAsync(1, Array.Empty<byte>());
        var terminator = await server.ReceiveNeedRangeAsync();
        Assert.Empty(terminator.Prefix);

        Assert.False(a.PeerStates.TryGetValue((b.NodeId, 1), out var aState) && aState.InitialSyncComplete);
        Assert.False(b.PeerStates.TryGetValue((a.NodeId, 1), out var bState) && bState.InitialSyncComplete);

        // Only the full two-direction operation may commit the baseline.
        var serverReconcile = server.AutoReconcileAsync(b, 1, initiate: false);
        await client.AutoReconcileAsync(a, 1, initiate: true);
        await serverReconcile;

        Assert.True(a.PeerStates[(b.NodeId, 1)].InitialSyncComplete);
        Assert.True(b.PeerStates[(a.NodeId, 1)].InitialSyncComplete);
    }

    [Fact]
    public void Snapshot_decoder_rejects_unbounded_chunk_and_record_counts()
    {
        var oversizedChunks = new Writer()
            .U16(1).U32(0).U32(uint.MaxValue).Varint(0).Value();
        Assert.Throws<FormatException>(() => Reconcile.DecodeSnapshotChunk(oversizedChunks));

        var oversizedRecords = new Writer()
            .U16(1).U32(0).U32(1)
            .Varint((ulong)Reconcile.MaxSnapshotRecordsPerChunk + 1).Value();
        Assert.Throws<FormatException>(() => Reconcile.DecodeSnapshotChunk(oversizedRecords));
    }

    [Fact]
    public void Snapshot_decoder_rejects_excessive_field_count_before_reading_fields()
    {
        var oversizedFields = new Writer()
            .U16(1).U32(0).U32(1)
            .Varint(1).B128(Fix.Rid(222))
            .Varint((ulong)Reconcile.MaxSnapshotFieldsPerChunk + 1).Value();
        Assert.Throws<FormatException>(() => Reconcile.DecodeSnapshotChunk(oversizedFields));
    }

    [Fact]
    public void Snapshot_builder_splits_chunks_by_wire_payload_size()
    {
        var source = Fix.MakeNode(64);
        source.WriteField(1, Fix.Rid(230), 0x100, new byte[9 * 1024 * 1024]);
        source.WriteField(1, Fix.Rid(231), 0x100, new byte[9 * 1024 * 1024]);

        var chunks = Reconcile.BuildSnapshotChunks(source, 1);
        Assert.Equal(2, chunks.Count);
        Assert.All(chunks, chunk =>
            Assert.True(Reconcile.EncodeSnapshotChunk(chunk).Length <= Frames.MAX_PAYLOAD_SIZE));
    }

    [Fact]
    public void Snapshot_builder_rejects_record_limit_above_parser_bound()
    {
        var source = Fix.MakeNode(65);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Reconcile.BuildSnapshotChunks(source, 1, maxRecords: Reconcile.MaxSnapshotRecordsPerChunk + 1));
    }

    [Fact]
    public void ApplySnapshotChunks_is_atomic_when_a_late_record_is_invalid()
    {
        var target = Fix.MakeNode(66);
        var existingId = Fix.Rid(240);
        target.WriteField(1, existingId, 0x100, new byte[] { 0x11 });
        var original = target.Storage[1][existingId].State[0x100].Value.ToArray();
        var newId = Fix.Rid(241);
        var author = Fix.MakeNode(67).NodeId.ToArray();

        var chunks = new[]
        {
            new SnapshotChunkMessage
            {
                NamespaceId = 1, ChunkIndex = 0, ChunkCount = 2,
                Records = new List<SnapshotRecord>
                {
                    new()
                    {
                        RecordId = existingId.ToArray(),
                        Fields = new List<SnapshotField>
                        {
                            new() { FieldId = 0x100, Wall = 10, AuthorNodeId = author, Value = new byte[] { 0x22 } }
                        }
                    }
                }
            },
            new SnapshotChunkMessage
            {
                NamespaceId = 1, ChunkIndex = 1, ChunkCount = 2,
                Records = new List<SnapshotRecord>
                {
                    new()
                    {
                        RecordId = newId.ToArray(),
                        Fields = new List<SnapshotField>
                        {
                            new() { FieldId = 0x100, Wall = 11, AuthorNodeId = new byte[15], Value = new byte[] { 0x33 } }
                        }
                    }
                }
            }
        };

        Assert.Throws<FormatException>(() => Reconcile.ApplySnapshotChunks(target, chunks));
        Assert.Equal(original, target.Storage[1][existingId].State[0x100].Value);
        Assert.False(target.Storage[1].ContainsKey(newId));
    }

    [Fact]
    public void ApplySnapshotChunks_rejects_incomplete_or_reordered_transfer_without_mutation()
    {
        var target = Fix.MakeNode(68);
        var rid = Fix.Rid(242);
        var chunk0 = new SnapshotChunkMessage
        {
            NamespaceId = 1, ChunkIndex = 0, ChunkCount = 2,
            Records = new List<SnapshotRecord>
            {
                new()
                {
                    RecordId = rid.ToArray(),
                    Fields = new List<SnapshotField>
                    {
                        new() { FieldId = 0x100, Wall = 1, AuthorNodeId = Fix.MakeNode(69).NodeId.ToArray(), Value = new byte[] { 1 } }
                    }
                }
            }
        };

        Assert.Throws<FormatException>(() => Reconcile.ApplySnapshotChunks(target, new[] { chunk0 }));
        Assert.Empty(target.Storage);
        Assert.Throws<FormatException>(() => Reconcile.ApplySnapshotChunks(target, new[]
        {
            chunk0,
            new SnapshotChunkMessage { NamespaceId = 1, ChunkIndex = 0, ChunkCount = 2 }
        }));
        Assert.Empty(target.Storage);
    }



    [Fact]
    public async Task Snapshot_transfer_disconnect_mid_stream_is_atomic_and_full_retry_succeeds()
    {
        var source = Fix.MakeNode(70);
        var target = Fix.MakeNode(71);
        var existingId = Fix.Rid(250);
        var firstId = Fix.Rid(251);
        var secondId = Fix.Rid(252);
        target.WriteField(1, existingId, 0x100, new byte[] { 0x5A });

        // Snapshot transfer must not advance or rewrite peer-relative delta cursors.
        var peerState = new PeerState(source.NodeId, 1)
        {
            InitialSyncComplete = false,
        };
        peerState.StreamOffsets[source.NodeId] = 4;
        peerState.StreamAcked[source.NodeId] = 7;
        peerState.StreamPushed[source.NodeId] = 9;
        target.PeerStates[(source.NodeId, 1)] = peerState;

        void AssertPeerCursorsUnchanged()
        {
            Assert.Same(peerState, target.PeerStates[(source.NodeId, 1)]);
            Assert.False(peerState.InitialSyncComplete);
            Assert.Equal(4, peerState.StreamOffsets[source.NodeId]);
            Assert.Equal(7, peerState.StreamAcked[source.NodeId]);
            Assert.Equal(9, peerState.StreamPushed[source.NodeId]);
        }
        source.WriteField(1, firstId, 0x100, new byte[9 * 1024 * 1024]);
        source.WriteField(1, secondId, 0x100, new byte[9 * 1024 * 1024]);

        // First connection: deliver chunk zero, then close the sender before chunk one.
        var (brokenSenderPipe, brokenReceiverPipe) = PipeTransport.CreatePair();
        var acceptTask = SyncSession.AcceptAsync(target, brokenReceiverPipe);
        await using (var sender = await SyncSession.ConnectAsync(
            source, new FailAfterFirstSnapshotChunkTransport(brokenSenderPipe)))
        await using (var receiver = await acceptTask)
        {
            var receiveTask = receiver.ReceiveSnapshotAsync(target);
            await Assert.ThrowsAsync<IOException>(() => sender.SendSnapshotAsync(source, 1));
            await Assert.ThrowsAsync<IOException>(() => receiveTask);
        }

        Assert.Single(target.Storage[1]);
        Assert.Equal(new byte[] { 0x5A }, target.Storage[1][existingId].State[0x100].Value);
        Assert.False(target.Storage[1].ContainsKey(firstId));
        Assert.False(target.Storage[1].ContainsKey(secondId));
        AssertPeerCursorsUnchanged();

        // Reconnect with fresh transports and retry the complete snapshot from chunk zero.
        var (retrySenderPipe, retryReceiverPipe) = PipeTransport.CreatePair();
        var retryAcceptTask = SyncSession.AcceptAsync(target, retryReceiverPipe);
        await using var retrySender = await SyncSession.ConnectAsync(source, retrySenderPipe);
        await using var retryReceiver = await retryAcceptTask;
        var retrySendTask = retrySender.SendSnapshotAsync(source, 1);
        var applied = await retryReceiver.ReceiveSnapshotAsync(target);
        await retrySendTask;

        Assert.True(applied >= 2);
        Assert.Equal(3, target.Storage[1].Count);
        Assert.Equal(9 * 1024 * 1024, target.Storage[1][firstId].State[0x100].Value.Length);
        Assert.Equal(9 * 1024 * 1024, target.Storage[1][secondId].State[0x100].Value.Length);
        Assert.Equal(new byte[] { 0x5A }, target.Storage[1][existingId].State[0x100].Value);
        AssertPeerCursorsUnchanged();
    }

    private sealed class FailAfterFirstSnapshotChunkTransport : ISyncTransport
    {
        private readonly PipeTransport _inner;
        private int _snapshotChunks;

        public FailAfterFirstSnapshotChunkTransport(PipeTransport inner) => _inner = inner;

        public NodeId PeerNodeId => _inner.PeerNodeId;
        public SyncTransportCapabilities Capabilities => _inner.Capabilities;
        public int? MaxPacketSize => null;

        public async ValueTask SendAsync(ReadOnlyMemory<byte> packet, CancellationToken ct = default)
        {
            var frame = FrameCodec.ParseFrame(packet.ToArray()).Frame;
            if (frame.FrameType == ReconcileFrames.SnapshotChunk &&
                Interlocked.Increment(ref _snapshotChunks) == 2)
            {
                await _inner.DisposeAsync();
                throw new IOException("Injected disconnect after first snapshot chunk");
            }

            await _inner.SendAsync(packet, ct);
        }

        public ValueTask<byte[]?> ReceiveAsync(CancellationToken ct = default) =>
            _inner.ReceiveAsync(ct);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

}
