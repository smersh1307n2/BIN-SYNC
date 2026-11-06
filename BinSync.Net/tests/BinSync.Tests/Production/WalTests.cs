// WAL append / replay / checkpoint tests.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class WalTests
{
    private static OutboxEntry MakeLocalFieldEntry(Node node, RecordId rid, ushort fieldId,
        byte[] value, Hlc hlc, long seq)
    {
        var op = new OpFieldWrite
        {
            FieldId = fieldId, KeyEpoch = 0, Wall = hlc.Wall, Logical = hlc.Logical,
            Origin = 1, AuthorNodeId = node.NodeId, Value = value
        };
        return new OutboxEntry
        {
            NamespaceId = 1, Seq = seq, ChangeId = BatchId.From(Guid.NewGuid().ToByteArray()),
            RecordId = rid, OpKind = OpsConst.TagFieldWrite,
            Payload = Ops.EncodeOp(op, fullAuthor: false), AuthorNodeId = node.NodeId,
            CreatedAt = (long)hlc.Wall
        };
    }

    [Fact]
    public void Node_snapshot_rejects_trailing_bytes()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-state-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "node.state");
            NodePersistence.Save(Fix.MakeNode(131), path);
            var bytes = File.ReadAllBytes(path);
            var corrupted = bytes.Concat(new byte[] { 0xA5 }).ToArray();
            Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(corrupted));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_append_and_replay_restores_field_and_outbox()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var source = Fix.MakeNode(120);
            var rid = Fix.Rid(300);
            using (var wal = Wal.Open(walPath))
            {
                source.AttachWal(wal);
                Assert.True(source.WriteField(1, rid, 0x100, new byte[] { 0x42 }, new Hlc(12345, 7)));
                Assert.Equal(1, wal.Sequence);
            }

            var recovered = Fix.MakeNode(120);
            int applied = Wal.ReplayFile(walPath, recovered);
            Assert.Equal(1, applied);
            Assert.True(recovered.Storage.ContainsKey(1) && recovered.Storage[1].ContainsKey(rid));
            Assert.Equal(new byte[] { 0x42 }, recovered.Storage[1][rid].State[0x100].Value);
            Assert.Equal(12345u, recovered.Storage[1][rid].State[0x100].Timestamp.Wall);
            Assert.Equal(7u, recovered.Storage[1][rid].State[0x100].Timestamp.Logical);
            Assert.Single(recovered.Outbox[1]);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_open_truncates_torn_tail_before_accepting_new_appends()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var first = Fix.MakeNode(125);
            var firstRid = Fix.Rid(304);
            var secondRid = Fix.Rid(305);

            using (var wal = Wal.Open(walPath))
            {
                first.AttachWal(wal);
                first.WriteField(1, firstRid, 0x100, new byte[] { 1 }, new Hlc(100, 0));
            }

            // Simulate a process/power failure after the length prefix and only
            // part of the final record payload reached the file.
            using (var fs = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.None))
            {
                Span<byte> length = stackalloc byte[4];
                System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(length, 64);
                fs.Write(length);
                fs.Write(new byte[] { 0x01, 0x02, 0x03 });
                fs.Flush(flushToDisk: true);
            }

            // Reopen the WAL and append the next sequence using a fresh process view.
            var second = Fix.MakeNode(125);
            using (var wal = Wal.Open(walPath))
            {
                Assert.Equal(1, wal.Sequence);
                second.AttachWal(wal); // Attach performs replay before enabling new writes.
                Assert.True(second.TryGetRecord(1, firstRid, out _));
                Assert.Equal(1, second.GetOutboxCount(1));
                second.WriteField(1, secondRid, 0x100, new byte[] { 2 }, new Hlc(200, 0));
                Assert.Equal(2, wal.Sequence);
            }

            var recovered = Fix.MakeNode(125);
            Assert.Equal(2, Wal.ReplayFile(walPath, recovered));
            Assert.True(recovered.TryGetRecord(1, firstRid, out _));
            Assert.True(recovered.TryGetRecord(1, secondRid, out _));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_local_mutation_replay_restores_field_and_exact_outbox_identity()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-local-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "node.wal");
        try
        {
            var source = Fix.MakeNode(134);
            var rid = Fix.Rid(330);
            var changeId = BatchId.From(Guid.NewGuid().ToByteArray());
            var op = new OpFieldWrite
            {
                FieldId = 0x100, KeyEpoch = 0, Wall = 12345, Logical = 7,
                Origin = 1, AuthorNodeId = source.NodeId, Value = new byte[] { 0x51, 0x52 }
            };
            var entry = new OutboxEntry
            {
                NamespaceId = 1, Seq = 1, ChangeId = changeId, RecordId = rid,
                OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOp(op, fullAuthor: false),
                AuthorNodeId = source.NodeId, CreatedAt = 12345,
                CausalDeps = new List<BatchId> { BatchId.From(Guid.NewGuid().ToByteArray()) }
            };

            using (var wal = Wal.Open(path))
                wal.AppendLocalMutation(entry, applyToStorage: true);

            var recovered = Fix.MakeNode(134);
            using (var wal = Wal.Open(path))
            {
                Assert.Equal(1, wal.Replay(recovered));
                Assert.Equal(0, wal.Replay(recovered));
            }

            Assert.True(recovered.TryGetField(1, rid, 0x100, out var field));
            Assert.Equal(new byte[] { 0x51, 0x52 }, field!.Value);
            var restored = Assert.Single(recovered.Outbox[1]);
            Assert.Equal(entry.Seq, restored.Seq);
            Assert.Equal(entry.ChangeId, restored.ChangeId);
            Assert.Equal(entry.RecordId, restored.RecordId);
            Assert.Equal(entry.OpKind, restored.OpKind);
            Assert.Equal(entry.Payload.ToArray(), restored.Payload.ToArray());
            Assert.Equal(entry.AuthorNodeId, restored.AuthorNodeId);
            Assert.Equal(entry.CreatedAt, restored.CreatedAt);
            Assert.Equal(entry.CausalDeps, restored.CausalDeps);
            Assert.Equal(1, recovered.OutboxSeqRef[(source.NodeId, 1)]);
            Assert.True(recovered.Clock.LastWall >= 12345);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Node_attached_wal_commits_local_write_before_publishing_state()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-attached-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "node.wal");
        try
        {
            var node = Fix.MakeNode(135);
            var rid = Fix.Rid(331);
            using (var wal = Wal.Open(path))
            {
                node.AttachWal(wal);
                Assert.True(node.WriteField(1, rid, 0x100, new byte[] { 0x61 }));
                Assert.Equal(1, wal.Sequence);
                Assert.Equal(1, node.GetOutboxCount(1));
            }

            var recovered = Fix.MakeNode(135);
            Assert.Equal(1, Wal.ReplayFile(path, recovered));
            Assert.True(recovered.TryGetField(1, rid, 0x100, out var field));
            Assert.Equal(new byte[] { 0x61 }, field!.Value);
            var restored = Assert.Single(recovered.Outbox[1]);
            Assert.Equal(1, restored.Seq);
            Assert.Equal(1, recovered.OutboxSeqRef[(recovered.NodeId, 1)]);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Node_attached_wal_replays_field_and_record_lifecycle_with_exact_outbox()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-lifecycle-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "node.wal");
        var snapshotPath = Path.Combine(dir, "node.state");
        try
        {
            var node = Fix.MakeNode(137);
            var rid = Fix.Rid(333);
            node.WriteField(1, rid, 0x100, new byte[] { 0x01 });
            NodePersistence.Save(node, snapshotPath);

            using (var wal = Wal.Open(path))
            {
                node.AttachWal(wal);
                Assert.True(node.TombField(1, rid, 0x101));
                Assert.True(node.DeleteRecord(1, rid));
                Assert.True(node.UndeleteRecord(1, rid));
                var outboxOnly = new OpFieldWrite
                {
                    FieldId = 0x102, KeyEpoch = 0, Wall = 999, Logical = 1,
                    Origin = 1, AuthorNodeId = node.NodeId, Value = new byte[] { 0x7A }
                };
                Assert.NotNull(node.PushOutbox(1, rid, outboxOnly, node.NodeId));
                Assert.Equal(4, wal.Sequence);
            }

            var recovered = NodePersistence.Load(snapshotPath);
            Assert.Equal(4, Wal.ReplayFile(path, recovered));
            Assert.True(recovered.TryGetRecord(1, rid, out var record));
            Assert.True(record!.FieldTombs.ContainsKey(0x101));
            Assert.False(record.IsDead);
            Assert.False(record.State.ContainsKey(0x102)); // PushOutbox is Outbox-only.
            Assert.Equal(5, recovered.Outbox[1].Count); // includes the snapshot's pre-WAL entry
            Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, recovered.Outbox[1].Select(x => x.Seq).ToArray());
            Assert.Equal(5, recovered.OutboxSeqRef[(recovered.NodeId, 1)]);
            Assert.Equal(5, recovered.SeenChanges.Count);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task Attached_wal_checkpoint_is_consistent_with_concurrent_local_writes()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-attached-checkpoint-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var walPath = Path.Combine(dir, "node.wal");
        var snapshotPath = Path.Combine(dir, "node.state");
        try
        {
            var node = Fix.MakeNode(138);
            using var wal = Wal.Open(walPath);
            node.AttachWal(wal);
            var writer = Task.Run(() =>
            {
                for (int i = 0; i < 96; i++)
                    node.WriteField(1, Fix.Rid(4000 + i), 0x100, new byte[] { (byte)i });
            });
            var checkpoint = Task.Run(() => wal.Checkpoint(node, snapshotPath));
            await Task.WhenAll(writer, checkpoint).WaitAsync(TimeSpan.FromSeconds(10));

            var recovered = NodePersistence.Load(snapshotPath);
            Wal.ReplayFile(walPath, recovered);
            for (int i = 0; i < 96; i++)
                Assert.True(recovered.TryGetRecord(1, Fix.Rid(4000 + i), out _), $"Lost attached-WAL record {i}");
            Assert.Equal(96, recovered.Outbox[1].Count);
            Assert.Equal(96, recovered.OutboxSeqRef[(recovered.NodeId, 1)]);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Attached_wal_rejects_unlogged_local_sequence_reservations()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-seq-reserve-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var node = Fix.MakeNode(143);
            using var wal = Wal.Open(Path.Combine(dir, "node.wal"));
            node.AttachWal(wal);

            Assert.Throws<InvalidOperationException>(() => node.NextOutboxSeq(node.NodeId, 1));
            Assert.Equal(0, wal.Sequence);
            Assert.False(node.OutboxSeqRef.ContainsKey((node.NodeId, 1)));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Node_attached_wal_failure_does_not_publish_local_write()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-fail-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "node.wal");
        try
        {
            var node = Fix.MakeNode(136);
            var rid = Fix.Rid(332);
            var wal = Wal.Open(path);
            node.AttachWal(wal);
            wal.Dispose();

            Assert.Throws<ObjectDisposedException>(() => node.WriteField(1, rid, 0x100, new byte[] { 0x62 }));
            Assert.False(node.TryGetField(1, rid, 0x100, out _));
            Assert.False(node.Storage.ContainsKey(1)); // failed WAL append must not publish an empty namespace
            Assert.Equal(0, node.GetOutboxCount(1));
            Assert.False(node.OutboxSeqRef.ContainsKey((node.NodeId, 1)));

            Assert.Throws<ObjectDisposedException>(() => node.TombField(1, rid, 0x101));
            Assert.Throws<ObjectDisposedException>(() => node.DeleteRecord(1, rid));
            Assert.Throws<ObjectDisposedException>(() => node.UndeleteRecord(1, rid));
            Assert.False(node.Storage.ContainsKey(1));
            Assert.Equal(0, node.GetOutboxCount(1));
            Assert.False(node.OutboxSeqRef.ContainsKey((node.NodeId, 1)));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_v2_detects_payload_corruption_before_replay()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "node.wal");
            var source = Fix.MakeNode(132);
            var rid = Fix.Rid(307);
            using (var wal = Wal.Open(path))
            {
                source.AttachWal(wal);
                source.WriteField(1, rid, 0x100, new byte[] { 0x42 }, new Hlc(12345, 7));
            }

            var bytes = File.ReadAllBytes(path);
            bytes[bytes.Length - 17] ^= 0x01; // last payload byte, immediately before the digest
            File.WriteAllBytes(path, bytes);

            var target = Fix.MakeNode(132);
            Assert.Throws<InvalidDataException>(() => Wal.ReplayFile(path, target));
            Assert.False(target.TryGetRecord(1, rid, out _));
            Assert.Throws<InvalidDataException>(() => Wal.Open(path));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_rejects_local_mutation_payload_with_nonlocal_origin()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-origin-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var node = Fix.MakeNode(142);
            var op = new OpFieldWrite
            {
                FieldId = 0x100, KeyEpoch = 0, Wall = 100, Logical = 0,
                Origin = 2, AuthorNodeId = node.NodeId, Value = new byte[] { 1 }
            };
            var entry = new OutboxEntry
            {
                NamespaceId = 1, Seq = 1, ChangeId = BatchId.From(Guid.NewGuid().ToByteArray()),
                RecordId = Fix.Rid(9201), OpKind = OpsConst.TagFieldWrite,
                Payload = Ops.EncodeOp(op, fullAuthor: false), AuthorNodeId = node.NodeId,
                CreatedAt = 100
            };

            using var wal = Wal.Open(Path.Combine(dir, "node.wal"));
            Assert.Throws<InvalidDataException>(() => wal.AppendLocalMutation(entry, applyToStorage: true));
            Assert.Equal(0, wal.Sequence);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_replay_validates_all_local_sequences_before_mutating_node()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-sequence-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "node.wal");
            var source = Fix.MakeNode(141);
            using (var wal = Wal.Open(path))
            {
                var first = MakeLocalFieldEntry(source, Fix.Rid(9101), 0x100,
                    new byte[] { 1 }, new Hlc(100, 0), seq: 1);
                var gap = MakeLocalFieldEntry(source, Fix.Rid(9102), 0x100,
                    new byte[] { 2 }, new Hlc(200, 0), seq: 3);
                wal.AppendLocalMutation(first, applyToStorage: true);
                wal.AppendLocalMutation(gap, applyToStorage: true);
            }

            var target = Fix.MakeNode(141);
            Assert.Throws<InvalidDataException>(() => Wal.ReplayFile(path, target));
            Assert.False(target.TryGetRecord(1, Fix.Rid(9101), out _));
            Assert.False(target.TryGetRecord(1, Fix.Rid(9102), out _));
            Assert.Empty(target.Outbox);
            Assert.Empty(target.SeenChanges);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_replay_validates_entire_log_before_mutating_node()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var source = Fix.MakeNode(129);
            var rid = Fix.Rid(306);
            using (var wal = Wal.Open(walPath))
            {
                source.AttachWal(wal);
                source.WriteField(1, rid, 0x100, new byte[] { 0x42 }, new Hlc(300, 0));
            }

            // Append a complete unknown record with a valid v2 digest.
            using (var fs = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.None))
            {
                Span<byte> length = stackalloc byte[4];
                System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(length, 1);
                fs.Write(length);
                var malformed = new byte[] { 0xFF };
                fs.Write(malformed);
                fs.Write(Blake3Core.TaggedHash("BIN-SYNC/WAL-RECORD/v1", malformed).AsSpan(0, 16));
                fs.Flush(flushToDisk: true);
            }

            var target = Fix.MakeNode(129);
            Assert.Throws<InvalidDataException>(() => Wal.ReplayFile(walPath, target));
            Assert.False(target.TryGetRecord(1, rid, out _));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_checkpoint_truncates_after_snapshot()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var snapPath = Path.Combine(dir, "node.state");
            var node = Fix.MakeNode(122);
            var rid = Fix.Rid(301);

            using var wal = Wal.Open(walPath);
            node.AttachWal(wal);
            node.WriteField(1, rid, 0x100, new byte[] { 1 });
            Assert.Equal(1, wal.Sequence);

            wal.Checkpoint(node, snapPath);
            Assert.Equal(0, wal.Sequence);
            Assert.True(File.Exists(snapPath));

            var loaded = NodePersistence.Load(snapPath);
            Assert.True(loaded.Storage.ContainsKey(1) && loaded.Storage[1].ContainsKey(rid));
            var empty = Fix.MakeNode(122);
            Assert.Equal(0, Wal.ReplayFile(walPath, empty));

            // The replaced stream must be positioned after its fresh header;
            // the first post-checkpoint append must remain replayable.
            var afterCheckpoint = Fix.Rid(302);
            Assert.True(node.WriteField(1, afterCheckpoint, 0x100, new byte[] { 2 }));
            Assert.Equal(1, wal.Sequence);

            var recovered = NodePersistence.Load(snapPath);
            Assert.Equal(1, Wal.ReplayFile(walPath, recovered));
            Assert.True(recovered.TryGetRecord(1, afterCheckpoint, out _));
            Assert.Equal(2, recovered.Outbox[1].Count);
            Assert.Equal(2, recovered.OutboxSeqRef[(recovered.NodeId, 1)]);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task Wal_checkpoint_preserves_writes_racing_with_snapshot_and_truncate()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var snapPath = Path.Combine(dir, "node.state");
            var node = Fix.MakeNode(128);
            using var wal = Wal.Open(walPath);
            node.AttachWal(wal);

            var writer = Task.Run(() =>
            {
                for (int i = 0; i < 128; i++)
                {
                    var rid = Fix.Rid(2000 + i);
                    node.WriteField(1, rid, 0x100, new byte[] { (byte)i });
                }
            });

            var checkpoint = Task.Run(() => wal.Checkpoint(node, snapPath));
            await Task.WhenAll(writer, checkpoint).WaitAsync(TimeSpan.FromSeconds(10));

            var recovered = NodePersistence.Load(snapPath);
            Wal.ReplayFile(walPath, recovered);
            for (int i = 0; i < 128; i++)
                Assert.True(recovered.TryGetRecord(1, Fix.Rid(2000 + i), out _), $"Lost record {i}");
            Assert.Equal(128, recovered.Outbox[1].Count);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task ReplayFile_waits_for_node_gates_before_reading_wal()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-replay-gate-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "node.wal");
            var source = Fix.MakeNode(139);
            using (var wal = Wal.Open(path))
            {
                source.AttachWal(wal);
                source.WriteField(1, Fix.Rid(9001), 0x100, new byte[] { 1 });
            }

            var target = Fix.MakeNode(139);
            Task<int> replay;
            using (target.EnterAllGates())
            {
                replay = Task.Run(() => Wal.ReplayFile(path, target));
                Thread.Sleep(100); // Keep the Monitor-owning thread synchronous until disposal.
                Assert.False(replay.IsCompleted);
            }

            Assert.Equal(1, await replay.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.True(target.TryGetRecord(1, Fix.Rid(9001), out _));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void Wal_replay_after_snapshot_applies_delta()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var snapPath = Path.Combine(dir, "node.state");
            var node = Fix.MakeNode(124);
            var rid1 = Fix.Rid(302);
            var rid2 = Fix.Rid(303);
            node.WriteField(1, rid1, 0x100, new byte[] { 10 });
            NodePersistence.Save(node, snapPath);

            using (var wal = Wal.Open(walPath))
            {
                node.AttachWal(wal);
                node.WriteField(1, rid2, 0x100, new byte[] { 20 });
            }

            var recovered = NodePersistence.Load(snapPath);
            int n = Wal.ReplayFile(walPath, recovered);
            Assert.Equal(1, n);
            Assert.True(recovered.Storage[1].ContainsKey(rid1));
            Assert.True(recovered.Storage[1].ContainsKey(rid2));
            Assert.Equal(new byte[] { 20 }, recovered.Storage[1][rid2].State[0x100].Value);
            Assert.Equal(2, recovered.Outbox[1].Count);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }


}
