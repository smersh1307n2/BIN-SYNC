// WAL append / replay / checkpoint tests.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class WalTests
{
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
    public void Wal_append_and_replay_restores_field()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var walPath = Path.Combine(dir, "node.wal");
            var a = Fix.MakeNode(120);
            var rid = Fix.Rid(300);
            var hlc = new Hlc(12345, 7);

            using (var wal = Wal.Open(walPath))
            {
                wal.AppendFieldWrite(1, rid, 0x100, new byte[] { 0x42 }, hlc, a.NodeId);
                Assert.True(wal.Sequence >= 1);
            }

            var b = Fix.MakeNode(121);
            int applied = Wal.ReplayFile(walPath, b);
            Assert.Equal(1, applied);
            Assert.True(b.Storage.ContainsKey(1) && b.Storage[1].ContainsKey(rid));
            Assert.Equal(new byte[] { 0x42 }, b.Storage[1][rid].State[0x100].Value);
            Assert.Equal(12345u, b.Storage[1][rid].State[0x100].Timestamp.Wall);
            Assert.Equal(7u, b.Storage[1][rid].State[0x100].Timestamp.Logical);
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
            var second = Fix.MakeNode(126);
            var firstRid = Fix.Rid(304);
            var secondRid = Fix.Rid(305);

            using (var wal = Wal.Open(walPath))
            {
                wal.AppendFieldWrite(1, firstRid, 0x100, new byte[] { 1 },
                    new Hlc(100, 0), first.NodeId);
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

            using (var wal = Wal.Open(walPath))
            {
                Assert.Equal(1, wal.Sequence);
                wal.AppendFieldWrite(1, secondRid, 0x100, new byte[] { 2 },
                    new Hlc(200, 0), second.NodeId);
                Assert.Equal(2, wal.Sequence);
            }

            var recovered = Fix.MakeNode(127);
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
                wal.AppendFieldWrite(1, rid, 0x100, new byte[] { 0x42 },
                    new Hlc(12345, 7), source.NodeId);

            var bytes = File.ReadAllBytes(path);
            bytes[6 + 4 + 1 + 1 + 16 + 2 + 1] ^= 0x01;
            File.WriteAllBytes(path, bytes);

            var target = Fix.MakeNode(133);
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
    public void Wal_v1_remains_readable_and_truncate_upgrades_to_v2()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-wal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "legacy.wal");
            var source = Fix.MakeNode(134);
            var rid = Fix.Rid(308);
            var payload = new Writer().U8((byte)WalRecordType.FieldWrite)
                .Varint(1).B128(rid).U16(0x100).Bytes(new byte[] { 0x24 })
                .U64(500).U32(2).B128(source.NodeId).Value();

            using (var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                fs.Write(new byte[] { 0x42, 0x53, 0x57, 0x4C, 0x00, 0x01 });
                Span<byte> length = stackalloc byte[4];
                System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(length, payload.Length);
                fs.Write(length);
                fs.Write(payload);
                fs.Flush(flushToDisk: true);
            }

            var restored = Fix.MakeNode(135);
            Assert.Equal(1, Wal.ReplayFile(path, restored));
            Assert.True(restored.TryGetRecord(1, rid, out _));
            using (var wal = Wal.Open(path)) wal.Truncate();

            var upgraded = File.ReadAllBytes(path);
            Assert.Equal(2, System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(upgraded.AsSpan(4)));
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
            var validPayload = new Writer().U8((byte)WalRecordType.FieldWrite)
                .Varint(1).B128(rid).U16(0x100).Bytes(new byte[] { 0x42 })
                .U64(300).U32(0).B128(source.NodeId).Value();

            // Legacy v1 is used to construct a complete malformed frame
            // without bypassing v2's mandatory record digest.
            using (var fs = new FileStream(walPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                fs.Write(new byte[] { 0x42, 0x53, 0x57, 0x4C, 0x00, 0x01 });
                Span<byte> length = stackalloc byte[4];
                System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(length, validPayload.Length);
                fs.Write(length);
                fs.Write(validPayload);
                System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(length, 1);
                fs.Write(length);
                fs.Write(new byte[] { 0xFF });
                fs.Flush(flushToDisk: true);
            }

            var target = Fix.MakeNode(130);
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
            var a = Fix.MakeNode(122);
            var rid = Fix.Rid(301);
            a.WriteField(1, rid, 0x100, new byte[] { 1 });

            using var wal = Wal.Open(walPath);
            wal.AppendFieldWrite(1, rid, 0x100, new byte[] { 1 },
                a.Storage[1][rid].State[0x100].Timestamp, a.NodeId);
            Assert.True(wal.Sequence >= 1);

            wal.Checkpoint(a, snapPath);
            Assert.Equal(0, wal.Sequence);
            Assert.True(File.Exists(snapPath));

            var loaded = NodePersistence.Load(snapPath);
            Assert.True(loaded.Storage.ContainsKey(1) && loaded.Storage[1].ContainsKey(rid));

            var empty = Fix.MakeNode(123);
            Assert.Equal(0, Wal.ReplayFile(walPath, empty));
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

            var writer = Task.Run(() =>
            {
                for (int i = 0; i < 128; i++)
                {
                    var rid = Fix.Rid(2000 + i);
                    node.WriteField(1, rid, 0x100, new byte[] { (byte)i });
                    Assert.True(node.TryGetField(1, rid, 0x100, out var field));
                    wal.AppendFieldWrite(1, rid, 0x100, field!.Value,
                        field.Timestamp, node.NodeId.ToArray());
                }
            });

            var checkpoint = Task.Run(() => wal.Checkpoint(node, snapPath));
            await Task.WhenAll(writer, checkpoint).WaitAsync(TimeSpan.FromSeconds(10));

            var recovered = NodePersistence.Load(snapPath);
            Wal.ReplayFile(walPath, recovered);
            for (int i = 0; i < 128; i++)
                Assert.True(recovered.TryGetRecord(1, Fix.Rid(2000 + i), out _), $"Lost record {i}");
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
            var a = Fix.MakeNode(124);
            var rid1 = Fix.Rid(302);
            var rid2 = Fix.Rid(303);
            a.WriteField(1, rid1, 0x100, new byte[] { 10 });
            NodePersistence.Save(a, snapPath);

            using (var wal = Wal.Open(walPath))
            {
                a.WriteField(1, rid2, 0x100, new byte[] { 20 });
                var fs = a.Storage[1][rid2].State[0x100];
                wal.AppendFieldWrite(1, rid2, 0x100, fs.Value, fs.Timestamp, a.NodeId);
            }

            var recovered = NodePersistence.Load(snapPath);
            int n = Wal.ReplayFile(walPath, recovered);
            Assert.Equal(1, n);
            Assert.True(recovered.Storage[1].ContainsKey(rid1));
            Assert.True(recovered.Storage[1].ContainsKey(rid2));
            Assert.Equal(new byte[] { 20 }, recovered.Storage[1][rid2].State[0x100].Value);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }
}
