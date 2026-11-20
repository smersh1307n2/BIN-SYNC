// §14.5 opt-in WriteFieldSealed / ReadFieldOpen.
using System.Security.Cryptography;
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class E2eeWriteTests
{
    static byte[] Rid(byte b)
    {
        var a = new byte[16];
        a[0] = b;
        return a;
    }

    [Fact]
    public void WriteFieldSealed_stores_ciphertext_not_plaintext()
    {
        var n = Fix.MakeNode(200);
        var key = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key, keyEpoch: 0);

        var rid = Rid(1);
        var plain = System.Text.Encoding.UTF8.GetBytes("top-secret");
        Assert.True(n.WriteFieldSealed(1, rid, 0x100, plain));

        var fs = n.Storage[1][rid].State[0x100];
        Assert.NotEqual(plain, fs.Value);
        Assert.True(fs.Value.Length >= plain.Length + E2ee.TagSize);
    }

    [Fact]
    public void ReadFieldOpen_roundtrip()
    {
        var n = Fix.MakeNode(201);
        var key = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key);

        var rid = Rid(2);
        var plain = new byte[] { 9, 8, 7, 6 };
        n.WriteFieldSealed(1, rid, 0x200, plain);
        var opened = n.ReadFieldOpen(1, rid, 0x200);
        Assert.Equal(plain, opened);
    }

    [Fact]
    public void WriteFieldSealed_without_key_throws()
    {
        var n = Fix.MakeNode(202);
        n.E2eeKeys = new E2eeKeyRing();
        Assert.Throws<InvalidOperationException>(() =>
            n.WriteFieldSealed(1, Rid(1), 1, new byte[] { 1 }));
    }

    [Fact]
    public void WriteFieldSealed_without_ring_throws()
    {
        var n = Fix.MakeNode(203);
        Assert.Null(n.E2eeKeys);
        Assert.Throws<InvalidOperationException>(() =>
            n.WriteFieldSealed(1, Rid(1), 1, new byte[] { 1 }));
    }

    [Fact]
    public void Plain_WriteField_unaffected_when_ring_present()
    {
        var n = Fix.MakeNode(204);
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey());
        var rid = Rid(3);
        var plain = new byte[] { 0xDE, 0xAD };
        Assert.True(n.WriteField(1, rid, 0x300, plain));
        Assert.Equal(plain, n.Storage[1][rid].State[0x300].Value);
    }

    [Fact]
    public void KeyEpoch_recorded_on_FieldState()
    {
        var n = Fix.MakeNode(205);
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey(), keyEpoch: 3);
        n.WriteFieldSealed(1, Rid(4), 0x400, new byte[] { 1 });
        Assert.Equal(3, n.Storage[1][Rid(4)].State[0x400].KeyEpoch);
    }

    [Fact]
    public void AdvanceEpoch_changes_nonce_context()
    {
        var n = Fix.MakeNode(206);
        var key = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key, keyEpoch: 0);
        var rid = Rid(5);
        n.WriteFieldSealed(1, rid, 0x500, new byte[] { 1, 2 });
        var ct0 = n.Storage[1][rid].State[0x500].Value.ToArray();

        n.E2eeKeys.AdvanceEpoch(1, 1);
        var h = new Hlc(n.Clock.SendTick().Wall + 1000, 0);
        n.WriteFieldSealed(1, rid, 0x501, new byte[] { 1, 2 }, hlc: h);
        var ct1 = n.Storage[1][rid].State[0x501].Value;
        Assert.NotEqual(ct0, ct1);
        Assert.Equal(1, n.Storage[1][rid].State[0x501].KeyEpoch);
    }

    [Fact]
    public void WriteFieldSealed_wal_failure_does_not_publish_storage_or_outbox()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-e2ee-wal-fail-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var node = Fix.MakeNode(210);
            node.E2eeKeys = new E2eeKeyRing();
            node.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey());
            var wal = Wal.Open(Path.Combine(dir, "node.wal"));
            node.AttachWal(wal);
            wal.Dispose();

            var recordId = RecordId.From(Rid(8));
            Assert.Throws<ObjectDisposedException>(() =>
                node.WriteFieldSealed(1, recordId, 0x800, new byte[] { 1, 2, 3 }));

            Assert.False(node.TryGetField(1, recordId, 0x800, out _));
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
    public void WriteFieldSealed_wal_replay_restores_ciphertext_and_key_epoch()
    {
        var dir = Path.Combine(Path.GetTempPath(), "binsync-e2ee-wal-replay-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var node = Fix.MakeNode(212);
            var key = E2ee.GenerateNamespaceKey();
            node.E2eeKeys = new E2eeKeyRing();
            node.E2eeKeys.Set(1, key, keyEpoch: 4);
            var path = Path.Combine(dir, "node.wal");
            var wal = Wal.Open(path);
            node.AttachWal(wal);
            var recordId = RecordId.From(Rid(10));
            var plaintext = new byte[] { 7, 8, 9 };

            Assert.True(node.WriteFieldSealed(1, recordId, 0x802, plaintext));
            wal.Dispose();

            var recovered = new Node(node.NodeId, epoch: node.Epoch)
            {
                E2eeKeys = new E2eeKeyRing(),
            };
            recovered.E2eeKeys!.Set(1, key, keyEpoch: 4);
            Assert.Equal(1, Wal.ReplayFile(path, recovered));
            Assert.True(recovered.TryGetField(1, recordId, 0x802, out var field));
            Assert.Equal((ushort)4, field!.KeyEpoch);
            Assert.Equal(plaintext, recovered.ReadFieldOpen(1, recordId, 0x802));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void WriteFieldSealed_raises_field_written_after_releasing_node_gates()
    {
        var node = Fix.MakeNode(211);
        node.E2eeKeys = new E2eeKeyRing();
        node.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey());
        int events = 0;

        node.FieldWritten += (_, _, _) =>
        {
            if (Interlocked.Increment(ref events) == 1)
            {
                var writer = Task.Run(() =>
                    node.WriteField(1, RecordId.From(Rid(9)), 0x801, new byte[] { 4 }));
                Assert.True(writer.Wait(TimeSpan.FromSeconds(3)));
            }
        };

        Assert.True(node.WriteFieldSealed(1, RecordId.From(Rid(9)), 0x800, new byte[] { 1, 2, 3 }));
        Assert.Equal(2, events);
    }

    [Fact]
    public void Wrong_key_cannot_open()
    {
        var n = Fix.MakeNode(207);
        var key1 = E2ee.GenerateNamespaceKey();
        n.E2eeKeys = new E2eeKeyRing();
        n.E2eeKeys.Set(1, key1);
        n.WriteFieldSealed(1, Rid(6), 0x600, new byte[] { 0xAA });

        n.E2eeKeys.Set(1, E2ee.GenerateNamespaceKey());
        Assert.ThrowsAny<CryptographicException>(() =>
            n.ReadFieldOpen(1, Rid(6), 0x600));
    }

    [Fact]
    public void TryGet_returns_an_owned_copy_not_the_internal_key_buffer()
    {
        var ring = new E2eeKeyRing();
        var original = Enumerable.Range(0, E2ee.KeySize).Select(i => (byte)i).ToArray();
        ring.Set(7, original, keyEpoch: 2);

        Assert.True(ring.TryGet(7, out var copy, out var epoch));
        Assert.Equal((ushort)2, epoch);
        Assert.NotSame(original, copy);
        copy[0] ^= 0xFF;

        Assert.True(ring.TryGet(7, out var secondCopy, out _));
        Assert.Equal(original, secondCopy);
        CryptographicOperations.ZeroMemory(copy);
        CryptographicOperations.ZeroMemory(secondCopy);
    }

    [Fact]
    public void Shared_key_opens_peer_ciphertext()
    {
        var key = E2ee.GenerateNamespaceKey();
        var a = Fix.MakeNode(208);
        a.E2eeKeys = new E2eeKeyRing();
        a.E2eeKeys.Set(1, key);

        var rid = Rid(7);
        var plain = System.Text.Encoding.UTF8.GetBytes("shared-secret");
        a.WriteFieldSealed(1, rid, 0x700, plain);
        var fs = a.Storage[1][rid].State[0x700];

        var opened = E2ee.OpenField(
            key, rid, 0x700, fs.Timestamp.Wall, fs.Timestamp.Logical,
            fs.AuthorNodeId, fs.KeyEpoch, fs.Value);
        Assert.Equal(plain, opened);
    }
}
