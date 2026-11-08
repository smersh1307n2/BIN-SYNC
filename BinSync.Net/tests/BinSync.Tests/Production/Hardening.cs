using Xunit;

// Production-hardening suite for BinSync.Net: in-tree BLAKE3, atomic persistence of the
// FULL node state, async transport (cancellation + §12.18 gate discipline), and
// thread-safety of the re-entrant node gate.
using System.Buffers.Binary;

namespace BinSync.Tests;

public class Production : IDisposable
{
    private readonly string _dir = MakeTempDir();

    private static string MakeTempDir()
    {
        var d = Path.Combine(Path.GetTempPath(), "binsync-prod-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        return d;
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private const int NS = 7;
    private static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    /// Build two fully-synced nodes with rich state: local writes, remote-authored
    /// fields (via sync), lifecycle tomb, outbox seq counters, checkpoints, aliases,
    /// peer cursors, seen-set — everything NodePersistence must round-trip.
    private static (Node a, Node b) BuildHistory()
    {
        var a = Fix.MakeNode(1);
        var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(100), 0x101, Val("hello"));
        a.WriteField(NS, Fix.Rid(100), 0x102, Val("world"));
        a.WriteField(NS, Fix.Rid(101), 0x101, Val("solo"));
        b.WriteField(NS, Fix.Rid(200), 0x101, Val("from-b"));
        b.TombField(NS, Fix.Rid(200), 0x102);
        Transport.SyncRound(a, b, NS);
        a.DeleteRecord(NS, Fix.Rid(101));           // dead record w/ ghost semantics
        Transport.SyncRound(a, b, NS);
        return (a, b);
    }

    private static bool StateEqual(Node x, Node y)
    {
        if (x.NodeId != y.NodeId) return false;
        if (x.Epoch != y.Epoch) return false;
        if (x.Clock.LastWall != y.Clock.LastWall || x.Clock.LastLogical != y.Clock.LastLogical) return false;
        if (x.Storage.Count != y.Storage.Count) return false;
        foreach (var (ns, recs) in x.Storage)
        {
            if (!y.Storage.TryGetValue(ns, out var other)) return false;
            if (recs.Count != other.Count) return false;
            foreach (var (rid, rec) in recs)
            {
                if (!other.TryGetValue(rid, out var o)) return false;
                if (rec.LifecycleValue != o.LifecycleValue) return false;
                if ((rec.LifecycleHlc is null) != (o.LifecycleHlc is null)) return false;
                if (rec.LifecycleHlc is not null &&
                    (rec.LifecycleHlc!.Wall != o.LifecycleHlc!.Wall || rec.LifecycleHlc.Logical != o.LifecycleHlc.Logical ||
                     rec.LifecycleAuthor != o.LifecycleAuthor)) return false;
                if (rec.State.Count != o.State.Count) return false;
                foreach (var (fid, fs) in rec.State)
                {
                    if (!o.State.TryGetValue(fid, out var ofs)) return false;
                    if (fs.Timestamp.Wall != ofs.Timestamp.Wall || fs.Timestamp.Logical != ofs.Timestamp.Logical) return false;
                    if (fs.AuthorNodeId != ofs.AuthorNodeId) return false;
                    if (!fs.Value.AsSpan().SequenceEqual(ofs.Value)) return false;
                }
                if (rec.FieldTombs.Count != o.FieldTombs.Count) return false;
                foreach (var (tfid, tfs) in rec.FieldTombs)
                {
                    if (!o.FieldTombs.TryGetValue(tfid, out var otfs)) return false;
                    if (tfs.Timestamp.Key() != otfs.Timestamp.Key()) return false;
                }
            }
        }
        if (x.Inbox.Count != y.Inbox.Count) return false;
        if (x.InboxAuthors.Count != y.InboxAuthors.Count) return false;
        foreach (var (key, author) in x.InboxAuthors)
            if (!y.InboxAuthors.TryGetValue(key, out var otherAuthor) || !otherAuthor.Equals(author)) return false;
        if (x.LastBatchDigest.Count != y.LastBatchDigest.Count) return false;
        foreach (var (k, dig) in x.LastBatchDigest)
            if (!y.LastBatchDigest.TryGetValue(k, out var od) || dig != od) return false;
        if (x.LastStreamDigest.Count != y.LastStreamDigest.Count) return false;
        if (x.DurableAuthors.Count != y.DurableAuthors.Count) return false;
        if (!x.SeenChanges.SetEquals(y.SeenChanges)) return false;
        if (x.KnownEpochs.Count != y.KnownEpochs.Count) return false;
        foreach (var (k, ep) in x.KnownEpochs)
            if (!y.KnownEpochs.TryGetValue(k, out var oe) || ep != oe) return false;
        if (x.AliasToNode.Count != y.AliasToNode.Count) return false;
        foreach (var (peer, map) in x.AliasToNode)
        {
            if (!y.AliasToNode.TryGetValue(peer, out var om) || map.Count != om.Count) return false;
            foreach (var (alias, (nid, ep)) in map)
            {
                if (!om.TryGetValue(alias, out var onidEp)) return false;
                if (nid != onidEp.Id || ep != onidEp.Epoch) return false;
            }
        }
        if (x.NodeToAlias.Count != y.NodeToAlias.Count) return false;
        foreach (var (peer, map) in x.NodeToAlias)
        {
            if (!y.NodeToAlias.TryGetValue(peer, out var om) || map.Count != om.Count) return false;
            foreach (var (binding, alias) in map)
                if (!om.TryGetValue(binding, out var otherAlias) || alias != otherAlias) return false;
        }
        if (x.PeerStates.Count != y.PeerStates.Count) return false;
        foreach (var (k, ps) in x.PeerStates)
        {
            if (!y.PeerStates.TryGetValue(k, out var ops)) return false;
            if (ps.LastPushedSeq != ops.LastPushedSeq || ps.LastReceivedSeq != ops.LastReceivedSeq ||
                ps.LastAckedSeq != ops.LastAckedSeq || ps.InitialSyncComplete != ops.InitialSyncComplete) return false;
            if (ps.ReceivedStreams.Count != ops.ReceivedStreams.Count) return false;
            foreach (var (streamKey, seq) in ps.ReceivedStreams)
                if (!ops.ReceivedStreams.TryGetValue(streamKey, out var otherSeq) || otherSeq != seq) return false;
            if (ps.AuthorProgress.Count != ops.AuthorProgress.Count) return false;
            foreach (var (author, progress) in ps.AuthorProgress)
                if (!ops.AuthorProgress.TryGetValue(author, out var otherProgress) ||
                    !otherProgress.Equals(progress)) return false;
            if (ps.IdsRef.Count != ops.IdsRef.Count) return false;
        }
        // Outbox entries + per-author seq counters
        var sx = x.OutboxSeqRef;
        var sy = y.OutboxSeqRef;
        if (sx.Count != sy.Count) return false;
        foreach (var ((author, ns), seq) in sx)
            if (!sy.TryGetValue((author, ns), out var os) || seq != os) return false;
        if (x.Outbox.Count != y.Outbox.Count) return false;
        foreach (var (ns, list) in x.Outbox)
        {
            if (!y.Outbox.TryGetValue(ns, out var ol) || list.Count != ol.Count) return false;
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i].Seq != ol[i].Seq || list[i].OpKind != ol[i].OpKind) return false;
                if (list[i].ChangeId != ol[i].ChangeId) return false;
                if (!list[i].Payload.Span.SequenceEqual(ol[i].Payload.Span)) return false;
                if (list[i].CausalDeps.Count != ol[i].CausalDeps.Count) return false;
            }
        }
        return true;
    }

    // ============================================================ BLAKE3 (in-tree)

    [Fact]
    public void Blake3_MatchesOfficialTestVectors()
    {
        // Official BLAKE3 test vectors (hash_test.json), input[i] = i % 251, first 24 hex chars.
        var vectors = new (int Len, string Prefix)[]
        {
            (0,     "af1349b9f5f9a1a6a0404dea"),
            (1,     "2d3adedff11b61f14c886e35"),
            (2,     "7b7015bb92cf0b318037702a"),
            (5,     "b40b44dfd97e7a84a996a91a"),
            (10,    "87fcf07cac5be3c91735b34e"),
            (1023,  "10108970eeda3eb932baac14"),
            (1024,  "42214739f095a406f3fc83de"),
            (1025,  "d00278ae47eb27b34faecf67"),
            (2048,  "e776b6028c7cd22a4d0ba182"),
            (2080,  "f28b7ca0b5de51703019b0b3"),
            (3072,  "b98cb0ff3623be03326b373d"),
            (4096,  "015094013f57a5277b59d847"),
            (5000,  "ee78d92070de3df1c57c3700"),
            (31774, "fdba8afea01e015596a4289c"),
        };
        foreach (var (len, prefix) in vectors)
        {
            var data = new byte[len];
            for (int i = 0; i < len; i++) data[i] = (byte)(i % 251);
            var got = Convert.ToHexString(Blake3Core.Hash256(data)).ToLowerInvariant();
            Assert.True(got.StartsWith(prefix, StringComparison.Ordinal),
                $"BLAKE3 mismatch at len={len}: got {got[..Math.Min(24, got.Length)]}…, expected {prefix}…");
        }
    }

    [Fact]
    public void Blake3_TaggedHash_IsDomainSeparated()
    {
        var data = Val("state");
        var h1 = Blake3Core.TaggedHash("BIN-SYNC/NODESTATE/v1", data);
        var h2 = Blake3Core.TaggedHash("BIN-SYNC/OTHER/v1", data);
        var plain = Blake3Core.Hash256(data);
        Assert.False(h1.AsSpan().SequenceEqual(h2));
        Assert.False(h1.AsSpan().SequenceEqual(plain));
        // Deterministic: same tag+input ⇒ same digest.
        Assert.Equal(Convert.ToHexString(h1), Convert.ToHexString(Blake3Core.TaggedHash("BIN-SYNC/NODESTATE/v1", data)));
    }

    [Fact]
    public void Digests_StillUseInTreeBlake3_AfterDependencyRemoval()
    {
        // The core hash entry point must equal the in-tree implementation bit-for-bit.
        var data = Val("bin-sync dependency-free check");
        Assert.Equal(Blake3Core.Hash256(data), Digests.Blake3(data));
        Assert.Equal(Blake3Core.TaggedHash("BIN-SYNC/NODESTATE/v1", data),
                     Digests.TaggedBlake3("BIN-SYNC/NODESTATE/v1", data));
    }

    // ============================================================ Persistence

    [Fact]
    public void Persistence_RoundTrip_PreservesFullState()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "nodeA.state");
        NodePersistence.Save(a, path);
        var restored = NodePersistence.Load(path);
        Assert.True(StateEqual(a, restored), "restored node state diverges from the original");
    }

    [Fact]
    public void Persistence_SequencesContinueAfterRestart()
    {
        var (a, _) = BuildHistory();
        var seqMap = a.OutboxSeqRef;
        long before = seqMap[(a.NodeId, NS)];
        Assert.True(before > 0);
        var path = Path.Combine(_dir, "seq.state");
        NodePersistence.Save(a, path);

        var restored = NodePersistence.Load(path);
        var rmap = restored.OutboxSeqRef;
        Assert.Equal(before, rmap[(restored.NodeId, NS)]);

        // A fresh write on the restored node must continue the durable per-author stream
        // (OutboxSeqRef). Cursor span for a brand-new peer is gap-normalised independently.
        var freshPeer = Fix.MakeNode(99);
        Transport.MakeSession(restored, freshPeer);
        restored.MarkInitialSyncComplete(freshPeer.NodeId, NS); // test resumes after a completed snapshot baseline
        restored.WriteField(NS, Fix.Rid(300), 0x101, Val("post-restart"));
        long after = restored.OutboxSeqRef[(restored.NodeId, NS)];
        Assert.Equal(before + 1, after);
        var batch = restored.CreateDeltaBatch(freshPeer.NodeId, NS);
        Assert.NotNull(batch);
        Assert.True(batch!.Entries.Count >= 1);
        Assert.Contains(batch.Entries, e => e.RecordId == Fix.Rid(300));
    }

    [Fact]
    public void Persistence_V16TagAndAuthorProgressAndAliasIndexesRoundTrip()
    {
        var (a, _) = BuildHistory();
        var peer = a.PeerStates.Values.First();
        var author = a.NodeId;
        peer.SetStreamOffset(author, 17);
        var path = Path.Combine(_dir, "tag-v16.state");
        NodePersistence.Save(a, path);
        var bytes = File.ReadAllBytes(path);
        Assert.Equal((ushort)16, BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(4, 2)));
        var bodyLen = checked((int)BinaryPrimitives.ReadUInt64BigEndian(bytes.AsSpan(6, 8)));
        var body = bytes.AsSpan(4 + 2 + 8 + 32, bodyLen).ToArray();
        var digest = Blake3Core.TaggedHash("BIN-SYNC/NODESTATE/v16", body);
        Assert.Equal(digest, bytes.AsSpan(14, 32).ToArray());
        var restored = NodePersistence.LoadFromBytes(bytes);
        Assert.True(StateEqual(a, restored));
    }


    [Fact]
    public void Persistence_StreamOffsetSurvivesRestartAndMapsNextAck()
    {
        var donor = Fix.MakeNode(861);
        var oldPeer = Fix.MakeNode(862);
        var newPeer = Fix.MakeNode(863);
        Transport.MakeSession(donor, oldPeer);

        donor.WriteField(NS, Fix.Rid(861), 0x100, Val("prefix-1"));
        donor.WriteField(NS, Fix.Rid(862), 0x100, Val("prefix-2"));
        var oldBatch = donor.CreateDeltaBatch(oldPeer.NodeId, NS)!;
        donor.AdvanceStream(oldPeer.NodeId, NS, oldBatch);
        donor.ProcessAck(oldPeer.NodeId, NS, oldBatch.CursorEnd, oldBatch.DurableAuthor);
        Assert.Equal(2L, donor.PeerStates[(oldPeer.NodeId, NS)].AckedFor(donor.NodeId));

        // Prefix is now acknowledged by the only existing peer and may be collected.
        donor.CompactOutbox(NS);
        Assert.Empty(donor.Outbox[NS]);

        Transport.MakeSession(donor, newPeer);
        var snapshot = Reconcile.BuildSnapshot(donor, NS);
        Assert.True(Reconcile.ApplySnapshot(newPeer, snapshot) >= 2);

        donor.WriteField(NS, Fix.Rid(863), 0x100, Val("tail-3"));
        Assert.Throws<SyncException>(() => donor.CreateDeltaBatch(newPeer.NodeId, NS));
        donor.PeerStates[(newPeer.NodeId, NS)].InitialSyncComplete = true; // simulate completed snapshot receipt
        var tail = donor.CreateDeltaBatch(newPeer.NodeId, NS)!;
        Assert.Equal(1L, tail.CursorStart);
        Assert.Equal(1L, tail.CursorEnd);
        var received = newPeer.ApplyBatchPayload(donor.EncodeBatchPayload(tail), donor.NodeId);
        donor.AdvanceStream(newPeer.NodeId, NS, tail);
        donor.ProcessAck(newPeer.NodeId, NS, received.CursorEnd, received.DurableAuthor);
        Assert.Equal(1L, donor.PeerStates[(newPeer.NodeId, NS)].AckedFor(donor.NodeId));
        Assert.True(donor.PeerStates[(newPeer.NodeId, NS)].TryGetStreamOffset(donor.NodeId, out var donorOffset));
        Assert.Equal(2L, donorOffset);

        var path = Path.Combine(_dir, "stream-offset-restart.state");
        NodePersistence.Save(donor, path);
        var resumed = NodePersistence.Load(path);
        Assert.True(resumed.PeerStates[(newPeer.NodeId, NS)].TryGetStreamOffset(donor.NodeId, out var resumedOffset));
        Assert.Equal(2L, resumedOffset);

        // The persisted alias table is sufficient for a resumed sender to rebuild
        // the exact peer-relative cursor before a live session is reattached.
        resumed.WriteField(NS, Fix.Rid(864), 0x100, Val("tail-4"));
        var next = resumed.CreateDeltaBatch(newPeer.NodeId, NS)!;
        Assert.Equal(2L, next.CursorStart);
        Assert.Equal(2L, next.CursorEnd);
        resumed.AdvanceStream(newPeer.NodeId, NS, next);
        resumed.ProcessAck(newPeer.NodeId, NS, next.CursorEnd, next.DurableAuthor);

        var peerState = resumed.PeerStates[(newPeer.NodeId, NS)];
        Assert.Equal(2L, peerState.AckedFor(resumed.NodeId));
        Assert.True(peerState.TryGetStreamOffset(resumed.NodeId, out var peerOffset));
        Assert.Equal(2L, peerOffset);
        Assert.Contains(resumed.Outbox[NS], e => e.Seq == 4);
    }

    [Fact]
    public void CompactOutbox_UsesMinimumPeerAckAndFailsClosedForMissingPeerState()
    {
        var donor = Fix.MakeNode(870);
        var peerA = Fix.MakeNode(871);
        var peerB = Fix.MakeNode(872);
        donor.WriteField(NS, Fix.Rid(870), 0x100, Val("one"));
        donor.WriteField(NS, Fix.Rid(871), 0x100, Val("two"));
        Transport.MakeSession(donor, peerA);
        Transport.MakeSession(donor, peerB);

        var batchA = donor.CreateDeltaBatch(peerA.NodeId, NS)!;
        donor.AdvanceStream(peerA.NodeId, NS, batchA);
        var batchB = donor.CreateDeltaBatch(peerB.NodeId, NS)!;
        donor.AdvanceStream(peerB.NodeId, NS, batchB);

        donor.ProcessAck(peerA.NodeId, NS, batchA.CursorEnd, batchA.DurableAuthor);
        Assert.Equal(0L, donor.GetSafeOutboxAckedThrough(NS, donor.NodeId));
        Assert.Equal(2, donor.Outbox[NS].Count); // peer B is still behind
        donor.ProcessAck(peerB.NodeId, NS, batchB.CursorEnd, batchB.DurableAuthor);
        Assert.Equal(2L, donor.GetSafeOutboxAckedThrough(NS, donor.NodeId));
        Assert.Empty(donor.Outbox[NS]);

        var missingState = Fix.MakeNode(873);
        missingState.WriteField(NS, Fix.Rid(873), 0x100, Val("blocked"));
        missingState.AliasToNode[Fix.Nid(874)] = new Dictionary<int, (NodeId Id, int Epoch)>();
        missingState.CompactOutbox(NS);
        Assert.Equal(0L, missingState.GetSafeOutboxAckedThrough(NS, missingState.NodeId));
        Assert.Single(missingState.Outbox[NS]); // known peer without PeerState blocks GC

        var noPeers = Fix.MakeNode(875);
        noPeers.WriteField(NS, Fix.Rid(875), 0x100, Val("offline"));
        noPeers.CompactOutbox(NS);
        Assert.Equal(0L, noPeers.GetSafeOutboxAckedThrough(NS, noPeers.NodeId));
        Assert.Single(noPeers.Outbox[NS]); // no interested peers means retain history
    }

    [Fact]
    public void OutboxChangeIdentity_ResolvesRetainedSequencesAndFailsClosedWhenMissing()
    {
        var node = Fix.MakeNode(880);
        node.WriteField(NS, Fix.Rid(880), 0x100, Val("first"));
        node.WriteField(NS, Fix.Rid(881), 0x100, Val("second"));
        var entries = node.Outbox[NS];
        var requested = new[]
        {
            new OutboxChangeIdentity(entries[1].AuthorNodeId, entries[1].ChangeId),
            new OutboxChangeIdentity(entries[0].AuthorNodeId, entries[0].ChangeId),
        };
        var sequences = new long[requested.Length];

        Assert.True(node.TryResolveOutboxSequences(NS, requested, sequences));
        Assert.Equal(new long[] { 2, 1 }, sequences);

        var missing = new[]
        {
            new OutboxChangeIdentity(Fix.Nid(882), BatchId.From(Fix.Rid(0xF8).ToArray())),
        };
        var missingSequences = new long[] { 123 };
        Assert.False(node.TryResolveOutboxSequences(NS, missing, missingSequences));
        Assert.Equal(0L, missingSequences[0]);
        Assert.True(node.TryResolveOutboxSequences(NS,
            ReadOnlySpan<OutboxChangeIdentity>.Empty, Span<long>.Empty));

        var duplicateChangeId = new[]
        {
            requested[0],
            new OutboxChangeIdentity(Fix.Nid(883), requested[0].ChangeId),
        };
        bool duplicateRejected = false;
        try { node.TryResolveOutboxSequences(NS, duplicateChangeId, new long[2]); }
        catch (ArgumentException) { duplicateRejected = true; }
        Assert.True(duplicateRejected);
    }

    [Fact]
    public void Persistence_UnifiedInboxReceiptMapDoesNotDuplicateKeysAndRejectsZeroAuthor()
    {
        var (a, _) = BuildHistory();
        Assert.NotEmpty(a.Inbox);
        var key = a.Inbox.First();

        // Removing a receipt removes its key and author together; there is no
        // second set whose key could remain orphaned.
        a.InboxAuthors.Remove(key);
        var missingPath = Path.Combine(_dir, "missing-inbox-receipt.state");
        NodePersistence.Save(a, missingPath);
        using (var restored = NodePersistence.Load(missingPath))
            Assert.DoesNotContain(key, restored.Inbox);

        a.InboxAuthors[key] = default;
        var zeroPath = Path.Combine(_dir, "zero-inbox-author.state");
        NodePersistence.Save(a, zeroPath);
        Assert.Throws<InvalidDataException>(() => NodePersistence.Load(zeroPath));
    }

    [Fact]
    public void Persistence_CorruptedDigest_IsRejected()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "corrupt.state");
        NodePersistence.Save(a, path);
        var bytes = File.ReadAllBytes(path);
        bytes[^1] ^= 0xFF;   // flip one bit of the body
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes));
    }

    [Fact]
    public void Persistence_TruncatedFile_IsRejected()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "trunc.state");
        NodePersistence.Save(a, path);
        var bytes = File.ReadAllBytes(path);
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes[..(bytes.Length / 2)]));
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes[..8]));
    }

    [Fact]
    public void Persistence_RejectsCollectionCountLargerThanRemainingBodyBeforeAllocation()
    {
        using var node = Fix.MakeNode(887);
        var path = Path.Combine(_dir, "count-overflow-source.state");
        NodePersistence.Save(node, path);
        var original = File.ReadAllBytes(path);
        var body = original.AsSpan(46).ToArray();

        // The namespace-count varint follows NodeId, Epoch, HLC wall and logical.
        const int namespaceCountOffset = 16 + 1 + 8 + 4;
        var hugeCount = Codec.EncodeVarint(ulong.MaxValue);
        var malformedBody = new byte[body.Length - 1 + hugeCount.Length];
        body.AsSpan(0, namespaceCountOffset).CopyTo(malformedBody);
        hugeCount.CopyTo(malformedBody.AsSpan(namespaceCountOffset));
        body.AsSpan(namespaceCountOffset + 1).CopyTo(
            malformedBody.AsSpan(namespaceCountOffset + hugeCount.Length));

        var malformed = new byte[46 + malformedBody.Length];
        original.AsSpan(0, 14).CopyTo(malformed);
        BinaryPrimitives.WriteUInt64BigEndian(malformed.AsSpan(6, 8), (ulong)malformedBody.Length);
        var digest = Blake3Core.TaggedHash(NodePersistence.StateTag, malformedBody);
        digest.CopyTo(malformed.AsSpan(14, 32));
        malformedBody.CopyTo(malformed.AsSpan(46));

        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(malformed));
    }

    [Fact]
    public void Persistence_RejectsOverflowingIntegerAndNamespaceVarints()
    {
        static byte[] ReplaceBodyVarint(byte[] image, int bodyOffset, ulong value)
        {
            var body = image.AsSpan(46).ToArray();
            var encoded = Codec.EncodeVarint(value);
            var changedBody = new byte[body.Length - 1 + encoded.Length];
            body.AsSpan(0, bodyOffset).CopyTo(changedBody);
            encoded.CopyTo(changedBody.AsSpan(bodyOffset));
            body.AsSpan(bodyOffset + 1).CopyTo(changedBody.AsSpan(bodyOffset + encoded.Length));
            var changed = new byte[46 + changedBody.Length];
            image.AsSpan(0, 14).CopyTo(changed);
            BinaryPrimitives.WriteUInt64BigEndian(changed.AsSpan(6, 8), (ulong)changedBody.Length);
            var digest = Blake3Core.TaggedHash(NodePersistence.StateTag, changedBody);
            digest.CopyTo(changed.AsSpan(14, 32));
            changedBody.CopyTo(changed.AsSpan(46));
            return changed;
        }

        using var empty = Fix.MakeNode(888);
        var emptyPath = Path.Combine(_dir, "integer-overflow-source.state");
        NodePersistence.Save(empty, emptyPath);
        var emptyImage = File.ReadAllBytes(emptyPath);
        // NodeId occupies 16 bytes; the next field is the node epoch varint.
        var invalidEpoch = ReplaceBodyVarint(emptyImage, 16, (ulong)int.MaxValue + 1);
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(invalidEpoch));

        using var populated = Fix.MakeNode(889);
        populated.WriteField(7, Fix.Rid(889), 0x100, Val("namespace"));
        var populatedPath = Path.Combine(_dir, "namespace-overflow-source.state");
        NodePersistence.Save(populated, populatedPath);
        var populatedImage = File.ReadAllBytes(populatedPath);
        // NodeId + epoch + HLC + namespace-count precede the first namespace id.
        var invalidNamespace = ReplaceBodyVarint(populatedImage, 30, (ulong)ushort.MaxValue + 1);
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(invalidNamespace));
    }

    [Fact]
    public void Persistence_BadMagicAndVersion_IsRejected()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "magic.state");
        NodePersistence.Save(a, path);
        var bytes = File.ReadAllBytes(path);
        bytes[0] ^= 0x01;    // magic
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes));

        var bytes2 = File.ReadAllBytes(path);
        BinaryPrimitives.WriteUInt16BigEndian(bytes2.AsSpan(4, 2), 99);  // unsupported version
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(bytes2));
    }

    [Fact]
    public void Persistence_SaveIsAtomic_NoTempLeftovers_AndOverwriteWorks()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "atomic.state");
        NodePersistence.Save(a, path);
        NodePersistence.Save(a, path);   // overwrite via rename must succeed
        var files = Directory.GetFiles(_dir, "*", SearchOption.AllDirectories);
        Assert.Contains(path, files);
        // Only the target state file should remain (no .tmp-* leftovers).
        Assert.DoesNotContain(files, f => Path.GetFileName(f).StartsWith(".atomic.state.tmp-", StringComparison.Ordinal));
        // No leftover temp files besides the target itself.
        Assert.Empty(Directory.GetFiles(_dir, ".atomic.state.tmp-*"));
        Assert.True(File.Exists(path));
    }

    [Fact]
    public void Persistence_LoadedNode_SyncsWithItsPeer()
    {
        var (a, b) = BuildHistory();
        var path = Path.Combine(_dir, "resume.state");
        NodePersistence.Save(a, path);
        var a2 = NodePersistence.Load(path);

        // Fresh writes on b replicate into the RESTARTED a through the real pipeline.
        b.WriteField(NS, Fix.Rid(400), 0x101, Val("after-a-restart"));
        Transport.SyncRound(b, a2, NS);
        Assert.True(a2.Storage.TryGetValue(NS, out var store));
        Assert.True(store!.TryGetValue(Fix.Rid(400), out var rec));
        Assert.Equal("after-a-restart", System.Text.Encoding.UTF8.GetString(rec.State[0x101].Value));
    }

    // ============================================================ Async transport

    [Fact]
    public async Task AsyncTransport_SyncRound_Converges()
    {
        var a = Fix.MakeNode(11);
        var b = Fix.MakeNode(12);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(1), 0x101, Val("from-a"));
        b.WriteField(NS, Fix.Rid(2), 0x101, Val("from-b"));

        var res = await AsyncTransport.SyncRoundAsync(a, b, NS);
        Assert.True(res.Total >= 2);
        Assert.True(b.Storage[NS].ContainsKey(Fix.Rid(1)));
        Assert.True(a.Storage[NS].ContainsKey(Fix.Rid(2)));

        // Idempotent second round: nothing pending.
        var again = await AsyncTransport.SyncRoundAsync(a, b, NS);
        Assert.Equal(0, again.Total);
    }

    [Fact]
    public async Task AsyncTransport_DeliverBatchAsync_ReturnsNull_WhenNothingPending()
    {
        var a = Fix.MakeNode(13);
        var b = Fix.MakeNode(14);
        Transport.MakeSession(a, b);
        var r = await AsyncTransport.DeliverBatchAsync(a, b, NS);
        Assert.Null(r);
    }

    [Fact]
    public async Task AsyncTransport_HonorsCancellation()
    {
        var a = Fix.MakeNode(15);
        var b = Fix.MakeNode(16);
        Transport.MakeSession(a, b);
        for (int i = 0; i < 50; i++) a.WriteField(NS, Fix.Rid(100 + i), 0x101, Val($"v{i}"));

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => AsyncTransport.SyncRoundAsync(a, b, NS, ct: cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => AsyncTransport.DeliverBatchAsync(a, b, NS, ct: cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => AsyncTransport.SaveAsync(a, Path.Combine(_dir, "never.state"), cts.Token));
    }

    [Fact]
    public async Task AsyncTransport_SaveLoadRoundTripsConcurrentlyWithWrites()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "concurrent.state");

        // Hammer the node from another thread while persisting repeatedly.
        var writerTask = Task.Run(() =>
        {
            for (int i = 0; i < 200; i++)
                a.WriteField(NS, Fix.Rid(1000 + i), 0x101, Val($"hammer-{i}"));
        });
        for (int i = 0; i < 5; i++)
        {
            await AsyncTransport.SaveAsync(a, path);
            var snap = NodePersistence.Load(path);   // must never see a torn image
            Assert.Equal(a.NodeId, snap.NodeId);
        }
        await writerTask;
        NodePersistence.Save(a, path);
        var final = NodePersistence.Load(path);
        Assert.True(StateEqual(a, final));
    }

    // ============================================================ Thread safety

    [Fact]
    public void Node_Gate_IsReentrant_AndSerializesConcurrentWriters()
    {
        var n = Fix.MakeNode(17);
        // Re-entrancy: CreateDeltaBatch internally calls PushOutbox/NextOutboxSeq under
        // the same gate — a non-reentrant lock would deadlock here.
        n.WriteField(NS, Fix.Rid(1), 0x101, Val("x"));
        Assert.NotNull(n.CreateDeltaBatch(Fix.Nid(98), NS));

        // Concurrent writers must serialize without corruption or exceptions.
        // WriteFields takes the gate ONCE per thread so every entry gets a distinct
        // HLC stamp — [П1] monotonicity holds even under full concurrency.
        int threads = 8, perThread = 100;
        var ex = Xunit.Record.Exception(() =>
        {
            Parallel.For(0, threads, t =>
            {
                n.WriteFields(NS, Enumerable.Range(0, perThread)
                    .Select(i => (RecordId.From(Fix.Rid(t * 1000 + i)), 0x101, Val($"t{t}-{i}"))));
            });
        });
        Assert.Null(ex);
        Assert.Equal(threads * perThread, n.Storage[NS].Count);

        // Every durable op got a unique monotonically increasing seq ([П1]).
        // Smoke write above is also a durable op, so the counter is 1 + the
        // concurrent batch. Rid(1) is overwritten by thread 0, hence storage
        // count stays at threads*perThread while the seq counter does not.
        var seqMap = n.OutboxSeqRef;
        Assert.Equal(1L + threads * perThread, seqMap[(n.NodeId, NS)]);
        var seqs = n.Outbox[NS].Select(e => e.Seq).ToList();
        Assert.Equal(seqs.Count, seqs.Distinct().Count());
        Assert.True(seqs.SequenceEqual(seqs.Order()));
    }

    [Fact]
    public async Task Node_ConcurrentReadersAndSaver_NeverTear()
    {
        var (a, _) = BuildHistory();
        var path = Path.Combine(_dir, "readers.state");
        Exception? err = null;
        var stop = DateTime.UtcNow.AddSeconds(2);
        var tasks = Enumerable.Range(0, 4).Select(i => Task.Run(() =>
        {
            try
            {
                while (DateTime.UtcNow < stop)
                {
                    a.WriteField(NS, Fix.Rid(500 + i * 100 + Random.Shared.Next(100)), 0x101, Val("r"));
                    NodePersistence.Save(a, path);
                    var snap = NodePersistence.Load(path);
                    if (snap.NodeId != a.NodeId)
                        throw new InvalidOperationException("torn identity");
                }
            }
            catch (Exception e) { Interlocked.CompareExchange(ref err, e, null); }
        })).ToArray();
        await Task.WhenAll(tasks);
        Assert.Null(err);
    }
}
