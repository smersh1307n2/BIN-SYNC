using BinSync;
using Xunit;

namespace BinSync.Tests;

public class ChangeIdWireTests
{
    private static byte[] Id(byte value)
    {
        var bytes = new byte[16];
        bytes[15] = value;
        return bytes;
    }

    [Fact]
    public void Provider_snapshot_restore_clears_pending_remote_ledger_but_preserves_local_identity()
    {
        using var local = Fix.MakeNode(201);
        using var remote = Fix.MakeNode(202);
        const int ns = 7;
        RecordId localRecord = RecordId.From(Fix.Rid(0x71));
        RecordId remoteRecord = RecordId.From(Fix.Rid(0x72));
        byte[] localValue = { 0x10, 0x20 };
        byte[] remoteValue = { 0x30, 0x40 };

        Assert.True(local.WriteField(ns, localRecord, 0x110, localValue, new Hlc(7100, 0)));
        BatchId localChangeId = Assert.Single(local.Outbox[ns]).ChangeId;
        Assert.True(remote.WriteField(ns, remoteRecord, 0x120, remoteValue, new Hlc(7200, 0)));
        BatchId remoteChangeId = Assert.Single(remote.Outbox[ns]).ChangeId;

        Transport.MakeSession(remote, local);
        Assert.NotNull(Transport.DeliverBatch(remote, local, ns));
        Assert.True(local.GlobalChangeLedger.ContainsKey((ns, localChangeId)));
        Assert.Contains((ns, localChangeId), local.SeenChanges);
        Assert.True(local.GlobalChangeLedger.ContainsKey((ns, remoteChangeId)));
        Assert.DoesNotContain((ns, remoteChangeId), local.SeenChanges);

        int restoredRecords = local.RestoreProviderSnapshot(ns, new[]
        {
            new ProviderMutation
            {
                RecordId = localRecord,
                FieldId = 0x110,
                HlcWall = 7100,
                AuthorNodeId = local.NodeId,
                Value = localValue
            },
            new ProviderMutation
            {
                RecordId = remoteRecord,
                FieldId = 0x120,
                HlcWall = 7200,
                AuthorNodeId = remote.NodeId,
                Value = remoteValue
            }
        }, new[] { localChangeId, remoteChangeId });

        Assert.Equal(2, restoredRecords);
        Assert.True(local.GlobalChangeLedger.ContainsKey((ns, localChangeId)));
        Assert.False(local.GlobalChangeLedger.ContainsKey((ns, remoteChangeId)));
        Assert.True(local.TryGetField(ns, localRecord, 0x110, out var restoredLocal));
        Assert.Equal(localValue, restoredLocal!.Value);
        Assert.True(local.TryGetField(ns, remoteRecord, 0x120, out var restoredRemote));
        Assert.Equal(remoteValue, restoredRemote!.Value);
    }

    [Fact]
    public void Provider_snapshot_restore_rejects_duplicate_and_reserved_rows_atomically()
    {
        using var node = Fix.MakeNode(203);
        const int ns = 7;
        RecordId existingRecord = RecordId.From(Fix.Rid(0x73));
        byte[] originalValue = { 0x51, 0x52 };
        Assert.True(node.WriteField(ns, existingRecord, 0x130, originalValue, new Hlc(7300, 0)));

        var duplicateRows = new[]
        {
            new ProviderMutation
            {
                RecordId = existingRecord,
                FieldId = 0x130,
                HlcWall = 7400,
                AuthorNodeId = node.NodeId,
                Value = new byte[] { 0x61 }
            },
            new ProviderMutation
            {
                RecordId = existingRecord,
                FieldId = 0x130,
                HlcWall = 7500,
                AuthorNodeId = node.NodeId,
                Value = new byte[] { 0x62 }
            }
        };
        Assert.Throws<InvalidDataException>(() =>
            node.RestoreProviderSnapshot(ns, duplicateRows, Array.Empty<BatchId>()));
        Assert.True(node.TryGetField(ns, existingRecord, 0x130, out var preserved));
        Assert.Equal(originalValue, preserved!.Value);

        var reservedRow = new[]
        {
            new ProviderMutation
            {
                RecordId = existingRecord,
                FieldId = Model.LifecycleFieldId,
                HlcWall = 7600,
                AuthorNodeId = node.NodeId,
                Value = new byte[] { 0x01 }
            }
        };
        Assert.Throws<InvalidDataException>(() =>
            node.RestoreProviderSnapshot(ns, reservedRow, Array.Empty<BatchId>()));
        Assert.True(node.TryGetField(ns, existingRecord, 0x130, out preserved));
        Assert.Equal(originalValue, preserved!.Value);
    }

    [Fact]
    public void Batch_payload_roundtrip_preserves_change_id_and_binds_it_to_records_root()
    {
        var node = new Node(Id(1));
        var changeId = BatchId.From(Id(0xA1));
        var entry = new OutboxEntry
        {
            NamespaceId = 7,
            Seq = 1,
            ChangeId = changeId,
            RecordId = RecordId.From(Id(0x22)),
            OpKind = 0x01,
            Payload = new byte[] { 0x10, 0x20, 0x30 },
            AuthorNodeId = NodeId.From(Id(1)),
        };
        var entries = new List<OutboxEntry> { entry };
        var root = Node.ComputeRecordsRoot(entries);
        var batchId = BatchId.From(Id(0xB1));
        var issuer = NodeId.From(Id(1));
        var digest = Node.ComputeBatchDigest(batchId, 7, issuer, 1, 1, 1, root, default);
        var batch = new Batch(7, 0, 1, 0x02, 0, issuer, 1, 1, 1,
            batchId, default, root, digest, entries);

        var encoded = node.EncodeBatchPayload(batch);
        var decoded = node.DecodeBatchPayload(encoded);

        Assert.Equal(changeId, decoded.Entries[0].ChangeId);
        Assert.Equal(root, Node.ComputeRecordsRoot(decoded.Entries));
        Assert.Equal(digest, decoded.BatchDigest);

        var changedIdEntry = new OutboxEntry
        {
            NamespaceId = entry.NamespaceId,
            ChangeId = BatchId.From(Id(0xA2)),
            RecordId = entry.RecordId,
            OpKind = entry.OpKind,
            Payload = entry.Payload,
            AuthorNodeId = entry.AuthorNodeId,
        };
        var changedRoot = Node.ComputeRecordsRoot(new List<OutboxEntry> { changedIdEntry });
        Assert.NotEqual(root, changedRoot);

        var (seed, publicKey) = BatchSignature.GenerateKeyPair();
        var signature = BatchSignature.SignBatch(seed, batch);
        Assert.True(BatchSignature.VerifyBatch(publicKey, batch, signature));
        var changedDigest = Node.ComputeBatchDigest(batch.BatchId, batch.NamespaceId,
            batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
            changedRoot, batch.PrevBatchDigest);
        var changedBatch = new Batch(batch.NamespaceId, batch.SchemaId, batch.SchemaVersion,
            batch.MergePolicy, batch.Flags, batch.IssuerNodeId, batch.IssuerEpoch,
            batch.CursorStart, batch.CursorEnd, batch.BatchId, batch.PrevBatchDigest,
            changedRoot, changedDigest, new List<OutboxEntry> { changedIdEntry });
        Assert.False(BatchSignature.VerifyBatch(publicKey, changedBatch, signature));
    }

    [Fact]
    public void Records_array_roundtrip_includes_change_id_in_wire_layout()
    {
        var entry = new OutboxEntry
        {
            ChangeId = BatchId.From(Id(0xCC)),
            RecordId = RecordId.From(Id(0x11)),
            OpKind = 3,
            Payload = new byte[] { 4, 5 },
        };

        var bytes = Node.EncodeRecordsArray(new List<OutboxEntry> { entry });
        var decoded = Node.DecodeRecordsArray(bytes);

        Assert.Single(decoded);
        Assert.Equal(entry.RecordId, decoded[0].Rid);
        Assert.Equal(entry.ChangeId, decoded[0].ChangeId);
        Assert.Equal(entry.OpKind, decoded[0].Kind);
        Assert.Equal(entry.Payload.ToArray(), decoded[0].Payload);
        Assert.Throws<InvalidDataException>(() => Node.DecodeRecordsArray(bytes.Append((byte)0).ToArray()));
    }
    [Fact]
    public void Change_id_survives_A_to_B_to_C_to_A_and_duplicate_arrival_from_second_peer()
    {
        var a = Fix.MakeNode(41);
        var b = Fix.MakeNode(42);
        var c = Fix.MakeNode(43);

        // Establish all three sessions before the write so A retains its outbox
        // entry until both B and C have acknowledged it.
        Transport.MakeSession(a, b);
        Transport.MakeSession(b, c);
        Transport.MakeSession(a, c);

        Assert.True(a.WriteField(1, Fix.Rid(0x41), 0x100, new byte[] { 0xCA, 0xFE }));
        var originalChangeId = Assert.Single(a.Outbox[1]).ChangeId;
        Assert.False(originalChangeId.IsZero);

        // A -> B: first network hop.
        Assert.NotNull(Transport.DeliverBatch(a, b, 1));
        var bRelay = Assert.Single(b.Outbox[1]);
        Assert.Equal(originalChangeId, bRelay.ChangeId);

        // B -> C: second hop. The issuer changes to B, but ChangeId must not.
        Assert.NotNull(Transport.DeliverBatch(b, c, 1));
        var cRelay = Assert.Single(c.Outbox[1]);
        Assert.Equal(originalChangeId, cRelay.ChangeId);

        // A -> C: the same logical mutation reaches C through a second peer.
        // It must be applied idempotently and must not be queued for relay twice.
        Assert.NotNull(Transport.DeliverBatch(a, c, 1));
        Assert.Single(c.Outbox[1]);
        Assert.Equal(originalChangeId, c.Outbox[1][0].ChangeId);
        Assert.DoesNotContain((1, originalChangeId), c.SeenChanges);
        Assert.True(c.GlobalChangeLedger.ContainsKey((1, originalChangeId)));

        // C -> A closes the mesh loop. A already owns this ChangeId, so it must
        // not create a second outbox item or invent a replacement identity.
        Assert.NotNull(Transport.DeliverBatch(c, a, 1));
        Assert.False(a.Outbox.TryGetValue(1, out var returnedEntries) &&
                     returnedEntries.Any(e => e.ChangeId == originalChangeId));
        Assert.Contains((1, originalChangeId), a.SeenChanges);
        Assert.True(a.GlobalChangeLedger.ContainsKey((1, originalChangeId)));

        Assert.True(a.Storage[1].ContainsKey(Fix.Rid(0x41)));
        Assert.True(b.Storage[1].ContainsKey(Fix.Rid(0x41)));
        Assert.True(c.Storage[1].ContainsKey(Fix.Rid(0x41)));
    }

    [Fact]
    public void Global_ChangeId_dedup_survives_origin_outbox_compaction_and_second_route()
    {
        using var a = Fix.MakeNode(111);
        using var b = Fix.MakeNode(112);
        using var c = Fix.MakeNode(113);

        Transport.MakeSession(a, b);
        Assert.True(a.WriteField(1, Fix.Rid(0x71), 0x100, new byte[] { 0xCA, 0xFE }));
        var original = a.CreateDeltaBatch(b.NodeId, 1)!;
        a.SignBatchFor(original, b.NodeId);
        var originalPayload = a.EncodeBatchPayload(original);

        var acceptedByB = b.ApplyBatchPayload(originalPayload, a.NodeId);
        a.AdvanceStream(b.NodeId, 1, original);
        a.ProcessAck(b.NodeId, 1, acceptedByB.CursorEnd, acceptedByB.DurableAuthor);
        Assert.Equal(0, a.OutboxSize(1)); // original operation payload is now gone

        Transport.MakeSession(b, c);
        Assert.NotNull(Transport.DeliverBatch(b, c, 1));
        Assert.Single(c.Outbox[1]);
        var changeId = original.Entries[0].ChangeId;
        Assert.True(c.GlobalChangeLedger.ContainsKey((1, changeId)));

        // C echoes the operation back to its original author after A's Outbox
        // payload was compacted. A's local ledger suppresses a second relay.
        var echoedOp = Ops.DecodeOp(original.Entries[0].Payload).Op;
        switch (echoedOp)
        {
            case OpFieldWrite x: x.AuthorNodeId = a.NodeId; x.FullAuthorMode = true; break;
            case OpFieldTomb x: x.AuthorNodeId = a.NodeId; x.FullAuthorMode = true; break;
            case OpRecordTombstone x: x.AuthorNodeId = a.NodeId; x.FullAuthorMode = true; break;
        }
        var echoEntry = new OutboxEntry
        {
            NamespaceId = 1,
            Seq = 1,
            ChangeId = changeId,
            RecordId = original.Entries[0].RecordId,
            OpKind = original.Entries[0].OpKind,
            Payload = Ops.EncodeOp(echoedOp, fullAuthor: true),
            AuthorNodeId = a.NodeId
        };
        var echoBatchId = BatchId.From(Id(0xD3));
        var echoRoot = Node.ComputeRecordsRoot(new List<OutboxEntry> { echoEntry });
        var echoDigest = Node.ComputeBatchDigest(echoBatchId, 1, c.NodeId, c.Epoch,
            1, 1, echoRoot, default);
        var echoBatch = new Batch(1, 0, 1, 0x02, 0, c.NodeId, c.Epoch,
            1, 1, echoBatchId, default, echoRoot, echoDigest, new List<OutboxEntry> { echoEntry });
        var echoResult = a.ApplyBatch(echoBatch, c.NodeId);
        Assert.Equal(0, echoResult.Applied);
        Assert.Equal(0, a.OutboxSize(1));
        Assert.Contains((1, changeId), a.SeenChanges);

        // The same operation also arrives directly from A through another route.
        // Re-establish A as alias 1 so the saved compact payload remains valid.
        Transport.MakeSession(a, c);
        var replay = c.ApplyBatchPayload(originalPayload, a.NodeId);
        Assert.Equal(0, replay.Applied);
        Assert.Single(c.Outbox[1]);
        Assert.Single(c.GlobalChangeLedger);
        Assert.True(a.GlobalChangeLedger.ContainsKey((1, changeId)));
        Assert.True(b.GlobalChangeLedger.ContainsKey((1, changeId)));
    }

    [Fact]
    public void ChangeIdentity_fingerprint_is_alias_independent_but_covers_all_semantic_inputs()
    {
        var author = Fix.Nid(129);
        var recordId = Fix.Rid(130);
        var depA = BatchId.From(Id(0xA9));
        var depB = BatchId.From(Id(0xAA));
        var first = new OpFieldWrite
        {
            FieldId = 0x1234, KeyEpoch = 7, Wall = 123456, Logical = 9,
            Origin = 1, Value = new byte[] { 1, 2, 3 }
        };
        var second = new OpFieldWrite
        {
            FieldId = 0x1234, KeyEpoch = 7, Wall = 123456, Logical = 9,
            Origin = 37, Value = new byte[] { 1, 2, 3 }
        };

        var fingerprint = ChangeIdentity.Fingerprint(5, recordId, OpsConst.TagFieldWrite,
            first, author, new[] { depA, depB, depA });
        Assert.Equal(fingerprint, ChangeIdentity.Fingerprint(5, recordId, OpsConst.TagFieldWrite,
            second, author, new[] { depB, depA }));

        Assert.NotEqual(fingerprint, ChangeIdentity.Fingerprint(5, recordId, OpsConst.TagFieldWrite,
            second, Fix.Nid(131), new[] { depA, depB }));
        var changedKeyEpoch = new OpFieldWrite
        {
            FieldId = 0x1234, KeyEpoch = 8, Wall = 123456, Logical = 9,
            Origin = 37, Value = new byte[] { 1, 2, 3 }
        };
        Assert.NotEqual(fingerprint, ChangeIdentity.Fingerprint(5, recordId, OpsConst.TagFieldWrite,
            changedKeyEpoch, author, new[] { depA, depB }));
        var changedValue = new OpFieldWrite
        {
            FieldId = 0x1234, KeyEpoch = 7, Wall = 123456, Logical = 9,
            Origin = 37, Value = new byte[] { 1, 2, 4 }
        };
        Assert.NotEqual(fingerprint, ChangeIdentity.Fingerprint(5, recordId, OpsConst.TagFieldWrite,
            changedValue, author, new[] { depA, depB }));
        Assert.NotEqual(fingerprint, ChangeIdentity.Fingerprint(6, recordId, OpsConst.TagFieldWrite,
            second, author, new[] { depA, depB }));
        Assert.NotEqual(fingerprint, ChangeIdentity.Fingerprint(5, Fix.Rid(132), OpsConst.TagFieldWrite,
            second, author, new[] { depA, depB }));
        Assert.NotEqual(fingerprint, ChangeIdentity.Fingerprint(5, recordId, OpsConst.TagFieldWrite,
            second, author, new[] { depA }));

        var fieldTomb = new OpFieldTomb
        {
            FieldId = 0x1234, KeyEpoch = 7, Wall = 123456, Logical = 9, Origin = 1
        };
        var fieldTombFingerprint = ChangeIdentity.Fingerprint(5, recordId,
            OpsConst.TagFieldTomb, fieldTomb, author);
        Assert.Equal(fieldTombFingerprint, ChangeIdentity.Fingerprint(5, recordId,
            OpsConst.TagFieldTomb, new OpFieldTomb
            {
                FieldId = 0x1234, KeyEpoch = 7, Wall = 123456, Logical = 9, Origin = 99
            }, author));
        Assert.NotEqual(fieldTombFingerprint, ChangeIdentity.Fingerprint(5, recordId,
            OpsConst.TagFieldTomb, new OpFieldTomb
            {
                FieldId = 0x1234, KeyEpoch = 8, Wall = 123456, Logical = 9, Origin = 99
            }, author));

        var recordTomb = new OpRecordTombstone
        {
            RecordId = recordId, Wall = 123456, Logical = 9, Origin = 1
        };
        var recordTombFingerprint = ChangeIdentity.Fingerprint(5, recordId,
            OpsConst.TagRecordTomb, recordTomb, author);
        Assert.NotEqual(recordTombFingerprint, ChangeIdentity.Fingerprint(5, recordId,
            OpsConst.TagRecordTomb, new OpRecordTombstone
            {
                RecordId = Fix.Rid(133), Wall = 123456, Logical = 9, Origin = 99
            }, author));
    }

    [Fact]
    public void ChangeIdentity_direct_encoding_matches_canonical_reference_for_all_operation_kinds()
    {
        static Digest16 Reference(int ns, RecordId recordId, byte opKind, object operation,
            NodeId author, IReadOnlyList<BatchId>? dependencies)
        {
            object canonical = operation switch
            {
                OpFieldWrite x => new OpFieldWrite
                {
                    FieldId = x.FieldId, KeyEpoch = x.KeyEpoch, Wall = x.Wall,
                    Logical = x.Logical, AuthorNodeId = author, FullAuthorMode = true,
                    Value = x.Value.ToArray()
                },
                OpFieldTomb x => new OpFieldTomb
                {
                    FieldId = x.FieldId, KeyEpoch = x.KeyEpoch, Wall = x.Wall,
                    Logical = x.Logical, AuthorNodeId = author, FullAuthorMode = true
                },
                OpRecordTombstone x => new OpRecordTombstone
                {
                    RecordId = x.RecordId, Wall = x.Wall, Logical = x.Logical,
                    AuthorNodeId = author, FullAuthorMode = true
                },
                _ => throw new InvalidDataException()
            };
            var encoded = Ops.EncodeOp(canonical, fullAuthor: true);
            var writer = new Writer();
            writer.Raw(System.Text.Encoding.ASCII.GetBytes("BIN-SYNC/CHANGE-ID/v1"))
                .U16((ushort)ns).B128(recordId).U8(opKind)
                .Varint((ulong)encoded.Length).Raw(encoded);
            if (dependencies is null || dependencies.Count == 0)
                writer.Varint(0);
            else
            {
                var unique = dependencies.Distinct().OrderBy(id => id.Lo).ThenBy(id => id.Hi).ToArray();
                writer.Varint((ulong)unique.Length);
                foreach (var dependency in unique) writer.B128(dependency);
            }
            return Digests.Blake3Digest16(writer.WrittenSpan);
        }

        var author = Fix.Nid(201);
        var record = Fix.Rid(202);
        var dependencies = new[] { BatchId.From(Id(0xA1)), BatchId.From(Id(0xA2)), BatchId.From(Id(0xA1)) };
        object[] operations =
        [
            new OpFieldWrite { FieldId = 0x1234, KeyEpoch = 7, Wall = 123456, Logical = 9, Origin = 5, Value = [1, 2, 3, 4] },
            new OpFieldTomb { FieldId = 0x2345, KeyEpoch = 8, Wall = 123457, Logical = 10, Origin = 6 },
            new OpRecordTombstone { RecordId = record, Wall = 123458, Logical = 11, Origin = 7 }
        ];
        byte[] kinds = [OpsConst.TagFieldWrite, OpsConst.TagFieldTomb, OpsConst.TagRecordTomb];

        for (int i = 0; i < operations.Length; i++)
        {
            var deps = i == 0 ? dependencies : i == 1 ? Array.Empty<BatchId>() : new[] { dependencies[0] };
            var expected = Reference(17, record, kinds[i], operations[i], author, deps);
            var actual = ChangeIdentity.Fingerprint(17, record, kinds[i], operations[i], author, deps);
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void Global_ChangeId_reuse_with_different_operation_is_rejected_atomically()
    {
        using var a = Fix.MakeNode(114);
        using var b = Fix.MakeNode(115);
        using var c = Fix.MakeNode(116);

        Transport.MakeSession(a, b);
        Assert.True(a.WriteField(1, Fix.Rid(0x72), 0x100, new byte[] { 0x11 }));
        var original = a.CreateDeltaBatch(b.NodeId, 1)!;
        a.SignBatchFor(original, b.NodeId);
        var payload = a.EncodeBatchPayload(original);
        var accepted = b.ApplyBatchPayload(payload, a.NodeId);
        a.AdvanceStream(b.NodeId, 1, original);
        a.ProcessAck(b.NodeId, 1, accepted.CursorEnd, accepted.DurableAuthor);

        Transport.MakeSession(b, c);
        Assert.NotNull(Transport.DeliverBatch(b, c, 1));
        var recordId = original.Entries[0].RecordId;
        Assert.True(c.TryGetField(1, recordId, 0x100, out var before));
        Assert.Equal(new byte[] { 0x11 }, before!.Value);
        int outboxBefore = c.OutboxSize(1);
        int inboxBefore = c.Inbox.Count;

        Transport.MakeSession(a, c);
        var conflict = new Batch(original.NamespaceId, original.SchemaId, original.SchemaVersion,
            original.MergePolicy, original.Flags, original.IssuerNodeId, original.IssuerEpoch,
            original.CursorStart, original.CursorEnd, original.BatchId, original.PrevBatchDigest,
            original.RecordsRoot, original.BatchDigest,
            original.Entries.Select(e => new OutboxEntry
            {
                NamespaceId = e.NamespaceId,
                Seq = e.Seq,
                ChangeId = e.ChangeId,
                RecordId = e.RecordId,
                OpKind = e.OpKind,
                Payload = e.Payload.ToArray(),
                AuthorNodeId = e.AuthorNodeId,
                CreatedAt = e.CreatedAt
            }).ToList());
        var changedOp = Ops.DecodeOp(conflict.Entries[0].Payload).Op;
        ((OpFieldWrite)changedOp).Value = new byte[] { 0x22 };
        conflict.Entries[0].Payload = Ops.EncodeOp(changedOp);
        Rehash(conflict);

        var error = Assert.Throws<SyncException>(() => c.ApplyBatch(conflict, a.NodeId));
        Assert.Equal("BATCH_TRANSACTION_FAILED", error.CodeName);
        Assert.Equal(inboxBefore, c.Inbox.Count);
        Assert.Equal(outboxBefore, c.OutboxSize(1));
        Assert.True(c.TryGetField(1, recordId, 0x100, out var after));
        Assert.Equal(new byte[] { 0x11 }, after!.Value);
    }

    [Fact]
    public void Global_ChangeId_ledger_survives_node_persistence_including_unknown_legacy_marker()
    {
        using var node = Fix.MakeNode(117);
        Assert.True(node.WriteField(1, Fix.Rid(0x73), 0x100, new byte[] { 0x33 }));
        var known = Assert.Single(node.GlobalChangeLedger);
        var legacyId = BatchId.From(Id(0xF1));
        node.GlobalChangeLedger[(9, legacyId)] = new ChangeLedgerValue(default, isKnown: false);

        var path = Path.Combine(Path.GetTempPath(), $"binsync-ledger-{Guid.NewGuid():N}.bin");
        try
        {
            NodePersistence.Save(node, path);
            using var restored = NodePersistence.Load(path);

            Assert.Equal(known.Value.Fingerprint, restored.GlobalChangeLedger[known.Key].Fingerprint);
            Assert.True(restored.GlobalChangeLedger[known.Key].IsKnown);
            Assert.False(restored.GlobalChangeLedger[(9, legacyId)].IsKnown);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void InboxReceiptStore_promotes_multi_receipt_streams_and_demotes_after_removal()
    {
        using var node = Fix.MakeNode(127);
        var issuer = Fix.Nid(128);
        var author = Fix.Nid(129);
        var fingerprint = Digest16.From(Id(0xA8));
        var batch1 = BatchId.From(Id(0xA9));
        var batch2 = BatchId.From(Id(0xAA));
        var key1 = (issuer, 7, 3, batch1);
        var key2 = (issuer, 7, 3, batch2);
        var receipt1 = new InboxReceipt(author, 10, true, fingerprint, true, 100, 0);
        var receipt2 = new InboxReceipt(author, 11, true, fingerprint, true, 101, 0);

        Assert.True(node.InboxAuthors.TryAdd(key1, receipt1));
        Assert.Single(node.InboxAuthors.Keys);
        Assert.Equal(1, node.InboxAuthors.SingletonStreamCount);
        Assert.Equal(0, node.InboxAuthors.GroupedStreamCount);
        Assert.True(node.InboxAuthors.TryAdd(key2, receipt2));
        Assert.Equal(2, node.InboxAuthors.Count);
        Assert.Equal(0, node.InboxAuthors.SingletonStreamCount);
        Assert.Equal(1, node.InboxAuthors.GroupedStreamCount);
        Assert.Equal(receipt1.CursorEnd, node.InboxAuthors[key1].CursorEnd);
        Assert.Equal(receipt2.CursorEnd, node.InboxAuthors[key2].CursorEnd);

        Assert.True(node.InboxAuthors.Remove(key2));
        Assert.Single(node.InboxAuthors.Keys);
        Assert.Equal(1, node.InboxAuthors.SingletonStreamCount);
        Assert.Equal(0, node.InboxAuthors.GroupedStreamCount);
        Assert.True(node.InboxAuthors.TryGetValue(key1, out var remaining));
        Assert.Equal(receipt1.CursorEnd, remaining.CursorEnd);
        Assert.False(node.InboxAuthors.ContainsKey(key2));

        Assert.True(node.InboxAuthors.Remove(key1));
        Assert.Empty(node.InboxAuthors.Keys);
        Assert.Equal(0, node.InboxAuthors.Count);
        Assert.Equal(0, node.InboxAuthors.SingletonStreamCount);
        Assert.Equal(0, node.InboxAuthors.GroupedStreamCount);
    }

    [Fact]
    public void Persistence_v16_groups_receipts_by_author_and_roundtrips_all_entries()
    {
        using var node = Fix.MakeNode(123);
        var issuer = Fix.Nid(124);
        var author = Fix.Nid(125);
        var author2 = Fix.Nid(126);
        var fingerprint = Digest16.From(Id(0xA5));
        var batch1 = BatchId.From(Id(0xA1));
        var batch2 = BatchId.From(Id(0xA2));
        node.InboxAuthors[(issuer, 7, 3, batch1)] = new InboxReceipt(author, 11, true, fingerprint, true, 100, 0);
        node.InboxAuthors[(issuer, 7, 3, batch2)] = new InboxReceipt(author, 12, true, fingerprint, true, 101, 0);
        var batchOtherAuthor = BatchId.From(Id(0xA6));
        node.InboxAuthors[(issuer, 7, 3, batchOtherAuthor)] = new InboxReceipt(author2, 1, true, fingerprint, true, 101, 0);
        var batchDifferentAuthor = BatchId.From(Id(0xA3));
        node.InboxAuthors[(issuer, 8, 3, batchDifferentAuthor)] = new InboxReceipt(author2, 1, true, fingerprint, true, 102, 0);
        var change1 = BatchId.From(Id(0xB1));
        var change2 = BatchId.From(Id(0xB2));
        var change3 = BatchId.From(Id(0xB3));
        node.GlobalChangeLedger[(7, change1)] = new ChangeLedgerValue(fingerprint, true);
        node.GlobalChangeLedger[(7, change2)] = new ChangeLedgerValue(Digest16.From(Id(0xA7)), true);
        node.GlobalChangeLedger[(8, change3)] = new ChangeLedgerValue(fingerprint, true);

        var path = Path.Combine(Path.GetTempPath(), $"binsync-v16-{Guid.NewGuid():N}.bin");
        try
        {
            NodePersistence.Save(node, path);
            var bytes = File.ReadAllBytes(path);
            Assert.Equal((ushort)16, System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(4, 2)));
            using var restored = NodePersistence.Load(path);
            Assert.Equal(4, restored.InboxAuthors.Count);
            Assert.Equal(11, restored.InboxAuthors[(issuer, 7, 3, batch1)].CursorEnd);
            Assert.Equal(12, restored.InboxAuthors[(issuer, 7, 3, batch2)].CursorEnd);
            Assert.Equal(author2, restored.InboxAuthors[(issuer, 7, 3, batchOtherAuthor)].DurableAuthor);
            Assert.Equal(author2, restored.InboxAuthors[(issuer, 8, 3, batchDifferentAuthor)].DurableAuthor);
            Assert.Equal(fingerprint, restored.InboxAuthors[(issuer, 7, 3, batch1)].Fingerprint);
            Assert.All(restored.InboxAuthors.Values, receipt => Assert.True(receipt.HasFingerprint && receipt.HasCursorEnd));
            Assert.Equal(3, restored.GlobalChangeLedger.Count);
            Assert.Equal(fingerprint, restored.GlobalChangeLedger[(7, change1)].Fingerprint);
            Assert.True(restored.GlobalChangeLedger[(7, change2)].IsKnown);
            Assert.True(restored.GlobalChangeLedger[(8, change3)].IsKnown);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Persistence_v16_outbox_preserves_origin_and_record_tombstone_id()
    {
        using var node = Fix.MakeNode(127);
        const int ns = 7;
        var localAuthor = node.NodeId;
        var relayAuthor = Fix.Nid(128);
        var fieldRecord = Fix.Rid(129);
        var tombRecord = Fix.Rid(130);
        var fieldOp = new OpFieldWrite { FieldId = 0x0101, KeyEpoch = 0, Wall = 1000, Logical = 1, Origin = 1, Value = System.Text.Encoding.UTF8.GetBytes("compact-outbox") };
        var tombOp = new OpRecordTombstone { RecordId = tombRecord, Wall = 1001, Logical = 0, Origin = 1 };
        node.Outbox[ns] = new List<OutboxEntry>
        {
            new() { NamespaceId = ns, Seq = 1, ChangeId = BatchId.From(Id(0xC1)), OriginAuthorEpoch = checked((uint)node.Epoch), OriginSequence = 1, RecordId = fieldRecord, OpKind = OpsConst.TagFieldWrite, Payload = Ops.EncodeOp(fieldOp, fullAuthor: false), AuthorNodeId = localAuthor, CreatedAt = 1_700_000_000_000_000 },
            new() { NamespaceId = ns, Seq = 4, ChangeId = BatchId.From(Id(0xC2)), OriginAuthorEpoch = 1, OriginSequence = 1, RecordId = tombRecord, OpKind = OpsConst.TagRecordTomb, Payload = Ops.EncodeOp(tombOp, fullAuthor: false), AuthorNodeId = relayAuthor, CreatedAt = 1_700_000_000_000_001 },
            new() { NamespaceId = ns, Seq = 2, ChangeId = BatchId.From(Id(0xC3)), OriginAuthorEpoch = checked((uint)node.Epoch), OriginSequence = 2, RecordId = Fix.Rid(131), OpKind = OpsConst.TagFieldTomb, Payload = Ops.EncodeOp(new OpFieldTomb { FieldId = 0x0102, KeyEpoch = 0, Wall = 1002, Logical = 0, Origin = 1 }, fullAuthor: false), AuthorNodeId = localAuthor, CreatedAt = 1_700_000_000_000_002 }
        };
        var path = Path.Combine(Path.GetTempPath(), $"binsync-v16-outbox-{Guid.NewGuid():N}.bin");
        try
        {
            NodePersistence.Save(node, path);
            using var restored = NodePersistence.Load(path);
            var entries = restored.Outbox[ns];
            Assert.Equal(3, entries.Count);
            Assert.Equal(localAuthor, entries[0].AuthorNodeId);
            Assert.Equal(relayAuthor, entries[1].AuthorNodeId);
            Assert.Equal(localAuthor, entries[2].AuthorNodeId);
            Assert.Equal(1, entries[0].Seq);
            Assert.Equal(4, entries[1].Seq);
            Assert.Equal(2, entries[2].Seq);
            Assert.Equal((uint)node.Epoch, entries[0].OriginAuthorEpoch);
            Assert.Equal((ulong)1, entries[0].OriginSequence);
            Assert.Equal((uint)1, entries[1].OriginAuthorEpoch);
            Assert.Equal((ulong)1, entries[1].OriginSequence);
            Assert.Equal((ulong)2, entries[2].OriginSequence);
            Assert.Equal(OpsConst.TagRecordTomb, entries[1].OpKind);
            Assert.Equal(tombRecord, entries[1].RecordId);
            Assert.Equal(fieldRecord, entries[0].RecordId);
            Assert.Equal(entries[0].Payload.ToArray(), restored.Outbox[ns][0].Payload.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void ChangeLedgerValue_reserves_zero_fingerprint_for_unknown_and_reader_rejects_known_zero()
    {
        var unknown = new ChangeLedgerValue(default, isKnown: false);
        Assert.False(unknown.IsKnown);
        Assert.True(unknown.Fingerprint.IsZero);
        var known = new ChangeLedgerValue(Digest16.From(Id(0xF1)), isKnown: true);
        Assert.True(known.IsKnown);
        Assert.Throws<ArgumentException>(() => new ChangeLedgerValue(default, isKnown: true));

        using var node = Fix.MakeNode(130);
        var changeId = BatchId.From(Id(0xF2));
        node.GlobalChangeLedger[(7, changeId)] = known;
        var path = Path.Combine(Path.GetTempPath(), $"binsync-zero-ledger-{Guid.NewGuid():N}.bin");
        try
        {
            NodePersistence.Save(node, path);
            var image = File.ReadAllBytes(path);
            var body = image.AsSpan(46).ToArray();
            // Empty state sections precede the single namespace-grouped ledger entry.
            const int fingerprintOffset = 60;
            body.AsSpan(fingerprintOffset, 16).Clear();
            var malformed = new byte[46 + body.Length];
            image.AsSpan(0, 14).CopyTo(malformed);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(
                malformed.AsSpan(6, 8), (ulong)body.Length);
            var digest = Blake3Core.TaggedHash(NodePersistence.StateTag, body);
            digest.CopyTo(malformed.AsSpan(14, 32));
            body.CopyTo(malformed.AsSpan(46));

            Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(malformed));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Legacy_v11_state_is_rejected_by_v3_clean_break()
    {
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(BuildUnsupportedStateImage(11)));
    }

    [Fact]
    public void Legacy_v10_state_is_rejected_by_v3_clean_break()
    {
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(BuildUnsupportedStateImage(10)));
    }

    [Fact]
    public void Legacy_v10_state_is_rejected_before_legacy_payload_decode()
    {
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(BuildUnsupportedStateImage(10)));
    }

    [Fact]
    public void Legacy_v9_state_is_rejected_by_v3_clean_break()
    {
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(BuildUnsupportedStateImage(9)));
    }

    [Fact]
    public void Global_ChangeId_fingerprint_and_relay_preserve_causal_dependency_set()
    {
        using var origin = Fix.MakeNode(118);
        using var relay = Fix.MakeNode(119);
        using var receiver = Fix.MakeNode(120);
        Transport.MakeSession(origin, relay);
        var walPath = Path.Combine(Path.GetTempPath(), $"binsync-causal-ledger-{Guid.NewGuid():N}.wal");
        using var wal = Wal.Open(walPath);
        relay.AttachWal(wal);
        Assert.True(origin.WriteField(1, Fix.Rid(0x74), 0x100, new byte[] { 0x44 }));

        var batch = origin.CreateDeltaBatch(relay.NodeId, 1)!;
        var dependency = BatchId.From(Id(0xE1));
        batch.Entries[0].CausalDeps.Add(dependency);
        Assert.Equal(1, relay.ApplyBatch(batch, origin.NodeId).Applied);
        Assert.Equal(new[] { dependency }, Assert.Single(relay.Outbox[1]).CausalDeps);

        Transport.MakeSession(relay, receiver);
        var forwarded = relay.CreateDeltaBatch(receiver.NodeId, 1)!;
        Assert.Equal(new[] { dependency }, Assert.Single(forwarded.Entries).CausalDeps);
        var decoded = receiver.DecodeBatchPayloadCausal(relay.EncodeBatchPayloadCausal(forwarded));
        Assert.Equal(new[] { dependency }, Assert.Single(decoded.Entries).CausalDeps);

        using var recovered = new Node(relay.NodeId, relay.Epoch);
        Assert.Equal(1, wal.Replay(recovered));
        Assert.Equal(new[] { dependency }, Assert.Single(recovered.Outbox[1]).CausalDeps);
        Assert.True(recovered.GlobalChangeLedger.ContainsKey((1, batch.Entries[0].ChangeId)));
        var receiptKey = (origin.NodeId, 1, origin.Epoch, batch.BatchId);
        Assert.Equal(relay.InboxAuthors[receiptKey].Fingerprint,
            recovered.InboxAuthors[receiptKey].Fingerprint);
        wal.Dispose();
        if (File.Exists(walPath)) File.Delete(walPath);
    }

    [Fact]
    public void Legacy_v7_state_is_rejected_by_v3_clean_break()
    {
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(BuildUnsupportedStateImage(7)));
    }

    [Fact]
    public void Duplicate_batch_id_uses_receipt_header_without_redecoding_payload()
    {
        using var sender = Fix.MakeNode(121);
        using var receiver = Fix.MakeNode(122);
        Transport.MakeSession(sender, receiver);
        Assert.True(sender.WriteField(1, Fix.Rid(0x75), 0x100, new byte[] { 0x51 }));

        var original = sender.CreateDeltaBatch(receiver.NodeId, 1)!;
        Assert.Equal(1, receiver.ApplyBatch(original, sender.NodeId).Applied);
        var acceptedValue = new byte[] { 0x51 };
        receiver.AcksOut.Clear();

        Batch CopyBatch(Batch source) => new(source.NamespaceId, source.SchemaId,
            source.SchemaVersion, source.MergePolicy, source.Flags, source.IssuerNodeId,
            source.IssuerEpoch, source.CursorStart, source.CursorEnd, source.BatchId,
            source.PrevBatchDigest, source.RecordsRoot, source.BatchDigest,
            source.Entries.Select(e => new OutboxEntry
            {
                NamespaceId = e.NamespaceId,
                Seq = e.Seq,
                ChangeId = e.ChangeId,
                RecordId = e.RecordId.ToArray(),
                OpKind = e.OpKind,
                Payload = e.Payload.ToArray(),
                AuthorNodeId = e.AuthorNodeId.ToArray(),
                CreatedAt = e.CreatedAt
            }).ToList());

        // A duplicate BatchId is acknowledged from its durable receipt. The
        // replay payload is deliberately not trusted or re-decoded.
        var payloadReplay = CopyBatch(original);
        var changedOp = Ops.DecodeOp(payloadReplay.Entries[0].Payload).Op;
        ((OpFieldWrite)changedOp).Value = new byte[] { 0x52 };
        payloadReplay.Entries[0].Payload = Ops.EncodeOp(changedOp);
        var duplicate = receiver.ApplyBatch(payloadReplay, sender.NodeId);
        Assert.True(duplicate.Duplicate);
        Assert.Equal(original.CursorEnd, duplicate.CursorEnd);
        Assert.Single(receiver.AcksOut);

        receiver.AcksOut.Clear();
        var headerConflict = CopyBatch(original);
        headerConflict.SchemaId++;
        var headerError = Assert.Throws<SyncException>(() =>
            receiver.ApplyBatch(headerConflict, sender.NodeId));
        Assert.Equal("BATCH_TRANSACTION_FAILED", headerError.CodeName);
        Assert.Empty(receiver.AcksOut);

        var cursorConflict = CopyBatch(original);
        cursorConflict.CursorEnd++;
        var cursorError = Assert.Throws<SyncException>(() =>
            receiver.ApplyBatch(cursorConflict, sender.NodeId));
        Assert.Equal("BATCH_TRANSACTION_FAILED", cursorError.CodeName);
        Assert.Empty(receiver.AcksOut);

        Assert.True(receiver.TryGetField(1, Fix.Rid(0x75), 0x100, out var current));
        Assert.Equal(acceptedValue, current!.Value);
        Assert.Equal(1, receiver.GetOutboxCount(1));
        Assert.Single(receiver.InboxAuthors);
    }

    [Fact]
    public void Legacy_v5_state_is_rejected_by_v3_clean_break()
    {
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(BuildUnsupportedStateImage(5)));
    }

    [Fact]
    public void Legacy_v6_state_is_rejected_by_v3_clean_break()
    {
        Assert.Throws<InvalidDataException>(() => NodePersistence.LoadFromBytes(BuildUnsupportedStateImage(6)));
    }

    [Fact]
    public void Snapshot_apply_rejects_zero_or_duplicate_change_ids_before_mutating_storage()
    {
        var source = Fix.MakeNode(51);
        var target = Fix.MakeNode(52);
        Assert.True(source.WriteField(1, Fix.Rid(0x51), 0x100, new byte[] { 7 }));
        Assert.True(source.WriteField(1, Fix.Rid(0x51), 0x101, new byte[] { 8 }));
        var snapshot = Assert.Single(source.BuildSnapshot(1));

        var zeroIdBatch = new Batch(snapshot.NamespaceId, snapshot.SchemaId, snapshot.SchemaVersion,
            snapshot.MergePolicy, snapshot.Flags, snapshot.IssuerNodeId, snapshot.IssuerEpoch,
            snapshot.CursorStart, snapshot.CursorEnd, snapshot.BatchId, snapshot.PrevBatchDigest,
            snapshot.RecordsRoot, snapshot.BatchDigest,
            snapshot.Entries.Select(e => new OutboxEntry
            {
                NamespaceId = e.NamespaceId, Seq = e.Seq, ChangeId = default,
                RecordId = e.RecordId, OpKind = e.OpKind, Payload = e.Payload,
                AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt
            }).ToList());

        Assert.Throws<SyncException>(() => target.ApplySnapshot(zeroIdBatch));
        Assert.Empty(target.Storage);

        var tamperedEntries = snapshot.Entries.Select((e, i) => new OutboxEntry
        {
            NamespaceId = e.NamespaceId, Seq = e.Seq,
            ChangeId = i == 0 ? BatchId.From(Id(0x77)) : e.ChangeId,
            RecordId = e.RecordId, OpKind = e.OpKind, Payload = e.Payload,
            AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt
        }).ToList();
        var tamperedBatch = new Batch(snapshot.NamespaceId, snapshot.SchemaId, snapshot.SchemaVersion,
            snapshot.MergePolicy, snapshot.Flags, snapshot.IssuerNodeId, snapshot.IssuerEpoch,
            snapshot.CursorStart, snapshot.CursorEnd, snapshot.BatchId, snapshot.PrevBatchDigest,
            snapshot.RecordsRoot, snapshot.BatchDigest, tamperedEntries);
        Assert.Throws<SyncException>(() => target.ApplySnapshot(tamperedBatch));
        Assert.Empty(target.Storage);

        var duplicateEntries = snapshot.Entries.Select(e => new OutboxEntry
        {
            NamespaceId = e.NamespaceId, Seq = e.Seq, ChangeId = snapshot.Entries[0].ChangeId,
            RecordId = e.RecordId, OpKind = e.OpKind, Payload = e.Payload,
            AuthorNodeId = e.AuthorNodeId, CreatedAt = e.CreatedAt
        }).ToList();
        var duplicateBatch = new Batch(snapshot.NamespaceId, snapshot.SchemaId, snapshot.SchemaVersion,
            snapshot.MergePolicy, snapshot.Flags, snapshot.IssuerNodeId, snapshot.IssuerEpoch,
            snapshot.CursorStart, snapshot.CursorEnd, snapshot.BatchId, snapshot.PrevBatchDigest,
            snapshot.RecordsRoot, snapshot.BatchDigest, duplicateEntries);

        Assert.Throws<SyncException>(() => target.ApplySnapshot(duplicateBatch));
        Assert.Empty(target.Storage);
    }

    private static void Rehash(Batch batch)
    {
        batch.RecordsRoot = Node.ComputeRecordsRoot(batch.Entries);
        batch.BatchDigest = Node.ComputeBatchDigest(batch.BatchId, batch.NamespaceId,
            batch.IssuerNodeId, batch.IssuerEpoch, batch.CursorStart, batch.CursorEnd,
            batch.RecordsRoot, batch.PrevBatchDigest);
    }

    [Fact]
    public void ApplySnapshot_rejects_op_kind_that_disagrees_with_payload()
    {
        using var source = Fix.MakeNode(101);
        using var target = Fix.MakeNode(102);
        Assert.True(source.WriteField(1, Fix.Rid(0x65), 0x100, new byte[] { 1 }));
        var batch = Assert.Single(source.BuildSnapshot(1));
        batch.Entries[0].OpKind = OpsConst.TagFieldTomb;
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplySnapshot(batch));
        Assert.Empty(target.Storage);
    }

    [Fact]
    public void ApplySnapshot_rejects_record_tombstone_with_different_envelope_record_id()
    {
        using var source = Fix.MakeNode(103);
        using var target = Fix.MakeNode(104);
        var recordId = Fix.Rid(0x66);
        Assert.True(source.WriteField(1, recordId, 0x100, new byte[] { 1 }));
        Assert.True(source.DeleteRecord(1, recordId));
        var batch = Assert.Single(source.BuildSnapshot(1));
        var tombstone = Assert.Single(batch.Entries, e =>
            Ops.DecodeOp(e.Payload).Op is OpRecordTombstone);
        tombstone.RecordId = RecordId.From(Id(0xFE));
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplySnapshot(batch));
        Assert.Empty(target.Storage);
    }

    [Fact]
    public void ApplyBatch_rejects_op_kind_that_disagrees_with_payload_before_mutation()
    {
        using var source = Fix.MakeNode(105);
        using var target = Fix.MakeNode(106);
        Transport.MakeSession(source, target);
        Assert.True(source.WriteField(1, Fix.Rid(0x67), 0x100, new byte[] { 1 }));
        var batch = source.CreateDeltaBatch(target.NodeId, 1)!;
        batch.Entries[0].OpKind = OpsConst.TagFieldTomb;
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplyBatch(batch, source.NodeId));
        Assert.Empty(target.Storage);
        Assert.Equal(0, target.GetRecordCount(1));
    }

    [Fact]
    public void ApplyBatch_rejects_author_stream_cursor_span_that_does_not_match_operation_count()
    {
        using var source = Fix.MakeNode(109);
        using var target = Fix.MakeNode(110);
        Transport.MakeSession(source, target);
        Assert.True(source.WriteField(1, Fix.Rid(0x69), 0x100, new byte[] { 1 }));
        var batch = source.CreateDeltaBatch(target.NodeId, 1)!;
        batch.CursorEnd++;
        Rehash(batch);

        byte[] malformedPayload = source.EncodeBatchPayload(batch);
        Assert.Throws<SyncException>(() =>
            target.DecodeBatchPayloadForMetadataAuthority(malformedPayload, source.NodeId));
        Assert.Throws<SyncException>(() =>
            target.PrepareBatchPayloadForMetadata(malformedPayload, source.NodeId));

        var error = Assert.Throws<SyncException>(() => target.ApplyBatch(batch, source.NodeId));
        Assert.Equal("BATCH_TRANSACTION_FAILED", error.CodeName);
        Assert.Empty(target.Storage);
        Assert.Equal(0, target.GetRecordCount(1));
    }

    [Fact]
    public void ApplyBatch_rejects_record_tombstone_with_different_envelope_record_id_before_mutation()
    {
        using var source = Fix.MakeNode(107);
        using var target = Fix.MakeNode(108);
        Transport.MakeSession(source, target);
        var recordId = Fix.Rid(0x68);
        Assert.True(source.WriteField(1, recordId, 0x100, new byte[] { 1 }));
        Assert.True(source.DeleteRecord(1, recordId));
        var batch = source.CreateDeltaBatch(target.NodeId, 1)!;
        var tombstone = Assert.Single(batch.Entries, e =>
            Ops.DecodeOp(e.Payload).Op is OpRecordTombstone);
        tombstone.RecordId = RecordId.From(Id(0xFD));
        Rehash(batch);

        Assert.Throws<SyncException>(() => target.ApplyBatch(batch, source.NodeId));
        Assert.Empty(target.Storage);
        Assert.Equal(0, target.GetRecordCount(1));
    }

    [Fact]
    public void Ack_cannot_advance_beyond_sent_stream_or_accept_another_author()
    {
        var a = Fix.MakeNode(61);
        var b = Fix.MakeNode(62);
        Transport.MakeSession(a, b);
        Assert.True(a.WriteField(1, Fix.Rid(0x61), 0x100, new byte[] { 1 }));

        var batch = a.CreateDeltaBatch(b.NodeId, 1)!;
        a.SignBatchFor(batch, b.NodeId);
        a.AdvanceStream(b.NodeId, 1, batch);

        Assert.Throws<SyncException>(() =>
            a.ProcessAck(b.NodeId, 1, batch.CursorEnd + 100, batch.DurableAuthor));
        Assert.Throws<SyncException>(() =>
            a.ProcessAck(b.NodeId, 1, batch.CursorEnd, NodeId.From(Id(0xEE))));
        Assert.Equal(1, a.OutboxSize(1));

        a.ProcessAck(b.NodeId, 1, batch.CursorEnd, batch.DurableAuthor);
        Assert.Equal(0, a.OutboxSize(1));
    }

    [Fact]
    public void Ack_watermark_and_pushed_stream_survive_restart_and_duplicate_ack_is_idempotent()
    {
        var a = Fix.MakeNode(63);
        var b = Fix.MakeNode(64);
        Transport.MakeSession(a, b);
        Assert.True(a.WriteField(1, Fix.Rid(0x63), 0x100, new byte[] { 2 }));

        var batch = a.CreateDeltaBatch(b.NodeId, 1)!;
        a.SignBatchFor(batch, b.NodeId);
        a.AdvanceStream(b.NodeId, 1, batch);
        var peerState = a.PeerStates[(b.NodeId, 1)];
        Assert.Single(peerState.Ids);
        var streamCheckpointKey = ((a.NodeId, 1, batch.IssuerEpoch), b.NodeId);
        var sentDigestCheckpoint = a.LastStreamDigest[streamCheckpointKey];

        a.ProcessAck(b.NodeId, 1, batch.CursorEnd, batch.DurableAuthor);

        // The ACK is contiguous and covers the cached range: release its replay
        // mapping, but retain the independent digest-chain checkpoint.
        Assert.Empty(peerState.Ids);
        Assert.Equal(sentDigestCheckpoint, a.LastStreamDigest[streamCheckpointKey]);

        var path = Path.Combine(Path.GetTempPath(), $"binsync-ack-{Guid.NewGuid():N}.bin");
        try
        {
            NodePersistence.Save(a, path);
            var restored = NodePersistence.Load(path);
            Assert.True(restored.PeerStates[(b.NodeId, 1)].TryGetStreamPushed(batch.DurableAuthor, out var pushed));
            Assert.Equal(batch.CursorEnd, pushed);
            Assert.Equal(batch.CursorEnd, restored.PeerStates[(b.NodeId, 1)].AckedFor(batch.DurableAuthor));
            Assert.Empty(restored.PeerStates[(b.NodeId, 1)].Ids);
            restored.ProcessAck(b.NodeId, 1, batch.CursorEnd, batch.DurableAuthor);
            Assert.Empty(restored.PeerStates[(b.NodeId, 1)].Ids);
            Assert.Equal(0, restored.OutboxSize(1));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public void Duplicate_batch_ack_uses_original_author_after_another_author_stream_arrives()
    {
        var author = Fix.MakeNode(65);
        var relay = Fix.MakeNode(66);
        var receiver = Fix.MakeNode(67);
        Transport.MakeSession(author, relay);
        Assert.True(author.WriteField(1, Fix.Rid(0x65), 0x100, new byte[] { 1 }));
        Transport.DeliverBatch(author, relay, 1);
        Transport.MakeSession(relay, receiver);

        var firstBatch = relay.CreateDeltaBatch(receiver.NodeId, 1,
            filterAuthorNodeId: author.NodeId.ToArray())!;
        relay.SignBatchFor(firstBatch, receiver.NodeId);
        var firstPayload = relay.EncodeBatchPayload(firstBatch);
        var firstAccepted = receiver.ApplyBatchPayload(firstPayload, relay.NodeId);
        relay.AdvanceStream(receiver.NodeId, 1, firstBatch);
        Assert.Equal(author.NodeId, firstAccepted.DurableAuthor);

        Assert.True(relay.WriteField(1, Fix.Rid(0x66), 0x100, new byte[] { 2 }));
        var secondBatch = relay.CreateDeltaBatch(receiver.NodeId, 1,
            filterAuthorNodeId: relay.NodeId.ToArray())!;
        relay.SignBatchFor(secondBatch, receiver.NodeId);
        var secondAccepted = receiver.ApplyBatchPayload(relay.EncodeBatchPayload(secondBatch), relay.NodeId);
        relay.AdvanceStream(receiver.NodeId, 1, secondBatch);
        relay.ProcessAck(receiver.NodeId, 1, secondAccepted.CursorEnd, secondAccepted.DurableAuthor);
        Assert.Equal(relay.NodeId, receiver.LastStreamAuthor[(relay.NodeId, 1)]);

        var replay = receiver.ApplyBatchPayload(firstPayload, relay.NodeId);
        Assert.True(replay.Duplicate);
        Assert.Equal(author.NodeId, replay.DurableAuthor);
        relay.ProcessAck(receiver.NodeId, 1, replay.CursorEnd, replay.DurableAuthor);
        Assert.Equal(firstBatch.CursorEnd, relay.PeerStates[(receiver.NodeId, 1)].AckedFor(author.NodeId));
    }


    private static byte[] BuildUnsupportedStateImage(ushort version)
    {
        var body = Array.Empty<byte>();
        var image = new byte[46];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(image.AsSpan(0, 4), 0x42534E53);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(image.AsSpan(4, 2), version);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(image.AsSpan(6, 8), 0);
        Blake3Core.TaggedHash($"BIN-SYNC/NODESTATE/v{version}", body).CopyTo(image.AsSpan(14, 32));
        return image;
    }
}
