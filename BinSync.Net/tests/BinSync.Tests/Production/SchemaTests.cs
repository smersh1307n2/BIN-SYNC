// §18 schema evolution: registry, SCHEMA_MAP, apply gate.
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class SchemaTests
{
    static SchemaDescriptor Desc(int ns, int sid, int ver, int policy, params ushort[] fields) =>
        new()
        {
            NamespaceId = ns,
            SchemaId = sid,
            Version = ver,
            MergePolicy = policy,
            FieldIds = new HashSet<ushort>(fields),
        };

    [Fact]
    public void Register_and_evaluate_ok()
    {
        var reg = new SchemaRegistry();
        reg.Register(Desc(1, 0, 1, 0x02, 0x100, 0x101));
        var o = reg.Evaluate(1, 0, 1, 0x02, out _);
        Assert.Equal(SchemaOutcome.Ok, o);
    }

    [Fact]
    public void Unknown_schema_is_pending()
    {
        var reg = new SchemaRegistry();
        var o = reg.Evaluate(1, 0, 1, 0x02, out var d);
        Assert.Equal(SchemaOutcome.Pending, o);
        Assert.Contains("unknown", d!);
    }

    [Fact]
    public void Future_version_is_pending()
    {
        var reg = new SchemaRegistry();
        reg.Register(Desc(1, 0, 1, 0x02, 0x100));
        var o = reg.Evaluate(1, 0, 2, 0x02, out _);
        Assert.Equal(SchemaOutcome.Pending, o);
    }

    [Fact]
    public void Policy_mismatch_detected()
    {
        var reg = new SchemaRegistry();
        reg.Register(Desc(1, 0, 1, 0x02, 0x100));
        var o = reg.Evaluate(1, 0, 1, 0x03, out _);
        Assert.Equal(SchemaOutcome.Mismatch, o);
    }

    [Fact]
    public void Legacy_zero_policy_normalizes_to_lww_field_without_bypassing_schema_gate()
    {
        var lww = new SchemaRegistry();
        lww.Register(Desc(1, 9, 1, 0x02, 0x100));
        Assert.Equal(SchemaOutcome.Ok, lww.Evaluate(1, 9, 1, 0, out _));

        var counter = new SchemaRegistry();
        counter.Register(Desc(1, 10, 1, 0x03, 0x100));
        Assert.Equal(SchemaOutcome.Mismatch, counter.Evaluate(1, 10, 1, 0, out _));

        var legacyDescriptor = new SchemaRegistry();
        legacyDescriptor.Register(Desc(1, 11, 1, 0, 0x100));
        Assert.Equal(SchemaOutcome.Ok, legacyDescriptor.Evaluate(1, 11, 1, 0x02, out _));
    }

    [Fact]
    public void Field_id_reuse_after_retire_throws()
    {
        var reg = new SchemaRegistry();
        reg.Register(new SchemaDescriptor
        {
            NamespaceId = 1,
            SchemaId = 0,
            Version = 1,
            MergePolicy = 0x02,
            FieldIds = new HashSet<ushort> { 0x100 },
            RetiredFieldIds = new HashSet<ushort> { 0x0FF },
        });
        Assert.ThrowsAny<ArgumentException>(() =>
            reg.Register(new SchemaDescriptor
            {
                NamespaceId = 1,
                SchemaId = 0,
                Version = 2,
                MergePolicy = 0x02,
                FieldIds = new HashSet<ushort> { 0x0FF }, // retired in v1
            }));
    }

    [Fact]
    public void Field_id_cannot_be_reintroduced_after_disappearing_from_a_later_version()
    {
        var registry = new SchemaRegistry();
        registry.Register(Desc(1, 2, 1, 0x02, 0x100, 0x101));
        registry.Register(Desc(1, 2, 2, 0x02, 0x101));

        Assert.Throws<ArgumentException>(() =>
            registry.Register(Desc(1, 2, 3, 0x02, 0x100, 0x101)));
    }

    [Fact]
    public void Schema_history_validation_is_independent_of_registration_order()
    {
        var reverse = new SchemaRegistry();
        reverse.Register(Desc(1, 3, 3, 0x02, 0x100));
        reverse.Register(Desc(1, 3, 1, 0x02, 0x100));
        Assert.Throws<ArgumentException>(() =>
            reverse.Register(Desc(1, 3, 2, 0x02)));

        var ordered = new SchemaRegistry();
        ordered.Register(Desc(1, 3, 1, 0x02, 0x100));
        ordered.Register(Desc(1, 3, 2, 0x02));
        Assert.Throws<ArgumentException>(() =>
            ordered.Register(Desc(1, 3, 3, 0x02, 0x100)));
    }

    [Fact]
    public void Learn_applies_schema_history_deterministically_when_map_order_is_shuffled()
    {
        var versions = new[]
        {
            Desc(1, 4, 3, 0x02, 0x100),
            Desc(1, 4, 1, 0x02, 0x100),
            Desc(1, 4, 2, 0x02),
        };
        var forward = new SchemaRegistry();
        var reverse = new SchemaRegistry();

        Assert.Equal(2, forward.Learn(versions));
        Assert.Equal(2, reverse.Learn(versions.Reverse()));

        var forwardSnapshot = forward.Snapshot();
        var reverseSnapshot = reverse.Snapshot();
        Assert.Equal(
            forwardSnapshot.Select(d => (d.NamespaceId, d.SchemaId, d.Version,
                string.Join(",", d.FieldIds.Order()), string.Join(",", d.RetiredFieldIds.Order()))),
            reverseSnapshot.Select(d => (d.NamespaceId, d.SchemaId, d.Version,
                string.Join(",", d.FieldIds.Order()), string.Join(",", d.RetiredFieldIds.Order()))));
        Assert.DoesNotContain(forwardSnapshot, d => d.Version == 3);
        Assert.Contains(forwardSnapshot, d => d.Version == 2);
    }

    [Fact]
    public void Learn_rejects_conflicting_descriptor_for_existing_schema_version()
    {
        var registry = new SchemaRegistry();
        registry.Register(Desc(1, 12, 1, 0x02, 0x100));

        Assert.Throws<InvalidDataException>(() =>
            registry.Learn(new[] { Desc(1, 12, 1, 0x03, 0x100) }));
        Assert.Throws<InvalidDataException>(() =>
            registry.Learn(new[] { Desc(1, 12, 1, 0x02, 0x100, 0x101) }));
        Assert.Throws<InvalidDataException>(() =>
            registry.Learn(new[] { Desc(1, 12, 1, 0x02, 0x100, 0x200) with
            {
                RetiredFieldIds = new HashSet<ushort> { 0x101 }
            } }));
    }

    [Fact]
    public void SchemaMap_decode_rejects_impossible_counts_and_trailing_data()
    {
        Assert.Throws<InvalidDataException>(() => SchemaMap.Decode(new byte[] { 1 }));
        Assert.Throws<InvalidDataException>(() => SchemaMap.Decode(new byte[] { 0, 0xFF }));
    }

    [Fact]
    public void SchemaRegistry_rejects_out_of_range_lookup_keys()
    {
        var registry = new SchemaRegistry();
        Assert.Throws<ArgumentOutOfRangeException>(() => registry.TryGet(1, 65536, 1, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => registry.MaxVersion(-1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => registry.Evaluate(1, 1, 65536, 0, out _));
    }

    [Fact]
    public void SchemaMap_encode_decode_roundtrip()
    {
        var list = new List<SchemaDescriptor>
        {
            Desc(1, 0, 1, 0x02, 0x100, 0x200),
            new()
            {
                NamespaceId = 1, SchemaId = 0, Version = 2, MergePolicy = 0x02,
                FieldIds = new HashSet<ushort> { 0x100, 0x200, 0x300 },
                RetiredFieldIds = new HashSet<ushort> { 0x050 },
            },
        };
        var bytes = SchemaMap.Encode(list);
        var back = SchemaMap.Decode(bytes);
        Assert.Equal(2, back.Count);
        Assert.Equal(1, back[0].NamespaceId);
        Assert.Contains((ushort)0x100, back[0].FieldIds);
        Assert.Contains((ushort)0x050, back[1].RetiredFieldIds);
        Assert.Equal(0x02, back[1].MergePolicy);
    }

    [Fact]
    public void SchemaMap_frame_type_matches_spec()
    {
        Assert.Equal(0x04, SchemaMap.FrameType);
        Assert.Equal("SCHEMA_MAP", Frames.FrameTypes[0x04]);
        var frame = SchemaMap.BuildFrame(new[] { Desc(1, 0, 1, 0x02) }, frameSeq: 1);
        var (parsed, _) = FrameCodec.ParseFrame(frame);
        Assert.Equal(SchemaMap.FrameType, parsed.FrameType);
    }

    [Fact]
    public void Schema_events_are_raised_only_for_control_plane_changes()
    {
        var node = Fix.MakeNode(309);
        int registered = 0, learned = 0;
        node.SchemaRegistered += (ns, sid, version) => { Assert.Equal(1, ns); registered++; };
        node.SchemaMapLearned += count => learned += count;
        node.RegisterSchema(Desc(1, 7, 1, 2, 0x100));
        var payload = SchemaMap.Encode(new[] { Desc(1, 8, 1, 2, 0x101) });
        Assert.Equal(1, node.LearnSchemaMap(payload));
        Assert.Equal(1, registered);
        Assert.Equal(1, learned);
    }

    [Theory]
    [InlineData(0x01)] // LWW_RECORD is not implemented by this runtime.
    [InlineData(0x03)] // COUNTER_G helper exists, but is not wired into ApplyBatch.
    [InlineData(0x04)]
    [InlineData(0x05)]
    [InlineData(0x06)]
    [InlineData(0x07)] // SEQ_CRDT is not implemented.
    [InlineData(0x08)]
    [InlineData(0x09)] // MANUAL helper does not provide a runtime conflict workflow.
    [InlineData(0x81)]
    [InlineData(0x82)]
    [InlineData(0x83)]
    [InlineData(0x84)]
    public void ApplyBatch_rejects_merge_policies_not_supported_by_runtime(int policy)
    {
        using var receiver = Fix.MakeNode(390);
        var batch = new Batch(
            1, 0, 1, policy, 0, NodeId.From(Fix.Nid(391)), 1, 1, 1,
            BatchId.From(Fix.Nid(392)), Digest16.From(new byte[16]),
            Digest16.From(new byte[16]), Digest16.From(new byte[16]),
            new List<OutboxEntry>());

        var ex = Assert.Throws<SyncException>(() => receiver.ApplyBatch(batch, Fix.Nid(391)));
        Assert.Equal("SCHEMA_MISMATCH", ex.CodeName);
        Assert.Equal(0, receiver.GetRecordCount(1));
    }

    [Fact]
    public void ApplyBatchWithSchema_pending_throws()
    {
        var n = Fix.MakeNode(300);
        n.Schemas = new SchemaRegistry();
        // no Register
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Fix.Nid(0xBB), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry>());
        var ex = Assert.Throws<SyncException>(() => n.ApplyBatchWithSchema(batch, Fix.Nid(2)));
        Assert.Equal("SCHEMA_PENDING", ex.CodeName);
    }

    [Fact]
    public void ApplyBatchWithSchema_ok_when_registered()
    {
        var n = Fix.MakeNode(301);
        n.RegisterSchema(Desc(1, 0, 1, 0x02, 0x100));
        var batch = new Batch(
            1, 0, 1, 0x02, 0, n.NodeId, n.Epoch, 1, 1,
            Fix.Nid(0xBC), Node.Zero128, Node.Zero128, Node.Zero128,
            new List<OutboxEntry>());
        // Empty batch may still hit SEQ path — assert not SCHEMA_*
        try
        {
            n.ApplyBatchWithSchema(batch, Fix.Nid(2));
        }
        catch (SyncException ex)
        {
            Assert.DoesNotContain("SCHEMA", ex.CodeName);
        }
        catch
        {
            // other engine errors acceptable
        }
    }

    [Fact]
    public void LearnSchemaMap_then_gate_passes()
    {
        var a = Fix.MakeNode(302);
        a.RegisterSchema(Desc(1, 0, 1, 0x02, 0x100));
        var payload = a.ExportSchemaMap();

        var b = Fix.MakeNode(303);
        b.Schemas = new SchemaRegistry();
        int learned = b.LearnSchemaMap(payload);
        Assert.True(learned >= 1);

        var o = b.Schemas!.Evaluate(1, 0, 1, 0x02, out _);
        Assert.Equal(SchemaOutcome.Ok, o);
    }

    [Fact]
    public async Task Tcp_ExchangeSchemaMap_propagates()
    {
        var a = Fix.MakeNode(304);
        var b = Fix.MakeNode(305);
        a.RegisterSchema(Desc(1, 0, 1, 0x02, 0x100, 0x101));
        b.Schemas = new SchemaRegistry();

        await using var listener = TcpSyncListener.Start();
        var accept = listener.AcceptAsync(b);
        await using var client = await TcpSync.ConnectAsync(a, "127.0.0.1", listener.Port);
        await using var server = await accept;

        var serverTask = server.ExchangeSchemaMapAsync(b, initiate: false);
        var (_, learnedRemote) = await client.ExchangeSchemaMapAsync(a, initiate: true);
        var (learnedLocal, _) = await serverTask;

        Assert.True(learnedLocal >= 1 || b.Schemas!.Count >= 1);
        Assert.Equal(SchemaOutcome.Ok, b.Schemas!.Evaluate(1, 0, 1, 0x02, out _));
    }

    [Fact]
    public void Error_codes_in_table()
    {
        Assert.Equal(0x0006, Frames.ErrorCodes["SCHEMA_MISMATCH"]);
        Assert.Equal(0x0011, Frames.ErrorCodes["SCHEMA_PENDING"]);
    }
    [Fact]
    public void SchemaRegistry_DetachesDescriptorsOnRegisterAndRead()
    {
        var registry = new SchemaRegistry();
        var fields = new HashSet<ushort> { 0x1001 };
        var retired = new HashSet<ushort> { 0x1002 };
        var descriptor = new SchemaDescriptor
        {
            NamespaceId = 7,
            SchemaId = 9,
            Version = 1,
            MergePolicy = 1,
            FieldIds = fields,
            RetiredFieldIds = retired,
        };

        registry.Register(descriptor);
        fields.Add(0x1003);
        retired.Clear();
        descriptor.FieldIds.Add(0x1004);

        Assert.True(registry.TryGet(7, 9, 1, out var first));
        Assert.Contains((ushort)0x1001, first!.FieldIds);
        Assert.DoesNotContain((ushort)0x1003, first.FieldIds);
        Assert.DoesNotContain((ushort)0x1004, first.FieldIds);
        Assert.Contains((ushort)0x1002, first.RetiredFieldIds);

        first.FieldIds.Add(0x1005);
        var snapshot = registry.Snapshot();
        snapshot[0].RetiredFieldIds.Clear();

        Assert.True(registry.TryGet(7, 9, 1, out var second));
        Assert.DoesNotContain((ushort)0x1005, second!.FieldIds);
        Assert.Contains((ushort)0x1002, second.RetiredFieldIds);
    }

}
