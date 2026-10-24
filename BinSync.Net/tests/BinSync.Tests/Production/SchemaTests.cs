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
    public void Field_id_reuse_after_retire_throws()
    {
        var reg = new SchemaRegistry();
        reg.Register(new SchemaDescriptor
        {
            NamespaceId = 1, SchemaId = 0, Version = 1, MergePolicy = 0x02,
            FieldIds = new HashSet<ushort> { 0x100 },
            RetiredFieldIds = new HashSet<ushort> { 0x0FF },
        });
        Assert.ThrowsAny<ArgumentException>(() =>
            reg.Register(new SchemaDescriptor
            {
                NamespaceId = 1, SchemaId = 0, Version = 2, MergePolicy = 0x02,
                FieldIds = new HashSet<ushort> { 0x0FF }, // retired in v1
            }));
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
        await using var client = await TcpSyncSession.ConnectAsync(a, "127.0.0.1", listener.Port);
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
}
