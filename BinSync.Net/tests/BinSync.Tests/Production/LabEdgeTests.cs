// §17 edge scenarios: S4 snapshot+tail, S12 fragmentation, S13 quarantine.
using System.Text;
using BinSync;
using Xunit;

namespace BinSync.Tests;

public class LabEdgeTests
{
    const int NS = 1;
    static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);

    // ---------- S4: snapshot + delta tail for new peer ----------

    [Fact]
    public void S4_Snapshot_plus_tail_brings_new_peer_current()
    {
        var donor = Fix.MakeNode(801);
        var newbie = Fix.MakeNode(802);
        Transport.MakeSession(donor, newbie);

        // History on donor
        donor.WriteField(NS, Fix.Rid(1), 0x100, Val("base"));
        donor.WriteField(NS, Fix.Rid(2), 0x100, Val("old"));

        // Snapshot captures current state
        var snap = Reconcile.BuildSnapshot(donor, NS);
        Assert.True(snap.Records.Count >= 2);
        int applied = Reconcile.ApplySnapshot(newbie, snap);
        Assert.True(applied >= 2);
        Assert.Equal("base", Encoding.UTF8.GetString(newbie.Storage[NS][Fix.Rid(1)].State[0x100].Value));

        // Tail: new write after snapshot
        donor.WriteField(NS, Fix.Rid(3), 0x100, Val("tail"));
        Transport.SyncRound(donor, newbie, NS);

        Assert.Equal("tail", Encoding.UTF8.GetString(newbie.Storage[NS][Fix.Rid(3)].State[0x100].Value));
        // Prior snapshot data still present
        Assert.Equal("base", Encoding.UTF8.GetString(newbie.Storage[NS][Fix.Rid(1)].State[0x100].Value));
    }

    [Fact]
    public void S4_Multi_chunk_snapshot_indices()
    {
        var n = Fix.MakeNode(803);
        for (int i = 0; i < 5; i++)
            n.WriteField(NS, Fix.Rid(100 + i), 0x100, Val($"r{i}"));

        var full = Reconcile.BuildSnapshot(n, NS, maxRecords: 2);
        Assert.True(full.Records.Count <= 2);

        // Simulate multi-chunk metadata
        full.ChunkIndex = 0;
        full.ChunkCount = 3;
        var bytes = Reconcile.EncodeSnapshotChunk(full);
        var back = Reconcile.DecodeSnapshotChunk(bytes);
        Assert.Equal(0u, back.ChunkIndex);
        Assert.Equal(3u, back.ChunkCount);
        Assert.Equal(full.Records.Count, back.Records.Count);
    }

    // ---------- S12: fragmentation ----------

    [Fact]
    public void S12_Fragment_split_and_reassemble_roundtrip()
    {
        var payload = Encoding.UTF8.GetBytes(new string('X', 500));
        var msgId = Fix.Nid(900);
        var parts = FragmentCodec.Split(payload, maxBody: 100, msgId: msgId);
        Assert.True(parts.Count >= 5);

        var re = new FragmentReassembler();
        byte[]? done = null;
        // Feed out of order
        for (int i = parts.Count - 1; i >= 0; i--)
        {
            done = re.Accept(parts[i]);
            if (i > 0) Assert.Null(done);
        }
        Assert.NotNull(done);
        Assert.Equal(payload, done);
        Assert.Equal(0, re.PendingMessages);
    }

    [Fact]
    public void S12_Fragment_frames_carry_FLAG_FRAGMENT()
    {
        var body = new byte[250];
        Random.Shared.NextBytes(body);
        var frames = FragmentCodec.BuildFrames(
            logicalType: 0x09, streamId: 1, baseSeq: 10, logicalPayload: body, maxBody: 80);
        Assert.True(frames.Count >= 3);
        foreach (var fr in frames)
        {
            var (parsed, _) = FrameCodec.ParseFrame(fr);
            Assert.Equal(Frames.FLAG_FRAGMENT, parsed.Flags & Frames.FLAG_FRAGMENT);
            Assert.Equal(0x09, parsed.FrameType);
        }

        var re = new FragmentReassembler();
        byte[]? logical = null;
        foreach (var fr in frames)
        {
            var (parsed, _) = FrameCodec.ParseFrame(fr);
            logical = re.Accept(parsed.Payload) ?? logical;
        }
        Assert.Equal(body, logical);
    }

    [Fact]
    public void S12_Fragment_rejects_index_out_of_range()
    {
        var bad = FragmentCodec.EncodeOne(Fix.Nid(1), index: 5, count: 3, body: new byte[] { 1 });
        var re = new FragmentReassembler();
        Assert.ThrowsAny<FrameException>(() => re.Accept(bad));
    }

    // ---------- S13: quarantine / outdated watermark ----------

    [Fact]
    public void S13_GcNotice_quarantine_propagates()
    {
        var a = Fix.MakeNode(811);
        var zombie = Fix.Nid(999);
        a.Quarantined.Add(zombie);
        a.DurableAuthors[zombie] = 2;

        var msg = GcNotice.Build(a, NS, tombstoneCutoffWall: 1_000);
        Assert.Contains(msg.Quarantined, e => Fix.Eq(e.NodeId, Codec.CanonicalB128(zombie)));

        var b = Fix.MakeNode(812);
        int added = GcNotice.Apply(b, msg);
        Assert.True(added >= 1);
        Assert.Contains(b.Quarantined, id => Fix.Eq(id, Codec.CanonicalB128(zombie)));
    }

    [Fact]
    public void S13_OutdatedWatermark_error_code_registered()
    {
        Assert.Equal(0x000B, Frames.ErrorCodes["OUTDATED_WATERMARK"]);
        // Zombie peer that is quarantined should not be treated as active author source
        var a = Fix.MakeNode(821);
        var zombie = Fix.Nid(822);
        a.Quarantined.Add(Codec.CanonicalB128(zombie));
        Assert.Contains(a.Quarantined, id => Fix.Eq(id, Codec.CanonicalB128(zombie)));
    }

    [Fact]
    public void S13_PurgeBelowCutoff_removes_stale_fields()
    {
        var n = Fix.MakeNode(831);
        var rid = Fix.Rid(50);
        // Write with known low wall via ApplyRemoteField
        n.ApplyRemoteField(NS, rid, 0x100, Val("old"), new Hlc(100, 0), n.NodeId);
        n.ApplyRemoteField(NS, rid, 0x200, Val("new"), new Hlc(5_000_000, 0), n.NodeId);

        int removed = GcNotice.PurgeBelowCutoff(n, NS, cutoffWall: 1_000);
        Assert.True(removed >= 1);
        Assert.False(n.Storage[NS][rid].State.ContainsKey(0x100));
        Assert.True(n.Storage[NS][rid].State.ContainsKey(0x200));
    }

    // ---------- S9-ish: bucket digest order independence smoke ----------

    [Fact]
    public void S9_Snapshot_record_set_independent_of_write_order()
    {
        var a = Fix.MakeNode(841);
        var b = Fix.MakeNode(842);
        var ids = new[] { 10L, 20L, 30L };
        foreach (var id in ids)
            a.WriteField(NS, Fix.Rid(id), 0x100, Val($"v{id}"));
        foreach (var id in ids.Reverse())
            b.WriteField(NS, Fix.Rid(id), 0x100, Val($"v{id}"));

        var sa = Reconcile.BuildSnapshot(a, NS);
        var sb = Reconcile.BuildSnapshot(b, NS);
        Assert.Equal(sa.Records.Count, sb.Records.Count);

        // Same record ids present
        var setA = sa.Records.Select(r => Convert.ToHexString(r.RecordId)).OrderBy(x => x).ToList();
        var setB = sb.Records.Select(r => Convert.ToHexString(r.RecordId)).OrderBy(x => x).ToList();
        Assert.Equal(setA, setB);
    }
}
