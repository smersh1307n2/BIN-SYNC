using Xunit;

namespace BinSync.Tests;

public class Diag : IDisposable
{
    private static string MakeTempDir()
    {
        var d = Path.Combine(Path.GetTempPath(), "binsync-diag-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        return d;
    }
    private readonly string _dir = MakeTempDir();
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }
    const int NS = 7;
    static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void Diag1()
    {
        var a = Fix.MakeNode(1);
        var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(100), 0x101, Val("hello"));
        b.WriteField(NS, Fix.Rid(200), 0x101, Val("from-b"));
        Transport.SyncRound(a, b, NS);

        void Dump(Node n, string tag)
        {
            foreach (var (peer, m) in n.AliasToNode)
                foreach (var (al, tup) in m)
                    Console.WriteLine(tag + ": peer=" + peer.ToString()[..8] + " alias=" + al +
                        " -> nid=" + tup.Id.ToString()[..8] + " ep=" + tup.Epoch);
            Console.WriteLine(tag + ": NodeToAlias peers=" + n.NodeToAlias.Count + " counts: " +
                string.Join(",", n.NodeToAlias.Select(kv => kv.Key.ToString()[..8] + ":" + kv.Value.Count)));
            Console.WriteLine(tag + ": PeersByNid keys=" + string.Join(",",
                (n.PeersByNid?.Keys.Select(k => k.ToArray()) ?? Enumerable.Empty<byte[]>()).Select(k => Convert.ToHexString(k)[..8])));
        }
        Dump(a, "A-live");
        Dump(b, "B-live");

        var path = Path.Combine(_dir, "s.state");
        NodePersistence.Save(a, path);
        var a2 = NodePersistence.Load(path);
        Dump(a2, "A2-loaded");

        var fwd = a2.AliasToNode[b.NodeId];
        var revMap = a2.NodeToAlias[b.NodeId];
        foreach (var (al, tup) in fwd)
        {
            bool hit = revMap.TryGetValue((tup.Id, tup.Epoch), out var got);
            Console.WriteLine("rev-lookup alias=" + al + " nid=" + tup.Id.ToString()[..8] +
                " hit=" + hit + " got=" + got);
        }

        b.WriteField(NS, Fix.Rid(400), 0x101, Val("after-a-restart"));
        Exception? ex = null;
        try { Transport.SyncRound(b, a2, NS); } catch (Exception e) { ex = e; }
        Console.WriteLine("SyncRound(b,a2) exception: " + (ex?.Message ?? "none") + "\n" + (ex?.StackTrace ?? ""));

        // Full BuildHistory scenario to compare against the failing test
        var (ha, hb) = ProdBuild();
        var hpath = Path.Combine(_dir, "h.state");
        NodePersistence.Save(ha, hpath);
        var ha2 = NodePersistence.Load(hpath);
        Console.WriteLine("--- loaded ha2 alias maps:");
        foreach (var (peer, m) in ha2.AliasToNode)
            foreach (var (al, tup) in m)
                Console.WriteLine("ha2: peer=" + peer.ToString()[..8] + " alias=" + al +
                    " -> nid=" + tup.Id.ToString()[..8] + " ep=" + tup.Epoch);
        hb.WriteField(NS, Fix.Rid(400), 0x101, Val("after-a-restart"));
        Exception? ex2 = null;
        try { Transport.SyncRound(hb, ha2, NS); } catch (Exception e) { ex2 = e; }
        Console.WriteLine("full SyncRound(hb,ha2) exception: " + (ex2?.Message ?? "none") + "\n" + (ex2?.StackTrace ?? ""));
    }

    static (Node a, Node b) ProdBuild()
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
        a.DeleteRecord(NS, Fix.Rid(101));
        Transport.SyncRound(a, b, NS);
        return (a, b);
    }

    [Fact]
    public void Diag2_FullHistory()
    {
        var (a, b) = ProdBuild();
        var path = Path.Combine(_dir, "full.state");
        NodePersistence.Save(a, path);
        var a2 = NodePersistence.Load(path);

        Console.WriteLine("=== alias maps ===");
        foreach (var n in new[] { ("a", a), ("b", b), ("a2", a2) })
            foreach (var (peer, m) in n.Item2.AliasToNode)
                foreach (var (al, tup) in m)
                    Console.WriteLine(n.Item1 + ": peer=" + peer.ToString()[..8] + " alias=" + al +
                        " -> nid=" + tup.Id.ToString()[..8] + " ep=" + tup.Epoch);

        Console.WriteLine("=== b outbox (author, seq, acked-for-a2) ===");
        var psBA = b.PeerStates.TryGetValue((a2.NodeId, NS), out var p1) ? p1 : null;
        foreach (var (nsId, ol) in b.Outbox)
            foreach (var e in ol)
                Console.WriteLine("b.outbox ns=" + nsId + " seq=" + e.Seq + " author=" + Convert.ToHexString(e.AuthorNodeId)[..8] +
                                  " rec=" + Convert.ToHexString(e.RecordId)[..4] + " kind=" + e.OpKind +
                                  " ackedForA2=" + (psBA?.AckedFor(e.AuthorNodeId) ?? -1));

        Console.WriteLine("=== a2 outbox ===");
        foreach (var (nsId, ol) in a2.Outbox)
            foreach (var e in ol)
                Console.WriteLine("a2.outbox ns=" + nsId + " seq=" + e.Seq + " author=" + Convert.ToHexString(e.AuthorNodeId)[..8]);

        // dump origins inside b's pending payloads for target a2
        foreach (var (nsId, ol) in b.Outbox)
            foreach (var e in ol)
            {
                var op = Ops.DecodeOp(e.Payload).Op;
                ushort origin = op switch { OpFieldWrite x => x.Origin, OpFieldTomb x => x.Origin, OpRecordTombstone x => x.Origin, _ => (ushort)999 };
                Console.WriteLine("b.payload ns=" + nsId + " seq=" + e.Seq + " origin=" + origin + " author=" + Convert.ToHexString(e.AuthorNodeId)[..8]);
            }

        b.WriteField(NS, Fix.Rid(400), 0x101, Val("after-a-restart"));
        // Manual step-through to find WHICH batch/origin throws
        try
        {
            for (int i = 0; i < 20; i++)
            {
                var batch = b.CreateDeltaBatch(a2.NodeId, NS, schemaVersion: 1);
                if (batch is null) { Console.WriteLine("no more batches"); break; }
                var opsBef = batch.Ops();
                for (int j = 0; j < batch.Entries.Count; j++)
                {
                    var op = opsBef[j];
                    ushort org = op switch { OpFieldWrite x => x.Origin, OpFieldTomb x => x.Origin, OpRecordTombstone x => x.Origin, _ => (ushort)999 };
                    Console.WriteLine($"batch#{i} entry{j} origin={org} kind={op.GetType().Name} author={Convert.ToHexString(batch.Entries[j].AuthorNodeId ?? Array.Empty<byte>())[..8]}");
                }
                var decoded = a2.DecodeBatchPayload(b.EncodeBatchPayload(batch));
                try
                {
                    var res = a2.ApplyBatch(decoded, b.NodeId);
                    Console.WriteLine($"batch#{i} applied ok dup={res.Duplicate} applied={res.Applied}");
                    b.AdvanceStream(a2.NodeId, NS, batch);
                    b.ProcessAck(a2.NodeId, NS, decoded.CursorEnd, authorStream: decoded.DurableAuthor);
                }
                catch (Exception ex2)
                {
                    Console.WriteLine($"batch#{i} APPLY EX: {ex2.Message}");
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("CreateDeltaBatch EX: " + ex.Message);
        }
        // Diagnostic harness: logs above are the assertion surface.
        Assert.True(true);
    }
}
