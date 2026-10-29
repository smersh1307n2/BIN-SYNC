using System;
using System.IO;
using Xunit;
using BinSync;

namespace BinSync.Tests;

public class Diag2
{
    private const int NS = 7;
    private static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);
    private static string H(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();
    private static string H(NodeId n) => n.ToString().ToLowerInvariant();

    [Fact]
    public void Diag_SeqGap()
    {
        var dir = Path.Combine(Path.GetTempPath(), "diag2-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var a = Fix.MakeNode(1);
        var b = Fix.MakeNode(2);
        Transport.MakeSession(a, b);
        a.WriteField(NS, Fix.Rid(100), 0x101, Val("hello"));
        b.WriteField(NS, Fix.Rid(200), 0x101, Val("from-b"));
        Transport.SyncRound(a, b, NS);
        var path = Path.Combine(dir, "s.state");
        NodePersistence.Save(a, path);
        var a2 = NodePersistence.Load(path);
        b.WriteField(NS, Fix.Rid(400), 0x101, Val("after"));

        foreach (var (ns_, list) in b.Outbox)
            foreach (var e in list)
                Console.WriteLine($"DIAG B-OUTBOX ns={ns_} seq={e.Seq} author={H(e.AuthorNodeId)[..8]} rid={H(e.RecordId)}");
        foreach (var (k, ps) in b.PeerStates)
        {
            Console.WriteLine($"DIAG B-PS peer={H(k.Item1)[..8]} ns={k.Item2} acked={ps.LastAckedSeq} pushed={ps.LastPushedSeq}");
            foreach (var (sk, sv) in ps.StreamAcked) Console.WriteLine($"DIAG   B-StreamAcked {H(sk)[..8]} -> {sv}");
            foreach (var (rk, rv) in ps.ReceivedStreams) Console.WriteLine($"DIAG   B-Recv ({H(rk.Item1)[..8]},{rk.Item2},{rk.Item3},{H(rk.Item4)[..8]}) -> {rv}");
        }
        foreach (var (k, ps) in a2.PeerStates)
        {
            Console.WriteLine($"DIAG A2-PS peer={H(k.Item1)[..8]} ns={k.Item2} acked={ps.LastAckedSeq} received={ps.LastReceivedSeq}");
            foreach (var (rk, rv) in ps.ReceivedStreams) Console.WriteLine($"DIAG   A2-Recv ({H(rk.Item1)[..8]},{rk.Item2},{rk.Item3},{H(rk.Item4)[..8]}) -> {rv}");
        }
        try { Transport.SyncRound(b, a2, NS); Console.WriteLine("DIAG SYNC OK"); }
        catch (Exception ex) { Console.WriteLine("DIAG ERR: " + ex.Message); }
        Assert.True(true);
    }
}
