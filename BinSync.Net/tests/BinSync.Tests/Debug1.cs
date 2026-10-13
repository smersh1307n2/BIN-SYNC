using BinSync;
using Xunit;
namespace BinSync.Tests;
public class Debug1 {
  const int NS = 7;
  static byte[] Rid(long n) => Fix.Rid(n);
  static byte[] Asc(string s) => System.Text.Encoding.ASCII.GetBytes(s);
  [Fact]
  public void D() {
    var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
    Transport.MakeSession(a, b);
    for (int i = 0; i < 12; i++) a.WriteField(NS, Rid(i + 1), 0x0100, Asc($"v{i}"), new Hlc((ulong)(1000 + i), 0));
    var batch = a.CreateDeltaBatch(b.NodeId, NS)!;
    var decoded = b.DecodeBatchPayload(a.EncodeBatchPayload(batch));
    var r = b.ApplyBatch(decoded, a.NodeId);
    a.AdvanceStream(b.NodeId, NS, batch);
    a.ProcessAck(b.NodeId, NS, batch.CursorEnd, authorStream: decoded.DurableAuthor);
    b.AcksOut.Clear();
    var dup = b.ApplyBatch(b.DecodeBatchPayload(a.EncodeBatchPayload(batch)), a.NodeId);
    Assert.True(dup.Duplicate);
    Assert.Single(b.AcksOut);
    a.WriteField(NS, Rid(99), 0x0100, Asc("late"), new Hlc(2000, 0));
    var next = a.CreateDeltaBatch(b.NodeId, NS)!;
    // dump outbox seqs
    var ob = string.Join(",", a.Outbox[NS].Select(e => $"{Convert.ToHexString(e.RecordId.AsSpan(8))}:{e.Seq}"));
    var st = string.Join(",", b.Storage[NS].Keys.Select(k => Convert.ToHexString(k.AsSpan(8))));
    throw new Exception($"next={next.CursorStart}..{next.CursorEnd} entries={next.Entries.Count} outbox=[{ob}] storage=[{st}]");
  }
}
