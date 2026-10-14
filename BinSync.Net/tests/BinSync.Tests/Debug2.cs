using BinSync;
using Xunit;
namespace BinSync.Tests;
public class Debug2 {
  const int NS = 7;
  static byte[] Rid(long n) => Fix.Rid(n);
  static byte[] Asc(string s) => System.Text.Encoding.ASCII.GetBytes(s);
  [Fact]
  public void D() {
    var a = Fix.MakeNode(1); var b = Fix.MakeNode(2);
    Transport.MakeSession(a, b);
    for (int i = 0; i < 12; i++)
        a.WriteField(NS, Rid(i + 1), 0x0100, Asc($"v{i}"), new Hlc((ulong)(1000 + i), 0));
    var batch = a.CreateDeltaBatch(b.NodeId, NS)!;
    var decoded = b.DecodeBatchPayload(a.EncodeBatchPayload(batch));
    var r = b.ApplyBatch(decoded, a.NodeId);
    a.AdvanceStream(b.NodeId, NS, batch);
    a.ProcessAck(b.NodeId, NS, batch.CursorEnd, authorStream: decoded.DurableAuthor);
    Assert.Null(a.CreateDeltaBatch(b.NodeId, NS));
    b.AcksOut.Clear();
    var dup = b.ApplyBatch(b.DecodeBatchPayload(a.EncodeBatchPayload(batch)), a.NodeId);
    Assert.True(dup.Duplicate);
    Assert.Equal(0, dup.Applied);
    Assert.Single(b.AcksOut);
    a.WriteField(NS, Rid(99), 0x0100, Asc("late"), new Hlc(2000, 0));
    var next = a.CreateDeltaBatch(b.NodeId, NS)!;
    var st = string.Join(",", b.Storage[NS].Keys.Select(k => Convert.ToHexString(k.AsSpan(8))));
    var keys = string.Join(",", Enumerable.Range(1,12).Select(i => Rid(i+1)).Append(Rid(99)).Select(k => Convert.ToHexString(k.AsSpan(8))));
    var missing = string.Join(",", Enumerable.Range(1,12).Select(i => Rid(i+1)).Append(Rid(99)).Where(k => !b.Storage[NS].ContainsKey(k)).Select(k => Convert.ToHexString(k)));
    throw new Exception($"storage=[{st}] expected=[{keys}] MISSING=[{missing}]");
  }
}
