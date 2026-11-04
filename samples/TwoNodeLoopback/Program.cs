// Minimal two-node loopback sample for BinSync.Core 2.6+.
// Run from repo root:
//   dotnet run --project samples/TwoNodeLoopback -c Release
using System.Text;
using BinSync;

const int Ns = 1;

static RecordId Rid(int n)
{
    var b = new byte[16];
    BitConverter.GetBytes(n).CopyTo(b, 0);
    return RecordId.From(b);
}

static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);

static bool HasField(Node node, int ns, RecordId rid, int fieldId, string expected)
{
    if (!node.TryGetField(ns, rid, fieldId, out var fs) || fs is null)
        return false;
    return Encoding.UTF8.GetString(fs.Value) == expected;
}

using var a = new NodeBuilder().WithNodeId(Rid(1).ToArray()).WithEpoch(1).Build();
using var b = new NodeBuilder().WithNodeId(Rid(2).ToArray()).WithEpoch(1).Build();

a.RegisterLoopbackPeer(b);
b.RegisterLoopbackPeer(a);
Transport.MakeSession(a, b);

int writes = 0;
a.FieldWritten += (_, _, _) => writes++;
b.BatchApplied += (result, sender) =>
    Console.WriteLine($"  batch from {Convert.ToHexString(sender.ToArray())[..8]}… applied={result.Applied} dup={result.Duplicate}");

a.WriteField(Ns, Rid(100), fieldId: 0x101, Val("hello-from-a"));
b.WriteField(Ns, Rid(200), fieldId: 0x101, Val("hello-from-b"));

int rounds = Transport.SyncRound(a, b, Ns);
Console.WriteLine($"SyncRound delivered {rounds} batch(es); local writes raised FieldWritten={writes}.");

bool bHasA = HasField(b, Ns, Rid(100), 0x101, "hello-from-a");
bool aHasB = HasField(a, Ns, Rid(200), 0x101, "hello-from-b");
Console.WriteLine($"B has A's write: {bHasA}");
Console.WriteLine($"A has B's write: {aHasB}");

if (!bHasA || !aHasB)
{
    Console.Error.WriteLine("Sample failed: expected cross-visible field writes after SyncRound.");
    return 1;
}

Console.WriteLine("OK — two-node loopback sample passed.");
return 0;
