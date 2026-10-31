// Minimal two-node loopback sample for BinSync.Core 1.0.
// Run from repo root:
//   bash scripts/vendor-ed25519-from-ztsharp.sh   # once
//   dotnet run --project samples/TwoNodeLoopback -c Release
using BinSync;

const int Ns = 1;
static byte[] Rid(int n)
{
    var b = new byte[16];
    BitConverter.GetBytes(n).CopyTo(b, 0);
    return b;
}
static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);

static bool HasField(Node node, int ns, byte[] rid, ushort fieldId, string expected)
{
    if (!node.Storage.TryGetValue(ns, out var store)) return false;
    if (!store.TryGetValue(rid, out var rec)) return false;
    if (!rec.State.TryGetValue(fieldId, out var fs)) return false;
    return System.Text.Encoding.UTF8.GetString(fs.Value) == expected;
}

var a = new Node(nodeId: Rid(1), epoch: 1);
var b = new Node(nodeId: Rid(2), epoch: 1);
Transport.MakeSession(a, b);

a.WriteField(Ns, Rid(100), fieldId: 0x101, Val("hello-from-a"));
b.WriteField(Ns, Rid(200), fieldId: 0x101, Val("hello-from-b"));

int rounds = Transport.SyncRound(a, b, Ns);
Console.WriteLine($"SyncRound delivered {rounds} batch(es).");

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
