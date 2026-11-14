// Two-node in-memory session sample using the transport-independent SyncSession API.
// Run from repo root:
//   dotnet run --project samples/TwoNodeLoopback -c Release
using System.Text;
using BinSync;

const int Ns = 1;

static RecordId Rid(int n)
{
    var bytes = new byte[16];
    BitConverter.GetBytes(n).CopyTo(bytes, 0);
    return RecordId.From(bytes);
}

static byte[] Val(string value) => Encoding.UTF8.GetBytes(value);

static bool HasField(Node node, RecordId recordId, int fieldId, string expected)
{
    return node.TryGetField(Ns, recordId, fieldId, out var field)
        && field is not null
        && Encoding.UTF8.GetString(field.Value) == expected;
}

using var a = new NodeBuilder().WithNodeId(Rid(1).ToArray()).WithEpoch(1).Build();
using var b = new NodeBuilder().WithNodeId(Rid(2).ToArray()).WithEpoch(1).Build();

a.WriteField(Ns, Rid(100), fieldId: 0x101, Val("hello-from-a"));
b.WriteField(Ns, Rid(200), fieldId: 0x101, Val("hello-from-b"));

int appliedEvents = 0;
a.BatchApplied += (result, sender) =>
{
    Interlocked.Increment(ref appliedEvents);
    Console.WriteLine($"A applied={result.Applied} duplicate={result.Duplicate} from={sender}");
};
b.BatchApplied += (result, sender) =>
{
    Interlocked.Increment(ref appliedEvents);
    Console.WriteLine($"B applied={result.Applied} duplicate={result.Duplicate} from={sender}");
};

var (transportA, transportB) = PipeTransport.CreatePair();
var establishA = SyncSession.EstablishAsync(a, transportA);
var establishB = SyncSession.EstablishAsync(b, transportB);
await using var sessionA = await establishA;
await using var sessionB = await establishB;

Console.WriteLine($"Established generic sessions: A↔{sessionA.PeerNodeId}, B↔{sessionB.PeerNodeId}");

// Push both directions concurrently. Each receiver applies the batch and returns its ACK.
var receiveAtA = sessionA.ReceiveBatchAsync(a);
var receiveAtB = sessionB.ReceiveBatchAsync(b);
var pushFromA = sessionA.PushBatchAsync(a, Ns);
var pushFromB = sessionB.PushBatchAsync(b, Ns);
await Task.WhenAll(receiveAtA, receiveAtB, pushFromA, pushFromB);

bool bHasA = HasField(b, Rid(100), 0x101, "hello-from-a");
bool aHasB = HasField(a, Rid(200), 0x101, "hello-from-b");
Console.WriteLine($"A→B pushed={pushFromA.Result}, applied={receiveAtB.Result.Applied}");
Console.WriteLine($"B→A pushed={pushFromB.Result}, applied={receiveAtA.Result.Applied}");
Console.WriteLine($"B has A's write: {bHasA}");
Console.WriteLine($"A has B's write: {aHasB}");
Console.WriteLine($"BatchApplied events: {appliedEvents}");

if (!pushFromA.Result || !pushFromB.Result || !bHasA || !aHasB)
{
    Console.Error.WriteLine("Sample failed: expected bidirectional visibility after SyncSession exchange.");
    return 1;
}

Console.WriteLine("OK — two-node in-memory SyncSession sample passed.");
return 0;
