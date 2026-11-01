// Two-node TCP loopback: server accepts, client connects, push/receive deltas.
using System.Text;
using BinSync;

const int Ns = 1;
static byte[] Rid(int n)
{
    var b = new byte[16];
    BitConverter.GetBytes(n).CopyTo(b, 0);
    return b;
}
static byte[] Val(string s) => Encoding.UTF8.GetBytes(s);
static bool Has(Node node, byte[] rid, string expected)
{
    if (!node.Storage.TryGetValue(Ns, out var store)) return false;
    if (!store.TryGetValue(rid, out var rec)) return false;
    if (!rec.State.TryGetValue(0x101, out var fs)) return false;
    return Encoding.UTF8.GetString(fs.Value) == expected;
}

var serverNode = new Node(nodeId: Rid(1), epoch: 1);
var clientNode = new Node(nodeId: Rid(2), epoch: 1);
serverNode.WriteField(Ns, Rid(100), 0x101, Val("via-tcp-from-server"));
clientNode.WriteField(Ns, Rid(200), 0x101, Val("via-tcp-from-client"));

await using var listener = TcpSyncListener.Start();
Console.WriteLine($"[INFO] listening on 127.0.0.1:{listener.Port}");

var acceptTask = listener.AcceptAsync(serverNode);
await using var clientSession = await TcpSyncSession.ConnectAsync(clientNode, "127.0.0.1", listener.Port);
await using var serverSession = await acceptTask;
Console.WriteLine($"[INFO] sessions up tls={clientSession.IsTls}");

var recvServer = serverSession.ReceiveBatchAsync(serverNode);
bool pushedClient = await clientSession.PushBatchAsync(clientNode, Ns);
var rServer = await recvServer;
Console.WriteLine($"[INFO] client push={pushedClient} server Applied={rServer.Applied}");

var recvClient = clientSession.ReceiveBatchAsync(clientNode);
bool pushedServer = await serverSession.PushBatchAsync(serverNode, Ns);
var rClient = await recvClient;
Console.WriteLine($"[INFO] server push={pushedServer} client Applied={rClient.Applied}");

bool ok = Has(clientNode, Rid(100), "via-tcp-from-server")
       && Has(serverNode, Rid(200), "via-tcp-from-client");
Console.WriteLine(ok ? "OK — two-node TCP sample passed." : "FAIL — TCP cross-visibility missing.");
return ok ? 0 : 1;
