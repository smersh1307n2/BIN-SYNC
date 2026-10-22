# BYN-SYNC / BinSync.Net

Compact binary sync protocol (BIN-SYNC CORE) and a .NET 10 reference implementation.

| Artifact | Path |
|---|---|
| Spec (v2.3.6) | [`BYN-SYNC-SPECIFICATION.txt`](BYN-SYNC-SPECIFICATION.txt) |
| Normative errata (v2.3.7, E-1…E-13) | [`docs/ERRATA-v2.3.7.md`](docs/ERRATA-v2.3.7.md) |
| Production gap matrix | [`docs/PRODUCTION-STATUS.md`](docs/PRODUCTION-STATUS.md) |
| .NET engine | [`BinSync.Net/`](BinSync.Net/) |

## What works today

In-process engine suitable for embedding when peers are trusted, plus a TCP/TLS session host:

- LWW_FIELD + lifecycle delete-wins, HLC, durable `authorNodeId`
- Outbox/inbox, per-author `seq`, `DELTA_BATCH` chain digests, ACK (incl. author-stream trailer)
- Session alias maps, epoch watermarks, relay with origin re-stamp, echo suppression (E-8/E-13)
- Frame codec (25-byte header, CRC32C), sneakernet export/import
- Atomic node snapshot persistence (BLAKE3-tagged `NODESTATE/v1`)
- Async pairwise drain (`AsyncTransport`)
- TCP accept/connect: HELLO / HELLO_ACK / ALIAS_MAP, then delta + ACK (`TcpSyncListener`, `TcpSyncSession`)
- Optional TLS via `SslStream` (pass `serverCertificate` / `SslClientAuthenticationOptions`)

**Not production-complete vs the full spec:** mutual TLS, STATE_DIGEST / NEED_* / IBLT, membership & GC, extra merge policies, E2EE. See the status matrix.

## Requirements

- [.NET SDK 10](https://dotnet.microsoft.com/download) (`net10.0`)

```bash
cd BinSync.Net
dotnet test BinSync.slnx
```

## Minimal usage sketch

```csharp
var a = new Node(nodeIdA, epoch: 1);
var b = new Node(nodeIdB, epoch: 1);
Transport.MakeSession(a, b);

a.WriteField(ns: 1, recordId, fieldId: 1, valueBytes);
Transport.SyncRound(a, b, namespaceId: 1);

// optional durable snapshot
NodePersistence.Save(a, "/path/a.bin");
var a2 = NodePersistence.Load("/path/a.bin");
```

`Transport.MakeSession` / `DeliverBatch` / `SyncRound` are in-process helpers.

TCP (initiator is alias 1, acceptor is alias 2; acceptor-only durable authors are appended):

```csharp
await using var listener = TcpSyncListener.Start(port: 9250);
var accepted = listener.AcceptAsync(serverNode);          // HELLO + ALIAS_MAP
await using var client = await TcpSyncSession.ConnectAsync(clientNode, "127.0.0.1", listener.Port);
await using var server = await accepted;

clientNode.WriteField(1, recordId, fieldId, valueBytes);
var incoming = server.ReceiveBatchAsync(serverNode);
await client.PushBatchAsync(clientNode, namespaceId: 1);   // DELTA_BATCH + ACK
await incoming;
```

TLS (lab):

```csharp
using var cert = TlsTestCert.CreateEphemeral();
var accept = listener.AcceptAsync(serverNode, serverCertificate: cert);
var ssl = TlsTestCert.ClientOptions("localhost", cert.GetCertHashString());
await using var client = await TcpSyncSession.ConnectAsync(
    clientNode, "127.0.0.1", listener.Port, sslOptions: ssl);
```

## Branch notes

Active hardening branch: `installing-.net-10-ea6be`.  
Safety copy of the same tip: `backup/installing-.net-10-ea6be`.
