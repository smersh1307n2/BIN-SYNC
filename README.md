# BYN-SYNC / BinSync.Net

Compact binary sync protocol (**BIN-SYNC CORE**) and a **.NET 10** reference implementation.

| Artifact | Path |
|---|---|
| Spec (v2.3.6) | [`BYN-SYNC-SPECIFICATION.txt`](BYN-SYNC-SPECIFICATION.txt) |
| Addendum / errata (v2.3.7) | [`BYN-SYNC-v2.3.7-ADDENDUM.txt`](BYN-SYNC-v2.3.7-ADDENDUM.txt) |
| Production status | [`docs/PRODUCTION-STATUS.md`](docs/PRODUCTION-STATUS.md) |
| Public API freeze | [`docs/PUBLIC-API.md`](docs/PUBLIC-API.md) |
| Changelog | [`docs/CHANGELOG.md`](docs/CHANGELOG.md) |
| Crypto notice | [`docs/CRYPTO-NOTICE.md`](docs/CRYPTO-NOTICE.md) |
| .NET engine | [`BinSync.Net/`](BinSync.Net/) |

**Package:** `BinSync.Core` **`0.1.0-alpha.5`** · **Target:** `net10.0`  
**Branch:** `installing-.net-10-ea6be` · **Tests:** 273 passed (CI)  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) — **no** `BouncyCastle.Cryptography` NuGet.

---

## What works

Production-hardened reference engine (~95%+ of the normative spec):

| Area | Status |
|------|--------|
| LWW + HLC + durable author | done |
| Outbox/inbox, DELTA_BATCH, ACK | done |
| Frame codec, CRC32C, FLAG_FRAGMENT | done |
| STATE_DIGEST / NEED_* / IBLT ladder | done |
| Membership / GC_NOTICE / quarantine | done |
| Merge §8 (COUNTER, SET_OR, SEQ, BLOB, MANUAL, Max/Min) | done |
| Schema §18 (opt-in) | done |
| E2EE ChaCha20-Poly1305 (opt-in) | done |
| Ed25519 batch signatures + KEY_ROTATION | done |
| Causal deps (opt-in) | done |
| TrustStore TOFU / PreShared / revoke (§15.6) | done |
| TCP + optional TLS | done |
| Lab §17 core + edge | done |

---

## Requirements

- [.NET SDK 10](https://dotnet.microsoft.com/download) (`net10.0`)

```bash
# CI also runs scripts/vendor-ed25519-from-ztsharp.sh before build
cd BinSync.Net
dotnet test
```

Pack NuGet locally:

```bash
bash scripts/pack-nupkg.sh
```

---

## Minimal usage

```csharp
var a = new Node(nodeIdA, epoch: 1);
var b = new Node(nodeIdB, epoch: 1);
Transport.MakeSession(a, b);

a.WriteField(ns: 1, recordId, fieldId: 1, valueBytes);
Transport.SyncRound(a, b, namespaceId: 1);

NodePersistence.Save(a, "/path/a.bin");
var a2 = NodePersistence.Load("/path/a.bin");
```

### Opt-in surfaces

```csharp
// E2EE
a.EnsureE2ee().InstallNamespaceKey(1, E2ee.GenerateNamespaceKey());
a.WriteFieldSealed(1, recordId, fieldId, plaintext);

// Trust (TOFU / PreShared)
b.EnsureTrust(TrustMode.Tofu);
b.TrustPeer(a.NodeId, aPublicKey, TrustMode.PreShared);
b.VerifyAndApplyBatch(batch, sender, signature, candidatePublicKey);

// Causal
a.Causal = new CausalTracker();
a.ApplyBatchCausal(batch, senderPeer);

// Schema gate
a.Schemas.Register(descriptor);
a.ApplyBatchWithSchema(batch, senderPeer);
```

### TCP

```csharp
await using var listener = TcpSyncListener.Start(port: 9250);
var accepted = listener.AcceptAsync(serverNode);
await using var client = await TcpSyncSession.ConnectAsync(clientNode, "127.0.0.1", listener.Port);
await using var server = await accepted;

await client.PushBatchAsync(clientNode, namespaceId: 1);
await server.ReceiveBatchAsync(serverNode);
```

TLS (lab):

```csharp
using var cert = TlsTestCert.CreateEphemeral();
var accept = listener.AcceptAsync(serverNode, serverCertificate: cert);
var ssl = TlsTestCert.ClientOptions("localhost", cert.GetCertHashString());
await using var client = await TcpSyncSession.ConnectAsync(
    clientNode, "127.0.0.1", listener.Port, sslOptions: ssl);
```

---

## Tag / release

```bash
git fetch origin installing-.net-10-ea6be
git tag -a v0.1.0-alpha.5 a12bc30bd1747ab8c607087a8128160c8efc4665 \
  -m "BinSync.Core 0.1.0-alpha.5"
git push origin v0.1.0-alpha.5
```

Or create a GitHub Release from that tag.

---

## License

MIT (engine). Vendored Ed25519: MIT (ZtSharp / Bouncy Castle subset) — see [`docs/CRYPTO-NOTICE.md`](docs/CRYPTO-NOTICE.md).
