# BYN-SYNC / BinSync.Net

Compact binary sync protocol (**BIN-SYNC CORE**) and a **.NET 10** reference implementation.

| Artifact | Path |
|---|---|
| Spec (v2.3.6) | [`BYN-SYNC-SPECIFICATION.txt`](BYN-SYNC-SPECIFICATION.txt) |
| Addendum / errata (v2.3.7) | [`BYN-SYNC-v2.3.7-ADDENDUM.txt`](BYN-SYNC-v2.3.7-ADDENDUM.txt) |
| Production status | [`docs/PRODUCTION-STATUS.md`](docs/PRODUCTION-STATUS.md) |
| Public API freeze | [`docs/PUBLIC-API.md`](docs/PUBLIC-API.md) |
| Changelog | [`docs/CHANGELOG.md`](docs/CHANGELOG.md) |
| License | [`LICENSE.md`](LICENSE.md) |
| Crypto notice | [`docs/CRYPTO-NOTICE.md`](docs/CRYPTO-NOTICE.md) |
| Third-party | [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md) |
| .NET engine | [`BinSync.Net/`](BinSync.Net/) |

**Package:** `BinSync.Core` **`0.1.0-alpha.5`** · **Target:** `net10.0`  
**Branch:** `installing-.net-10-ea6be` · **Tests:** 273 passed (CI)  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset) — **no** `BouncyCastle.Cryptography` NuGet.

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

---

## License

Same dual-license model as [ZtSharp](https://github.com/smersh1307n2/ZtSharp):

| Component | License |
|-----------|---------|
| **Original BYN-SYNC / BinSync code** | **AGPL-3.0-or-later** + **commercial** option |
| Vendored Ed25519 (Bouncy Castle subset) | **MIT** (not owned by BYN-SYNC; not dual-licensed) |
| Blake3.Managed | Apache-2.0 (upstream) |

Commercial inquiries: **smersh@bk.ru**

Details: [`LICENSE.md`](LICENSE.md) · [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md) · [`docs/CRYPTO-NOTICE.md`](docs/CRYPTO-NOTICE.md)

> A commercial license covers only original BYN-SYNC code. MIT crypto and other third-party packages stay under their own terms.
