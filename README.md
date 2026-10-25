# BIN-SYNC / BinSync.Net

Compact binary sync protocol (**BIN-SYNC CORE**) and a **.NET 10** reference implementation.

> **Name:** **BIN-SYNC** (binary sync). Not “BYN-SYNC” — that was a typo / old repo slug. See [`docs/NAME.md`](docs/NAME.md).

| Artifact | Path |
|---|---|
| Addendum (v2.3.7) | [`BIN-SYNC-v2.3.7-ADDENDUM.txt`](BIN-SYNC-v2.3.7-ADDENDUM.txt) |
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
cd BinSync.Net && dotnet test
```

---

## License

| Component | License |
|-----------|---------|
| **Original BIN-SYNC / BinSync code** | **AGPL-3.0-or-later** + **commercial** |
| Vendored Ed25519 (Bouncy Castle subset) | **MIT** |
| Blake3.Managed | Apache-2.0 |

Commercial: **smersh@bk.ru** · Details: [`LICENSE.md`](LICENSE.md)

---

## GitHub repository name

