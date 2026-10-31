# BIN-SYNC / BinSync.Net

Compact binary sync protocol (**BIN-SYNC CORE**) and a **.NET 10** reference implementation.

| Artifact | Path |
|---|---|
| Spec (v2.3.6) | [`BIN-SYNC-SPECIFICATION.txt`](BIN-SYNC-SPECIFICATION.txt) |
| Addendum (v2.3.7) | [`BIN-SYNC-v2.3.7-ADDENDUM.txt`](BIN-SYNC-v2.3.7-ADDENDUM.txt) |
| Production status | [`docs/PRODUCTION-STATUS.md`](docs/PRODUCTION-STATUS.md) |
| Public API freeze (1.0) | [`docs/PUBLIC-API.md`](docs/PUBLIC-API.md) |
| Changelog | [`docs/CHANGELOG.md`](docs/CHANGELOG.md) |
| License | [`LICENSE.md`](LICENSE.md) |
| Crypto notice | [`docs/CRYPTO-NOTICE.md`](docs/CRYPTO-NOTICE.md) |
| Third-party | [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md) |
| Naming | [`docs/NAME.md`](docs/NAME.md) |
| .NET engine | [`BinSync.Net/`](BinSync.Net/) |
| Two-node sample | [`samples/TwoNodeLoopback/`](samples/TwoNodeLoopback/) |

**Package:** `BinSync.Core` **`1.0.0`** · **Target:** `net10.0`  
**Branch:** `main` · **Tests:** 274 (CI)  
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
| Lab / adversarial fuzz in CI | done |
| Hot-path encode/decode/apply optimisations | done (on `main`) |

---

## Quick start

```bash
# restore + test
cd BinSync.Net
dotnet test tests/BinSync.Tests -c Release

# pack
dotnet pack src/BinSync.Core -c Release
```

Embedder contract and SemVer rules: **[docs/PUBLIC-API.md](docs/PUBLIC-API.md)**.

---

### Minimal sample (two nodes, loopback)

```bash
bash scripts/vendor-ed25519-from-ztsharp.sh   # once
dotnet run --project samples/TwoNodeLoopback -c Release
```


## License

Dual: **AGPL-3.0-or-later** + commercial (see `LICENSE.md`). Vendored Ed25519 remains MIT.
