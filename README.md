# BIN-SYNC / BinSync.Net

Compact binary sync protocol (BIN-SYNC CORE) and a .NET 10 reference implementation.

| Artifact | Path |
|---|---|
| Spec (v2.4.17) | [BIN-SYNC-SPECIFICATION.txt](BIN-SYNC-SPECIFICATION.txt) |
| Historical addendum (v2.3.7) | [BIN-SYNC-v2.3.7-ADDENDUM.txt](BIN-SYNC-v2.3.7-ADDENDUM.txt) |
| Production status | [docs/PRODUCTION-STATUS.md](docs/PRODUCTION-STATUS.md) |
| Metadata storage / external provider architecture | [docs/METADATA-STORAGE-ARCHITECTURE.md](docs/METADATA-STORAGE-ARCHITECTURE.md) |
| FLD-DB synchronized-database contract | [docs/FLD-DB-INTEGRATION-CONTRACT.md](docs/FLD-DB-INTEGRATION-CONTRACT.md) |
| Public API surface (3.0.0) | [docs/PUBLIC-API.md](docs/PUBLIC-API.md) |
| Changelog | [docs/CHANGELOG.md](docs/CHANGELOG.md) |
| License | [LICENSE.md](LICENSE.md) |
| Crypto notice | [docs/CRYPTO-NOTICE.md](docs/CRYPTO-NOTICE.md) |
| Third-party | [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md) |
| Naming | [docs/NAME.md](docs/NAME.md) |
| .NET engine | [BinSync.Net/](BinSync.Net/) |

**Package:** BinSync.Core 3.0.0 · **Target:** net10.0  
**Branch:** feature/fld-external-storage (PR #16, Draft)  
**CI:** unit tests + fuzz/lab + build/run of all three public samples  
**Crypto:** vendored RFC 8032 Ed25519 (MIT subset) — no BouncyCastle.Cryptography NuGet.

---

## Runtime support profile

The current Node apply path supports **LWW_FIELD** only (wire code 0x02; legacy code 0 maps to LWW). Unsupported merge policy codes are rejected with SCHEMA_MISMATCH instead of silently being treated as LWW.

The CRDT and MAX/MIN merge classes are available as standalone payload/helper APIs. MergePolicyRegistry is not wired into Node.ApplyBatch; schema policy metadata validates compatibility but does not dispatch a merge algorithm. See §8 of the specification and the public API contract.

| Area | Current status |
|---|---|
| Generic record model, HLC/LWW_FIELD, lifecycle and field tombstones | implemented |
| Outbox/inbox, DELTA_BATCH, ACK, relay | implemented |
| Frame codec, CRC32C, fragmentation, generic SyncSession | implemented |
| STATE_DIGEST / NEED_* / IBLT reconciliation | implemented |
| Membership / GC_NOTICE / quarantine | implemented |
| CRDT payload codecs and merge helpers | helper APIs; not active in Node apply path |
| Schema evolution | opt-in metadata gate; not merge-policy dispatch |
| E2EE ChaCha20-Poly1305 | opt-in |
| Ed25519 batch signatures + KEY_ROTATION | implemented |
| Causal dependencies | opt-in |
| TrustStore TOFU / PreShared / revoke | implemented; not full X.509 PKI |
| Lab / adversarial fuzz | in CI |
| Hot-path encode/decode/apply optimisations | audited on this branch |

---

## Quick start

```bash
cd BinSync.Net
dotnet test BinSync.slnx -c Release
dotnet pack src/BinSync.Core -c Release
```

Embedder contract and SemVer rules: [docs/PUBLIC-API.md](docs/PUBLIC-API.md).

## Samples

Run from repository root:

```bash
dotnet run --project samples/TwoNodeLoopback -c Release
dotnet run --project samples/TwoNodeTcp -c Release
dotnet run --project samples/ScenarioHarness -c Release
dotnet run --project samples/ScenarioHarness -c Release -- --soak 10
```

All three projects are built and executed by CI. See [samples/README.md](samples/README.md) for the scenario matrix.

## License

Dual: **AGPL-3.0-or-later** + commercial (see LICENSE.md). Vendored Ed25519 remains MIT.
