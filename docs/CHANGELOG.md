# Changelog — BinSync.Core

## 0.1.0-alpha.5 — 2026-10-24

Production-hardening cut on branch `installing-.net-10-ea6be`.
**Tip SHA:** `a12bc30` (TrustStore) · **CI:** 273 tests passed.

### Crypto
- **Removed** NuGet dependency on `BouncyCastle.Cryptography`
- Vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) via CI `vendor-ed25519-from-ztsharp.sh`
- Batch signatures, E2EE (ChaCha20-Poly1305), KEY_ROTATION

### Protocol / engine
- **Codec §6:** TLV, UTF-8, f64 (NaN reject, −0→+0), signed ints, b256, canonical fields
- **Merge §8 complete:** LWW, COUNTER_G/PN, SET_OR, SEQ_FRACTIONAL (0x06), BLOB_REF (0x08), MANUAL (0x09), Max/Min 0x81–0x84
- **Schema §18:** SchemaRegistry, SCHEMA_MAP (0x04), ApplyBatchWithSchema
- **Causal deps** (opt-in): CausalWire CCD1, quarantine + drain
- **E2EE** (opt-in): WriteFieldSealed / ReadFieldOpen
- **KEY_ROTATION** + KeyEpochStore
- **FLAG_FRAGMENT** split / out-of-order reassembly
- **Security §15.6:** TrustStore TOFU / PreShared / CA roots / revoke; VerifyAndApplyBatch

### Lab §17
- Core fuzz + edge: S4 snapshot+tail, S12 fragmentation, S13 quarantine/cutoff

### Package
- Target: `net10.0`
- Single dependency: `Blake3.Managed` 1.10.0
- Pack: `scripts/pack-nupkg.sh`

### Tag (create on GitHub)
```bash
git fetch origin installing-.net-10-ea6be
git tag -a v0.1.0-alpha.5 a12bc30bd1747ab8c607087a8128160c8efc4665 -m "BinSync.Core 0.1.0-alpha.5"
git push origin v0.1.0-alpha.5
# or: GitHub → Releases → Draft a new release → tag v0.1.0-alpha.5
```

## 0.1.0-alpha.4
- Codec extensions + version bump after BouncyCastle removal path

## 0.1.0-alpha.3
- Initial net10.0 packaging, public API freeze notes
