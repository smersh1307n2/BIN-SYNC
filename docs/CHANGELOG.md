# Changelog — BinSync.Core

## 0.1.0-alpha.6 — 2026-10-28

Production performance pass on `main` after alpha.5 protocol surface.
**Tip SHA:** `661666e` · **Target:** `net10.0` · **Package:** `BinSync.Core` `0.1.0-alpha.6`

### Engine / hot paths
- **Comparers:** shared `BA.Instance` / `BAT` / `BA4` / `BAI*` / `SeenKeyComparer.Instance` — no per-call `new BA()` on dictionary/set construction
- **`BA.GetHashCode`:** `HashCode.AddBytes` instead of manual foreach hash loops
- **Codec:** `WriteVarint` into `Span<byte>`, stackalloc paths for fixed LE writes and varint encode
- **HLC:** wall clock in **microseconds** (`UnixTimeMicroseconds`); author bytes via `SequenceCompareTo`
- **Transport / SessionWire / Reconcile / NodePersistence:** same shared comparers; dropped duplicate `ByteArrEq` / `BAI2b` / `ByteArrayComparer`

### Unchanged from alpha.5 (still in tree)
- Vendored Ed25519 (MIT), no BouncyCastle NuGet
- Merge §8 full set, Schema §18, E2EE / causal / KEY_ROTATION (opt-in)
- TrustStore §15.6, FLAG_FRAGMENT, Lab §17
- Dual license AGPL-3.0-or-later + commercial; crypto MIT

### Package
```
PackageId: BinSync.Core
Version:   0.1.0-alpha.6
```
Single external dependency: `Blake3.Managed` 1.10.0.

### Tag
```bash
git fetch origin main
git tag -a v0.1.0-alpha.6 661666e234bb51659ac087981aa7481e6fa9c86e -m "BinSync.Core 0.1.0-alpha.6"
git push origin v0.1.0-alpha.6
```

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

## 0.1.0-alpha.4
- Codec extensions + version bump after BouncyCastle removal path

## 0.1.0-alpha.3
- Initial net10.0 packaging, public API freeze notes
