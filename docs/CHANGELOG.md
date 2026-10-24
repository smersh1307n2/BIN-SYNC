# Changelog — BinSync.Core

## 0.1.0-alpha.5 — 2026-10-24

Production-hardening cut on branch `installing-.net-10-ea6be`.

### Crypto
- **Removed** NuGet dependency on `BouncyCastle.Cryptography`
- Vendored RFC 8032 Ed25519 (MIT subset from ZtSharp) via CI `vendor-ed25519-from-ztsharp.sh`
- Batch signatures, E2EE (ChaCha20-Poly1305), KEY_ROTATION remain functional

### Protocol / engine
- **Codec §6:** TLV, UTF-8 strings, IEEE f64 (NaN reject, −0→+0), signed ints, b256, field canonicalization
- **Merge §8:** COUNTER_G (0x03), COUNTER_PN (0x04), SET_OR (0x05), MANUAL (0x09); Max/Min engine extensions 0x81–0x84
- **Schema §18:** `SchemaRegistry`, SCHEMA_MAP (0x04), `ApplyBatchWithSchema` gate
- **Causal deps** (opt-in): CausalWire trailer CCD1, quarantine + drain
- **E2EE** (opt-in): `WriteFieldSealed` / `ReadFieldOpen`, key ring + epochs
- **KEY_ROTATION** frame hooks + `KeyEpochStore`

### Tests
- ~249 tests green on .NET 10 (CI)
- Lab §17 core: parallelism, delete-wins, skew, epoch, CRC, flow control, MANUAL, multi-node fuzz

### Package
- Target: `net10.0`
- Single dependency: `Blake3.Managed` 1.10.0

### Not in this cut (remaining ~10–15%)
- Full §17 matrix (snapshot ladder, fragmentation, zombie quarantine deep paths)
- SEQ_FRACTIONAL / BLOB_REF merge policies
- Expanded §15 trust / TOFU UX beyond current hooks
- NuGet.org publish automation

## 0.1.0-alpha.4
- Codec extensions + version bump after BouncyCastle removal path

## 0.1.0-alpha.3
- Initial net10.0 packaging, public API freeze notes
