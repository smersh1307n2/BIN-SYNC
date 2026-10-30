# BinSync.Core — Public API surface (freeze for 1.0)

**Package:** `BinSync.Core` **`1.0.0-rc.2`** · **Target:** `net10.0`  
**Protocol:** **BIN-SYNC CORE**  
Normative: **BIN-SYNC-SPECIFICATION.txt v2.3.6** + [ERRATA-v2.3.7](ERRATA-v2.3.7.md) / addendum.  
Status: [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md) · Changelog: [CHANGELOG.md](CHANGELOG.md) · Naming: [NAME.md](NAME.md).

## SemVer policy (1.x)

| Change | Version impact |
|--------|----------------|
| Wire/protocol break, remove/rename public type or member | **major** (2.0) |
| New public API, additive frame/op support, perf-only | **minor** (1.1) |
| Bugfix, docs, internal optimisations | **patch** (1.0.1) |

**Public** = types and members documented below and in XML docs shipped with the package.  
Everything under `internal`, `*.Tests`, `bench/`, and unmarked helpers is **not** a compatibility contract.

Opt-in surfaces (causal, E2EE, schema, trust gates) stay opt-in: enabling them is configuration, not a silent default change.

---

## Supported (embedder contract)

### Engine
`Node`, `BoundedNode`, `ResourceLimits`, batch/outbox models, `WriteField` / `ApplyBatch` / alias registry, `ApplyRemoteField`, `RepublishUnderPrefix`.

### Wire & codecs (§6, §12)
`FrameCodec`, `Codec` / `Writer` / `Reader`, `Ops`, `Hlc`, digests, `FragmentCodec` / `FragmentReassembler` (FLAG_FRAGMENT).

### Merge (§8)
| Policy | Code | API |
|--------|------|-----|
| LWW_FIELD | 0 / default | `WriteField`, `MergeField` |
| COUNTER_G | 0x03 | `CounterG` + `MergePolicy.CounterG` |
| COUNTER_PN | 0x04 | `CounterPn` |
| SET_OR | 0x05 | `SetOr` |
| SEQ_FRACTIONAL | 0x06 | `SeqFractional` |
| BLOB_REF | 0x08 | `BlobRef` |
| MANUAL | 0x09 | `MergePolicy.Manual` + FieldResolve 0x00FF |
| Max/Min bytes/u64 | 0x81–0x84 | engine extensions |

`MergePolicyRegistry` maps `(ns, fieldId)` → policy.

### Reconcile (§13)
`Reconcile` ladder, `Iblt`, `GcNotice`, `MembershipRoster`, `BuildSnapshot` / `ApplySnapshot`, AutoReconcile / IBLT over TCP.

### Session / transport
`ISyncTransport`, TCP (+ TLS opt-in), pipe, loopback, `TcpSyncSession` / `TcpSyncListener`.

### Persistence
`NodePersistence` (full snapshot), `Wal` (append / replay / checkpoint).

### Schema (§18, opt-in)
`SchemaDescriptor`, `SchemaRegistry`, `Node.Schemas`, `ApplyBatchWithSchema`, SCHEMA_MAP frame 0x04.

### Causal deps (§16.19, opt-in)

| API | Role |
|-----|------|
| `CausalTracker` | Known set, pending, KЗ1–KЗ5 |
| `Node.Causal` | Optional tracker |
| `Node.ApplyBatchCausal` / `DrainCausal` | Gate before apply |
| `CausalWire` | CCD1 trailer encode/decode |
| `CAUSAL_DEP_MISSING` | 0x0012 |

`ApplyBatch` ignores causal; use `ApplyBatchCausal` when enabled.

### E2EE (§14, opt-in)

| API | Role |
|-----|------|
| `E2ee.SealField` / `OpenField` | ChaCha20-Poly1305 |
| `E2eeKeyRing` / `Node.EnsureE2ee` | Per-namespace keys + epochs |
| `WriteFieldSealed` / `ReadFieldOpen` | Convenience |
| `KeyRotation` | Frame 0x11 + proof |

Blind merge compares ciphertext with open HLC + author.

### Signatures & trust (§14.8, §15.6)

| API | Role |
|-----|------|
| `BatchSignature` | Ed25519 sign/verify (vendored) |
| `TrustStore` | TOFU / PreShared / CA roots / revoke |
| `Node.TrustPeer` / `RevokeTrust` | Pin keys |
| `VerifyAndApplyBatch` | Signature gate then apply |
| `SIGNATURE_INVALID` / `TRUST_REVOKED` | 0x0013 / 0x0014 |

### Lab
`LabFuzzTests` scenarios, `TlsTestCert`, flow-control hooks.

---

## Explicitly out of scope (1.0)

- Full X.509 certificate path validation (CA roots are **node-id** allow-list only)
- Auto-WAL on every `WriteField`
- Forcing causal / E2EE / trust on every `ApplyBatch` (all opt-in)
- Guaranteed allocation-free paths under all policies (hot paths optimised; domain objects still allocate)
- NuGet.org publish automation (use `scripts/pack-nupkg.sh`)
