# BinSync.Core — Public API surface (freeze for 0.1.x-alpha)

**Package:** `BinSync.Core` `0.1.0-alpha.2` · **Target:** `net10.0`  
Normative: **BYN-SYNC-SPECIFICATION.txt v2.3.6** + [ERRATA-v2.3.7](ERRATA-v2.3.7.md).  
Gaps: [PRODUCTION-STATUS.md](PRODUCTION-STATUS.md).

---

## Supported (embedder contract)

### Engine
`Node`, `BoundedNode`, `ResourceLimits`, batch/outbox models, `WriteField` / `ApplyBatch` / alias registry, `ApplyRemoteField`, `RepublishUnderPrefix`.

### Wire & codecs
`FrameCodec`, `Codec`/`Writer`/`Reader` (`B128` vs `Bytes()`), `Ops`, `Hlc`, `Merge`/`MergePolicy`/`MergePolicyRegistry`, digests.

### Merge (§8)
`LwwField` (default on `WriteField`), `MaxBytes`/`MinBytes`/`MaxU64`/`MinU64` via `MergeFieldWithPolicy`.

### Reconcile (§13)
`Reconcile` ladder, `Iblt`, `GcNotice`, `MembershipRoster`, bilateral AutoReconcile / IBLT.

### Session / transport
`ISyncTransport`, TCP (+ TLS opt-in), pipe, loopback, `TcpSyncSession` orchestration APIs.

### Persistence
`NodePersistence` (full snapshot), `Wal` (manual append / replay / checkpoint).

### Causal deps (§16.19 experimental)
`CausalTracker`, `CausalOutcome`, `PendingCausalOp`, `CausalError.CausalDepMissing` (`0x0012`).  
KЗ1–KЗ5; **not** auto-wired into `ApplyBatch`.

### E2EE (§14.5 foundation)

| Type / API | Role |
|------------|------|
| `E2ee.DeriveNonce(...)` | KDF-SHA256 → 12-byte nonce [E1] |
| `E2ee.Encrypt` / `Decrypt` | ChaCha20-Poly1305 (ciphertext \|\| tag) |
| `E2ee.SealField` / `OpenField` | nonce from field context + seal/open |
| `E2ee.GenerateNamespaceKey` | random 32-byte key |

**Blind merge:** server/peer LWW compares **ciphertext bytes** with open HLC + `authorNodeId` (spec §14.6).  
**Not** auto-wired into `WriteField`. Embedder seals before write and opens after read.

```csharp
var nsKey = E2ee.GenerateNamespaceKey();
var sealedVal = E2ee.SealField(nsKey, recordId, fieldId, hlc.Wall, hlc.Logical, author, keyEpoch: 1, plaintext);
node.WriteField(ns, recordId, fieldId, sealedVal); // stores opaque ciphertext
// later:
var plain = E2ee.OpenField(nsKey, recordId, fieldId, wall, logical, author, 1, storedCiphertext);
```

**Still out of scope:** `KEY_ROTATION` frame (§14.9), Ed25519 batch signatures (§14.8), SHREDDED marker / crypto-shredding UX, capability bit plumbing for ENCRYPTED.

---

## Lab / unstable
`TlsTestCert`, partial `FlowControl`.

## Explicitly out of scope
Full E2EE product path, schema evolution (§18), auto-WAL / auto-causal / auto-merge-registry on `WriteField`.
