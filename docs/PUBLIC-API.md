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

| Type / API | Role |
|------------|------|
| `CausalTracker` | Known set, pending quarantine, cycle / timeout / lost (KЗ1–KЗ5) |
| `Node.Causal` | Optional tracker attached to a node |
| `Node.ApplyBatchCausal` | Gate on `OutboxEntry.CausalDeps` before `ApplyBatchInternal` |
| `Node.DrainCausal` | Release ready pending ops after parents arrive |
| `CausalError.CausalDepMissing` | `0x0012` |

- **`ApplyBatch`** — unchanged; ignores `Causal` and deps.
- **`ApplyBatchCausal`** — if `Causal` is set: EvaluateBatch; on Pending → EnqueuePending + throw `CAUSAL_DEP_MISSING` (no cursor advance, KЗ4); on Ready → apply + `NoteApplied` each changeId.
- Embedder re-delivers held batches after `DrainCausal` reports Ready.

### E2EE (§14.5 foundation)

| Type / API | Role |
|------------|------|
| `E2ee.DeriveNonce(...)` | KDF-SHA256 → 12-byte nonce [E1] |
| `E2ee.Encrypt` / `Decrypt` | ChaCha20-Poly1305 (ciphertext \|\| tag) |
| `E2ee.SealField` / `OpenField` | nonce from field context + seal/open |
| `E2ee.GenerateNamespaceKey` | random 32-byte key |

**Blind merge:** server/peer LWW compares **ciphertext bytes** with open HLC + `authorNodeId` (spec §14.6).  
**Not** auto-wired into `WriteField`. Embedder seals before write and opens after read.

**Still out of scope:** `KEY_ROTATION` frame (§14.9), Ed25519 batch signatures (§14.8), SHREDDED marker / crypto-shredding UX, capability bit plumbing for ENCRYPTED.

---

## Lab / unstable
`TlsTestCert`, partial `FlowControl`.

## Explicitly out of scope
Full E2EE product path, schema evolution (§18), auto-WAL on `WriteField`, forcing causal on every `ApplyBatch`.
