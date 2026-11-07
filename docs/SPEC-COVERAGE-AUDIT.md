# BIN-SYNC CORE normative coverage audit

**Baseline:** BIN-SYNC CORE v2.4.20 FINAL  
**Branch:** `feature/fld-external-storage`  
**Status:** first-pass section and contradiction audit. This is not a claim that every normative sentence has a one-to-one executable test. The remaining scenario-level mapping is an explicit release gate.

## 1. Section coverage matrix

| Spec section | Current implementation evidence | Status / remaining gate |
|---|---|---|
| §1 Goals and consistency | Core apply/merge model; multi-node scenario and lab tests | Core LWW convergence implemented. Business-level validity and application hooks remain outside the protocol. |
| §2 Terminology | Public API and wire DTOs | Documentation contract; no independent runtime test required for every term. Keep names aligned with public API. |
| §3 Transport | `SyncSession`, `ISyncTransport`, Pipe/Stream/TCP adapters; `PipeTransportTests`, `TcpSyncTests`, `SessionStreamBindingTests`, `TcpFragmentTests` | Implemented for reliable ordered adapters. Verify each new frame type against the stream-binding table. |
| §4 Identity / alias / epoch | `AliasAtomicityTests`, `SessionStreamBindingTests`, `Node.Alias`, handshake lifecycle | Implemented for current handshake contract. Session/peer epoch fencing must remain durable in Storage-backed mode. |
| §5 HLC | `Hardening.cs`, merge and WAL tests | Implemented for current HLC representation; retain overflow, clock rollback and tie-break regression coverage. |
| §6 Canonical codec | `CodecExtendedTests`, `CodecIdentityNormalizationTests`, fuzz tests | Implemented for supported types. Every malformed/overlong encoding rule still needs a direct negative test or an explicitly documented fuzz-only claim. |
| §7 Data model / lifecycle | `MergeFieldTombTests`, `SnapshotTests`, `Hardening.cs` | Current field/lifecycle model implemented. Tombstone reclamation remains disabled. |
| §8 Merge policies | `MergeCrdtTests`, `MergeSeqBlobTests`, `MergeFieldTombTests` | **Partial:** production Node apply path supports LWW_FIELD. Standalone CRDT helpers do not mean every advertised policy is enabled in the runtime. |
| §9 Digests / buckets | digest tests in `Hardening.cs`, `ReconcileTests`, `ReconcileExchangeTests` | Implemented for current digest version; verify root/bucket canonicalization against independent encoders. |
| §10 Outbox / Inbox / peer progress | `WalTests`, `ChangeIdWireTests`, `RemoteBatchWalAtomicityTests`, Metadata Store conformance and Storage integration | **Partial authority:** Node still owns a duplicate inbox/receive mirror and local sequence allocation. Durable Metadata Store outbound reservations are an opt-in path, not yet the sole authority. |
| §11 Membership / GC | `MembershipTests`, `GcNoticeTests`, `MetadataPeerStateStore` durable roster | Membership and peer fencing implemented in slices. Tombstone/ghost GC and Inbox/ChangeId GC remain disabled until their independent proofs exist. |
| §12 Wire format | codec, frame, stream binding, fragment and resource-limit tests | Implemented for currently supported frame types. Validate all frame type IDs, flags, stream IDs and exact payload boundaries as one table-driven suite. |
| §13 Sync / reconciliation / relay | `ReconcileTests`, `ReconcileExchangeTests`, `ReconcileFulfillTests`, `TcpSyncTests`, `InvariantsAndAdvanced.cs` | Main reconciliation ladder implemented. Durable provider baseline and receive authority cutover remain partial. |
| §14 E2EE | `E2eeWriteTests`, `KeyRotationTests`, `BatchSignatureTests` | Opt-in subset. Blind merge and supported encrypted operation families must not be presented as universal. |
| §15 Security / trust | `TrustStoreTests`, `TrustPersistTests`, `BatchSignatureTests` | TOFU, pre-shared trust and revocation implemented. CA mode is currently a NodeId allow-list, not X.509 certificate validation. |
| §16 Contract invariants | cross-cutting tests listed above, `Hardening.cs`, fuzz and lab suite | Broad coverage exists, but a requirement-ID → exact-test index is still needed. Causal dependency tracking remains experimental and process-memory state is not in Node WAL. |
| §17 Lab scenarios | `LabEdgeTests`, `FuzzTests`, CI Fuzz + Lab job | CI exercises 31 lab/fuzz cases. This does not prove every prose scenario in §17 has an exact test mapping; finish that mapping before release. |
| §18 Schema evolution | `SchemaTests` | **Partial:** metadata compatibility gate exists; runtime merge-policy dispatch and full schema migration behavior are not implemented. |

## 2. Normative contradictions corrected in v2.4.20

1. **Inbox identity:** the durable receipt key is exactly `(senderNodeId, senderEpoch, namespaceId, batchId)`. The resolved author stream is a separate receive cursor. The Storage adapter previously included author stream in the receipt key, which allowed the same protocol BatchId to occupy multiple receipt rows. New writes use the protocol key; the decoder accepts the prior 54-byte key layout for compatibility and normalizes it in memory.
2. **Peer progress:** the previous scalar `lastAckedSeq` description was insufficient for independent author sequence spaces. The spec now describes peer/session state separately from per-author outbound and receive cursors.
3. **Retention:** an ACK or TTL does not authorize deletion of receipts, ChangeId facts, relay history, Outbox payloads and tombstones as one coupled history. Each has its own proof horizon. Inbox receipt GC remains disabled because v2.4.20 has no explicit bilateral retention-checkpoint frame.
4. **Duplicate ACK:** the receive pseudocode now returns the stored receipt cursor and durable author, not replay-supplied values.
5. **Relay alias failure:** the old pseudocode silently skipped an operation with an unknown target alias. That contradicts contiguous author-stream cursors and the implementation's fail-closed behavior. The spec now rejects the whole batch with `UNKNOWN_ALIAS`.

These are semantic clarifications/hardening changes; they do not change the wire layout.

## 3. Remaining implementation/specification gaps

- **Receipt DTO parity:** the current Storage `MetadataBatchReceipt` does not itself carry the full protocol `headerFingerprint` and `durableAuthor` fields described by §10.2. Node's inbox mirror still supplies part of this information. The authoritative cutover must move the full duplicate-ACK/equivocation decision into durable Metadata Store state.
- **Receive authority:** Node currently validates and mutates its inbox/cursor/digest cache after Metadata Store staging. This is a transitional dual-authority arrangement, not final conformance to the Metadata Store ownership model.
- **Provider rehydration:** a fresh runtime must reconstruct user-visible state and pending recovery exclusively from the Data Provider plus Metadata Store; no stale NodePersistence copy may be required.
- **Crash proof:** deterministic fault injection is required before and after PendingApply commit, provider effect/receipt, Metadata Store finalize, ACK emission, and checkpoint/compaction. Ordinary restart tests do not cover all these cuts.
- **Retention:** no receipt or ChangeId GC implementation is approved until a protocol-level proof and durable checkpoint format exist. E-25 tombstone/ghost GC stays disabled.
- **Performance:** delta-only PendingApply finalization is implemented. Other `PersistPartition` mutations still clone/materialize the full partition. Benchmarks must report bytes written, allocations and p50/p95 latency at increasing metadata cardinalities.
- **Scenario index:** map each named §17 scenario and each §16 invariant ID to one or more exact tests. A test filename that broadly mentions a feature is not sufficient evidence by itself.

## 4. Release acceptance checklist

- [ ] One authoritative durable owner for receive receipts, cursors, digest checkpoints and ChangeId dedup.
- [ ] One authoritative durable owner for local sequence allocation, frozen Outbox and relay retention.
- [ ] Provider/Metadata Store restart reconstruction with no dependency on stale NodePersistence state.
- [ ] Deterministic crash matrix passes at every durability boundary.
- [ ] Per-domain retention proofs exist; unproven GC remains disabled.
- [ ] All §16 invariant IDs and §17 scenarios map to exact passing tests.
- [ ] Every partial feature in §§8, 14, 16.19 and 18 is explicitly labelled in API/docs.
- [ ] Current-head .NET, Storage Integration, sample, Fuzz and Lab CI all pass.
- [ ] Public API breaking changes and package version are reviewed before release.
