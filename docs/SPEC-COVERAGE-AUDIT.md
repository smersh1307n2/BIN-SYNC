# BIN-SYNC CORE normative coverage audit

**Baseline:** BIN-SYNC CORE v2.4.20 FINAL  
**Branch:** `feature/fld-external-storage`  
**Status:** first-pass section and contradiction audit, refreshed against code/test head `aafff5c5e12c901c2da4bd49fbabc03158418794`: 648/648 Core, 31/31 Fuzz/Lab, 70.4% line coverage; Storage Integration 122/122 on Ubuntu/Windows, with the macOS run queued after the FileShare.None WAL lease change. This is not a claim that every normative sentence has a one-to-one executable test. The remaining scenario-level mapping is an explicit release gate.

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
| §10 Outbox / Inbox / peer progress | `WalTests`, `ChangeIdWireTests`, `RemoteBatchWalAtomicityTests`, Metadata Store conformance and Storage integration | **Split authority:** Storage-backed provider cutover freezes stable ChangeIds directly into Metadata Store and assigns outbound author sequences; the Node local-author Outbox is fenced and duplicate local payload rows are reclaimed. Storage-backed receive uses PendingApply + the durable ChangeId ledger for validation/dedup and does not write duplicate Node GlobalChangeLedger/RemoteBatch receipt state. Legacy non-Storage Node-owned transport and other-author relay Outbox state remain transitional. |
| §11 Membership / GC | `MembershipTests`, `GcNoticeTests`, `MetadataPeerStateStore` durable roster | Membership and peer fencing implemented in slices. Tombstone/ghost GC and Inbox/ChangeId GC remain disabled until their independent proofs exist. |
| §12 Wire format | codec, frame, stream binding, fragment and resource-limit tests | Implemented for currently supported frame types. Validate all frame type IDs, flags, stream IDs and exact payload boundaries as one table-driven suite. |
| §13 Sync / reconciliation / relay | `ReconcileTests`, `ReconcileExchangeTests`, `ReconcileFulfillTests`, `TcpSyncTests`, `InvariantsAndAdvanced.cs`, `StorageSyncCoordinatorTests` | Main reconciliation ladder implemented. Durable provider baseline, Metadata-Store-authoritative receive and incremental provider-feed publication are implemented for the opt-in Storage path. Legacy non-Storage transport migration, relay-state retirement and the complete crash/restart matrix remain partial. |
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
6. **Version label:** commit `da787f38d05c528ab3357d4feb646458dd021d89` explicitly advanced the specification title to v2.4.20 while adding the receipt-retention/per-author-stream contract. The stale `Версия` metadata and end-of-document v2.4.13 statement are now aligned to v2.4.20; `ERRATA v2.4.19` intentionally remains the version label of E-25, which was introduced in that revision.

These are semantic clarifications/hardening changes; they do not change the wire layout.

## 3. Remaining implementation/specification gaps

- **Receipt DTO parity:** the Storage-backed authoritative receive path now persists `DurableAuthor` and the canonical full-wire-header fingerprint in Metadata Store receipts/completed PendingApply state. The coordinator verifies both before ACK. The legacy Node-owned receive path retains its own inbox validation and WAL mirrors until that path is migrated or explicitly fenced.
- **Receive authority:** for Storage-backed authoritative receive, Metadata Store validates receipt identity, full header fingerprint, ChangeId equivocation, cursor and digest-chain continuity before Node mutation. Node receives a per-operation new/duplicate mask and does not read/write its global ChangeId ledger or append duplicate RemoteBatch WAL. The non-Storage legacy path remains Node-owned; do not describe the opt-in cutover as universal.
- **Provider rehydration:** explicit provider-authoritative startup/recovery now drains and freezes the provider feed, installs a stable provider snapshot, checkpoints Node and replays/finalizes pending intents. This mode is safe only when all local namespace mutations are captured by the provider feed; the default mixed-origin coordinator path remains distinct.
- **Crash proof:** on-disk Storage regressions now construct torn final WAL tails, corrupt complete WAL records, torn non-final segments, orphaned rotation temp files, and both sides of the snapshot-manifest publication boundary. These on-disk recovery-state tests passed 119/119 on Ubuntu, Windows and macOS in GitHub Actions run `42446095432`. They reconstruct crash layouts on disk; they are not actual power-loss or OS-level I/O fault injection. The coordinator-level process-kill/reopen matrix is still open at every boundary from PendingApply staging through provider effect, Metadata Store finalize, Node checkpoint and ACK emission; deterministic physical power-loss behavior also remains unproven.
- **Retention:** no receipt or ChangeId GC implementation is approved until a protocol-level proof and durable checkpoint format exist. E-25 tombstone/ghost GC stays disabled.
- **Performance:** current-format receive, outbound and peer-lifecycle mutations use row-level delta transactions; `PersistPartition` remains a legacy-format migration fallback. Matched-runner retry benchmarks exist, but feed materialization, operation encoding and changed-record copy-on-write still need profiling. Report bytes written, allocations and p50/p95 latency at increasing metadata cardinalities.
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


## 5. Executable §17 scenario index

The named baseline scenarios 1–25 map directly to executable test methods. Extended scenarios 26–42 are additional regressions; they do not replace the baseline.

| Spec scenario | Exact test method(s) |
|---|---|
| 1. Field parallelism | `ScenariosA.Scenario01_FieldParallelism` |
| 2. Delete vs late write | `ScenariosA.Scenario02_DeleteVsLateWrite` |
| 3. Reconnect with new alias map | `ScenariosA.Scenario03_ReconnectNewAliasMap` |
| 4. Snapshot plus tail | `ScenariosA.Scenario04_SnapshotPlusTail`; `LabEdgeTests.S4_Snapshot_plus_tail_brings_new_peer_current`; `S4_NewPeerTailCursor_DoesNotRenumberExistingPeerOutbox` |
| 5. Counter with departed node | `ScenariosA.Scenario05_CounterWithGoneNode` |
| 6. Clock skew | `ScenariosA.Scenario06_ClockSkew` |
| 7. Epoch regression | `ScenariosA.Scenario07_EpochRegression` |
| 8. Canonical digest | `ScenariosA.Scenario08_CanonicalDigest`; `Hardening.Blake3_MatchesOfficialTestVectors` |
| 9. Incremental bucket digest | `ScenariosA.Scenario09_BucketIncremental`; `ScenariosB.Scenario41_BucketCommutativityPermutations` |
| 10. Tombstone GC prevention | `ScenariosA.Scenario10_TombstoneGcPrevention`; `LabEdgeTests.S13_PurgeBelowCutoff_preserves_live_fields_until_safe_watermark_gate` |
| 11. Blind encrypted merge | `ScenariosA.Scenario11_BlindMerge` |
| 12. Fragmentation | `ScenariosA.Scenario12_Fragmentation`; `LabEdgeTests.S12_Fragment_split_and_reassemble_roundtrip` |
| 13. Zombie quarantine | `ScenariosA.Scenario13_ZombieQuarantine`; `LabEdgeTests.S13_GcNotice_quarantine_propagates` |
| 14. Decompression bomb guard | `ScenariosA.Scenario14_DecompressionBombGuard` |
| 15. Flow-control backpressure | `ScenariosA.Scenario15_FlowControlBackpressure` |
| 16. Key rotation | `ScenariosA.Scenario16_KeyRotation` |
| 17. Range cursor coalescing | `ScenariosA.Scenario17_RangeCursorCoalescing` |
| 18. Lost ACK | `ScenariosA.Scenario18_LostAck` |
| 19. Manual conflict resolution | `ScenariosA.Scenario19_ManualConflictResolve` |
| 20. Multi-namespace isolation | `ScenariosA.Scenario20_MultiNamespaceIsolation` |
| 21. Bucket rollup | `ScenariosB.Scenario21_BucketRollup` |
| 22. TOFU | `ScenariosB.Scenario22_TrustOnFirstUse` |
| 23. Relay preserves authorship | `ScenariosB.Scenario23_RelayPreservesAuthorship`; `Scenario27_DigestConsistencyThroughRelay`; `Scenario28_RelaySeparateBatchesPerAuthor` |
| 24. Unknown alias atomic rollback | `ScenariosB.Scenario24_UnknownAliasAtomicRollback` |
| 25. Concurrent delete by author | `ScenariosB.Scenario25_ConcurrentDeleteByAuthor` |

Extended scenario methods: `ScenariosB.Scenario26_CorruptedFrameRecovery` through `Scenario42_ThreeNodeChaos`, with the individual names retained in the source test file. Latest verified Core/Fuzz run at code/test head `aafff5c5e12c901c2da4bd49fbabc03158418794`: 648/648 Core, 31/31 Fuzz/Lab and 70.4% line coverage. Latest Storage Integration run at the same head: 122/122 on Ubuntu and Windows; macOS is queued after the FileShare.None WAL lease change.

**Interpretation guard:** a scenario test proves only the assertions inside that method. In particular, scenario 10 proves fail-closed behavior, not that destructive tombstone GC is implemented; scenario 13 proves quarantine behavior, not that a complete safe-frontier GC engine exists.

**Durable peer-lifecycle and writer-fence regressions:** `StorageMetadataStoreConformanceTests.Peer_roster_baseline_and_epoch_retirement_commit_as_atomic_deltas` verifies that peer registration, initial-sync baseline publication and old-epoch retirement each commit as one Storage transaction, that repeated idempotent calls do not advance the commit sequence, and that roster/stream/retired-epoch state survives reopening. The WAL writer fence now uses a persistent `FileShare.None` lease and has same-process, cross-process, abrupt owner-process-kill, cross-thread disposal and path-alias regressions. This is not a substitute for the remaining coordinator crash matrix or protocol-authority cutover.

## 6. §16 invariant mapping status

The index below maps normative invariant families to exact executable methods where direct evidence exists. Each method supports only the behavior asserted by its own body; related scenario names are not treated as proof of every sentence in a family.

| Normative IDs | Exact executable test evidence | Remaining gap |
|---|---|---|
| [С1]–[С4] merge algebra | MergeCrdtTests.CounterG_merge_componentwise; CounterPn_merge_componentwise; SetOr_merge_unions_adds_and_removes; SetOr_merge_rejects_same_add_dot_for_different_elements_across_replicas; ScenariosB.Scenario41_BucketCommutativityPermutations | Helper/CRDT-specific evidence exists. No property suite proves all four laws for every production Node merge/lifecycle state. [С3] associativity and [С4] monotonicity need direct property coverage. |
| [У1]–[У7] delete/lifecycle | MergeFieldTombTests.Newer_field_tombstone_rejects_older_write; Newer_write_supersedes_and_removes_field_tombstone; Older_tombstone_does_not_replace_newer_value_or_remain_in_digest_state; ScenariosA.Scenario02_DeleteVsLateWrite; ScenariosB.Scenario25_ConcurrentDeleteByAuthor; Scenario38_FullLifecycleCycle; LabEdgeTests.S13_PurgeBelowCutoff_preserves_live_fields_until_safe_watermark_gate | [У1] and core delete ordering have direct evidence. Isolated ghost-author comparison [У2]/[У7], ordinary-write lifecycle preservation [У3], empty-vs-deleted [У5], and stale-peer rejection after actual reclamation [У6] are not all independently asserted. GC remains disabled/fail-closed. |
| [И1]–[И6] durable identity / aliases | ChangeIdWireTests.ChangeIdentity_fingerprint_is_alias_independent_but_covers_all_semantic_inputs; ChangeIdentity_direct_encoding_matches_canonical_reference_for_all_operation_kinds; Global_ChangeId_dedup_survives_origin_outbox_compaction_and_second_route; Provider_snapshot_restore_clears_pending_remote_ledger_but_preserves_local_identity; ScenariosB.Scenario23_RelayPreservesAuthorship; Scenario24_UnknownAliasAtomicRollback; Scenario28_RelaySeparateBatchesPerAuthor; AliasAtomicityTests.Resolve_alias_fails_closed_while_peer_metadata_is_locked | Strong direct coverage for route-independent identity, local-vs-remote ledger provenance during provider-cache restore, unknown aliases and relay authorship. [И6] cover identity is evidenced indirectly; add a focused test if the spec requires an explicit author-plus-sequence cover object. The provider rehydration lifecycle is now wired through `StorageSyncCoordinator.RehydrateNodeFromProviderSnapshot`; continue verifying local-vs-remote identity provenance across reopen and recovery. |
| [Д1]–[Д8] digest family | ScenariosA.Scenario08_CanonicalDigest; Scenario09_BucketIncremental; ScenariosB.Scenario27_DigestConsistencyThroughRelay; Scenario41_BucketCommutativityPermutations; MergeFieldTombTests.Record_digest_distinguishes_empty_value_from_tombstone_with_same_metadata; ChangeIdWireTests.ChangeIdentity_fingerprint_is_alias_independent_but_covers_all_semantic_inputs; E2eeTests.Blind_merge_sees_only_ciphertext_bytes | Canonical digest, bucket behavior, route/author identity and ciphertext behavior have direct evidence. Separate tests for every D2/D7/D8 boundary (version-only changes and lifecycle-author-only changes) remain missing. |
| [П1]–[П7] sequence / duplicate / chain | InvariantsAndAdvanced.OutboxStreamOrderingAndCursorMonotonicity; Hardening.Persistence_SequencesContinueAfterRestart; Persistence_StreamOffsetSurvivesRestartAndMapsNextAck; ChangeIdWireTests.Duplicate_batch_id_uses_receipt_header_without_redecoding_payload; Ack_cannot_advance_beyond_sent_stream_or_accept_another_author; Ack_watermark_and_pushed_stream_survive_restart_and_duplicate_ack_is_idempotent; ScenariosA.Scenario18_LostAck; ScenariosB.Scenario30_BatchChainIntegrity | Strong coverage for restart, per-author ACK bounds, duplicate ACK and digest-chain behavior. Concurrent sequence allocation and failed-commit non-reuse remain narrower than [П1]/[П5]. |
| [СМ1]–[СМ7] retention / GC | Hardening.CompactOutbox_UsesMinimumPeerAckAndFailsClosedForMissingPeerState; GcNoticeTests.PurgeBelowCutoff_does_not_delete_live_values_or_tombstones_without_safe_watermark; ScenariosA.Scenario10_TombstoneGcPrevention; ScenariosB.Scenario35_ZombieReplicaWatermark; LabEdgeTests.S13_PurgeBelowCutoff_preserves_live_fields_until_safe_watermark_gate | Tests prove fail-closed retention and minimum-peer-ACK fencing, not a complete safe-frontier/retirement/TTL engine. [СМ1]–[СМ7] are not fully implemented; destructive GC is intentionally disabled. |
| [СХ1]–[СХ7] schema | SchemaTests.Field_id_reuse_after_retire_throws; Field_id_cannot_be_reintroduced_after_disappearing_from_a_later_version; Policy_mismatch_detected; ApplyBatchWithSchema_pending_throws; SchemaMap_decode_rejects_impossible_counts_and_trailing_data; ScenariosB.Scenario32_SchemaPending | Version gates, policy mismatch and field-ID retirement have direct tests. Runtime merge-policy dispatch remains partial; raw preservation of unknown fields and all type/value compatibility rules are not proven. |
| [К1]–[К11] canonical encoding | CodecExtendedTests.Determinism_C1_same_input_same_bytes; Overlong_varint_rejected; F64_nan_rejected; F64_roundtrip_and_neg_zero; CanonicalizeFields_sorts_and_rejects_dup; ScenariosB.Scenario40_VarintCanonicalEdges; InvariantsAndAdvanced.OriginFixedOffsetC11; FrameHeaderExactSizeNoPadding | Direct evidence exists for deterministic bytes, canonical varints/floats, field ordering and [К11]. Not every aggregate count/length/fixed-layout clause has a dedicated boundary test. |
| [Т1]–[Т3] transaction / batch chain | ResourceLimitsAtomicityTests.Apply_batch_rejects_record_limit_for_whole_batch_atomically; Apply_batch_rejects_mixed_author_stream_before_any_commit; Apply_batch_rejects_duplicate_nonzero_change_ids_before_commit; ChangeIdWireTests.Provider_snapshot_restore_rejects_duplicate_and_reserved_rows_atomically; ScenariosB.Scenario39_MultiFieldAtomicBatch; Scenario30_BatchChainIntegrity; ScenariosA.Scenario18_LostAck; StorageSyncCoordinatorTests.Provider_failure_after_node_apply_keeps_pending_intent_for_recovery; Provider_commit_before_metadata_finalize_is_recovered_idempotently; Metadata_store_owns_receive_cursor_and_digest_after_node_recreation; Lost_ack_after_metadata_finalize_replays_as_durable_duplicate; StorageMetadataStoreConformanceTests.Durable_receipt_rejects_full_header_equivocation_after_reopen | Node batch atomicity and digest-chain ordering are covered. Cross-store receive cuts include PendingApply staged before Node apply, Node checkpoint failure with Storage reopen, provider-not-committed after Node apply, provider-committed before metadata finalization with Storage reopen and idempotent retry, Node recreation with empty local receive cursors/digest, lost-ACK replay after finalization and full-header equivocation rejection after reopen. On-disk WAL/snapshot crash-state tests cover torn tails, CRC corruption, non-final segment rejection, rotation temp files and both sides of manifest publication. Remaining gaps: process-kill/reopen at every participant boundary, actual power-loss/I/O fault injection and all multi-route relay crash cuts. |
| Trust [ДОВ1]–[ДОВ4] (§16.10) | TrustStoreTests.Tofu_pins_first_key_only_after_valid_signature; Tofu_rejects_key_substitution_after_verified_first_use; Tofu_invalid_signature_does_not_poison_peer_identity; PreShared_requires_explicit_allow; Revoke_blocks_verify; ScenariosB.Scenario22_TrustOnFirstUse; Scenario31_HelloAuth | TOFU, pre-shared allow-list and revoke paths have direct tests. CA mode is an allow-list, not full X.509 validation. |
| [А1]–[А6] authorship | ChangeIdWireTests.ChangeIdentity_fingerprint_is_alias_independent_but_covers_all_semantic_inputs; Global_ChangeId_reuse_with_different_operation_is_rejected_atomically; ScenariosB.Scenario23_RelayPreservesAuthorship; Scenario24_UnknownAliasAtomicRollback; Scenario28_RelaySeparateBatchesPerAuthor | Author preservation and alias rejection are directly covered. [А6] nonce binding is mapped under E2EE below. |
| [Ц1]–[Ц8] frame integrity | ScenariosB.Scenario26_CorruptedFrameRecovery; SessionStreamBindingTests.Delta_batch_rejects_wrong_namespace_stream_before_apply; Snapshot_rejects_chunk_on_wrong_namespace_stream_before_apply; TcpFragmentTests.Fragment_decoder_rejects_trailing_bytes; Reassembler_rejects_conflicting_duplicate_fragment_and_releases_slot; TransportFoundation.Fragmentation_rejects_logical_payload_and_sequence_overflow | Frame/stream/fragment bounds and atomic reassembly have direct evidence. A full per-code matrix for magic, CRC, every fragment corruption position and post-error session state is not indexed. |
| [Ж1]–[Ж4] lifecycle authorship | ScenariosB.Scenario25_ConcurrentDeleteByAuthor; Scenario38_FullLifecycleCycle; SnapshotTests.Snapshot_roundtrip_and_apply_preserve_field_and_record_tombstones | Lifecycle conflict and snapshot persistence are covered. Isolated lifecycle-author-only digest changes and every ghost comparison edge remain desirable. |
| [СН1]–[СН4] snapshot binding | SnapshotTests.Snapshot_chunks_cover_the_complete_record_set; ApplySnapshotChunks_is_atomic_when_a_late_record_is_invalid; ApplySnapshotChunks_rejects_incomplete_or_reordered_transfer_without_mutation; Snapshot_transfer_disconnect_mid_stream_is_atomic_and_full_retry_succeeds; LabEdgeTests.S4_Snapshot_plus_tail_brings_new_peer_current | Completeness, atomic apply, order and retry are directly covered. [СН3] safe outbound checkpoint digest binding is not equivalent to ordinary snapshot success and still needs explicit proof. |
| [Р1]–[Р4] relay | ScenariosB.Scenario23_RelayPreservesAuthorship; Scenario27_DigestConsistencyThroughRelay; Scenario28_RelaySeparateBatchesPerAuthor; InvariantsAndAdvanced.RewriteOrigin_KeepsWireRoundTripConsistent; ChangeIdWireTests.Change_id_survives_A_to_B_to_C_to_A_and_duplicate_arrival_from_second_peer | Original author preservation, relay chain and author-separated batches have direct evidence. Multi-route crash/restart with multiple author streams remains open. |
| [УП1]–[УП2] flow control | TransportFoundation.Negotiated_flow_control_replenishes_credit_during_fragment_reassembly; FlowControl_rejects_replayed_or_reordered_window_update_sequences; Flow_control_peer_disconnect_wakes_sender_waiting_for_credit; Flow_control_rejects_unsolicited_window_update_credit; ScenariosA.Scenario15_FlowControlBackpressure | Negotiated credit, replay/order rejection, disconnect and unsolicited credit have direct evidence. |
| [РП1]–[РП2] field resolution | ScenariosA.Scenario19_ManualConflictResolve; MergeCrdtTests.Manual_does_not_overwrite_divergent_value; Manual_advances_metadata_when_value_identical; InvariantsAndAdvanced.Public_field_mutations_reject_reserved_system_field_ids | Manual conflict behavior and reserved-field protection are covered. Strictly-greater resolution time and field-255 digest inclusion need dedicated assertions. |
| [Ш1]–[Ш2] encryption / echo | E2eeTests.Nonce_is_12_bytes_and_deterministic; Nonce_differs_when_any_context_changes_E1; Encrypt_decrypt_roundtrip; Tampered_ciphertext_fails_auth; E2eeWriteTests.WriteFieldSealed_wal_failure_does_not_publish_storage_or_outbox; ScenariosB.Scenario37_RingEchoStop; InvariantsAndAdvanced.ReplayStormIsIdempotent | Nonce context, AEAD integrity, WAL atomicity and echo suppression are covered. Exhaustive nonce uniqueness under every replay/restart mode remains open. |
| [КЗ1]–[КЗ5] causal (experimental) | CausalDepsTests.Timeout_drops_pending_KZ1; Cycle_detected_KZ2; MarkLost_drops_dependents_KZ3; Batch_all_or_nothing_KZ5; CausalApplyTests.Missing_dep_throws_CAUSAL_DEP_MISSING_and_quarantines_KZ4; Duplicate_batch_bypasses_causal_gate_and_does_not_trust_replay_change_ids; CausalWireTests.Trailer_parser_is_atomic_when_a_later_entry_is_truncated | Causal helper, batch gate and wire trailer behavior have direct tests. The tracker remains process-memory state and is not in Node WAL/snapshot; this is an explicit experimental limitation. |

**Normative ID namespace:** §16.10 trust invariants now use the distinct [ДОВ1]–[ДОВ4] prefix; [Д1]–[Д8] remain reserved for digest invariants in §16.4.

**Unmapped or only indirectly evidenced requirements:** [С3]/[С4] general merge algebra, [У2]/[У3]/[У5]/[У7] isolated lifecycle/ghost edges, [Д2]/[Д7]/[Д8] dedicated digest boundary cases, [СМ1]–[СМ7] full destructive-retention protocol, [СХ2]/[СХ4]/[СХ6] raw unknown-field/type rules, [СН3] snapshot-to-outbound-checkpoint binding, [РП1]/[РП2] exact field-resolution edges, exhaustive [Ц1]–[Ц8] corruption-position matrix, and E2EE nonce uniqueness across all restart/replay modes. These are release gaps, not inferred passes.

The methods above provide evidence only for their actual assertions. They do not convert helper-only CRDT APIs into runtime merge-policy support, do not prove destructive GC is implemented, and do not replace deterministic cross-store crash/restart tests.The test families above are evidence sources, not a claim that every invariant has a unique positive and negative test. The highest-risk unmapped families remain retention/GC proof, remaining crash cuts across separate stores, E2EE nonce uniqueness under all replay modes, and runtime schema-policy dispatch.


## Provider-authoritative startup recovery (2026-11-08)

The explicit `StorageSyncCoordinator.RehydrateNodeFromProviderSnapshot` path now has integration evidence:

- `StorageSyncCoordinatorTests.Provider_authoritative_rehydration_replaces_node_cache_and_freezes_local_feed` verifies that the provider snapshot replaces stale Node namespace data, while provider-originated local changes are first frozen into the durable outbound Metadata Store.
- `StorageSyncCoordinatorTests.Pending_receive_intent_replays_wire_batch_into_node_and_provider_after_interruption` now enters through provider-authoritative rehydration and verifies that a durable PendingApply wire intent is replayed only after the provider-derived base is installed, then finalized in Metadata Store.
- `StorageSyncCoordinatorTests.Provider_authoritative_recovery_processes_only_its_namespace_pending_intents` verifies that coordinators recover only their namespace from a shared Metadata Store scope, foreign namespace intents do not block rehydration, and a current-namespace legacy intent without wire payload causes fail-closed behavior until recovered through `RecoverPendingApplies`.

These tests establish the ordering and idempotent recovery behavior at the coordinator API boundary. They do **not** prove a complete process-kill/reopen matrix for every NodePersistence/WAL boundary, nor do they authorize this mode when local Node writes bypass the provider feed. The default merge-based coordinator path remains distinct for mixed-origin workloads. Full Node/Transport authority cutover and legacy-path retirement remain release gates.


## Durable provider feed identity (2026-11-08)

- `StorageDataProviderConformanceTests.Local_feed_change_identity_survives_reopen_and_is_removed_on_ack` verifies that a provider local-feed ChangeId survives process reopen and is removed together with the feed row after acknowledgement.
- `StorageDataProviderConformanceTests.Legacy_local_feed_row_gets_stable_identity_on_first_read` verifies one-time in-place migration of pre-identity feed rows: the first read appends a durable identity atomically with the read watermark, and repeated reads return the same identity.

These are storage-provider conformance tests, not additional BIN-SYNC protocol invariants. The identity remains transient with the unpublished local feed; it is not a second long-term change-history table.


## Incremental outbound freeze retry (2026-11-08)

- `MetadataStoreConformanceSuite.Outbound_freeze_exact_and_partial_retries_preserve_existing_sequences` verifies that a mixed retry returns original sequences for already-frozen ChangeIds, allocates new sequences only for unseen IDs, remains idempotent on replay, and rejects conflicting fingerprint/payload reuse without advancing the high watermark.
- `StorageMetadataStoreConformanceTests.Partial_outbound_freeze_retry_after_reopen_preserves_existing_sequences` verifies the same behavior across a durable Metadata Store reopen.

This is the required idempotency boundary for replaying a durable provider feed page when additional local changes arrive after freeze but before provider-feed ACK.
