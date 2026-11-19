# Remaining Host/GC rollout gates

Status baseline: commit `3160744be9f7dc97502814af716e0e1988434b04` on `feature/fld-external-storage`.
This document tracks the remaining evidence needed after Stage 0–4 canaries and the two-process TCP lifecycle test.

## Non-negotiable invariants

- WAL remains recovery authority; FLD Outbox remains unpublished local changes; BIN-SYNC Metadata remains delivery/ACK/causality state.
- No separate Pending journal is introduced.
- Tombstones remain retained until the existing proof and reference checks authorize reclamation.
- Production GC and Ghost GC remain disabled until all gates below pass and a production Host exists.
- The standalone FldBinSyncHost.Runtime is a realistic test Host, not production-readiness evidence.
- GC must never hold a process monitor across an await; Host-side commit/reference leases must be acquired in the documented order.
- Snapshot application must be idempotent across process loss, preserve author/HLC/tombstone provenance, and must not manufacture ChangeIds or Outbox entries.

## Gate A — async snapshot reconciliation for populated namespaces

Current limitation: TestPeerRuntime rejects non-empty bootstrap. SyncSession.AutoReconcileAsync exposes a synchronous post-apply callback; the async FLD IPC provider exposes snapshot reads but no durable async snapshot-state-apply contract. Node.RestoreProviderSnapshot is an in-memory cache restore and is not a substitute for durable FLD apply.

Required implementation:
1. Add an async provider snapshot-state apply capability with a stable deterministic transfer identity, durable receipt, and atomic/staged commit semantics.
2. Implement the capability across the FLD IPC protocol and FLD Database Host. Do not route snapshot rows through DELTA_BATCH receipts or synthesize ChangeIds.
3. Add an async reconciliation callback path that awaits provider durability before marking/persisting the Node baseline. Preserve the existing synchronous API for legacy providers.
4. Make interrupted transfer recovery explicit: incomplete staging is invisible; committed receipt is replayable; retrying the same transfer is idempotent.
5. Support populated namespace bootstrap, including field tombstones and record lifecycle tombstones, with bounded pages and aggregate transfer limits.

Acceptance:
- Linux and Windows two-process tests start with divergent, non-empty databases on both sides.
- Verify values, tombstones, author, HLC, and lifecycle after bidirectional reconciliation.
- Kill either Host before staging, mid-transfer, after provider commit but before Node checkpoint, and after checkpoint but before baseline marker. Restart and retry without lost data, duplicate ChangeIds, resurrection, or false baseline completion.
- Reject malformed, oversized, incomplete, wrong-namespace, and replay-conflicting snapshots.

## Gate B — multi-peer and failure matrix

After Gate A:
- Exercise at least three peers with overlapping and disjoint writes, concurrent reconnects, duplicate sessions, and per-peer baselines.
- Inject TCP reset, half-close, delayed frames, lost ACK, process kill, and restart at each durable boundary.
- Verify one receive owner per session, no concurrent frame readers, durable peer epoch retirement only after replacement baseline commit, and no cross-peer ACK advancement.
- Reconcile after each failure and compare provider state and Node digest on every replica.

Acceptance:
- Deterministic Linux and Windows matrix; all expected failures fail closed.
- No lost writes, false initial-sync marker, duplicate publication, or tombstone resurrection.
- All peer sessions eventually converge after transport recovery.

## Gate C — GC concurrent with sync

Only after A and B:
- Remove the test Host's blanket GC pause during active peer sessions.
- Preserve writer-admission/reference-snapshot barriers and coordinator admission semantics.
- Exercise ChangeId, Receipt, and Tombstone maintenance while inbound/outbound batches, snapshot transfer, reconnect, and pending apply recovery are active.
- Keep Ghost GC disabled unless a separate proof gate is added.

Acceptance:
- Stress runs prove no GC of pending apply, unacknowledged immutable reservation, unpublished draft, active snapshot, or live record reference.
- Concurrent sync and maintenance make progress; no starvation/deadlock.
- Crash/restart during every GC commit boundary remains recoverable.
- Any unknown reference, lease loss, or incomplete proof blocks reclamation.

## Gate D — memory and endurance evidence

After A–C:
- Capture baseline and per-phase memory: FLD Host working set/private bytes, managed heap, GC heap, BIN-SYNC metadata bytes, WAL/outbox bytes, active snapshot staging, and per-peer queue/transfer memory.
- Measure after warm-up, during steady writes, during multi-peer sync, during snapshot recovery, during each GC stage, and after quiescence/compaction.
- Report peak, retained, and post-GC values separately; include object/record counts and throughput/latency so reduced memory is not achieved by stalled work.
- Repeat the 15-minute active endurance window on Linux and Windows, then a longer soak only if the short run is stable.

Acceptance:
- No unbounded growth under a bounded workload; retained memory returns to a documented envelope after quiescence/GC.
- No unexplained growth in Metadata, WAL, Outbox, snapshot staging, or per-peer queues.
- Publish raw artifacts and exact commit SHA with every report.

## Final release gate

Do not enable production ChangeId/Receipt/Tombstone GC or Ghost GC from test-host results alone. Require a production Host implementation, real identity lifecycle, supported transport failure evidence, operational stop/rollback, and a reviewed policy/proof configuration. Until then, all production GC switches remain off.
