# BIN-SYNC GC Stage 4 — Endurance validation

## Purpose

Stage 4 is the final pre-production confidence test after the isolated Stage 0 lifecycle suite and the ChangeId-only, Receipt-only and Tombstone-only canaries. It repeatedly exercises the complete tested GC lifecycle on both Ubuntu and Windows, including a larger real FLD IPC workload, peer retirement/rejoin/replay and process recovery.

This is a disposable integration test. It is not a production deployment and does not authorize enabling GC against user data.

## Execution

Workflow: `.github/workflows/gc-endurance-stage4.yml`

- Matrix: `ubuntu-latest` and `windows-latest`.
- Duration: at least 90 minutes per OS.
- Minimum acceptance: six complete cycles on each OS; any failed test, hang, crash or missing artifact fails the job.
- Every cycle rebuilds no code but runs the Release test binaries against the exact FLD Storage submodule SHA pinned by the BIN-SYNC commit.
- Each underlying canary creates isolated disposable FLD/Metadata databases and Host processes. The cycle repeats the complete sequence rather than sharing one database between tests; this intentionally keeps failure attribution and cleanup deterministic.
- Every test is run with hang diagnostics and TRX/log artifacts are retained.

## Workload in every cycle

1. **Host lifecycle:** startup ordering, durable policy restore, operator authentication, maintenance scheduler, emergency stop, ungraceful process death, restart, offline backup/rollback brake.
2. **ChangeId-only:** live relay reference veto, cancellation, durable policy restart, real FLD IPC reclamation. Scale case: 50,000 candidates with page limit **4096**.
3. **Receipt-only:** Host WAL seal/reference scan, retired-epoch reclamation, replay fence after restart, and explicit proof that Receipt GC does not authorize physical Tombstone deletion. Page limit remains **256**.
4. **Tombstone-only:** signed safe horizon, exact candidate revalidation, snapshot/relay vetoes, approval spool, two-host peer ACK/retirement/rejoin/FULL_REPLACE/replay, no-resurrection and compaction crash cuts. Scale case: 50,000 candidates with page limit **256**.
5. All three stages are repeated in sequence for at least six complete cycles on each OS.

## Pass criteria

- Both OS jobs complete at least six full cycles within the 120-minute job limit.
- Every invoked test exits successfully; no test is skipped by the selected filters (the workflow's TRX artifacts are part of review).
- Each cycle emits `GC_ENDURANCE_CYCLE_PASSED`.
- The artifact contains all per-test logs/TRX files and a summary with cycle count, elapsed time and the pinned FLD revision.
- No unbounded page processing is introduced; configured page limits remain as stated.
- No false reclamation, resurrection, replay acceptance, lost durable stop, stale policy restore, Host lease bypass or crash-recovery divergence is observed.
- No production feature flag is changed by this workflow.

## What this test can and cannot establish

This test provides repeated cross-platform evidence for the implemented algorithms, disposable Host composition and lifecycle, durable policy behavior, FLD IPC path, peer-retirement/rejoin scenarios, and recovery after the crash cuts covered by existing canaries.

It cannot prove behavior with a future production transport, real application identity provider, independent operational anti-rollback authority, real-world network partitions, physical power loss, or an application Host that does not yet exist. The test Host uses a deliberately test-only TCP listener and operator authorizer. Those remaining items require the future application environment and must not be represented as covered by Stage 4.

## Release interpretation

A green Stage 4 on both OSes means the repository's available GC test program is complete to the extent possible without a production application. It is not production activation approval. Production flags remain default-off; Ghost GC remains disabled; Tombstone remains the last physical-reclamation domain.
