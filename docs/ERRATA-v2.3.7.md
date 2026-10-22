# BIN-SYNC CORE — Normative Errata v2.3.7

**Status:** Normative for implementors of **BYN-SYNC-SPECIFICATION.txt v2.3.6**.  
**Date:** 2026-10-22  
**Source of truth in code:** comments marked `SPEC CORRECTION (errata E-n)` / `Errata E-n` in `BinSync.Net` (port of the Python reference).

These amendments supersede the corresponding prose in v2.3.6 where they conflict.  
Lab scenarios that already encode the corrected behaviour (§17.23, §17.37, scenario 23, etc.) remain valid.

---

## E-1 — Durable authorship is bare `nodeId` (§4.3 / §4.6 / §5.4)

- The durable author of an op is the **16-byte bare `nodeId`**, never `(nodeId, epoch)` and never a session alias.
- Tie-break on LWW / lifecycle always uses bare `authorNodeId`.
- The `epoch` argument to alias lookup is retained for session table construction only; it must not change which durable author is stored with the op.

## E-2 — Epoch watermarks are session-keyed (§4.4 / §12.17)

- Known-epoch watermarks are keyed by **`(senderPeer, namespaceId)`** (the session stream), not by the author of individual ops.
- Commit path only **raises** the watermark for batches that pass every receive gate.
- A new session (new HELLO) resets the session-scoped epoch view for that peer.

## E-3 — Receive cursor is session-stream keyed (§10.4)

- The receive cursor is keyed by **`(senderPeer, senderEpoch, namespaceId, authorStream)`**, where `authorStream` is the durable author of the batch (see E-4).
- Idempotent re-delivery of the same `batchId` must not advance the cursor twice.

## E-4 — Push watermarks and ACK name the author stream (§10.2.2 / §13.6.1 [R3])

- Push / ACK watermarks are keyed by the **author stream** (durable `authorNodeId` of the batch), not only by the transport peer.
- Wire ACK carries the acknowledged author stream so the sender can advance the correct outbox.
- On the receiver, batch entries may temporarily hold the issuer’s `nodeId` as a placeholder; the durable author is resolved from `op.origin` via `ALIAS_MAP` at apply time and cached on the batch (`AuthorStreamNodeId`).
- Gap normalisation (renumber un-acked outbox seq) applies on the explicit-filter path as well as the default path.

## E-5 — Alias collision is fail-closed (§4.3)

- If two different `(nodeId, epoch)` bindings compete for the same session alias, resolution returns **unknown** (`UNKNOWN_ALIAS`). Implementations must **not** guess.
- First successful registration of an alias wins; later conflicting registrations fail closed.

## E-6 — Delta-batch author selection for multi-author outbox (§13.6.1)

- When building a delta for a peer, the stream key is the **selected author stream**, not a stale loop variable from a previous candidate.
- One batch carries ops of a single durable author stream; mixing authors in one `DELTA_BATCH` is forbidden.

## E-7 — Relay preserves payload verbatim (§13.6.2 [R4] / [И4])

- A relay node may re-stamp the wire `origin` alias for the next hop, but the **op payload bytes stay verbatim**.
- Durable `authorNodeId` in local storage is never rewritten to the relay’s id.

## E-8 — Relay eligibility is provenance-based, not merge-outcome (§8.3 [E2] / §17.23)

- An op is eligible for relay only if this node **first observed** it on a given channel (see E-10 seen-set).
- Winning or losing the local LWW merge does **not** by itself decide relay.
- A node must not re-enqueue its own state when a ring path returns that state (echo of self).

## E-9 — Durable author registry for session tables (§4.3)

- Every `(nodeId, epoch)` learned through any `ALIAS_MAP` is retained in a **durable author registry** (survives `reset_session`).
- The registry is used **only** when constructing new session tables, never as a substitute for live alias resolution [А3].

## E-10 — Provenance seen-set and incremental `ALIAS_MAP` (§12.7 / §8.3)

- Provenance set is keyed by **`(senderPeer, namespaceId, changeId)`** (channel-scoped).
- First sighting decides relay eligibility (E-8).
- Third-party bindings learned in one session are propagated to other live sessions via **incremental `ALIAS_MAP`** updates; when both sides already hold a binding, keep the existing numbers (no silent remap).

## E-11 — Symmetric session tables and author-stream cache (§4.3)

- Both ends of a session must hold **identical** alias tables for the shared binding set after HELLO / `ALIAS_MAP` (and after any incremental update).
- Per-`(senderPeer, ns)` cache of the last accepted durable author stream is allowed as an optimisation; collisions remain fail-closed (E-5).
- Before failing closed on a missing third-party alias, attempt incremental `ALIAS_MAP` negotiation when the peer already knows the binding.

## E-12 — Chain digests and post-restart alias recovery (§12.17.2)

- `prevBatchDigest` chain is keyed by **`(issuer, namespaceId, epoch) + targetPeer`** (per stream).
- After a restart, if a wire alias is missing locally, the node may **consult the peer’s copy of this session** and adopt its binding for the same `nodeId` before rejecting with `UNKNOWN_ALIAS`.

## E-13 — Avalanche / restored-echo suppression (§8.3 [E2] / §17.37)

- Do not re-propagate an op that is only a mirror of a change already present in **our own un-ACKed outbox** as a byte-equal winner for the same `(recordId, fieldId)` (restored echo).
- Register our own `changeId` under our own channel key so a later ring delivery is recognised as echo.
- Suppress only when our own queued rival is the one being mirrored; do not suppress legitimate multi-path delivery of a foreign winner.

---

## Mapping to lab scenarios

| Errata | Primary scenarios |
|--------|-------------------|
| E-1, E-5 | alias / authorship tests |
| E-2, E-3, E-4 | cursor / ACK / multi-author outbox |
| E-6–E-8, E-10 | relay / mesh / scenario 23 |
| E-9, E-11 | durable authors + session symmetry |
| E-12 | persistence resume + chain |
| E-13 | scenario 37 restored echo |

---

## Implementation checklist

- [x] BinSync.Net engine comments and behaviour aligned with E-1…E-13  
- [x] Unit / production tests cover the scenarios above  
- [ ] Full merge of this text into the Russian normative body of `BYN-SYNC-SPECIFICATION.txt` (editorial pass; no behaviour change)  
- [ ] Bump printed version line to **2.3.7** once the body merge lands  
