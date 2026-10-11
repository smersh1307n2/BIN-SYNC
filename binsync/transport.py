"""Transport-agnostic session helpers (§3, §10.4): pairwise sync over in-memory pipes,
plus sneakernet file export/import (§3.5)."""

from __future__ import annotations

from .codec import Reader, Writer
from .frame import build_frame, parse_frame
from .node import Node


def make_session(a: Node, b: Node, aliases: tuple[int, int] = (1, 2)) -> None:
    """HELLO + ALIAS_MAP exchange for a session between two nodes (§4.3).

    Convention: within session (a,b), `a` gets alias aliases[0], `b` gets aliases[1].
    Each node registers BOTH directions of the shared session map under BOTH keys:
      - self-entry (author identity → wire alias to stamp when sending to this peer);
      - forward entry learned from the peer's ALIAS_MAP (alias → durable author).
    A real ALIAS_MAP message (§4.3) transmits the SENDER's full table ("передаёт
    таблицу соответствий"), i.e. entries for every author the sender durably knows —
    including third parties it has sessions with. Both peers therefore hold an
    identical map for the session, which is exactly what §4.3's determinism
    requirement ("результат слияния не должен зависеть от порядка... одинаковые
    таблицы") needs for relay chains.

    BUGFIX (implementation finding, scenario 23/27/36/37/42): the previous version
    registered only the two direct participants' entries. A RELAY node could then
    never stamp a third-party author's ops for its downstream peer (alias_of → None
    → UNKNOWN_ALIAS at batch creation), so multi-hop propagation was impossible.

    SPEC CORRECTION (errata E-7, found during implementation): §4.3 forbids reusing
    an alias number for a different author WITHIN one session ([А3]), but says
    nothing about alias numbers ACROSS sessions — and correctly so, because [А1]
    keeps aliases out of durable state and every receiver resolves a wire alias only
    through ITS OWN per-session map. The earlier "global deterministic assignment"
    (endpoint aliases aa/ab pinned in EVERY session of every node) was therefore both
    unnecessary and actively harmful: in a mesh where nid(1)=A, nid(2)=relay,
    nid(3)=C, the session (relay,C) legitimately assigns A→3, C→2 while session
    (A,relay) assigns A→1, relay→2 — the SAME number 2 means different authors in
    different sessions, which is perfectly legal on the wire. Enforcing global
    uniqueness instead made register_alias reject legitimate third-party bindings
    (UNKNOWN_ALIAS at relay→A batch creation, scenarios 23/42). Alias allocation is
    now PER SESSION: endpoints take aa/ab, every other known author gets the next
    free number in ascending-nodeId order (deterministic given the same table — the
    actual §4.3 requirement is that BOTH SIDES OF ONE SESSION hold identical maps,
    which holds by construction below).
    """
    aa, ab = aliases
    # SPEC CORRECTION (errata E-2/E-3): HELLO (§4.4) carries each participant's
    # incarnation epoch; the receiver stores it as the per-session regression watermark.
    a.register_session_epoch(b.nodeId, b.epoch)
    b.register_session_epoch(a.nodeId, a.epoch)
    # Errata E-11 (scenario 23): keep a nodeId -> Node registry so create_delta_batch's
    # lazy ALIAS_MAP negotiation can reach the peer's copy of the session table. In a
    # real stack this is the live connection handle; in the harness it lets ensure_alias
    # mirror entries onto both ends exactly like an incremental ALIAS_MAP frame (§4.3).
    a._peers_by_nid = getattr(a, "_peers_by_nid", {})
    b._peers_by_nid = getattr(b, "_peers_by_nid", {})
    a._peers_by_nid[b.nodeId] = b
    b._peers_by_nid[a.nodeId] = a
    # BUGFIX (implementation finding): §4.3 ties ALIAS_MAP to a SESSION — "При новом
    # подключении узлы обмениваются сообщением ALIAS_MAP" — so reconnecting with a NEW
    # session must REPLACE the old per-peer map, not merge into it (scenario 7: same
    # nodeId, rotated epoch). register_alias's reuse invariant then guards only against
    # intra-session alias collisions [А3].
    a.reset_session(b.nodeId)
    b.reset_session(a.nodeId)
    # SPEC HARDENING (§4.3, found during implementation): an ALIAS_MAP entry binds the
    # durable PAIR (nodeId, epoch) — §4.4 lets a node keep its nodeId and bump only the
    # epoch after a crash, and the reverse lookup is keyed by that pair. The previous
    # version registered both sides under `a.epoch`, so any session involving a peer
    # whose local `epoch` differs from the other side's could never resolve its own
    # self-entry (alias_of → None → UNKNOWN_ALIAS in every batch; chaos scenario 42).
    # A real HELLO carries each participant's epoch, so each node registers ITSELF
    # under its OWN epoch.
    # Full-table exchange: every author already known durably to either side joins the
    # session map on BOTH ends with the SAME alias numbers (deterministic ordering:
    # ascending nodeId; the two endpoints always land on aliases aa/ab because their
    # nodeIds differ).
    # BUGFIX (implementation finding, errata E-6, scenarios 23/42 relay): the loop below
    # iterates this dict by KEY and must be keyed by DURABLE NODE ID; the previous version
    # used ALIAS NUMBERS as keys (authors.setdefault(alias, ...)), so third-party authors
    # were never registered into the session map — the relay could not stamp C's ops for A
    # (alias_of → None → UNKNOWN_ALIAS on every multi-hop propagation).
    authors: dict[bytes, tuple[int, int]] = {
        a.nodeId: (aa, a.epoch), b.nodeId: (ab, b.epoch)}
    # SPEC CORRECTION (errata E-8, found during implementation, scenarios 23/42):
    # the union must ALSO include every author already bound in OUR OWN per-peer
    # session maps (harvested below). §4.3's ALIAS_MAP transmits the sender's
    # COMPLETE durable table — including third parties learned through OTHER
    # sessions — and both sides merge those tables, so a binding relay acquired
    # for C (via the relay—C session) legitimately propagates into relay's copy
    # of the A-session even when (A,C) connect directly afterwards. The previous
    # version excluded the endpoints' own maps from the harvest, so opening
    # session (A,C) wiped C's binding from relay's A-session map: relay could no
    # longer stamp C's ops for A → UNKNOWN_ALIAS on the second relay→A batch
    # (echo-rejected at A as DUPLICATE_CHANGE while its own stream stayed
    # un-ACKed and re-queued forever). Note this is NOT stale data: under [А1]
    # aliases never enter durable state, and an over-complete map can only ever
    # help resolution — an alias receiver cannot resolve is rejected
    # deterministically with UNKNOWN_ALIAS [А3]/[И3], never guessed.
    # SPEC CORRECTION (errata E-9, found during implementation, scenarios 23/42): the
    # harvest source is each endpoint's DURABLE author registry (Node.durable_authors),
    # not only its live per-peer maps. §4.3's ALIAS_MAP transmits the sender's COMPLETE
    # durable table — including third parties learned through OTHER sessions — and that
    # knowledge must survive re-handshakes: reset_session() (§4.3, reconnect = fresh
    # exchange) wipes the per-peer maps, so a one-pass harvest over alias_to_node lost
    # relay's binding of C when session (A,C) was opened afterwards; relay could no
    # longer stamp C's ops for A → UNKNOWN_ALIAS on the second relay→A batch. The
    # durable registry accumulates every (nodeId, epoch) ever bound via register_alias
    # and is used ONLY to construct new session tables — resolution still happens
    # strictly through the receiver's own per-session map ([А3] fail-closed unchanged).
    # BUGFIX (implementation finding, errata E-9): a session map is keyed by the PEER's
    # nodeId; when the SAME peer re-handshakes (e.g. make_session(relay,a) after
    # make_session(a,relay)) reset_session() above wiped the previously negotiated table
    # and rebuilt it WITHOUT the third-party bindings learned via other sessions — relay
    # lost its binding of C in the A-session and could no longer stamp C's ops for A
    # (UNKNOWN_ALIAS on the second relay→A batch, scenarios 23/42). §4.3's "reconnect =
    # fresh exchange" mandates replacing stale bindings for authors whose epoch ROTATED
    # (handled below: the durable registry always carries the newest epoch per nodeId);
    # it does not mandate forgetting still-valid bindings of unchanged authors — under
    # [А1] aliases never enter durable state, so retaining them is inert at worst and
    # essential for relay topologies. The rebuild therefore harvests BOTH endpoints'
    # complete durable author registries; register_alias's [А3] reuse-guard still rejects
    # genuinely conflicting assignments deterministically.
    for node in (a, b):
        for nid2, ep in node.durable_authors.items():
            if nid2 == a.nodeId:
                authors.setdefault(nid2, (aa, ep))
            elif nid2 == b.nodeId:
                authors.setdefault(nid2, (ab, ep))
            else:
                authors.setdefault(nid2, (None, ep))
    ordered = sorted(authors.items(), key=lambda kv: kv[0])
    used = {aa, ab}
    next_free = 1
    assigned: dict[bytes, int] = {a.nodeId: aa, b.nodeId: ab}
    for nid_, (_alias, _ep) in ordered:
        if nid_ in assigned:
            continue
        while next_free in used:
            next_free += 1
        assigned[nid_] = next_free
        used.add(next_free)
    for nid_, (_a0, ep) in ordered:
        alias = assigned[nid_]
        a.register_alias(b.nodeId, alias, nid_, ep)
        b.register_alias(a.nodeId, alias, nid_, ep)


def _encode_ack(batch_id: bytes, acked_seq: int, author_stream: bytes | None = None) -> bytes:
    w = Writer()
    w.b128(batch_id).u64(acked_seq)
    if author_stream is not None:      # Errata E-4: ACK names the acknowledged stream
        w.b128(author_stream)
    return w.value()


def _decode_ack(data: bytes):
    r = Reader(data)
    batch_id = r.b128()
    seq = r.u64()
    author = None
    try:
        author = r.b128()              # optional trailer (E-4); absent ⇒ legacy ACK
    except Exception:
        pass
    return batch_id, seq, author


def deliver_batch(sender: Node, receiver: Node, namespace_id: int,
                  filter_author: bytes | None = None, schema_version: int = 1,
                  drop_ack: bool = False):
    """Create DELTA_BATCH → wrap into frame → parse → apply on receiver → ACK back.

    Returns BatchResult or None if nothing pending. Raises SyncError on rejection
    (the frame-level error path is exercised separately in frame tests).

    `drop_ack=True` simulates a lost ACK (§10.2.2 / scenario 18): the ISSUER does NOT
    learn about the successful delivery, so its lastAckedSeq stays behind and a later
    create_delta_batch replays the very same batchId (deterministic id cache keyed by
    cursorEnd); the receiver answers the duplicate with a mandatory re-ACK [S7]/[П4].

    BUGFIX (implementation finding): previously the ACK was fed back via
    sender.process_ack(sender.nodeId, ...) — a self-keyed no-op that never advanced
    any real watermark, so every sync_round retransmitted the whole outbox forever.
    The ACK must advance the ISSUER's push cursor for the RECEIVER as peer:
    sender.process_ack(receiver.nodeId, ns, acked_seq).
    """
    batch = sender.create_delta_batch(receiver.nodeId, namespace_id,
                                      filter_author_node_id=filter_author,
                                      schema_version=schema_version)
    if batch is None:
        return None
    sender.sign_batch_for(batch, receiver.nodeId)   # idempotent re-bind of the SAME cached
                                                    # (batchId, prev) — see §10.2.2 replay
    payload = sender.encode_batch_payload(batch)
    stream_id = 0x0001_0000 | namespace_id  # §3.7 data streams
    raw = build_frame(0x09, stream_id, 1, payload)
    frame, _ = parse_frame(raw)
    decoded = receiver.decode_batch_payload(frame.payload)
    result = receiver.apply_batch(decoded, sender.nodeId)
    # Receiver's own stream-chain checkpoint for (issuer→me) advances on ACCEPT only;
    # the generic last_batch_digest map is keyed by (issuer, ns, epoch) and is used as
    # the per-receiver expectation because a node receives one chain per issuer stream.
    sender.advance_stream(receiver.nodeId, namespace_id, batch)
    # ACK frame flows back to the ISSUER: advances the issuer's lastAckedSeq [СМ6].
    # Errata E-4: the ACK names the acknowledged AUTHOR STREAM so the issuer moves only
    # that stream's per-author watermark (see Node.process_ack).
    ack_raw = build_frame(0x0B, 0x0002_0000 | namespace_id, 2,
                          _encode_ack(decoded.batchId, decoded.cursorEnd,
                                      decoded.durableAuthor))
    ack_frame, _ = parse_frame(ack_raw)
    bid, seq, ack_author = _decode_ack(ack_frame.payload)
    assert bid == decoded.batchId
    if not drop_ack:
        # ACK advances the ISSUER's push watermark FOR THE RECEIVER as peer (§10.4).
        sender.process_ack(receiver.nodeId, namespace_id, seq, author_stream=ack_author)
    return result


def sync_round(a: Node, b: Node, namespace_id: int, schema_version: int = 1) -> int:
    """Full pairwise delta exchange A→B then B→A. Returns count of delivered batches."""
    n = 0
    for src, dst in ((a, b), (b, a)):
        while True:
            r = deliver_batch(src, dst, namespace_id, schema_version=schema_version)
            if r is None:
                break
            n += 1
    return n


# ------------------------------------------------------------------ sneakernet (§3.5)
def export_delta(node: Node, namespace_id: int, target_hint: bytes, path: str) -> int:
    """Dump DELTA_BATCH frames to a binary file (writer side of exportDelta)."""
    count = 0
    with open(path, "wb") as f:
        while True:
            batch = node.create_delta_batch(target_hint, namespace_id)
            if batch is None:
                break
            payload = node.encode_batch_payload(batch)
            f.write(build_frame(0x09, 0x0001_0000 | namespace_id, count + 1, payload))
            count += 1
            node.process_ack(target_hint, namespace_id, batch.cursorEnd)
    return count


def import_delta(node: Node, path: str, issuer_hint: bytes) -> int:
    """Read frames from a file and apply them exactly like network delivery (§3.5):
    same applyBatch() path — identity and idempotency preserved."""
    with open(path, "rb") as f:
        data = f.read()
    pos = 0
    applied = 0
    while pos < len(data):
        frame, consumed = parse_frame(data[pos:])
        pos += consumed
        batch = node.decode_batch_payload(frame.payload)
        node.apply_batch(batch, issuer_hint)
        applied += 1
    return applied
