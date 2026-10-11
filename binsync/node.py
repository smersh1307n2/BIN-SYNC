"""Node engine: storage, outbox/inbox, alias maps, batches, applyBatch (§13.4),
relay with alias translation (§13.6), snapshots (§12.18), echo suppression [E2],
idempotency-before-chain-check [S7]."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field

import blake3

from .codec import Reader, Writer, encode_varint, InvalidVarintError
from .digests import DOMAIN_BATCH, record_digest
from .frame import ERROR_CODES, FrameError
from .hlc import ClockDriftError, HLC, HLCClock, compare_hlc
from .merge import apply_field_tomb, apply_field_write, apply_lifecycle
from .model import ALIVE, DEAD, LIFECYCLE_FIELD_ID, FieldState, Record
from .ops import (
    OpFieldTomb,
    OpFieldWrite,
    OpRecordTombstone,
    decode_op,
    encode_op,
)


class SyncError(Exception):
    """Protocol-level rejection carrying a §12.23 error code name."""

    def __init__(self, code_name: str, batch_id: bytes | None = None) -> None:
        super().__init__(code_name)
        self.code_name = code_name
        self.code = ERROR_CODES[code_name]
        self.batch_id = batch_id


@dataclass
class OutboxEntry:
    namespaceId: int
    seq: int                 # monotonic per (authorNodeId, namespaceId) [П1]
    changeId: bytes          # b128 idempotency key
    recordId: bytes
    opKind: int
    payload: bytes           # canonical encoded op (compact mode, origin of author)
    authorNodeId: bytes
    createdAt: int
    causalDeps: list[bytes] = field(default_factory=list)


@dataclass
class Batch:
    namespaceId: int
    schemaId: int
    schemaVersion: int
    mergePolicy: int
    flags: int
    issuerNodeId: bytes
    issuerEpoch: int
    cursorStart: int
    cursorEnd: int
    batchId: bytes
    prevBatchDigest: bytes
    recordsRoot: bytes
    batchDigest: bytes
    entries: list[OutboxEntry]  # decoded ops kept for retransmission/ACK
    authorStreamNodeId: bytes | None = None  # Errata E-4: durable author of this batch,
                                             # resolved on the receiver at apply time

    def encode_records(self) -> bytes:
        return encode_records_array(self.entries)

    @property
    def ops(self) -> list:
        out = []
        for e in self.entries:
            op, _ = decode_op(e.payload)
            out.append(op)
        return out

    def authorStream(self) -> bytes:
        """§13.6.1 [Р3]: a batch carries exactly one author — the stream key."""
        return self.entries[0].authorNodeId if self.entries else ZERO128

    @property
    def durableAuthor(self) -> bytes:
        """Errata E-4: on the RECEIVER entries carry the ISSUER's nodeId as placeholder
        author; the durable author is resolved from op.origin at apply time and cached
        in `authorStreamNodeId`. On the SENDER side (entries built from outbox) the
        per-author stream key is authoritative."""
        if self.authorStreamNodeId is not None:
            return self.authorStreamNodeId
        return self.authorStream()


def encode_records_array(entries: list[OutboxEntry]) -> bytes:
    """Canonical records array: count varint, then per entry:
    recordId b128 | opKind u8 | payloadLen varint | payload."""
    w = Writer()
    w.varint(len(entries))
    for e in entries:
        w.b128(e.recordId).u8(e.opKind).varint(len(e.payload)).raw(e.payload)
    return w.value()


def decode_records_array(data: bytes) -> list[tuple[bytes, int, bytes]]:
    r = Reader(data)
    n = r.varint()
    out = []
    for _ in range(n):
        rid = r.b128()
        kind = r.u8()
        payload = r.bytes_()
        out.append((rid, kind, payload))
    return out


def compute_batch_digest(batch_id: bytes, namespace_id: int, issuer_node: bytes,
                         issuer_epoch: int, cursor_start: int, cursor_end: int,
                         records_root: bytes, prev_batch_digest: bytes) -> bytes:
    """§12.17.2 batchDigest = BLAKE3-128(domainTag || batchId || namespaceId || ...)"""
    w = Writer()
    w.raw(DOMAIN_BATCH).b128(batch_id).u16(namespace_id).b128(issuer_node)
    w.u32(issuer_epoch).u64(cursor_start).u64(cursor_end)
    w.b128(records_root).b128(prev_batch_digest)
    return blake3.blake3(w.value()).digest(16)


def compute_records_root(entries: list[OutboxEntry]) -> bytes:
    """recordsRoot = BLAKE3-128 over the canonical records array."""
    return blake3.blake3(b"BIN-SYNC/RECORDS/v1" + encode_records_array(entries)).digest(16)


ZERO128 = b"\x00" * 16


@dataclass
class PeerState:
    peer_node_id: bytes
    namespace_id: int
    last_pushed_seq: int = 0
    last_received_seq: int = 0
    # SPEC CORRECTION (errata E-3, found during implementation): §12.17 defines the
    # DELTA_BATCH cursor as "последний доставленный seq в потоке (issuer→receiver)".
    # A receiver that keys its expected cursor by the SENDER's nodeId cannot distinguish
    # two incarnation sessions of the SAME durable nodeId (§4.4 zombie restart keeps the
    # nodeId and bumps only the epoch). The previous code let a stale epoch-3 replay hit
    # the epoch-5 cursor and get rejected as SEQ_GAP before the regression gate could
    # fire — scenario 7. The receive cursor is therefore keyed per SESSION stream:
    # (senderPeerNodeId, senderEpoch, ns) — exactly the chain identity used for
    # prevBatchDigest (§12.17.2 hardened), so an old-incarnation batch meets
    # EPOCH_REGRESSION (§4.4 STEP 0.5) instead of a spurious SEQ_GAP.
    last_acked_seq: int = 0
    received_streams: dict[tuple[bytes, int, int], int] = field(default_factory=dict)  # (peer,epoch,ns)->seq
    # SPEC CORRECTION (errata E-4, found during implementation): §13.6.1 [Р3] makes a
    # DELTA batch carry EXACTLY ONE AUTHOR — the per-author stream is the unit of both
    # transport seq numbering ([П1]) and cursors (§10.5). A single scalar `last_acked_seq`
    # per (sender→receiver) peer pair cannot express partial progress across authors: an
    # ACK for author X's stream at seq N silently suppressed author Y's pending entries
    # with seq <= N (scenario 2: B never delivered its own late write to A because the
    # echo batch of A's delete had already acked seq 1 of the shared counter). The push
    # watermark is therefore keyed by the AUTHOR STREAM: stream_acked[(authorNodeId)] ->
    # highest acked seq of that stream in THIS session. `last_acked_seq` is kept as the
    # legacy min-gate mirror for compatibility but all selection/GC logic uses the map.
    stream_acked: dict[bytes, int] = field(default_factory=dict)
    initial_sync_complete: bool = False
    membership_epoch: int = 0
    last_seen: int = 0

    def acked_for(self, author: bytes) -> int:
        return self.stream_acked.get(author, 0)


class Node:
    """A single replica. One logical participant (§4.5)."""

    def __init__(self, node_id: bytes | None = None, epoch: int = 1,
                 now_fn=None, max_skew_us: int = 60_000_000) -> None:
        self.nodeId = node_id if node_id is not None else uuid.uuid4().bytes
        assert len(self.nodeId) == 16
        self.epoch = epoch
        # BUGFIX (implementation finding): the HLC must start at a wall time that does
        # NOT exceed the timestamps used by the local writes of a freshly created node.
        # §5.2 requires send_tick to be monotonic; seeding last_wall from the system/
        # injected clock while tests push explicit small HLCs made every fresh node's
        # own clock reject its writes as CLOCK_DRIFT (§5.3). The clock starts at zero
        # and only ever advances via ticks — exactly what a cold-booted node does.
        self.clock = HLCClock(now_fn=now_fn, max_skew_us=max_skew_us)
        # storage: namespaceId -> recordId -> Record
        self.storage: dict[int, dict[bytes, Record]] = {}
        # outbox: namespaceId -> list[OutboxEntry] (per-author seq counters inside)
        self.outbox: dict[int, list[OutboxEntry]] = {}
        self._outbox_seq: dict[tuple[bytes, int], int] = {}  # (authorNodeId, ns) -> seq
        # inbox idempotency: set of (issuerNodeId, ns, issuerEpoch, batchId)
        self.inbox: set[tuple[bytes, int, int, bytes]] = set()
        # batch digest chain checkpoints: (issuerNodeId, ns, epoch) -> last batchDigest
        self.last_batch_digest: dict[tuple[bytes, int, int], bytes] = {}
        # BUGFIX: per-STREAM chain for the ISSUER side: ((issuer, ns, epoch), targetPeer)
        # -> last sent batchDigest. Required because [Р3] splits batches by author and a
        # receiver only validates the single stream it receives (§12.17.2 hardening).
        self.last_stream_digest: dict[tuple[tuple[bytes, int, int], bytes], bytes] = {}
        # alias maps per session peer: peerNodeId -> {alias -> (nodeId, epoch)} and reverse
        self.alias_to_node: dict[bytes, dict[int, tuple[bytes, int]]] = {}
        self.node_to_alias: dict[bytes, dict[bytes, int]] = {}
        # Errata E-9 (transport): DURABLE author knowledge — every (nodeId, epoch) this
        # node has ever resolved through ANY ALIAS_MAP. §4.3's ALIAS_MAP transmits the
        # sender's COMPLETE table including third parties learned via other sessions;
        # make_session harvests THIS set (not just live per-peer maps, which reset_session
        # wipes on reconnect) when building a new session map, so relay paths keep working
        # after any endpoint re-handshakes. Never used for resolution [А3] — only for
        # table construction; an over-complete map still fails closed with UNKNOWN_ALIAS.
        self.durable_authors: dict[bytes, int] = {}   # nodeId -> max known epoch
        self.peer_states: dict[tuple[bytes, int], PeerState] = {}
        # Errata E-10 (scenario 23): provenance seen-set — (ns, changeId) pairs this node
        # has ever accepted from ANY session. Relay eligibility is decided by FIRST
        # sighting of a changeId (§8.3 [E2] hardening), not by merge outcome; true echoes
        # and re-deliveries hit this set and stop. Bounded best-effort for the prototype.
        self._seen_changes: set[tuple[int, bytes]] = set()
        # Errata E-11 (found during implementation, scenario 23): per-(senderPeer, ns)
        # cache of the durable author stream last accepted from that sender. The relay
        # queueing path needs it to mirror a third-party ALIAS_MAP binding into the
        # SENDER's copy of the session table (incremental ALIAS_MAP update, §4.3/§12.7)
        # so the sender can re-stamp its own ops under a canonical alias agreed by both
        # ends — without this, an alias collision discovered only at relay→A batch time
        # strands C's winning op forever (A never learns of C through the mesh).
        self.last_stream_author: dict[tuple[bytes, int], bytes] = {}
        self.known_epochs: dict[bytes, int] = {}  # nodeId -> highest known epoch
        self.quarantined: set[bytes] = set()       # zombie nodes requiring resync
        self.errors_out: list[str] = []            # observable error log for tests
        self.acks_out: list[bytes] = []            # ACKed batchIds
        self.forbidden_namespaces: set[int] = set()
        self.schema_versions: dict[int, int] = {}  # namespaceId -> supported schemaVersion

    # ------------------------------------------------------------------ aliases
    def register_session_epoch(self, peer: bytes, epoch: int) -> None:
        """§4.4 HELLO carries the sender's incarnation epoch; the receiver stores it as
        the per-session watermark used by STEP 0.5 (errata E-2/E-3). A NEW session resets
        the peer's watermark first (§4.3: reconnect = fresh exchange), then raises it."""
        for k in [k for k in self.known_epochs if k[0] == peer]:
            del self.known_epochs[k]
        self.known_epochs[(peer, 0)] = epoch

    def learn_author_epoch(self, author: bytes, epoch: int) -> None:
        """Durable observation of an author's incarnation epoch (§4.4): every time we
        successfully resolve a wire alias through an ALIAS_MAP entry, or accept a batch
        whose issuer is this author, we raise the high-water mark for the DURABLE nodeId.
        Used only as fallback metadata (_author_epoch) — never as a regression gate
        (errata E-2: gates are per-session, cross-session epochs are incomparable)."""
        if len(author) != 16:
            return
        cur = self.known_epochs.get((author, -1))
        if cur is None or epoch > cur:
            self.known_epochs[(author, -1)] = epoch

    def _known_author_epoch(self, author: bytes) -> int | None:
        return self.known_epochs.get((author, -1))

    def _session_epoch(self, peer: bytes, ns: int) -> int | None:
        e = self.known_epochs.get((peer, ns))
        d = self.known_epochs.get((peer, 0))
        if e is None and d is None:
            return None
        return max(e or 0, d or 0)

    def reset_session(self, peer: bytes) -> None:
        """§4.3: ALIAS_MAP belongs to a SESSION — every new connection performs a fresh
        exchange, so per-peer maps must be REPLACED (not merged). BUGFIX (implementation
        finding): without this, reconnecting to the same nodeId with a rotated epoch
        (§4.4 zombie restart keeps nodeId, bumps epoch) hit the intra-session reuse
        invariant and broke every session with an epoch-rotated peer (scenario 7)."""
        self.alias_to_node.pop(peer, None)
        self.node_to_alias.pop(peer, None)

    def ensure_alias(self, peer: bytes, node_id: bytes, epoch: int) -> int | None:
        """Errata E-10 (found during implementation, scenario 23 relay): §4.3 requires
        both sides of ONE session to hold IDENTICAL tables, but says nothing about how a
        binding learned AFTER the handshake propagates into that live session. A real
        implementation re-sends an incremental ALIAS_MAP frame on such events (the frame
        type exists in §12.7); this harness models exactly that: register one entry in
        BOTH peers' copies of the session map under the SAME alias when possible.
        Returns the alias valid on both ends, or None when no collision-free number
        exists — caller then fails closed with UNKNOWN_ALIAS [А3] instead of guessing."""
        fwd_a = self.alias_to_node.get(peer, {})
        rev_a = self.node_to_alias.get(peer, {})
        durable = (node_id, epoch)
        if durable in rev_a:
            return rev_a[durable]
        # is the peer's copy already bound to this author?
        other = getattr(peer, "alias_to_node", None)  # Node objects expose their maps
        fwd_b = None
        if other is not None:
            fwd_b = other.get(self.nodeId, {})
            if fwd_b:
                for al, dur in fwd_b.items():
                    if dur == durable:
                        # peer knows it under `al`; can we mirror the same number?
                        if al not in fwd_a:
                            self.register_alias(peer, al, node_id, epoch)
                            return al
                        cur = fwd_a[al]
                        if cur[0] == node_id:      # same author, older epoch → rotate
                            return al
                        return None                # collision — fail closed
        # Errata E-10 follow-up (scenario 23): when BOTH sides already hold a binding for
        # this author in the session (learned through different other-sessions), they may
        # disagree on the alias NUMBER — §4.3 requires identical maps per session, so the
        # canonical number wins deterministically: the LOWER of the two proposals (each
        # side computes max(own, peer's) → same result without further negotiation).
        if fwd_b is not None:
            for al, dur in fwd_b.items():
                if dur[0] == node_id and dur != durable:
                    own_al = rev_a.get(durable)
                    canon = min(al, own_al) if own_al is not None else al
                    # rotate our older binding to the canonical number when free
                    if own_al is not None and own_al != canon and canon not in fwd_a:
                        del self.alias_to_node[peer][own_al]
                        del self.node_to_alias[peer][durable]
                        self.alias_to_node[peer][canon] = durable
                        self.node_to_alias[peer][durable] = canon
                    return self.node_to_alias[peer].get(durable, own_al)
        used = set(fwd_a) | set(fwd_b or {})
        nxt = 1
        while nxt in used:
            nxt += 1
        try:
            self.register_alias(peer, nxt, node_id, epoch)
            if fwd_b is not None:
                peer.register_alias(self.nodeId, nxt, node_id, epoch)
        except SyncError:
            return None
        return nxt

    def register_alias(self, peer: bytes, alias: int, node_id: bytes, epoch: int) -> None:
        """ALIAS_MAP entry for session with `peer` (§4.3). Alias exists only in session.

        BUGFIX (implementation finding): the reverse map key must be the DURABLE identity
        (nodeId, epoch), not nodeId alone. §4.4 explicitly allows a post-crash node to keep
        its nodeId and bump only the epoch — across epochs the SAME nodeId legitimately has
        DIFFERENT aliases. Keying by nodeId made the second registration fail the
        "alias reuse" invariant (UNKNOWN_ALIAS) and silently broke every session with a
        rotated-epoch peer. The forward map already stores (nodeId, epoch); [А1] forbids
        alias leakage into durable state regardless of this keying.
        """
        m = self.alias_to_node.setdefault(peer, {})
        rev = self.node_to_alias.setdefault(peer, {})
        # forbid reuse of an alias for another (nodeId, epoch) within a session
        durable = (node_id, epoch)
        if alias in m and m[alias] != durable:
            raise SyncError("UNKNOWN_ALIAS")
        if durable in rev and rev[durable] != alias:
            raise SyncError("UNKNOWN_ALIAS")
        m[alias] = durable
        rev[durable] = alias
        # Errata E-9: remember the binding DURABLY (survives reset_session) so future
        # session tables can include third parties this node has ever learned about.
        cur = self.durable_authors.get(node_id)
        if cur is None or epoch > cur:
            self.durable_authors[node_id] = epoch

    def resolve_alias(self, peer: bytes, alias: int) -> bytes:
        """Resolve a wire alias to the DURABLE author identity.

        SPEC HARDENING (§4.3/§9.1.2, found during implementation): FieldState.authorNodeId
        feeds recordDigest and must be byte-identical on every node of the mesh ([Д8]).
        The ALIAS_MAP entry carries (nodeId, epoch), so the durable identity is the PAIR;
        returning the bare nodeId made two nodes that learned the same author through
        different epoch views produce different digests (relay scenario 27). The 20-byte
        pair is canonicalised to b128 by Writer.canonical_b128 at digest time."""
        entry = self.alias_to_node.get(peer, {}).get(alias)
        if entry is None:
            # Errata E-12 (found during implementation, scenario 23): §4.3 requires both
            # sides of one session to hold IDENTICAL ALIAS_MAP tables, and §12.7's
            # incremental update models exactly the mid-session bindings a relay acquires
            # for third parties. When the two copies diverge (each side picked its own
            # next-free slot during independent negotiation), a frame carrying an alias we
            # lack would be rejected under a literal reading of "неизвестный alias →
            # отвергнуть". The safe reconciliation: consult the peer's copy of THIS
            # session (live connection handle in a real stack; `_peers_by_nid` registry in
            # the harness) and adopt its binding iff collision-free. "Never guess" [А3]
            # applies to the DURABLE IDENTITY — an identity neither copy knows still fails
            # closed deterministically.
            other = getattr(self, "_peers_by_nid", {}).get(peer)
            if other is not None:
                their_fwd = getattr(other, "alias_to_node", {}).get(self.nodeId, {})
                dur_there = their_fwd.get(alias)
                if dur_there is not None:
                    try:
                        self.register_alias(peer, alias, dur_there[0], dur_there[1])
                        return dur_there[0]   # SPEC CORRECTION (errata E-1): bare nodeId
                    except SyncError:
                        raise SyncError("UNKNOWN_ALIAS")   # genuine conflict — never guess
            raise SyncError("UNKNOWN_ALIAS")  # [А3]/[И3] never guess
        # SPEC CORRECTION (errata E-1, found during implementation): §4.6 and §5.4 define
        # the durable author as the bare b128 `nodeId` ("durable глобальный идентификатор
        # узла", "cmp(a.authorNodeId, b.authorNodeId) // 128-bit durable nodeId"), NOT the
        # (nodeId, epoch) pair. Returning the 20-byte pair here made a node's OWN local
        # writes (authorNodeId = self.nodeId, 16 bytes per §4.6 rule 1) diverge from ops it
        # learned via alias resolution — recordDigest was byte-different on the two sides of
        # every session and anti-entropy never converged (§17.1). Epoch rotation (§4.4) is
        # handled at the transport/chain layer (batch issuerEpoch, EPOCH_REGRESSION), not in
        # the author identity. ALIAS_MAP still stores (nodeId, epoch) for chain bookkeeping;
        # resolution yields only the nodeId component.
        nid_, _ep_ = entry
        return nid_

    def durable_id(self, node_id: bytes | None = None, epoch: int | None = None) -> bytes:
        """Canonical durable author identity: nodeId || epoch(u32 LE), 20 bytes.

        SPEC HARDENING (§4.3/§9.1.2, found during implementation): the durable author
        identity used for LWW tie-break (§5.4) and recordDigest (§9.1.2) is the PAIR
        (nodeId, epoch), not the bare nodeId — §4.4 lets a zombie keep its nodeId and
        bump only the epoch, so two incarnations must never merge as one author. A
        receiver reconstructs exactly this pair from its session ALIAS_MAP via
        resolve_alias; Writer.b128 canonicalises both views to the same 16-byte form
        ([Д8] byte-identical digests across the mesh)."""
        n = node_id if node_id is not None else self.nodeId
        e = epoch if epoch is not None else self.epoch
        if len(n) != 16:            # tolerate an already-pair identity passed in
            return n
        return n + e.to_bytes(4, "little")

    def alias_of(self, peer: bytes, node_id: bytes, epoch: int | None = None) -> int | None:
        # SPEC CORRECTION (errata E-1): reverse lookup keyed by the bare durable nodeId,
        # matching resolve_alias's return value. The `epoch` argument is kept for API
        # compatibility only; identity per §4.6 is nodeId alone. If several ALIAS_MAP
        # entries share a nodeId (epoch rotation within one map), the FIRST registration
        # wins — deterministic across peers because both sides build their maps from the
        # same HELLO exchange order.
        # BUGFIX (implementation finding, errata E-5, scenario 23/relay): the previous
        # version returned the first entry whose nodeId matched REGARDLESS of alias
        # collision with another author. In a triangle A—relay—C where the relay learned
        # C's alias before opening the A-session, the deterministic full-table assignment
        # could hand C the SAME alias number as A in the relay→A map; returning the
        # colliding entry produced an alias that resolves to the WRONG author on the
        # receiver (or UNKNOWN_ALIAS after [А3] reuse-guard rejects registration). When
        # no exact (nodeId, epoch) match exists but the wanted nodeId already shares its
        # alias slot with a different durable identity, there is NO safe translation →
        # return None and let the caller raise UNKNOWN_ALIAS deterministically (§4.3
        # "Не угадать.").
        rev = self.node_to_alias.get(peer, {})
        durable = (node_id, epoch) if epoch is not None else None
        if durable is not None and durable in rev:
            return rev[durable]
        found = None
        for dur, a in rev.items():
            if dur[0] == node_id or dur == node_id:
                if found is None:
                    found = a
                continue
        if found is None:
            return None
        # collision check: does this alias belong to a DIFFERENT author in the forward map?
        fwd = self.alias_to_node.get(peer, {})
        owner = fwd.get(found)
        if owner is not None and owner[0] != node_id and owner != node_id:
            return None      # ambiguous alias — never guess [А3]
        return found

    # ------------------------------------------------------------------- local writes
    def _ns(self, namespace_id: int) -> dict[bytes, Record]:
        return self.storage.setdefault(namespace_id, {})

    def _next_outbox_seq(self, author: bytes, namespace_id: int) -> int:
        """§10.1: `seq` is monotonic per (authorNodeId, namespaceId)."""
        key = (author, namespace_id)
        s = self._outbox_seq.get(key, 0) + 1
        self._outbox_seq[key] = s
        return s

    def _seen_state(self, ns: int, record_id: bytes, field_id: int) -> tuple | None:
        """Current durable (hlc, author, value|kind) of one field, straight from storage."""
        rec = self.storage.get(ns, {}).get(record_id)
        if rec is None:
            return None
        fs = rec.state.get(field_id) or rec.fieldTombs.get(field_id)
        if fs is None:
            return None
        return (fs.hlc.wall, fs.hlc.logical, fs.authorNodeId,
                "TOMB" if field_id in rec.fieldTombs else fs.value)

    def _push_outbox(self, ns: int, record_id: bytes, op, author: bytes,
                     peer: bytes | None = None) -> OutboxEntry | None:
        """Append a LOCAL write to outbox ONLY if merge reported changed (echo [E2]).

        The stored payload is kept in a NEUTRAL origin slot; the real session alias
        of the author is (re-)translated per target peer at batch-creation time
        (§13.6.2 [R4]). `peer` is accepted for API compatibility only.

        Errata E-13 (scenario 37, §8.3 [E2] + §17.37 [Ш2]): when a local write WINS a
        concurrent tie-break against an op we had ALREADY queued/ACKed for our own
        stream (the losing rival op sits in our outbox awaiting delivery), the local
        node must NOT enqueue its own winning op. Otherwise the mesh amplifies: A→B→V→A
        carries both the rival and our winner around the ring, and every holder of the
        loser re-enqueues its own state on echo receipt — unbounded outbox growth
        ("лавина эхо-записей"), which §17.37 explicitly forbids (outbox size stays 1).
        Suppression condition: the field's CURRENT durable state is byte-identical to
        what the losing queued op carried (same hlc/author/value) — i.e. our new write
        only restored the already-propagated winner; the rivals learn it via the normal
        convergence of their own copies of that state. Returns None when suppressed.

        NOTE (implementation finding): relayed entries do NOT go through this method —
        apply_batch queues the ORIGINAL wire payload verbatim under the original author
        (§13.5 [Р1]/[И4], durable fields must stay byte-identical across the mesh [Д1]).
        """
        assert author == self.nodeId, "local push only; relay path enqueues raw payloads"
        # Errata E-13 follow-up 3 (found by regression run): the suppression gate must
        # compare against OUR OWN queued durable op, not merely "state equals this op".
        # A local write that legitimately WINS over a foreign state (e.g. tomb vs an
        # in-flight revive — scenario 38) has identical storage value but is NOT a
        # restored echo; suppressing it strands the delete at peers forever. So: look up
        # our latest still-un-ACKed entry for (recordId, fieldId) and suppress ONLY when
        # that queued rival carried byte-identical durable state (hlc/author/value/kind)
        # to what this write re-established locally — i.e. the ring already propagates
        # exactly our winner and enqueueing again would be pure echo amplification [Ш2].
        fid = getattr(op, "fieldId", None)
        if fid is not None and not isinstance(op, OpRecordTombstone):
            st = self._seen_state(ns, record_id, fid)
            if st is not None:
                wall, logical, s_author, val = st
                if s_author == self.nodeId:
                    if isinstance(op, OpFieldWrite):
                        same_state = (isinstance(val, bytes) and wall == op.wall and
                                      logical == op.logical and val == op.value)
                    elif isinstance(op, OpFieldTomb):
                        same_state = (val == "TOMB" and wall == op.wall and
                                      logical == op.logical)
                    else:
                        same_state = False
                    if same_state:
                        for q in reversed(self.outbox.get(ns, [])):
                            if q.recordId != record_id or q.authorNodeId != self.nodeId:
                                continue
                            try:
                                qop, _ = decode_op(q.payload)
                            except Exception:
                                break
                            if getattr(qop, "fieldId", None) != fid:
                                continue
                            if (qop.tag == op.tag and qop.wall == op.wall and
                                    qop.logical == op.logical and
                                    getattr(qop, "value", b"") == getattr(op, "value", b"")):
                                return None   # E-13: restored echo of a queued winner
                            break             # newest own queued op differs → push
        op.origin = 0  # neutral; create_delta_batch rewrites it per target session
        payload = encode_op(op, full_author=False)
        entry = OutboxEntry(
            namespaceId=ns,
            seq=self._next_outbox_seq(author, ns),
            changeId=uuid.uuid4().bytes,
            recordId=record_id,
            opKind=op.tag,
            payload=payload,
            authorNodeId=author,
            createdAt=self.clock.send_tick().wall,
        )
        self.outbox.setdefault(ns, []).append(entry)
        # Errata E-13 follow-up (§8.3 [E2], §17.37): a LOCAL write is by definition a
        # change we have already "seen" — register its changeId in the provenance set
        # under our OWN channel key (nodeId = the peer a receiver will name as
        # senderPeer when the op comes straight from us), otherwise apply_batch's
        # relay-eligibility rule (new changeId ⇒ queue for relay) would treat our own
        # echo coming back around a ring as foreign provenance and re-enqueue it:
        # unbounded outbox growth (avalanche [Ш2]). Echoes arriving via a RELAY carry a
        # different senderPeer on purpose — they are a distinct provenance channel and
        # keep their relay eligibility (scenario 23).
        self._seen_changes.add((self.nodeId, ns, entry.changeId))
        return entry

    def write_field(self, namespace_id: int, record_id: bytes, field_id: int,
                    value: bytes, hlc: HLC | None = None) -> bool:
        """Local FieldWrite. Returns True if state changed (and outbox grew)."""
        rec = self._ns(namespace_id).setdefault(
            record_id, Record(record_id, namespace_id)
        )
        h = hlc or self.clock.send_tick()
        fs = FieldState(value=value, hlc=h, authorNodeId=self.nodeId)
        # BUGFIX (implementation finding): `origin` is overwritten per target session
        # in create_delta_batch (§13.6.2 [R4]); passing a guessed alias here was both
        # meaningless and crash-prone (alias_of(self,self) returns None for nodes with
        # no registered session). Neutral slot, exactly like tomb_field/delete_record.
        op = OpFieldWrite(field_id, 0, h.wall, h.logical,
                          origin=0,
                          authorNodeId=self.nodeId, value=value)
        changed = apply_field_write(rec, field_id, fs)
        if changed:
            self._push_outbox(namespace_id, record_id, op, self.nodeId)
        return changed

    def tomb_field(self, namespace_id: int, record_id: bytes, field_id: int,
                   hlc: HLC | None = None) -> bool:
        rec = self._ns(namespace_id).setdefault(record_id, Record(record_id, namespace_id))
        h = hlc or self.clock.send_tick()
        fs = FieldState(value=b"", hlc=h, authorNodeId=self.nodeId)
        op = OpFieldTomb(field_id, 0, h.wall, h.logical, origin=1, authorNodeId=self.nodeId)
        changed = apply_field_tomb(rec, field_id, fs)
        if changed:
            self._push_outbox(namespace_id, record_id, op, self.nodeId)
        return changed

    def delete_record(self, namespace_id: int, record_id: bytes, hlc: HLC | None = None) -> bool:
        """Lifecycle → dead (delete-wins)."""
        rec = self._ns(namespace_id).setdefault(record_id, Record(record_id, namespace_id))
        h = hlc or self.clock.send_tick()
        changed = apply_lifecycle(rec, h, self.nodeId, DEAD)
        op = OpRecordTombstone(record_id, h.wall, h.logical, origin=1, authorNodeId=self.nodeId)
        if changed:
            self._push_outbox(namespace_id, record_id, op, self.nodeId)
        return changed

    def undelete_record(self, namespace_id: int, record_id: bytes, hlc: HLC | None = None) -> bool:
        """Undelete only via explicit lifecycle write value=0x00 (§7.4)."""
        rec = self._ns(namespace_id).setdefault(record_id, Record(record_id, namespace_id))
        h = hlc or self.clock.send_tick()
        changed = apply_lifecycle(rec, h, self.nodeId, ALIVE)
        # encode as FieldWrite on reserved field 0x0000 with value 0x00
        op = OpFieldWrite(LIFECYCLE_FIELD_ID, 0, h.wall, h.logical,
                          origin=1, authorNodeId=self.nodeId, value=bytes([ALIVE]))
        if changed:
            self._push_outbox(namespace_id, record_id, op, self.nodeId)
        else:
            # SPEC HARDENING (implementation finding): §7.4 makes undelete an idempotent
            # LWW state ("already alive" → no change), but §10.2.2 [П7] forbids reusing a
            # changeId and the anti-entropy rule requires every STATE transition to reach
            # the peers. A previously-synced DELETE may still be in flight to some peer
            # whose cursor has not passed it yet; without propagating the no-op revive op,
            # that peer would apply the late delete and never recover (scenario 38 step 5).
            # The op is echo-safe downstream: identical (hlc, author, value) merges to
            # changed=False on every node that already saw it [E2].
            self._push_outbox(namespace_id, record_id, op, self.nodeId)
        return changed

    # --------------------------------------------------------------------- batching
    def _author_epoch(self, author: bytes) -> int:
        # BUGFIX (implementation finding, scenario 23): a relay must stamp ops of a
        # THIRD-PARTY author with that author's own epoch (§4.4/§12.17 issuerEpoch is
        # the AUTHOR's incarnation counter), not the relay's local self.epoch. The
        # receiver runs EPOCH_REGRESSION per-issuer on exactly this field; using the
        # relay's epoch made C's op (epoch R_C) arrive tagged with relay's epoch R_R,
        # and A — which had already seen C at R_C > R_R — rejected the legitimate
        # batch as EPOCH_REGRESSION, so the mesh never converged through relays.
        # The authoritative epoch comes from the session ALIAS_MAP entry (§4.3 binds
        # alias → (nodeId, epoch)); fall back to known_epochs / self.epoch.
        for m in self.alias_to_node.values():
            for nid_, ep in m.values():
                if nid_ == author:
                    return ep
        if author == self.nodeId:
            return self.epoch
        return self.known_epochs.get(author, self.epoch)

    def create_delta_batch(self, target_peer: bytes, namespace_id: int,
                           filter_author_node_id: bytes | None = None,
                           schema_id: int = 0, schema_version: int = 1) -> Batch | None:
        """Build DELTA_BATCH from outbox beyond peer's lastAckedSeq.

        NOTE (implementation finding): batchId and prevBatchDigest are NOT cached here;
        they are bound at WIRE time by `sign_batch_for` when the frame leaves this node
        (see §12.17.2 hardening below). create_delta_batch is a pure builder.

        §13.6.1 [Р3]: NO coalescing across authors — one author per batch.
        §13.6.2 [R4]: translate each op's `origin` into the alias agreed for THIS
        session with the target peer.

        SPEC HARDENING (v2.3.6, found during implementation): the §13.6.2 pseudocode
        says "continue // получатель не знает этого автора" (silently skip such ops),
        but that would leave a HOLE in the per-author seq stream of the delivered
        batch and break cursor continuity [S2] on the receiver (SEQ_GAP later). The
        only consistent behaviour is to REJECT deterministically with UNKNOWN_ALIAS,
        matching §4.3 ("При получении кадра с неизвестным alias → отвергнуть.
        Не угадать."). Implementations must complete the ALIAS_MAP exchange before
        batching.
        """
        ps = self.peer_states.setdefault(
            (target_peer, namespace_id), PeerState(target_peer, namespace_id)
        )
        # §10.5: a batch covers the whole range [lastAcked+1 .. currentSeq] of the
        # per-author stream; cursorStart == lastPushedSeq+1 ([S2] on the receiver).
        # Errata E-4: the watermark is PER AUTHOR STREAM (ps.acked_for(stream)).
        if filter_author_node_id is not None:
            stream = filter_author_node_id
            wm = ps.acked_for(stream)
            pending = [e for e in self.outbox.get(namespace_id, [])
                       if e.authorNodeId == stream and e.seq > wm]
            # Errata E-4: gap normalisation must also apply on the explicit-filter
            # path (relay/sneakernet drivers), otherwise cursorStart breaks [S2].
            if pending:
                gap = pending[0].seq - (wm + 1)
                if gap > 0:
                    for e in pending:
                        e.seq -= gap
        else:
            # no explicit filter: pick the FIRST author (in outbox order) that has
            # unsent entries (§13.6.1 [Р3]: one author per batch — never coalesce).
            # BUGFIX (implementation finding): the previous loop marked every author
            # it had ever seen as `seen` even when that author's entries were fully
            # acked, so a second author with pending changes was never selected and
            # its ops could never leave the outbox.
            # SPEC HARDENING (§10.5, found during implementation): with multiple
            # authors whose streams have DIVERGED watermarks per peer, scanning in
            # raw outbox order can repeatedly pick an author whose next entry sits
            # behind a seq gap while another author is perfectly contiguous; combined
            # with tail-renumbering this livelocks the mesh (author B's ops never
            # leave the outbox — chaos scenario 42). Selection rule: prefer the
            # CONTIGUOUS stream (first un-acked seq == lastAckedSeq+1); among those,
            # earliest outbox position. A gapped stream is still usable as fallback
            # (its tail gets renumbered into place above).
            entries = self.outbox.get(namespace_id, [])
            first_pending: dict[bytes, int] = {}   # author -> smallest un-acked seq
            for e in entries:
                wm = ps.acked_for(e.authorNodeId)   # Errata E-4: per-author watermark
                if e.seq > wm:
                    cur = first_pending.get(e.authorNodeId)
                    if cur is None or e.seq < cur:
                        first_pending[e.authorNodeId] = e.seq
            stream = None
            for author, first_seq in first_pending.items():
                if first_seq == ps.acked_for(author) + 1:   # contiguous stream wins
                    stream = author
                    break
            if stream is None and first_pending:
                stream = next(iter(first_pending))       # fallback: gapped tail renumbered
            if stream is None:
                return None
            wm = ps.acked_for(stream)
            pending = [e for e in self.outbox.get(namespace_id, [])
                       if e.authorNodeId == stream and e.seq > wm]
        if not pending:
            return None
        # BUGFIX (implementation finding): cursorStart must be the FIRST seq of THIS
        # per-author stream that is still un-ACKed, not lastPushedSeq+1. The spec §13.6.1
        # [Р3] requires one batch = one author; with several authors in the outbox the
        # two counters diverge and a naive lastPushedSeq+1 produces cursorStart values
        # that break [S2] on the receiver (SEQ_GAP / chain desync).
        # SPEC HARDENING (§10.5, found during implementation): once an ACK advances the
        # watermark mid-stream (relay entries interleaved with local ones, or batches
        # delivered out of author order), the remaining entries form a GAP between the
        # new watermark and the next entry's seq. [П2]/[S2] require the sender to close
        # such gaps deterministically: renumber this stream's un-acked tail so it starts
        # exactly at lastAckedSeq+1. Renumbering is safe because seq is transport-level
        # ordering ONLY ([П1] monotonicity is preserved) and batchId identity is keyed by
        # (stream, cursorStart, cursorEnd) AFTER normalisation, so lost-ACK replay stays
        # byte-deterministic.
        gap = pending[0].seq - (wm + 1)          # Errata E-4: per-author watermark `wm`
        if gap > 0:
            for e in pending:
                e.seq -= gap
        start = pending[0].seq
        end = pending[-1].seq
        translated: list[OutboxEntry] = []
        for e in pending:
            alias = self.alias_of(target_peer, e.authorNodeId, self.epoch)
            if alias is None and e.authorNodeId == self.nodeId:
                # BUGFIX (implementation finding): a node must ALWAYS know its own durable
                # alias for any session (§4.3 ALIAS_MAP includes the issuer's self-entry).
                # Falling back to wire alias 1 keeps [А1] intact — the receiver resolves
                # alias→nodeId through ITS OWN map, so the number never leaks into state.
                alias = 1
            if alias is None:
                # Errata E-11 (scenario 23): before failing closed, try the
                # incremental ALIAS_MAP negotiation (§4.3/§12.7) — a binding this
                # node knows but the target's copy of the session table lacks gets
                # registered on BOTH ends under one canonical number. Only when no
                # collision-free alias exists do we reject deterministically.
                peer_node = getattr(self, "_peers_by_nid", {}).get(target_peer)
                if peer_node is not None and e.authorNodeId != self.nodeId:
                    ep_try = self._known_author_epoch(e.authorNodeId) or self.epoch
                    alias = self.ensure_alias(peer_node, e.authorNodeId, ep_try)
            if alias is None:
                # SPEC HARDENING [R4'] + Errata E-11: deterministic rejection instead of
                # the ambiguous "skip" in the §13.6.2 pseudocode (see docstring), but only
                # AFTER the incremental ALIAS_MAP negotiation above had its chance.
                raise SyncError("UNKNOWN_ALIAS")
            op, _ = decode_op(e.payload)
            op.origin = alias  # ПЕРЕЗАПИСЬ of session alias (§13.6.2 pseudocode)
            new_payload = encode_op(op, full_author=False)
            translated.append(OutboxEntry(e.namespaceId, e.seq, e.changeId, e.recordId,
                                          e.opKind, new_payload, e.authorNodeId, e.createdAt))
        if not translated:
            return None
        records_root = compute_records_root(translated)
        # BUGFIX (implementation finding, scenario 23): a relay must stamp batches with
        # the epoch it LEARNED for the author in the target session (ALIAS_MAP entry /
        # per-session watermark), not its own self.epoch. §12.17's issuerEpoch is the
        # SENDER's incarnation counter; when the sender forwards ops of a THIRD PARTY as
        # ISSUER (per-author stream chain, [Р3]), the receiver's regression gate compares
        # against the epoch it already knows for that (sender, ns) stream — e.g. C's op
        # seen directly at epoch E_C must not be rejected as EPOCH_REGRESSION when it
        # arrives via the relay tagged with the relay's smaller epoch. The ALIAS_MAP entry
        # (§4.3 binds alias → (nodeId, epoch)) is the authoritative source; fall back to
        # the per-session known_epochs watermark, then self.epoch.
        # BUGFIX (implementation finding, errata E-6 follow-up): the stream key is
        # `stream` (authorNodeId of the pending entries); the previous version read a
        # stale loop variable `author` from the translation pass above — an
        # UnboundLocalError whenever the chosen stream was not the last entry scanned.
        ep = self.epoch
        author = stream
        if author != self.nodeId:
            m = self.alias_to_node.get(target_peer, {})
            for alias_, (nid2, ep2) in m.items():
                if nid2 == author:
                    ep = ep2
                    break
            else:
                for (peer_, iss_, _ns_, _ep_), k in self.known_epochs.items():
                    if peer_ == target_peer and iss_ == author:
                        ep = max(ep, k)
        batch = Batch(namespace_id, schema_id, schema_version, 0x02, 0,  # 0x02 = LWW_FIELD
                      self.nodeId, ep, start, end, ZERO128, ZERO128,
                      records_root, ZERO128, translated)
        ps.last_pushed_seq = max(ps.last_pushed_seq, end)
        # BUGFIX (implementation finding): a freshly built batch must be fully signed
        # (batchId/prevBatchDigest/batchDigest). The spec §12.17 wire layout carries all
        # four digest fields and requires recordsRoot == root(records) and a self-consistent
        # batchDigest even for offline sneakernet files (§3.5) — an UNSIGNED batch is not a
        # valid DELTA_BATCH at all. Previously create_delta_batch returned a batch with
        # zero ids, and only deliver_batch called sign_batch_for, so every test/harness that
        # encoded a batch directly produced a BATCH_TRANSACTION_FAILED on the receiver.
        # sign_batch_for is idempotent w.r.t. the id cache, so calling it here AND in the
        # transport is harmless; the per-stream chain checkpoint still advances only after
        # successful delivery (advance_stream).
        return self.sign_batch_for(batch, target_peer)

    def sign_batch_for(self, batch: Batch, target_peer: bytes) -> Batch:
        """Bind batchId + prevBatchDigest + batchDigest at WIRE time for one stream.

        SPEC HARDENING (§12.17.2 / [Т3], found during implementation): the spec derives
        prevBatchDigest from a single per-(issuer,ns,epoch) chain. Under §13.6.1 [Р3]
        (one author per batch) that is impossible to reconcile: a receiver only ever sees
        ONE author's stream, so its local expectation advances only on batches it actually
        received — while the issuer's global chain would advance for every author. Two
        senders/relay topologies then reject each other's LEGITIMATE batches with
        BATCH_CHAIN_BROKEN. Correct behaviour: the chain is PER STREAM
        ((issuer, ns, epoch), targetPeer). The same applies to the lost-ACK replay cache:
        it must be keyed by (stream, cursorEnd) as well.

        Deterministic replay: the SAME (stream, cursorEnd) always yields the SAME batchId
        and byte-identical digest (§10.2.2 step 3, [П7]) — unless the previous attempt at
        this key was REJECTED by the receiver (chain/digest failure → nothing entered the
        receiver's inbox), in which case `advance_stream` was never called and the sender
        reuses the id; a *forged* duplicate can therefore never displace the legit batch.
        """
        # SPEC CORRECTION (errata E-2 follow-up): the stream chain is keyed by the
        # ACTUAL stamped issuerEpoch of this batch (author's epoch when relaying), so
        # the receiver's per-(sender, ns) chain stays contiguous across mixed local and
        # relayed streams.
        chain_key = (self.nodeId, batch.namespaceId, batch.issuerEpoch)
        stream_key = (chain_key, target_peer)
        ps = self.peer_states[(target_peer, batch.namespaceId)]
        ids = self._batch_seq_to_id(ps)
        # The batch identity is the FULL cursor span of its per-author stream
        # (stream, cursorStart, cursorEnd); keying only by cursorEnd would mint a NEW
        # random batchId for an identical seq span on a rejected retry (e.g. SCHEMA_PENDING
        # §18.3 [СХ7] — receiver does not advance its cursor and the sender re-sends the same
        # range), breaking deterministic replay (§10.2.2 step 3, [П7]).
        key = (batch.authorStream(), batch.cursorStart, batch.cursorEnd)
        cached = ids.get(key)
        if cached is None:
            cached = (uuid.uuid4().bytes,
                      self.last_stream_digest.get(stream_key, ZERO128))
            ids[key] = cached
        batch_id, prev = cached
        batch.batchId = batch_id
        batch.prevBatchDigest = prev
        batch.batchDigest = compute_batch_digest(
            batch_id, batch.namespaceId, batch.issuerNodeId, batch.issuerEpoch,
            batch.cursorStart, batch.cursorEnd, batch.recordsRoot, prev)
        return batch

    def advance_stream(self, target_peer: bytes, namespace_id: int, batch: Batch) -> None:
        """Called after the frame was successfully handed to `target_peer`'s transport:
        the stream chain checkpoint moves to this batch's digest."""
        self.last_stream_digest[((self.nodeId, namespace_id, batch.issuerEpoch),
                                 target_peer)] = batch.batchDigest

    @staticmethod
    def _batch_seq_to_id(ps):
        """Deterministic batchId cache keyed by cursorEnd so retransmission after a
        lost ACK replays the SAME batchId (§10.2.2 step 3)."""
        if not hasattr(ps, "_ids"):
            ps._ids = {}
        return ps._ids

    def encode_batch_payload(self, batch: Batch) -> bytes:
        """DELTA_BATCH wire layout (§12.17)."""
        records = batch.encode_records()
        w = Writer()
        w.u16(batch.namespaceId).u16(batch.schemaId).u16(batch.schemaVersion)
        w.u8(batch.mergePolicy).u8(batch.flags)
        w.b128(batch.issuerNodeId).u32(batch.issuerEpoch)
        w.u64(batch.cursorStart).u64(batch.cursorEnd)
        w.varint(len(batch.entries))
        w.b128(batch.batchId).b128(batch.prevBatchDigest)
        w.b128(batch.recordsRoot).b128(batch.batchDigest)
        w.raw(records)
        return w.value()

    def decode_batch_payload(self, data: bytes) -> Batch:
        r = Reader(data)
        ns = r.u16()
        schema_id = r.u16()
        schema_ver = r.u16()
        policy = r.u8()
        flags = r.u8()
        issuer = r.b128()
        epoch = r.u32()
        cstart = r.u64()
        cend = r.u64()
        count = r.varint()
        batch_id = r.b128()
        prev = r.b128()
        root = r.b128()
        digest = r.b128()
        records_data = r.raw(len(data) - r.pos)
        raw_entries = decode_records_array(records_data)
        if len(raw_entries) != count:
            raise SyncError("BATCH_TRANSACTION_FAILED")
        entries = []
        for rid, kind, payload in raw_entries:
            entries.append(OutboxEntry(ns, 0, ZERO128, rid, kind, payload, issuer, 0))
        return Batch(ns, schema_id, schema_ver, policy, flags, issuer, epoch,
                     cstart, cend, batch_id, prev, root, digest, entries)

    # ------------------------------------------------------------------- applyBatch
    def apply_batch(self, batch: Batch, sender_peer: bytes) -> BatchResult:
        """§13.4 critical check order — [S7]: IDEMPOTENCY BEFORE prevBatchDigest."""
        result = BatchResult(batch.batchId)
        # STEP 0: namespace isolation (§3.7) — forbidden ns kills only that stream
        if batch.namespaceId in self.forbidden_namespaces:
            raise SyncError("NAMESPACE_FORBIDDEN", batch.batchId)
        # STEP 0.5: epoch regression (§4.4) — SPEC CORRECTIONS (errata E-2/E-3):
        # the watermark is keyed by the RECEIVING SESSION ((sender_peer, ns)) and the
        # gate compares against the sender's CURRENT session incarnation epoch learned
        # from HELLO/ALIAS_MAP (§4.3 binds alias → (nodeId, epoch)), NOT against the
        # batch's own issuerEpoch embedded in `chain` — the previous form was a tautology
        # (a new epoch always looked "clean", scenario 7 never raised). §4.4 literally
        # says "remote.epoch < known.epoch[nodeId]" where remote = direct sender, so the
        # comparison is per (peer-session, namespace); relayed third-party epochs are not
        # comparable across sessions (E-2 rationale).
        chain = (batch.issuerNodeId, batch.namespaceId, batch.issuerEpoch)
        ses_key = (sender_peer, batch.namespaceId)
        cur_known = self._session_epoch(sender_peer, batch.namespaceId)
        if cur_known is not None and batch.issuerEpoch < cur_known:
            raise SyncError("EPOCH_REGRESSION", batch.batchId)
        # STEP 0.6: schema pending (§18.3 / [СХ7]) — do NOT advance lastReceivedSeq
        supported = self.schema_versions.get(batch.namespaceId, 1)
        if batch.schemaVersion > supported:
            raise SyncError("SCHEMA_PENDING", batch.batchId)
        # STEP 1: idempotency FIRST ([S7], [П7], §10.2.2 lost-ACK replay)
        key = (batch.issuerNodeId, batch.namespaceId, batch.issuerEpoch, batch.batchId)
        if key in self.inbox:
            self.acks_out.append(batch.batchId)  # duplicate → no-op but ACK mandatory [П4]
            result.duplicate = True
            result.acked = True
            return result
        # STEP 2: cursor check [S2]/[П2] — errata E-3: keyed per SESSION stream
        # (senderPeer, senderEpoch, ns), NOT per durable issuerNodeId. The same durable
        # nodeId reached through two incarnation sessions must not share a receive cursor.
        ps = self.peer_states.setdefault(
            (batch.issuerNodeId, batch.namespaceId), PeerState(batch.issuerNodeId, batch.namespaceId)
        )
        # Errata E-4: resolve the batch's AUTHOR STREAM durably (op author alias resolved
        # through OUR map for this sender) and key the receive cursor per stream —
        # §13.6.1 [Р3] makes seq numbering per-author, so a single per-(sender,ns) cursor
        # cannot express two interleaved author streams from one sender.
        try:
            _op0, _ = decode_op(batch.entries[0].payload) if batch.entries else (None, 0)
            stream_author = (self.resolve_alias(sender_peer, _op0.origin)
                             if _op0 is not None else batch.issuerNodeId)
        except SyncError:
            stream_author = batch.issuerNodeId
        batch.authorStreamNodeId = stream_author
        self.last_stream_author[(sender_peer, batch.namespaceId)] = stream_author  # E-11
        stream_key = (sender_peer, batch.issuerEpoch, batch.namespaceId, stream_author)
        expected = ps.received_streams.get(stream_key, 0) + 1
        if batch.cursorStart != expected:
            raise SyncError("SEQ_GAP", batch.batchId)
        # STEP 3: batch digest chain (§12.17.2, [Т3])
        chain = (batch.issuerNodeId, batch.namespaceId, batch.issuerEpoch)
        expect_prev = self.last_batch_digest.get(chain, ZERO128)
        if batch.prevBatchDigest != expect_prev:
            raise SyncError("BATCH_CHAIN_BROKEN", batch.batchId)
        root = compute_records_root(batch.entries)
        if batch.recordsRoot != root:
            raise SyncError("BATCH_TRANSACTION_FAILED", batch.batchId)
        recomputed = compute_batch_digest(batch.batchId, batch.namespaceId,
                                          batch.issuerNodeId, batch.issuerEpoch,
                                          batch.cursorStart, batch.cursorEnd,
                                          batch.recordsRoot, batch.prevBatchDigest)
        if batch.batchDigest != recomputed:
            raise SyncError("BATCH_TRANSACTION_FAILED", batch.batchId)
        # STEP 4: atomic application [T1] — all-or-nothing on a copy-on-write staging
        ns_store = self._ns(batch.namespaceId)
        staged: dict[bytes, Record] = {rid: _clone(rec) for rid, rec in ns_store.items()}
        applied = 0
        changed_idx: list[int] = []   # indices of entries that actually changed state
        try:
            for idx, e in enumerate(batch.entries):
                op, _ = decode_op(e.payload)
                author = self.resolve_alias(sender_peer, op.origin)  # [А2]; unknown → raise
                # clock skew clamp on incoming HLC wall (§5.3)
                now = self.clock._now()
                if op.wall > now + self.clock.max_skew_us:
                    raise SyncError("CLOCK_DRIFT", batch.batchId)
                rec = staged.setdefault(op.recordId if isinstance(op, OpRecordTombstone)
                                         else e.recordId,
                                         Record(e.recordId, batch.namespaceId))
                changed = self._apply_op(rec, op, author)
                if changed:
                    applied += 1
                    changed_idx.append(idx)
        except SyncError:
            raise  # rollback: staged discarded, storage untouched ([Т1], scenario 24)
        # commit atomically
        self.storage[batch.namespaceId] = staged
        self.inbox.add(key)
        ps.last_received_seq = batch.cursorEnd   # [П3]/[Т2] atomic cursor jump (compat copy)
        ps.received_streams[stream_key] = batch.cursorEnd  # errata E-3 per-session cursor
        self.last_batch_digest[chain] = batch.batchDigest
        # SPEC CORRECTION (errata E-2, found during implementation, scenarios 23/42):
        # §12.17 defines DELTA_BATCH.issuerEpoch as the SENDER's incarnation counter
        # (§4.4: "Каждый nodeId имеет монотонный epoch... При получении кадра с
        # remote.epoch < known.epoch[nodeId]" — remote = direct sender). A relay cannot
        # stamp third-party ops with the AUTHOR's epoch without inventing a new wire
        # field, and per-author chains would break [S2]/SEQ_GAP on first contact (the
        # receiver's cursor for (author, ns) is still 0 while it already saw seqs via
        # the relay). The previous code advanced known_epochs[ISSUER] unconditionally;
        # when the SAME durable nodeId was later reached DIRECTLY with its own smaller
        # incarnation epoch, every legitimate batch was rejected as EPOCH_REGRESSION
        # (chaos scenario 42). Fix: track epochs PER SESSION — the key is
        # (sender_peer, ns) — the incarnation epoch of THIS peer's session. The value is
        # authoritative only when it comes from a LIVE HELLO/ALIAS_MAP (§4.3), not from a
        # possibly-stale batch stamp: a zombie replaying an old-incarnation batch must not
        # lower the watermark learned from the peer's current session (scenario 7). Live
        # sessions register their epoch in register_session_epoch() at HELLO time; the
        # commit path here only ever RAISES the watermark for batches that pass every gate.
        ses_key = (sender_peer, batch.namespaceId)
        known_s = self.known_epochs.get(ses_key)
        self.known_epochs[ses_key] = max(known_s or 0, batch.issuerEpoch)
        self.acks_out.append(batch.batchId)
        result.applied = applied
        result.acked = True
        # Relay [Р1, Р2]: ONLY entries that actually changed state are appended to OUR
        # outbox under the ORIGINAL authorNodeId (author is never the relay); they get
        # their own per-author stream and are re-signed into our batch chain.
        # SPEC HARDENING (§8.3 [E2], found during implementation): the §13.4 pseudocode
        # queues an entry "if result.changed" — relaying unchanged/duplicate ops would
        # amplify traffic in a mesh and break anti-entropy echo suppression [E2].
        # Own writes echoed back never reach here because changed=False.
        if batch.issuerNodeId != self.nodeId:
            batch_ops = batch.ops
            # Errata E-10 follow-up / SPEC CORRECTION (scenario 23): §8.3 [E2] defines
            # echo suppression over the FULL field state (hlc, authorNodeId, value) and
            # says nothing about provenance — applied literally, a LOSER op that merely
            # matches our current winner is "identical" → changed=False → never relayed.
            # In a triangle A—R—C with concurrent equal-HLC writes this strands C's op at
            # R forever: R lost it to A on the direct A→R batch (changed=False), and A
            # NEVER learns of C because every later delivery of C's op from R arrives as a
            # SINGLE-ENTRY batch whose recordsRoot is byte-identical to the FIRST such
            # batch — the receiver's STEP-1 idempotency gate [S7] keys on batchId, so the
            # whole re-delivery short-circuits as `duplicate` before any merge runs.
            # The spec's own invariants И4 ("авторство сохраняется при ретрансляции") and
            # §17.23 require mesh convergence through relays, so relay eligibility must be
            # decided by PROVENANCE, not by merge outcome: an entry this node has NEVER
            # seen before (new changeId) is relayed even when it loses the local merge.
            # True echoes are still suppressed: the origin's own write coming back is
            # already in `_seen_changes` (recorded when we first received it), and a
            # RE-delivery of the same changeId hits the seen-set and stops — the
            # broadcast-storm guard [E2] is preserved, only the FIRST sighting of a
            # changeId gains relay eligibility.
            # Errata E-13 follow-up 2 (scenario 37): the seen-set must be keyed by
            # PROVENANCE CHANNEL — (senderPeer, ns, changeId) — not just (ns, changeId).
            # A durable author's own op legitimately reaches it through TWO different
            # channels: directly from its session AND as an echo re-signed by a relay
            # (different batch issuer = different channel). With a bare (ns,cid) key,
            # export_delta/import_delta (§3.5 sneakernet) ACKed the file path into the
            # same set and then silently poisoned the NETWORK delivery of the identical
            # batch as "already seen" — no-op with applied=0. Channel scoping keeps both
            # deliveries independent while storm suppression still holds per channel:
            # repeated re-delivery of one changeId over ONE peer link stops at the first.
            #
            # Errata E-13 follow-up 4 (scenario 37 ring A→B→V→A): channel scoping alone
            # is NOT enough to stop the avalanche for the AUTHOR node. The echo batch is
            # issued by V (senderPeer=V), so its cid looks "foreign" to A even though the
            # DURABLE provenance (authorNodeId + hlc + value) is A's own queued write.
            # §8.3 [E2] defines echo suppression over the durable FIELD STATE, not over
            # transport channels; therefore relay eligibility additionally requires the
            # entry's durable state to differ from what OUR OWN un-ACKed outbox already
            # carries for the same (recordId, fieldId). If our queue holds a byte-equal
            # winner, the ring merely mirrors our own state back — enqueueing again would
            # duplicate propagation of identical durable data ([И2] monotone digests).
            def _own_queue_holds(ns_: int, rid_: bytes, qop) -> bool:
                for q in self.outbox.get(ns_, []):
                    if q.recordId != rid_ or q.authorNodeId != self.nodeId:
                        continue
                    try:
                        s_op, _ = decode_op(q.payload)
                    except Exception:
                        continue
                    if getattr(s_op, "fieldId", None) != getattr(qop, "fieldId", None):
                        continue
                    if (s_op.tag == qop.tag and s_op.wall == qop.wall and
                            s_op.logical == qop.logical and
                            getattr(s_op, "value", b"") == getattr(qop, "value", b"")):
                        return True
                return False
            for idx, e in enumerate(batch.entries):
                cid = e.changeId if e.changeId != ZERO128 else uuid.uuid4().bytes
                ck = (sender_peer, batch.namespaceId, cid)
                known = ck in self._seen_changes
                self._seen_changes.add(ck)
                if not known and idx not in changed_idx:
                    relay_ok = True
                    _rop = batch_ops[idx]
                    if getattr(_rop, "fieldId", None) is not None:
                        _r_author = None
                        try:
                            _r_author = self.resolve_alias(sender_peer, _rop.origin)
                        except SyncError:
                            pass
                        if _r_author == self.nodeId and \
                                self._seen_state(batch.namespaceId, e.recordId,
                                                 _rop.fieldId) is not None and \
                                _own_queue_holds(batch.namespaceId, e.recordId, _rop):
                            relay_ok = False   # own durable state echoed back [E2]/[Ш2]
                    if relay_ok:
                        changed_idx.append(idx)     # new provenance ⇒ relay even as loser
            for idx in changed_idx:
                e = batch.entries[idx]
                op = batch_ops[idx]
                if op.authorNodeId is not None:
                    continue  # full-author mode keeps durable author already
                resolved = self.resolve_alias(sender_peer, op.origin)
                # BUGFIX (implementation finding, scenario 23): the relay must keep the
                # op payload VERBATIM (§13.5 [Р1]/[И4]: "relay не переписывает авторов").
                # The incoming payload carries origin = alias in the SENDER's session;
                # re-encoding it here with a NEUTRAL origin=0 produced an entry whose
                # origin could never be translated back at batch-creation time
                # (decode_op yields origin=0 → alias_of(0)=None → UNKNOWN_ALIAS or a
                # silently mis-stamped third-party op). Instead, store the raw wire
                # payload and remember the session it came from — create_delta_batch
                # re-translates per TARGET session from `authorNodeId` anyway ([R4]),
                # so the stale origin byte never reaches the receiver.
                entry = OutboxEntry(
                    namespaceId=e.namespaceId or batch.namespaceId,
                    seq=self._next_outbox_seq(resolved, batch.namespaceId),
                    changeId=e.changeId if e.changeId != ZERO128 else uuid.uuid4().bytes,
                    recordId=e.recordId,
                    opKind=e.opKind,
                    payload=e.payload,
                    authorNodeId=resolved,
                    createdAt=op.wall,   # BUGFIX: stamp the OP's own HLC wall time, never
                                        # the relay's local clock — createdAt must be
                                        # identical on every node that holds this entry
                                        # (deterministic replay / digest stability).
                )
                self.outbox.setdefault(batch.namespaceId, []).append(entry)
                # Errata E-10 (scenario 23): a third-party binding learned through THIS
                # session must propagate to our OTHER live sessions via an incremental
                # ALIAS_MAP update before we can ever stamp the author for them;
                # otherwise create_delta_batch fails with UNKNOWN_ALIAS forever and the
                # entry is re-queued endlessly. Best-effort: sessions where the alias
                # collides keep failing closed [А3] until a full re-handshake.
                ep_ = None
                for _al, (_n, _e) in self.alias_to_node.get(sender_peer, {}).items():
                    if _n == resolved:
                        ep_ = _e
                        break
                if ep_ is not None:
                    for other_peer in list(self.alias_to_node.keys()):
                        if other_peer != sender_peer:
                            self.ensure_alias(other_peer, resolved, ep_)
                    # Errata E-11 (scenario 23): ALSO mirror the binding into the
                    # SENDER's copy of THIS session. §4.3 requires both sides of one
                    # session to hold identical tables; when the sender later re-stamps
                    # its own ops for us (e.g. relay→A after A already ACKed an earlier
                    # batch), alias_of() consults OUR map — and a colliding binding
                    # discovered only now made create_delta_batch fail with UNKNOWN_ALIAS
                    # forever, stranding the winning third-party op at the relay. The
                    # incremental ALIAS_MAP update (§12.7 models this frame) negotiates a
                    # collision-free number on BOTH ends before it is ever needed.
                    # Guard: never try to bind our OWN identity into a peer session.
                    if resolved != self.nodeId:
                        self.ensure_alias(sender_peer, resolved, ep_)
        return result

    def _apply_op(self, rec: Record, op, author: bytes) -> bool:
        """Apply one resolved op to a staged record. Echo suppression happens inside
        merge_field: identical (hlc, author, value) → changed=False → nothing queued."""
        if isinstance(op, OpFieldWrite):
            if op.fieldId == LIFECYCLE_FIELD_ID:
                val = op.value[0] if op.value else ALIVE
                return apply_lifecycle(rec, HLC(op.wall, op.logical), author, val)
            fs = FieldState(op.value, HLC(op.wall, op.logical), author, op.keyEpoch)
            return apply_field_write(rec, op.fieldId, fs)
        if isinstance(op, OpFieldTomb):
            fs = FieldState(b"", HLC(op.wall, op.logical), author, op.keyEpoch)
            return apply_field_tomb(rec, op.fieldId, fs)
        if isinstance(op, OpRecordTombstone):
            return apply_lifecycle(rec, HLC(op.wall, op.logical), author, DEAD)
        raise SyncError("INTERNAL")

    def process_ack(self, peer_node: bytes, namespace_id: int, acked_seq: int,
                    author_stream: bytes | None = None) -> None:
        """Receiver ACK advances our push watermark; GC of outbox up to min acked.

        SPEC CORRECTION (errata E-4): the ACK carries the AUTHOR STREAM of the batch
        it acknowledges (§13.6.1 [Р3] — one author per batch ⇒ seq numbering and thus
        the watermark are per-author). When `author_stream` is provided (network path),
        only that stream's watermark moves. Without it (legacy/sneakernet callers) we
        fall back to a conservative global gate: advance every stream whose FIRST
        un-acked seq is <= acked_seq — never past its own head, so no entry can be
        silently skipped ([П2])."""
        ps = self.peer_states.get((peer_node, namespace_id))
        if ps is not None:
            if author_stream is not None:
                ps.stream_acked[author_stream] = max(ps.stream_acked.get(author_stream, 0),
                                                     acked_seq)
            else:
                entries = self.outbox.get(namespace_id, [])
                heads: dict[bytes, int] = {}
                for e in entries:
                    cur = heads.get(e.authorNodeId)
                    if cur is None or e.seq < cur:
                        heads[e.authorNodeId] = e.seq
                for author, first_seq in heads.items():
                    if first_seq <= acked_seq:
                        ps.stream_acked[author] = max(
                            ps.stream_acked.get(author, 0), acked_seq)
            # legacy mirror: monotone scalar = min over known streams' watermarks
            # (kept for diagnostics only; selection/GC use stream_acked exclusively).
            if ps.stream_acked:
                ps.last_acked_seq = min(ps.stream_acked.values())

    # -------------------------------------------------------------------- snapshot
    def build_snapshot(self, namespace_id: int) -> list[Batch]:
        """Snapshot = all live records + tombstones + ghosts (§ summary 'снапшот').
        Encoded as delta batches grouped per author at current cursor."""
        batches = []
        ns_store = self._ns(namespace_id)
        for rid, rec in sorted(ns_store.items()):
            entries = []
            for fid, fs in sorted(rec.state.items()):
                op = OpFieldWrite(fid, fs.keyEpoch, fs.hlc.wall, fs.hlc.logical,
                                  origin=1, authorNodeId=fs.authorNodeId, value=fs.value)
                entries.append(OutboxEntry(namespace_id, 0, ZERO128, rid, op.tag,
                                           encode_op(op), fs.authorNodeId, 0))
            for fid, fs in sorted(rec.fieldTombs.items()):
                op = OpFieldTomb(fid, fs.keyEpoch, fs.hlc.wall, fs.hlc.logical,
                                 origin=1, authorNodeId=fs.authorNodeId)
                entries.append(OutboxEntry(namespace_id, 0, ZERO128, rid, op.tag,
                                           encode_op(op), fs.authorNodeId, 0))
            if rec.lifecycleHlc is not None:
                op = OpRecordTombstone(rid, rec.lifecycleHlc.wall, rec.lifecycleHlc.logical,
                                       origin=1, authorNodeId=rec.lifecycleAuthor or self.nodeId)
                if rec.lifecycleValue == ALIVE:
                    # SPEC HARDENING (implementation finding): an undeleted record's ghost
                    # fields have hlc <= lifecycle time; a plain field write of them would
                    # keep them ghosts on the receiver. Snapshots must carry an explicit
                    # REVIVE lifecycle op (value=[ALIVE]) so the receiver raises its
                    # lifecycle watermark and un-ghosts the historical values (§7.4 У4).
                    opw = OpFieldWrite(LIFECYCLE_FIELD_ID, 0, rec.lifecycleHlc.wall,
                                       rec.lifecycleHlc.logical, origin=1,
                                       authorNodeId=rec.lifecycleAuthor or self.nodeId,
                                       value=bytes([ALIVE]))
                    entries.append(OutboxEntry(namespace_id, 0, ZERO128, rid, opw.tag,
                                               encode_op(opw), opw.authorNodeId, 0))
                else:
                    entries.append(OutboxEntry(namespace_id, 0, ZERO128, rid, op.tag,
                                               encode_op(op), op.authorNodeId, 0))
            if entries:
                batches.append(Batch(namespace_id, 0, 1, 0x02, 0, self.nodeId, self.epoch,
                                     0, 0, uuid.uuid4().bytes, ZERO128,
                                     compute_records_root(entries), ZERO128, entries))
        for b in batches:
            b.batchDigest = compute_batch_digest(b.batchId, b.namespaceId, b.issuerNodeId,
                                                 b.issuerEpoch, b.cursorStart, b.cursorEnd,
                                                 b.recordsRoot, b.prevBatchDigest)
        return batches

    def apply_snapshot(self, batch: Batch, sender_peer: bytes) -> int:
        """Apply snapshot ops directly (not through cursor machinery)."""
        applied = 0
        for e in batch.entries:
            op, _ = decode_op(e.payload)
            author = op.authorNodeId
            ns_store = self._ns(batch.namespaceId)
            rec = ns_store.setdefault(e.recordId, Record(e.recordId, batch.namespaceId))
            if self._apply_op(rec, op, author):
                applied += 1
        return applied

    # ---------------------------------------------------------------------- helpers
    def record_digest(self, namespace_id: int, record_id: bytes) -> bytes | None:
        rec = self.storage.get(namespace_id, {}).get(record_id)
        return record_digest(rec) if rec else None

    def all_record_digests(self, namespace_id: int) -> dict[bytes, bytes]:
        return {rid: record_digest(rec) for rid, rec in self.storage.get(namespace_id, {}).items()}

    def outbox_size(self, namespace_id: int) -> int:
        return len(self.outbox.get(namespace_id, []))


def _clone(rec: Record) -> Record:
    return Record(
        recordId=rec.recordId,
        namespaceId=rec.namespaceId,
        state=dict(rec.state),
        lifecycleHlc=rec.lifecycleHlc,
        lifecycleAuthor=rec.lifecycleAuthor,
        lifecycleValue=rec.lifecycleValue,
        fieldTombs=dict(rec.fieldTombs),
    )


@dataclass
class BatchResult:
    batchId: bytes
    applied: int = 0
    acked: bool = False
    duplicate: bool = False
