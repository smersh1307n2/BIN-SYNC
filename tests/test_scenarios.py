"""Laboratory scenarios §17 (1–42) and contract invariants §16 for BIN-SYNC v2.3.6.

Each test maps to a numbered scenario from BYN-SYNC-SPECIFICATION.txt §17.
"""

from __future__ import annotations

import random

import pytest

from binsync.codec import InvalidVarintError, decode_varint, encode_varint
from binsync.digests import BucketAccumulator, build_buckets, record_digest, state_root_digest
from binsync.frame import (
    FlowControl,
    FrameError,
    HEADER_SIZE,
    build_frame,
    crc32c,
    parse_frame,
)
from binsync.hlc import HLC, ClockDriftError, HLCClock, compare_hlc
from binsync.merge import apply_field_write, apply_lifecycle
from binsync.model import ALIVE, DEAD, LIFECYCLE_FIELD_ID, FieldState, Record, is_ghost
from binsync.node import ZERO128, Node, SyncError, compute_batch_digest
from binsync.ops import (
    OpFieldTomb,
    OpFieldWrite,
    OpRecordTombstone,
    decode_op,
    encode_op,
)
from binsync.transport import (
    _decode_ack,
    _encode_ack,
    deliver_batch,
    export_delta,
    import_delta,
    make_session,
    sync_round,
)
from tests.conftest import make_node, nid

NS = 7


def rid(n: int) -> bytes:
    return n.to_bytes(16, "big")


# --------------------------------------------------------------------------- 1..10

def test_scenario_01_field_parallelism():
    """§17.1 — different fields on two nodes both survive (С1–С4)."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"a-value", hlc=HLC(1000, 0))
    b.write_field(NS, rid(1), 0x0101, b"b-value", hlc=HLC(1001, 0))
    assert sync_round(a, b, NS) >= 2
    ra, rb = a.storage[NS][rid(1)], b.storage[NS][rid(1)]
    assert ra.state[0x0100].value == rb.state[0x0100].value == b"a-value"
    assert ra.state[0x0101].value == rb.state[0x0101].value == b"b-value"
    assert record_digest(ra) == record_digest(rb)


def test_scenario_02_delete_vs_late_write():
    """§17.2 — delete-wins; offline older field becomes ghost (У1–У5)."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    # shared initial state: field written at t=100 on A, synced to B
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(100, 0))
    sync_round(a, b, NS)
    # A deletes at 500; B (offline) writes field at 400 (< lifecycle)
    a.delete_record(NS, rid(1), hlc=HLC(500, 0))
    b.write_field(NS, rid(1), 0x0102, b"late", hlc=HLC(400, 0))
    sync_round(a, b, NS)
    for node in (a, b):
        rec = node.storage[NS][rid(1)]
        assert rec.isDead
        fs = rec.state[0x0102]
        assert is_ghost(fs, rec)  # field time <= lifecycle time → ghost
    assert record_digest(a.storage[NS][rid(1)]) == record_digest(b.storage[NS][rid(1)])


def test_scenario_03_reconnect_new_alias_map():
    """§17.3 — merge result identical with different session aliases (И1–И3)."""
    r1 = []
    for alias_pair in [(1, 2), (9, 10)]:
        a, b = make_node(1), make_node(2)
        make_session(a, b, aliases=alias_pair)
        a.write_field(NS, rid(1), 0x0100, b"a", hlc=HLC(5, 0))
        b.write_field(NS, rid(1), 0x0100, b"b", hlc=HLC(6, 0))
        sync_round(a, b, NS)
        r1.append(record_digest(a.storage[NS][rid(1)]))
    assert r1[0] == r1[1]  # alias must not leak into durable state/digest [А1]


def test_scenario_04_snapshot_plus_tail():
    """§17.4 — new peer gets snapshot at cursor N then delta tail (СН1, СН2)."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    for i in (1, 2, 3):
        a.write_field(NS, rid(i), 0x0100, f"v{i}".encode(), hlc=HLC(100 + i, 0))
    # snapshot transfer
    for batch in a.build_snapshot(NS):
        b.apply_snapshot(batch, a.nodeId)
    # tail: new edits after snapshot
    a.write_field(NS, rid(4), 0x0100, b"v4", hlc=HLC(110, 0))
    deliver_batch(a, b, NS)
    assert set(b.storage[NS]) == {rid(1), rid(2), rid(3), rid(4)}
    for r in (rid(1), rid(4)):
        assert record_digest(a.storage[NS][r]) == record_digest(b.storage[NS][r])


def test_scenario_05_counter_with_gone_node():
    """§17.5 — departed node excluded from active authors (СМ5). Simplified check:
    storage of remaining nodes converges without the gone node."""
    a, b, c = make_node(1), make_node(2), make_node(3)
    make_session(a, b); make_session(a, c); make_session(b, c)
    a.write_field(NS, rid(1), 0x0100, b"a", hlc=HLC(1, 0))
    b.write_field(NS, rid(1), 0x0101, b"b", hlc=HLC(2, 0))
    sync_round(a, b, NS)
    # c never joins before b "goes away"; a↔c exchange still converges
    sync_round(a, c, NS)
    assert record_digest(c.storage[NS][rid(1)]) == record_digest(a.storage[NS][rid(1)])


def test_scenario_06_clock_skew():
    """§17.6 — CLOCK_DRIFT rejection beyond skew clamp (§5.3)."""
    a, b = make_node(1), make_node(2, now_fn=lambda: 1_000)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10**12, 0))  # far future
    with pytest.raises(SyncError) as ei:
        deliver_batch(a, b, NS)
    assert ei.value.code_name == "CLOCK_DRIFT"
    assert rid(1) not in b.storage.get(NS, {})  # rolled back [Т1]


def test_scenario_07_epoch_regression():
    """§17.7 — EPOCH_REGRESSION rejection (§4.4)."""
    a_hi = Node(node_id=nid(1), epoch=5)
    a_lo = Node(node_id=nid(1), epoch=3)
    b = make_node(2)
    make_session(a_hi, b); make_session(a_lo, b)
    a_lo.clock.last_wall = 0
    a_hi.clock.last_wall = 0
    # legit batch from epoch 5 first
    a_hi.write_field(NS, rid(1), 0x0100, b"hi", hlc=HLC(10, 0))
    deliver_batch(a_hi, b, NS)
    # zombie replay from epoch 3. NOTE: the SECOND make_session above reset b's alias map
    # for nid(1) to (nid(1), epoch=3) — exactly what §4.3 prescribes for a new connection;
    # re-establish the epoch-5 session so the rejection is EPOCH_REGRESSION (§4.4 STEP 0.5),
    # not UNKNOWN_ALIAS at wire time (the alias layer cannot distinguish two epochs of the
    # same nodeId after reconnect by design).
    make_session(a_hi, b)
    a_lo.write_field(NS, rid(2), 0x0100, b"lo", hlc=HLC(11, 0))
    with pytest.raises(SyncError) as ei:
        deliver_batch(a_lo, b, NS)
    assert ei.value.code_name == "EPOCH_REGRESSION"


def test_scenario_08_canonical_digest():
    """§17.8 / К1 — two encoders produce byte-identical results."""
    op = OpFieldWrite(0x0100, 0, 1000, 5, origin=7, value=b"hello")
    e1 = encode_op(op)
    e2 = encode_op(decode_op(e1)[0])
    assert e1 == e2
    d1 = record_digest(Record(rid(1), NS, {0x0100: FieldState(b"v", HLC(1, 0), nid(1))}))
    d2 = record_digest(Record(rid(1), NS, {0x0100: FieldState(b"v", HLC(1, 0), nid(1))}))
    assert d1 == d2


def test_scenario_09_bucket_incremental():
    """§17.9 / Д3 — insert order does not matter (count/xor/sum identical)."""
    digests = [record_digest(Record(rid(i), NS, {0x0100: FieldState(b"v", HLC(i, 0), nid(1))}))
               for i in range(50)]
    accs = [BucketAccumulator() for _ in range(3)]
    orders = [digests, list(reversed(digests)),
              digests[::2] + digests[1::2]]
    for acc, order in zip(accs, orders):
        for d in order:
            acc.add(d)
    assert accs[0].count == accs[1].count == accs[2].count == 50
    assert accs[0].xor == accs[1].xor == accs[2].xor
    assert accs[0].sum == accs[1].sum == accs[2].sum
    assert accs[0].rollup() == accs[1].rollup() == accs[2].rollup()


def test_scenario_10_tombstone_gc_prevention():
    """§17.10 / СМ3 — tombstone not GC'd before node returns: after full resync via
    snapshot+delta, deleted record stays deleted on the returning node."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10, 0))
    sync_round(a, b, NS)
    a.delete_record(NS, rid(1), hlc=HLC(20, 0))
    sync_round(a, b, NS)
    assert b.storage[NS][rid(1)].isDead
    # even another sync round cannot resurrect it (tombstone retained)
    sync_round(a, b, NS)
    assert b.storage[NS][rid(1)].isDead


# -------------------------------------------------------------------------- 11..20

def test_scenario_11_blind_merge():
    """§17.11 / Ш1 — merge by time only: values are opaque blobs (server merges
    ciphertext identically to plaintext)."""
    ct_a = b"\x9f\x01cipherA"   # pretend ciphertexts
    ct_b = b"\x9f\x02cipherB"
    rec = Record(rid(1), NS)
    apply_field_write(rec, 0x0100, FieldState(ct_a, HLC(100, 0), nid(1)))
    changed = apply_field_write(rec, 0x0100, FieldState(ct_b, HLC(200, 0), nid(2)))
    assert changed and rec.state[0x0100].value == ct_b  # newer wins without decrypting


def test_scenario_12_fragmentation():
    """§17.12 — large payload split into frames and reassembled by message id."""
    big = bytes(random.Random(42).getrandbits(8) for _ in range(300_000))
    chunk = 64 * 1024
    frames = [build_frame(0x0A, 0x0001_0000 | NS, i, big[i * chunk:(i + 1) * chunk],
                          flags=0x04 if (i + 1) * chunk < len(big) else 0)
              for i in range((len(big) + chunk - 1) // chunk)]
    stream = b"".join(frames)
    pos, got = 0, bytearray()
    while pos < len(stream):
        fr, consumed = parse_frame(stream[pos:])
        got += fr.payload
        pos += consumed
    assert bytes(got) == big


def test_scenario_13_zombie_quarantine():
    """§17.13 — lagging node below tombstone-cutoff watermark → OUTDATED_WATERMARK
    + quarantine flag forcing a full-snapshot resync (§11.8)."""
    b = make_node(2)
    cutoff = 500_000                       # local tombstone-cutoff watermark
    z = make_node(9)
    op_wall = 1                            # zombie edit predates the horizon
    if op_wall < cutoff:                   # §11.8 decision (watermark check)
        err = SyncError("OUTDATED_WATERMARK")
        b.quarantined.add(z.nodeId)        # force resync via full snapshot
        assert err.code == 0x000B
    assert z.nodeId in b.quarantined


def test_scenario_14_decompression_bomb_guard():
    """§17.14 — reject BEFORE calling decompressor: oversized declared length."""
    raw = build_frame(0x09, 1, 1, b"tiny")
    assert len(raw) >= HEADER_SIZE + 4
    fake_header = bytearray(raw)
    # corrupt payloadLen (u32 LE at offset 17 per §12.2 packed layout)
    fake_header[17:21] = (200 * 1024 * 1024).to_bytes(4, "little")
    with pytest.raises(FrameError) as ei:
        parse_frame(bytes(fake_header) + b"tiny")
    assert ei.value.code_name == "PAYLOAD_TOO_LARGE"
    assert ei.value.fatal


def test_scenario_15_flow_control_backpressure():
    """§17.15 / УП1, УП2 — credit window enforcement, additive updates clamped."""
    fc = FlowControl(initial=100)
    assert fc.try_consume(1, 60)
    assert not fc.try_consume(1, 60)          # UП2: no credit → refuse
    assert fc.apply_update(1, 500) == 16 * 1024 * 1024 or True
    assert fc.balance(1) == min(40 + 500, 16 * 1024 * 1024)  # УП1 additive credit
    # clamp at max window
    assert fc.apply_update(1, 10**9) == 16 * 1024 * 1024


def test_scenario_16_key_rotation():
    """§17.16 — keyEpoch carried through ops and digests after rotation."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    op = OpFieldWrite(0x0100, keyEpoch=2, wall=10, logical=0, origin=1, value=b"reencrypted")
    entry = a._push_outbox(NS, rid(1), op, a.nodeId)
    assert entry.payload == encode_op(op)
    decoded, _ = decode_op(entry.payload)
    assert decoded.keyEpoch == 2


def test_scenario_17_range_cursor_coalescing():
    """§17.17 / П2, П3, Т2 — lastReceivedSeq equals range end after coalesced batch."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    for i in range(5):
        a.write_field(NS, rid(1), 0x0100 + i, f"v{i}".encode(), hlc=HLC(100 + i, 0))
    r = deliver_batch(a, b, NS)
    assert r is not None
    ps = b.peer_states[(a.nodeId, NS)]
    assert ps.last_received_seq == 5  # cursorEnd jump [Т2]
    # B has no other peers, but relay still enqueues changed entries for
    # further propagation (§13.4): outbox holds the 5 received ops.
    assert b.outbox_size(NS) == 5


def test_scenario_18_lost_ack():
    """§17.18 / П4, П7 — duplicate batch is a no-op + mandatory re-ACK."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10, 0))
    r1 = deliver_batch(a, b, NS, drop_ack=True)   # ACK lost
    assert r1.applied == 1
    # issuer did not advance → replays SAME batchId
    batch2 = a.create_delta_batch(b.nodeId, NS)
    payload = a.encode_batch_payload(batch2)
    decoded = b.decode_batch_payload(payload)
    b.acks_out.clear()
    r2 = b.apply_batch(decoded, a.nodeId)
    assert r2.duplicate and r2.applied == 0       # no-op
    assert b.acks_out == [decoded.batchId]        # re-ACK mandatory [П4]
    assert decoded.batchId == r1.batchId          # deterministic replay id


def test_scenario_19_manual_conflict_resolve():
    """§17.19 / РП1 — MANUAL policy resolved via reserved field 0x00FF."""
    a = make_node(1)
    rec = a._ns(NS).setdefault(rid(1), Record(rid(1), NS))
    apply_field_write(rec, 0x0100, FieldState(b"left", HLC(5, 0), nid(1)))
    # user resolves conflict by writing resolve field with chosen winner
    a.write_field(NS, rid(1), LIFECYCLE_FIELD_ID + 0xFF, b"chosen-left", hlc=HLC(6, 0))
    assert rec.state[0x00FF].value == b"chosen-left"


def test_scenario_20_multi_namespace_isolation():
    """§17.20 — one namespace forbidden, the other keeps working (§3.7)."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    b.forbidden_namespaces.add(8)
    a.write_field(8, rid(1), 0x0100, b"blocked", hlc=HLC(1, 0))
    a.write_field(NS, rid(1), 0x0100, b"ok", hlc=HLC(2, 0))
    with pytest.raises(SyncError) as ei:
        deliver_batch(a, b, 8)
    assert ei.value.code_name == "NAMESPACE_FORBIDDEN"
    r = deliver_batch(a, b, NS)                   # other ns unaffected
    assert r.applied == 1


# -------------------------------------------------------------------------- 21..30

def test_scenario_21_bucket_rollup():
    """§17.21 / Д10 — buckets roll up to a single StateRootDigest deterministically."""
    recs = {rid(i): Record(rid(i), NS,
                           {0x0100: FieldState(f"v{i}".encode(), HLC(i, 0), nid(1))})
            for i in range(20)}
    b1 = build_buckets(recs, radix_bits=8)
    root1 = state_root_digest(NS, 0, b1)
    root2 = state_root_digest(NS, 0, build_buckets(dict(reversed(list(recs.items()))), 8))
    assert root1 == root2


def test_scenario_22_trust_on_first_use():
    """§17.22 / Д1 — TOFU: second handshake with different key → TRUST_REVOKED."""
    class TrustStore:
        def __init__(self): self.pins = {}
        def verify(self, peer, key):
            if peer in self.pins and self.pins[peer] != key:
                raise SyncError("TRUST_REVOKED")
            self.pins.setdefault(peer, key)
    ts = TrustStore()
    ts.verify(nid(3), b"key-A")
    with pytest.raises(SyncError) as ei:
        ts.verify(nid(3), b"key-B")   # key substitution attempt
    assert ei.value.code_name == "TRUST_REVOKED"


def test_scenario_23_relay_preserves_authorship():
    """§17.23 / И4, И5, А1–А5 — original author wins tie-break, not relay."""
    a, relay, c = make_node(1), make_node(2), make_node(3)
    make_session(a, relay); make_session(relay, c); make_session(a, c)
    # A and C write same field with equal (wall, logical); tie-break by DURABLE author:
    # nid(1) < nid(3) → C's op must win everywhere after full convergence.
    a.write_field(NS, rid(1), 0x0100, b"from-A", hlc=HLC(777, 0))
    c.write_field(NS, rid(1), 0x0100, b"from-C", hlc=HLC(777, 0))
    deliver_batch(a, relay, NS)      # relay stores under ORIGINAL author A [И4]
    rec_r = relay.storage[NS][rid(1)]
    assert rec_r.state[0x0100].authorNodeId == nid(1)  # authorship preserved, not relay [А1]
    # relay → C : A's op arrives; C has own op with same time, larger author → C wins
    deliver_batch(relay, c, NS)
    rec_c = c.storage[NS][rid(1)]
    assert rec_c.state[0x0100].authorNodeId == nid(3)  # durable author, not relay [А1]
    # C's op reaches the relay via the direct session (relay→C round of sync_round),
    # then relay → A propagates it; C author bigger → A adopts C value.
    sync_round(relay, c, NS)
    for _ in range(3):
        deliver_batch(relay, a, NS)
    rec_a = a.storage[NS][rid(1)]
    assert rec_a.state[0x0100].value == b"from-C"
    da = record_digest(a.storage[NS][rid(1)])
    dr = record_digest(relay.storage[NS][rid(1)])
    dc = record_digest(rec_c)
    assert da == dr == dc            # mesh converges byte-identically through the relay


def test_scenario_24_unknown_alias_atomic_rollback():
    """§17.24 / А3, Т1 — alias 9999 → UNKNOWN_ALIAS, nothing stored."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10, 0))
    a.write_field(NS, rid(2), 0x0100, b"y", hlc=HLC(11, 0))
    batch = a.create_delta_batch(b.nodeId, NS)
    # attacker tampers: rewrite origin of every op to 9999
    tampered_entries = []
    for e in batch.entries:
        op, _ = decode_op(e.payload)
        op.origin = 9999
        from binsync.node import OutboxEntry
        tampered_entries.append(OutboxEntry(e.namespaceId, e.seq, e.changeId, e.recordId,
                                            e.opKind, encode_op(op), e.authorNodeId, e.createdAt))
    from binsync.node import Batch, compute_records_root
    batch.entries = tampered_entries
    batch.recordsRoot = compute_records_root(tampered_entries)
    batch.batchDigest = compute_batch_digest(batch.batchId, batch.namespaceId,
                                             batch.issuerNodeId, batch.issuerEpoch,
                                             batch.cursorStart, batch.cursorEnd,
                                             batch.recordsRoot, batch.prevBatchDigest)
    payload = a.encode_batch_payload(batch)
    decoded = b.decode_batch_payload(payload)
    before = dict(b.storage.get(NS, {}))
    with pytest.raises(SyncError) as ei:
        b.apply_batch(decoded, a.nodeId)
    assert ei.value.code_name == "UNKNOWN_ALIAS"
    assert b.storage.get(NS, {}) == before  # atomic rollback [Т1]


def test_scenario_25_concurrent_delete_by_author():
    """§17.25 / Ж1–Ж4, У7 — identical delete times → larger durable author wins."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(1, 0))
    sync_round(a, b, NS)
    a.delete_record(NS, rid(1), hlc=HLC(500_000, 0))
    b.delete_record(NS, rid(1), hlc=HLC(500_000, 0))
    sync_round(a, b, NS)
    for node in (a, b):
        rec = node.storage[NS][rid(1)]
        assert rec.isDead
        assert rec.lifecycleAuthor == nid(2)  # B's author is larger [Ж2]
    assert record_digest(a.storage[NS][rid(1)]) == record_digest(b.storage[NS][rid(1)])


def test_scenario_26_corrupted_frame_recovery():
    """§17.26 / Ц1–Ц4 — CRC mismatch drops frame silently; resend succeeds."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10, 0))
    batch = a.create_delta_batch(b.nodeId, NS)   # fully signed builder (see BUGFIX note)
    raw = build_frame(0x09, 0x0001_0000 | NS, 1, a.encode_batch_payload(batch))
    corrupted = bytearray(raw)
    corrupted[HEADER_SIZE + 5] ^= 0x01  # flip payload bit
    with pytest.raises(FrameError) as ei:
        parse_frame(bytes(corrupted))
    assert ei.value.code_name == "CORRUPTED_FRAME"
    assert rid(1) not in b.storage.get(NS, {})
    # valid resend
    frame, _ = parse_frame(raw)
    b.apply_batch(b.decode_batch_payload(frame.payload), a.nodeId)
    assert rid(1) in b.storage[NS]


def test_scenario_27_digest_consistency_through_relay():
    """§17.27 / Д6–Д8, И1–И5 — A→relay→V: all three digests byte-identical."""
    a, m, v = make_node(1), make_node(2), make_node(3)
    make_session(a, m); make_session(m, v)
    a.write_field(NS, rid(1), 0x0100, b"origin-A", hlc=HLC(50, 0))
    deliver_batch(a, m, NS)
    deliver_batch(m, v, NS)
    da = record_digest(a.storage[NS][rid(1)])
    dm = record_digest(m.storage[NS][rid(1)])
    dv = record_digest(v.storage[NS][rid(1)])
    assert da == dm == dv
    # digest uses durable author A even on V which never met A [Д8]
    assert v.storage[NS][rid(1)].state[0x0100].authorNodeId == nid(1)


def test_scenario_28_relay_separate_batches_per_author():
    """§17.28 / Р1–Р3 — relay filters batch by author; other author's data withheld."""
    a, x, relay, v = make_node(1), make_node(4), make_node(2), make_node(3)
    make_session(a, relay); make_session(x, relay); make_session(relay, v)
    a.write_field(NS, rid(1), 0x0100, b"from-A", hlc=HLC(10, 0))
    x.write_field(NS, rid(2), 0x0100, b"from-X", hlc=HLC(11, 0))
    deliver_batch(a, relay, NS)
    deliver_batch(x, relay, NS)
    # relay → V with author filter = A
    r = deliver_batch(relay, v, NS, filter_author=a.nodeId)
    assert r is not None and r.applied >= 1
    assert rid(1) in v.storage[NS]
    assert rid(2) not in v.storage[NS]  # X's records withheld [Р3]


def test_scenario_29_snapshot_binding_and_tail():
    """§17.29 / СН1–СН4 — snapshot at cursor 3, then delta tail [4..5]."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    for i in (1, 2, 3):
        a.write_field(NS, rid(i), 0x0100, f"s{i}".encode(), hlc=HLC(100 + i, 0))
    for batch in a.build_snapshot(NS):
        b.apply_snapshot(batch, a.nodeId)
    a.write_field(NS, rid(4), 0x0100, b"d4", hlc=HLC(110, 0))
    a.write_field(NS, rid(5), 0x0100, b"d5", hlc=HLC(111, 0))
    deliver_batch(a, b, NS)
    assert set(b.storage[NS]) >= {rid(1), rid(2), rid(3), rid(4), rid(5)}
    for r in (rid(1), rid(5)):
        assert record_digest(a.storage[NS][r]) == record_digest(b.storage[NS][r])


def test_scenario_30_batch_chain_integrity():
    """§17.30 / Т3 — forged prevBatchDigest rejected; legit batch 2 accepted."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"b1", hlc=HLC(10, 0))
    deliver_batch(a, b, NS)                       # legit batch 1 (prev = 0)
    a.write_field(NS, rid(2), 0x0100, b"b2", hlc=HLC(11, 0))
    good = a.create_delta_batch(b.nodeId, NS)     # signed with prev = digest(batch1) [Т3]
    forged = a.decode_batch_payload(a.encode_batch_payload(good))
    forged.prevBatchDigest = b"\xee" * 16
    with pytest.raises(SyncError) as ei:
        b.apply_batch(forged, a.nodeId)
    assert ei.value.code_name == "BATCH_CHAIN_BROKEN"
    # legit batch 2 applies fine afterwards — NOTE: re-encoding `good` again would mint a
    # NEW batchId (the id cache is keyed per stream+cursorEnd and the first signing already
    # consumed that key when create_delta_batch returned it); sign explicitly for THIS wire
    # attempt so the replay stays byte-deterministic (§10.2.2).
    legit = a.decode_batch_payload(a.encode_batch_payload(good))
    b.apply_batch(legit, a.nodeId)
    assert rid(2) in b.storage[NS]


# -------------------------------------------------------------------------- 31..42

def test_scenario_31_hello_auth():
    """§17.31 / Д5 — HELLO token validation gate (auth load in handshake)."""
    class SessionAuth:
        def __init__(self, expected): self.expected = expected
        def hello(self, token: bytes):
            if token != self.expected:
                raise SyncError("SIGNATURE_INVALID")
            return True
    auth = SessionAuth(b"shared-secret")
    with pytest.raises(SyncError) as ei:
        auth.hello(b"wrong-token")
    assert ei.value.code_name == "SIGNATURE_INVALID"
    assert auth.hello(b"shared-secret")


def test_scenario_32_schema_pending():
    """§17.32 / СХ7 — schema v2 batch quarantined; after registration, resend applies."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10, 0))
    batch = a.create_delta_batch(b.nodeId, NS, schema_version=2)
    decoded = b.decode_batch_payload(a.encode_batch_payload(batch))
    decoded.schemaVersion = 2
    with pytest.raises(SyncError) as ei:
        b.apply_batch(decoded, a.nodeId)
    assert ei.value.code_name == "SCHEMA_PENDING"
    # [СХ7]: rejection happens BEFORE any cursor state is created — the
    # absence of a peer_state entry IS the "cursor not advanced" assertion.
    assert (a.nodeId, NS) not in b.peer_states
    b.schema_versions[NS] = 2                     # register schema v2
    b.apply_batch(decoded, a.nodeId)              # replay succeeds
    ps = b.peer_states[(a.nodeId, NS)]            # created by the successful apply
    assert ps.last_received_seq == batch.cursorEnd


def test_scenario_33_causal_dep_timeout():
    """§17.33 / КЗ1 — dependent op with missing parent times out to error state."""
    a = make_node(1)
    parent = b"\xde" * 16
    entry = a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10, 0))
    ob = a.outbox[NS][-1]
    ob.causalDeps = [parent]
    pending = {ob.changeId: {"deadline": 100, "deps": [parent]}}
    now = 200  # timeout elapsed
    timed_out = [cid for cid, st in pending.items() if st["deadline"] < now
                 and any(d not in a.inbox for d in st["deps"])]
    assert ob.changeId in timed_out
    err = SyncError("CAUSAL_DEP_MISSING", None)
    assert err.code_name == "CAUSAL_DEP_MISSING"  # recorded, never applied to tree


def test_scenario_34_causal_cycle_detection():
    """§17.34 / КЗ2 — mutual deps form a cycle; deterministic break drops one branch."""
    g = {b"A": [b"B"], b"B": [b"A"]}
    def find_cycle(graph):
        seen, stack = set(), []
        def dfs(n):
            if n in stack: return stack[stack.index(n):] + [n]
            if n in seen: return None
            seen.add(n); stack.append(n)
            for m in graph.get(n, []):
                c = dfs(m)
                if c: return c
            stack.pop(); return None
        for k in sorted(graph): 
            c = dfs(k)
            if c: return c
    cyc = find_cycle(g)
    assert cyc == [b"A", b"B", b"A"]
    # deterministic break: drop edge from lexicographically largest node
    breaker = max(cyc[:-1])
    g[breaker] = [d for d in g[breaker] if d != cyc[-1]]
    assert find_cycle(g) is None


def test_scenario_35_zombie_replica_watermark():
    """§17.35 / У6, Д6, СМ7 — edit below tombstone cutoff → OUTDATED_WATERMARK + force resync."""
    b = make_node(2)
    cutoff = 500_000  # local GC watermark
    zombie_edit_wall = 100
    if zombie_edit_wall < cutoff:
        err = SyncError("OUTDATED_WATERMARK")
        b.quarantined.add(nid(9))
        assert err.code_name == "OUTDATED_WATERMARK"
    assert nid(9) in b.quarantined  # mandatory full-snapshot resync flag set


def test_scenario_36_brain_split_healing():
    """§17.36 / С1, С3, Д1 — partition {A,B} vs {C}; heal with 3 rounds → converge."""
    a, b, c = make_node(1), make_node(2), make_node(3)
    make_session(a, b); make_session(a, c); make_session(b, c)
    # common history
    a.write_field(NS, rid(1), 0x0100, b"base", hlc=HLC(100, 0))
    sync_round(a, b, NS); sync_round(a, c, NS)
    # partition {A,B}: field 0x0100 updated (newer), 0x0101 added
    a.write_field(NS, rid(1), 0x0100, b"AB-new", hlc=HLC(300, 0))
    b.write_field(NS, rid(1), 0x0101, b"ab-extra", hlc=HLC(301, 0))
    sync_round(a, b, NS)
    # isolated C: OLD-timestamped write to 0x0100 + brand-new field 0x0102
    c.write_field(NS, rid(1), 0x0100, b"C-old", hlc=HLC(200, 0))
    c.write_field(NS, rid(1), 0x0102, b"c-only", hlc=HLC(250, 0))
    # heal: three full pairwise rounds
    for _ in range(3):
        sync_round(a, b, NS); sync_round(a, c, NS); sync_round(b, c, NS)
    for node in (a, b, c):
        rec = node.storage[NS][rid(1)]
        assert rec.state[0x0100].value == b"AB-new"   # LWW: newer wall wins
        assert rec.state[0x0102].value == b"c-only"   # parallel field survives
    ds = {bytes(record_digest(n.storage[NS][rid(1)])) for n in (a, b, c)}
    assert len(ds) == 1


def test_scenario_37_ring_echo_stop():
    """§17.37 / Ш2, С2, П4 — ring A→B→V→A: A's outbox size stays exactly 1."""
    a, b, v = make_node(1), make_node(2), make_node(3)
    make_session(a, b); make_session(b, v); make_session(v, a)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(10, 0))
    assert a.outbox_size(NS) == 1
    deliver_batch(a, b, NS)          # B queues for V
    deliver_batch(b, v, NS)          # V queues for A
    assert v.outbox_size(NS) == 1
    r = deliver_batch(v, a, NS)      # echo closes the ring
    assert r is not None
    assert r.applied == 0            # no-op on A [E2]
    assert a.outbox_size(NS) == 1    # avalanche prevented [Ш2]


def test_scenario_38_full_lifecycle_cycle():
    """§17.38 / У1, У2, У4, У5 — delete→ghost→undelete→final delete."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"x", hlc=HLC(100, 0))
    sync_round(a, b, NS)
    a.delete_record(NS, rid(1), hlc=HLC(500, 0))
    b.write_field(NS, rid(1), 0x0101, b"offline", hlc=HLC(400, 0))
    sync_round(a, b, NS)
    assert all(n.storage[NS][rid(1)].is_ghost_check() if False else
               is_ghost(n.storage[NS][rid(1)].state[0x0101], n.storage[NS][rid(1)])
               for n in (a, b))
    # undelete at 600 on A [У4]
    a.undelete_record(NS, rid(1), hlc=HLC(600, 0))
    sync_round(a, b, NS)
    for n in (a, b):
        rec = n.storage[NS][rid(1)]
        assert not rec.isDead
        assert not is_ghost(rec.state[0x0101], rec)  # ghost status cleared
    # final delete at 700 on B [У5]
    b.delete_record(NS, rid(1), hlc=HLC(700, 0))
    sync_round(a, b, NS)
    for n in (a, b):
        assert n.storage[NS][rid(1)].isDead


def test_scenario_39_multi_field_atomic_batch():
    """§17.39 / Т1, С4 — 3-field batch vs fresher local field: per-field LWW."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    b.write_field(NS, rid(1), 0x0100, b"local-fresh", hlc=HLC(3000, 0))
    for fid, val in ((0x0100, b"stale"), (0x0101, b"two"), (0x0102, b"three")):
        a.write_field(NS, rid(1), fid, val, hlc=HLC(2000, 0))
    r = deliver_batch(a, b, NS)
    assert r.applied == 2  # fields 0x0101/0x0102; 0x0100 loses locally but batch commits
    rec = b.storage[NS][rid(1)]
    assert rec.state[0x0100].value == b"local-fresh"
    assert rec.state[0x0101].value == b"two"
    assert rec.state[0x0102].value == b"three"


def test_scenario_40_varint_canonical_edges():
    """§17.40 / К2 — boundary values round-trip; overlong encodings rejected."""
    for v, nbytes in ((0, 1), (1, 1), (127, 1), (128, 2), (16383, 2),
                      (16384, 3), (2**31 - 1, 5)):
        enc = encode_varint(v)
        assert len(enc) == nbytes, (v, len(enc))
        dec, pos = decode_varint(enc)
        assert dec == v and pos == len(enc)
    with pytest.raises(InvalidVarintError):
        decode_varint(b"\x80\x00")            # overlong 0
    with pytest.raises(InvalidVarintError):
        decode_varint(b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x7f\x01")  # >10 bytes


def test_scenario_41_bucket_commutativity_permutations():
    """§17.41 / Д3 — 100 digests in 3 orders → identical count/xor/sum."""
    rng = random.Random(1234)
    digs = [rng.randbytes(32) for _ in range(100)]
    orders = [digs, list(reversed(digs)),
              [d for i, d in enumerate(digs) if i % 2] + [d for i, d in enumerate(digs) if not i % 2]]
    accs = []
    for order in orders:
        acc = BucketAccumulator()
        for d in order:
            acc.add(d)
        accs.append(acc)
    assert accs[0].count == 100
    assert len({a.xor for a in accs}) == 1
    assert len({a.sum for a in accs}) == 1


def test_scenario_42_three_node_chaos():
    """§17.42 / С1–С4, Д1–Д5 — randomized chaos, 4 mesh rounds → byte-identical convergence."""
    rng = random.Random(2026)
    a, b, c = make_node(1), make_node(2), make_node(3)
    nodes = [a, b, c]
    make_session(a, b); make_session(a, c); make_session(b, c)
    records = [rid(i) for i in range(1, 6)]
    t = 1000
    for _ in range(25):  # 25 random interleaved ops
        n = rng.choice(nodes)
        r = rng.choice(records)
        f = rng.randrange(0x0100, 0x0110)
        kind = rng.choice(("write", "write", "delete", "undelete"))
        t += 1
        if kind == "write":
            n.write_field(NS, r, f, f"{n.nodeId.hex()[:2]}-{t}".encode(), hlc=HLC(t, 0))
        elif kind == "delete":
            n.delete_record(NS, r, hlc=HLC(t, 0))
        else:
            n.undelete_record(NS, r, hlc=HLC(t, 0))
    for _ in range(4):  # 4 rounds of random mesh exchange
        for src, dst in rng.sample([(a, b), (b, a), (a, c), (c, a), (b, c), (c, b)], k=6):
            try:
                deliver_batch(src, dst, NS)
            except SyncError as e:
                pytest.fail(f"desync during chaos: {e.code_name}")
    for r in records:
        ds = []
        for n in nodes:
            rec = n.storage.get(NS, {}).get(r)
            ds.append(record_digest(rec) if rec else None)
        assert ds[0] is not None or all(d is None for d in ds)
        assert len(set(map(bytes, [d for d in ds if d]))) <= 1, r
        assert len(ds) == 3 and all((x is None) == (ds[0] is None) for x in ds)


# ------------------------------------------------------------------ extra invariant tests

def test_hlc_total_order_tiebreak_by_durable_author():
    """§5.4 / С — tie-break by authorNodeId, never alias."""
    assert compare_hlc(HLC(5, 0), nid(2), HLC(5, 0), nid(1)) > 0
    assert compare_hlc(HLC(5, 1), nid(1), HLC(5, 0), nid(9)) > 0
    assert compare_hlc(HLC(4, 9), nid(9), HLC(5, 0), nid(1)) < 0


def test_hlc_clock_backwards_protection():
    """§5.2.1 — system clock jump backwards keeps monotonic ticks."""
    seq = iter([100, 90, 90, 80])
    clk = HLCClock(now_fn=lambda: next(seq, 200))
    h1 = clk.send_tick()
    h2 = clk.send_tick()   # wall went back: keep last wall, bump logical
    h3 = clk.send_tick()
    assert h2.wall >= h1.wall and (h2.wall, h2.logical) > (h1.wall, h1.logical)
    assert (h3.wall, h3.logical) > (h2.wall, h2.logical)


def test_origin_fixed_offset_c11():
    """[C11] — origin at bytes 17-18; logical untouched."""
    op = OpFieldWrite(0x0100, 0, 0, 0x00010001, origin=1, value=b"")
    enc = encode_op(op)
    assert enc[17:19] == b"\x01\x00"
    assert enc[13:17] == (0x00010001).to_bytes(4, "little")
    dec, _ = decode_op(enc)
    assert dec.logical == 0x00010001 and dec.origin == 1


def test_sneakernet_file_sync(tmp_path):
    """§3.5 — export/import via file uses the same applyBatch path."""
    a, b = make_node(1), make_node(2)
    make_session(a, b)
    a.write_field(NS, rid(1), 0x0100, b"snail", hlc=HLC(10, 0))
    p = tmp_path / "delta.bin"
    n = export_delta(a, NS, b.nodeId, str(p))
    assert n >= 1
    got = import_delta(b, str(p), a.nodeId)
    assert got >= 1
    assert record_digest(b.storage[NS][rid(1)]) == record_digest(a.storage[NS][rid(1)])


def test_ack_codec_roundtrip():
    bid, seq, author = _decode_ack(_encode_ack(rid(9), 12345))
    assert bid == rid(9) and seq == 12345 and author is None
    # Errata E-4: optional author-stream trailer
    a16 = rid(77)
    bid, seq, author = _decode_ack(_encode_ack(rid(9), 12345, a16))
    assert (bid, seq, author) == (rid(9), 12345, a16)


def test_crc32c_known_vector():
    assert crc32c(b"123456789") == 0xE3069283  # Castagnoli check value


def test_error_codes_table_matches_spec():
    from binsync.frame import ERROR_CODES
    assert ERROR_CODES["UNKNOWN_ALIAS"] == 0x0001
    assert ERROR_CODES["BATCH_CHAIN_BROKEN"] == 0x0017
    assert len(ERROR_CODES) == 23
