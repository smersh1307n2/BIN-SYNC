"""Wire operations (§12.24): FieldWrite / FieldTomb / RecordTombstone byte layouts.

Invariant [C11]: `origin` sits at FIXED offset 17 (bytes 17-18) for FieldWrite and
FieldTomb. Offsets 13-16 are hlc.logical (u32 LE). Writing origin at offset 15 would
clobber the high half of logical (1 → 0x00010001 = 65537) and break determinism.
"""

from __future__ import annotations

from dataclasses import dataclass

from .codec import Reader, Writer

TAG_FIELD_WRITE = 0x01
TAG_FIELD_TOMB = 0x02
TAG_RECORD_TOMB = 0x03

ORIGIN_OFFSET = 17  # [C11] for FieldWrite/FieldTomb
ORIGIN_OFFSET_RECORD_TOMB = 29  # for RecordTombstone


@dataclass
class OpFieldWrite:
    fieldId: int
    keyEpoch: int
    wall: int
    logical: int
    origin: int = 0            # compact mode: session alias
    authorNodeId: bytes | None = None  # full-author mode ([§12.24.4]) or resolved
    value: bytes = b""

    tag = TAG_FIELD_WRITE


@dataclass
class OpFieldTomb:
    fieldId: int
    keyEpoch: int
    wall: int
    logical: int
    origin: int = 0
    authorNodeId: bytes | None = None

    tag = TAG_FIELD_TOMB


@dataclass
class OpRecordTombstone:
    recordId: bytes
    wall: int
    logical: int
    origin: int = 0
    authorNodeId: bytes | None = None

    tag = TAG_RECORD_TOMB


def encode_op(op, full_author: bool = False) -> bytes:
    """Canonical encoding. In compact mode author slot is u16 origin; in FULL_AUTHOR_ID
    mode it is the durable b128 authorNodeId (§12.24.4)."""
    w = Writer()
    if isinstance(op, OpFieldWrite):
        w.u8(TAG_FIELD_WRITE).u16(op.fieldId).u16(op.keyEpoch).u64(op.wall).u32(op.logical)
        assert w.value().__len__() == ORIGIN_OFFSET, "[C11] prefix must be exactly 17 bytes"
        if full_author:
            assert op.authorNodeId is not None
            w.b128(op.authorNodeId)
        else:
            w.u16(op.origin)
        w.varint(len(op.value)).raw(op.value)
        return w.value()
    if isinstance(op, OpFieldTomb):
        w.u8(TAG_FIELD_TOMB).u16(op.fieldId).u16(op.keyEpoch).u64(op.wall).u32(op.logical)
        assert len(w.value()) == ORIGIN_OFFSET, "[C11] prefix must be exactly 17 bytes"
        if full_author:
            assert op.authorNodeId is not None
            w.b128(op.authorNodeId)
        else:
            w.u16(op.origin)
        return w.value()
    if isinstance(op, OpRecordTombstone):
        w.u8(TAG_RECORD_TOMB).b128(op.recordId).u64(op.wall).u32(op.logical)
        assert len(w.value()) == ORIGIN_OFFSET_RECORD_TOMB, "origin at offset 29"
        if full_author:
            assert op.authorNodeId is not None
            w.b128(op.authorNodeId)
        else:
            w.u16(op.origin)
        return w.value()
    raise TypeError(f"unknown op {op!r}")


def decode_op(data: bytes, pos: int = 0, full_author: bool | None = None):
    """`full_author=None` (default) → AUTO-DETECT the mode from the payload length.

    BUGFIX (implementation finding): §12.24 defines two layouts for every op — compact
    (u16 origin, [C11] fixed offset) and FULL_AUTHOR_ID (b128 durable author). The wire
    header of DELTA_BATCH carries a batch-level flag (§12.17), but snapshot chunk records
    are re-read through generic helpers that historically passed only the payload; an
    explicit-mode decoder then truncated/over-read the author slot (EOFError on
    apply_snapshot). Auto-detection is exact because both layouts have a deterministic
    prefix length and the compact remainder is always < the b128 variant's.
    """
    r = Reader(data, pos)
    tag = r.u8()
    if tag == TAG_FIELD_WRITE:
        field_id = r.u16()
        key_epoch = r.u16()
        wall = r.u64()
        logical = r.u32()
        if full_author is None:
            # compact: 2-byte origin + varint len + value; full: 16-byte author + ...
            # remainder after offset 17: compact >= 3 (origin2 + valLen0), full >= 17.
            full = (len(data) - r.pos) >= 17
        else:
            full = full_author
        if full:
            author = r.b128()
            origin = 0
        else:
            origin = r.u16()
            author = None
        assert r.pos == ORIGIN_OFFSET + (16 if full else 2), "[C11] author slot at fixed offset"
        value = r.bytes_()
        return OpFieldWrite(field_id, key_epoch, wall, logical, origin, author, value), r.pos
    if tag == TAG_FIELD_TOMB:
        field_id = r.u16()
        key_epoch = r.u16()
        wall = r.u64()
        logical = r.u32()
        if full_author is None:
            full = (len(data) - r.pos) == 16   # tomb has no trailing fields
        else:
            full = full_author
        if full:
            author = r.b128()
            origin = 0
        else:
            origin = r.u16()
            author = None
        assert r.pos == ORIGIN_OFFSET + (16 if full else 2)
        return OpFieldTomb(field_id, key_epoch, wall, logical, origin, author), r.pos
    if tag == TAG_RECORD_TOMB:
        record_id = r.b128()
        wall = r.u64()
        logical = r.u32()
        if full_author is None:
            full = (len(data) - r.pos) == 16
        else:
            full = full_author
        if full:
            author = r.b128()
            origin = 0
        else:
            origin = r.u16()
            author = None
        assert r.pos == ORIGIN_OFFSET_RECORD_TOMB + (16 if full else 2)
        return OpRecordTombstone(record_id, wall, logical, origin, author), r.pos
    raise ValueError(f"unknown op tag {tag:#x}")


def rewrite_origin(op, new_origin: int):
    """[R4] Relay alias translation: rewrite the session alias of an operation."""
    op.origin = new_origin
    return op
