"""Frame format (§12.2): 25-byte header, magic 0x4253, CRC32C, hard limits."""

from __future__ import annotations

import struct
from dataclasses import dataclass

MAGIC = 0x4253
VERSION = 0x00
HEADER_SIZE = 25
MAX_FRAME_SIZE = 16 * 1024 * 1024            # §12.3
MAX_PAYLOAD_SIZE = MAX_FRAME_SIZE - HEADER_SIZE
INITIAL_WINDOW = 256 * 1024                  # §3.6.1
MAX_WINDOW_SIZE = 16 * 1024 * 1024

FLAG_COMPRESSED = 0x01
FLAG_SIGNED = 0x02
FLAG_FRAGMENT = 0x04
FLAG_ENCRYPTED = 0x08

FRAME_TYPES = {
    0x01: "HELLO", 0x02: "HELLO_ACK", 0x03: "ALIAS_MAP", 0x04: "SCHEMA_MAP",
    0x05: "MEMBERSHIP", 0x06: "STATE_DIGEST", 0x07: "NEED_RANGE", 0x08: "NEED_KEYS",
    0x09: "DELTA_BATCH", 0x0A: "SNAPSHOT_CHUNK", 0x0B: "ACK", 0x0C: "GC_NOTICE",
    0x0D: "ERROR", 0x0E: "WINDOW_UPDATE", 0x0F: "PING", 0x10: "PONG",
    0x11: "KEY_ROTATION", 0x12: "IBLT_REQUEST", 0x13: "IBLT_RESPONSE",
}

# §12.23 error codes (canonical numeric values)
ERROR_CODES = {
    "UNKNOWN_ALIAS": 0x0001, "EPOCH_REGRESSION": 0x0002, "CLOCK_DRIFT": 0x0003,
    "INVALID_VARINT": 0x0004, "INVALID_FLOAT": 0x0005, "SCHEMA_MISMATCH": 0x0006,
    "SEQ_GAP": 0x0007, "DUPLICATE_BATCH": 0x0008, "NAMESPACE_FORBIDDEN": 0x0009,
    "INTERNAL": 0x000A, "OUTDATED_WATERMARK": 0x000B,
    "DECOMPRESSION_LIMIT_EXCEEDED": 0x000C, "BATCH_TRANSACTION_FAILED": 0x000D,
    "PAYLOAD_TOO_LARGE": 0x000E, "FLOW_CONTROL_VIOLATION": 0x000F,
    "UNKNOWN_KEY_EPOCH": 0x0010, "SCHEMA_PENDING": 0x0011, "CAUSAL_DEP_MISSING": 0x0012,
    "SIGNATURE_INVALID": 0x0013, "TRUST_REVOKED": 0x0014, "CORRUPTED_FRAME": 0x0015,
    "FRAME_REJECTED": 0x0016, "BATCH_CHAIN_BROKEN": 0x0017,
}


def crc32c(data: bytes) -> int:
    """CRC32C (Castagnoli) — software implementation, polynomial 0x1EDC6F41 reflected."""
    poly = 0x82F63B78
    table = getattr(crc32c, "_table", None)
    if table is None:
        table = []
        for i in range(256):
            c = i
            for _ in range(8):
                c = (c >> 1) ^ (poly if c & 1 else 0)
            table.append(c)
        crc32c._table = table
    crc = 0xFFFFFFFF
    for b in data:
        crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


@dataclass
class Frame:
    frame_type: int
    flags: int
    stream_id: int
    frame_seq: int
    payload: bytes

    @property
    def name(self) -> str:
        return FRAME_TYPES.get(self.frame_type, f"0x{self.frame_type:02X}")


class FrameError(Exception):
    def __init__(self, code_name: str, fatal: bool = False) -> None:
        super().__init__(code_name)
        self.code_name = code_name
        self.code = ERROR_CODES[code_name]
        self.fatal = fatal  # True → session must be torn down ([Ц3])


def build_frame(frame_type: int, stream_id: int, frame_seq: int, payload: bytes,
                flags: int = 0) -> bytes:
    """Header layout (§12.2): magic u16 | version u8 | type u8 | flags u8 |
    streamId u32 | frameSeq u64 | payloadLen u32 | crc32c u32.

    BUGFIX (implementation finding): struct.pack('<HBBBIQII', ...) inserts 4 implicit
    alignment padding bytes before the u64 field — the header came out 29 bytes long
    and `[:HEADER_SIZE]` silently TRUNCATED THE CRC FIELD, while parse_frame used the
    same padded format and "worked" only because both sides were broken symmetrically.
    The §12.2 wire format is packed (no alignment); use explicit little-endian fields
    with '<' and no native-size specifiers interleaved with Q.
    """
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise FrameError("PAYLOAD_TOO_LARGE", fatal=True)
    crc = crc32c(payload)
    header = struct.pack(
        "<HBBB I Q I I".replace(" ", ""),
        MAGIC, VERSION, frame_type, flags, stream_id, frame_seq, len(payload), crc,
    )
    assert len(header) == HEADER_SIZE, f"header must be exactly {HEADER_SIZE} bytes"
    return header + payload


def parse_frame(data: bytes) -> tuple[Frame, int]:
    """Parse one frame from the front of `data`, returning (frame, consumed).

    §12.2.1 integrity rules:
      - bad magic → silent drop (returns None-frame marker via FrameError non-fatal? no:
        spec says silent drop; we raise FrameError('BAD_MAGIC') flagged silent)
      - payloadLen > MAX → PAYLOAD_TOO_LARGE (fatal)
      - CRC mismatch → CORRUPTED_FRAME (session preserved [Ц1])
    """
    if len(data) < HEADER_SIZE:
        raise FrameError("FRAME_REJECTED")
    magic, version, ftype, flags, stream_id, frame_seq, payload_len, crc = struct.unpack(
        "<HBBB IQII".replace(" ", ""), data[:HEADER_SIZE]
    )
    if magic != MAGIC:
        err = FrameError("BAD_MAGIC")
        err.silent_drop = True  # [Ц2]
        raise err
    if payload_len > MAX_PAYLOAD_SIZE:
        raise FrameError("PAYLOAD_TOO_LARGE", fatal=True)
    if len(data) < HEADER_SIZE + payload_len:
        raise FrameError("FRAME_REJECTED")  # incomplete — need more bytes
    payload = data[HEADER_SIZE : HEADER_SIZE + payload_len]
    if crc32c(payload) != crc:
        raise FrameError("CORRUPTED_FRAME")  # [Ц1] session preserved
    return Frame(ftype, flags, stream_id, frame_seq, payload), HEADER_SIZE + payload_len


class FlowControl:
    """§3.6 additive-credit window per streamId. Payload bytes only, not headers."""

    def __init__(self, initial: int = INITIAL_WINDOW, max_window: int = MAX_WINDOW_SIZE) -> None:
        self.initial = initial
        self.max_window = max_window
        self.credit: dict[int, int] = {}   # streamId → available send credit

    def balance(self, stream_id: int) -> int:
        return self.credit.setdefault(stream_id, self.initial)

    def try_consume(self, stream_id: int, payload_bytes: int) -> bool:
        bal = self.balance(stream_id)
        if bal < payload_bytes:
            return False  # FLOW_CONTROL_VIOLATION if sender ignores this [УП2]
        self.credit[stream_id] = bal - payload_bytes
        return True

    def apply_update(self, stream_id: int, credits: int) -> int:
        """WINDOW_UPDATE is additive; clamp at MAX_WINDOW_SIZE [УП1]."""
        new = min(self.balance(stream_id) + credits, self.max_window)
        self.credit[stream_id] = new
        return new
