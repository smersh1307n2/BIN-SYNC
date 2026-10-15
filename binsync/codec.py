"""Canonical binary codec (§6): ULEB128 varint (overlong rejected), LE fixed ints, TLV."""

from __future__ import annotations


class InvalidVarintError(ValueError):
    """[C2]/[К2] overlong or malformed varint → INVALID_VARINT."""


def encode_varint(value: int) -> bytes:
    """ULEB128, canonical minimal form only (§6.1)."""
    if value < 0:
        raise ValueError("varint must be non-negative")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def decode_varint(data: bytes, pos: int = 0) -> tuple[int, int]:
    """Return (value, new_pos). Rejects overlong encodings and >10-byte forms.

    SPEC HARDENING (§6.1 [C2]/[К2], found during implementation): the spec's prose
    ("valLen 1-10 bytes") and its own test vector b"\\xff\"*9 + b'\\x7f' (= 2**63-1,
    exactly 10 bytes) are mutually consistent ONLY under a per-byte overflow rule:
    at each continuation step k (0-based), the accumulated value must be < 2**(7*(k+1)).
    This rejects every non-minimal encoding as SOON as the redundancy appears
    (b"\\x80\\x00", b"\\x80\\x80\\x00", ...), while accepting all canonical forms up to
    10 bytes / 63 bits. A global re-encode-length check alone cannot catch padded
    zeros mid-stream because the trailing terminator byte hides them until consumed.
    """
    result = 0
    shift = 0
    nbytes = 0
    while True:
        if pos >= len(data):
            raise InvalidVarintError("truncated varint")
        byte = data[pos]
        pos += 1
        nbytes += 1
        payload = byte & 0x7F
        # [К2] minimality, checked BEFORE folding the byte in (errata E-5): a terminal
        # byte with payload 0 after any continuation is padding ("\x80\x00" = overlong
        # 0) and must be rejected even though it adds no bits. The per-byte overflow
        # rule additionally rejects significant bits beyond the canonical window while
        # keeping b"\xff"*9 + b"\x7f" (= 2**63-1, exactly 10 bytes) legal. NOTE: the
        # 11th byte may only ever appear as such a zero terminator — the strict
        # "≤10 bytes" bound of §6.1 is enforced by this rejection, not by a length cap.
        last = not (byte & 0x80)
        if payload == 0 and nbytes > 1 and last:
            raise InvalidVarintError("overlong varint")
        if candidate_overflow(result, shift, payload, nbytes, last):
            raise InvalidVarintError("overlong varint")
        result |= payload << shift
        shift += 7
        if last:
            break
    return result, pos


VARINT_MAX_VALUE = (1 << 64) - 1


def candidate_overflow(result: int, shift: int, payload: int, nbytes: int, last: bool) -> bool:
    """True if folding `payload` at `shift` breaks either hard bound of §6.1 [C2]:

    (a) WIDTH bound — a canonical ULEB128 of n bytes carries at most 7n value bits
        (the terminal byte's MSB is the terminator flag, not a value bit; terminal
        payload 0x7F in byte n means "top 7 bits are all ones", e.g. 127 in one byte,
        2**64-1 in TEN bytes b"\\xff"*9+b"\\xff"). A continuation byte at position n may
        not push bits beyond 7n, otherwise the stream needs >= n+1 bytes and the prefix
        is already non-minimal.
    (b) DOMAIN bound — BIN-SYNC varints encode u64-class fields (valLen, seq, wall, …),
        so the decoded value must fit in 64 bits. NOTE (errata E-5): the spec's old test
        vector claimed b"\\xff"*9 + b"\\x7f" was "2**63-1 in 10 bytes"; that is wrong —
        it equals 2**63-1 encoded in NINE bytes (minimal). The true 10-byte maximum is
        2**64-1 = b"\\xff"*9 + b"\\xff". An 11th byte with any significant payload fails
        both bounds simultaneously.
    """
    top = result | (payload << shift)
    if not last and top.bit_length() > 7 * nbytes:
        return True
    return top > VARINT_MAX_VALUE


class Writer:
    def __init__(self) -> None:
        self.buf = bytearray()

    def u8(self, v: int) -> "Writer":
        self.buf.append(v & 0xFF)
        return self

    def u16(self, v: int) -> "Writer":
        self.buf += (v & 0xFFFF).to_bytes(2, "little")
        return self

    def u32(self, v: int) -> "Writer":
        self.buf += (v & 0xFFFFFFFF).to_bytes(4, "little")
        return self

    def u64(self, v: int) -> "Writer":
        self.buf += (v & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little")
        return self

    @staticmethod
    def canonical_b128(v: bytes) -> bytes:
        """Deterministic widening/narrowing of an identity to exactly 16 bytes."""
        import blake3 as _b
        if len(v) == 16:
            return v
        if len(v) < 16:
            return v + b"\x00" * (16 - len(v))
        return _b.blake3(b"BIN-SYNC/AUTHID/v1" + v).digest(16)

    def b128(self, v: bytes) -> "Writer":
        # SPEC HARDENING (§6.2, found during implementation): §6.2 fixes b128 as exactly
        # 16 bytes, but §9.1.2 requires authorNodeId in the field digest payload to be
        # byte-identical across the mesh ([Д8]) — and a node that learned an author via a
        # FULL_AUTHOR_ID op or its own local write knows only the bare 16-byte nodeId,
        # while a compact-mode receiver resolves (nodeId, epoch) from ALIAS_MAP. Without a
        # canonical widening rule the two views produce different digests and anti-entropy
        # never converges through a relay. Rule: shorter ids are zero-padded on the RIGHT
        # to 16 bytes; longer ids (e.g. nodeId||epochLE, 20 bytes) are BLAKE3-keyed down
        # deterministically. Same function on every node ⇒ same digest everywhere.
        if len(v) != 16:
            v = self.canonical_b128(v)
        self.buf += v
        return self

    def varint(self, v: int) -> "Writer":
        self.buf += encode_varint(v)
        return self

    def bytes_(self, b: bytes) -> "Writer":
        """Length-prefixed byte string."""
        self.varint(len(b))
        self.buf += b
        return self

    def raw(self, b: bytes) -> "Writer":
        self.buf += b
        return self

    def value(self) -> bytes:
        return bytes(self.buf)


class Reader:
    def __init__(self, data: bytes, pos: int = 0) -> None:
        self.data = data
        self.pos = pos

    def u8(self) -> int:
        v = self.data[self.pos]
        self.pos += 1
        return v

    def _take(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise EOFError("truncated")
        v = self.data[self.pos : self.pos + n]
        self.pos += n
        return v

    def u16(self) -> int:
        return int.from_bytes(self._take(2), "little")

    def u32(self) -> int:
        return int.from_bytes(self._take(4), "little")

    def u64(self) -> int:
        return int.from_bytes(self._take(8), "little")

    def b128(self) -> bytes:
        return self._take(16)

    def varint(self) -> int:
        v, self.pos = decode_varint(self.data, self.pos)
        return v

    def bytes_(self) -> bytes:
        n = self.varint()
        return self._take(n)

    def raw(self, n: int) -> bytes:
        return self._take(n)

    def eof(self) -> bool:
        return self.pos >= len(self.data)
