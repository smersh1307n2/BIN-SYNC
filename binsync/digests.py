"""Digests (§9): record digest, field/lifecycle digest payloads, bucket accumulators,
StateRootDigest. Domain tags per [К8]. BLAKE3 via the `blake3` package."""

from __future__ import annotations

import blake3

from .codec import Writer
from .model import FieldState, Record

DOMAIN_RECORD = b"BIN-SYNC/RECORD/v1"
DOMAIN_FIELD = b"BIN-SYNC/FIELD/v1"
DOMAIN_BUCKET = b"BIN-SYNC/BUCKET/v1"
DOMAIN_STATE = b"BIN-SYNC/STATE/v1"
DOMAIN_BATCH = b"BIN-SYNC/BATCH/v1"


def field_digest_payload(
    field_id: int, key_epoch: int, wall: int, logical: int, author_node_id: bytes, value: bytes
) -> bytes:
    """§9.1.2 exact formula:
    fieldId u16 LE | keyEpoch u16 LE | wall u64 LE | logical u32 LE |
    authorNodeId b128 (DURABLE, never origin!) | valLen varint | valueBytes
    """
    w = Writer()
    w.u16(field_id).u16(key_epoch).u64(wall).u32(logical).b128(Writer.canonical_b128(author_node_id))
    w.varint(len(value)).raw(value)
    return w.value()


def lifecycle_digest_payload(wall: int, logical: int, lifecycle_author: bytes, value: int) -> bytes:
    """§9.1.3 — fixed 34 bytes: same layout as a field payload with fieldId=0,
    keyEpoch=0, valLen=1, value=[lifecycleValue]. Includes lifecycleAuthor [Д8]."""
    return field_digest_payload(0x0000, 0, wall, logical, lifecycle_author, bytes([value]))


def record_digest(rec: Record) -> bytes:
    """recordDigest = BLAKE3-N(recordId || sort_by_fieldId(FieldDigestPayload*) ||
    LifecycleDigestPayload) (§9.1). Ghosts and tombstones are included in the digest
    (they're stored for digest purposes, §2 'Ghost')."""
    parts = []
    # user/system fields sorted by ascending fieldId (§6.2)
    all_fields: list[tuple[int, FieldState]] = list(rec.state.items())
    for fid, fs in rec.fieldTombs.items():
        all_fields.append((fid, fs))
    all_fields.sort(key=lambda t: t[0])
    for fid, fs in all_fields:
        parts.append(
            field_digest_payload(fid, fs.keyEpoch, fs.hlc.wall, fs.hlc.logical, fs.authorNodeId, fs.value)
        )
    lc = lifecycle_digest_payload(
        rec.lifecycleHlc.wall if rec.lifecycleHlc else 0,
        rec.lifecycleHlc.logical if rec.lifecycleHlc else 0,
        rec.lifecycleAuthor if rec.lifecycleAuthor else b"\x00" * 16,
        rec.lifecycleValue,
    )
    h = blake3.blake3(DOMAIN_RECORD + rec.recordId)
    for p in parts:
        h.update(p)
    h.update(lc)
    return h.digest(32)


class BucketAccumulator:
    """§9.2 / A.9: count + xor + sum over record digests keyed by BLAKE3(recordId) prefix.
    Commutative & incremental [Д3]: insert/delete in any order gives identical aggregates."""

    def __init__(self, prefix: bytes = b"") -> None:
        self.prefix = prefix
        self.count = 0
        self.xor = 0
        self.sum = 0  # big-int sum of first 16 bytes of digest as LE integer

    @staticmethod
    def bucket_key(record_id: bytes) -> bytes:
        return blake3.blake3(record_id).digest(32)

    def add(self, digest: bytes) -> None:
        num = int.from_bytes(digest[:16], "little")
        self.count += 1
        self.xor ^= num
        self.sum += num

    def remove(self, digest: bytes) -> None:
        num = int.from_bytes(digest[:16], "little")
        self.count -= 1
        self.xor ^= num
        self.sum -= num

    def canonical_encoding(self) -> bytes:
        """A.9-style: prefix || count(u32 LE) || xor(16 bytes) || sum(16 bytes)."""
        w = Writer()
        w.raw(self.prefix)
        w.u32(self.count & 0xFFFFFFFF)
        w.raw(self.xor.to_bytes(16, "little", signed=False) if self.xor >= 0 else b"\xff" * 16)
        s = self.sum % (1 << 128)
        w.raw(s.to_bytes(16, "little"))
        return w.value()

    def rollup(self) -> bytes:
        """Bucket digest = BLAKE3(domainTag || canonical encoding)."""
        return blake3.blake3(DOMAIN_BUCKET + self.canonical_encoding()).digest(32)


def state_root_digest(namespace_id: int, outbox_cursor: int, buckets: list[BucketAccumulator]) -> bytes:
    """§9.1.4 StateRootDigest = BLAKE3-256(domainTag || namespaceId || outboxCursor ||
    recordCount || sorted bucket encodings)."""
    buckets_sorted = sorted(buckets, key=lambda b: (b.prefix, b.count, b.xor))
    total = sum(b.count for b in buckets)
    w = Writer()
    w.raw(DOMAIN_STATE).u16(namespace_id).u64(outbox_cursor).varint(total)
    for b in buckets_sorted:
        w.bytes_(b.canonical_encoding())
    return blake3.blake3(w.value()).digest(32)


def build_buckets(records: dict[bytes, Record], radix_bits: int = 8) -> list[BucketAccumulator]:
    """Group records into hash-prefix buckets with dynamic radix (8/12/16 bits)."""
    nbuckets = 1 << min(radix_bits, 16)
    step = 32 // nbuckets if nbuckets <= 256 else 0
    buckets: dict[bytes, BucketAccumulator] = {}
    for rid, rec in records.items():
        key = BucketAccumulator.bucket_key(rid)
        prefix = key[: max(1, radix_bits // 8)]
        acc = buckets.setdefault(prefix, BucketAccumulator(prefix))
        acc.add(record_digest(rec))
    return list(buckets.values())
