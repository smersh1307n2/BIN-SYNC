"""BIN-SYNC CORE v2.3.6 — Python reference implementation (Phases 1–2 of Appendix B)."""

from .codec import encode_varint, decode_varint, Writer, Reader, InvalidVarintError
from .hlc import HLC, HLCClock, compare_hlc, ClockDriftError
from .frame import (
    Frame, build_frame, parse_frame, crc32c, FRAME_TYPES, ERROR_CODES,
    FlowControl, MAGIC, HEADER_SIZE, MAX_PAYLOAD_SIZE, INITIAL_WINDOW, MAX_WINDOW_SIZE,
)
from .model import FieldState, Record, LIFECYCLE_FIELD_ID, ALIVE, DEAD, is_ghost
from .merge import merge_field, merge_lifecycle, apply_field_write, apply_field_tomb
from .digests import (
    field_digest_payload,
    lifecycle_digest_payload,
    record_digest,
    BucketAccumulator,
    state_root_digest,
    build_buckets,
)
from .ops import (
    OpFieldWrite,
    OpFieldTomb,
    OpRecordTombstone,
    encode_op,
    decode_op,
    rewrite_origin,
    ORIGIN_OFFSET,
)
from .node import Node, SyncError, BatchResult, Batch, OutboxEntry
from .transport import sync_round, deliver_batch, make_session, export_delta, import_delta

__version__ = "2.3.6"
