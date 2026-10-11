"""Merge policies (§8): LWW_FIELD with echo suppression [E2], lifecycle delete-wins."""

from __future__ import annotations

from .hlc import HLC, compare_hlc
from .model import ALIVE, DEAD, FieldState, Record


def merge_field(local: FieldState | None, remote: FieldState) -> tuple[FieldState, bool]:
    """Return (winner, changed). Implements §8.3 including echo suppression [E2]/[Ш2]:
    identical (wall, logical, authorNodeId, value) → no-op, changed=False."""
    if local is not None:
        # Echo suppression: identical operation → no-op
        if (
            local.hlc.wall == remote.hlc.wall
            and local.hlc.logical == remote.hlc.logical
            and local.authorNodeId == remote.authorNodeId
            and local.value == remote.value
        ):
            return local, False
        if compare_hlc(remote.hlc, remote.authorNodeId, local.hlc, local.authorNodeId) > 0:
            return remote, True
        return local, False
    return remote, True


def merge_lifecycle(
    local_hlc: HLC | None,
    local_author: bytes | None,
    local_value: int,
    remote_hlc: HLC,
    remote_author: bytes,
    remote_value: int,
) -> tuple[HLC, bytes, int, bool]:
    """Lifecycle conflict resolution by full order (wall, logical, authorNodeId)
    [Ж1–Ж2]. Returns (hlc, author, value, changed)."""
    if local_hlc is None:
        return remote_hlc, remote_author, remote_value, True
    cmpres = compare_hlc(remote_hlc, remote_author, local_hlc, local_author)
    if cmpres > 0:
        return remote_hlc, remote_author, remote_value, True
    if cmpres == 0 and local_value != remote_value:
        # same time & author but different value — deterministic: dead wins (§7.4)
        return local_hlc, local_author, max(local_value, remote_value), local_value != max(
            local_value, remote_value
        )
    return local_hlc, local_author, local_value, False


def apply_field_write(record: Record, field_id: int, op: FieldState) -> bool:
    """Apply a FieldWrite to a record in place; returns changed (§8.3 anti-entropy rule).
    Note: LWW_FIELD forbids record-level discard — every field merges independently."""
    local = record.state.get(field_id)
    winner, changed = merge_field(local, op)
    if changed:
        record.state[field_id] = winner
        # a newer write resurrects nothing by itself ([У3]: normal write doesn't change lifecycle)
    return changed


def apply_field_tomb(record: Record, field_id: int, op: FieldState) -> bool:
    """FieldTomb: mark field deleted. Tombstone suppresses writes with time <= its own
    ([У1]); later writes win over the tomb ([У2] semantics handled via merge on time)."""
    local = record.state.get(field_id)
    tomb_local = record.fieldTombs.get(field_id)
    # Merge against existing tomb first
    if tomb_local is not None:
        winner, changed = merge_field(tomb_local, op)
        if changed:
            record.fieldTombs[field_id] = winner
        tomb_local = winner
    else:
        record.fieldTombs[field_id] = op
        tomb_local = op
        changed = True
    # If the surviving field value loses to the tomb (field time <= tomb time), hide it
    if local is not None:
        keep, _ = merge_field(local, tomb_local)
        if keep is tomb_local or (
            compare_hlc(tomb_local.hlc, tomb_local.authorNodeId, local.hlc, local.authorNodeId) >= 0
            and tomb_local.value == b""
        ):
            # tomb wins or ties → field removed from visible state, kept for digest as tomb
            del record.state[field_id]
            changed = True
    return changed


def apply_lifecycle(record: Record, hlc: HLC, author: bytes, value: int) -> bool:
    lc, la, lv, changed = merge_lifecycle(
        record.lifecycleHlc, record.lifecycleAuthor, record.lifecycleValue, hlc, author, value
    )
    record.lifecycleHlc, record.lifecycleAuthor, record.lifecycleValue = lc, la, lv
    return changed
