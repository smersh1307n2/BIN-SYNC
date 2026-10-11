"""HLC (§5): wall µs + logical + origin (wire only). Tick rules, skew clamp, tie-break."""

from __future__ import annotations

from dataclasses import dataclass

MAX_SKEW_US_DEFAULT = 60_000_000  # §5.3, configurable


@dataclass(frozen=True)
class HLC:
    wall: int      # u64 microseconds UTC
    logical: int   # u32
    origin: int = 0  # u16 wire alias; NOT part of ordering (§5.1)

    def key(self) -> tuple[int, int]:
        return (self.wall, self.logical)


def compare_hlc(a: HLC, a_author: bytes, b: HLC, b_author: bytes) -> int:
    """§5.4 full order: wall, then logical, then durable authorNodeId (never alias).

    BUGFIX (implementation finding): §5.4 leaves the tie-break undefined when one of
    the two DURABLE authors is absent (e.g. an op received in compact mode whose
    session ALIAS_MAP entry carries no epoch, so the receiver cannot reconstruct the
    (nodeId, epoch) pair used as FieldState.authorNodeId). A literal implementation
    then compares `bytes < NoneType` and crashes with TypeError. Deterministic rule:
    an unknown (None) author sorts BEFORE every known author — the comparison stays a
    total order and never depends on arrival order of the two ops."""
    if a_author is None and b_author is not None:
        a_author = b""  # unknown author < any durable 16-byte id
    elif b_author is None and a_author is not None:
        b_author = b""
    if a.wall != b.wall:
        return -1 if a.wall < b.wall else 1
    if a.logical != b.logical:
        return -1 if a.logical < b.logical else 1
    if a_author != b_author:
        return -1 if a_author < b_author else 1
    return 0


class ClockDriftError(Exception):
    """CLOCK_DRIFT (0x0003) per §5.3."""


class HLCClock:
    """Send/receive tick rules with backwards-clock protection (§5.2.1)."""

    LOGICAL_MAX = 2**32 - 1

    def __init__(self, now_fn=None, max_skew_us: int = MAX_SKEW_US_DEFAULT) -> None:
        # BUGFIX (implementation finding): HLCClock must NEVER tick the wall clock at
        # construction. A cold-booted node (§5.2) starts at (0,0); seeding last_wall
        # from now_fn made every subsequently supplied explicit HLC with a small
        # timestamp fail the monotonicity invariant and be misclassified as skew.
        self._now = now_fn if now_fn is not None else self._system_now
        self.max_skew_us = max_skew_us
        self.last_wall = 0
        self.last_logical = 0

    @staticmethod
    def _system_now() -> int:
        import time

        return time.time_ns() // 1000

    def send_tick(self) -> HLC:
        """§5.2.1 — monotonic even when system clock jumps backwards."""
        now = self._now()
        if now > self.last_wall:
            wall, logical = now, 0
        elif now == self.last_wall:
            wall, logical = self.last_wall, self.last_logical + 1
        else:
            # clock moved backwards (NTP / sleep resume): keep wall, bump logical
            wall, logical = self.last_wall, self.last_logical + 1
        if logical >= self.LOGICAL_MAX:
            raise AssertionError("Logical clock overflow")
        self.last_wall, self.last_logical = wall, logical
        return HLC(wall, logical)

    def receive_tick(self, remote: HLC) -> HLC:
        """§5.2.2 with §5.3 skew clamp → CLOCK_DRIFT rejection."""
        now = self._now()
        if remote.wall > now + self.max_skew_us:
            raise ClockDriftError(
                f"remote.wall {remote.wall} beyond now+{self.max_skew_us}"
            )
        wall = max(now, self.last_wall, remote.wall)
        if wall == now and wall == self.last_wall and wall == remote.wall:
            logical = max(self.last_logical, remote.logical) + 1
        elif wall == self.last_wall:
            logical = self.last_logical + 1
        elif wall == remote.wall:
            logical = remote.logical + 1
        else:
            logical = 0
        if logical >= self.LOGICAL_MAX:
            raise AssertionError("Logical clock overflow")
        self.last_wall, self.last_logical = wall, logical
        return HLC(wall, logical)
