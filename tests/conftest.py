"""Shared test fixtures for BIN-SYNC test-suite."""

import uuid

import pytest

from binsync.node import Node


def nid(seed: int) -> bytes:
    """Deterministic 128-bit nodeId from a small seed (big-endian padded)."""
    return seed.to_bytes(16, "big")


class FakeClock:
    """Controllable µs clock.

    BUGFIX (harness finding): the previous version auto-incremented on every call,
    so explicit HLC timestamps in tests were silently inconsistent with the node's
    own send-tick stream and CLOCK_DRIFT / skew assertions became order-dependent.
    The clock now returns exactly what was set; explicit advancement is done via
    set()/advance()."""

    def __init__(self, t: int = 1_000_000) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t

    def set(self, t: int) -> None:
        self.t = t

    def advance(self, dt: int = 1) -> None:
        self.t += dt


def make_node(seed: int, now_fn=None) -> Node:
    return Node(node_id=nid(seed), epoch=1, now_fn=now_fn or FakeClock(seed * 1_000_000))


@pytest.fixture
def nodes():
    def _factory(count: int):
        return [make_node(i + 1) for i in range(count)]

    return _factory
