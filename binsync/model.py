"""Data model (§7): Record, FieldState, lifecycle (delete-wins), ghosts."""

from __future__ import annotations

from dataclasses import dataclass, field

from .hlc import HLC

LIFECYCLE_FIELD_ID = 0x0000        # §7.2 reserved
FIELD_RESOLVE_ID = 0x00FF          # §8.9 MANUAL resolve field
USER_FIELD_MIN = 0x0100

ALIVE = 0x00
DEAD = 0x01


@dataclass(frozen=True)
class FieldState:
    value: bytes
    hlc: HLC
    authorNodeId: bytes  # durable 128-bit author, never the alias (§4.6, [А1])
    keyEpoch: int = 0


@dataclass
class Record:
    recordId: bytes            # b128
    namespaceId: int           # u16
    state: dict[int, FieldState] = field(default_factory=dict)
    lifecycleHlc: HLC | None = None
    lifecycleAuthor: bytes | None = None
    lifecycleValue: int = ALIVE
    # tombstones of individual fields: fieldId -> FieldState(value=b"")
    fieldTombs: dict[int, FieldState] = field(default_factory=dict)

    @property
    def isDead(self) -> bool:
        return self.lifecycleValue == DEAD

    def visible_fields(self) -> dict[int, FieldState]:
        """Ghost semantics (§7.4): when dead, fields with hlc <= lifecycle.hlc are
        stored as ghosts (hidden); fields newer than lifecycle are visible but the
        entity still counts as deleted until explicit undelete. For read purposes
        on a live record all non-tombstoned fields are visible."""
        out = {}
        for fid, fs in self.state.items():
            if self.isDead and not _ghost_status_by_author(fs, self):
                # field hlc > lifecycle hlc → visible per §7.4 wording
                out[fid] = fs
            elif not self.isDead:
                out[fid] = fs
        return out


def _ghost_status_by_author(fs: FieldState, rec: Record) -> bool:
    """[D7]/[У7]: ghost status computed by comparing field.authorNodeId against
    record.lifecycleAuthor — NEVER by session alias `origin`.

    SPEC HARDENING (implementation finding): the second clause of §7.4 ("Поля с
    hlc > lifecycle.hlc видны") does NOT contradict [D7] once undelete is modelled
    correctly — but [D7]'s literal "<=" must be replaced by the STRICT form
    "field loses ghost status iff its full order position is strictly greater than
    the lifecycle's" (author is the tie-break at equal times). Rationale:
      * Scenario 38 step 4 requires ghosting to depend on TIME only: after an
        undelete at a later wall, historical fields lose ghost status regardless of
        author ordering. With the literal "<=" (no author tie-break) an offline
        LATER write by a SMALLER author stays a ghost while an EARLIER write by a
        LARGER author shows — non-deterministic visibility across peers.
      * The equality case keeps the full-order comparison so that a concurrent
        delete/write at identical (wall, logical) resolves consistently with the
        delete-wins rule [У1] ("не больше Т" includes ties won by the delete when
        the delete's durable author is larger).
    """
    from .hlc import compare_hlc

    if rec.lifecycleHlc is None or rec.lifecycleAuthor is None:
        return False
    if not rec.isDead:
        # SPEC HARDENING (§7.4, found during implementation): the spec defines ghost
        # status only for DEAD records ("Запись удалена → все её поля ..."). On a live
        # record every stored field is visible — otherwise an undeleted record would
        # permanently hide its own history (scenario 38 step 4: поле теряет статус
        # призрака после revive).
        return False
    # time dominates; durable author is only the tie-break at exactly equal times
    if fs.hlc.key() != rec.lifecycleHlc.key():
        return fs.hlc.key() < rec.lifecycleHlc.key()
    return compare_hlc(fs.hlc, fs.authorNodeId, rec.lifecycleHlc, rec.lifecycleAuthor) <= 0


def is_ghost(field_state: FieldState, record: Record) -> bool:
    """Public helper implementing [D7]."""
    return _ghost_status_by_author(field_state, record)
