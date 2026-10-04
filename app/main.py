"""HTTP API for reconciling multi-replica offline writes.

A batch of events (each carrying a per-replica sequence number and a vector
clock covering every replica) is validated against the causal history it
declares, and then reduced to the effective version of every key under the
vector-clock partial order.

Besides the resolution endpoint, a catch-up endpoint turns the same batch
into a directly replayable incremental transfer plan for a target replica:
given the target's ``known_clock`` (the highest seq contiguously received
from each source), missing events are scheduled into rounds of causally
satisfied dependencies.

A conflict-resolution endpoint turns a confirmed resolution of a still
conflicting key into a new, replayable event: it appends a put/delete event
to the writing replica, with a clock covering every conflicting version and
the writer's own prior history, and returns the new event together with the
key's post-append verdict.

The three endpoints above are stateless: request payloads are never persisted.
A second family of endpoints (/api/histories) gives multiple callers one
shared, persistent history created from a valid batch: callers then merge in
more offline events by ID (identical events -- full content including every
clock component -- are ignored; same replica_id/seq with different key,
operation, value or clock, including a missing or extra-zero clock
component, is 409) and commit conflict resolutions
by ID with only the original resolution parameters (key, writer, versions,
op) -- the stored versions are re-checked at commit time and stale lists get
409.  Histories live in a local SQLite database (HISTORY_DB_PATH, WAL) and
therefore survive container restarts.

A read-only withdrawal-preview endpoint
(/api/histories/{id}/withdraw-preview) rehearses, on one committed history,
which events and key verdicts withdrawing a single offline event would
affect: the target and everything that directly or transitively depends on
it (across keys) are reported as withdrawn, causally unrelated concurrent
events remain, and every changed key is shown with its before/after verdict
(null after when the key empties).  It never modifies the stored history,
renumbers seqs or rewrites clocks.

A key-migration endpoint (/api/histories/{id}/migrate-key) moves a key's
current put value onto an idle key in one atomic commit and returns the two
new, independently replayable events.  Given a writing replica, source/target
keys and both keys' currently observed maximal versions, it appends to the
writer two consecutive seqs -- first a delete of the source key, then a put
of the source value on the target key; the first event's clock covers both
keys' current versions and the writer's prior history, and the second covers
the first.  Both keys are re-checked inside the same serialised transaction:
the source must be a unique put, the target must have no events or a unique
delete, and each version list must equal exactly that key's current maximal
events (an empty list for an eventless target); a stale state or version list
is 409, structural violations and fewer than two free event slots are 422.
On success the source reads deleted, the target holds the source value, and
every other key's verdict is unchanged; on failure the history is untouched.

A third family of endpoints (/api/histories/{id}/import-sessions) stages
offline events that arrive in several batches into an import session before
they touch the persistent history.  A session is created empty by history
ID and accepts event appends where the per-event checks (fields, replica
membership, full clock coverage, self component == seq) apply only to
genuinely new (replica_id, seq) identities; missing predecessors or
cross-replica dependencies are tolerated while staging and only fail at
commit time.  A retransmission of an identity already staged or seen
earlier in the same batch is compared on full content instead: identical
events (full content including every clock component, key order
irrelevant) are ignored, while the same replica_id/seq with different key,
operation, value or clock -- including a missing component, an extra
component even when its value is 0, or a changed own component -- is 409,
never 422.  The 120-event session cap counts already-staged identities
plus the new identities after in-batch deduplication, so 121 identical
resends stage exactly one event and a full session re-sending its events
still succeeds.  Committing merges the staged events into the
history's latest committed state with the original full-equality, causal
and 120-event rules (content mismatch 409, missing dependencies or other
violations 422) and closes the session; the merge and the close take effect
in the same transaction, a failed commit leaves the open session and its
staged events untouched for a retry, and a closed session rejects further
appends and commits with 409.  Contiguity is checked from the events
actually present -- gaps are summarised as closed intervals and the span
is never expanded, so a staged sparse seq such as 10**12 commits as one
cheap ``non_contiguous_seq`` 422 instead of exhausting memory; no upper
seq bound is imposed.  Staging never affects the history reads or
verdicts, and sessions persist in the same SQLite database, so they survive
restarts.

A read-only missing-ranges endpoint
(/api/histories/{id}/import-sessions/{sid}/missing-ranges) reports, from one
committed snapshot of the history and one session (open or closed), which
per-replica seq ranges still have to be fetched from the source: a staged
event whose full content differs from the committed event under the same
identity is a 409 listing every conflicting replica_id/seq; otherwise each
replica's required upper bound is the max of its own highest merged seq and
the highest clock component any merged event carries for it, and the missing
seqs from 1 to that bound come back as maximal contiguous closed ranges
(never enumerated one by one, so a 10**12 bound is still one range).  It
commits nothing and never changes the history, the staged events or the
session state.

A read-only evidence-batch endpoint
(/api/histories/{id}/evidence-batch) extracts, for a non-empty duplicate-free
list of selected keys, a self-contained causal evidence batch: every event
touching a selected key (including puts and deletes later overwritten on that
key) plus the recursive causal closure -- each event's previous event on its
own replica and the events other replicas' non-zero clock components point at,
which may themselves touch auxiliary keys that are only carried as
dependencies.  Nothing outside the closure is added.  The response carries the
history's original replica_ids and the selected events in original wire
format, sorted by replica_id then seq, with no renumbered seqs or rewritten
clocks, so it passes the original batch validation unchanged and can be
POSTed as-is to /api/resolve, /api/catchup and /api/resolve-conflict.  The
selected keys' verdicts over the batch equal their verdicts over the whole
history; an auxiliary key's verdict may differ (it only supplies dependency
evidence).  Unknown history or a selected key with no event is 404; an illegal
keys list is 422.  The query is read-only.

A read-only event-relation endpoint
(/api/histories/{id}/event-relation) explains the causal relation between any
two events of one committed history, across keys: given two {replica_id, seq}
identifiers (first/second) it reports whether the first event happens before
or after the second, whether the two are concurrent, or whether they are the
same event.  A before/after answer carries a witness chain of event
identities from the earlier to the later event in which every event directly
depends on the previous one (its own-replica seq - 1, or the event one of its
non-zero cross-replica clock components points at); the chain has the fewest
possible edges, ties broken by the lexicographically smallest (replica_id,
seq) sequence with seq compared numerically.  A concurrent answer carries an
empty chain plus, for each side, one clock component on which that side's own
clock exceeds the other's (smallest replica ID when several qualify) together
with both events' values; identical identifiers yield a single-node chain.
The relation and its evidence come from one committed snapshot of the history
(staged import-session events are not part of it) and the query never
modifies the history or any session.  Unknown history or event IDs are 404;
illegal identifiers are 422.

A replica-admission endpoint (/api/histories/{id}/replicas) lets an already
created persistent history accept one more replica without closing existing
import sessions.  On success the new ID is appended to replica_ids and every
committed event and every staged event of every open or closed session gains
a zero clock component for it; seq, key, operation, value, existing clock
components, session IDs and statuses stay untouched, so existing key
verdicts do not change.  Staging tolerates missing dependencies, so no
full-batch validation gates admission.  The expanded history row and the
rewritten session rows commit in one serialised transaction and reads use
one snapshot, so a reader can never see a mixed replica set; concurrency
with legacy merges and session staging/commits cannot lose events.  Unknown
history is 404, an already-member replica is 409, an empty ID or membership
beyond eight replicas is 422.  The response is the expanded history's normal
read structure; subsequent writes still require the full clock (the server
does not auto-fill missing request components), the admitted replica may
then write from seq 1, the result survives restarts, and replicas can only be
added -- never removed or renamed.

A fourth family of endpoints (/api/histories/{id}/receipts) records, per
receiving replica, the causal prefix it has acknowledged and queries the
stable watermark every receiver has in common.  A report carries the
receiving replica id and a known_clock that must cover exactly the history's
current replicas with non-negative, non-boolean integer components, each no
greater than that source's highest *committed* seq; every already-received
event must keep its clock within the declared prefix (staged sessions are
not receivable), and any violation is 422.  A re-report may not move any
component below the stored receipt (409); an identical report is an
idempotent retransmission and succeeds; unknown history or receiving replica
is 404.  The validation and the upsert run against one locked committed
state, so concurrent reports serialise and can never overwrite a larger
acknowledged prefix.  Receipts persist in the same SQLite database.  Replica
admission rewrites every old receipt with a zero component for the new
replica and the new receiver counts as all-zero, all in one transaction with
the expanded history and sessions: a query never mixes pre/post-expansion
replica sets or receipts, and the componentwise-minimum stable clock (over
all current receivers, unreported ones as zero) is allowed to drop.
"""

from __future__ import annotations

import json
import uuid
from collections import deque
from typing import Annotated, Iterable, Optional

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictStr
from pydantic import model_validator

from . import store

app = FastAPI(
    title="Multi-Replica Offline Write Reconciler",
    version="1.0.0",
    description=(
        "Validate a batch of offline write/delete events from multiple "
        "replicas, resolve the effective (maximal) version of each key, and "
        "build replayable incremental catch-up plans for a target replica."
    ),
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class Event(BaseModel):
    """A single put/delete event observed on one replica."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Non-empty ID of the originating replica."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Per-replica sequence number, starting at 1.",
        ),
    ]
    key: Annotated[str, Field(min_length=1, description="Non-empty key.")]
    put: Optional[StrictStr] = Field(
        default=None, description="Set when the event is a put (empty string allowed)."
    )
    delete: Optional[StrictStr] = Field(
        default=None, description="Must equal 'delete' when the event is a deletion."
    )
    clock: dict[
        Annotated[str, Field(min_length=1)],
        Annotated[int, Field(strict=True, ge=0)],
    ] = Field(
        ...,
        description=(
            "Vector clock covering every replica ID; non-negative integer "
            "component per replica."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_op(self) -> "Event":
        has_put = self.put is not None
        has_delete = self.delete is not None
        if has_put == has_delete:
            raise ValueError(
                "exactly one of 'put' (string value) or 'delete' (\"delete\") "
                "must be provided"
            )
        if has_delete and self.delete != "delete":
            raise ValueError("the 'delete' field must equal the string \"delete\"")
        return self


class ResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replica_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]],
        Field(
            min_length=2,
            max_length=8,
            description="2-8 non-empty, unique replica IDs.",
        ),
    ]
    events: Annotated[
        list[Event],
        Field(min_length=1, max_length=120, description="1-120 events, any order."),
    ]


class CatchupRequest(ResolveRequest):
    known_clock: Annotated[
        dict[
            Annotated[str, Field(min_length=1)],
            Annotated[int, Field(strict=True, ge=0)],
        ],
        Field(
            ...,
            description=(
                "Per-source highest seq contiguously received by the target; "
                "one non-negative integer component per replica_id."
            ),
        ),
    ]


class ConflictVersion(BaseModel):
    """One of the currently maximal (conflicting) versions of a key."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Replica that produced the version."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Seq of the maximal event on that replica.",
        ),
    ]


class ConflictDecision(BaseModel):
    """The original conflict-resolution parameters, without any batch."""

    model_config = ConfigDict(extra="forbid")

    key: Annotated[str, Field(min_length=1, description="Conflicting key to resolve.")]
    replica_id: Annotated[
        str,
        Field(
            min_length=1,
            description="Replica that writes the resolution event; must be "
            "one of the history's replica_ids.",
        ),
    ]
    versions: Annotated[
        list[ConflictVersion],
        Field(
            min_length=2,
            description=(
                "The conflicting versions as seen when resolving: each with "
                "replica_id and seq; no duplicates, at least two."
            ),
        ),
    ]
    put: Optional[StrictStr] = Field(
        default=None, description="Set when the resolution is a put (empty string allowed)."
    )
    delete: Optional[StrictStr] = Field(
        default=None,
        description="Must equal 'delete' when the resolution is a deletion.",
    )

    @model_validator(mode="after")
    def _exactly_one_op(self) -> "ConflictDecision":
        has_put = self.put is not None
        has_delete = self.delete is not None
        if has_put == has_delete:
            raise ValueError(
                "exactly one of 'put' (string value) or 'delete' (\"delete\") "
                "must be provided"
            )
        if has_delete and self.delete != "delete":
            raise ValueError("the 'delete' field must equal the string \"delete\"")
        return self


class ResolveConflictRequest(ResolveRequest, ConflictDecision):
    """Original batch plus the confirmed resolution of one conflicting key."""


class MergeEventsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: Annotated[
        list[Event],
        Field(min_length=1, max_length=120, description="1-120 events to merge, any order."),
    ]


class KeyMigrationRequest(BaseModel):
    """Move one key's current value onto an idle key as two replayable events.

    Structural shape violations (missing fields, empty keys, malformed version
    entries) are schema 422s; the whole-request semantic 422s (equal keys,
    unknown writing/version replica, duplicate versions, no room for two more
    events) are raised in the same BatchError shape as everywhere else.
    """

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(
            min_length=1,
            description=(
                "Replica that writes the migration; must be one of the "
                "history's replica_ids."
            ),
        ),
    ]
    source_key: Annotated[
        str,
        Field(
            min_length=1,
            description="Key whose current (single put) value is moved away.",
        ),
    ]
    target_key: Annotated[
        str,
        Field(
            min_length=1,
            description=(
                "Idle destination key: no events at all, or a single delete "
                "as its current verdict."
            ),
        ),
    ]
    source_versions: Annotated[
        list[ConflictVersion],
        Field(
            description=(
                "The source key's current maximal events as observed by the "
                "caller; must equal them exactly (the source is a unique put, "
                "so this is exactly one entry on success)."
            ),
        ),
    ] = []
    target_versions: Annotated[
        list[ConflictVersion],
        Field(
            description=(
                "The target key's current maximal events as observed by the "
                "caller; must equal them exactly -- an empty list when the "
                "target key has no events, one delete version otherwise."
            ),
        ),
    ] = []


class EvidenceBatchRequest(BaseModel):
    """The selected keys whose causal evidence batch is extracted.

    A non-empty list of non-empty, unique keys; structural shape violations
    (missing/non-list/empty/non-string entries) are schema 422s and duplicate
    keys are rejected with a ``duplicate_key`` 422 in the same BatchError
    shape as the other whole-request semantic failures.
    """

    model_config = ConfigDict(extra="forbid")

    keys: Annotated[
        list[Annotated[str, Field(min_length=1)]],
        Field(
            min_length=1,
            description="Non-empty list of non-empty, unique keys to extract.",
        ),
    ]


class StageEventsRequest(BaseModel):
    """A session append: 1+ raw events, no hard cap on the request length.

    Retransmissions of an already-staged (or earlier-in-batch) identity are
    deduplicated by full content, so a batch may legitimately carry more than
    120 raw entries (e.g. 121 identical resends) while staging far fewer
    distinct events.  The 120-event cap is enforced on the number of distinct
    identities after deduplication.
    """

    model_config = ConfigDict(extra="forbid")

    events: Annotated[
        list[Event],
        Field(
            min_length=1,
            description=(
                "One or more events to stage, any order; same-identity "
                "retransmissions are compared by full content and deduplicated."
            ),
        ),
    ]


class EventRef(BaseModel):
    """Pointer to one event of a history: originating replica plus seq."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Non-empty ID of the originating replica."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Per-replica sequence number, starting at 1.",
        ),
    ]


class EventRelationRequest(BaseModel):
    """The two events whose causal relation is explained.

    The relation direction is anchored at ``first``: ``before``/``after``
    mean ``first`` causally precedes/follows ``second``.
    """

    model_config = ConfigDict(extra="forbid")

    first: EventRef = Field(
        ..., description="The event the relation direction is anchored at."
    )
    second: EventRef = Field(
        ..., description="The event ``first`` is compared against."
    )


class AdmitReplicaRequest(BaseModel):
    """A new replica joining an already-created persistent history.

    Only the new replica's ID is carried; the history ID is in the path.
    The empty string (or a non-string value) is rejected by schema (422);
    the replica already being a member is 409 and membership growing beyond
    eight replicas is 422, both evaluated under the history's write lock.
    """

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(
            min_length=1,
            description="Non-empty ID of the replica admitted to the history.",
        ),
    ]


class ReceiptReportRequest(BaseModel):
    """One receiving replica's acknowledged causal prefix of a history.

    ``known_clock`` must cover exactly the history's current replicas with
    non-negative, non-boolean integer components; semantic checks (exact
    coverage, no component past the highest committed seq, received events'
    clocks staying within the prefix) run against the same committed state
    the receipt is saved to, under one serialised write transaction.
    """

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(
            min_length=1,
            description="Non-empty ID of the receiving replica reporting in.",
        ),
    ]
    known_clock: Annotated[
        dict[
            Annotated[str, Field(min_length=1)],
            Annotated[int, Field(strict=True, ge=0)],
        ],
        Field(
            ...,
            description=(
                "Acknowledged causal prefix: one non-negative, non-boolean "
                "integer component per current replica_id."
            ),
        ),
    ]


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class BatchError(Exception):
    """A whole-batch semantic validation failure (HTTP 422)."""

    def __init__(self, errors: list[dict]):
        super().__init__("invalid event batch")
        self.errors = errors


class ConflictStateError(Exception):
    """The declared resolution no longer matches the batch state (HTTP 409)."""

    def __init__(self, errors: list[dict]):
        super().__init__("conflict resolution out of date")
        self.errors = errors


class HistoryNotFound(Exception):
    """No persistent history exists for the referenced ID (HTTP 404)."""

    def __init__(self, history_id: str):
        super().__init__(f"history {history_id!r} not found")
        self.history_id = history_id


class EventNotFound(Exception):
    """A history exists but it has no event with that replica_id/seq (404)."""

    def __init__(self, replica_id: str, seq: int):
        super().__init__(f"event ({replica_id!r}, {seq}) not found")
        self.replica_id = replica_id
        self.seq = seq


class SelectedKeyNotFound(Exception):
    """A selected key has no event in the referenced history (404)."""

    def __init__(self, keys: list[str]):
        super().__init__(f"selected key(s) not found: {keys!r}")
        self.keys = keys


class SessionNotFound(Exception):
    """No import session with that ID exists on the history (HTTP 404)."""

    def __init__(self, session_id: str):
        super().__init__(f"import session {session_id!r} not found")
        self.session_id = session_id


class SessionClosedError(Exception):
    """The import session is already closed; no more appends/commits (409)."""

    def __init__(self, session_id: str):
        super().__init__(f"import session {session_id!r} is closed")
        self.session_id = session_id


class ReplicaAlreadyExistsError(Exception):
    """The replica offered for admission is already a member (HTTP 409)."""

    def __init__(self, replica_id: str):
        super().__init__(f"replica {replica_id!r} already in replica_ids")
        self.replica_id = replica_id


class ReceiptReplicaNotFound(Exception):
    """A receipt is reported for a replica outside the history (HTTP 404)."""

    def __init__(self, replica_id: str):
        super().__init__(f"receiving replica {replica_id!r} not in replica_ids")
        self.replica_id = replica_id


class ReceiptRegressionError(Exception):
    """A re-report acknowledges less than the stored receipt (HTTP 409)."""

    def __init__(self, replica_id: str, current: dict[str, int], incoming: dict[str, int]):
        super().__init__(
            f"receipt of replica {replica_id!r} regresses from {current} to {incoming}"
        )
        self.replica_id = replica_id
        self.current = current
        self.incoming = incoming


def _err(code: str, message: str, event_index: int | None = None) -> dict:
    e: dict = {"code": code, "message": message}
    if event_index is not None:
        e["event_index"] = event_index
    return e


# ---------------------------------------------------------------------------
# Validation + resolution
# ---------------------------------------------------------------------------


def _clock_le(
    a: dict[str, int], b: dict[str, int], replicas: list[str]
) -> tuple[bool, bool]:
    """Return (<= under componentwise order, < under partial order).

    ``a <= b`` when every component of ``a`` is <= the corresponding
    component of ``b``; ``a < b`` when additionally some component is
    strictly smaller.
    """

    any_less = False
    for r in replicas:
        x, y = a[r], b[r]
        if x > y:
            return False, False
        if x < y:
            any_less = True
    return True, any_less


def _closed_gap_ranges(
    present: Iterable[int], upper: int
) -> list[tuple[int, int]]:
    """Missing numbers of the closed span ``1..upper`` as maximal closed ranges.

    Given the numbers that are *present*, return the largest possible
    contiguous runs of missing ones as ``(start, end)`` closed-interval pairs
    sorted ascending.  Iteration is bounded solely by the present identities
    -- never by the span itself -- so a ``10**12`` upper bound with only a
    handful of present identities costs a handful of intervals: no missing
    number is ever allocated, enumerated or listed one by one.
    """

    ranges: list[tuple[int, int]] = []
    cursor = 1
    for n in sorted(present):
        if n > cursor:
            ranges.append((cursor, n - 1))
        cursor = n + 1
    if cursor <= upper:
        ranges.append((cursor, upper))
    return ranges


def _format_gap_ranges(ranges: list[tuple[int, int]]) -> str:
    """Render closed gap intervals compactly, e.g. ``[2, 2], [4, 10**12]``."""

    return ", ".join(f"[{start}, {end}]" for start, end in ranges)


def validate_batch(
    replica_ids: list[str], events: list[Event]
) -> dict[tuple[str, int], Event]:
    """Validate the batch and index events by (replica_id, seq).

    Raises BatchError (-> 422) on any violation, pointing at event indices.
    Every check works from the events actually present: contiguity gaps are
    diagnosed from the per-replica seq sets and summarised as closed intervals,
    never by expanding ``1..max(seq)``, so a sparse ``10**12`` seq is rejected
    cheaply with one ``non_contiguous_seq`` error instead of exhausting memory.
    """

    errors: list[dict] = []

    # Rule 1: replica IDs unique (cardinality/non-empty handled by schema).
    seen_replicas: set[str] = set()
    duplicate_replicas: set[str] = set()
    for rid in replica_ids:
        if rid in seen_replicas:
            duplicate_replicas.add(rid)
        seen_replicas.add(rid)
    for rid in sorted(duplicate_replicas):
        errors.append(
            _err("duplicate_replica_id", f"replica_id {rid!r} appears more than once")
        )

    # Per-replica structural checks, one pass.
    seen: dict[tuple[str, int], Event] = {}
    seen_index: dict[tuple[str, int], int] = {}
    seqs_by_replica: dict[str, list[int]] = {rid: [] for rid in replica_ids}

    for i, ev in enumerate(events):
        bad = False

        if ev.replica_id not in seen_replicas:
            errors.append(
                _err(
                    "unknown_replica_id",
                    f"event replica_id {ev.replica_id!r} is not listed in replica_ids",
                    i,
                )
            )
            bad = True
        else:
            seqs_by_replica[ev.replica_id].append(ev.seq)

        # Rule 3: clock covers exactly all replicas, non-negative integers.
        clock_keys = set(ev.clock)
        if clock_keys != seen_replicas:
            missing = sorted(seen_replicas - clock_keys)
            extra = sorted(clock_keys - seen_replicas)
            parts = []
            if missing:
                parts.append(f"missing components for {missing}")
            if extra:
                parts.append(f"unexpected components {extra}")
            errors.append(
                _err(
                    "clock_replica_coverage",
                    "clock must cover exactly all replica_ids: " + "; ".join(parts),
                    i,
                )
            )
            bad = True

        # Rule 4a: per-replica seq unique.
        key = (ev.replica_id, ev.seq)
        if key in seen:
            errors.append(
                _err(
                    "duplicate_seq",
                    f"replica {ev.replica_id!r} already has an event with seq {ev.seq} "
                    f"(event index {seen_index[key]})",
                    i,
                )
            )
            bad = True
        else:
            seen[key] = ev
            seen_index[key] = i

    # Rule 4b: per-replica seq must be a contiguous 1..N range.  Gaps are
    # derived from the present seq set as closed intervals; the missing
    # numbers (up to a 10**12-large max seq) are never themselves allocated
    # or enumerated.
    valid_seqs: dict[str, set[int]] = {}
    for rid, seqs in seqs_by_replica.items():
        if not seqs:
            continue
        uniq = set(seqs)
        valid_seqs[rid] = uniq
        upper = max(uniq)
        gap_ranges = _closed_gap_ranges(uniq, upper)
        if len(uniq) != len(seqs) or gap_ranges:
            errors.append(
                _err(
                    "non_contiguous_seq",
                    f"seqs for replica {rid!r} must be unique and start at 1 with no "
                    f"gaps; got sorted seqs {sorted(seqs)} "
                    f"(missing closed interval(s): {_format_gap_ranges(gap_ranges)})",
                )
            )

    if errors:
        raise BatchError(errors)

    # From here the structure is sound; replica/seq/clock lookups are safe.

    # Rule 5: the clock's own component must equal this event's seq.
    for i, ev in enumerate(events):
        if ev.clock[ev.replica_id] != ev.seq:
            raise BatchError(
                [
                    _err(
                        "clock_self_component",
                        f"clock[{ev.replica_id!r}] = {ev.clock[ev.replica_id]} "
                        f"must equal seq = {ev.seq}",
                        i,
                    )
                ]
            )

    # Rules 6 + 7: referenced history must exist and its clocks must be
    # strictly smaller (componentwise <= with at least one strict <).
    for i, ev in enumerate(events):
        referenced: list[Event] = []

        # The immediately previous event on this replica.
        if ev.seq > 1:
            referenced.append(seen[(ev.replica_id, ev.seq - 1)])

        # For every other replica, the latest event its component points at;
        # component 0 means "nothing known".
        for rid in replica_ids:
            if rid == ev.replica_id:
                continue
            n = ev.clock[rid]
            if n > 0:
                if n not in valid_seqs.get(rid, set()):
                    raise BatchError(
                        [
                            _err(
                                "clock_points_to_missing_event",
                                f"clock[{rid!r}] = {n}, but replica {rid!r} has no "
                                f"event with seq {n}",
                                i,
                            )
                        ]
                    )
                referenced.append(seen[(rid, n)])

        for ref in referenced:
            le, any_less = _clock_le(ref.clock, ev.clock, replica_ids)
            if not le or not any_less:
                ref_idx = seen_index[(ref.replica_id, ref.seq)]
                kind = "not <= current clock" if not le else "equal to current clock"
                raise BatchError(
                    [
                        _err(
                            "clock_causality",
                            f"referenced event (index {ref_idx}, replica "
                            f"{ref.replica_id!r}, seq {ref.seq}) clock {ref.clock} is "
                            f"{kind} {ev.clock}; it must be componentwise <= with at "
                            f"least one strictly smaller component",
                            i,
                        )
                    ]
                )

    return seen


def _maximal_events(group: list[Event], replica_ids: list[str]) -> list[Event]:
    """Maximal events of one key under the vector-clock partial order.

    Maximal = no *other* event in the group is strictly greater.
    """

    maximals: list[Event] = []
    for ev in group:
        dominated = False
        for other in group:
            if other is ev:
                continue
            le, any_less = _clock_le(ev.clock, other.clock, replica_ids)
            if le and any_less:
                dominated = True
                break
        if not dominated:
            maximals.append(ev)

    maximals.sort(key=lambda e: (e.replica_id, e.seq))
    return maximals


def _event_shape(ev: Event, replica_ids: list[str]) -> dict:
    """Verdict-shaped event projection used in resolve responses."""

    return {
        "replica_id": ev.replica_id,
        "seq": ev.seq,
        "op": "put" if ev.put is not None else "delete",
        "value": ev.put,
        "clock": {rid: ev.clock[rid] for rid in replica_ids},
    }


def _event_wire(ev: Event) -> dict:
    """Event in request wire format (exactly one of put/delete present).

    The returned object can be appended to a batch's ``events`` and replayed
    through /api/resolve and /api/catchup unchanged.
    """

    out = {
        "replica_id": ev.replica_id,
        "seq": ev.seq,
        "key": ev.key,
    }
    if ev.put is not None:
        out["put"] = ev.put
    else:
        out["delete"] = "delete"
    out["clock"] = dict(ev.clock)
    return out


def _canonical_wire(ev: Event, replica_ids: list[str]) -> dict:
    """Wire form with clock keys in the history's replica_ids order."""

    out = _event_wire(ev)
    out["clock"] = {rid: ev.clock[rid] for rid in replica_ids}
    return out


def _wire_model(raw: dict, replica_ids: list[str]) -> Event:
    """Parse a stored wire event back into an Event (clock order irrelevant)."""

    payload = dict(raw)
    payload["clock"] = {rid: raw["clock"][rid] for rid in replica_ids}
    return Event.model_validate(payload)


def _load_row(row: object) -> tuple[str, list[str], list[Event]]:
    replica_ids: list[str] = json.loads(row["replica_ids"])
    events = [_wire_model(raw, replica_ids) for raw in json.loads(row["events"])]
    return row["id"], replica_ids, events


def _canonical_events(
    events: list[Event], replica_ids: list[str]
) -> list[dict]:
    """Canonical persisted/returned order: replica_id then seq."""

    return [
        _canonical_wire(ev, replica_ids)
        for ev in sorted(events, key=lambda e: (e.replica_id, e.seq))
    ]


def _history_state(
    history_id: str, replica_ids: list[str], events: list[Event]
) -> dict:
    """Full original-format batch plus the verdict over the same events."""

    ordered = sorted(events, key=lambda e: (e.replica_id, e.seq))
    return {
        "id": history_id,
        "replica_ids": list(replica_ids),
        "events": [_canonical_wire(ev, replica_ids) for ev in ordered],
        "results": resolve(ordered, replica_ids),
    }


def _batch_full_error() -> BatchError:
    return BatchError(
        [
            _err(
                "batch_full",
                "the history already contains 120 events; the 120-event "
                "limit leaves no room to merge or append more events",
            )
        ]
    )


def _same_event_content(a: Event, b: Event) -> bool:
    """Whether two events sharing an identity carry identical content.

    Compares the full wire content: key, operation kind, value and the
    *complete* clock -- not just the components projected onto the history's
    replica_ids.  Clock component order is irrelevant (dict equality), but a
    missing component, an extra component (even when its value is 0), or any
    differing component value makes the two events different content.
    """

    return (
        a.key == b.key
        and (a.put is None) == (b.put is None)
        and a.put == b.put
        and a.clock == b.clock
    )


def _merge_events(
    replica_ids: list[str],
    current: list[Event],
    incoming: list[Event],
) -> list[Event]:
    """Merge incoming events by (replica_id, seq) into the stored history.

    Identical events (same identity and same full content, including every
    clock component) are ignored; the same (replica_id, seq) with different
    key, operation, value or clock is a 409 -- an extra zero-valued clock
    component or a missing component already count as different content.
    The merged list is then checked against the original batch validation
    (so a genuinely new event with illegal clock coverage still gets 422) and
    the 120-event cap.  Either the whole merge applies or nothing does.
    """

    stored = {(ev.replica_id, ev.seq): ev for ev in current}

    merged = list(current)
    for i, ev in enumerate(incoming):
        prior = stored.get((ev.replica_id, ev.seq))
        if prior is not None:
            if not _same_event_content(prior, ev):
                raise ConflictStateError(
                    [
                        {
                            "code": "event_content_mismatch",
                            "message": (
                                f"event with replica_id {ev.replica_id!r}, "
                                f"seq {ev.seq} already exists in this history "
                                "with different content; stored "
                                f"{_event_wire(prior)!r}, incoming "
                                f"{_event_wire(ev)!r}"
                            ),
                            "event_index": i,
                        }
                    ]
                )
            # Same identity, identical content -> ignore.
            continue
        stored[(ev.replica_id, ev.seq)] = ev
        merged.append(ev)

    if len(merged) > 120:
        raise _batch_full_error()

    # Re-run the original validation on the whole merged history.
    validate_batch(replica_ids, merged)
    return merged


def _stage_events(
    replica_ids: list[str],
    staged: list[Event],
    incoming: list[Event],
) -> list[Event]:
    """Stage incoming events into an open import session.

    The per-event checks (unknown replica, clock must cover exactly all
    replica_ids, own clock component == seq; all 422) apply only to
    *genuinely new* (replica_id, seq) identities -- the first occurrence of
    an identity that is not already staged.  Missing predecessors or
    cross-replica dependencies of such events are tolerated here and only
    fail when the session is committed.

    A *retransmission* -- an identity already staged, or seen earlier in this
    same batch -- does not go through the per-event checks at all; a missing
    clock component, an extra component (even one with value 0) or a changed
    own component on a retransmitted identity is therefore never a 422.
    Retransmissions are compared by full content (key, operation kind, value
    and the *complete* clock; clock key order is irrelevant): identical
    content is ignored, different content is a 409 (event_content_mismatch)
    pointing at the offending batch index.

    The session stages at most 120 distinct identities: the cap counts
    already-staged identities plus the genuinely new identities after
    in-batch deduplication, so e.g. 121 identical resends of one legal event
    stage exactly one and a full (120-event) session re-sending its events
    still succeeds.  Either the whole append applies or nothing does.
    """

    replica_set = set(replica_ids)

    # Resolve every incoming identity against what is already staged and the
    # earlier entries of this same batch; only the first occurrence of an
    # identity that is not already staged is treated as genuinely new.
    staged_by_id = {(ev.replica_id, ev.seq): ev for ev in staged}
    seen_new: set[tuple[str, int]] = set()
    new_identity: list[bool] = []
    for ev in incoming:
        ident = (ev.replica_id, ev.seq)
        is_new = ident not in staged_by_id and ident not in seen_new
        new_identity.append(is_new)
        if is_new:
            seen_new.add(ident)

    # Per-event checks, collected in index order, for genuinely new events
    # only -- retransmissions are judged by full-content comparison below.
    errors: list[dict] = []
    for i, ev in enumerate(incoming):
        if not new_identity[i]:
            continue
        if ev.replica_id not in replica_set:
            errors.append(
                _err(
                    "unknown_replica_id",
                    f"event replica_id {ev.replica_id!r} is not listed in "
                    "replica_ids",
                    i,
                )
            )
        clock_keys = set(ev.clock)
        if clock_keys != replica_set:
            missing = sorted(replica_set - clock_keys)
            extra = sorted(clock_keys - replica_set)
            parts = []
            if missing:
                parts.append(f"missing components for {missing}")
            if extra:
                parts.append(f"unexpected components {extra}")
            errors.append(
                _err(
                    "clock_replica_coverage",
                    "clock must cover exactly all replica_ids: "
                    + "; ".join(parts),
                    i,
                )
            )
    if errors:
        raise BatchError(errors)

    # Own component check for genuinely new events only; those events have
    # exact coverage from here on, so the self-component lookup is safe.
    for i, ev in enumerate(incoming):
        if not new_identity[i]:
            continue
        if ev.clock[ev.replica_id] != ev.seq:
            raise BatchError(
                [
                    _err(
                        "clock_self_component",
                        f"clock[{ev.replica_id!r}] = {ev.clock[ev.replica_id]} "
                        f"must equal seq = {ev.seq}",
                        i,
                    )
                ]
            )

    merged_by_id = dict(staged_by_id)
    merged = list(staged)
    for i, ev in enumerate(incoming):
        ident = (ev.replica_id, ev.seq)
        prior = merged_by_id.get(ident)
        if prior is not None:
            if not _same_event_content(prior, ev):
                raise ConflictStateError(
                    [
                        {
                            "code": "event_content_mismatch",
                            "message": (
                                f"event with replica_id {ev.replica_id!r}, "
                                f"seq {ev.seq} is already present in this "
                                "session with different content; prior "
                                f"{_event_wire(prior)!r}, incoming "
                                f"{_event_wire(ev)!r}"
                            ),
                            "event_index": i,
                        }
                    ]
                )
            # Same identity, identical content -> ignore.
            continue
        merged_by_id[ident] = ev
        merged.append(ev)

    if len(merged) > 120:
        raise BatchError(
            [
                _err(
                    "batch_full",
                    "the import session already stages 120 distinct events; "
                    "the 120-event limit leaves no room to stage more",
                )
            ]
        )
    return merged


def _load_session_row(
    row: object, replica_ids: list[str]
) -> tuple[str, str, list[Event]]:
    events = [_wire_model(raw, replica_ids) for raw in json.loads(row["events"])]
    return row["id"], row["status"], events


def _admit_replica(
    replica_ids: list[str],
    committed: list[Event],
    sessions: list[tuple[str, list[Event]]],
    new_replica: str,
) -> tuple[list[str], list[Event], list[list[Event]]]:
    """Admit one new replica into a persistent history.

    The new replica is appended to ``replica_ids`` and every committed event
    plus every staged event of every open *and* closed import session gains a
    fresh zero clock component for it.  Seq, key, operation kind, value, the
    existing clock components, session IDs and statuses are all untouched;
    adding an all-zero component cannot change any key's verdict (every old
    event stays incomparable/ordered exactly as before -- the new replica's
    own writes are the only events that may later dominate on it).

    Staging tolerates missing dependencies by construction, so unlike a merge
    no full-batch validation runs here: nothing about the staged events'
    current completeness is allowed to block admission.

    A replica already in the set raises ReplicaAlreadyExistsError (409); a
    membership that would grow beyond eight raises BatchError (422).
    """

    if new_replica in set(replica_ids):
        raise ReplicaAlreadyExistsError(new_replica)
    if len(replica_ids) >= 8:
        raise BatchError(
            [
                _err(
                    "replica_limit_exceeded",
                    "a history holds at most 8 replicas and it already has "
                    f"{len(replica_ids)} ({sorted(replica_ids)}); admitting "
                    f"{new_replica!r} would exceed the limit",
                )
            ]
        )

    expanded = list(replica_ids) + [new_replica]

    # Stored invariants make this a plain assignment: a committed event's
    # clock covers exactly the current replica_ids (every write path runs
    # validate_batch), and each staged event is the first occurrence of its
    # identity, which staging accepts only with exact clock coverage (a
    # retransmission with an extra/missing component is a 409, never stored).
    patched_sessions: list[list[Event]] = []
    for _session_id, staged in sessions:
        for ev in staged:
            ev.clock[new_replica] = 0
        patched_sessions.append(staged)
    for ev in committed:
        ev.clock[new_replica] = 0

    return expanded, committed, patched_sessions


def _session_missing_ranges(
    replica_ids: list[str],
    committed: list[Event],
    staged: list[Event],
) -> list[dict]:
    """Seqs an import session must still fetch before it *could* be complete.

    The committed history and the staged events are read as one merged set of
    ``(replica_id, seq)`` identities.  First the two sources are checked
    against each other on the same identity: a staged event whose full content
    differs from the committed event under the same ``(replica_id, seq)`` is
    a 409 (and every such identity is reported together, sorted by
    replica_id then seq).  Identical content is the same event and counts once.

    For each replica the required upper bound is

        max(its own highest event seq,
            max clock component any merged event carries for it),

    i.e. everything the merged events causally claim to know must be present.
    Missing identities are the numbers in the closed span ``1..bound`` minus
    the identities already present; they are coalesced into the largest
    possible contiguous closed ranges.  The bound of a replica no event
    references and that has no events itself is 0, which yields no range.  The
    algorithm works from the present identities alone -- it never enumerates
    the individual missing numbers, so a bound of ``10**12`` still costs one
    range.

    An empty result only means every referenced identity is available; it
    says nothing about whether a commit would pass (causal order, caps).
    """

    committed_by_id = {(ev.replica_id, ev.seq): ev for ev in committed}

    # Same identity in both the committed history and the staging area must
    # carry identical *full* content; collect every mismatch before failing.
    conflicts: list[tuple[str, int]] = []
    for ev in staged:
        prior = committed_by_id.get((ev.replica_id, ev.seq))
        if prior is not None and not _same_event_content(prior, ev):
            conflicts.append((ev.replica_id, ev.seq))
    if conflicts:
        conflicts.sort()
        raise ConflictStateError(
            [
                {
                    "code": "event_content_mismatch",
                    "message": (
                        f"staged event with replica_id {rid!r}, seq {seq} "
                        "differs in content from the event with the same "
                        "identity already committed in this history; resolve "
                        "the conflicting retransmission before the session can "
                        "be completed"
                    ),
                    "replica_id": rid,
                    "seq": seq,
                }
                for rid, seq in conflicts
            ]
        )

    # Present identities per replica and the per-replica required upper bound.
    present: dict[str, set[int]] = {rid: set() for rid in replica_ids}
    bound: dict[str, int] = {rid: 0 for rid in replica_ids}

    def account(ev: Event) -> None:
        seqs = present.setdefault(ev.replica_id, set())
        seqs.add(ev.seq)
        if ev.seq > bound.get(ev.replica_id, 0):
            bound[ev.replica_id] = ev.seq
        # The clock covers exactly the history's replica_ids for committed
        # events and for genuinely new staged identities; use get so a
        # retransmission-shaped clock never raises a KeyError here.
        for rid in replica_ids:
            n = ev.clock.get(rid, 0)
            if n > bound.get(rid, 0):
                bound[rid] = n

    for ev in committed:
        account(ev)
    for ev in staged:
        account(ev)

    ranges: list[dict] = []
    for rid in replica_ids:
        upper = bound.get(rid, 0)
        if upper <= 0:
            continue
        # Coalesce the gaps inside 1..upper from the present identities only;
        # the shared helper iterates over present identities, never over the
        # (possibly 10**12-large) span itself.
        for start, end in _closed_gap_ranges(present.get(rid, ()), upper):
            ranges.append(
                {"replica_id": rid, "start_seq": start, "end_seq": end}
            )

    ranges.sort(key=lambda r: (r["replica_id"], r["start_seq"]))
    return ranges


def _session_state(
    session_id: str,
    history_id: str,
    status: str,
    events: list[Event],
    replica_ids: list[str],
) -> dict:
    """Session status plus the staged events in original wire format."""

    ordered = sorted(events, key=lambda e: (e.replica_id, e.seq))
    return {
        "id": session_id,
        "history_id": history_id,
        "status": status,
        "events": [_canonical_wire(ev, replica_ids) for ev in ordered],
    }


def _key_verdict(key: str, group: list[Event], replica_ids: list[str]) -> dict:
    """Reduce the events touching one key to its effective-version verdict."""

    maximals = _maximal_events(group, replica_ids)

    if len(maximals) == 1:
        m = maximals[0]
        if m.put is not None:
            return {
                "key": key,
                "state": "put",
                "value": m.put,
                "event": _event_shape(m, replica_ids),
            }
        return {"key": key, "state": "deleted", "event": _event_shape(m, replica_ids)}

    return {
        "key": key,
        "state": "conflict",
        "events": [_event_shape(m, replica_ids) for m in maximals],
    }


def resolve(events: list[Event], replica_ids: list[str]) -> list[dict]:
    """Compute maximal events per key under the vector-clock partial order."""

    by_key: dict[str, list[Event]] = {}
    for ev in events:
        by_key.setdefault(ev.key, []).append(ev)

    return [
        _key_verdict(key, by_key[key], replica_ids)
        for key in sorted(by_key)
    ]


# ---------------------------------------------------------------------------
# Read-only projections: withdrawal preview, evidence-batch extraction
# ---------------------------------------------------------------------------


def _direct_dependencies(
    ev: Event, replica_ids: list[str]
) -> set[tuple[str, int]]:
    """Events an event causally builds on: the previous event on its own
    replica and, for every other replica, the event its clock component
    points at (non-zero components only).  Stored histories have passed
    batch validation, so every referenced identity exists."""

    deps: set[tuple[str, int]] = set()
    if ev.seq > 1:
        deps.add((ev.replica_id, ev.seq - 1))
    for rid in replica_ids:
        if rid == ev.replica_id:
            continue
        n = ev.clock[rid]
        if n > 0:
            deps.add((rid, n))
    return deps


def preview_withdrawal(
    replica_ids: list[str],
    events: list[Event],
    target_replica: str,
    target_seq: int,
) -> dict:
    """Pure, non-mutating projection of what withdrawing one event would take.

    The withdrawn set is the target event plus every event that directly or
    transitively depends on it (its reverse reachability under the same
    dependency edges used by batch validation: each replica's previous event
    and the events other replicas' non-zero clock components point at).  All
    other events, including causally unrelated concurrent writes, are kept.

    Returns the withdrawn identities (sorted by replica_id then seq), the
    kept events in the original canonical order (same order, seqs and clocks
    -- nothing is renumbered), and the before/after verdict of every key
    whose full verdict structure would change (sorted by key); a key with no
    remaining event reports an ``after`` verdict of ``None``.
    """

    seen = {(ev.replica_id, ev.seq): ev for ev in events}
    if (target_replica, target_seq) not in seen:
        raise EventNotFound(target_replica, target_seq)

    # Reverse dependency graph: identity -> events that directly depend on it.
    dependents: dict[tuple[str, int], set[tuple[str, int]]] = {}
    for ev in events:
        for dep in _direct_dependencies(ev, replica_ids):
            dependents.setdefault(dep, set()).add((ev.replica_id, ev.seq))

    # Reverse reachability from the target = target plus its transitive
    # dependents, i.e. everything that would have to be withdrawn with it.
    withdrawn: set[tuple[str, int]] = set()
    frontier = [(target_replica, target_seq)]
    while frontier:
        ident = frontier.pop()
        if ident in withdrawn:
            continue
        withdrawn.add(ident)
        frontier.extend(dependents.get(ident, ()))

    ordered = sorted(events, key=lambda e: (e.replica_id, e.seq))
    kept = [ev for ev in ordered if (ev.replica_id, ev.seq) not in withdrawn]

    before_by_key = {v["key"]: v for v in resolve(ordered, replica_ids)}
    after_by_key = {v["key"]: v for v in resolve(kept, replica_ids)}
    touched_keys = {
        ev.key for ev in ordered if (ev.replica_id, ev.seq) in withdrawn
    }

    changes: list[dict] = []
    for key in sorted(touched_keys):
        before = before_by_key[key]
        after = after_by_key.get(key)
        # Compare the complete verdict structures (put/deleted/conflict
        # shapes differ as a whole), not just a state/value subset.
        if before != after:
            changes.append({"key": key, "before": before, "after": after})

    withdrawn_ids = sorted(withdrawn)
    return {
        "withdrawn": [
            {"replica_id": rid, "seq": seq} for rid, seq in withdrawn_ids
        ],
        "events": [_canonical_wire(ev, replica_ids) for ev in kept],
        "changes": changes,
    }


def build_evidence_batch(
    replica_ids: list[str],
    events: list[Event],
    keys: list[str],
) -> list[Event]:
    """Extract the independently replayable causal evidence batch for keys.

    The seeds are every event whose key is selected (all of them, including
    puts and deletes later overwritten on that key).  The seed set is then
    closed under the same forward dependency edges used by batch validation
    and the withdrawal preview: each included event's previous event on its
    own replica (``seq - 1``) and, for every other replica, the event its
    non-zero clock component points at.  Cross-key dependencies are kept, so
    events on auxiliary keys enter the batch solely as dependency evidence;
    no event outside the closure is added.  Stored histories have passed
    batch validation, so every referenced identity exists.

    The closure is a causal prefix per replica: whenever seq N on a replica
    is included, seq 1..N-1 on that replica are included transitively, and
    every cross-replica component points at an included event.  The selected
    events therefore keep their original seqs and clocks and the subset
    passes the original ``validate_batch`` unchanged, which is re-run as a
    guarantee before return.

    A selected key with no event raises SelectedKeyNotFound (-> 404); every
    missing key is reported together, sorted.
    """

    by_key: dict[str, list[Event]] = {}
    for ev in events:
        by_key.setdefault(ev.key, []).append(ev)

    missing = sorted({k for k in keys if k not in by_key})
    if missing:
        raise SelectedKeyNotFound(missing)

    by_id = {(ev.replica_id, ev.seq): ev for ev in events}
    included: set[tuple[str, int]] = set()
    frontier: list[tuple[str, int]] = []
    for key in keys:
        for ev in by_key[key]:
            ident = (ev.replica_id, ev.seq)
            if ident not in included:
                included.add(ident)
                frontier.append(ident)

    # Recursive causal closure: own-replica predecessor plus the events other
    # replicas' non-zero clock components point at (cross-key included).
    while frontier:
        ident = frontier.pop()
        for dep in _direct_dependencies(by_id[ident], replica_ids):
            if dep not in included:
                included.add(dep)
                frontier.append(dep)

    batch = [ev for ev in events if (ev.replica_id, ev.seq) in included]

    # The construction guarantees validity, but re-run the original full
    # validation so the returned batch is replayable by construction.
    validate_batch(replica_ids, batch)
    return batch


def _shortest_dependency_chain(
    replica_ids: list[str],
    events: list[Event],
    start: tuple[str, int],
    end: tuple[str, int],
) -> list[tuple[str, int]]:
    """Fewest-edge identity chain from ``start`` to ``end``.

    Consecutive identities are direct dependency edges: each event is the
    next event's own-replica predecessor (``seq - 1``) or the event one of
    the next event's non-zero cross-replica clock components points at.
    Among all chains with the fewest edges, the one whose (replica_id, seq)
    sequence is lexicographically smallest (replica_id as a string, seq
    numerically) is returned.  ``end`` must be a strict causal descendant of
    ``start`` (which a strict clock order already guarantees), so a chain
    always exists.
    """

    by_id = {(ev.replica_id, ev.seq): ev for ev in events}

    # Reverse adjacency: identity -> events that directly depend on it.
    dependents: dict[tuple[str, int], set[tuple[str, int]]] = {}
    for ev in events:
        for dep in _direct_dependencies(ev, replica_ids):
            dependents.setdefault(dep, set()).add((ev.replica_id, ev.seq))

    # Fewest edges to ``end`` for every causal ancestor, via BFS backwards
    # over the direct dependency edges.  A FIFO queue is essential: a stack
    # (DFS) can record a longer detour distance first depending on the
    # unspecified set iteration order -- the assigned distance is then both
    # non-minimal and hash-seed-dependent.  BFS first-discovery distances are
    # minimal regardless of neighbour iteration order.
    dist: dict[tuple[str, int], int] = {end: 0}
    frontier: deque[tuple[str, int]] = deque([end])
    while frontier:
        cur = frontier.popleft()
        for dep in _direct_dependencies(by_id[cur], replica_ids):
            if dep not in dist:
                dist[dep] = dist[cur] + 1
                frontier.append(dep)

    # Greedy walk from ``start``: always continue with the smallest identity
    # one edge closer to ``end``.  Every such identity lies on a fewest-edge
    # chain, and all those chains have the same length, so the greedy choice
    # yields the lexicographically smallest identity sequence among them.
    chain = [start]
    cur = start
    while cur != end:
        cur = min(
            succ
            for succ in dependents.get(cur, ())
            if dist.get(succ) == dist[cur] - 1
        )
        chain.append(cur)
    return chain


def explain_event_relation(
    replica_ids: list[str],
    events: list[Event],
    first: tuple[str, int],
    second: tuple[str, int],
) -> dict:
    """Causal relation between two events of one committed history.

    The direction is anchored at ``first``: ``before``/``after`` mean
    ``first`` causally precedes/follows ``second`` under the vector-clock
    partial order, ``concurrent`` means the two are incomparable and
    ``same`` means the identities coincide.  The comparison crosses keys:
    any two events of the history may be related.

    For ``before``/``after`` the result carries a ``chain`` of identities
    from the earlier to the later event (regardless of direction) in which
    every event directly depends on the previous one; the chain has the
    fewest possible edges, then the lexicographically smallest
    (replica_id, seq) sequence.  ``same`` yields the single-node chain.  For
    ``concurrent`` the chain is empty and ``first_ahead``/``second_ahead``
    each name one clock component (the smallest replica ID when several
    qualify) on which that side's own clock exceeds the other's, with both
    events' values.  An identity with no event raises EventNotFound.
    """

    by_id = {(ev.replica_id, ev.seq): ev for ev in events}
    if first not in by_id:
        raise EventNotFound(*first)
    if second not in by_id:
        raise EventNotFound(*second)

    if first == second:
        return {
            "relation": "same",
            "chain": [{"replica_id": first[0], "seq": first[1]}],
        }

    a, b = by_id[first], by_id[second]
    le_ab, less_ab = _clock_le(a.clock, b.clock, replica_ids)
    le_ba, less_ba = _clock_le(b.clock, a.clock, replica_ids)

    if le_ab and less_ab:
        relation, earlier, later = "before", first, second
    elif le_ba and less_ba:
        relation, earlier, later = "after", second, first
    else:
        relation = "concurrent"

    if relation != "concurrent":
        chain = _shortest_dependency_chain(replica_ids, events, earlier, later)
        return {
            "relation": relation,
            "chain": [{"replica_id": rid, "seq": seq} for rid, seq in chain],
        }

    # Concurrent: each side's clock exceeds the other's on at least one
    # component (distinct events of a valid history never share a clock);
    # report the smallest such replica ID per side, with both values.
    first_ahead = min(rid for rid in replica_ids if a.clock[rid] > b.clock[rid])
    second_ahead = min(rid for rid in replica_ids if b.clock[rid] > a.clock[rid])
    return {
        "relation": "concurrent",
        "chain": [],
        "first_ahead": {
            "replica_id": first_ahead,
            "first_value": a.clock[first_ahead],
            "second_value": b.clock[first_ahead],
        },
        "second_ahead": {
            "replica_id": second_ahead,
            "first_value": a.clock[second_ahead],
            "second_value": b.clock[second_ahead],
        },
    }


# ---------------------------------------------------------------------------
# Conflict resolution event
# ---------------------------------------------------------------------------


def build_resolution_event(
    replica_ids: list[str],
    events: list[Event],
    seen: dict[tuple[str, int], Event],
    key: str,
    writer: str,
    versions: list[ConflictVersion],
    put: Optional[str],
    delete: Optional[str],
) -> tuple[Event, dict]:
    """Build the replayable event that records a confirmed conflict resolution.

    Structural checks (unknown writer, duplicate versions, full batch) raise
    BatchError (422).  When the declared conflict no longer matches the batch
    -- the key is not conflicting, or the version list is stale -- raises
    ConflictStateError (409) and generates no event.

    Returns the new event and the key's verdict over the appended batch.
    """

    replica_set = set(replica_ids)

    # Structural 422s first.
    if writer not in replica_set:
        raise BatchError(
            [
                _err(
                    "unknown_replica_id",
                    f"writing replica_id {writer!r} is not listed in replica_ids",
                )
            ]
        )

    dupes: set[tuple[str, int]] = set()
    seen_versions: set[tuple[str, int]] = set()
    for v in versions:
        ident = (v.replica_id, v.seq)
        if ident in seen_versions:
            dupes.add(ident)
        seen_versions.add(ident)
    if dupes:
        raise BatchError(
            [
                _err(
                    "duplicate_conflict_version",
                    "conflict version list must contain no duplicates; "
                    f"repeated entries: {sorted(dupes)}",
                )
            ]
        )

    if len(events) >= 120:
        raise BatchError(
            [
                _err(
                    "batch_full",
                    "the original batch already contains 120 events; no room "
                    "to append the resolution event (batch limit is 120)",
                )
            ]
        )

    for v in versions:
        if v.replica_id not in replica_set:
            raise BatchError(
                [
                    _err(
                        "unknown_replica_id",
                        f"version replica_id {v.replica_id!r} is not listed in "
                        "replica_ids",
                    )
                ]
            )

    # 409s: the declared conflict must still be live and exactly observed.
    group = [ev for ev in events if ev.key == key]
    if not group:
        raise ConflictStateError(
            [
                {
                    "code": "key_not_conflicted",
                    "message": f"key {key!r} has no events in the batch",
                }
            ]
        )

    maximals = _maximal_events(group, replica_ids)
    if len(maximals) < 2:
        raise ConflictStateError(
            [
                {
                    "code": "key_not_conflicted",
                    "message": f"key {key!r} is no longer in conflict",
                }
            ]
        )

    current = {(m.replica_id, m.seq) for m in maximals}
    if seen_versions != current:
        raise ConflictStateError(
            [
                {
                    "code": "conflict_versions_mismatch",
                    "message": (
                        "the supplied version list is stale or incomplete: it "
                        f"must equal exactly all current maximal events of "
                        f"{key!r}; supplied {sorted(seen_versions)}, current "
                        f"maximals {sorted(current)}"
                    ),
                }
            ]
        )

    # The new seq continues the writer's tail (1 when it has no events yet).
    writer_max = max(
        (ev.seq for ev in events if ev.replica_id == writer), default=0
    )
    new_seq = writer_max + 1

    # Clock = componentwise maximum of every conflicting version's clock and
    # the writer's prior history, then advance the writer's own component to
    # the new seq.
    clock = {rid: 0 for rid in replica_ids}
    for m in maximals:
        for rid in replica_ids:
            if m.clock[rid] > clock[rid]:
                clock[rid] = m.clock[rid]
    if writer_max > 0:
        prior = seen[(writer, writer_max)]
        for rid in replica_ids:
            if prior.clock[rid] > clock[rid]:
                clock[rid] = prior.clock[rid]
    clock[writer] = new_seq

    new_event = Event(
        replica_id=writer,
        seq=new_seq,
        key=key,
        put=put,
        delete=delete,
        clock=clock,
    )

    appended = events + [new_event]

    # The construction guarantees this, but re-run the original validation on
    # the extended batch so the returned event is replayable by construction.
    validate_batch(replica_ids, appended)

    verdict = _key_verdict(
        key, [ev for ev in appended if ev.key == key], replica_ids
    )
    return new_event, verdict


def build_key_migration_events(
    replica_ids: list[str],
    events: list[Event],
    seen: dict[tuple[str, int], Event],
    writer: str,
    source_key: str,
    target_key: str,
    source_versions: list[ConflictVersion],
    target_versions: list[ConflictVersion],
) -> tuple[list[Event], list[dict]]:
    """Build the two replayable events that move a value from one key to another.

    Structural checks (unknown writer/version replica, duplicate versions,
    fewer than two free event slots) raise BatchError (422).  When the declared
    state no longer matches the history -- the source key is not a unique put,
    the target key is not empty or uniquely deleted, or either version list is
    stale/incomplete -- raises ConflictStateError (409) and generates no event.

    Returns the two new events in write order (the source-key delete at the
    writer's next seq, then the target-key put at the seq after that) and the
    two keys' verdicts over the appended batch, sorted by key.
    """

    replica_set = set(replica_ids)

    # ---- Structural 422s -------------------------------------------------
    if source_key == target_key:
        raise BatchError(
            [
                _err(
                    "source_target_key_equal",
                    f"source_key and target_key must differ; both are "
                    f"{source_key!r}",
                )
            ]
        )

    if writer not in replica_set:
        raise BatchError(
            [
                _err(
                    "unknown_replica_id",
                    f"writing replica_id {writer!r} is not listed in replica_ids",
                )
            ]
        )

    def check_versions(versions: list[ConflictVersion], field: str) -> set[tuple[str, int]]:
        dupes: set[tuple[str, int]] = set()
        idents: set[tuple[str, int]] = set()
        for v in versions:
            ident = (v.replica_id, v.seq)
            if ident in idents:
                dupes.add(ident)
            idents.add(ident)
            if v.replica_id not in replica_set:
                raise BatchError(
                    [
                        _err(
                            "unknown_replica_id",
                            f"{field} version replica_id {v.replica_id!r} is "
                            "not listed in replica_ids",
                        )
                    ]
                )
        if dupes:
            raise BatchError(
                [
                    _err(
                        "duplicate_conflict_version",
                        f"{field} must contain no duplicate versions; repeated "
                        f"entries: {sorted(dupes)}",
                    )
                ]
            )
        return idents

    source_idents = check_versions(source_versions, "source_versions")
    target_idents = check_versions(target_versions, "target_versions")

    # Two new events continue the same writer's tail consecutively.
    writer_max = max(
        (ev.seq for ev in events if ev.replica_id == writer), default=0
    )
    if len(events) + 2 > 120:
        raise BatchError(
            [
                _err(
                    "batch_full",
                    f"the history already contains {len(events)} events; a key "
                    "migration appends two events and the 120-event limit leaves "
                    "fewer than two free slots",
                )
            ]
        )

    # ---- State/version 409s, checked against both keys together ----------
    state_errors: list[dict] = []

    source_group = [ev for ev in events if ev.key == source_key]
    source_maximals = (
        _maximal_events(source_group, replica_ids) if source_group else []
    )
    source_ok = (
        len(source_maximals) == 1 and source_maximals[0].put is not None
    )
    if not source_ok:
        state_errors.append(
            {
                "code": "source_key_not_movable",
                "message": (
                    f"source key {source_key!r} must currently hold a unique "
                    "put (a single maximal put event); it is missing, deleted, "
                    "or in conflict"
                ),
            }
        )

    target_group = [ev for ev in events if ev.key == target_key]
    target_maximals = (
        _maximal_events(target_group, replica_ids) if target_group else []
    )
    target_ok = not target_maximals or (
        len(target_maximals) == 1 and target_maximals[0].put is None
    )
    if not target_ok:
        state_errors.append(
            {
                "code": "target_key_not_idle",
                "message": (
                    f"target key {target_key!r} must have no events or a unique "
                    "delete as its current verdict; it currently holds a put or "
                    "a conflict"
                ),
            }
        )

    if not source_ok or not target_ok:
        raise ConflictStateError(state_errors)

    source_current = {(m.replica_id, m.seq) for m in source_maximals}
    target_current = {(m.replica_id, m.seq) for m in target_maximals}
    if source_idents != source_current or target_idents != target_current:
        raise ConflictStateError(
            [
                {
                    "code": "migration_versions_mismatch",
                    "message": (
                        "the supplied version lists are stale or incomplete: "
                        "each must equal exactly all current maximal events of "
                        f"its key; supplied source {sorted(source_idents)} vs "
                        f"current {sorted(source_current)}, supplied target "
                        f"{sorted(target_idents)} vs current "
                        f"{sorted(target_current)}"
                    ),
                }
            ]
        )

    source_event = source_maximals[0]
    delete_clock = {rid: 0 for rid in replica_ids}
    put_clock = {rid: 0 for rid in replica_ids}

    def merge_clock(target: dict[str, int], clock: dict[str, int]) -> None:
        for rid in replica_ids:
            if clock[rid] > target[rid]:
                target[rid] = clock[rid]

    # The delete covers every current version of both keys (the target's
    # versions when it holds a unique delete, nothing when it is eventless).
    for m in source_maximals + target_maximals:
        merge_clock(delete_clock, m.clock)
    # ...plus the writer's own prior history.
    if writer_max > 0:
        merge_clock(delete_clock, seen[(writer, writer_max)].clock)

    delete_seq = writer_max + 1
    delete_clock[writer] = delete_seq
    delete_event = Event(
        replica_id=writer,
        seq=delete_seq,
        key=source_key,
        delete="delete",
        clock=delete_clock,
    )

    # The put follows the delete on the same writer and covers it.
    merge_clock(put_clock, delete_clock)
    put_seq = delete_seq + 1
    put_clock[writer] = put_seq
    put_event = Event(
        replica_id=writer,
        seq=put_seq,
        key=target_key,
        put=source_event.put,
        clock=put_clock,
    )

    new_events = [delete_event, put_event]
    appended = events + new_events

    # The construction guarantees this, but re-run the original validation on
    # the extended batch so both events are replayable by construction.
    validate_batch(replica_ids, appended)

    verdicts = {
        v["key"]: v
        for v in resolve(appended, replica_ids)
        if v["key"] in (source_key, target_key)
    }
    verdicts_sorted = [verdicts[k] for k in sorted(verdicts)]
    return new_events, verdicts_sorted


# ---------------------------------------------------------------------------
# Incremental catch-up plan
# ---------------------------------------------------------------------------


def _max_seq_by_replica(
    replica_ids: list[str], events: list[Event]
) -> dict[str, int]:
    max_seq: dict[str, int] = {rid: 0 for rid in replica_ids}
    for ev in events:
        if ev.seq > max_seq[ev.replica_id]:
            max_seq[ev.replica_id] = ev.seq
    return max_seq


def validate_known_clock(
    replica_ids: list[str],
    known_clock: dict[str, int],
    max_seq: dict[str, int],
) -> None:
    """Validate known_clock against replicas and the batch.

    It must cover exactly all replicas with non-negative integer components,
    and no component may exceed that source's highest seq in this batch.
    Raises BatchError (-> 422) otherwise.
    """

    replica_set = set(replica_ids)
    clock_keys = set(known_clock)

    errors: list[dict] = []
    missing = sorted(replica_set - clock_keys)
    if missing:
        errors.append(
            _err(
                "known_clock_coverage",
                f"known_clock must cover exactly all replica_ids; missing "
                f"components for {missing}",
            )
        )
    extra = sorted(clock_keys - replica_set)
    if extra:
        errors.append(
            _err(
                "known_clock_coverage",
                f"known_clock must cover exactly all replica_ids; unexpected "
                f"components {extra}",
            )
        )
    if errors:
        raise BatchError(errors)

    over: list[str] = []
    for rid in replica_ids:
        if known_clock[rid] > max_seq[rid]:
            over.append(
                f"{rid!r}: known_clock {known_clock[rid]} > highest batch seq "
                f"{max_seq[rid]}"
            )
    if over:
        raise BatchError(
            [
                _err(
                    "known_clock_ahead_of_batch",
                    "known_clock component exceeds that source's highest seq "
                    "in the batch; "
                    + "; ".join(over),
                )
            ]
        )


def validate_received_events(
    replica_ids: list[str],
    events: list[Event],
    known_clock: dict[str, int],
) -> None:
    """Reject batches where an already-received event claims newer history.

    An event whose own seq is at or below its source's known_clock entry has
    already been received by the target; every component of its clock must
    then be at or below the corresponding known_clock entry.
    """

    for i, ev in enumerate(events):
        if ev.seq > known_clock[ev.replica_id]:
            continue
        beyond = {
            rid: ev.clock[rid]
            for rid in replica_ids
            if ev.clock[rid] > known_clock[rid]
        }
        if beyond:
            raise BatchError(
                [
                    _err(
                        "clock_exceeds_known_clock",
                        f"event already received (replica {ev.replica_id!r}, "
                        f"seq {ev.seq} <= known_clock {known_clock[ev.replica_id]}) "
                        f"but its clock {ev.clock} exceeds known_clock; "
                        f"components beyond target knowledge: {beyond}",
                        i,
                    )
                ]
            )


def plan_catchup(
    replica_ids: list[str],
    events: list[Event],
    known_clock: dict[str, int],
    max_seq: dict[str, int],
) -> dict:
    """Schedule missing events into replay rounds.

    A missing event is eligible once its previous event on the same replica
    and, for every other replica, the event its clock component points at
    are all either already at the target (covered by known_clock) or were
    scheduled in an earlier round.
    """

    remaining = {
        (ev.replica_id, ev.seq): ev
        for ev in events
        if ev.seq > known_clock[ev.replica_id]
    }

    rounds: list[list[dict]] = []
    received = dict(known_clock)

    while remaining:
        ready: list[Event] = []
        for (rid, n), ev in remaining.items():
            # Previous event on the same replica must be present.
            if n > 1 and n - 1 > received[rid]:
                continue

            # Events on other replicas pointed at by this clock component.
            blocked = any(
                other != rid
                and ev.clock[other] > received[other]
                for other in replica_ids
            )
            if blocked:
                continue

            ready.append(ev)

        if not ready:
            # Defensive: unreachable for a batch that passed causal
            # validation, reported rather than looping forever.
            raise BatchError(
                [
                    _err(
                        "catchup_unschedulable",
                        "missing events form a dependency cycle and cannot be "
                        "scheduled into rounds",
                    )
                ]
            )

        ready.sort(key=lambda e: (e.replica_id, e.seq))
        round_events: list[dict] = []
        for ev in ready:
            round_events.append(
                {
                    "replica_id": ev.replica_id,
                    "seq": ev.seq,
                    "key": ev.key,
                    "op": "put" if ev.put is not None else "delete",
                    "value": ev.put,
                    "clock": {rid: ev.clock[rid] for rid in replica_ids},
                }
            )
            # The local component is the highest seq observed from this source.
            received[ev.replica_id] = ev.seq
            del remaining[(ev.replica_id, ev.seq)]
        rounds.append(round_events)

    return {"rounds": rounds, "progress": {rid: max_seq[rid] for rid in replica_ids}}


# ---------------------------------------------------------------------------
# Delivery receipts: acknowledged causal prefixes + stable watermark
# ---------------------------------------------------------------------------


def validate_receipt_clock(
    replica_ids: list[str],
    events: list[Event],
    known_clock: dict[str, int],
) -> None:
    """Validate a reported acknowledged prefix against committed state.

    Reuses the stateless catch-up rules: the clock must cover exactly the
    current replica_ids, no component may exceed that source's highest
    committed seq, and every already-received event (own seq at or below the
    reported component) must have a clock componentwise within the prefix.
    Staged (not yet committed) events are outside the receivable range and
    play no part here.  Any violation raises BatchError (-> 422).
    """

    max_seq = _max_seq_by_replica(replica_ids, events)
    validate_known_clock(replica_ids, known_clock, max_seq)
    validate_received_events(replica_ids, events, known_clock)


def check_receipt_no_regression(
    receiving_replica: str,
    replica_ids: list[str],
    stored: dict[str, int] | None,
    incoming: dict[str, int],
) -> None:
    """A re-report must not move any component backwards (-> 409).

    Every component of the incoming clock must be at least the stored one;
    equality on all components is an idempotent retransmission and succeeds.
    ``stored`` of None means this replica has never reported.
    """

    if stored is None:
        return
    regressed = {
        rid: {"stored": stored[rid], "incoming": incoming[rid]}
        for rid in replica_ids
        if incoming[rid] < stored[rid]
    }
    if regressed:
        err = ReceiptRegressionError(receiving_replica, stored, incoming)
        err.regressed = regressed  # type: ignore[attr-defined]
        raise err


def _receipt_watermark(
    replica_ids: list[str],
    receipts: list[tuple[str, dict[str, int]]],
) -> tuple[list[dict], dict[str, int]]:
    """Receipts sorted by replica_id and the componentwise-min stable clock.

    A replica that has never reported contributes an all-zero clock to the
    watermark but is not listed among the receipts.  ``receipts`` is an
    arbitrary order of ``(replica_id, known_clock)`` pairs; each stored clock
    is projected onto the current replica_ids (zero for any key it lacks,
    which can only be the newest admitted replica between an old write and
    its read).
    """

    by_replica = dict(receipts)
    items = [
        {"replica_id": rid, "known_clock": {k: clock[k] for k in replica_ids}}
        for rid, clock in sorted(by_replica.items())
    ]
    stable = {rid: 0 for rid in replica_ids}
    if by_replica:
        for rid in replica_ids:
            stable[rid] = min(
                by_replica.get(member, {}).get(rid, 0)
                for member in replica_ids
            )
    return items, stable


# ---------------------------------------------------------------------------
# Routes / error handling
# ---------------------------------------------------------------------------


@app.exception_handler(BatchError)
async def batch_error_handler(request: Request, exc: BatchError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors},
    )


@app.exception_handler(ConflictStateError)
async def conflict_state_error_handler(
    request: Request, exc: ConflictStateError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": exc.errors},
    )


@app.exception_handler(HistoryNotFound)
async def history_not_found_handler(
    request: Request, exc: HistoryNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "history_not_found",
                    "message": f"no history exists with id {exc.history_id!r}",
                }
            ]
        },
    )


@app.exception_handler(EventNotFound)
async def event_not_found_handler(
    request: Request, exc: EventNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "event_not_found",
                    "message": (
                        f"this history has no event with replica_id "
                        f"{exc.replica_id!r} and seq {exc.seq}"
                    ),
                }
            ]
        },
    )


@app.exception_handler(SelectedKeyNotFound)
async def selected_key_not_found_handler(
    request: Request, exc: SelectedKeyNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "key_not_found",
                    "message": f"this history has no event with key {key!r}",
                    "key": key,
                }
                for key in exc.keys
            ]
        },
    )


@app.exception_handler(SessionNotFound)
async def session_not_found_handler(
    request: Request, exc: SessionNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "session_not_found",
                    "message": (
                        f"this history has no import session with id "
                        f"{exc.session_id!r}"
                    ),
                }
            ]
        },
    )


@app.exception_handler(SessionClosedError)
async def session_closed_handler(
    request: Request, exc: SessionClosedError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": [
                {
                    "code": "session_closed",
                    "message": (
                        f"import session {exc.session_id!r} is already "
                        "closed; it accepts no further appends or commits"
                    ),
                }
            ]
        },
    )


@app.exception_handler(ReplicaAlreadyExistsError)
async def replica_already_exists_handler(
    request: Request, exc: ReplicaAlreadyExistsError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": [
                {
                    "code": "replica_already_exists",
                    "message": (
                        f"replica {exc.replica_id!r} is already a member of "
                        "this history's replica_ids; a replica can only be "
                        "admitted once"
                    ),
                    "replica_id": exc.replica_id,
                }
            ]
        },
    )


@app.exception_handler(ReceiptReplicaNotFound)
async def receipt_replica_not_found_handler(
    request: Request, exc: ReceiptReplicaNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "receipt_replica_not_found",
                    "message": (
                        f"receiving replica {exc.replica_id!r} is not a member "
                        "of this history's replica_ids"
                    ),
                    "replica_id": exc.replica_id,
                }
            ]
        },
    )


@app.exception_handler(ReceiptRegressionError)
async def receipt_regression_handler(
    request: Request, exc: ReceiptRegressionError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": [
                {
                    "code": "receipt_regressed",
                    "message": (
                        f"receipt reported by replica {exc.replica_id!r} moves "
                        "at least one component backwards; each component may "
                        "stay equal or grow, never shrink: "
                        f"{exc.regressed!r}"
                    ),
                    "replica_id": exc.replica_id,
                    "regressed": exc.regressed,
                }
            ]
        },
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Keep FastAPI's 422 shape, but drop the non-JSON-serializable 'ctx'
    # payload (it holds the original ValueError object).
    safe_errors: list[dict] = []
    for e in exc.errors():
        safe = {k: v for k, v in e.items() if k != "ctx"}
        safe_errors.append(safe)
    return JSONResponse(status_code=422, content={"detail": safe_errors})


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/resolve", tags=["resolve"], response_model=None)
async def resolve_batch(body: ResolveRequest) -> dict:
    """Validate the batch and return the effective version of each key."""

    validate_batch(body.replica_ids, body.events)
    return {"results": resolve(body.events, body.replica_ids)}


@app.post("/api/resolve-conflict", tags=["resolve"], response_model=None)
async def resolve_conflict(body: ResolveConflictRequest) -> dict:
    """Record a confirmed conflict resolution as a replayable event.

    The original batch is validated with the same rules as /api/resolve; the
    supplied conflict versions must then be exactly the key's current maximal
    events (>= 2, no duplicates).  The new event continues the writing
    replica's seq tail with a clock covering the conflicting versions and the
    writer's prior history.
    """

    seen = validate_batch(body.replica_ids, body.events)
    new_event, verdict = build_resolution_event(
        replica_ids=body.replica_ids,
        events=body.events,
        seen=seen,
        key=body.key,
        writer=body.replica_id,
        versions=body.versions,
        put=body.put,
        delete=body.delete,
    )
    return {"event": _event_wire(new_event), "result": verdict}


@app.post("/api/catchup", tags=["catchup"], response_model=None)
async def catchup_batch(body: CatchupRequest) -> dict:
    """Build a replayable incremental transfer plan for a target replica."""

    validate_batch(body.replica_ids, body.events)
    max_seq = _max_seq_by_replica(body.replica_ids, body.events)
    validate_known_clock(body.replica_ids, body.known_clock, max_seq)
    validate_received_events(body.replica_ids, body.events, body.known_clock)
    return plan_catchup(
        body.replica_ids, body.events, body.known_clock, max_seq
    )


# ---------------------------------------------------------------------------
# Persistent histories (/api/histories)
#
# Every mutation runs inside one BEGIN IMMEDIATE transaction (see store.py),
# which serialises all read-modify-write sequences: merges and conflict
# commits can neither lose events nor allocate the same seq twice, and a
# commit is always evaluated against -- and written back to -- the current
# committed state.  Any validation/conflict failure rolls the transaction
# back, so each write is all-or-nothing.
# ---------------------------------------------------------------------------


@app.post("/api/histories", tags=["histories"], response_model=None, status_code=201)
def create_history(body: ResolveRequest) -> dict:
    """Validate a batch and persist it as a new history, returning its ID."""

    validate_batch(body.replica_ids, body.events)
    history_id = uuid.uuid4().hex
    with store.write_transaction() as conn:
        store.insert_history(
            conn,
            history_id,
            body.replica_ids,
            _canonical_events(body.events, body.replica_ids),
        )
    return {"id": history_id}


@app.get("/api/histories/{history_id}", tags=["histories"], response_model=None)
def read_history(history_id: str) -> dict:
    """Return the stored batch (original wire format) and its verdicts.

    Events are sorted by replica_id then seq; both the batch and ``results``
    come from one committed snapshot.  Unknown IDs get 404.
    """

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    hid, replica_ids, events = _load_row(row)
    return _history_state(hid, replica_ids, events)


@app.get(
    "/api/histories/{history_id}/withdraw-preview",
    tags=["histories"],
    response_model=None,
)
def preview_history_event_withdrawal(
    history_id: str,
    replica_id: Annotated[
        str,
        Query(
            min_length=1,
            description="Replica ID of the event whose withdrawal is previewed.",
        ),
    ],
    seq: Annotated[
        int,
        Query(
            ge=1,
            description="Per-replica seq of the event whose withdrawal is previewed.",
        ),
    ],
) -> dict:
    """Preview withdrawing one offline event without touching the history.

    Read-only: the stored history is fetched as one committed snapshot and
    never written back.  The previewed withdrawal removes the target event
    plus every event that directly or transitively depends on it (following
    each replica's previous event and other replicas' non-zero clock
    components), across every key; unrelated concurrent events are kept with
    their original seqs and clocks.  The response lists the withdrawn
    identities, the remaining events in original wire order, and the
    before/after verdict of every changed key (``after`` is null when the key
    has no remaining event).  Unknown history/event -> 404; invalid params
    (missing/non-integer/non-positive) -> 422.
    """

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    hid, replica_ids, events = _load_row(row)
    preview = preview_withdrawal(replica_ids, events, replica_id, seq)
    return {"id": hid, **preview}


@app.post(
    "/api/histories/{history_id}/evidence-batch",
    tags=["histories"],
    response_model=None,
)
def extract_history_evidence_batch(
    history_id: str, body: EvidenceBatchRequest
) -> dict:
    """Extract a self-contained causal evidence batch for selected keys.

    Read-only: the stored history is fetched as one committed snapshot and
    never written back.  The response is a plain original-format batch --
    the history's ``replica_ids`` plus every selected-key event (including
    overwritten puts and deletes) closed under causal dependencies: each
    event's previous event on its own replica and the events other replicas'
    non-zero clock components point at, across keys.  Auxiliary keys are
    carried only as dependency evidence.  Events keep their original seqs
    and clocks, sorted by replica_id then seq; the batch passes the original
    validation and can be POSTed unchanged to /api/resolve, /api/catchup and
    /api/resolve-conflict, where the selected keys resolve exactly as in the
    full history (auxiliary keys may resolve differently).  An unknown
    history or a selected key without any event is 404; an illegal keys list
    (missing, empty, non-string entries or duplicates) is 422.
    """

    # Whole-request semantic checks (duplicate keys) are 422 before the
    # history lookup, mirroring validate_batch's duplicate_replica_id rule.
    keys = body.keys
    seen_keys: set[str] = set()
    duplicate_keys: set[str] = set()
    for key in keys:
        if key in seen_keys:
            duplicate_keys.add(key)
        seen_keys.add(key)
    if duplicate_keys:
        raise BatchError(
            [
                {
                    "code": "duplicate_key",
                    "message": f"key {key!r} appears more than once in keys",
                }
                for key in sorted(duplicate_keys)
            ]
        )

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    _, replica_ids, events = _load_row(row)
    batch = build_evidence_batch(replica_ids, events, keys)
    return {
        "replica_ids": list(replica_ids),
        "events": _canonical_events(batch, replica_ids),
    }


@app.post(
    "/api/histories/{history_id}/event-relation",
    tags=["histories"],
    response_model=None,
)
def explain_history_event_relation(
    history_id: str, body: EventRelationRequest
) -> dict:
    """Explain the causal relation between two events of one history.

    Read-only: the stored history is fetched as one committed snapshot and
    never written back; events still staged in an import session are not
    part of it.  The request body carries the two event identifiers
    ``first``/``second`` (each ``{replica_id, seq}``); the response
    ``relation`` is anchored at ``first`` -- ``before``/``after`` when
    ``first`` causally precedes/follows ``second``, ``concurrent`` when the
    two are incomparable, ``same`` when the identities coincide.  A
    before/after answer includes ``chain``: the identifiers from the earlier
    to the later event where every event directly depends on the previous
    one (fewest edges, then the lexicographically smallest identity
    sequence); ``same`` returns the single-node chain.  A concurrent answer
    has an empty chain plus ``first_ahead``/``second_ahead``: one clock
    component per side (smallest replica ID when several qualify) on which
    that side's own clock exceeds the other's, with both events' values.
    Unknown history or event -> 404; illegal identifiers -> 422.
    """

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    hid, replica_ids, events = _load_row(row)
    result = explain_event_relation(
        replica_ids,
        events,
        (body.first.replica_id, body.first.seq),
        (body.second.replica_id, body.second.seq),
    )
    return {"id": hid, **result}


@app.post(
    "/api/histories/{history_id}/events",
    tags=["histories"],
    response_model=None,
)
def merge_history_events(history_id: str, body: MergeEventsRequest) -> dict:
    """Merge offline events into a history and return the merged state.

    Merge key is (replica_id, seq): identical events are ignored and the same
    identity with different content is 409.  The merged history must still
    satisfy the original validation and the 120-event limit, otherwise the
    whole merge is rejected with 422 and nothing is written.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, events = _load_row(row)
        merged = _merge_events(replica_ids, events, body.events)
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        return _history_state(hid, replica_ids, merged)


@app.post(
    "/api/histories/{history_id}/resolve-conflict",
    tags=["histories"],
    response_model=None,
)
def commit_history_conflict(history_id: str, body: ConflictDecision) -> dict:
    """Commit a confirmed conflict resolution into a stored history.

    Only the original resolution parameters are supplied (key, writing
    replica_id, versions, put/delete); the batch itself is read from the
    history under a serialised write lock.  ``versions`` must equal the
    key's current maximal events -- a stale list gets 409 and no event is
    generated.  The new event continues the writer's seq tail and is
    persisted together with the whole history atomically.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, events = _load_row(row)
        new_event, verdict = build_resolution_event(
            replica_ids=replica_ids,
            events=events,
            seen={(ev.replica_id, ev.seq): ev for ev in events},
            key=body.key,
            writer=body.replica_id,
            versions=body.versions,
            put=body.put,
            delete=body.delete,
        )
        merged = events + [new_event]
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        return {"id": hid, "event": _canonical_wire(new_event, replica_ids),
                "result": verdict}


@app.post(
    "/api/histories/{history_id}/migrate-key",
    tags=["histories"],
    response_model=None,
)
def migrate_history_key(history_id: str, body: KeyMigrationRequest) -> dict:
    """Move one key's current put value onto an idle key, atomically.

    Two events are appended to the writing replica in one serialised
    transaction: a delete of ``source_key`` at the next seq, then a put of the
    source's value on ``target_key`` at the seq after that.  Both keys'
    declared versions are re-checked against the current committed state
    inside the same lock -- source must be a unique put, target must have no
    events or a unique delete, and each version list must equal exactly the
    key's current maximal events (empty list for an eventless target).  A
    stale state/list is 409 and nothing is written; fewer than two free event
    slots is 422.  Both generated events pass the original batch validation
    and are replayable on their own.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, events = _load_row(row)
        new_events, verdicts = build_key_migration_events(
            replica_ids=replica_ids,
            events=events,
            seen={(ev.replica_id, ev.seq): ev for ev in events},
            writer=body.replica_id,
            source_key=body.source_key,
            target_key=body.target_key,
            source_versions=body.source_versions,
            target_versions=body.target_versions,
        )
        merged = events + new_events
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        return {
            "id": hid,
            "events": [
                _canonical_wire(ev, replica_ids) for ev in new_events
            ],
            "results": verdicts,
        }


@app.post(
    "/api/histories/{history_id}/replicas",
    tags=["histories"],
    response_model=None,
)
def admit_history_replica(history_id: str, body: AdmitReplicaRequest) -> dict:
    """Admit one new replica into a stored history, extending every clock.

    The new replica is appended to ``replica_ids`` and a fresh zero clock
    component for it is added to every committed event and to the staged
    events of *every* import session of the history, open or closed --
    sessions need not be closed before admission.  Existing seq, key,
    operation, value, clock components, session IDs and statuses are
    untouched, so existing keys keep the same verdict.  Staging allows
    missing dependencies, so no full-batch validation runs: admission is
    never blocked by an incomplete session.

    The expanded history row, every rewritten session row and every delivery
    receipt commit in one serialised transaction, serialised with legacy
    merges, conflict commits, session commits and receipt reports; a reader
    therefore sees either the pre- or post-admission replica set, never a
    mixture.  Every previously stored receipt gains a zero component for the
    new replica, and the new replica itself starts as a never-reported
    receiver (an all-zero contribution to the stable watermark) -- so the
    watermark may drop on admission even though no existing receipt regressed.
    On any failure nothing is written.  Afterwards the new replica may write
    from seq 1 via the ordinary endpoints, whose full-clock validation is
    unchanged (requests still have to carry every component explicitly).
    Replicas can only be added, never removed or renamed.

    Unknown history -> 404; the replica is already a member -> 409; an empty
    replica_id (schema) or membership that would exceed eight replicas ->
    422.  The response is the expanded history's ordinary read structure.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, committed = _load_row(row)
        srows = store.select_sessions_for_update(conn, history_id)
        sessions = [
            (srow["id"], _load_session_row(srow, replica_ids)[2])
            for srow in srows
        ]
        receipt_rows = store.select_receipts_for_update(conn, history_id)
        receipts: list[tuple[str, dict[str, int]]] = [
            (rrow["replica_id"], json.loads(rrow["known_clock"]))
            for rrow in receipt_rows
        ]
        expanded, patched_committed, patched_sessions = _admit_replica(
            replica_ids, committed, sessions, body.replica_id
        )
        # Old receipts cover the new replica's component with 0, in lockstep
        # with the expanded history and sessions.  No row is inserted for the
        # new receiver: an absent receipt means an all-zero acknowledgement.
        patched_receipts = [(hid, rid, dict(clock, **{body.replica_id: 0}))
                            for rid, clock in receipts]
        store.update_replica_ids_and_events(
            conn,
            hid,
            expanded,
            _canonical_events(patched_committed, expanded),
        )
        for srow, patched in zip(srows, patched_sessions):
            store.update_session_events(
                conn, srow["id"], _canonical_events(patched, expanded)
            )
        store.update_receipt_clocks(conn, patched_receipts)
        return _history_state(hid, expanded, patched_committed)


# ---------------------------------------------------------------------------
# Import sessions (/api/histories/{id}/import-sessions)
#
# Sessions stage offline events that arrive in several batches; the staged
# events only reach the persistent history when the session is committed.
# Staging applies the per-event checks (fields, replica membership, full
# clock coverage, own component == seq) to genuinely new identities only and
# tolerates not-yet-arrived predecessors or cross-replica dependencies;
# same-identity retransmissions -- against staged events or earlier entries
# of the same batch -- skip those checks and are judged by full-content
# equality (identical ignored; a missing component, an extra component even
# when zero, or a changed component -> 409, never 422).  The 120-event cap
# counts distinct identities after in-batch deduplication.  Commit re-runs
# the original merge (full-content equality, causal validation, 120-event
# cap) against the history's latest committed state.  Every mutation runs
# inside one BEGIN IMMEDIATE transaction together with the history row lock,
# so a commit serialises with the legacy write endpoints and cannot lose
# events, the merge and the session close take effect together, and any
# failure rolls everything back, leaving the open session and its staged
# events untouched for a retry.
# ---------------------------------------------------------------------------


@app.post(
    "/api/histories/{history_id}/import-sessions",
    tags=["import-sessions"],
    response_model=None,
    status_code=201,
)
def create_import_session(history_id: str) -> dict:
    """Open an empty import session on a history and return its ID."""

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        session_id = uuid.uuid4().hex
        store.insert_session(conn, session_id, history_id)
    return {"id": session_id}


@app.get(
    "/api/histories/{history_id}/import-sessions/{session_id}",
    tags=["import-sessions"],
    response_model=None,
)
def read_import_session(history_id: str, session_id: str) -> dict:
    """Return the session status and its staged events.

    Staged events come back in the original wire format, sorted by
    replica_id then seq.  Unknown history or session IDs get 404.

    The history row and the session row come from one shared read snapshot:
    a replica-admission commit rewrites both rows together, so reading them
    on separate connections could pair the new replica set with an old
    staged clock (and silently drop its new component); one snapshot makes
    such a mixed read impossible.
    """

    with store.read_transaction() as conn:
        if conn is None:
            raise HistoryNotFound(history_id)
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        replica_ids: list[str] = json.loads(row["replica_ids"])
        srow = store.select_session_for_update(conn, history_id, session_id)
        if srow is None:
            raise SessionNotFound(session_id)
        sid, status, staged = _load_session_row(srow, replica_ids)
        return _session_state(sid, history_id, status, staged, replica_ids)


@app.post(
    "/api/histories/{history_id}/import-sessions/{session_id}/events",
    tags=["import-sessions"],
    response_model=None,
)
def stage_import_session_events(
    history_id: str, session_id: str, body: StageEventsRequest
) -> dict:
    """Stage a batch of events into an open session and return its state.

    Per-event checks (422 on unknown replica, clock coverage or own
    component) apply only to genuinely new (replica_id, seq) identities;
    missing dependencies are tolerated until commit.  Retransmissions of an
    already-staged or earlier-in-batch identity skip those checks and are
    compared by full content: a missing component, an extra component --
    even one with value 0 -- or a changed own component is different content
    and gets 409 (event_content_mismatch, with the batch event_index), never
    422; identical content is ignored.  The 120-event cap counts already
    staged identities plus the new identities after in-batch deduplication,
    so 121 identical resends of one legal event stage one and a full session
    re-sending its events still succeeds; a distinct identity beyond the cap
    is 422.  A failed append changes nothing; a closed session is 409.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, _ = _load_row(row)
        srow = store.select_session_for_update(conn, history_id, session_id)
        if srow is None:
            raise SessionNotFound(session_id)
        if srow["status"] != "open":
            raise SessionClosedError(session_id)
        _, status, staged = _load_session_row(srow, replica_ids)
        merged = _stage_events(replica_ids, staged, body.events)
        store.update_session_events(
            conn, session_id, _canonical_events(merged, replica_ids)
        )
        return _session_state(session_id, hid, status, merged, replica_ids)


@app.post(
    "/api/histories/{history_id}/import-sessions/{session_id}/commit",
    tags=["import-sessions"],
    response_model=None,
)
def commit_import_session(history_id: str, session_id: str) -> dict:
    """Merge the staged events into the history and close the session.

    The merge runs against the history's latest committed state under the
    same serialised write lock as the legacy write endpoints, with the
    original full-content equality (identical events are ignored, different
    content under a stored identity is 409), causal validation and the
    120-event total limit (violations are 422).  On success the merged
    history and the session close take effect together and the response is
    the history read structure; on failure the open session and its staged
    events are kept, so missing events can be appended and the commit
    retried.  A closed session is 409.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, committed = _load_row(row)
        srow = store.select_session_for_update(conn, history_id, session_id)
        if srow is None:
            raise SessionNotFound(session_id)
        if srow["status"] != "open":
            raise SessionClosedError(session_id)
        _, _, staged = _load_session_row(srow, replica_ids)
        merged = _merge_events(replica_ids, committed, staged)
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        store.close_session(conn, session_id)
        return _history_state(hid, replica_ids, merged)


@app.get(
    "/api/histories/{history_id}/import-sessions/{session_id}/missing-ranges",
    tags=["import-sessions"],
    response_model=None,
)
def read_import_session_missing_ranges(
    history_id: str, session_id: str
) -> dict:
    """List the seq ranges still missing from the source for this session.

    The history's committed events and the session's staged events are read
    from one committed snapshot (the session may be open *or* closed) and
    merged by identity.  An identity present in both with different full
    content is a 409 listing every conflicting replica_id/seq (sorted), with
    no range list.  Otherwise, for each replica the required upper bound is
    the maximum of that replica's own highest merged seq and the highest
    clock component any merged event carries for it; the missing numbers in
    ``1..bound`` are returned as the largest possible contiguous *closed*
    ranges, sorted by replica_id then start_seq -- individual missing seqs
    are never expanded (a ``10**12`` bound stays one range).  A bound of 0
    yields nothing, and an empty list means identities are complete, not
    that a commit would succeed.  Purely read-only: history, staging area
    and session status are never modified.  Unknown history or session ->
    404.
    """

    with store.read_transaction() as conn:
        if conn is None:
            raise HistoryNotFound(history_id)
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        _, replica_ids, committed = _load_row(row)
        srow = store.select_session_for_update(conn, history_id, session_id)
        if srow is None:
            raise SessionNotFound(session_id)
        _, _, staged = _load_session_row(srow, replica_ids)
        ranges = _session_missing_ranges(replica_ids, committed, staged)
    return {"missing_ranges": ranges}


# ---------------------------------------------------------------------------
# Delivery receipts (/api/histories/{id}/receipts)
#
# A receiving replica reports the causal prefix it has acknowledged: a
# known_clock over the history's current replica_ids.  Receipts persist in
# the same SQLite database (one row per history/receiver), survive restarts,
# and take effect together with replica admission in the same serialised
# transaction: admitting a replica appends a zero component to every old
# receipt and the new receiver counts as all-zero, so the replica set and
# the receipt clock shapes can never be observed mixed, and the stable
# watermark is allowed to drop on expansion.
# ---------------------------------------------------------------------------


@app.post(
    "/api/histories/{history_id}/receipts",
    tags=["receipts"],
    response_model=None,
)
def report_history_receipt(history_id: str, body: ReceiptReportRequest) -> dict:
    """Record one receiving replica's acknowledged causal prefix.

    The validation and the save run against one locked committed state: the
    history's current replica_ids and its committed events (staged
    import-session events are not receivable yet and never enter the checks).
    The clock must cover exactly the current replicas with non-negative
    integer components (booleans rejected by schema), no component may exceed
    that source's highest committed seq, and every already-received event
    (own seq at or below the reported component) must keep its clock within
    the prefix; structural/semantic violations are 422.  A re-report must not
    move any component below the stored receipt (409); an identical report is
    an idempotent retransmission and succeeds.  Unknown history or a
    receiving replica outside the set is 404.  The stored receipt is the
    componentwise max, so a concurrent report can never overwrite a larger
    acknowledged prefix.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, committed = _load_row(row)
        if body.replica_id not in set(replica_ids):
            raise ReceiptReplicaNotFound(body.replica_id)
        # Coverage / bounds / received-event checks against this same state.
        validate_receipt_clock(replica_ids, committed, body.known_clock)
        incoming = {rid: body.known_clock[rid] for rid in replica_ids}

        stored_row = None
        for rrow in store.select_receipts_for_update(conn, hid):
            if rrow["replica_id"] == body.replica_id:
                stored_row = rrow
                break
        if stored_row is not None:
            stored_clock = json.loads(stored_row["known_clock"])
            stored = {rid: int(stored_clock.get(rid, 0)) for rid in replica_ids}
        else:
            stored = None
        check_receipt_no_regression(
            body.replica_id, replica_ids, stored, incoming
        )
        store.upsert_receipt(conn, hid, body.replica_id, incoming)

        receipt_rows = store.select_receipts_for_update(conn, hid)
        all_receipts = [
            (rrow["replica_id"], json.loads(rrow["known_clock"]))
            for rrow in receipt_rows
        ]
        items, stable = _receipt_watermark(replica_ids, all_receipts)
    return {
        "id": hid,
        "replica_ids": list(replica_ids),
        "receipt": {
            "replica_id": body.replica_id,
            "known_clock": incoming,
        },
        "receipts": items,
        "stable_clock": stable,
    }


@app.get(
    "/api/histories/{history_id}/receipts",
    tags=["receipts"],
    response_model=None,
)
def read_history_receipts(history_id: str) -> dict:
    """Return every reported receipt and the common stable watermark.

    Both the history row (replica set) and the receipts come from one read
    snapshot, so an expansion's new replica set can never be paired with
    old-shape receipts.  Receipts are sorted by receiving replica_id and
    carry a clock projected onto the current replicas.  ``stable_clock``
    takes, per component, the minimum over *all* current receivers: a replica
    that has never reported -- including a freshly admitted one -- counts as
    all zero (so the watermark may be zero, and may drop after expansion)
    without the replica appearing in ``receipts``.  Unknown history -> 404.
    Read-only.
    """

    with store.read_transaction() as conn:
        if conn is None:
            raise HistoryNotFound(history_id)
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, _ = _load_row(row)
        receipt_rows = store.select_receipts_for_update(conn, hid)
        all_receipts = [
            (rrow["replica_id"], json.loads(rrow["known_clock"]))
            for rrow in receipt_rows
        ]
        items, stable = _receipt_watermark(replica_ids, all_receipts)
    return {
        "id": hid,
        "replica_ids": list(replica_ids),
        "receipts": items,
        "stable_clock": stable,
    }
