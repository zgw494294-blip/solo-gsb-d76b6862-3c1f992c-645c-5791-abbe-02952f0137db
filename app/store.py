"""SQLite persistence for mergeable offline-event histories.

Each history is one table row holding the fixed ``replica_ids`` and the full
event list (canonical JSON).  Import sessions are one row each: the owning
history id, an ``open``/``closed`` status and the staged events (canonical
JSON), so an interrupted staging process can be resumed after a restart.
Per-replica delivery receipts (the causal prefix each receiving replica has
acknowledged) are one row each as well; they are rewritten together with the
history row when a new replica is admitted, so a reader can never observe a
mixed replica set.  Every request opens a short-lived connection:

* writes take a ``BEGIN IMMEDIATE`` transaction, which acquires the database
  reserved lock up front.  Concurrent writers therefore serialise, and a
  read-modify-write sequence (merge / conflict commit / session commit /
  receipt report) can never interleave with another one -- no lost events, no
  double-allocated seqs, no stale ``versions`` overwriting a conflict, and no
  receipt regression overwriting a larger acknowledged prefix;
* readers run on autocommit snapshot reads and never block on writers (WAL).
  A history row stores ``replica_ids`` and ``events`` together, so a single
  SELECT always yields both from the same committed state.

The database location comes from ``HISTORY_DB_PATH`` so tests and containers
can point it at an isolated file.  The schema is created lazily on the first
write, which keeps read-only/stateless usage from materialising a database
file.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from typing import Iterator

DEFAULT_DB_PATH = "history.db"
_BUSY_TIMEOUT_MS = 30_000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS histories (
    id          TEXT PRIMARY KEY,
    replica_ids TEXT NOT NULL,
    events      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS import_sessions (
    id          TEXT PRIMARY KEY,
    history_id  TEXT NOT NULL,
    status      TEXT NOT NULL,
    events      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
    history_id  TEXT NOT NULL,
    replica_id  TEXT NOT NULL,
    known_clock TEXT NOT NULL,
    PRIMARY KEY (history_id, replica_id)
);
"""


def db_path() -> str:
    return os.environ.get("HISTORY_DB_PATH", DEFAULT_DB_PATH)


def _connect(*, for_write: bool) -> sqlite3.Connection:
    path = db_path()
    if for_write:
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)

    conn = sqlite3.connect(path, timeout=_BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    if for_write:
        # WAL is a persistent file property; applying it per connection is
        # cheap and makes reads/writes concurrent.
        if path != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(_SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
    return conn


@contextlib.contextmanager
def write_transaction() -> Iterator[sqlite3.Connection]:
    """Serialised (BEGIN IMMEDIATE) write transaction, all-or-nothing."""

    conn = _connect(for_write=True)
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextlib.contextmanager
def read_transaction() -> Iterator[sqlite3.Connection]:
    """Read-only deferred transaction spanning several SELECTs.

    All reads inside share one snapshot (and, with the journal in WAL mode,
    never block on -- or block -- a writer), so e.g. a history and one of its
    import sessions are always observed from the same committed state.  A
    read-only transaction takes no locks and is simply dropped on close.
    Before any writer has created the schema (or when the database file does
    not exist at all) it yields ``None``: nothing can be stored in a
    non-existent schema.

    Only the two original tables gate readiness: a database created by an
    older release (without the ``receipts`` table) stays readable -- the
    newer receipt queries simply see no rows -- so mounting an old data
    volume keeps every old endpoint working unchanged.
    """

    try:
        conn = _connect(for_write=False)
    except sqlite3.OperationalError:
        yield None
        return

    ready = False
    try:
        try:
            conn.execute("BEGIN")
            # A deferred BEGIN succeeds even before any writer has created the
            # schema; probe for it so missing-table reads report "no row"
            # instead of raising on their first SELECT.
            tables = conn.execute(
                "SELECT count(*) FROM sqlite_master "
                "WHERE type = 'table' AND name IN ('histories', "
                "'import_sessions')"
            ).fetchone()[0]
            ready = tables == 2
        except sqlite3.OperationalError:
            ready = False
        if not ready:
            conn.close()
            yield None
            return
        try:
            yield conn
        finally:
            conn.rollback()
    finally:
        conn.close()


def get_history(history_id: str) -> sqlite3.Row | None:
    """Committed snapshot of one history, or None when it does not exist.

    Uses a fresh autocommit connection so the caller observes the most recent
    committed state (equivalent to a process restart reopening the file).
    """

    try:
        conn = _connect(for_write=False)
    except sqlite3.OperationalError:
        return None
    try:
        try:
            cursor = conn.execute(
                "SELECT id, replica_ids, events FROM histories WHERE id = ?",
                (history_id,),
            )
        except sqlite3.OperationalError:
            # No writer has created the schema yet -> no history can exist.
            return None
        return cursor.fetchone()
    finally:
        conn.close()


def insert_history(
    conn: sqlite3.Connection,
    history_id: str,
    replica_ids: list[str],
    events: list[dict],
) -> None:
    import json

    conn.execute(
        "INSERT INTO histories (id, replica_ids, events) VALUES (?, ?, ?)",
        (
            history_id,
            json.dumps(replica_ids, ensure_ascii=False),
            json.dumps(events, ensure_ascii=False),
        ),
    )


def select_for_update(
    conn: sqlite3.Connection, history_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, replica_ids, events FROM histories WHERE id = ?",
        (history_id,),
    ).fetchone()


def update_events(
    conn: sqlite3.Connection, history_id: str, events: list[dict]
) -> None:
    import json

    conn.execute(
        "UPDATE histories SET events = ? WHERE id = ?",
        (json.dumps(events, ensure_ascii=False), history_id),
    )


def update_replica_ids_and_events(
    conn: sqlite3.Connection,
    history_id: str,
    replica_ids: list[str],
    events: list[dict],
) -> None:
    """Persist an expanded replica set together with the rewritten events.

    Both columns live on the same row and are written by one UPDATE inside
    the caller's serialised transaction, so a reader can never observe the
    new replica set with the old clocks (or vice versa).
    """

    import json

    conn.execute(
        "UPDATE histories SET replica_ids = ?, events = ? WHERE id = ?",
        (
            json.dumps(replica_ids, ensure_ascii=False),
            json.dumps(events, ensure_ascii=False),
            history_id,
        ),
    )


# ---------------------------------------------------------------------------
# Import sessions: staged offline events, merged into the history on commit
# ---------------------------------------------------------------------------


def insert_session(
    conn: sqlite3.Connection, session_id: str, history_id: str
) -> None:
    """Create an empty open session owned by ``history_id``."""

    conn.execute(
        "INSERT INTO import_sessions (id, history_id, status, events) "
        "VALUES (?, ?, 'open', '[]')",
        (session_id, history_id),
    )


def get_session(history_id: str, session_id: str) -> sqlite3.Row | None:
    """Committed snapshot of one session, or None when it does not exist.

    A session is addressed by the owning history id plus its own id, so an
    id belonging to a different history reads as missing.  Fresh autocommit
    connection: the caller observes the most recent committed state.
    """

    try:
        conn = _connect(for_write=False)
    except sqlite3.OperationalError:
        return None
    try:
        try:
            cursor = conn.execute(
                "SELECT id, history_id, status, events FROM import_sessions "
                "WHERE id = ? AND history_id = ?",
                (session_id, history_id),
            )
        except sqlite3.OperationalError:
            # No writer has created the schema yet -> no session can exist.
            return None
        return cursor.fetchone()
    finally:
        conn.close()


def select_session_for_update(
    conn: sqlite3.Connection, history_id: str, session_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, history_id, status, events FROM import_sessions "
        "WHERE id = ? AND history_id = ?",
        (session_id, history_id),
    ).fetchone()


def update_session_events(
    conn: sqlite3.Connection, session_id: str, events: list[dict]
) -> None:
    import json

    conn.execute(
        "UPDATE import_sessions SET events = ? WHERE id = ?",
        (json.dumps(events, ensure_ascii=False), session_id),
    )


def close_session(conn: sqlite3.Connection, session_id: str) -> None:
    """Mark a session closed; runs in the same transaction as the merge."""

    conn.execute(
        "UPDATE import_sessions SET status = 'closed' WHERE id = ?",
        (session_id,),
    )


def select_sessions_for_update(
    conn: sqlite3.Connection, history_id: str
) -> list[sqlite3.Row]:
    """All import sessions (open and closed) owned by one history.

    Admitting a new replica rewrites the staged clocks of every session, so
    all of them are read within the same serialised transaction.
    """

    return conn.execute(
        "SELECT id, history_id, status, events FROM import_sessions "
        "WHERE history_id = ? ORDER BY id",
        (history_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Delivery receipts: per-replica acknowledged causal prefixes
#
# One row per (history, receiving replica).  The acknowledged clock covers
# exactly the history's current replica_ids; admitting a new replica appends
# a zero component to every stored receipt and commits those rewrites in the
# same transaction as the history row, so a reader always pairs one replica
# set with one-shape receipts.
# ---------------------------------------------------------------------------


def _receipts_table_exists(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'receipts'"
    ).fetchone()[0] == 1


def select_receipts_for_update(
    conn: sqlite3.Connection, history_id: str
) -> list[sqlite3.Row]:
    """All delivery receipts of one history, ordered by replica_id.

    Returns an empty list when the table is absent, i.e. when the database
    file was created by an older release: the old data volume keeps working
    and receipts simply start from no reports.
    """

    if not _receipts_table_exists(conn):
        return []
    return conn.execute(
        "SELECT history_id, replica_id, known_clock FROM receipts "
        "WHERE history_id = ? ORDER BY replica_id",
        (history_id,),
    ).fetchall()


def upsert_receipt(
    conn: sqlite3.Connection,
    history_id: str,
    replica_id: str,
    known_clock: dict[str, int],
) -> None:
    """Insert or replace one receiving replica's acknowledged prefix."""

    import json

    conn.execute(
        "INSERT INTO receipts (history_id, replica_id, known_clock) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(history_id, replica_id) DO UPDATE SET "
        "known_clock = excluded.known_clock",
        (history_id, replica_id, json.dumps(known_clock, ensure_ascii=False)),
    )


def update_receipt_clocks(
    conn: sqlite3.Connection,
    receipts: list[tuple[str, str, dict[str, int]]],
) -> None:
    """Rewrite several receipts in the caller's transaction.

    Used by replica admission, which appends the new replica's zero component
    to every previously stored acknowledgement together with the history row.
    """

    import json

    conn.executemany(
        "UPDATE receipts SET known_clock = ? WHERE history_id = ? AND replica_id = ?",
        [
            (json.dumps(clock, ensure_ascii=False), history_id, replica_id)
            for history_id, replica_id, clock in receipts
        ],
    )
