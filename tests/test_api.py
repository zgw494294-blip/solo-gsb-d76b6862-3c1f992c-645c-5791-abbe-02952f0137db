"""Tests for the multi-replica offline write reconciliation API."""

import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.main import (
    Event,
    _direct_dependencies,
    _shortest_dependency_chain,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def _isolated_history_db(tmp_path, monkeypatch):
    """Point the persistent store at a per-test SQLite file."""

    monkeypatch.setenv("HISTORY_DB_PATH", str(tmp_path / "history.db"))
    yield


def ev(replica, seq, key, put=None, delete=None, clock=None):
    e = {"replica_id": replica, "seq": seq, "key": key, "clock": clock or {}}
    if put is not None:
        e["put"] = put
    elif delete is not None:
        e["delete"] = delete
    else:
        e["delete"] = "delete"
    return e


def post(replica_ids, events):
    return client.post(
        "/api/resolve", json={"replica_ids": replica_ids, "events": events}
    )


def post_catchup(replica_ids, events, known_clock):
    return client.post(
        "/api/catchup",
        json={
            "replica_ids": replica_ids,
            "events": events,
            "known_clock": known_clock,
        },
    )


def post_resolve_conflict(replica_ids, events, key, writer, versions, **ops):
    body = {
        "replica_ids": replica_ids,
        "events": events,
        "key": key,
        "replica_id": writer,
        "versions": versions,
    }
    body.update(ops)
    return client.post("/api/resolve-conflict", json=body)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_concurrent_puts_conflict_even_when_values_equal():
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="x", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "conflict"
    assert [e["replica_id"] for e in item["events"]] == ["r1", "r2"]


def test_results_sorted_by_key_and_put_shadows_older_event():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "z", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "a", delete="delete", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "a", put="kept", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    results = r.json()["results"]
    assert [x["key"] for x in results] == ["a", "z"]
    assert results[0]["state"] == "put"
    assert results[0]["value"] == "kept"
    assert results[0]["event"]["replica_id"] == "r1"
    assert results[0]["event"]["seq"] == 2


def test_unique_maximal_delete_is_reported_deleted():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", delete="delete", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "deleted"
    assert item["event"]["op"] == "delete"
    assert item["event"]["value"] is None


def test_events_may_arrive_out_of_order():
    events = [
        ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 1}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
    ]
    r = post(["r1", "r2"], events)
    assert r.status_code == 200, r.text
    k = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert k["state"] == "put"
    assert k["value"] == "c"


def test_empty_put_value_is_allowed():
    r = post(
        ["a", "b"],
        [ev("a", 1, "k", put="", clock={"a": 1, "b": 0})],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "put"
    assert item["value"] == ""


def test_conflict_events_sorted_by_replica_then_seq():
    # Concurrent branches after common predecessor r2/1:
    # r1/2 and r2/2 are incomparable (each advances a different component).
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "other", put="x", clock={"r1": 1, "r2": 0}),
            ev("r2", 2, "k", put="d", clock={"r1": 1, "r2": 2}),
            ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    item = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert item["state"] == "conflict"
    assert [(e["replica_id"], e["seq"]) for e in item["events"]] == [
        ("r1", 2),
        ("r2", 2),
    ]


def test_three_replicas_and_eventless_replica():
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0, "r3": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["event"]["clock"] == {"r1": 1, "r2": 0, "r3": 0}


# ---------------------------------------------------------------------------
# Validation failures -> 422
# ---------------------------------------------------------------------------


def assert_422(r, code=None):
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    if code is not None:
        assert any(e.get("code") == code for e in detail)
    return detail


def test_duplicate_replica_ids_rejected():
    r = post(["r1", "r1"], [ev("r1", 1, "k", put="v", clock={"r1": 0})])
    assert_422(r, "duplicate_replica_id")


def test_unknown_event_replica_rejected():
    r = post(
        ["r1", "r2"],
        [ev("r9", 1, "k", put="v", clock={"r1": 0, "r2": 0})],
    )
    assert_422(r, "unknown_replica_id")


def test_clock_missing_and_extra_components():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r3": 0})],
    )
    assert_422(r, "clock_replica_coverage")


def test_clock_negative_component():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": -1})],
    )
    assert r.status_code == 422


def test_self_component_must_equal_seq():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
    )
    detail = assert_422(r, "clock_self_component")
    assert detail[0]["event_index"] == 0


def test_duplicate_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 1, "k", put="b", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert_422(r, "duplicate_seq")


def test_gap_in_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 3, "k", put="b", clock={"r1": 3, "r2": 0}),
        ],
    )
    assert_422(r, "non_contiguous_seq")


def test_seq_must_start_at_one():
    r = post(
        ["r1", "r2"],
        [ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 0})],
    )
    assert_422(r, "non_contiguous_seq")


def test_non_contiguous_seq_gaps_reported_as_closed_intervals():
    # Small-gap regression: the gap diagnosis summarises the missing seqs as
    # closed intervals, not by listing every missing seq one by one.
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 3, "k", put="b", clock={"r1": 3, "r2": 0}),
        ],
    )
    detail = assert_422(r, "non_contiguous_seq")
    msg = detail[0]["message"]
    assert "[2, 2]" in msg
    # Only the interval overview is allowed for the missing numbers -- no
    # flat enumeration list "(missing [2])".
    assert "(missing [" not in msg


def test_non_contiguous_seq_several_gaps_coalesce_into_intervals():
    # Present 1, 3, 5: two singleton gaps, each its own closed interval; the
    # gap interiors (2 and 4 here) are never enumerated as a seq list.
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 3, "k", put="b", clock={"r1": 3, "r2": 0}),
            ev("r1", 5, "k", put="c", clock={"r1": 5, "r2": 0}),
        ],
    )
    detail = assert_422(r, "non_contiguous_seq")
    msg = detail[0]["message"]
    assert "[2, 2]" in msg and "[4, 4]" in msg
    assert "(missing [" not in msg


def test_non_contiguous_seq_huge_sparse_seq_rejected_without_enumerating():
    # A single sparse 10**12 event must be one cheap non_contiguous_seq 422;
    # the 10**12-2 missing seqs are summarised as one closed interval and
    # never allocated or listed (the message stays tiny).
    huge = 10**12
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", huge, "k", put="b", clock={"r1": huge, "r2": 0}),
        ],
    )
    detail = assert_422(r, "non_contiguous_seq")
    msg = detail[0]["message"]
    assert f"[2, {huge - 1}]" in msg
    # Sorted present seqs are still reported (there are only two actual
    # events), but the gap interior is not enumerated.
    assert "(missing [" not in msg
    assert len(msg) < 400


@pytest.mark.parametrize(
    "endpoint, body",
    [
        (
            "/api/catchup",
            {
                "known_clock": {"r1": 0, "r2": 0},
                "events": [
                    ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
                    ev("r1", 10**12, "k", put="b",
                       clock={"r1": 10**12, "r2": 0}),
                ],
            },
        ),
        (
            "/api/resolve-conflict",
            {
                "key": "k",
                "replica_id": "r1",
                "versions": [
                    {"replica_id": "r1", "seq": 1},
                    {"replica_id": "r1", "seq": 10**12},
                ],
                "put": "x",
                "events": [
                    ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
                    ev("r1", 10**12, "k", put="b",
                       clock={"r1": 10**12, "r2": 0}),
                ],
            },
        ),
    ],
    ids=["catchup", "resolve-conflict"],
)
def test_other_full_batch_endpoints_reject_sparse_huge_seq(endpoint, body):
    # Every endpoint sharing the full validate_batch path must reject the same
    # sparse input cheaply with 422/non_contiguous_seq (not 500/OOM).
    r = client.post(endpoint, json={"replica_ids": ["r1", "r2"], **body})
    detail = assert_422(r, "non_contiguous_seq")
    assert f"[2, {10**12 - 1}]" in detail[0]["message"]


def test_history_create_rejects_sparse_huge_seq():
    r = hist_create(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 10**12, "k", put="b", clock={"r1": 10**12, "r2": 0}),
        ],
    )
    detail = assert_422(r, "non_contiguous_seq")
    assert f"[2, {10**12 - 1}]" in detail[0]["message"]
    # The rejected batch never became a history.
    assert hist_get("deadbeef" * 4).status_code == 404


def test_clock_pointing_to_missing_event():
    # r1/1 claims to know r3/1, but no event was produced by r3.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0, "r3": 1}),
        ],
    )
    detail = assert_422(r, "clock_points_to_missing_event")
    assert detail[0]["event_index"] == 0


def test_other_replica_clock_equal_rejected():
    # Alias covered by test_equal_clock_events_are_causally_invalid:
    # re-verify the offending event index is reported.
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 0


def test_equal_clock_events_are_causally_invalid():
    # Cross-replica: two events with identical clocks cannot reference each
    # other (the referenced clock must be strictly smaller).
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert_422(r, "clock_causality")


def test_predecessor_clock_with_component_greater_rejected():
    # r1/1 knows r2/2 (clock r2=2), but r1/2 only knows r2/1 (r2=1):
    # the predecessor's clock is not <= the current one. Events arrive
    # out of order; the violation must still be pinpointed at r1/2.
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "x", put="x", clock={"r1": 0, "r2": 1}),
            ev("r2", 2, "x", put="x", clock={"r1": 0, "r2": 2}),
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 2}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_other_component_referenced_clock_not_le_rejected():
    # Three replicas: r1/1 references r2/1 (r2 component = 1), but r2/1
    # already knows r3/1 while r1/1 does not -> referenced clock not <=.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r3", 1, "z", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1, "r3": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1, "r3": 0}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_put_and_delete_together_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "put": "v",
                "delete": "delete",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_delete_field_wrong_value_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "delete": "remove",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_boundary_replica_and_event_counts():
    # 8 replicas, single event
    ids = [f"r{i}" for i in range(8)]
    r = post(ids, [ev("r0", 1, "k", put="v", clock={rid: 0 for rid in ids} | {"r0": 1})])
    assert r.status_code == 200, r.text

    # 1 replica -> schema error
    r = post(["only"], [ev("only", 1, "k", put="v", clock={"only": 1})])
    assert r.status_code == 422

    # 121 events (one replica with 121 seqs) -> schema error
    many = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0}) for n in range(1, 122)
    ]
    r = post(["r1", "r2"], many)
    assert r.status_code == 422


def test_empty_events_rejected():
    r = client.post(
        "/api/resolve", json={"replica_ids": ["r1", "r2"], "events": []}
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# /api/catchup
# ---------------------------------------------------------------------------


def test_catchup_schedules_by_dependency_rounds():
    # r1/1 -> r2/1 (knows r1/1) -> r1/2 (knows r2/1); target only has r1/1.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ],
        {"r1": 1, "r2": 0},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    rounds = data["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r2", 1)],
        [("r1", 2)],
    ]
    # Operations, values and clocks are preserved verbatim.
    first = rounds[0][0]
    assert first["key"] == "k"
    assert first["op"] == "put"
    assert first["value"] == "b"
    assert first["clock"] == {"r1": 1, "r2": 1}
    assert data["progress"] == {"r1": 2, "r2": 1}


def test_catchup_round_includes_all_currently_ready_sorted():
    # Independent first events on both replicas are ready simultaneously;
    # second events wait for cross-replica knowledge.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "k", put="a2", clock={"r1": 2, "r2": 1}),
            ev("r2", 2, "k", put="b2", clock={"r1": 1, "r2": 2}),
        ],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1), ("r2", 1)],
        [("r1", 2), ("r2", 2)],
    ]


def test_catchup_concurrent_equal_value_events_are_both_transferred():
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="x", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0}),
        ],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    assert len(rounds) == 1
    assert [(e["replica_id"], e["seq"]) for e in rounds[0]] == [
        ("r1", 1),
        ("r2", 1),
    ]
    assert all(e["value"] == "x" for e in rounds[0])


def test_catchup_all_received_has_empty_rounds():
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
        {"r1": 1, "r2": 1},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["rounds"] == []
    assert data["progress"] == {"r1": 1, "r2": 1}


def test_catchup_delete_events_preserved():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", delete="delete", clock={"r1": 1, "r2": 0})],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    (only,) = r.json()["rounds"][0]
    assert only["op"] == "delete"
    assert only["value"] is None
    assert only["key"] == "k"


def test_catchup_progress_reports_eventless_replica_as_zero():
    r = post_catchup(
        ["r1", "r2", "r3"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0, "r3": 0})],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert r.status_code == 200, r.text
    assert r.json()["progress"] == {"r1": 1, "r2": 0, "r3": 0}


def test_catchup_missing_known_clock_component():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 0},
    )
    detail = assert_422(r, "known_clock_coverage")
    assert "event_index" not in detail[0]


def test_catchup_extra_known_clock_component():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert_422(r, "known_clock_coverage")


def test_catchup_known_clock_beyond_batch_max_seq():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 1, "r2": 3},
    )
    assert_422(r, "known_clock_ahead_of_batch")


def test_catchup_received_event_clock_beyond_known_clock():
    # r2/1 is missing (known_clock r2 = 0), but the already-received r1/1
    # claims to know r2/1 -> the target's knowledge is inconsistent.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
        ],
        {"r1": 1, "r2": 0},
    )
    detail = assert_422(r, "clock_exceeds_known_clock")
    assert detail[0]["event_index"] == 1


def test_catchup_negative_known_clock_component_is_422():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": -1, "r2": 0},
    )
    assert r.status_code == 422


def test_catchup_still_validates_batch_with_original_rules():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
        {"r1": 0, "r2": 0},
    )
    assert_422(r, "clock_self_component")


def test_catchup_strictly_alternating_chain_takes_one_event_per_round():
    events = [
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
        ev("r2", 2, "k", put="d", clock={"r1": 2, "r2": 2}),
        ev("r1", 3, "k", put="e", clock={"r1": 3, "r2": 2}),
    ]
    r = post_catchup(["r1", "r2"], events, {"r1": 0, "r2": 0})
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    # Dependencies must be present at the target or scheduled in an *earlier*
    # round, so the strictly alternating chain cannot batch events together.
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1)],
        [("r2", 1)],
        [("r1", 2)],
        [("r2", 2)],
        [("r1", 3)],
    ]
    assert r.json()["progress"] == {"r1": 3, "r2": 2}


# ---------------------------------------------------------------------------
# /api/resolve-conflict
# ---------------------------------------------------------------------------


def two_branch_conflict(put_a="alice", put_b="bob"):
    return [
        ev("r2", 1, "name", put=put_b, clock={"r1": 0, "r2": 1}),
        ev("r1", 1, "name", put=put_a, clock={"r1": 1, "r2": 0}),
    ]


def test_resolve_conflict_appends_dominating_put_and_returns_verdict():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    new_event = data["event"]
    assert new_event == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"] == {
        "key": "name",
        "state": "put",
        "value": "chosen",
        "event": {
            "replica_id": "r1",
            "seq": 2,
            "op": "put",
            "value": "chosen",
            "clock": {"r1": 2, "r2": 1},
        },
    }


def test_resolve_conflict_equal_value_conflict_is_also_resolved():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict("x", "x"),
        key="name",
        writer="r2",
        versions=[{"replica_id": "r2", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="x",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["event"]["seq"] == 2
    assert data["event"]["clock"] == {"r1": 1, "r2": 2}
    assert data["result"]["state"] == "put"
    assert data["result"]["event"]["seq"] == 2


def test_resolve_conflict_delete_op():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        delete="delete",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["event"] == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "delete": "delete",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"]["state"] == "deleted"
    assert data["result"]["event"]["op"] == "delete"
    assert data["result"]["event"]["value"] is None


def test_resolve_conflict_empty_put_allowed():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["put"] == ""
    assert r.json()["result"]["value"] == ""


def test_resolve_conflict_writer_with_no_prior_events_starts_at_seq_one():
    # r3 is eventless; it writes the resolution as r3/1 with an all-history
    # clock covering both branches.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ],
        key="name",
        writer="r3",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"] == {
        "replica_id": "r3",
        "seq": 1,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 1, "r2": 1, "r3": 1},
    }


def test_resolve_conflict_clock_covers_writer_prior_history_beyond_branches():
    # r1/1 (key z) knows r3/1; r1/2 and r2/1 conflict on name but neither
    # branch has seen r3/1.  The resolution clock must still carry r3=1 from
    # the writer r1's own prior history.
    events = [
        ev("r3", 1, "z", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
        ev("r1", 1, "z", put="z2", clock={"r1": 1, "r2": 0, "r3": 1}),
        ev("r1", 2, "name", put="a", clock={"r1": 2, "r2": 0, "r3": 1}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["clock"] == {"r1": 3, "r2": 1, "r3": 1}


def test_resolve_conflict_event_replays_through_resolve():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    new_event = r.json()["event"]

    rr = post(["r1", "r2"], two_branch_conflict() + [new_event])
    assert rr.status_code == 200, rr.text
    (item,) = rr.json()["results"]
    assert item["state"] == "put"
    assert item["value"] == "chosen"
    assert item["event"]["seq"] == 2


def test_resolve_conflict_event_replays_through_catchup_by_dependency():
    # Eventless writer r3 resolves a 3-replica conflict; replaying onto a
    # target that knows nothing must transfer all three branches first, with
    # the resolution in a later round.
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r3",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    resolution = r.json()["event"]

    cr = post_catchup(
        ["r1", "r2", "r3"],
        branches + [resolution],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert cr.status_code == 200, cr.text
    rounds = cr.json()["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1), ("r2", 1)],
        [("r3", 1)],
    ]
    assert rounds[1][0]["value"] == "chosen"


def test_resolve_conflict_does_not_change_other_keys_verdicts():
    events = two_branch_conflict() + [
        ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1})
    ]
    # Note: r1/2 here dominates r2/1; it sits on a different key, unaffected.
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r2",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        delete="delete",
    )
    assert r.status_code == 200, r.text
    resolution = r.json()["event"]

    rr = post(["r1", "r2"], events + [resolution])
    assert rr.status_code == 200, rr.text
    results = {x["key"]: x for x in rr.json()["results"]}
    assert results["name"]["state"] == "deleted"
    assert results["other"]["state"] == "put"
    assert results["other"]["value"] == "keep"


def test_resolve_conflict_versions_may_arrive_out_of_order():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r2", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["clock"] == {"r1": 2, "r2": 1}


def test_resolve_conflict_unknown_writer_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r9",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "unknown_replica_id")


def test_resolve_conflict_version_with_unknown_replica_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r9", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "unknown_replica_id")


def test_resolve_conflict_duplicate_versions_are_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "duplicate_conflict_version")


def test_resolve_conflict_single_version_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 422, r.text


def test_resolve_conflict_missing_version_is_409():
    # Three pairwise-incomparable maximals; listing only two omits one.
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "name", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )

    # A one-entry list is a shape error (at least two required) -> 422.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 422, r.text

    # Two listed but one is not maximal (r3/1 lives on another key): 409.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
            ev("r3", 1, "other", put="x", clock={"r1": 0, "r2": 0, "r3": 1}),
        ],
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r3", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_extra_version_is_409():
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
            ev("r3", 1, "other", put="x", clock={"r1": 0, "r2": 0, "r3": 1}),
        ],
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
            {"replica_id": "r3", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_version_pointing_at_nonexistent_seq_is_409():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 7}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_key_already_resolved_is_409():
    # r1/2 causally dominates r2/1: name is a plain put, not a conflict.
    events = [
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1}),
        ev("r1", 1, "other", put="x", clock={"r1": 1, "r2": 0}),
        ev("r1", 2, "name", put="a", clock={"r1": 2, "r2": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(e["code"] == "key_not_conflicted" for e in r.json()["detail"])


def test_resolve_conflict_unknown_key_is_409():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="missing",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(e["code"] == "key_not_conflicted" for e in r.json()["detail"])


def test_resolve_conflict_full_batch_is_422():
    # 120 valid events on r1 (seq 1..60) and r2 (seq 1..60), all key 'name';
    # the batch already contains a conflict and cannot take one more event.
    events = []
    for n in range(1, 61):
        c1 = {"r1": n, "r2": n - 1}
        c2 = {"r1": n - 1, "r2": n}
        events.append(ev("r1", n, "name", put="a", clock=c1))
        events.append(ev("r2", n, "name", put="b", clock=c2))
    assert len(events) == 120
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 60}, {"replica_id": "r2", "seq": 60}],
        put="chosen",
    )
    assert r.status_code == 422, r.text
    assert any(e["code"] == "batch_full" for e in r.json()["detail"])


def test_resolve_conflict_still_validates_original_batch():
    bad = [
        ev("r1", 1, "name", put="a", clock={"r1": 2, "r2": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2"],
        bad,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "clock_self_component")


def test_resolve_conflict_requires_exactly_one_op():
    r = client.post(
        "/api/resolve-conflict",
        json={
            "replica_ids": ["r1", "r2"],
            "events": two_branch_conflict(),
            "key": "name",
            "replica_id": "r1",
            "versions": [
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
        },
    )
    assert r.status_code == 422

    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="x",
        delete="delete",
    )
    assert r.status_code == 422


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Persistent histories
# ---------------------------------------------------------------------------


def hist_create(replica_ids, events):
    return client.post(
        "/api/histories",
        json={"replica_ids": replica_ids, "events": events},
    )


def hist_get(hid):
    return client.get(f"/api/histories/{hid}")


def hist_merge(hid, events):
    return client.post(
        f"/api/histories/{hid}/events", json={"events": events}
    )


def hist_commit(hid, key, writer, versions, **ops):
    body = {
        "key": key,
        "replica_id": writer,
        "versions": versions,
    }
    body.update(ops)
    return client.post(
        f"/api/histories/{hid}/resolve-conflict", json=body
    )


def hist_withdraw(hid, replica_id, seq):
    return client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": replica_id, "seq": seq},
    )


def hist_event_relation(hid, first, second):
    return client.post(
        f"/api/histories/{hid}/event-relation",
        json={"first": first, "second": second},
    )


def create_conflict_history():
    r = hist_create(["r1", "r2"], two_branch_conflict())
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_history_create_returns_id_and_reads_back_sorted_batch_and_verdict():
    r = hist_create(
        ["r1", "r2"],
        [
            ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 201, r.text
    hid = r.json()["id"]
    assert isinstance(hid, str) and len(hid) >= 16

    got = hist_get(hid)
    assert got.status_code == 200, got.text
    data = got.json()
    assert data["id"] == hid
    assert data["replica_ids"] == ["r1", "r2"]
    # Events sorted by replica_id then seq regardless of input order.
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r2", 1),
    ]
    # The verdict is computed over that same committed state.
    (result,) = data["results"]
    assert result["state"] == "conflict"
    assert [e["seq"] for e in result["events"]] == [1, 1]


def test_history_create_invalid_batch_is_422_and_nothing_stored():
    r = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
    )
    assert_422(r, "clock_self_component")

    # The bad batch never became a history (its id was never issued anyway,
    # and the DB still has no rows: reads of random ids keep returning 404).
    assert hist_get("deadbeef" * 4).status_code == 404


def test_history_unknown_id_is_404_on_every_endpoint():
    hid = "0" * 32
    assert hist_get(hid).status_code == 404
    assert (
        hist_merge(
            hid, [ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0})]
        ).status_code
        == 404
    )
    assert (
        hist_commit(
            hid,
            key="k",
            writer="r1",
            versions=[
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
            put="x",
        ).status_code
        == 404
    )
    assert (
        hist_event_relation(
            hid,
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ).status_code
        == 404
    )


def test_history_merge_appends_events_and_returns_full_state():
    hid = create_conflict_history()

    r = hist_merge(
        hid,
        [ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1})],
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    results = {x["key"]: x for x in data["results"]}
    assert results["name"]["state"] == "conflict"
    assert results["other"]["state"] == "put"

    # A subsequent read returns exactly what was committed.
    got = hist_get(hid).json()
    assert got["events"] == data["events"]
    assert got["results"] == data["results"]


def test_history_merge_identical_event_is_ignored():
    hid = create_conflict_history()
    same = two_branch_conflict()

    # Re-send both stored events verbatim: no new events, no error.
    r = hist_merge(hid, same)
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 2

    # Mix of identical + genuinely new in the same call.
    r = hist_merge(
        hid,
        [
            same[0],
            ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 3


def test_history_merge_same_identity_different_content_is_409_and_atomic():
    hid = create_conflict_history()

    before = hist_get(hid).json()
    r = hist_merge(
        hid,
        [
            ev("r2", 1, "name", put="different", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "other", put="x", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "event_content_mismatch" for e in r.json()["detail"]
    )

    # Neither event (not even the valid second one) was written.
    after = hist_get(hid).json()
    assert after["events"] == before["events"]


def assert_merge_409(r, expected_index):
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    (err,) = detail
    assert err["code"] == "event_content_mismatch"
    assert err["event_index"] == expected_index
    return err


def test_history_merge_duplicate_extra_zero_clock_component_is_409_and_atomic():
    # Regression: a resend must be compared on its FULL clock.  An extra
    # component (even with value 0) is different content -> 409, and the
    # legal new event travelling in the same batch must not be written.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r1", 2, "other", put="x", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert_merge_409(r, 0)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_duplicate_missing_clock_component_is_409_not_500():
    # Regression: a missing clock component must not raise KeyError while
    # projecting the clock; it is simply different content -> 409.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [ev("r2", 1, "name", put="bob", clock={"r2": 1})],
    )
    assert_merge_409(r, 0)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_duplicate_changed_clock_component_value_is_409():
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            # Same key/op/value as stored r2/1 but a different clock component.
            ev("r2", 1, "name", put="bob", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert_merge_409(r, 0)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_duplicate_clock_key_order_does_not_matter():
    # Equal clocks serialized with the keys in a different order are still
    # the same event -> ignored, nothing changes.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "name",
                "put": "alice",
                "clock": {"r2": 0, "r1": 1},
            }
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["events"] == before["events"]


def test_history_merge_batch_internal_duplicate_extra_clock_component_is_409():
    # The same full-content comparison applies to two copies of a brand-new
    # (replica_id, seq) inside one batch: an extra clock component on the
    # second copy is a content mismatch -> 409, nothing is written.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1, "r3": 0}),
        ],
    )
    assert_merge_409(r, 1)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_new_event_illegal_clock_coverage_is_still_422():
    # Coverage rules only apply to genuinely new events (the full history is
    # re-validated): extra or missing clock components on a not-yet-stored
    # identity stay 422, not 409, and nothing is written.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})],
    )
    assert_422(r, "clock_replica_coverage")
    assert hist_get(hid).json()["events"] == before["events"]

    r = hist_merge(
        hid,
        [ev("r1", 2, "k", put="x", clock={"r1": 2})],
    )
    assert_422(r, "clock_replica_coverage")
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_failing_validation_is_422_and_atomic():
    # Valid seed on r1; incoming batch breaks contiguity on r2.
    r = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    )
    hid = r.json()["id"]

    before = hist_get(hid).json()
    r = hist_merge(
        hid,
        [ev("r2", 2, "k", put="b", clock={"r1": 1, "r2": 2})],
    )
    assert_422(r, "non_contiguous_seq")
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_enforces_120_event_limit():
    # 60 paired events -> r1 has 1..60 and r2 has 1..60 (120 total).
    seed = []
    for n in range(1, 61):
        seed.append(
            ev("r1", n, "k", put="a", clock={"r1": n, "r2": n - 1})
        )
        seed.append(
            ev("r2", n, "k", put="b", clock={"r1": n, "r2": n})
        )
    assert len(seed) == 120
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    r = hist_merge(
        hid, [ev("r1", 61, "k", put="c", clock={"r1": 61, "r2": 60})]
    )
    assert_422(r, "batch_full")
    assert len(hist_get(hid).json()["events"]) == 120


def test_history_merge_unknown_replica_is_422():
    hid = create_conflict_history()
    r = hist_merge(
        hid, [ev("r9", 1, "k", put="x", clock={"r1": 0, "r2": 0})]
    )
    assert_422(r, "unknown_replica_id")


def test_history_commit_resolves_conflict_and_persists_event():
    hid = create_conflict_history()

    r = hist_commit(
        hid,
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == hid
    assert data["event"] == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"]["state"] == "put"
    assert data["result"]["value"] == "chosen"

    # The committed history contains the resolution event and its verdict.
    got = hist_get(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in got["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    assert got["results"][0]["state"] == "put"


def test_history_commit_then_stale_versions_get_409_and_nothing_written():
    hid = create_conflict_history()
    versions = [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r2", "seq": 1},
    ]

    assert hist_commit(
        hid, key="name", writer="r1", versions=versions, put="chosen"
    ).status_code == 200
    state_after_first = hist_get(hid).json()

    # Replaying the same stale versions against the resolved key -> 409.
    r = hist_commit(
        hid, key="name", writer="r2", versions=versions, delete="delete"
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "key_not_conflicted" for e in r.json()["detail"]
    )

    # No r2/2 was allocated; the history is unchanged.
    assert hist_get(hid).json() == state_after_first


def test_history_commit_version_mismatch_is_409():
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "name", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = hist_create(["r1", "r2", "r3"], branches).json()["id"]

    r = hist_commit(
        hid,
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "conflict_versions_mismatch"
        for e in r.json()["detail"]
    )


def test_history_commit_writer_seq_continues_tail_under_concurrency():
    # Ten independent conflicting keys; resolve each from r1 concurrently.
    events = []
    for n in range(1, 11):
        events.append(
            ev("r1", n, f"k{n}", put=f"a{n}", clock={"r1": n, "r2": n - 1})
        )
        events.append(
            ev("r2", n, f"k{n}", put=f"b{n}", clock={"r1": n - 1, "r2": n})
        )
    hid = hist_create(["r1", "r2"], events).json()["id"]
    barrier = threading.Barrier(10)

    def resolve(n):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/resolve-conflict",
            json={
                "key": f"k{n}",
                "replica_id": "r1",
                "versions": [
                    {"replica_id": "r1", "seq": n},
                    {"replica_id": "r2", "seq": n},
                ],
                "put": f"chosen{n}",
            },
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(resolve, range(1, 11)))

    assert all(r.status_code == 200 for r in results), [
        r.text for r in results if r.status_code != 200
    ]
    # Exactly one resolution per seq 11..20 -- no duplicated seq, none lost.
    got = hist_get(hid).json()
    r1_seqs = sorted(
        e["seq"] for e in got["events"] if e["replica_id"] == "r1"
    )
    assert r1_seqs == list(range(1, 21))
    assert len(got["events"]) == 30
    # All ten keys end resolved (single maximal put from r1).
    assert all(x["state"] == "put" for x in got["results"])


def test_history_concurrent_merges_lose_no_events_and_validate():
    # Seven writers each append their own independent replica tail, so the
    # only correctness under concurrency is that every append survives and
    # every writer's seq tail is contiguous/unique.
    replicas = ["r1", "r2", "r3", "r4", "r5", "r6", "r7"]
    seed = [
        ev("r1", 1, "k", put="start", clock={r: 0 for r in replicas} | {"r1": 1})
    ]
    hid = hist_create(replicas, seed).json()["id"]
    barrier = threading.Barrier(6)

    def append_tail(rid):
        c = TestClient(app)
        barrier.wait()
        # First event on this replica knows r1/1; later ones chain locally.
        c.post(
            f"/api/histories/{hid}/events",
            json={
                "events": [
                    ev(
                        rid,
                        1,
                        f"k-{rid}-1",
                        put="x",
                        clock={r: 0 for r in replicas} | {"r1": 1, rid: 1},
                    )
                ]
            },
        )
        for n in range(2, 7):
            r = c.post(
                f"/api/histories/{hid}/events",
                json={
                    "events": [
                        ev(
                            rid,
                            n,
                            f"k-{rid}-{n}",
                            put="x",
                            clock={r: 0 for r in replicas}
                            | {"r1": 1, rid: n},
                        )
                    ]
                },
            )
            assert r.status_code == 200, r.text

    with ThreadPoolExecutor(max_workers=7) as pool:
        list(pool.map(append_tail, replicas[1:]))

    got = hist_get(hid).json()
    assert len(got["events"]) == 1 + 6 * 6
    for rid in replicas[1:]:
        seqs = sorted(
            e["seq"] for e in got["events"] if e["replica_id"] == rid
        )
        assert seqs == [1, 2, 3, 4, 5, 6]


def test_history_concurrent_commits_to_same_conflict_one_wins():
    hid = create_conflict_history()
    versions = [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r2", "seq": 1},
    ]
    barrier = threading.Barrier(2)

    def commit(writer):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/resolve-conflict",
            json={
                "key": "name",
                "replica_id": writer,
                "versions": versions,
                "put": f"from-{writer}",
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(commit, ["r1", "r2"]))

    statuses = sorted(r.status_code for r in results)
    assert statuses == [200, 409], [r.text for r in results]
    got = hist_get(hid).json()
    # Exactly one resolution event was ever allocated.
    assert len(got["events"]) == 3
    resolution = [
        e for e in got["events"]
        if (e["replica_id"], e["seq"]) not in [("r1", 1), ("r2", 1)]
    ]
    assert len(resolution) == 1
    assert got["results"][0]["state"] == "put"


def test_history_survives_process_restart_with_same_sqlite_file(tmp_path):
    db = tmp_path / "history.db"
    env = dict(os.environ, HISTORY_DB_PATH=str(db), PYTHONPATH="/workspace")

    def run(code):
        return subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
        )

    # Process A: create a history, merge an event, commit a resolution.
    create = run(
        """
import json
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice',
         'clock': {'r1': 1, 'r2': 0}},
        {'replica_id': 'r2', 'seq': 1, 'key': 'name', 'put': 'bob',
         'clock': {'r1': 0, 'r2': 1}},
    ],
})
hid = r.json()['id']
c.post(f'/api/histories/{hid}/events', json={'events': [
    {'replica_id': 'r2', 'seq': 2, 'key': 'other', 'put': 'x',
     'clock': {'r1': 0, 'r2': 2}}
]})
c.post(f'/api/histories/{hid}/resolve-conflict', json={
    'key': 'name', 'replica_id': 'r1',
    'versions': [
        {'replica_id': 'r1', 'seq': 1},
        {'replica_id': 'r2', 'seq': 1},
    ], 'put': 'chosen'})
print(hid)
"""
    )
    assert create.returncode == 0, create.stderr
    hid = create.stdout.strip()

    # Process B (fresh import, same file): everything is still readable.
    read = run(
        f"""
import json
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.get('/api/histories/{hid}')
assert r.status_code == 200, r.text
data = r.json()
assert data['replica_ids'] == ['r1', 'r2']
assert [(e['replica_id'], e['seq']) for e in data['events']] == [
    ('r1', 1), ('r1', 2), ('r2', 1), ('r2', 2)
]
name = [x for x in data['results'] if x['key'] == 'name'][0]
assert name['state'] == 'put' and name['value'] == 'chosen'
print('ok')
"""
    )
    assert read.returncode == 0, read.stderr
    assert read.stdout.strip() == "ok"



# ---------------------------------------------------------------------------
# Read-only withdrawal preview
# ---------------------------------------------------------------------------


def test_withdraw_preview_cascades_across_keys_and_keeps_concurrent_events():
    # r1/1 (key a) -> r2/1 synced with it (key b) -> r1/2 (key a);
    # r3/1 is a causally unrelated concurrent write on key c.
    seed = [
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "b", put="y", clock={"r1": 1, "r2": 1, "r3": 0}),
        ev("r1", 2, "a", delete="delete", clock={"r1": 2, "r2": 1, "r3": 0}),
        ev("r3", 1, "c", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = hist_create(["r1", "r2", "r3"], seed).json()["id"]

    r = hist_withdraw(hid, "r1", 1)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == hid
    # Withdrawn identities sorted by (replica_id, seq): target plus its
    # transitive dependents across both keys.
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    # Remaining events keep original wire form and canonical order; no seq or
    # clock is renumbered; the unrelated concurrent r3/1 survives untouched.
    assert data["events"] == [
        ev("r3", 1, "c", put="z", clock={"r1": 0, "r2": 0, "r3": 1})
    ]
    # Changes sorted by key, full verdict structures compared.
    assert [ch["key"] for ch in data["changes"]] == ["a", "b"]
    by_key = {ch["key"]: ch for ch in data["changes"]}
    assert by_key["a"]["before"]["state"] == "deleted"
    assert by_key["a"]["after"] is None
    assert by_key["b"]["before"]["state"] == "put"
    assert by_key["b"]["before"]["value"] == "y"
    assert by_key["b"]["after"] is None
    # Key c never appears: its verdict did not change.
    assert "c" not in by_key


def test_withdraw_preview_conflict_resolves_to_remaining_concurrent_branch():
    seed = [
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "a", put="y", clock={"r1": 0, "r2": 1}),
        # Unrelated tail on r1: withdrawing r2/1 must not touch it.
        ev("r1", 2, "b", put="z", clock={"r1": 2, "r2": 0}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    data = hist_withdraw(hid, "r2", 1).json()
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r2", 1)
    ]
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
    ]
    assert [ch["key"] for ch in data["changes"]] == ["a"]
    change = data["changes"][0]
    assert change["before"]["state"] == "conflict"
    assert len(change["before"]["events"]) == 2
    assert change["after"] == {
        "key": "a",
        "state": "put",
        "value": "x",
        "event": {
            "replica_id": "r1",
            "seq": 1,
            "op": "put",
            "value": "x",
            "clock": {"r1": 1, "r2": 0},
        },
    }


def test_withdraw_preview_removing_only_event_still_returns_200_empty_list():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "only", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]

    r = hist_withdraw(hid, "r1", 1)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["withdrawn"] == [{"replica_id": "r1", "seq": 1}]
    assert data["events"] == []
    assert len(data["changes"]) == 1
    assert data["changes"][0]["key"] == "only"
    assert data["changes"][0]["before"]["state"] == "put"
    # An emptied key's after verdict is null, which is not a "deleted" state.
    assert data["changes"][0]["after"] is None


def test_withdraw_preview_follows_clock_components_on_other_replicas():
    # r1/1 (k1); r2/1 depends on r1/1 (k2); r1/2 depends on r2/1 and its own
    # previous event (k3): withdrawing r1/1 must pull all three out.
    seed = [
        ev("r1", 1, "k1", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k2", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "k3", delete="delete", clock={"r1": 2, "r2": 1}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    data = hist_withdraw(hid, "r1", 1).json()
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    assert data["events"] == []
    assert [ch["key"] for ch in data["changes"]] == ["k1", "k2", "k3"]
    assert all(ch["after"] is None for ch in data["changes"])


def test_withdraw_preview_does_not_mutate_the_stored_history():
    before_events = [
        ev("r2", 1, "a", put="y", clock={"r1": 1, "r2": 1}),
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0}),
    ]
    hid = hist_create(["r1", "r2"], before_events).json()["id"]
    before = hist_get(hid).json()

    preview = hist_withdraw(hid, "r1", 1)
    assert preview.status_code == 200
    # Repeated previews are identical...
    assert hist_withdraw(hid, "r1", 1).json() == preview.json()
    # ...and the committed history is byte-for-byte unchanged.
    after = hist_get(hid).json()
    assert after == before
    assert len(after["events"]) == 2


def test_withdraw_preview_unknown_history_and_missing_event_are_404():
    r = client.get(
        f"/api/histories/{'0' * 32}/withdraw-preview",
        params={"replica_id": "r1", "seq": 1},
    )
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"

    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]

    # Existing history, seq never used on that replica.
    r = hist_withdraw(hid, "r1", 2)
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "event_not_found"

    # replica_id outside the history's replica set is an unknown event, not
    # a 422: the parameters themselves are well-formed strings.
    r = hist_withdraw(hid, "r9", 1)
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "event_not_found"


def test_withdraw_preview_invalid_params_are_422():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]

    # seq below the minimum of 1.
    r = hist_withdraw(hid, "r1", 0)
    assert r.status_code == 422, r.text

    # Empty replica_id.
    r = client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": "", "seq": 1},
    )
    assert r.status_code == 422, r.text

    # Missing seq.
    r = client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": "r1"},
    )
    assert r.status_code == 422, r.text

    # Non-integer seq.
    r = client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": "r1", "seq": "x"},
    )
    assert r.status_code == 422, r.text


def test_withdraw_preview_works_after_merge_and_conflict_commit():
    # The preview reads the same committed history the mutating endpoints
    # write; a resolution event causally dominates both conflict branches.
    hid = create_conflict_history()
    assert (
        hist_commit(
            hid,
            key="name",
            writer="r1",
            versions=[
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
            put="chosen",
        ).status_code
        == 200
    )

    # Withdrawing r1/1 pulls the committed r1/2 resolution (whose previous
    # event is r1/1), but r2/1 is a *concurrent* branch (its clock has
    # r1 = 0) and therefore stays; the key reverts to r2's put.
    data = hist_withdraw(hid, "r1", 1).json()
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r1", 1),
        ("r1", 2),
    ]
    assert data["events"] == [
        ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1})
    ]
    change = data["changes"][0]
    assert change["before"]["state"] == "put"
    assert change["before"]["value"] == "chosen"
    assert change["after"] == {
        "key": "name",
        "state": "put",
        "value": "bob",
        "event": {
            "replica_id": "r2",
            "seq": 1,
            "op": "put",
            "value": "bob",
            "clock": {"r1": 0, "r2": 1},
        },
    }


# ---------------------------------------------------------------------------
# Import sessions: staged multi-batch offline imports, committed at once
# ---------------------------------------------------------------------------


def session_create(hid):
    return client.post(f"/api/histories/{hid}/import-sessions")


def session_get(hid, sid):
    return client.get(f"/api/histories/{hid}/import-sessions/{sid}")


def session_append(hid, sid, events):
    return client.post(
        f"/api/histories/{hid}/import-sessions/{sid}/events",
        json={"events": events},
    )


def session_commit(hid, sid):
    return client.post(f"/api/histories/{hid}/import-sessions/{sid}/commit")


def create_session(hid):
    r = session_create(hid)
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_import_session_create_returns_id_and_reads_back_empty_open():
    hid = create_conflict_history()

    sid = create_session(hid)
    assert isinstance(sid, str) and len(sid) >= 16

    r = session_get(hid, sid)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data == {
        "id": sid,
        "history_id": hid,
        "status": "open",
        "events": [],
    }


def test_import_session_unknown_history_or_session_is_404():
    hid = create_conflict_history()
    sid = create_session(hid)
    missing_hid = "0" * 32
    missing_sid = "1" * 32

    # Unknown history on every session endpoint.
    assert session_create(missing_hid).status_code == 404
    assert session_get(missing_hid, sid).status_code == 404
    assert (
        session_append(
            missing_hid,
            sid,
            [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 404
    )
    assert session_commit(missing_hid, sid).status_code == 404
    r = session_create(missing_hid)
    assert r.json()["detail"][0]["code"] == "history_not_found"

    # Unknown session (including a session id of another history).
    other_hid = create_conflict_history()
    assert session_get(hid, missing_sid).status_code == 404
    assert session_get(other_hid, sid).status_code == 404
    r = session_get(hid, missing_sid)
    assert r.json()["detail"][0]["code"] == "session_not_found"
    assert (
        session_append(
            hid,
            missing_sid,
            [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 404
    )
    assert session_commit(hid, missing_sid).status_code == 404


def test_import_session_stages_out_of_order_with_missing_dependencies():
    # Staging tolerates events whose predecessors or cross-replica
    # dependencies have not arrived yet; they only fail at commit time.
    hid = create_conflict_history()
    sid = create_session(hid)

    # r1/3 before r1/2; r2/2 points at a not-yet-staged r1/3.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 3, "name", put="c", clock={"r1": 3, "r2": 1}),
            ev("r2", 2, "other", put="x", clock={"r1": 3, "r2": 2}),
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "open"

    r = session_append(
        hid, sid, [ev("r1", 2, "name", put="b", clock={"r1": 2, "r2": 1})]
    )
    assert r.status_code == 200, r.text

    # The session read returns the staged events in original wire format,
    # sorted by replica_id then seq regardless of arrival order.
    data = session_get(hid, sid).json()
    assert data["status"] == "open"
    assert data["events"] == [
        ev("r1", 2, "name", put="b", clock={"r1": 2, "r2": 1}),
        ev("r1", 3, "name", put="c", clock={"r1": 3, "r2": 1}),
        ev("r2", 2, "other", put="x", clock={"r1": 3, "r2": 2}),
    ]

    # Committing the completed session merges everything at once.
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r1", 3),
        ("r2", 1),
        ("r2", 2),
    ]
    assert session_get(hid, sid).json()["status"] == "closed"


def test_import_session_append_per_event_validation_is_422_and_atomic():
    hid = create_conflict_history()
    sid = create_session(hid)
    ok = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})

    # Unknown replica.
    r = session_append(
        hid, sid, [ev("r9", 1, "k", put="x", clock={"r1": 0, "r2": 0})]
    )
    assert_422(r, "unknown_replica_id")

    # Clock coverage: extra or missing component.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})]
    )
    assert_422(r, "clock_replica_coverage")
    r = session_append(hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2})])
    assert_422(r, "clock_replica_coverage")

    # Self component must equal seq.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1})]
    )
    assert_422(r, "clock_self_component")

    # Schema-level field errors (put+delete together) are 422 as well.
    r = session_append(
        hid,
        sid,
        [
            {
                "replica_id": "r1",
                "seq": 2,
                "key": "k",
                "put": "x",
                "delete": "delete",
                "clock": {"r1": 2, "r2": 1},
            }
        ],
    )
    assert r.status_code == 422

    # A valid event travelling with an illegal one is not staged either.
    r = session_append(
        hid,
        sid,
        [ok, ev("r1", 3, "k", put="y", clock={"r1": 4, "r2": 1})],
    )
    assert_422(r, "clock_self_component")
    assert session_get(hid, sid).json()["events"] == []


def test_import_session_append_identical_ignored_different_content_409():
    hid = create_conflict_history()
    sid = create_session(hid)
    staged = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})
    assert session_append(hid, sid, [staged]).status_code == 200

    # Identical re-staging (clock key order irrelevant) is ignored.
    r = session_append(
        hid,
        sid,
        [
            {
                "replica_id": "r1",
                "seq": 2,
                "key": "k",
                "put": "x",
                "clock": {"r2": 1, "r1": 2},
            }
        ],
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 1

    # Same identity, different content -> 409, and the valid event in the
    # same append is not staged either.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="different", clock={"r1": 2, "r2": 1}),
            ev("r2", 2, "k", put="y", clock={"r1": 2, "r2": 2}),
        ],
    )
    assert r.status_code == 409, r.text
    (err,) = r.json()["detail"]
    assert err["code"] == "event_content_mismatch"
    assert err["event_index"] == 0
    assert len(session_get(hid, sid).json()["events"]) == 1

    # Two copies of a new identity inside one append: identical -> one
    # staged; different content -> 409.
    new = ev("r1", 3, "k", put="z", clock={"r1": 3, "r2": 1})
    r = session_append(hid, sid, [new, dict(new)])
    assert r.status_code == 200
    assert len(r.json()["events"]) == 2
    r = session_append(
        hid,
        sid,
        [
            ev("r2", 2, "k", put="y", clock={"r1": 3, "r2": 2}),
            ev("r2", 2, "k", put="z", clock={"r1": 3, "r2": 2}),
        ],
    )
    assert r.status_code == 409
    assert r.json()["detail"][0]["event_index"] == 1


def assert_session_409(r, expected_index):
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    (err,) = detail
    assert err["code"] == "event_content_mismatch"
    assert err["event_index"] == expected_index
    return err


def test_import_session_append_retransmission_bad_clock_is_409_not_422():
    # Regression: a same-identity retransmission whose clock misses a
    # component, adds an extra (even zero-valued) component, or changes the
    # own component must be judged by full-content comparison -> 409, not by
    # the per-event coverage/self-component checks -> 422.
    hid = create_conflict_history()
    sid = create_session(hid)
    original = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})
    assert session_append(hid, sid, [original]).status_code == 200
    before = session_get(hid, sid).json()
    hist_before = hist_get(hid).json()

    # Extra component (value 0) -> 409.
    r = session_append(
        hid,
        sid,
        [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})],
    )
    assert_session_409(r, 0)

    # Missing component -> 409 (and no KeyError projecting the clock).
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2})]
    )
    assert_session_409(r, 0)

    # Own component changed (r1 = 3 != seq 2) -> 409, not clock_self_component.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1})]
    )
    assert_session_409(r, 0)

    # Another component value changed -> 409.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 2})]
    )
    assert_session_409(r, 0)

    # A legal new event travelling with the mismatched retransmission is not
    # staged either; the history is untouched as well.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1}),
            ev("r2", 2, "other", put="y", clock={"r1": 2, "r2": 2}),
        ],
    )
    assert_session_409(r, 0)
    assert session_get(hid, sid).json() == before
    assert hist_get(hid).json() == hist_before

    # A byte-identical retransmission (only clock key order differs) is still
    # ignored.
    r = session_append(
        hid,
        sid,
        [
            {
                "replica_id": "r1",
                "seq": 2,
                "key": "k",
                "put": "x",
                "clock": {"r2": 1, "r1": 2},
            }
        ],
    )
    assert r.status_code == 200, r.text
    assert session_get(hid, sid).json() == before


def test_import_session_append_batch_internal_retransmission_bad_clock_is_409():
    # Two copies of a brand-new identity inside one append: an extra zero
    # component (or missing component) on the second copy is a content
    # mismatch against the earlier copy -> 409, not a 422 coverage error.
    hid = create_conflict_history()
    sid = create_session(hid)

    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1}),
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0}),
        ],
    )
    assert_session_409(r, 1)
    assert session_get(hid, sid).json()["events"] == []

    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1}),
            ev("r1", 2, "k", put="x", clock={"r1": 2}),
        ],
    )
    assert_session_409(r, 1)
    assert session_get(hid, sid).json()["events"] == []


def test_import_session_append_new_identity_still_gets_per_event_422():
    # The per-event checks still apply to genuinely new identities; missing
    # dependencies remain tolerated.
    hid = create_conflict_history()
    sid = create_session(hid)
    before = session_get(hid, sid).json()

    # New identity with an illegal clock -> 422 (even though the same clock
    # shape on a retransmission would be 409).
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})]
    )
    assert_422(r, "clock_replica_coverage")
    r = session_append(hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2})])
    assert_422(r, "clock_replica_coverage")
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1})]
    )
    assert_422(r, "clock_self_component")
    r = session_append(
        hid, sid, [ev("r9", 1, "k", put="x", clock={"r1": 0, "r2": 0})]
    )
    assert_422(r, "unknown_replica_id")
    assert session_get(hid, sid).json() == before

    # 422 (illegal new identity) wins over a 409 (mismatched retransmission)
    # in the same append, and nothing is staged.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0}),
            ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1}),
        ],
    )
    assert_422(r, "clock_replica_coverage")
    assert session_get(hid, sid).json() == before


def test_import_session_append_121_identical_events_stage_one():
    # A batch may carry more than 120 raw entries: 121 identical copies of
    # one legal new event deduplicate to a single staged event.
    hid = create_conflict_history()
    sid = create_session(hid)
    one = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})

    r = session_append(hid, sid, [dict(one) for _ in range(121)])
    assert r.status_code == 200, r.text
    assert r.json()["events"] == [one]

    # Mix: 121 copies of the staged event plus 120 copies of one more new
    # identity -> two distinct events staged, still within the cap.
    other = ev("r2", 2, "other", put="y", clock={"r1": 2, "r2": 2})
    r = session_append(
        hid, sid, [dict(one) for _ in range(121)] + [dict(other) for _ in range(120)]
    )
    assert r.status_code == 200, r.text
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 2),
        ("r2", 2),
    ]


def test_import_session_full_session_retransmissions_still_succeed():
    # A session holding exactly 120 distinct events accepts another raw
    # batch that only re-sends (all of) them, even though the batch itself
    # has 120 entries and one more copy makes it 121 raw entries.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r2", 1, "seed", put="s", clock={"r1": 0, "r2": 1})],
    ).json()["id"]
    sid = create_session(hid)
    staged = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 121)
    ]
    assert session_append(hid, sid, staged).status_code == 200
    assert len(session_get(hid, sid).json()["events"]) == 120

    # Re-send all 120, plus one extra copy -> 121 raw entries, all repeats.
    r = session_append(hid, sid, staged + [dict(staged[0])])
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 120

    # A genuinely new identity still exceeds the cap -> 422, even when it
    # rides along with 121 retransmissions; the append is atomic.
    r = session_append(
        hid,
        sid,
        [dict(staged[0]) for _ in range(121)]
        + [ev("r1", 121, "k", put="v", clock={"r1": 121, "r2": 0})],
    )
    assert_422(r, "batch_full")
    assert len(session_get(hid, sid).json()["events"]) == 120


def test_import_session_cap_counts_distinct_identities_only():
    # 119 distinct events staged; an append with one genuinely new identity
    # duplicated twice reaches exactly 120 distinct -> OK.  One more new
    # identity (also duplicated) would make 121 distinct -> 422, and the
    # whole append (including its duplicates) is rejected.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r2", 1, "seed", put="s", clock={"r1": 0, "r2": 1})],
    ).json()["id"]
    sid = create_session(hid)
    first = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 120)
    ]
    assert session_append(hid, sid, first).status_code == 200

    last = ev("r1", 120, "k", put="v", clock={"r1": 120, "r2": 0})
    r = session_append(hid, sid, [dict(last), dict(last)])
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 120

    overflow = ev("r1", 121, "k", put="v", clock={"r1": 121, "r2": 0})
    r = session_append(hid, sid, [dict(overflow), dict(overflow)])
    assert_422(r, "batch_full")
    assert len(session_get(hid, sid).json()["events"]) == 120

    # 121 distinct identities in a single raw append on an empty session is
    # likewise rejected by the distinct-identity cap, not accepted.
    sid2 = create_session(hid)
    many = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 122)
    ]
    r = session_append(hid, sid2, many)
    assert_422(r, "batch_full")
    assert session_get(hid, sid2).json()["events"] == []


def test_import_session_append_enforces_120_distinct_event_limit():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r2", 1, "seed", put="s", clock={"r1": 0, "r2": 1})],
    ).json()["id"]
    sid = create_session(hid)

    first = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 61)
    ]
    second = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(61, 121)
    ]
    assert session_append(hid, sid, first).status_code == 200
    r = session_append(hid, sid, second)
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 120

    # Re-staging an already-staged event is still fine (no new identity)...
    r = session_append(hid, sid, [first[0]])
    assert r.status_code == 200
    # ...but one more distinct event exceeds the session limit.
    r = session_append(
        hid, sid, [ev("r1", 121, "k", put="v", clock={"r1": 121, "r2": 0})]
    )
    assert_422(r, "batch_full")
    assert len(session_get(hid, sid).json()["events"]) == 120


def test_import_session_commit_returns_history_read_structure_and_closes():
    hid = create_conflict_history()
    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [
                ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1}),
                ev("r2", 2, "name", put="bob2", clock={"r1": 2, "r2": 2}),
            ],
        ).status_code
        == 200
    )

    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    data = r.json()
    # Same shape as GET /api/histories/{id}.
    assert data["id"] == hid
    assert data["replica_ids"] == ["r1", "r2"]
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
        ("r2", 2),
    ]
    results = {x["key"]: x for x in data["results"]}
    assert results["other"]["state"] == "put"
    assert results["name"]["state"] == "put"
    assert results["name"]["value"] == "bob2"

    # The merge and the close took effect together.
    assert hist_get(hid).json() == data
    assert session_get(hid, sid).json()["status"] == "closed"


def test_import_session_commit_missing_dependency_422_then_fix_and_retry():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)

    # r1/3 staged without r1/2: staging accepts, commit rejects (422).
    assert (
        session_append(
            hid, sid, [ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 0})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert_422(r, "non_contiguous_seq")

    # The failed commit changed nothing: the session is still open with its
    # staged events, and the history is untouched.
    session = session_get(hid, sid).json()
    assert session["status"] == "open"
    assert [(e["replica_id"], e["seq"]) for e in session["events"]] == [
        ("r1", 3)
    ]
    assert len(hist_get(hid).json()["events"]) == 1

    # Cross-replica dependencies are checked at commit time too: a second
    # session stages r2/1 pointing at r1/9, which exists nowhere.
    sid2 = create_session(hid)
    assert (
        session_append(
            hid, sid2, [ev("r2", 1, "k", put="b", clock={"r1": 9, "r2": 1})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid2)
    assert_422(r, "clock_points_to_missing_event")
    assert session_get(hid, sid2).json()["status"] == "open"
    assert len(hist_get(hid).json()["events"]) == 1

    # Append the missing r1/2 to the first session and retry the same
    # commit: now it succeeds and closes the session.
    assert (
        session_append(
            hid, sid, [ev("r1", 2, "k", put="b", clock={"r1": 2, "r2": 0})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r1", 3),
    ]
    assert session_get(hid, sid).json()["status"] == "closed"
    # The second session is unaffected and still open.
    assert session_get(hid, sid2).json()["status"] == "open"


def test_import_session_commit_sparse_huge_seq_is_422_and_changes_nothing():
    # Regression: history holds r1/1; staging accepts a sparse r1/10^12, but
    # the commit must reject it cheaply with 422/non_contiguous_seq -- never
    # exhaust memory expanding 1..10^12.  History, staging and open status
    # stay untouched for a retry.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    huge = 10**12
    assert (
        session_append(
            hid,
            sid,
            [ev("r1", huge, "k", put="b", clock={"r1": huge, "r2": 0})],
        ).status_code
        == 200
    )

    r = session_commit(hid, sid)
    detail = assert_422(r, "non_contiguous_seq")
    assert f"[2, {huge - 1}]" in detail[0]["message"]

    # History, staged events and the open status are all unchanged.
    assert [(e["replica_id"], e["seq"]) for e in hist_get(hid).json()["events"]] == [
        ("r1", 1)
    ]
    session = session_get(hid, sid).json()
    assert session["status"] == "open"
    assert [(e["replica_id"], e["seq"]) for e in session["events"]] == [
        ("r1", huge)
    ]

    # The existing gap query still describes the hole as one closed range.
    r = session_missing(hid, sid)
    assert r.json() == {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 2, "end_seq": huge - 1}
        ]
    }

    # A retry after appending the (small) missing predecessors can never fill
    # 10^12-2 events under the 120 cap, so commit keeps rejecting -- but a
    # distinct small gap fills and commits normally (next test).
    r = session_commit(hid, sid)
    assert_422(r, "non_contiguous_seq")
    assert session_get(hid, sid).json()["status"] == "open"


def test_import_session_commit_small_gap_is_422_with_interval_diagnosis():
    # Small-gap regression at commit time: history r1/1, staged r1/3 -> one
    # [2, 2] gap interval in the 422; filling r1/2 makes the retry succeed.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid, sid, [ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 0})]
        ).status_code
        == 200
    )

    r = session_commit(hid, sid)
    detail = assert_422(r, "non_contiguous_seq")
    assert "[2, 2]" in detail[0]["message"]
    assert session_get(hid, sid).json()["status"] == "open"

    assert (
        session_append(
            hid, sid, [ev("r1", 2, "k", put="b", clock={"r1": 2, "r2": 0})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert session_get(hid, sid).json()["status"] == "closed"


def test_history_merge_sparse_huge_seq_is_422_and_atomic():
    # The legacy merge endpoint shares full-batch validation and must reject
    # the same sparse input cheaply; the history stays at r1/1.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [ev("r1", 10**12, "k", put="b", clock={"r1": 10**12, "r2": 0})],
    )
    detail = assert_422(r, "non_contiguous_seq")
    assert f"[2, {10**12 - 1}]" in detail[0]["message"]
    assert hist_get(hid).json() == before


def test_sparse_huge_seq_validation_uses_no_gap_memory_under_rlimit(
    tmp_path,
):
    """Hard OOM guard: under a 768 MiB address-space cap the OLD
    ``set(range(1, 10**12 + 1))`` expansion dies (MemoryError/SIGKILL), while
    the interval-based validation must return a 422 both statelessly and on a
    session commit, leaving the session open."""

    db = tmp_path / "history.db"
    code = """
import json, resource, sys
resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024, 768 * 1024 * 1024))
from fastapi.testclient import TestClient
from app.main import app

huge = 10**12
c = TestClient(app, raise_server_exceptions=False)

# Stateless validation rejects the sparse batch with 422.
r = c.post('/api/resolve', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'k', 'put': 'a',
         'clock': {'r1': 1, 'r2': 0}},
        {'replica_id': 'r1', 'seq': huge, 'key': 'k', 'put': 'b',
         'clock': {'r1': huge, 'r2': 0}},
    ],
})
assert r.status_code == 422, r.status_code
codes = [e.get('code') for e in r.json()['detail']]
assert 'non_contiguous_seq' in codes, codes

# Persistent path: the sparse event stages but its commit rejects under cap.
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [{'replica_id': 'r1', 'seq': 1, 'key': 'k', 'put': 'a',
                'clock': {'r1': 1, 'r2': 0}}],
})
assert r.status_code == 201, r.text
hid = r.json()['id']
sid = c.post(f'/api/histories/{hid}/import-sessions').json()['id']
r = c.post(f'/api/histories/{hid}/import-sessions/{sid}/events', json={
    'events': [{'replica_id': 'r1', 'seq': huge, 'key': 'k', 'put': 'b',
                'clock': {'r1': huge, 'r2': 0}}],
})
assert r.status_code == 200, r.text
r = c.post(f'/api/histories/{hid}/import-sessions/{sid}/commit')
assert r.status_code == 422, r.status_code
codes = [e.get('code') for e in r.json()['detail']]
assert 'non_contiguous_seq' in codes, codes
data = c.get(f'/api/histories/{hid}/import-sessions/{sid}').json()
assert data['status'] == 'open'
assert [(e['replica_id'], e['seq']) for e in data['events']] == [('r1', huge)]
print('ok')
"""
    proc = subprocess.run(
        [sys.executable, "-c", code],
        env=dict(
            os.environ,
            HISTORY_DB_PATH=str(db),
            PYTHONPATH="/workspace",
        ),
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
    assert proc.stdout.strip() == "ok"


def test_import_session_commit_content_mismatch_with_history_is_409():
    hid = create_conflict_history()
    sid = create_session(hid)
    # Stage an event whose identity already exists in the committed history
    # but with different content: staging does not compare against the
    # history, the commit does.
    assert (
        session_append(
            hid,
            sid,
            [ev("r1", 1, "name", put="different", clock={"r1": 1, "r2": 0})],
        ).status_code
        == 200
    )
    before = hist_get(hid).json()
    r = session_commit(hid, sid)
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "event_content_mismatch" for e in r.json()["detail"]
    )
    # Session stays open, history untouched.
    assert session_get(hid, sid).json()["status"] == "open"
    assert hist_get(hid).json() == before


def test_import_session_commit_identical_to_committed_is_ignored():
    hid = create_conflict_history()
    sid = create_session(hid)
    # Re-stage an event that is already committed, plus a genuinely new one.
    assert (
        session_append(
            hid,
            sid,
            two_branch_conflict()
            + [ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 3


def test_import_session_commit_enforces_120_total_limit():
    seed = []
    for n in range(1, 61):
        seed.append(ev("r1", n, "k", put="a", clock={"r1": n, "r2": n - 1}))
        seed.append(ev("r2", n, "k", put="b", clock={"r1": n, "r2": n}))
    hid = hist_create(["r1", "r2"], seed).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid, sid, [ev("r1", 61, "k", put="c", clock={"r1": 61, "r2": 60})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert_422(r, "batch_full")
    assert session_get(hid, sid).json()["status"] == "open"
    assert len(hist_get(hid).json()["events"]) == 120


def test_import_session_closed_rejects_append_and_commit_with_409():
    hid = create_conflict_history()
    sid = create_session(hid)
    assert session_commit(hid, sid).status_code == 200  # empty commit closes

    r = session_commit(hid, sid)
    assert r.status_code == 409, r.text
    assert r.json()["detail"][0]["code"] == "session_closed"

    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})]
    )
    assert r.status_code == 409, r.text
    assert r.json()["detail"][0]["code"] == "session_closed"

    # The closed session remains readable, with its (empty) staged set.
    data = session_get(hid, sid).json()
    assert data["status"] == "closed"
    assert data["events"] == []


def test_import_session_staging_does_not_affect_history_reads_or_verdicts():
    hid = create_conflict_history()
    before = hist_get(hid).json()

    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [ev("r1", 2, "name", put="staged", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 200
    )

    # The committed history read and the withdrawal preview are unchanged
    # while events are only staged.
    assert hist_get(hid).json() == before
    assert hist_withdraw(hid, "r1", 1).json()["events"] == [
        ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1})
    ]

    # After the commit, the staged events are part of the history.
    assert session_commit(hid, sid).status_code == 200
    assert len(hist_get(hid).json()["events"]) == 3


def test_import_session_commit_concurrent_with_legacy_merge_loses_nothing():
    replicas = ["r1", "r2", "r3"]
    seed = [
        ev("r1", 1, "k", put="start", clock={"r1": 1, "r2": 0, "r3": 0})
    ]
    hid = hist_create(replicas, seed).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [ev("r2", 1, "a", put="x", clock={"r1": 1, "r2": 1, "r3": 0})],
        ).status_code
        == 200
    )
    barrier = threading.Barrier(2)

    def commit_session():
        c = TestClient(app)
        barrier.wait()
        return c.post(f"/api/histories/{hid}/import-sessions/{sid}/commit")

    def legacy_merge():
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/events",
            json={
                "events": [
                    ev(
                        "r3",
                        1,
                        "b",
                        put="y",
                        clock={"r1": 1, "r2": 0, "r3": 1},
                    )
                ]
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda f: f(), [commit_session, legacy_merge]))

    assert all(r.status_code == 200 for r in results), [
        r.text for r in results
    ]
    got = hist_get(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in got["events"]] == [
        ("r1", 1),
        ("r2", 1),
        ("r3", 1),
    ]
    assert session_get(hid, sid).json()["status"] == "closed"


def test_import_session_survives_process_restart_with_same_sqlite_file(
    tmp_path,
):
    db = tmp_path / "history.db"
    env = dict(os.environ, HISTORY_DB_PATH=str(db), PYTHONPATH="/workspace")

    def run(code):
        return subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
        )

    # Process A: create a history and a session, stage one event.
    create = run(
        """
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice',
         'clock': {'r1': 1, 'r2': 0}},
    ],
})
hid = r.json()['id']
r = c.post(f'/api/histories/{hid}/import-sessions')
sid = r.json()['id']
r = c.post(f'/api/histories/{hid}/import-sessions/{sid}/events', json={
    'events': [
        {'replica_id': 'r1', 'seq': 3, 'key': 'name', 'put': 'c',
         'clock': {'r1': 3, 'r2': 0}},
    ],
})
assert r.status_code == 200, r.text
print(hid, sid)
"""
    )
    assert create.returncode == 0, create.stderr
    hid, sid = create.stdout.split()

    # Process B (fresh import, same file): the open session is still there,
    # accepts more events, and commits.
    resume = run(
        f"""
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.get('/api/histories/{hid}/import-sessions/{sid}')
assert r.status_code == 200, r.text
data = r.json()
assert data['status'] == 'open'
assert [(e['replica_id'], e['seq']) for e in data['events']] == [('r1', 3)]

r = c.post('/api/histories/{hid}/import-sessions/{sid}/events', json={{
    'events': [
        {{'replica_id': 'r1', 'seq': 2, 'key': 'other', 'put': 'b',
          'clock': {{'r1': 2, 'r2': 0}}}},
    ],
}})
assert r.status_code == 200, r.text
r = c.post('/api/histories/{hid}/import-sessions/{sid}/commit')
assert r.status_code == 200, r.text
assert [(e['replica_id'], e['seq']) for e in r.json()['events']] == [
    ('r1', 1), ('r1', 2), ('r1', 3)
]
r = c.get('/api/histories/{hid}/import-sessions/{sid}')
assert r.json()['status'] == 'closed'
print('ok')
"""
    )
    assert resume.returncode == 0, resume.stderr
    assert resume.stdout.strip() == "ok"


# ---------------------------------------------------------------------------
# Import session missing-ranges: what still has to come from the source
# ---------------------------------------------------------------------------


def session_missing(hid, sid):
    return client.get(
        f"/api/histories/{hid}/import-sessions/{sid}/missing-ranges"
    )


def test_missing_ranges_empty_session_over_contiguous_history_is_empty():
    # No staged events at all: the bounds come solely from the committed
    # history, which (having passed validation) references only events it
    # contains, so every range is empty.
    hid = hist_create(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
        ],
    ).json()["id"]
    sid = create_session(hid)

    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    assert r.json() == {"missing_ranges": []}

    # The clock-component bound is exercised by staged events below.


def test_missing_ranges_gaps_and_ranges_sorted_within_and_across_replicas():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)

    # r1: has 1 (committed) and 3 (staged); staged r2/1 knows r1 up to 4.
    # Required bounds: r1 -> 4 (clock component), r2 -> 1 (own seq).
    # Missing r1 seqs: 2, 4 -> ranges [2,2] and [4,4]; r2: none.
    assert (
        session_append(
            hid,
            sid,
            [
                ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 0}),
                ev("r2", 1, "k", put="b", clock={"r1": 4, "r2": 1}),
            ],
        ).status_code
        == 200
    )
    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    assert r.json() == {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 2, "end_seq": 2},
            {"replica_id": "r1", "start_seq": 4, "end_seq": 4},
        ]
    }

    # Coalescing: fill r1/2; the prefix up to the bound now lacks 4 only, and
    # staging r1/4 coalesces nothing further.  Stage r1/2 first.
    assert (
        session_append(
            hid, sid, [ev("r1", 2, "k", put="b2", clock={"r1": 2, "r2": 0})]
        ).status_code
        == 200
    )
    r = session_missing(hid, sid)
    assert r.json() == {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 4, "end_seq": 4}
        ]
    }

    # A long contiguous hole is one closed range, not an enumeration.
    sid2 = create_session(hid)
    assert (
        session_append(
            hid,
            sid2,
            [ev("r2", 1, "k", put="b", clock={"r1": 10, "r2": 1})],
        ).status_code
        == 200
    )
    r = session_missing(hid, sid2)
    assert r.json() == {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 2, "end_seq": 10}
        ]
    }


def test_missing_ranges_own_tail_sets_bound_beyond_clock_components():
    # r1/3 staged with clocks only at r1=3,r2=0; nothing references higher
    # seqs.  Its own seq still requires r1/1..3, and r1/1 is committed, so
    # the missing range is [2,3].
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid, sid, [ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 0})]
        ).status_code
        == 200
    )
    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    assert r.json() == {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 2, "end_seq": 2}
        ]
    }


def test_missing_ranges_bound_zero_replica_yields_nothing():
    hid = hist_create(
        ["r1", "r2", "r3"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0, "r3": 0})],
    ).json()["id"]
    sid = create_session(hid)
    # Nothing references r2 or r3: their bound is 0 and they get no range,
    # even though they have zero events.
    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    assert r.json() == {"missing_ranges": []}


def test_missing_ranges_huge_seq_is_one_range_and_one_identity():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    huge = 10**12
    # A single staged event on r2 claims to know r1 up to 10^12.
    assert (
        session_append(
            hid,
            sid,
            [ev("r2", 1, "k", put="b", clock={"r1": huge, "r2": 1})],
        ).status_code
        == 200
    )
    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data == {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 2, "end_seq": huge}
        ]
    }
    # The missing seqs are never enumerated: exactly one range.
    assert len(data["missing_ranges"]) == 1


def test_missing_ranges_identical_committed_and_staged_content_is_complete():
    hid = create_conflict_history()
    sid = create_session(hid)
    # Re-stage both committed events verbatim: identities merge, no gaps.
    assert session_append(hid, sid, two_branch_conflict()).status_code == 200
    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    assert r.json() == {"missing_ranges": []}


def test_missing_ranges_full_identities_do_not_guarantee_commit():
    # Empty ranges mean identities are complete -- not that the clocks are
    # causally consistent.  Here every seq 1..3 on r1 and 1..2 on r2 is
    # present, but the predecessor r1/2 (clock {r1:2,r2:1}) is not <=
    # r1/3's clock {r1:3,r2:0}: the gap query returns 200 + [], the later
    # commit returns 422.
    hid = hist_create(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
        ],
    ).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [
                ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
                ev("r2", 2, "k", put="d", clock={"r1": 1, "r2": 2}),
                ev("r1", 3, "k", put="e", clock={"r1": 3, "r2": 0}),
            ],
        ).status_code
        == 200
    )
    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    assert r.json() == {"missing_ranges": []}

    r = session_commit(hid, sid)
    assert r.status_code == 422, r.text
    # Query and failed commit leave everything in place.
    assert session_get(hid, sid).json()["status"] == "open"
    assert len(hist_get(hid).json()["events"]) == 2


def test_missing_ranges_staged_vs_committed_different_content_is_409():
    hid = create_conflict_history()
    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [
                # Same identity as committed r1/1, different value.
                ev("r1", 1, "name", put="different", clock={"r1": 1, "r2": 0}),
                # Plus a genuinely new event; it must not silence the 409.
                ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
            ],
        ).status_code
        == 200
    )
    before_hist = hist_get(hid).json()
    before_session = session_get(hid, sid).json()

    r = session_missing(hid, sid)
    assert r.status_code == 409, r.text
    body = r.json()
    # No range list is returned on conflict.
    assert "missing_ranges" not in body
    (err,) = body["detail"]
    assert err["code"] == "event_content_mismatch"
    assert err["replica_id"] == "r1"
    assert err["seq"] == 1

    # Read-only: history and staging area unchanged.
    assert hist_get(hid).json() == before_hist
    assert session_get(hid, sid).json() == before_session


def test_missing_ranges_all_conflicts_listed_and_sorted_by_replica_then_seq():
    # Committed r1/1, r1/2, r2/1; stage mismatching copies of r1/2 and r2/1
    # and r1/1 (arrival order deliberately unsorted).
    seed = [
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [
                ev("r1", 2, "k", put="c-X", clock={"r1": 2, "r2": 1}),
                ev("r2", 1, "k", put="b-X", clock={"r1": 1, "r2": 1}),
                ev("r1", 1, "k", put="a-X", clock={"r1": 1, "r2": 0}),
            ],
        ).status_code
        == 200
    )
    r = session_missing(hid, sid)
    assert r.status_code == 409, r.text
    conflicts = [
        (e["replica_id"], e["seq"]) for e in r.json()["detail"]
    ]
    assert conflicts == [("r1", 1), ("r1", 2), ("r2", 1)]
    assert all(e["code"] == "event_content_mismatch" for e in r.json()["detail"])


def test_missing_ranges_closed_session_is_still_queryable():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid, sid, [ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 0})]
        ).status_code
        == 200
    )
    # Gaps visible while open: r1/2 missing.
    r = session_missing(hid, sid)
    assert r.json() == {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 2, "end_seq": 2}
        ]
    }
    # Commit fails (gap) -> session stays open.  Fill it and commit to close.
    assert (
        session_append(
            hid, sid, [ev("r1", 2, "k", put="b", clock={"r1": 2, "r2": 0})]
        ).status_code
        == 200
    )
    assert session_commit(hid, sid).status_code == 200
    assert session_get(hid, sid).json()["status"] == "closed"

    # The closed session is still queryable; its staged identities are now
    # all committed, so the ranges are empty.
    r = session_missing(hid, sid)
    assert r.status_code == 200, r.text
    assert r.json() == {"missing_ranges": []}


def test_missing_ranges_unknown_history_or_session_is_404():
    missing_hid = "0" * 32
    r = session_missing(missing_hid, "1" * 32)
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"

    hid = create_conflict_history()
    r = session_missing(hid, "1" * 32)
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "session_not_found"

    # A session id of another history reads as missing.
    other_hid = create_conflict_history()
    sid = create_session(other_hid)
    r = session_missing(hid, sid)
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "session_not_found"


def test_missing_ranges_is_read_only_under_repeated_queries():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid, sid, [ev("r2", 1, "k", put="b", clock={"r1": 5, "r2": 1})]
        ).status_code
        == 200
    )
    expected = {
        "missing_ranges": [
            {"replica_id": "r1", "start_seq": 2, "end_seq": 5}
        ]
    }
    for _ in range(3):
        assert session_missing(hid, sid).json() == expected
    # Nothing moved into the history, the session stayed open and staged.
    assert len(hist_get(hid).json()["events"]) == 1
    assert session_get(hid, sid).json()["status"] == "open"
    assert len(session_get(hid, sid).json()["events"]) == 1


# ---------------------------------------------------------------------------
# Evidence batch: key-selected, independently replayable causal closure
# ---------------------------------------------------------------------------


def hist_evidence(hid, keys):
    return client.post(
        f"/api/histories/{hid}/evidence-batch", json={"keys": keys}
    )


def evidence_seed_events():
    # r1/1 writes key a; r2/1 has synced r1/1 but writes an auxiliary key
    # (cross-key dependency); r1/2 depends on r2/1 and its own predecessor and
    # overwrites a with a delete; r3/1 is a causally unrelated concurrent
    # write on another key.
    return [
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "aux", put="y", clock={"r1": 1, "r2": 1, "r3": 0}),
        ev("r1", 2, "a", delete="delete", clock={"r1": 2, "r2": 1, "r3": 0}),
        ev("r3", 1, "other", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]


def test_evidence_batch_includes_all_key_events_with_cross_key_closure():
    hid = hist_create(["r1", "r2", "r3"], evidence_seed_events()).json()["id"]

    r = hist_evidence(hid, ["a"])
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["replica_ids"] == ["r1", "r2", "r3"]
    # Both puts/deletes touching a are present, including the overwritten
    # r1/1 put; the cross-key dependency r2/1 is carried as evidence; the
    # unrelated concurrent r3/1 is outside the closure and excluded.
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    # Original wire content (op kind, value, clock) and key are verbatim.
    assert data["events"][0] == ev(
        "r1", 1, "a", put="x", clock={"r1": 1, "r2": 0, "r3": 0}
    )
    assert data["events"][1] == ev(
        "r1", 2, "a", delete="delete", clock={"r1": 2, "r2": 1, "r3": 0}
    )
    assert data["events"][2] == ev(
        "r2", 1, "aux", put="y", clock={"r1": 1, "r2": 1, "r3": 0}
    )


def test_evidence_batch_events_sorted_and_no_seq_or_clock_renumbering():
    hid = hist_create(
        ["r1", "r2", "r3"],
        list(reversed(evidence_seed_events())),  # creation order irrelevant
    ).json()["id"]

    data = hist_evidence(hid, ["a"]).json()
    idents = [(e["replica_id"], e["seq"]) for e in data["events"]]
    assert idents == sorted(idents)
    # seqs stay 1,2 for r1 (not renumbered to a dense per-key sequence) and
    # the zero r3 clock component is retained verbatim.
    assert data["events"][0]["clock"] == {"r1": 1, "r2": 0, "r3": 0}
    assert data["events"][1]["seq"] == 2


def test_evidence_batch_replays_through_resolve_and_matches_key_verdict():
    seed = evidence_seed_events()
    hid = hist_create(["r1", "r2", "r3"], seed).json()["id"]
    batch = hist_evidence(hid, ["a"]).json()

    # The returned body is itself a legal resolve request.
    rr = client.post("/api/resolve", json=batch)
    assert rr.status_code == 200, rr.text
    results = {x["key"]: x for x in rr.json()["results"]}
    # The selected key resolves exactly as over the full history.
    full = {x["key"]: x for x in hist_get(hid).json()["results"]}
    assert results["a"] == full["a"]
    assert results["a"]["state"] == "deleted"
    # The auxiliary key is present only as dependency evidence; its verdict
    # in the batch is allowed to differ from (here: equal to) the history's.
    assert "aux" in results
    # The excluded concurrent key never appears.
    assert "other" not in results


def test_evidence_batch_replays_through_catchup_by_dependency_rounds():
    hid = hist_create(["r1", "r2", "r3"], evidence_seed_events()).json()["id"]
    batch = hist_evidence(hid, ["a"]).json()

    cr = client.post(
        "/api/catchup",
        json={
            **batch,
            "known_clock": {"r1": 0, "r2": 0, "r3": 0},
        },
    )
    assert cr.status_code == 200, cr.text
    rounds = cr.json()["rounds"]
    # r2/1 needs r1/1, and r1/2 needs both: an earlier round unlocks the next.
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1)],
        [("r2", 1)],
        [("r1", 2)],
    ]


def test_evidence_batch_overwritten_puts_and_deletes_all_extracted():
    seed = [
        ev("r1", 1, "k", put="v1", clock={"r1": 1, "r2": 0}),
        ev("r1", 2, "k", delete="delete", clock={"r1": 2, "r2": 0}),
        ev("r1", 3, "k", put="v3", clock={"r1": 3, "r2": 0}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    data = hist_evidence(hid, ["k"]).json()
    assert [
        (e["replica_id"], e["seq"], e.get("put"), "delete" in e)
        for e in data["events"]
    ] == [
        ("r1", 1, "v1", False),
        ("r1", 2, None, True),
        ("r1", 3, "v3", False),
    ]
    rr = client.post("/api/resolve", json=data)
    assert rr.status_code == 200
    (result,) = rr.json()["results"]
    assert result["state"] == "put" and result["value"] == "v3"
    # Full history agrees.
    (full_result,) = hist_get(hid).json()["results"]
    assert full_result["state"] == "put" and full_result["value"] == "v3"


def test_evidence_batch_multiple_selected_keys_share_dependencies():
    seed = [
        ev("r1", 1, "a", put="a1", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "b", put="b1", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "c", put="c1", clock={"r1": 2, "r2": 1}),
        ev("r2", 2, "d", put="d1", clock={"r1": 2, "r2": 2}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    data = hist_evidence(hid, ["b", "d"]).json()
    # Both keys' events plus the transitive cross-key chain r1/1 <- r1/2...
    # r2/1 depends on r1/1 (auxiliary key a); r2/2 depends on r1/2 (key c,
    # itself auxiliary here), whose predecessor chain already covers r1/1.
    assert [(e["replica_id"], e["seq"], e["key"]) for e in data["events"]] == [
        ("r1", 1, "a"),
        ("r1", 2, "c"),
        ("r2", 1, "b"),
        ("r2", 2, "d"),
    ]
    results = {
        x["key"]: x
        for x in client.post("/api/resolve", json=data).json()["results"]
    }
    full = {x["key"]: x for x in hist_get(hid).json()["results"]}
    for key in ["b", "d"]:
        assert results[key] == full[key]
    # Auxiliary keys a and c ride along only as evidence.
    assert {"a", "c"} <= set(results)


def test_evidence_batch_auxiliary_key_verdict_may_differ():
    # Auxiliary key aux has two concurrent events: r2/1 (pulled in as a
    # dependency of the selected key a) and r3/1 (only relevant to aux and
    # therefore excluded).  Over the full history aux conflicts; in the
    # evidence batch it resolves to r2/1 alone, while a matches the history.
    seed = [
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "aux", put="y", clock={"r1": 1, "r2": 1, "r3": 0}),
        ev("r1", 2, "a", put="x2", clock={"r1": 2, "r2": 1, "r3": 0}),
        ev("r3", 1, "aux", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = hist_create(["r1", "r2", "r3"], seed).json()["id"]
    full = {x["key"]: x for x in hist_get(hid).json()["results"]}
    assert full["aux"]["state"] == "conflict"

    batch = hist_evidence(hid, ["a"]).json()
    assert [(e["replica_id"], e["seq"]) for e in batch["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    results = {
        x["key"]: x
        for x in client.post("/api/resolve", json=batch).json()["results"]
    }
    # Selected key: identical verdict to the full history.
    assert results["a"] == full["a"]
    # Auxiliary key: its evidence-only verdict is allowed to differ.
    assert results["aux"]["state"] == "put"
    assert results["aux"]["value"] == "y"


def test_evidence_batch_concurrent_unrelated_event_is_not_pulled_in():
    hid = hist_create(["r1", "r2", "r3"], evidence_seed_events()).json()["id"]

    data = hist_evidence(hid, ["a"]).json()
    assert all(
        not (e["replica_id"] == "r3" and e["seq"] == 1)
        for e in data["events"]
    )

    # Selecting the concurrent key returns just that event (no closure).
    data = hist_evidence(hid, ["other"]).json()
    assert data["events"] == [
        ev("r3", 1, "other", put="z", clock={"r1": 0, "r2": 0, "r3": 1})
    ]
    rr = client.post("/api/resolve", json=data)
    assert rr.status_code == 200


def test_evidence_batch_of_conflicting_key_replays_through_resolve_conflict():
    hid = create_conflict_history()
    batch = hist_evidence(hid, ["name"]).json()

    # Same replayable request shape as the stateless conflict endpoint.
    body = {
        "replica_ids": batch["replica_ids"],
        "events": batch["events"],
        "key": "name",
        "replica_id": "r1",
        "versions": [
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        "put": "chosen",
    }
    r = client.post("/api/resolve-conflict", json=body)
    assert r.status_code == 200, r.text
    assert r.json()["event"]["clock"] == {"r1": 2, "r2": 1}


def test_evidence_batch_after_merge_and_conflict_commit_covers_resolution():
    hid = create_conflict_history()
    assert hist_commit(
        hid,
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        put="chosen",
    ).status_code == 200
    # A later offline write on the same selected key (r2/2 deletes name)
    # merges in after the resolution; its clock knows r1 = 2, so selecting
    # name must pull the committed resolution r1/2 in as a dependency.
    assert hist_merge(
        hid,
        [ev("r2", 2, "name", delete="delete", clock={"r1": 2, "r2": 2})],
    ).status_code == 200

    batch = hist_evidence(hid, ["name"]).json()
    assert [(e["replica_id"], e["seq"]) for e in batch["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
        ("r2", 2),
    ]
    results = {
        x["key"]: x
        for x in client.post("/api/resolve", json=batch).json()["results"]
    }
    full = {x["key"]: x for x in hist_get(hid).json()["results"]}
    assert results["name"] == full["name"]
    assert results["name"]["state"] == "deleted"
    assert results["name"]["event"]["replica_id"] == "r2"
    assert results["name"]["event"]["seq"] == 2


def test_evidence_batch_unknown_history_is_404():
    r = hist_evidence("0" * 32, ["k"])
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"


def test_evidence_batch_unknown_key_is_404_listing_each_missing_key():
    seed = [
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "b", put="y", clock={"r1": 0, "r2": 1}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    r = hist_evidence(hid, ["nope"])
    assert r.status_code == 404, r.text
    (err,) = r.json()["detail"]
    assert err["code"] == "key_not_found"
    assert err["key"] == "nope"

    # Several missing keys are all reported, sorted by key; one existing key
    # mixed in still yields 404.
    r = hist_evidence(hid, ["zzz", "a", "mmm"])
    assert r.status_code == 404
    errs = r.json()["detail"]
    assert [e["key"] for e in errs] == ["mmm", "zzz"]
    assert all(e["code"] == "key_not_found" for e in errs)


def test_evidence_batch_illegal_keys_are_422():
    hid = create_conflict_history()

    # Missing keys field.
    r = client.post(f"/api/histories/{hid}/evidence-batch", json={})
    assert r.status_code == 422
    # Empty list.
    r = hist_evidence(hid, [])
    assert r.status_code == 422
    # Empty-string and non-string elements.
    r = hist_evidence(hid, [""])
    assert r.status_code == 422
    r = client.post(
        f"/api/histories/{hid}/evidence-batch", json={"keys": ["a", 1]}
    )
    assert r.status_code == 422
    # Duplicate keys -> semantic duplicate_key 422.
    r = hist_evidence(hid, ["name", "name"])
    assert_422(r, "duplicate_key")
    r = hist_evidence(hid, ["b", "a", "b"])
    assert_422(r, "duplicate_key")
    # Extra fields are forbidden.
    r = client.post(
        f"/api/histories/{hid}/evidence-batch",
        json={"keys": ["name"], "extra": 1},
    )
    assert r.status_code == 422


def test_evidence_batch_illegal_keys_422_takes_precedence_over_missing_history():
    r = hist_evidence("0" * 32, ["a", "a"])
    assert r.status_code == 422
    assert any(e.get("code") == "duplicate_key" for e in r.json()["detail"])


def test_evidence_batch_is_read_only_and_repeatable():
    seed = evidence_seed_events()
    hid = hist_create(["r1", "r2", "r3"], seed).json()["id"]
    before = hist_get(hid).json()

    first = hist_evidence(hid, ["a"])
    assert first.status_code == 200
    assert hist_evidence(hid, ["a"]).json() == first.json()
    # Repeated queries, including failing ones, leave the history untouched.
    hist_evidence(hid, ["missing"])
    hist_evidence(hid, ["a", "a"])
    assert hist_get(hid).json() == before


def test_evidence_batch_does_not_affect_sessions():
    hid = create_conflict_history()
    sid = create_session(hid)
    assert (
        session_append(
            hid, sid, [ev("r1", 2, "x", put="z", clock={"r1": 2, "r2": 1})]
        ).status_code
        == 200
    )
    staged_before = session_get(hid, sid).json()

    hist_evidence(hid, ["name"])
    assert session_get(hid, sid).json() == staged_before
    assert session_get(hid, sid).json()["status"] == "open"


def test_evidence_batch_selecting_all_key_events_matches_full_history_verdicts():
    seed = evidence_seed_events()
    hid = hist_create(["r1", "r2", "r3"], seed).json()["id"]

    # Selecting every key closes over every event (the full history).
    data = hist_evidence(hid, ["a", "aux", "other"]).json()
    assert len(data["events"]) == 4
    rr = client.post("/api/resolve", json=data)
    assert rr.status_code == 200
    assert rr.json()["results"] == hist_get(hid).json()["results"]


def test_evidence_batch_replica_ids_keep_eventless_replica_and_clock_components():
    # r3 never appears in the closure, but its original replica_ids slot and
    # the zero clock components must survive so validation still passes.
    hid = hist_create(["r1", "r2", "r3"], evidence_seed_events()).json()["id"]
    data = hist_evidence(hid, ["a"]).json()
    assert data["replica_ids"] == ["r1", "r2", "r3"]
    assert all(set(e["clock"]) == {"r1", "r2", "r3"} for e in data["events"])
    assert client.post("/api/resolve", json=data).status_code == 200


# ---------------------------------------------------------------------------
# Event relation: causal explanation between two events of one history
# ---------------------------------------------------------------------------


def create_relation_history():
    """Causal chain r1/1 -> r2/1 -> r1/2 -> r2/2 across keys a/dep, plus a
    fully concurrent r3/1 on key other."""
    r = hist_create(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "a", put="a1", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "dep", put="d1", clock={"r1": 1, "r2": 1, "r3": 0}),
            ev("r1", 2, "a", put="a2", clock={"r1": 2, "r2": 1, "r3": 0}),
            ev("r2", 2, "dep", put="d2", clock={"r1": 2, "r2": 2, "r3": 0}),
            ev("r3", 1, "other", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
        ],
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_event_relation_before_cross_key_direct_edge():
    hid = create_relation_history()

    r = hist_event_relation(
        hid, {"replica_id": "r2", "seq": 1}, {"replica_id": "r1", "seq": 2}
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == hid
    assert data["relation"] == "before"
    # r1/2's clock component r2 = 1 points directly at r2/1 (cross-key).
    assert data["chain"] == [
        {"replica_id": "r2", "seq": 1},
        {"replica_id": "r1", "seq": 2},
    ]


def test_event_relation_after_chain_still_runs_earlier_to_later():
    hid = create_relation_history()

    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["relation"] == "after"
    # Direction is anchored at first, but the chain always goes earlier->later.
    assert data["chain"] == [
        {"replica_id": "r2", "seq": 1},
        {"replica_id": "r1", "seq": 2},
    ]


def test_event_relation_chain_prefers_fewest_edges():
    hid = create_relation_history()

    # r1/2 is reachable from r1/1 directly (own seq - 1) and via r2/1; the
    # one-edge chain wins.
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r1", "seq": 2}
    )
    assert r.status_code == 200, r.text
    assert r.json()["relation"] == "before"
    assert r.json()["chain"] == [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r1", "seq": 2},
    ]


def test_event_relation_chain_tie_broken_by_smallest_identity_sequence():
    hid = create_relation_history()

    # r1/1 -> r1/2 -> r2/2 and r1/1 -> r2/1 -> r2/2 are both two-edge chains;
    # the lexicographically smallest (replica_id, seq) sequence wins.
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 2}
    )
    assert r.status_code == 200, r.text
    assert r.json()["relation"] == "before"
    assert r.json()["chain"] == [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r1", "seq": 2},
        {"replica_id": "r2", "seq": 2},
    ]


def test_event_relation_same_event_is_single_node_chain():
    hid = create_relation_history()

    r = hist_event_relation(
        hid, {"replica_id": "r2", "seq": 1}, {"replica_id": "r2", "seq": 1}
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["relation"] == "same"
    assert data["chain"] == [{"replica_id": "r2", "seq": 1}]
    assert "first_ahead" not in data
    assert "second_ahead" not in data


def test_event_relation_concurrent_equal_values_empty_chain_and_ahead_components():
    # Same value on both replicas: still concurrent, still evidenced.
    r = hist_create(
        ["r1", "r2"],
        [
            ev("r2", 1, "name", put="alice", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 201, r.text
    hid = r.json()["id"]

    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["relation"] == "concurrent"
    assert data["chain"] == []
    assert data["first_ahead"] == {
        "replica_id": "r1",
        "first_value": 1,
        "second_value": 0,
    }
    assert data["second_ahead"] == {
        "replica_id": "r2",
        "first_value": 0,
        "second_value": 1,
    }


def test_event_relation_concurrent_ahead_component_picks_smallest_replica_id():
    r = hist_create(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1, "r3": 0}),
            ev("r3", 1, "k", put="d", clock={"r1": 1, "r2": 0, "r3": 1}),
        ],
    )
    assert r.status_code == 201, r.text
    hid = r.json()["id"]

    # first (r1/2, clock {2,1,0}) is ahead of second (r3/1, clock {1,0,1}) on
    # both r1 and r2; the smallest replica ID is reported.
    data = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 2}, {"replica_id": "r3", "seq": 1}
    ).json()
    assert data["relation"] == "concurrent"
    assert data["first_ahead"] == {
        "replica_id": "r1",
        "first_value": 2,
        "second_value": 1,
    }
    assert data["second_ahead"] == {
        "replica_id": "r3",
        "first_value": 0,
        "second_value": 1,
    }

    # Swapping first/second mirrors the evidence values.
    data = hist_event_relation(
        hid, {"replica_id": "r3", "seq": 1}, {"replica_id": "r1", "seq": 2}
    ).json()
    assert data["relation"] == "concurrent"
    assert data["first_ahead"] == {
        "replica_id": "r3",
        "first_value": 1,
        "second_value": 0,
    }
    assert data["second_ahead"] == {
        "replica_id": "r1",
        "first_value": 1,
        "second_value": 2,
    }


def test_event_relation_unknown_history_is_404():
    r = hist_event_relation(
        "0" * 32,
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r2", "seq": 1},
    )
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"


def test_event_relation_unknown_event_is_404():
    hid = create_relation_history()

    # first is checked first.
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 9}, {"replica_id": "r2", "seq": 9}
    )
    assert r.status_code == 404
    detail = r.json()["detail"][0]
    assert detail["code"] == "event_not_found"
    assert "r1" in detail["message"] and "9" in detail["message"]

    # A replica outside the history's replica set is also event_not_found.
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r9", "seq": 1}
    )
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "event_not_found"


def test_event_relation_invalid_identifiers_are_422():
    hid = create_relation_history()
    ok_first = {"replica_id": "r1", "seq": 1}
    ok_second = {"replica_id": "r2", "seq": 1}

    bad_bodies = [
        # empty replica_id
        {"first": {"replica_id": "", "seq": 1}, "second": ok_second},
        # seq not a positive integer
        {"first": {"replica_id": "r1", "seq": 0}, "second": ok_second},
        {"first": {"replica_id": "r1", "seq": -2}, "second": ok_second},
        # booleans are rejected, not coerced to 0/1
        {"first": {"replica_id": "r1", "seq": True}, "second": ok_second},
        {"first": ok_first, "second": {"replica_id": "r2", "seq": False}},
        # strings/floats are rejected
        {"first": {"replica_id": "r1", "seq": "1"}, "second": ok_second},
        {"first": {"replica_id": "r1", "seq": 1.0}, "second": ok_second},
        # missing / extra fields
        {"first": ok_first},
        {"first": ok_first, "second": ok_second, "third": ok_first},
        {"first": {"replica_id": "r1", "seq": 1, "key": "a"},
         "second": ok_second},
        # identifiers must be objects
        {"first": ["r1", 1], "second": ok_second},
    ]
    for body in bad_bodies:
        r = client.post(f"/api/histories/{hid}/event-relation", json=body)
        assert r.status_code == 422, (body, r.status_code, r.text)

    # Schema validation precedes the history lookup: 422 even for unknown IDs.
    r = client.post(
        f"/api/histories/{'0' * 32}/event-relation",
        json={"first": {"replica_id": "r1", "seq": True}, "second": ok_second},
    )
    assert r.status_code == 422


def test_event_relation_is_read_only_and_ignores_staged_events():
    r = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    )
    assert r.status_code == 201, r.text
    hid = r.json()["id"]
    before = hist_get(hid).json()

    # Stage r1/2 in an import session without committing it.
    sid = create_session(hid)
    r = session_append(
        hid,
        sid,
        [ev("r1", 2, "k", put="b", clock={"r1": 2, "r2": 0})],
    )
    assert r.status_code == 200, r.text

    # The staged event is not part of the committed history: 404.
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r1", "seq": 2}
    )
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "event_not_found"

    # Committed events answer normally and nothing is modified.
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r1", "seq": 1}
    )
    assert r.status_code == 200
    assert r.json()["relation"] == "same"
    assert hist_get(hid).json() == before
    staged = session_get(hid, sid).json()
    assert staged["status"] == "open"
    assert [(e["replica_id"], e["seq"]) for e in staged["events"]] == [
        ("r1", 2)
    ]

    # After the session commits, the same query sees the new event.
    assert session_commit(hid, sid).status_code == 200
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r1", "seq": 2}
    )
    assert r.status_code == 200
    assert r.json()["relation"] == "before"
    assert r.json()["chain"] == [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r1", "seq": 2},
    ]


# ---------------------------------------------------------------------------
# Event relation regression: shortest chain at long/short path convergence,
# independent of set iteration order / hash seed
# ---------------------------------------------------------------------------


def converging_history_events():
    """The five puts from the convergence report (clock shown as (r1, r2)):

    r1/1=(1,0), r2/1=(1,1), r1/2=(2,1), r2/2=(2,2), r2/3=(2,3).

    From r1/1 to r2/3 a long path r1/1 -> r2/1 -> r1/2 -> r2/2 -> r2/3
    (four edges) and the short path r1/1 -> r1/2 -> r2/3 (two edges)
    converge at r1/2 and again at r2/3.
    """

    return [
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
        ev("r2", 2, "k", put="d", clock={"r1": 2, "r2": 2}),
        ev("r2", 3, "k", put="e", clock={"r1": 2, "r2": 3}),
    ]


def create_converging_history():
    r = hist_create(["r1", "r2"], converging_history_events())
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_event_relation_converging_paths_pick_short_chain_earlier_to_later():
    hid = create_converging_history()

    # first=r1/1, second=r2/3 -> before with the two-edge short chain,
    # not the four-edge detour through r2/1 and r2/2.
    r = hist_event_relation(
        hid, {"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 3}
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["relation"] == "before"
    assert data["chain"] == [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r1", "seq": 2},
        {"replica_id": "r2", "seq": 3},
    ]

    # Swapped identifiers -> after, but the chain still runs earlier->later.
    r = hist_event_relation(
        hid, {"replica_id": "r2", "seq": 3}, {"replica_id": "r1", "seq": 1}
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["relation"] == "after"
    assert data["chain"] == [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r1", "seq": 2},
        {"replica_id": "r2", "seq": 3},
    ]


def _event(rid, seq, clock):
    return Event(
        replica_id=rid, seq=seq, key="k", put="v", clock=dict(clock)
    )


def _oracle_shortest_chain(replica_ids, events, start, end):
    """Independent exhaustive oracle: lexicographically smallest simple
    dependency path under (edges first, then the (rid, int seq) tuple
    sequence)."""

    by_id = {(e.replica_id, e.seq): e for e in events}
    dependents = {}
    for e in events:
        for dep in _direct_dependencies(e, replica_ids):
            dependents.setdefault(dep, set()).add((e.replica_id, e.seq))

    best = None

    def walk(node, path):
        nonlocal best
        if node == end:
            candidate = tuple(path)
            key = (len(candidate) - 1, candidate)
            if best is None or key < best[0]:
                best = (key, candidate)
            return
        for succ in dependents.get(node, ()):
            if succ not in path:
                path.append(succ)
                walk(succ, path)
                path.pop()

    walk(start, [start])
    assert best is not None
    assert end in by_id and start in by_id
    return list(best[1])


def test_shortest_chain_matches_exhaustive_oracle_on_converging_histories():
    # Two-replica convergence from the report, with the events handed in in
    # every order -- the answer must never depend on list/set ordering.
    rids2 = ["r1", "r2"]
    events2 = [_event(rid, seq, clock) for rid, seq, clock in [
        ("r1", 1, {"r1": 1, "r2": 0}),
        ("r2", 1, {"r1": 1, "r2": 1}),
        ("r1", 2, {"r1": 2, "r2": 1}),
        ("r2", 2, {"r1": 2, "r2": 2}),
        ("r2", 3, {"r1": 2, "r2": 3}),
    ]]

    # A three-replica diamond: r1/1 fans out to r2/1 and r3/1, both reconverge
    # at r1/2; r2/2 depends on both r1/2 and r3/1, giving a second junction.
    rids3 = ["r1", "r2", "r3"]
    events3 = [_event(rid, seq, clock) for rid, seq, clock in [
        ("r1", 1, {"r1": 1, "r2": 0, "r3": 0}),
        ("r2", 1, {"r1": 1, "r2": 1, "r3": 0}),
        ("r3", 1, {"r1": 1, "r2": 0, "r3": 1}),
        ("r1", 2, {"r1": 2, "r2": 1, "r3": 1}),
        ("r2", 2, {"r1": 2, "r2": 2, "r3": 1}),
    ]]

    import itertools

    for replica_ids, base_events in [(rids2, events2), (rids3, events3)]:
        idents = [(e.replica_id, e.seq) for e in base_events]
        for order in itertools.permutations(base_events):
            events = list(order)
            by_id = {(e.replica_id, e.seq): e for e in events}
            for start in idents:
                for end in idents:
                    if start == end:
                        continue
                    a, b = by_id[start], by_id[end]
                    reachable = all(
                        a.clock[r] <= b.clock[r] for r in replica_ids
                    ) and any(a.clock[r] < b.clock[r] for r in replica_ids)
                    if not reachable:
                        continue
                    got = _shortest_dependency_chain(
                        replica_ids, events, start, end
                    )
                    expected = _oracle_shortest_chain(
                        replica_ids, events, start, end
                    )
                    assert got == expected, (replica_ids, start, end, got, expected)


def test_event_relation_converging_chain_stable_across_hash_seeds(tmp_path):
    """The reported symptom was order dependence: run the converging query in
    fresh interpreters under several PYTHONHASHSEED values and demand the same
    short chain every time."""

    db = tmp_path / "history.db"
    setup = """
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'k', 'put': 'a',
         'clock': {'r1': 1, 'r2': 0}},
        {'replica_id': 'r2', 'seq': 1, 'key': 'k', 'put': 'b',
         'clock': {'r1': 1, 'r2': 1}},
        {'replica_id': 'r1', 'seq': 2, 'key': 'k', 'put': 'c',
         'clock': {'r1': 2, 'r2': 1}},
        {'replica_id': 'r2', 'seq': 2, 'key': 'k', 'put': 'd',
         'clock': {'r1': 2, 'r2': 2}},
        {'replica_id': 'r2', 'seq': 3, 'key': 'k', 'put': 'e',
         'clock': {'r1': 2, 'r2': 3}},
    ],
})
print(r.json()['id'])
"""
    setup_proc = subprocess.run(
        [sys.executable, "-c", setup],
        env=dict(
            os.environ,
            HISTORY_DB_PATH=str(db),
            PYTHONPATH="/workspace",
            PYTHONHASHSEED="0",
        ),
        capture_output=True,
        text=True,
    )
    assert setup_proc.returncode == 0, setup_proc.stderr
    hid = setup_proc.stdout.strip()

    query = f"""
from fastapi.testclient import TestClient
from app.main import app
import json

c = TestClient(app)
r = c.post('/api/histories/{hid}/event-relation', json={{
    'first': {{'replica_id': 'r1', 'seq': 1}},
    'second': {{'replica_id': 'r2', 'seq': 3}},
}})
print(json.dumps(r.json(), sort_keys=True))
"""
    answers = set()
    for seed in ("0", "1", "2", "7", "42", "123456"):
        proc = subprocess.run(
            [sys.executable, "-c", query],
            env=dict(
                os.environ,
                HISTORY_DB_PATH=str(db),
                PYTHONPATH="/workspace",
                PYTHONHASHSEED=seed,
            ),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, (seed, proc.stderr)
        answers.add(proc.stdout.strip())

    assert len(answers) == 1
    data = json.loads(next(iter(answers)))
    assert data["id"] == hid
    assert data["relation"] == "before"
    assert data["chain"] == [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r1", "seq": 2},
        {"replica_id": "r2", "seq": 3},
    ]


# ---------------------------------------------------------------------------
# Key migration (/api/histories/{id}/migrate-key)
# ---------------------------------------------------------------------------


def hist_migrate(hid, writer, source, target, src_versions, tgt_versions):
    return client.post(
        f"/api/histories/{hid}/migrate-key",
        json={
            "replica_id": writer,
            "source_key": source,
            "target_key": target,
            "source_versions": src_versions,
            "target_versions": tgt_versions,
        },
    )


def migration_seed_history():
    """r1/1 puts name=alice; r2/1 (having seen r1/1) puts other=x."""
    events = [
        ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "other", put="x", clock={"r1": 1, "r2": 1}),
    ]
    r = hist_create(["r1", "r2"], events)
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_migration_appends_delete_then_put_and_returns_sorted_verdicts():
    hid = migration_seed_history()

    r = hist_migrate(
        hid, "r2", "name", "alias",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == hid

    # Two original wire-format events: delete source, then put target with
    # the source's value; the second continues the same writer's tail.
    assert data["events"] == [
        {
            "replica_id": "r2", "seq": 2, "key": "name",
            "delete": "delete", "clock": {"r1": 1, "r2": 2},
        },
        {
            "replica_id": "r2", "seq": 3, "key": "alias",
            "put": "alice", "clock": {"r1": 1, "r2": 3},
        },
    ]

    # The two verdicts are sorted by key: alias put, name deleted.
    assert [v["key"] for v in data["results"]] == ["alias", "name"]
    assert data["results"][0]["state"] == "put"
    assert data["results"][0]["value"] == "alice"
    assert data["results"][0]["event"]["seq"] == 3
    assert data["results"][1]["state"] == "deleted"
    assert data["results"][1]["event"]["seq"] == 2

    # Persisted: the committed history carries both events and the untouched
    # key 'other' still resolves to its old put.
    got = hist_get(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in got["events"]] == [
        ("r1", 1), ("r2", 1), ("r2", 2), ("r2", 3),
    ]
    results = {v["key"]: v for v in got["results"]}
    assert results["other"]["state"] == "put"
    assert results["other"]["value"] == "x"
    assert results["name"]["state"] == "deleted"
    assert results["alias"]["state"] == "put"
    assert results["alias"]["value"] == "alice"


def test_migration_events_replay_through_resolve():
    hid = migration_seed_history()
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 200, r.text
    new_events = r.json()["events"]

    got = hist_get(hid).json()
    replay = post(got["replica_ids"], got["events"])
    assert replay.status_code == 200, replay.text
    results = {v["key"]: v for v in replay.json()["results"]}
    assert results["name"]["state"] == "deleted"
    assert results["alias"]["state"] == "put"
    assert results["alias"]["value"] == "alice"
    # The generated events on their own are exactly the returned ones.
    assert new_events[0]["key"] == "name"
    assert new_events[1]["put"] == "alice"


def test_migration_events_replay_through_catchup_in_two_rounds():
    # Writer r2 already has r2/1, so the migration takes r2/2 then r2/3; the
    # put depends on the delete (same replica) and lands one round later.
    hid = migration_seed_history()
    r = hist_migrate(
        hid, "r2", "name", "alias",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 200, r.text
    delete_event, put_event = r.json()["events"]

    got = hist_get(hid).json()
    r = post_catchup(
        got["replica_ids"], got["events"],
        {"r1": 1, "r2": 1},
    )
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    assert [(e["replica_id"], e["seq"]) for e in rounds[0]] == [
        ("r2", 2)
    ]
    assert [(e["replica_id"], e["seq"]) for e in rounds[1]] == [
        ("r2", 3)
    ]
    assert rounds[0][0]["op"] == "delete"
    assert rounds[1][0]["op"] == "put"
    assert rounds[1][0]["value"] == "alice"
    assert delete_event["seq"] == 2 and put_event["seq"] == 3


def test_migration_writer_with_no_prior_events_starts_at_seq_one():
    # Only r1 has events; r2 is an eventless replica of the history.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0})],
    ).json()["id"]

    r = hist_migrate(
        hid, "r2", "name", "alias",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 200, r.text
    events = r.json()["events"]
    assert [(e["replica_id"], e["seq"]) for e in events] == [
        ("r2", 1), ("r2", 2),
    ]
    # The delete's clock covers the source version; the put covers the delete.
    assert events[0]["clock"] == {"r1": 1, "r2": 1}
    assert events[1]["clock"] == {"r1": 1, "r2": 2}


def test_migration_preserves_empty_string_value():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "name", put="", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 200, r.text
    events = r.json()["events"]
    assert events[1]["put"] == ""
    verdict = {v["key"]: v for v in r.json()["results"]}["alias"]
    assert verdict["state"] == "put"
    assert verdict["value"] == ""


def test_migration_onto_a_uniquely_deleted_target_uses_its_versions():
    # r1/1 puts src; r1/2 deletes tgt.  r2 (eventless) performs the move, and
    # the delete's clock must also cover the target's delete version r1/2.
    hid = hist_create(
        ["r1", "r2"],
        [
            ev("r1", 1, "src", put="v", clock={"r1": 1, "r2": 0}),
            ev("r1", 2, "tgt", delete="delete",
               clock={"r1": 2, "r2": 0}),
        ],
    ).json()["id"]

    r = hist_migrate(
        hid, "r2", "src", "tgt",
        [{"replica_id": "r1", "seq": 1}],
        [{"replica_id": "r1", "seq": 2}],
    )
    assert r.status_code == 200, r.text
    events = r.json()["events"]
    assert events[0]["clock"] == {"r1": 2, "r2": 1}
    assert events[1]["clock"] == {"r1": 2, "r2": 2}
    results = {v["key"]: v for v in r.json()["results"]}
    assert results["src"]["state"] == "deleted"
    assert results["tgt"]["state"] == "put"
    assert results["tgt"]["value"] == "v"


def test_migration_moves_the_top_put_when_older_events_are_dominated():
    # r1/1 put v, r2/1 deletes, r1/2 put v2 dominates both: the unique
    # maximal version is r1/2 with value v2.
    hid = hist_create(
        ["r1", "r2"],
        [
            ev("r1", 1, "name", put="v", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "name", delete="delete",
               clock={"r1": 1, "r2": 1}),
            ev("r1", 2, "name", put="v2", clock={"r1": 2, "r2": 1}),
        ],
    ).json()["id"]
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r1", "seq": 2}], [],
    )
    assert r.status_code == 200, r.text
    assert r.json()["events"][1]["put"] == "v2"
    # Stale versions pointing at the overwritten r1/1 are rejected with 409
    # (checked on a fresh history so the successful migration above has not
    # already deleted the source key).
    hid = hist_create(
        ["r1", "r2"],
        [
            ev("r1", 1, "name", put="v", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "name", delete="delete",
               clock={"r1": 1, "r2": 1}),
            ev("r1", 2, "name", put="v2", clock={"r1": 2, "r2": 1}),
        ],
    ).json()["id"]
    r = hist_migrate(
        hid, "r1", "name", "other",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "migration_versions_mismatch" for e in r.json()["detail"]
    )


def test_migration_equal_keys_is_422():
    hid = migration_seed_history()
    r = hist_migrate(
        hid, "r1", "name", "name",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 422
    assert any(
        e["code"] == "source_target_key_equal" for e in r.json()["detail"]
    )


def test_migration_unknown_writer_or_version_replica_is_422():
    hid = migration_seed_history()
    r = hist_migrate(
        hid, "r9", "name", "alias",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 422
    assert any(
        e["code"] == "unknown_replica_id" for e in r.json()["detail"]
    )

    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r9", "seq": 1}], [],
    )
    assert r.status_code == 422
    assert any(
        e["code"] == "unknown_replica_id" for e in r.json()["detail"]
    )


def test_migration_duplicate_versions_are_422():
    hid = migration_seed_history()
    for field in ("source_versions", "target_versions"):
        body = {
            "replica_id": "r1",
            "source_key": "name",
            "target_key": "alias",
            "source_versions": [{"replica_id": "r1", "seq": 1}],
            "target_versions": [],
        }
        body[field] = [
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r1", "seq": 1},
        ]
        r = client.post(f"/api/histories/{hid}/migrate-key", json=body)
        assert r.status_code == 422, (field, r.text)
        assert any(
            e["code"] == "duplicate_conflict_version"
            for e in r.json()["detail"]
        )


def test_migration_bad_schema_is_422():
    hid = migration_seed_history()
    url = f"/api/histories/{hid}/migrate-key"

    def code_status(body):
        r = client.post(url, json=body)
        return r.status_code

    base = {
        "replica_id": "r1", "source_key": "name", "target_key": "alias",
        "source_versions": [{"replica_id": "r1", "seq": 1}],
        "target_versions": [],
    }
    bad = [
        {**base, "source_key": ""},
        {**base, "target_key": ""},
        {**base, "replica_id": ""},
        {**base, "source_versions": [{"replica_id": "r1", "seq": 0}]},
        {**base, "source_versions": [{"replica_id": "r1", "seq": "1"}]},
        {**base, "unexpected": 1},
        {"replica_id": "r1"},
    ]
    for body in bad:
        assert code_status(body) == 422, body


def test_migration_conflicted_or_deleted_source_is_409():
    # Conflict on name (two maximal puts).
    hid = create_conflict_history()
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r1", "seq": 1},
         {"replica_id": "r2", "seq": 1}],
        [],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "source_key_not_movable" for e in r.json()["detail"]
    )

    # A source key with a unique delete is not movable either.
    hid = hist_create(
        ["r1", "r2"],
        [
            ev("r1", 1, "name", put="v", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "name", delete="delete",
               clock={"r1": 1, "r2": 1}),
        ],
    ).json()["id"]
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r2", "seq": 1}], [],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "source_key_not_movable" for e in r.json()["detail"]
    )

    # A source key with no events at all.
    r = hist_migrate(
        hid, "r1", "ghost", "alias", [], [],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "source_key_not_movable" for e in r.json()["detail"]
    )


def test_migration_busy_target_is_409_and_both_keys_are_checked_together():
    # 'name' is in conflict and 'other' is a unique put on r2.  The clocks
    # form a valid history: r1/1 and r2/1 split concurrently on name; r2/2
    # (having seen r1/1) writes other.
    hid = hist_create(
        ["r1", "r2"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1}),
            ev("r2", 2, "other", put="x", clock={"r1": 1, "r2": 2}),
        ],
    ).json()["id"]
    r = hist_migrate(
        hid, "r1", "other", "name",
        [{"replica_id": "r2", "seq": 2}],
        [{"replica_id": "r1", "seq": 1},
         {"replica_id": "r2", "seq": 1}],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "target_key_not_idle" for e in r.json()["detail"]
    )

    # Both keys invalid at once -> both error codes come back together.
    r = client.post(
        f"/api/histories/{hid}/migrate-key",
        json={
            "replica_id": "r1",
            "source_key": "name",   # conflicted
            "target_key": "other",  # holds a put
            "source_versions": [
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
            "target_versions": [{"replica_id": "r2", "seq": 2}],
        },
    )
    assert r.status_code == 409
    codes = {e["code"] for e in r.json()["detail"]}
    assert codes == {"source_key_not_movable", "target_key_not_idle"}


def test_migration_stale_versions_are_409_and_atomic():
    hid = migration_seed_history()
    before = hist_get(hid).json()

    # Source list points at an event that is not the current maximal.
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r2", "seq": 1}], [],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "migration_versions_mismatch"
        for e in r.json()["detail"]
    )

    # Eventless target but a non-empty version list is stale too.
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r1", "seq": 1}],
        [{"replica_id": "r2", "seq": 1}],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "migration_versions_mismatch"
        for e in r.json()["detail"]
    )

    # Unique source put but two versions supplied -> mismatch.
    r = hist_migrate(
        hid, "r1", "name", "alias",
        [{"replica_id": "r1", "seq": 1},
         {"replica_id": "r2", "seq": 1}],
        [],
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "migration_versions_mismatch"
        for e in r.json()["detail"]
    )

    # Nothing was written by any of the failures.
    assert hist_get(hid).json() == before


def test_migration_needs_two_free_event_slots():
    def chain_events(pairs, extra=0):
        events = []
        for n in range(1, pairs + 1):
            events.append(
                ev("r1", n, f"a{n}", put="x",
                   clock={"r1": n, "r2": n - 1})
            )
            events.append(
                ev("r2", n, f"b{n}", put="y",
                   clock={"r1": n, "r2": n})
            )
        for n in range(pairs + 1, pairs + 1 + extra):
            events.append(
                ev("r1", n, f"a{n}", put="x",
                   clock={"r1": n, "r2": pairs})
            )
        return events

    # 118 events: exactly room for two more -> succeeds, reaches 120.
    hid = hist_create(["r1", "r2"], chain_events(59)).json()["id"]
    r = hist_migrate(
        hid, "r1", "b59", "alias",
        [{"replica_id": "r2", "seq": 59}], [],
    )
    assert r.status_code == 200, r.text
    assert len(hist_get(hid).json()["events"]) == 120

    # Now full -> 422 batch_full.
    r = hist_migrate(
        hid, "r1", "b58", "other",
        [{"replica_id": "r2", "seq": 58}], [],
    )
    assert r.status_code == 422
    assert any(e["code"] == "batch_full" for e in r.json()["detail"])

    # 119 events leaves only one slot -> 422 as well.
    hid = hist_create(["r1", "r2"], chain_events(59, extra=1)).json()["id"]
    assert len(hist_get(hid).json()["events"]) == 119
    r = hist_migrate(
        hid, "r2", "a60", "alias",
        [{"replica_id": "r1", "seq": 60}], [],
    )
    assert r.status_code == 422
    assert any(e["code"] == "batch_full" for e in r.json()["detail"])


def test_migration_unknown_history_is_404():
    r = hist_migrate(
        "0" * 32, "r1", "a", "b", [], [],
    )
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"


def test_migration_concurrent_same_source_exactly_one_wins():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    barrier = threading.Barrier(6)

    def move(n):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/migrate-key",
            json={
                "replica_id": "r2",
                "source_key": "name",
                "target_key": f"alias{n}",
                "source_versions": [{"replica_id": "r1", "seq": 1}],
                "target_versions": [],
            },
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(move, range(6)))

    statuses = sorted(r.status_code for r in results)
    assert statuses == [200] + [409] * 5, [r.text for r in results]
    got = hist_get(hid).json()
    # Exactly one migration pair was ever allocated: r2/1 delete, r2/2 put.
    assert [(e["replica_id"], e["seq"]) for e in got["events"]] == [
        ("r1", 1), ("r2", 1), ("r2", 2),
    ]
    r2_events = [e for e in got["events"] if e["replica_id"] == "r2"]
    assert r2_events[0]["key"] == "name"
    assert "delete" in r2_events[0]
    winners = [r for r in results if r.status_code == 200]
    assert len(winners) == 1
    target = winners[0].json()["events"][1]["key"]
    results_by_key = {v["key"]: v for v in got["results"]}
    assert results_by_key["name"]["state"] == "deleted"
    assert results_by_key[target]["state"] == "put"
    assert results_by_key[target]["value"] == "alice"


def test_migration_concurrent_distinct_sources_allocate_unique_seqs():
    # r1 holds five independent unique puts; five writers move different keys
    # through replica r2 concurrently.  All succeed and r2 gets seqs 1..10.
    events = [
        ev("r1", n, f"k{n}", put=f"v{n}",
           clock={"r1": n, "r2": 0})
        for n in range(1, 6)
    ]
    hid = hist_create(["r1", "r2"], events).json()["id"]
    barrier = threading.Barrier(5)

    def move(n):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/migrate-key",
            json={
                "replica_id": "r2",
                "source_key": f"k{n}",
                "target_key": f"a{n}",
                "source_versions": [{"replica_id": "r1", "seq": n}],
                "target_versions": [],
            },
        )

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(move, range(1, 6)))
    assert all(r.status_code == 200 for r in results), [
        r.text for r in results if r.status_code != 200
    ]
    got = hist_get(hid).json()
    r2_seqs = sorted(
        e["seq"] for e in got["events"] if e["replica_id"] == "r2"
    )
    assert r2_seqs == list(range(1, 11))
    assert len(got["events"]) == 15
    by_key = {v["key"]: v for v in got["results"]}
    for n in range(1, 6):
        assert by_key[f"k{n}"]["state"] == "deleted"
        assert by_key[f"a{n}"]["state"] == "put"
        assert by_key[f"a{n}"]["value"] == f"v{n}"


def test_migration_concurrent_with_legacy_merge_loses_nothing():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    barrier = threading.Barrier(2)

    def migrate():
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/migrate-key",
            json={
                "replica_id": "r2",
                "source_key": "name",
                "target_key": "alias",
                "source_versions": [{"replica_id": "r1", "seq": 1}],
                "target_versions": [],
            },
        )

    def merge():
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/events",
            json={"events": [
                ev("r1", 2, "other", put="x",
                   clock={"r1": 2, "r2": 0})
            ]},
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_move = pool.submit(migrate)
        fut_merge = pool.submit(merge)
        r_move, r_merge = fut_move.result(), fut_merge.result()
    assert r_move.status_code == 200, r_move.text
    assert r_merge.status_code == 200, r_merge.text

    got = hist_get(hid).json()
    assert len(got["events"]) == 4
    by_id = {(e["replica_id"], e["seq"]): e for e in got["events"]}
    assert "delete" in by_id[("r2", 1)]
    assert by_id[("r2", 2)]["put"] == "alice"
    assert by_id[("r1", 2)]["key"] == "other"


def test_migration_ignores_staged_events_and_leaves_session_untouched():
    hid = migration_seed_history()
    sid = session_create(hid).json()["id"]

    # Stage a genuinely new identity on another key; it must not participate in
    # the migration's checks or verdicts.
    r = session_append(
        hid, sid,
        [ev("r1", 2, "staged-key", put="s",
            clock={"r1": 2, "r2": 1})],
    )
    assert r.status_code == 200, r.text

    r = hist_migrate(
        hid, "r2", "name", "alias",
        [{"replica_id": "r1", "seq": 1}], [],
    )
    assert r.status_code == 200, r.text
    # Verdicts cover exactly the two migrated committed keys.
    assert [v["key"] for v in r.json()["results"]] == ["alias", "name"]

    # The staged events and the open session are untouched.
    session = session_get(hid, sid).json()
    assert session["status"] == "open"
    assert [(e["replica_id"], e["seq"]) for e in session["events"]] == [
        ("r1", 2)
    ]

    # Committing the session later merges cleanly alongside the migration.
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    got = hist_get(hid).json()
    assert {(e["replica_id"], e["seq"]) for e in got["events"]} == {
        ("r1", 1), ("r1", 2), ("r2", 1), ("r2", 2), ("r2", 3),
    }
    assert session_get(hid, sid).json()["status"] == "closed"


# ---------------------------------------------------------------------------
# Admitting a new replica to a persistent history
# ---------------------------------------------------------------------------


def hist_admit(hid, replica_id):
    return client.post(
        f"/api/histories/{hid}/replicas", json={"replica_id": replica_id}
    )


def admit_seed_history():
    """r1/1 put and r2/1 delete conflict on 'name'; r2 also put 'other'."""
    r = hist_create(
        ["r1", "r2"],
        [
            ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "name", delete="delete", clock={"r1": 0, "r2": 1}),
            ev("r2", 2, "other", put="q", clock={"r1": 1, "r2": 2}),
        ],
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


def assert_clock_covers(data, replicas):
    for e in data["events"]:
        assert set(e["clock"]) == set(replicas)
    for result in data["results"]:
        shapes = (
            result["events"] if result["state"] == "conflict" else [result["event"]]
        )
        for shape in shapes:
            assert set(shape["clock"]) == set(replicas)


def test_admit_replica_appends_id_and_zero_components_preserving_verdicts():
    hid = admit_seed_history()
    before = hist_get(hid).json()

    r = hist_admit(hid, "r3")
    assert r.status_code == 200, r.text
    data = r.json()

    # The new replica is appended to the end; the response is the ordinary
    # history read structure of the expanded history.
    assert data["id"] == hid
    assert data["replica_ids"] == ["r1", "r2", "r3"]
    assert_clock_covers(data, ["r1", "r2", "r3"])

    # Every committed event gained exactly the r3=0 component; seq, key, op,
    # value and the old components are unchanged.
    for old, new in zip(before["events"], data["events"]):
        assert {k: v for k, v in new["clock"].items() if k != "r3"} == old["clock"]
        assert new["clock"]["r3"] == 0
        for field in ("replica_id", "seq", "key"):
            assert new[field] == old[field]
        assert ("put" in new) == ("put" in old)
        assert new.get("put") == old.get("put")
        assert new.get("delete") == old.get("delete")

    # Key states and values are unchanged (results just carry the new zero).
    def strip(results):
        out = json.loads(json.dumps(results))
        for res in out:
            shapes = res["events"] if res["state"] == "conflict" else [res["event"]]
            for s in shapes:
                s["clock"].pop("r3", None)
        return out

    assert strip(data["results"]) == strip(before["results"])

    # A plain GET afterwards returns the same expanded state.
    assert hist_get(hid).json() == data


def test_admit_replica_patches_open_and_closed_sessions_without_closing():
    hid = admit_seed_history()
    open_sid = create_session(hid)
    closed_sid = create_session(hid)

    # The open session stages an event whose dependency (r1/3) exists neither
    # in the history nor in the session: staging tolerates missing deps.
    assert session_append(
        hid, open_sid,
        [ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 2})],
    ).status_code == 200
    # A second open session holds an event pointing at a missing event.
    gappy_sid = create_session(hid)
    assert session_append(
        hid, gappy_sid,
        [ev("r2", 3, "k", put="b", clock={"r1": 9, "r2": 3})],
    ).status_code == 200

    # The closed session stages an event, commits, and ends up closed.
    assert session_append(
        hid, closed_sid,
        [ev("r2", 3, "c", put="z", clock={"r1": 1, "r2": 3})],
    ).status_code == 200
    assert session_commit(hid, closed_sid).status_code == 200
    assert session_get(hid, closed_sid).json()["status"] == "closed"

    # Admission must not be blocked by the incomplete open sessions.
    r = hist_admit(hid, "r3")
    assert r.status_code == 200, r.text

    open_data = session_get(hid, open_sid).json()
    closed_data = session_get(hid, closed_sid).json()
    gappy_data = session_get(hid, gappy_sid).json()

    assert open_data["status"] == "open"
    assert closed_data["status"] == "closed"
    assert gappy_data["status"] == "open"
    for data in (open_data, closed_data, gappy_data):
        assert len(data["events"]) == 1
        e = data["events"][0]
        assert set(e["clock"]) == {"r1", "r2", "r3"}
        assert e["clock"]["r3"] == 0
        assert e["clock"][e["replica_id"]] == e["seq"]

    # The committed r2/3 from the closed session made it into the history and
    # got patched there too.
    got = hist_get(hid).json()
    by_id = {(e["replica_id"], e["seq"]): e for e in got["events"]}
    assert by_id[("r2", 3)]["clock"] == {"r1": 1, "r2": 3, "r3": 0}


def test_admit_replica_unknown_history_is_404():
    r = hist_admit("0" * 32, "r3")
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"


def test_admit_replica_duplicate_is_409_and_changes_nothing():
    hid = admit_seed_history()
    before = hist_get(hid).json()
    r = hist_admit(hid, "r2")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail[0]["code"] == "replica_already_exists"
    assert detail[0]["replica_id"] == "r2"
    assert hist_get(hid).json() == before


def test_admit_replica_empty_or_missing_id_is_422():
    hid = admit_seed_history()
    assert hist_admit(hid, "").status_code == 422
    assert client.post(
        f"/api/histories/{hid}/replicas", json={}
    ).status_code == 422
    # Extra/unknown fields are rejected by the same schema.
    assert client.post(
        f"/api/histories/{hid}/replicas",
        json={"replica_id": "r3", "extra": 1},
    ).status_code == 422


def test_admit_replica_ninth_member_is_422_and_changes_nothing():
    replicas = ["a", "b", "c", "d", "e", "f", "g", "h"]
    clock = {rid: 0 for rid in replicas}
    clock["a"] = 1
    r = hist_create(
        replicas, [ev("a", 1, "k", put="v", clock=clock)]
    )
    assert r.status_code == 201, r.text
    hid = r.json()["id"]
    before = hist_get(hid).json()

    r = hist_admit(hid, "i")
    assert r.status_code == 422
    assert r.json()["detail"][0]["code"] == "replica_limit_exceeded"
    assert hist_get(hid).json() == before


def test_admitted_replica_writes_from_seq_one_with_full_clock_only():
    hid = admit_seed_history()
    assert hist_admit(hid, "r3").status_code == 200

    # A missing component is still 422: admission never auto-fills requests.
    r = hist_merge(
        hid, [ev("r3", 1, "k", put="v", clock={"r1": 1, "r3": 1})]
    )
    assert_422(r, "clock_replica_coverage")
    assert hist_get(hid).json()["replica_ids"] == ["r1", "r2", "r3"]

    # The full clock works and the new replica starts at seq 1.
    r = hist_merge(
        hid,
        [ev("r3", 1, "fresh", put="new", clock={"r1": 1, "r2": 2, "r3": 1})],
    )
    assert r.status_code == 200, r.text
    by_id = {(e["replica_id"], e["seq"]): e for e in r.json()["events"]}
    assert by_id[("r3", 1)]["put"] == "new"

    # The next event continues the tail.
    r = hist_merge(
        hid,
        [ev("r3", 2, "fresh", put="new2", clock={"r1": 1, "r2": 2, "r3": 2})],
    )
    assert r.status_code == 200, r.text
    assert ("r3", 2) in {
        (e["replica_id"], e["seq"]) for e in r.json()["events"]
    }


def test_conflict_commit_and_migration_work_for_admitted_replica():
    hid = admit_seed_history()
    assert hist_admit(hid, "r3").status_code == 200

    # The admitted replica resolves the 'name' conflict by writing r3/1.
    r = hist_commit(
        hid,
        key="name",
        writer="r3",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    event = r.json()["event"]
    assert event["replica_id"] == "r3" and event["seq"] == 1
    assert event["clock"] == {"r1": 1, "r2": 1, "r3": 1}


def test_open_session_continues_after_admission_and_commits():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    # r1/2 staged before admission with the old two-component clock.
    assert session_append(
        hid, sid,
        [ev("r1", 2, "name", put="alice2", clock={"r1": 2, "r2": 0})],
    ).status_code == 200

    assert hist_admit(hid, "r3").status_code == 200

    # The session stays open and its staged event now carries r3=0.
    data = session_get(hid, sid).json()
    assert data["status"] == "open"
    assert data["events"][0]["clock"] == {"r1": 2, "r2": 0, "r3": 0}

    # Retransmitting the staged identity with the OLD clock is different full
    # content after admission -> 409 (never silently re-patched per request).
    r = session_append(
        hid, sid,
        [ev("r1", 2, "name", put="alice2", clock={"r1": 2, "r2": 0})],
    )
    assert r.status_code == 409
    assert r.json()["detail"][0]["code"] == "event_content_mismatch"

    # The patched (stored) three-component form is identical -> ignored.
    assert session_append(
        hid, sid,
        [ev("r1", 2, "name", put="alice2", clock={"r1": 2, "r2": 0, "r3": 0})],
    ).status_code == 200

    # A genuinely new staged identity must already cover the full new set.
    r = session_append(
        hid, sid,
        [ev("r1", 3, "name", put="alice3", clock={"r1": 3, "r2": 0})],
    )
    assert_422(r, "clock_replica_coverage")
    assert session_append(
        hid, sid,
        [ev("r1", 3, "name", put="alice3", clock={"r1": 3, "r2": 0, "r3": 0})],
    ).status_code == 200

    # The same session commits and closes; staged events merge with the
    # post-admission full clocks.
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 1), ("r1", 2), ("r1", 3)
    ]
    assert session_get(hid, sid).json()["status"] == "closed"


def test_admission_failure_rolls_back_history_and_all_sessions():
    hid = admit_seed_history()
    sid = create_session(hid)
    assert session_append(
        hid, sid,
        [ev("r1", 4, "k", put="a", clock={"r1": 4, "r2": 2})],
    ).status_code == 200
    before = hist_get(hid).json()
    session_before = session_get(hid, sid).json()

    # Duplicate admission -> 409: nothing patched anywhere.
    assert hist_admit(hid, "r1").status_code == 409
    assert hist_get(hid).json() == before
    assert session_get(hid, sid).json() == session_before


def test_admission_does_not_touch_other_histories():
    hid = admit_seed_history()
    other = admit_seed_history()
    other_before = hist_get(other).json()

    assert hist_admit(hid, "r3").status_code == 200
    assert hist_get(other).json() == other_before


def test_admission_survives_process_restart(tmp_path):
    db = tmp_path / "history.db"
    env = dict(os.environ, HISTORY_DB_PATH=str(db), PYTHONPATH="/workspace")

    def run(code):
        return subprocess.run(
            [sys.executable, "-c", code],
            env=env, capture_output=True, text=True,
        )

    first = run(
        """
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice',
         'clock': {'r1': 1, 'r2': 0}},
    ],
})
hid = r.json()['id']
r = c.post(f'/api/histories/{hid}/import-sessions')
sid = r.json()['id']
r = c.post(f'/api/histories/{hid}/import-sessions/{sid}/events', json={
    'events': [
        {'replica_id': 'r1', 'seq': 2, 'key': 'name', 'put': 'alice2',
         'clock': {'r1': 2, 'r2': 0}},
    ],
})
assert r.status_code == 200, r.text
r = c.post(f'/api/histories/{hid}/replicas', json={'replica_id': 'r3'})
assert r.status_code == 200, r.text
assert r.json()['replica_ids'] == ['r1', 'r2', 'r3']
print(hid, sid)
"""
    )
    assert first.returncode == 0, first.stderr
    hid, sid = first.stdout.split()

    second = run(
        f"""
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.get('/api/histories/{hid}')
assert r.status_code == 200, r.text
data = r.json()
assert data['replica_ids'] == ['r1', 'r2', 'r3']
assert data['events'][0]['clock'] == {{'r1': 1, 'r2': 0, 'r3': 0}}

# The open session and its patched staged events survived the restart.
r = c.get('/api/histories/{hid}/import-sessions/{sid}')
assert r.status_code == 200, r.text
sdata = r.json()
assert sdata['status'] == 'open'
assert sdata['events'][0]['clock'] == {{'r1': 2, 'r2': 0, 'r3': 0}}

# The admitted replica writes seq 1 after the restart.
r = c.post('/api/histories/{hid}/events', json={{
    'events': [
        {{'replica_id': 'r3', 'seq': 1, 'key': 'name', 'put': 'carol',
         'clock': {{'r1': 1, 'r2': 0, 'r3': 1}}}},
    ],
}})
assert r.status_code == 200, r.text
print('ok')
"""
    )
    assert second.returncode == 0, second.stderr
    assert second.stdout.strip() == "ok"


def test_admission_concurrent_with_session_commit_loses_nothing():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="start", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)
    assert session_append(
        hid, sid,
        [ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1})],
    ).status_code == 200
    barrier = threading.Barrier(3)

    def admit():
        c = TestClient(app)
        barrier.wait()
        return c.post(f"/api/histories/{hid}/replicas", json={"replica_id": "r3"})

    def commit_session():
        c = TestClient(app)
        barrier.wait()
        return c.post(f"/api/histories/{hid}/import-sessions/{sid}/commit")

    def idempotent_merge():
        # r2/1 is the session's event; whichever serialisation order wins,
        # eventually the three-component identical retransmission is ignored.
        c = TestClient(app)
        barrier.wait()
        for _ in range(50):
            r = c.post(
                f"/api/histories/{hid}/events",
                json={"events": [
                    ev("r2", 1, "k", put="b",
                       clock={"r1": 1, "r2": 1, "r3": 0}),
                ]},
            )
            if r.status_code == 200:
                return r
        return r

    with ThreadPoolExecutor(max_workers=3) as pool:
        r_admit, r_commit, r_merge = list(
            pool.map(lambda f: f(), [admit, commit_session, idempotent_merge])
        )

    assert r_admit.status_code == 200, r_admit.text
    assert r_commit.status_code == 200, r_commit.text
    assert r_merge.status_code == 200, r_merge.text

    got = hist_get(hid).json()
    assert got["replica_ids"] == ["r1", "r2", "r3"]
    by_id = {(e["replica_id"], e["seq"]): e for e in got["events"]}
    assert set(by_id) == {("r1", 1), ("r2", 1)}
    assert by_id[("r1", 1)]["clock"] == {"r1": 1, "r2": 0, "r3": 0}
    assert by_id[("r2", 1)]["clock"] == {"r1": 1, "r2": 1, "r3": 0}
    assert session_get(hid, sid).json()["status"] == "closed"


def test_concurrent_reads_never_observe_mixed_replica_sets():
    hid = admit_seed_history()

    def read_until_done(stop):
        c = TestClient(app)
        seen = set()
        while not stop.is_set():
            data = c.get(f"/api/histories/{hid}").json()
            replicas = set(data["replica_ids"])
            # Each response must be internally consistent: every event clock
            # covers exactly the replica_ids the same response carries.
            for e in data["events"]:
                assert set(e["clock"]) == replicas
            assert replicas in ({"r1", "r2"}, {"r1", "r2", "r3"})
            seen.add(tuple(data["replica_ids"]))
        return seen

    stop = threading.Event()
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(read_until_done, stop) for _ in range(4)]
        time.sleep(0.05)
        r = hist_admit(hid, "r3")
        assert r.status_code == 200, r.text
        time.sleep(0.05)
        stop.set()
        seen_sets = [f.result() for f in futures]

    assert any(("r1", "r2", "r3") in s for s in seen_sets)
