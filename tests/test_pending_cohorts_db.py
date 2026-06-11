"""Feature 010 — live Postgres E2E for the new-cohort review+launch DB layer.

Runs against the REAL Vercel Postgres (the same DB the console reads), so it
catches schema / SQL / column-projection bugs the mocked unit tests can't. Skips
cleanly when DATABASE_URL is unset (CI without DB / contributors without creds):

    doppler run -- python3 -m pytest tests/test_pending_cohorts_db.py -q

Everything is scoped to a synthetic ramp id and torn down before + after, so it
never touches real ramps. It asserts the two halves of the isolation guarantee
at the DB layer:
  1. the scoped-prep ADDITIVE upsert (what ONLY_COHORT actually does for the
     target cohort) leaves the OTHER cohorts' cohort_icp rows byte-identical, and
  2. the ramp-wide DELETE (what main.py runs ONLY when ONLY_COHORT is unset) is
     genuinely ramp-wide — which is exactly why the `if not config.ONLY_COHORT`
     guard is the linchpin.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL", "").strip(),
    reason="DATABASE_URL not set — live Postgres E2E skipped (run via `doppler run --`).",
)

RAMP = "ZZZ-ISO-TEST"


def _icp(desc: str) -> dict:
    return {"cohort_description": desc, "creative_liberty": "medium"}


def _cleanup():
    from src.ui_decisions import _connect
    with _connect() as conn, conn.cursor() as cur:
        for tbl in ("cohort_icp", "ramp_audit_log", "ramp_decisions"):
            cur.execute(f"DELETE FROM {tbl} WHERE ramp_id = %s", (RAMP,))
        conn.commit()


@pytest.fixture
def clean_ramp():
    _cleanup()
    # Seed a decision row the helpers can UPDATE (mirrors a prepped ramp).
    from src.ui_decisions import _connect
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ramp_decisions (ramp_id, status) "
            "VALUES (%s, 'awaiting_approval'::ramp_status)",
            (RAMP,),
        )
        conn.commit()
    yield
    _cleanup()


def test_migration_columns_present():
    from src.ui_decisions import _connect
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'ramp_decisions' "
            "AND column_name IN ('pending_cohorts', 'prepped_cohort_ids')"
        )
        cols = {r[0] for r in cur.fetchall()}
    assert cols == {"pending_cohorts", "prepped_cohort_ids"}


def test_decision_read_includes_pending_cohorts(clean_ramp):
    """The console reads pending_cohorts via getDecision's projection — pin that
    the same SELECT works server-side and defaults to []."""
    from src.ui_decisions import get_decision
    dec = get_decision(RAMP)
    assert dec is not None
    assert dec.pending_cohorts == []


def test_prepped_snapshot_roundtrip(clean_ramp):
    from src.ui_decisions import (
        set_prepped_cohort_ids, add_prepped_cohort_ids, get_prepped_cohort_ids,
    )
    set_prepped_cohort_ids(RAMP, ["A", "B"])
    assert sorted(get_prepped_cohort_ids(RAMP)) == ["A", "B"]
    # Union (scoped prep adds the one new cohort), de-duped.
    add_prepped_cohort_ids(RAMP, ["B", "C"])
    assert sorted(get_prepped_cohort_ids(RAMP)) == ["A", "B", "C"]


def test_pending_cohorts_lifecycle(clean_ramp):
    from src.ui_decisions import (
        add_pending_cohorts, get_pending_cohorts, set_pending_cohort_status,
    )
    add_pending_cohorts(RAMP, [
        {"cohort_id": "C", "label": "new cohort", "detected_at": "t", "status": "detected"},
    ])
    # Re-detecting the same cohort is idempotent (no duplicate).
    add_pending_cohorts(RAMP, [
        {"cohort_id": "C", "label": "dup", "detected_at": "t2", "status": "detected"},
    ])
    pend = get_pending_cohorts(RAMP)
    assert [e["cohort_id"] for e in pend] == ["C"]
    assert pend[0]["status"] == "detected"

    # State machine: detected → prepping → awaiting_review → launched.
    for status in ("prepping", "awaiting_review", "launched"):
        set_pending_cohort_status(RAMP, "C", status)
        assert get_pending_cohorts(RAMP)[0]["status"] == status

    # Once launched it drops out of the console's "open" filter.
    open_new = [e for e in get_pending_cohorts(RAMP) if e["status"] != "launched"]
    assert open_new == []


def test_scoped_prep_isolation_contract(clean_ramp):
    """The isolation linchpin, at the DB layer.

    A scoped prep (ONLY_COHORT) is ADDITIVE: it upserts only the target cohort
    and skips the ramp-wide DELETE. Prove (1) upserting cohort A leaves cohort B
    untouched, and (2) the DELETE main.py runs when ONLY_COHORT is UNSET is
    genuinely ramp-wide — i.e. without the guard, B would be wiped.
    """
    from src.ui_decisions import upsert_cohort_icp, _connect

    def _sigs():
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT cohort_signature, cohort_description FROM cohort_icp "
                "WHERE ramp_id = %s ORDER BY cohort_signature",
                (RAMP,),
            )
            return cur.fetchall()

    # Seed two cohorts (as a normal full prep would).
    upsert_cohort_icp(ramp_id=RAMP, cohort_id="stgA", cohort_signature="Cohort A", icp_dict=_icp("A v1"))
    upsert_cohort_icp(ramp_id=RAMP, cohort_id="stgB", cohort_signature="Cohort B", icp_dict=_icp("B v1"))
    assert _sigs() == [("Cohort A", "A v1"), ("Cohort B", "B v1")]

    # (1) Scoped prep re-upserts ONLY the target (A). B must be byte-identical.
    upsert_cohort_icp(ramp_id=RAMP, cohort_id="stgA2", cohort_signature="Cohort A", icp_dict=_icp("A v2"))
    rows = _sigs()
    assert rows == [("Cohort A", "A v2"), ("Cohort B", "B v1")], (
        "scoped additive upsert must update only the target cohort, never touch B"
    )

    # (2) Demonstrate the danger the guard defends against: the ramp-wide DELETE
    # main.py runs in the UNGUARDED (no ONLY_COHORT) branch wipes ALL cohorts.
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM cohort_icp WHERE ramp_id = %s", (RAMP,))
        conn.commit()
    assert _sigs() == [], "unguarded ramp-wide DELETE removes every cohort (why ONLY_COHORT skips it)"
