"""Unit tests for src.attribution_resolver — pay-rate + activations lookup."""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import attribution_resolver as ar


# ── Fixtures ────────────────────────────────────────────────────────────


def _client(df: pd.DataFrame) -> MagicMock:
    """Build a mock RedashClient that returns the given DataFrame from _run_query."""
    mock = MagicMock()
    mock._run_query.return_value = df
    return mock


# ── resolve_pay_rate ────────────────────────────────────────────────────


def test_pay_rate_happy_path_returns_t1():
    df = pd.DataFrame([{
        "SIGNUP_FLOW_ID": "sf_1", "PROJECT_ID": "proj_1",
        "QUALIFICATION_ID": "qual_1", "QUALIFICATION_NAME": "Coding",
        "QUALIFICATION_TYPE": "worker_skill",
        "IS_PAY_MULTIPLIER": False, "IS_ASSESSMENT": False,
        "T1_RATE_USD": 30.0, "T2_RATE_USD": 45.0,
        "T3_RATE_USD": 55.0, "T4_RATE_USD": 70.0,
        "PRIMARY_RANK": 1,
    }])
    result = ar.resolve_pay_rate("sf_1", redash_client=_client(df))
    assert result.t1_rate_usd == 30.0
    assert result.qualification_name == "Coding"
    assert result.skip_country_multiplier is False
    assert result.has_rate


def test_pay_rate_language_qual_sets_skip_country_multiplier():
    df = pd.DataFrame([{
        "SIGNUP_FLOW_ID": "sf_2", "PROJECT_ID": "proj_2",
        "QUALIFICATION_ID": "qual_lang", "QUALIFICATION_NAME": "Spanish (es-MX)",
        "QUALIFICATION_TYPE": "language",
        "IS_PAY_MULTIPLIER": False, "IS_ASSESSMENT": False,
        "T1_RATE_USD": 15.0, "T2_RATE_USD": None,
        "T3_RATE_USD": None, "T4_RATE_USD": None,
        "PRIMARY_RANK": 1,
    }])
    result = ar.resolve_pay_rate("sf_2", redash_client=_client(df))
    assert result.skip_country_multiplier is True
    assert result.t1_rate_usd == 15.0


def test_pay_rate_picks_primary_rank_1_from_multiple_rows():
    df = pd.DataFrame([
        {"PRIMARY_RANK": 3, "T1_RATE_USD": 20.0, "QUALIFICATION_NAME": "Generalist",  "QUALIFICATION_TYPE": "worker_skill"},
        {"PRIMARY_RANK": 1, "T1_RATE_USD": 40.0, "QUALIFICATION_NAME": "Cardiology",  "QUALIFICATION_TYPE": "worker_skill"},
        {"PRIMARY_RANK": 2, "T1_RATE_USD": 30.0, "QUALIFICATION_NAME": "Anatomy",     "QUALIFICATION_TYPE": "worker_skill"},
    ])
    result = ar.resolve_pay_rate("sf_3", redash_client=_client(df))
    assert result.qualification_name == "Cardiology"
    assert result.t1_rate_usd == 40.0


def test_pay_rate_null_t1_returns_none_not_zero():
    """Deprecated qual → NULL T1 from Snowflake. Pipeline must soft-fail to None,
    NOT silently coerce to $0/hr."""
    df = pd.DataFrame([{
        "PRIMARY_RANK": 1, "T1_RATE_USD": None, "QUALIFICATION_NAME": "DeprecatedQual",
        "QUALIFICATION_TYPE": "worker_skill",
    }])
    result = ar.resolve_pay_rate("sf_4", redash_client=_client(df))
    assert result.t1_rate_usd is None
    assert result.has_rate is False


def test_pay_rate_empty_df_soft_fails():
    result = ar.resolve_pay_rate("sf_unknown", redash_client=_client(pd.DataFrame()))
    assert result.t1_rate_usd is None
    assert result.qualification_name is None
    assert result.has_rate is False


def test_pay_rate_query_failure_returns_soft_fail():
    """Redash exception must NEVER propagate — caller relies on soft-fail."""
    mock = MagicMock()
    mock._run_query.side_effect = RuntimeError("Redash timeout")
    result = ar.resolve_pay_rate("sf_5", redash_client=mock)
    assert result.t1_rate_usd is None
    assert result.has_rate is False


def test_pay_rate_empty_signup_flow_id_returns_soft_fail():
    result = ar.resolve_pay_rate("", redash_client=_client(pd.DataFrame()))
    assert result.t1_rate_usd is None
    # Should NOT even hit the client
    assert result.qualification_name is None


def test_pay_rate_cache_hit_skips_redash_call():
    df = pd.DataFrame([{"PRIMARY_RANK": 1, "T1_RATE_USD": 30.0, "QUALIFICATION_NAME": "X",
                       "QUALIFICATION_TYPE": "worker_skill"}])
    client = _client(df)
    cache: dict = {}
    ar.resolve_pay_rate("sf_cache", redash_client=client, cache=cache)
    ar.resolve_pay_rate("sf_cache", redash_client=client, cache=cache)
    # Two calls → only ONE Redash hit
    assert client._run_query.call_count == 1


def test_pay_rate_cache_failure_is_also_cached():
    """When the first call soft-fails, subsequent cache hits return the same
    soft-fail without re-querying."""
    mock = MagicMock()
    mock._run_query.side_effect = RuntimeError("Boom")
    cache: dict = {}
    r1 = ar.resolve_pay_rate("sf_x", redash_client=mock, cache=cache)
    r2 = ar.resolve_pay_rate("sf_x", redash_client=mock, cache=cache)
    assert r1 == r2
    assert mock._run_query.call_count == 1


# ── resolve_activations ─────────────────────────────────────────────────


def test_activations_happy_path():
    df = pd.DataFrame([{
        "SIGNUP_FLOW_ID": "sf_act_1",
        "ACTIVATIONS": 47,
        "SKILL_PASSES": 312,
    }])
    result = ar.resolve_activations("sf_act_1", redash_client=_client(df))
    assert result.activations == 47
    assert result.skill_passes == 312
    assert result.signup_flow_id == "sf_act_1"


def test_activations_empty_df_returns_zeros():
    result = ar.resolve_activations("sf_unknown", redash_client=_client(pd.DataFrame()))
    assert result.activations == 0
    assert result.skill_passes == 0


def test_activations_query_failure_returns_zeros():
    mock = MagicMock()
    mock._run_query.side_effect = RuntimeError("Redash 500")
    result = ar.resolve_activations("sf_y", redash_client=mock)
    assert result.activations == 0
    assert result.skill_passes == 0


def test_activations_handles_lowercase_columns():
    """Redash sometimes returns lowercased column names depending on engine."""
    df = pd.DataFrame([{"signup_flow_id": "sf_lc", "activations": 5, "skill_passes": 22}])
    result = ar.resolve_activations("sf_lc", redash_client=_client(df))
    assert result.activations == 5
    assert result.skill_passes == 22


def test_activations_cache_hit_skips_redash():
    df = pd.DataFrame([{"ACTIVATIONS": 1, "SKILL_PASSES": 2}])
    client = _client(df)
    cache: dict = {}
    ar.resolve_activations("sf_cached", redash_client=client, cache=cache)
    ar.resolve_activations("sf_cached", redash_client=client, cache=cache)
    assert client._run_query.call_count == 1


def test_sql_files_exist_and_have_signup_flow_id_placeholder():
    """Schema-invariant safety check — the .sql files must exist and embed
    the {signup_flow_id} placeholder the resolver substitutes."""
    pay_sql = ar._PAY_RATE_SQL_PATH.read_text()
    act_sql = ar._ACTIVATIONS_SQL_PATH.read_text()
    assert "{signup_flow_id}" in pay_sql
    assert "{signup_flow_id}" in act_sql
    # And the corrected schema bits
    assert "qpr.QUALIFICATION_ID = pq._ID" in pay_sql  # corrected join key
    assert "PAY_RATE" in pay_sql                        # flat column, not T1-T4
    assert "EVER_PASSED_SKILL_SCREENING" in act_sql     # CESF column
