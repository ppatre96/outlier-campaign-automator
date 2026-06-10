"""parse_job_post_pay_rate — extract a numeric USD/hr rate from Smart Ramp's
job_post_pay_rates. Authoritative pay-rate fallback when the guardrail/Snowflake
rate is absent (feedback_smart_ramp_authoritative_data)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.attribution_resolver import parse_job_post_pay_rate


def test_up_to_ceiling():
    assert parse_job_post_pay_rate(["up to $35 /hr"]) == 35.0  # the GMR-0024 value


def test_single_value():
    assert parse_job_post_pay_rate(["$25/hr"]) == 25.0


def test_range_takes_max():
    assert parse_job_post_pay_rate(["$15-$25/hr"]) == 25.0
    assert parse_job_post_pay_rate(["$15–$25/hr"]) == 25.0  # en-dash


def test_decimal():
    assert parse_job_post_pay_rate(["$7.50/hr"]) == 7.5


def test_max_across_entries():
    assert parse_job_post_pay_rate(["$15/hr", "$30/hr"]) == 30.0


def test_no_figure_returns_none():
    assert parse_job_post_pay_rate(["competitive pay"]) is None
    assert parse_job_post_pay_rate([]) is None
    assert parse_job_post_pay_rate(None) is None
