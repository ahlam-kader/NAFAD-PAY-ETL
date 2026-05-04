"""Anomaly detection sanity checks vs the README's expected counts."""
from __future__ import annotations

from sqlalchemy import text

from pipeline.common.io import pg_engine


def _scalar(sql: str) -> int:
    with pg_engine().connect() as conn:
        return int(conn.execute(text(sql)).scalar_one())


def test_clock_skew_count_close_to_readme():
    """README claims ~2 575 clock skews. Tolerate ±5 % drift."""
    n = _scalar("SELECT COUNT(*) FROM silver.transactions WHERE _has_clock_skew")
    assert 2_400 <= n <= 2_750, f"clock_skew count out of bounds: {n}"


def test_soft_deletes_propagated():
    """README's "58 CLOSED/SUSPENDED le 19" is a raw-row count in users_delta of
    2024-01-19. After 7 days of dumps are dedup-merged, the cumulative count is
    larger because the 18th full dump already contained CLOSED/SUSPENDED rows
    and later deltas add or overwrite. We assert the pipeline propagates *at
    least* the 58 expected, and no record drops below the floor implied by the
    18th full dump (181 CLOSED/SUSPENDED in the latest-known state)."""
    n_total = _scalar("SELECT COUNT(*) FROM silver.users WHERE _is_deleted")
    assert n_total >= 58, f"too few soft-deletes propagated: {n_total}"
    assert n_total >= 181, (
        f"silver lost CLOSED/SUSPENDED rows below the 2024-01-18 full-dump floor: {n_total}"
    )


def test_unique_dedup_keys_silver_transactions():
    n_total = _scalar("SELECT COUNT(*) FROM silver.transactions")
    n_unique = _scalar(
        "SELECT COUNT(*) FROM (SELECT DISTINCT id, reference FROM silver.transactions) s"
    )
    assert n_total == n_unique, "silver.transactions has dup (id, reference)"


def test_unique_pk_silver_users():
    n_total = _scalar("SELECT COUNT(*) FROM silver.users")
    n_unique = _scalar("SELECT COUNT(DISTINCT id) FROM silver.users")
    assert n_total == n_unique


def test_full_18_supersedes_deltas_users():
    """The full dump of 2024-01-18 contains 5 716 distinct users.
    Silver should hold *at least* that many (later deltas may add more)."""
    n = _scalar("SELECT COUNT(*) FROM silver.users")
    assert n >= 5_716, f"expected >= 5716 users, got {n}"


def test_cross_dc_flag_present():
    n = _scalar("SELECT COUNT(*) FROM silver.transactions WHERE _is_cross_dc")
    assert n > 0, "no cross-DC transactions detected — pipeline not flagging?"


def test_gold_daily_volume_aligned_with_silver():
    """Sum of gold.daily_volume.tx_count must equal silver.transactions count
    where transaction_date is non-null (gold is bucketed by transaction_date)."""
    n_silver = _scalar(
        "SELECT COUNT(*) FROM silver.transactions WHERE transaction_date IS NOT NULL"
    )
    n_gold = _scalar("SELECT COALESCE(SUM(tx_count),0) FROM gold.daily_volume")
    assert n_silver == n_gold, f"gold drifted from silver: {n_gold} vs {n_silver}"
