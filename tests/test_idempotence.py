"""Idempotence: a second pipeline run must yield bit-exact identical state."""
from __future__ import annotations

from pipeline import bronze, gold, silver, snapshot


def test_pipeline_is_idempotent():
    snap1 = snapshot.compute()

    # Re-run every layer.
    bronze.run()
    silver.run()
    gold.run()

    snap2 = snapshot.compute()

    assert snap1["_overall"] == snap2["_overall"], (
        f"snapshots diverged: {snap1} vs {snap2}"
    )
    for layer in ("silver", "gold"):
        assert snap1[layer] == snap2[layer], f"{layer} diverged"


def test_silver_tables_are_non_empty():
    snap = snapshot.compute()
    assert snap["silver"]["users"]["rows"] > 0
    assert snap["silver"]["accounts"]["rows"] > 0
    assert snap["silver"]["transactions"]["rows"] > 0
    assert snap["silver"]["fees"]["rows"] > 0


def test_gold_tables_are_non_empty():
    snap = snapshot.compute()
    assert snap["gold"]["daily_volume"]["rows"] > 0
    assert snap["gold"]["user_activity"]["rows"] > 0
    assert snap["gold"]["agency_perf"]["rows"] > 0
