"""Shared fixtures: spin up the full pipeline once for the test session."""
from __future__ import annotations

import pytest

from pipeline import bronze, gold, init_db, silver
from pipeline.common.io import wait_for_pg, wait_for_s3


@pytest.fixture(scope="session", autouse=True)
def pipeline_run():
    wait_for_s3()
    wait_for_pg()
    init_db.run()
    bronze.run()
    silver.run()
    gold.run()
    yield
