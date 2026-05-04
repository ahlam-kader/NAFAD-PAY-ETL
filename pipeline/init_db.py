"""Apply the warehouse DDL (sql/init_warehouse.sql)."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from .common.io import pg_tx, wait_for_pg
from .common.logging_setup import get_logger

log = get_logger(__name__)

DDL_PATH = Path(__file__).resolve().parent.parent / "sql" / "init_warehouse.sql"


def run() -> None:
    log.info(f"init_db.start ddl={DDL_PATH}")
    wait_for_pg()
    ddl = DDL_PATH.read_text(encoding="utf-8")
    with pg_tx() as conn:
        for stmt in [s for s in ddl.split(";\n") if s.strip()]:
            conn.execute(text(stmt))
    log.info("init_db.done")


if __name__ == "__main__":
    run()
