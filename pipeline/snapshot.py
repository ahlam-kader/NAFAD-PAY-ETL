"""Compute a deterministic hash of every Silver+Gold table — used by the
idempotence test to prove that two pipeline runs produce a bit-exact state."""
from __future__ import annotations

import hashlib
import json
import sys

from sqlalchemy import text

from .common.io import pg_tx
from .common.logging_setup import get_logger

log = get_logger(__name__)

SILVER_TABLES = ["users", "accounts", "transactions", "fees"]
GOLD_TABLES = ["daily_volume", "user_activity", "agency_perf"]


def _hash_table(conn, schema: str, table: str) -> tuple[str, int]:
    """MD5 over an ordered, text-cast projection of every row.

    Postgres `t::text` produces the canonical row repr, so the hash is stable
    across runs as long as the row set + column ordering is identical.
    """
    sql = text(
        f"""
        SELECT
            COALESCE(md5(string_agg(row_text, ',' ORDER BY row_text)), 'EMPTY') AS h,
            COUNT(*) AS n
        FROM (SELECT t::text AS row_text FROM {schema}.{table} t) s
        """
    )
    row = conn.execute(sql).one()
    return row.h, row.n


def compute() -> dict:
    out: dict = {"silver": {}, "gold": {}}
    with pg_tx() as conn:
        for t in SILVER_TABLES:
            h, n = _hash_table(conn, "silver", t)
            out["silver"][t] = {"hash": h, "rows": n}
        for t in GOLD_TABLES:
            h, n = _hash_table(conn, "gold", t)
            out["gold"][t] = {"hash": h, "rows": n}
    payload = json.dumps(out, sort_keys=True).encode("utf-8")
    out["_overall"] = hashlib.sha256(payload).hexdigest()
    return out


def main() -> None:
    snap = compute()
    json.dump(snap, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
