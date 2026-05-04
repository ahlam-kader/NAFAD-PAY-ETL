"""Gold layer — pure SQL, idempotent (DELETE/INSERT inside a single tx)."""
from __future__ import annotations

from sqlalchemy import text

from .common.io import pg_tx
from .common.logging_setup import get_logger

log = get_logger(__name__)


DAILY_VOLUME = """
INSERT INTO gold.daily_volume
  (transaction_date, tx_count, success_count, failed_count, pending_count,
   total_amount, total_fees, cross_dc_count, clock_skew_count)
SELECT
  transaction_date,
  COUNT(*)                                                    AS tx_count,
  COUNT(*) FILTER (WHERE status = 'SUCCESS')                  AS success_count,
  COUNT(*) FILTER (WHERE status = 'FAILED')                   AS failed_count,
  COUNT(*) FILTER (WHERE status NOT IN ('SUCCESS','FAILED'))  AS pending_count,
  COALESCE(SUM(amount) FILTER (WHERE status = 'SUCCESS'),0)   AS total_amount,
  COALESCE(SUM(fee)    FILTER (WHERE status = 'SUCCESS'),0)   AS total_fees,
  COUNT(*) FILTER (WHERE _is_cross_dc)                        AS cross_dc_count,
  COUNT(*) FILTER (WHERE _has_clock_skew)                     AS clock_skew_count
FROM silver.transactions
WHERE transaction_date IS NOT NULL
GROUP BY transaction_date;
"""

USER_ACTIVITY = """
INSERT INTO gold.user_activity
  (user_id, tx_count_30d, last_tx_date, active_flag, is_deleted)
SELECT
  u.id,
  COALESCE(t.tx_count_30d, 0)                          AS tx_count_30d,
  t.last_tx_date,
  COALESCE(t.tx_count_30d, 0) > 0                      AS active_flag,
  u._is_deleted                                        AS is_deleted
FROM silver.users u
LEFT JOIN (
  SELECT
    source_user_id AS user_id,
    COUNT(*) AS tx_count_30d,
    MAX(transaction_date) AS last_tx_date
  FROM silver.transactions
  WHERE source_user_id IS NOT NULL
    AND transaction_date >= (
      SELECT (MAX(transaction_date) - INTERVAL '30 days')::date FROM silver.transactions
    )
  GROUP BY source_user_id
) t ON t.user_id = u.id;
"""

AGENCY_PERF = """
INSERT INTO gold.agency_perf
  (agency_id, tx_count, success_count, failed_count, total_amount, total_fees_collected)
SELECT
  agency_id,
  COUNT(*)                                                    AS tx_count,
  COUNT(*) FILTER (WHERE status = 'SUCCESS')                  AS success_count,
  COUNT(*) FILTER (WHERE status = 'FAILED')                   AS failed_count,
  COALESCE(SUM(amount) FILTER (WHERE status = 'SUCCESS'),0)   AS total_amount,
  COALESCE(SUM(fee)    FILTER (WHERE status = 'SUCCESS'),0)   AS total_fees_collected
FROM silver.transactions
WHERE agency_id IS NOT NULL
GROUP BY agency_id;
"""


def run() -> None:
    log.info("gold.start")
    with pg_tx() as conn:
        conn.execute(text("TRUNCATE gold.daily_volume, gold.user_activity, gold.agency_perf"))
        conn.execute(text(DAILY_VOLUME))
        conn.execute(text(USER_ACTIVITY))
        conn.execute(text(AGENCY_PERF))
    log.info("gold.done")


if __name__ == "__main__":
    run()
