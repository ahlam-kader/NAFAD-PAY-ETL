"""Silver layer — read Bronze, dedupe, normalize, flag anomalies, load Postgres.

Reconciliation strategy:
  * users / accounts: union all dumps (full + delta), keep latest by `updated_at`,
    falling back to dump_date if `updated_at` ties. The full dump of 2024-01-18
    is the reset checkpoint — entries appearing only before it but missing from it
    are treated as historical (kept) but flagged via `_last_seen_dump_date`.
  * transactions: dedupe by (id, reference); the row with the latest dump_date
    wins; if tied, the one with the latest `completed_at` wins.
  * fees: dedupe by (id, transaction_reference); same rule as transactions.

Anomaly flags applied here:
  * `_has_clock_skew = (completed_at < created_at)` on transactions
  * `_is_cross_dc    = (is_cross_dc == True)`
  * `_is_deleted     = (status IN ('CLOSED','SUSPENDED'))` on users
"""
from __future__ import annotations

import io
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import text

from .common.config import CONFIG
from .common.io import list_keys, pg_engine, pg_tx, read_csv_from_s3
from .common.logging_setup import get_logger
from .common.schemas import (
    ACCOUNTS_SCHEMA,
    FEES_SCHEMA,
    TRANSACTIONS_SCHEMA,
    USERS_SCHEMA,
    validate,
)

log = get_logger(__name__)


@dataclass
class Bundle:
    users: pd.DataFrame
    accounts: pd.DataFrame
    transactions: pd.DataFrame
    fees: pd.DataFrame


def _parse_dump_date(key: str) -> str:
    # Expected: dt=YYYY-MM-DD/<file>.csv
    return key.split("/", 1)[0].removeprefix("dt=")


def _read_layer() -> Bundle:
    """Read every CSV from the Bronze bucket and tag each row with its dump_date."""
    users_parts: list[pd.DataFrame] = []
    accounts_parts: list[pd.DataFrame] = []
    tx_parts: list[pd.DataFrame] = []
    fees_parts: list[pd.DataFrame] = []

    for key in sorted(list_keys(CONFIG.bronze_bucket, "dt=")):
        dump_date = _parse_dump_date(key)
        filename = key.rsplit("/", 1)[-1]
        df = read_csv_from_s3(CONFIG.bronze_bucket, key, low_memory=False)
        df["_dump_date"] = dump_date
        df["_dump_kind"] = "full" if "_full" in filename else (
            "delta" if "_delta" in filename else "daily"
        )
        if filename.startswith("users_"):
            users_parts.append(df)
        elif filename.startswith("accounts_"):
            accounts_parts.append(df)
        elif filename.startswith("transactions_"):
            tx_parts.append(df)
        elif filename.startswith("fees_"):
            fees_parts.append(df)
        log.info(f"silver.read key={key} rows={len(df)}")

    return Bundle(
        users=pd.concat(users_parts, ignore_index=True) if users_parts else pd.DataFrame(),
        accounts=pd.concat(accounts_parts, ignore_index=True) if accounts_parts else pd.DataFrame(),
        transactions=pd.concat(tx_parts, ignore_index=True) if tx_parts else pd.DataFrame(),
        fees=pd.concat(fees_parts, ignore_index=True) if fees_parts else pd.DataFrame(),
    )


def _coerce_ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)


def _coerce_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def _coerce_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().isin({"true", "1", "t", "yes"})


def _coerce_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def _coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _dedup_users(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = validate(df, USERS_SCHEMA, "users")
    df["updated_at_ts"] = _coerce_ts(df["updated_at"])
    df = df.sort_values(
        by=["id", "updated_at_ts", "_dump_date"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["id"], keep="last")
    df["_is_deleted"] = df["status"].isin({"CLOSED", "SUSPENDED"})
    df["_last_seen_dump_date"] = df["_dump_date"]
    return df


def _dedup_accounts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = validate(df, ACCOUNTS_SCHEMA, "accounts")
    df["updated_at_ts"] = _coerce_ts(df["updated_at"])
    df = df.sort_values(
        by=["id", "updated_at_ts", "_dump_date"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["id"], keep="last")
    df["_last_seen_dump_date"] = df["_dump_date"]
    return df


def _dedup_transactions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["is_cross_dc"] = _coerce_bool(df["is_cross_dc"])
    df = validate(df, TRANSACTIONS_SCHEMA, "transactions")
    df["completed_at_ts"] = _coerce_ts(df["completed_at"])
    df["created_at_ts"] = _coerce_ts(df["created_at"])
    df = df.sort_values(
        by=["id", "reference", "_dump_date", "completed_at_ts"],
        ascending=[True, True, True, True],
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["id", "reference"], keep="last")
    df["_has_clock_skew"] = (
        df["completed_at_ts"].notna()
        & df["created_at_ts"].notna()
        & (df["completed_at_ts"] < df["created_at_ts"])
    )
    df["_is_cross_dc"] = df["is_cross_dc"].fillna(False).astype(bool)
    df["_last_seen_dump_date"] = df["_dump_date"]
    return df


def _dedup_fees(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = validate(df, FEES_SCHEMA, "fees")
    df["created_at_ts"] = _coerce_ts(df["created_at"])
    df = df.sort_values(
        by=["id", "transaction_reference", "_dump_date", "created_at_ts"],
        ascending=[True, True, True, True],
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["id", "transaction_reference"], keep="last")
    df["_last_seen_dump_date"] = df["_dump_date"]
    return df


def _truncate_silver() -> None:
    with pg_tx() as conn:
        conn.execute(
            text(
                "TRUNCATE silver.users, silver.accounts, silver.transactions, silver.fees"
            )
        )


def _df_to_pg(df: pd.DataFrame, table: str, schema: str = "silver", chunksize: int = 10_000) -> None:
    """Bulk-load via SQLAlchemy `to_sql` with method='multi' for speed."""
    eng = pg_engine()
    df.to_sql(
        table,
        eng,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=chunksize,
        method="multi",
    )


def _project_users(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "id", "nni", "full_name", "gender", "birth_date", "phone", "email",
        "wilaya_id", "wilaya_name", "profile_type", "kyc_level", "status",
        "registration_date", "last_login", "created_at", "updated_at",
        "_is_deleted", "_last_seen_dump_date",
    ]
    out = df.copy()
    out["wilaya_id"] = _coerce_int(out["wilaya_id"])
    out["birth_date"] = _coerce_date(out["birth_date"])
    out["registration_date"] = _coerce_date(out["registration_date"])
    out["last_login"] = _coerce_ts(out["last_login"])
    out["created_at"] = _coerce_ts(out["created_at"])
    out["updated_at"] = _coerce_ts(out["updated_at"])
    return out[cols]


def _project_accounts(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "id", "user_id", "account_number", "account_type", "currency", "balance",
        "available_balance", "daily_limit", "monthly_limit", "status",
        "is_primary", "opened_date", "last_activity", "created_at", "updated_at",
        "_last_seen_dump_date",
    ]
    out = df.copy()
    out["user_id"] = _coerce_int(out["user_id"])
    out["balance"] = _coerce_num(out["balance"])
    out["available_balance"] = _coerce_num(out["available_balance"])
    out["daily_limit"] = _coerce_num(out["daily_limit"]) if "daily_limit" in out else None
    out["monthly_limit"] = _coerce_num(out["monthly_limit"]) if "monthly_limit" in out else None
    out["is_primary"] = _coerce_bool(out["is_primary"]) if "is_primary" in out else False
    out["opened_date"] = _coerce_date(out["opened_date"]) if "opened_date" in out else None
    out["last_activity"] = _coerce_ts(out["last_activity"]) if "last_activity" in out else None
    out["created_at"] = _coerce_ts(out["created_at"])
    out["updated_at"] = _coerce_ts(out["updated_at"])
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]


def _project_transactions(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "id", "reference", "transaction_type", "amount", "fee", "total_amount",
        "currency", "source_account_id", "source_user_id", "destination_account_id",
        "destination_user_id", "merchant_id", "agency_id", "status", "failure_reason",
        "source_node", "processing_node", "source_datacenter", "processing_datacenter",
        "is_cross_dc", "transaction_date", "transaction_time", "created_at",
        "completed_at", "_has_clock_skew", "_is_cross_dc", "_last_seen_dump_date",
    ]
    out = df.copy()
    for c in [
        "source_account_id", "source_user_id", "destination_account_id",
        "destination_user_id", "merchant_id", "agency_id",
    ]:
        out[c] = _coerce_int(out[c])
    out["amount"] = _coerce_num(out["amount"])
    out["fee"] = _coerce_num(out["fee"])
    out["total_amount"] = _coerce_num(out["total_amount"])
    out["transaction_date"] = _coerce_date(out["transaction_date"])
    out["created_at"] = _coerce_ts(out["created_at"])
    out["completed_at"] = _coerce_ts(out["completed_at"])
    return out[cols]


def _project_fees(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "id", "transaction_reference", "transaction_id", "fee_type", "amount",
        "currency", "payer_account_id", "collector", "status", "created_at",
        "_last_seen_dump_date",
    ]
    out = df.copy()
    out["transaction_id"] = _coerce_int(out["transaction_id"])
    out["payer_account_id"] = _coerce_int(out["payer_account_id"])
    out["amount"] = _coerce_num(out["amount"])
    out["created_at"] = _coerce_ts(out["created_at"])
    return out[cols]


def run() -> None:
    log.info("silver.start")
    bundle = _read_layer()

    users = _dedup_users(bundle.users)
    accounts = _dedup_accounts(bundle.accounts)
    transactions = _dedup_transactions(bundle.transactions)
    fees = _dedup_fees(bundle.fees)

    log.info(
        f"silver.dedup users={len(users)} accounts={len(accounts)} "
        f"tx={len(transactions)} fees={len(fees)} "
        f"clock_skew={int(transactions['_has_clock_skew'].sum()) if len(transactions) else 0} "
        f"soft_deletes={int(users['_is_deleted'].sum()) if len(users) else 0}"
    )

    _truncate_silver()
    _df_to_pg(_project_users(users), "users")
    _df_to_pg(_project_accounts(accounts), "accounts")
    _df_to_pg(_project_transactions(transactions), "transactions")
    _df_to_pg(_project_fees(fees), "fees")
    log.info("silver.done")


if __name__ == "__main__":
    run()
