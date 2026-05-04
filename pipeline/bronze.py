from __future__ import annotations
import csv
import hashlib
import io
import uuid
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from .common.config import CONFIG
from .common.io import ensure_bucket, pg_tx, put_object, wait_for_pg, wait_for_s3
from .common.logging_setup import get_logger

log = get_logger(__name__)

BRONZE_NAMESPACE = uuid.UUID("00000000-0000-0000-0000-000000004242")  # seed 42
MAX_DUMP_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB safety net


def _deterministic_batch_id(dump_date: str, filename: str) -> uuid.UUID:
    return uuid.uuid5(BRONZE_NAMESPACE, f"{dump_date}/{filename}")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _enrich_with_metadata(raw_bytes: bytes, source_file: str, batch_id: str, dump_date: str) -> bytes:
    text_in = raw_bytes.decode("utf-8")
    reader = csv.reader(io.StringIO(text_in))
    rows = list(reader)
    if not rows:
        return raw_bytes
    header, *data = rows
    extra = ["_ingested_at", "_source_file", "_batch_id"]
    new_header = header + extra
    pinned_ts = f"{dump_date}T00:00:00Z"
    new_rows = [row + [pinned_ts, source_file, batch_id] for row in data]
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(new_header)
    writer.writerows(new_rows)
    return out.getvalue().encode("utf-8")


def _bronze_key(dump_date: str, filename: str) -> str:
    return f"dt={dump_date}/{filename}"


def _ingest_one(dump_date: str, file_path: Path) -> dict:
    if file_path.stat().st_size > MAX_DUMP_BYTES:
        raise RuntimeError(f"dump too large (>10GB), refusing: {file_path}")

    raw = file_path.read_bytes()
    line_count = max(0, raw.count(b"\n") - 1) 
    batch_id = str(_deterministic_batch_id(dump_date, file_path.name))
    enriched = _enrich_with_metadata(raw, str(file_path.name), batch_id, dump_date)
    sha = _sha256(enriched)
    key = _bronze_key(dump_date, file_path.name)

    put_object(
        CONFIG.bronze_bucket,
        key,
        enriched,
        metadata={
            "x-amz-meta-batch-id": batch_id,
            "x-amz-meta-dump-date": dump_date,
            "x-amz-meta-sha256": sha,
        },
    )

    return {
        "batch_id": batch_id,
        "dump_date": dump_date,
        "source_file": file_path.name,
        "bronze_key": key,
        "lines_read": line_count,
        "sha256": sha,
        "ingested_at": f"{dump_date}T00:00:00Z",
    }


def _record_run(rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO bronze_meta.bronze_runs
          (batch_id, dump_date, source_file, bronze_key, lines_read, sha256, ingested_at)
        VALUES
          (:batch_id, :dump_date, :source_file, :bronze_key, :lines_read, :sha256, :ingested_at)
        ON CONFLICT (dump_date, source_file) DO UPDATE SET
          batch_id    = EXCLUDED.batch_id,
          bronze_key  = EXCLUDED.bronze_key,
          lines_read  = EXCLUDED.lines_read,
          sha256      = EXCLUDED.sha256,
          ingested_at = EXCLUDED.ingested_at
        """
    )
    with pg_tx() as conn:
        conn.execute(sql, rows)


def run() -> None:
    log.info("bronze.start")
    wait_for_s3()
    wait_for_pg()
    ensure_bucket(CONFIG.bronze_bucket)
    ensure_bucket(CONFIG.quarantine_bucket)

    all_rows: list[dict] = []
    for dump_date in CONFIG.dump_dates:
        day_dir = CONFIG.daily_dumps_dir / dump_date
        if not day_dir.is_dir():
            log.warning(f"bronze.skip dump_date={dump_date} reason=missing_dir")
            continue
        for csv_path in sorted(day_dir.glob("*.csv")):
            row = _ingest_one(dump_date, csv_path)
            all_rows.append(row)
            log.info(
                f"bronze.ingest dump_date={dump_date} file={csv_path.name} "
                f"lines={row['lines_read']} sha256={row['sha256'][:12]}"
            )

    _record_run(all_rows)
    log.info(f"bronze.done files={len(all_rows)}")


if __name__ == "__main__":
    run()
