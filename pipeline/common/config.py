"""Centralised runtime config — read from env vars, with sensible local defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    daily_dumps_dir: Path
    shared_dir: Path

    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_use_ssl: bool
    s3_use_iam_role: bool
    bronze_bucket: str
    quarantine_bucket: str

    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str

    dump_dates: tuple[str, ...]


def _env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def load_config() -> Config:
    base = Path(_env("PROJECT_ROOT", "/app/pipeline")).resolve()
    return Config(
        daily_dumps_dir=Path(_env("DAILY_DUMPS_DIR", str(base.parent / "daily_dumps"))),
        shared_dir=Path(_env("SHARED_DIR", str(base.parent / "shared"))),
        s3_endpoint=_env("S3_ENDPOINT", "http://minio:9000"),
        s3_access_key=_env("S3_ACCESS_KEY", "minioadmin"),
        s3_secret_key=_env("S3_SECRET_KEY", "minioadmin123"),
        s3_region=_env("S3_REGION", "eu-west-3"),
        s3_use_ssl=_env("S3_USE_SSL", "false").lower() == "true",
        s3_use_iam_role=_env("S3_USE_IAM_ROLE", "false").lower() == "true",
        bronze_bucket=_env("BRONZE_BUCKET", "nafad-bronze"),
        quarantine_bucket=_env("QUARANTINE_BUCKET", "nafad-quarantine"),
        pg_host=_env("PG_HOST", "warehouse"),
        pg_port=int(_env("PG_PORT", "5432")),
        pg_db=_env("PG_DB", "warehouse"),
        pg_user=_env("PG_USER", "nafad"),
        pg_password=_env("PG_PASSWORD", "nafad_dev_password"),
        dump_dates=tuple(
            _env(
                "DUMP_DATES",
                "2024-01-15,2024-01-16,2024-01-17,2024-01-18,2024-01-19,2024-01-20,2024-01-21",
            ).split(",")
        ),
    )


CONFIG = load_config()
