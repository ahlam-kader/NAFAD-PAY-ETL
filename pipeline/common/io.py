"""Lightweight S3 / Postgres helpers used by every layer."""
from __future__ import annotations

import io
import time
from contextlib import contextmanager
from typing import Iterator

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError, EndpointConnectionError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import CONFIG
from .logging_setup import get_logger

log = get_logger(__name__)


def s3_client():
    """Return a boto3 S3 client.

    On AWS (ECS, EC2, Lambda) we want boto3 to pick credentials from the
    instance/task IAM role. Passing explicit `aws_access_key_id=` would
    *override* the role chain — so we only pass keys when the user has
    explicitly set `S3_USE_IAM_ROLE=false` (the local MinIO case).
    Likewise, on real AWS we omit `endpoint_url` to let boto3 resolve the
    regional S3 endpoint (s3.<region>.amazonaws.com).
    """
    use_iam_role = CONFIG.s3_use_iam_role
    kwargs = {
        "region_name": CONFIG.s3_region,
        "use_ssl": CONFIG.s3_use_ssl,
        "config": BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    }
    if not use_iam_role:
        kwargs["endpoint_url"] = CONFIG.s3_endpoint
        kwargs["aws_access_key_id"] = CONFIG.s3_access_key
        kwargs["aws_secret_access_key"] = CONFIG.s3_secret_key
    return boto3.client("s3", **kwargs)


def wait_for_s3(timeout_s: int = 60) -> None:
    """Block until MinIO/S3 answers — useful at container start-up."""
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            s3_client().list_buckets()
            return
        except (EndpointConnectionError, ClientError) as exc:
            last_err = exc
            time.sleep(1)
    raise RuntimeError(f"S3 endpoint not reachable: {last_err}")


def ensure_bucket(bucket: str) -> None:
    cli = s3_client()
    try:
        cli.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            log.info(f"creating bucket={bucket}")
            cli.create_bucket(Bucket=bucket)
        else:
            raise


def put_object(bucket: str, key: str, body: bytes, metadata: dict[str, str] | None = None) -> None:
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        Metadata=metadata or {},
        ContentType="text/csv",
    )


def get_object_bytes(bucket: str, key: str) -> bytes:
    return s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()


def list_keys(bucket: str, prefix: str) -> Iterator[str]:
    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def read_csv_from_s3(bucket: str, key: str, **read_csv_kwargs) -> pd.DataFrame:
    body = get_object_bytes(bucket, key)
    return pd.read_csv(io.BytesIO(body), **read_csv_kwargs)


def pg_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{CONFIG.pg_user}:{CONFIG.pg_password}"
        f"@{CONFIG.pg_host}:{CONFIG.pg_port}/{CONFIG.pg_db}"
    )
    return create_engine(url, pool_pre_ping=True, future=True)


def wait_for_pg(timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with pg_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(1)
    raise RuntimeError(f"Postgres not reachable: {last_err}")


@contextmanager
def pg_tx():
    eng = pg_engine()
    with eng.begin() as conn:
        yield conn
