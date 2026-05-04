"""Pandera schemas — minimal but enforced at the Bronze→Silver boundary."""
from __future__ import annotations

import pandera as pa
from pandera import Column, DataFrameSchema

USERS_SCHEMA = DataFrameSchema(
    {
        "id": Column(int, nullable=False),
        "nni": Column(str, nullable=True, coerce=True),
        "first_name": Column(str, nullable=True, coerce=True),
        "last_name": Column(str, nullable=True, coerce=True),
        "full_name": Column(str, nullable=True, coerce=True),
        "gender": Column(str, nullable=True, coerce=True),
        "birth_date": Column(str, nullable=True, coerce=True),
        "phone": Column(str, nullable=True, coerce=True),
        "email": Column(str, nullable=True, coerce=True),
        "wilaya_id": Column(int, nullable=True, coerce=True),
        "wilaya_name": Column(str, nullable=True, coerce=True),
        "profile_type": Column(str, nullable=True, coerce=True),
        "kyc_level": Column(str, nullable=True, coerce=True),
        "status": Column(str, nullable=False, coerce=True),
        "registration_date": Column(str, nullable=True, coerce=True),
        "last_login": Column(str, nullable=True, coerce=True),
        "created_at": Column(str, nullable=True, coerce=True),
        "updated_at": Column(str, nullable=True, coerce=True),
    },
    strict=False,
    coerce=True,
)

ACCOUNTS_SCHEMA = DataFrameSchema(
    {
        "id": Column(int, nullable=False),
        "user_id": Column(int, nullable=True, coerce=True),
        "account_number": Column(str, nullable=True, coerce=True),
        "account_type": Column(str, nullable=True, coerce=True),
        "currency": Column(str, nullable=True, coerce=True),
        "balance": Column(float, nullable=True, coerce=True),
        "available_balance": Column(float, nullable=True, coerce=True),
        "status": Column(str, nullable=False, coerce=True),
        "created_at": Column(str, nullable=True, coerce=True),
        "updated_at": Column(str, nullable=True, coerce=True),
    },
    strict=False,
    coerce=True,
)

TRANSACTIONS_SCHEMA = DataFrameSchema(
    {
        "id": Column(int, nullable=False),
        "reference": Column(str, nullable=False, coerce=True),
        "transaction_type": Column(str, nullable=False, coerce=True),
        "amount": Column(float, nullable=True, coerce=True),
        "fee": Column(float, nullable=True, coerce=True),
        "total_amount": Column(float, nullable=True, coerce=True),
        "currency": Column(str, nullable=True, coerce=True),
        "status": Column(str, nullable=False, coerce=True),
        "source_node": Column(str, nullable=True, coerce=True),
        "processing_node": Column(str, nullable=True, coerce=True),
        "source_datacenter": Column(str, nullable=True, coerce=True),
        "processing_datacenter": Column(str, nullable=True, coerce=True),
        "is_cross_dc": Column(bool, nullable=True, coerce=True),
        "transaction_date": Column(str, nullable=True, coerce=True),
        "created_at": Column(str, nullable=True, coerce=True),
        "completed_at": Column(str, nullable=True, coerce=True),
    },
    strict=False,
    coerce=True,
)

FEES_SCHEMA = DataFrameSchema(
    {
        "id": Column(int, nullable=False),
        "transaction_id": Column(int, nullable=True, coerce=True),
        "transaction_reference": Column(str, nullable=True, coerce=True),
        "fee_type": Column(str, nullable=True, coerce=True),
        "amount": Column(float, nullable=True, coerce=True),
        "currency": Column(str, nullable=True, coerce=True),
        "status": Column(str, nullable=True, coerce=True),
        "created_at": Column(str, nullable=True, coerce=True),
    },
    strict=False,
    coerce=True,
)


def validate(df, schema: DataFrameSchema, label: str):
    """Validate (lazy) and let the caller handle the exception with context."""
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        raise ValueError(f"schema validation failed for {label}: {exc}") from exc
