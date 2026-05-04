-- Warehouse schema bootstrap. Idempotent: safe to re-run.

CREATE SCHEMA IF NOT EXISTS bronze_meta;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze metadata (lineage / replay catalogue).
CREATE TABLE IF NOT EXISTS bronze_meta.bronze_runs (
    batch_id         UUID         NOT NULL,
    dump_date        DATE         NOT NULL,
    source_file      TEXT         NOT NULL,
    bronze_key       TEXT         NOT NULL,
    lines_read       INTEGER      NOT NULL,
    sha256           TEXT         NOT NULL,
    ingested_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (dump_date, source_file)
);

-- Silver entities (clean, deduped, anomaly-flagged).
CREATE TABLE IF NOT EXISTS silver.users (
    id                   INTEGER PRIMARY KEY,
    nni                  TEXT,
    full_name            TEXT,
    gender               TEXT,
    birth_date           DATE,
    phone                TEXT,
    email                TEXT,
    wilaya_id            INTEGER,
    wilaya_name          TEXT,
    profile_type         TEXT,
    kyc_level            TEXT,
    status               TEXT NOT NULL,
    registration_date    DATE,
    last_login           TIMESTAMPTZ,
    created_at           TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ,
    _is_deleted          BOOLEAN NOT NULL DEFAULT FALSE,
    _last_seen_dump_date DATE
);

CREATE TABLE IF NOT EXISTS silver.accounts (
    id                   INTEGER PRIMARY KEY,
    user_id              INTEGER,
    account_number       TEXT,
    account_type         TEXT,
    currency             TEXT,
    balance              NUMERIC(20,2),
    available_balance    NUMERIC(20,2),
    daily_limit          NUMERIC(20,2),
    monthly_limit        NUMERIC(20,2),
    status               TEXT NOT NULL,
    is_primary           BOOLEAN,
    opened_date          DATE,
    last_activity        TIMESTAMPTZ,
    created_at           TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ,
    _last_seen_dump_date DATE
);

CREATE TABLE IF NOT EXISTS silver.transactions (
    id                       BIGINT NOT NULL,
    reference                TEXT   NOT NULL,
    transaction_type         TEXT   NOT NULL,
    amount                   NUMERIC(20,2),
    fee                      NUMERIC(20,2),
    total_amount             NUMERIC(20,2),
    currency                 TEXT,
    source_account_id        INTEGER,
    source_user_id           INTEGER,
    destination_account_id   INTEGER,
    destination_user_id      INTEGER,
    merchant_id              INTEGER,
    agency_id                INTEGER,
    status                   TEXT NOT NULL,
    failure_reason           TEXT,
    source_node              TEXT,
    processing_node          TEXT,
    source_datacenter        TEXT,
    processing_datacenter    TEXT,
    is_cross_dc              BOOLEAN,
    transaction_date         DATE,
    transaction_time         TEXT,
    created_at               TIMESTAMPTZ,
    completed_at             TIMESTAMPTZ,
    _has_clock_skew          BOOLEAN NOT NULL DEFAULT FALSE,
    _is_cross_dc             BOOLEAN NOT NULL DEFAULT FALSE,
    _last_seen_dump_date     DATE,
    PRIMARY KEY (id, reference)
);

CREATE INDEX IF NOT EXISTS ix_silver_tx_date ON silver.transactions (transaction_date);
CREATE INDEX IF NOT EXISTS ix_silver_tx_status ON silver.transactions (status);
CREATE INDEX IF NOT EXISTS ix_silver_tx_clock_skew ON silver.transactions (_has_clock_skew) WHERE _has_clock_skew;

CREATE TABLE IF NOT EXISTS silver.fees (
    id                       BIGINT NOT NULL,
    transaction_reference    TEXT   NOT NULL,
    transaction_id           BIGINT,
    fee_type                 TEXT,
    amount                   NUMERIC(20,2),
    currency                 TEXT,
    payer_account_id         INTEGER,
    collector                TEXT,
    status                   TEXT,
    created_at               TIMESTAMPTZ,
    _last_seen_dump_date     DATE,
    PRIMARY KEY (id, transaction_reference)
);

-- Gold (business aggregates). Truncate-and-load on every run for full idempotence.
CREATE TABLE IF NOT EXISTS gold.daily_volume (
    transaction_date  DATE PRIMARY KEY,
    tx_count          BIGINT NOT NULL,
    success_count     BIGINT NOT NULL,
    failed_count      BIGINT NOT NULL,
    pending_count     BIGINT NOT NULL,
    total_amount      NUMERIC(24,2) NOT NULL,
    total_fees        NUMERIC(24,2) NOT NULL,
    cross_dc_count    BIGINT NOT NULL,
    clock_skew_count  BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.user_activity (
    user_id           INTEGER PRIMARY KEY,
    tx_count_30d      BIGINT NOT NULL,
    last_tx_date      DATE,
    active_flag       BOOLEAN NOT NULL,
    is_deleted        BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.agency_perf (
    agency_id              INTEGER PRIMARY KEY,
    tx_count               BIGINT NOT NULL,
    success_count          BIGINT NOT NULL,
    failed_count           BIGINT NOT NULL,
    total_amount           NUMERIC(24,2) NOT NULL,
    total_fees_collected   NUMERIC(24,2) NOT NULL
);
