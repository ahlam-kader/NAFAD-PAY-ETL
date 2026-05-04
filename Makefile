SHELL := /bin/bash
COMPOSE := docker compose
RUN := $(COMPOSE) run --rm runner

.PHONY: help up down build init bronze silver gold pipeline snapshot test idempotence anomalies clean logs ps psql minio-ui

help:
	@echo "NAFAD-PAY G3 ETL - Medallion pipeline"
	@echo ""
	@echo "Stack lifecycle:"
	@echo "  make up           - start postgres + minio"
	@echo "  make down         - stop everything"
	@echo "  make clean        - down + remove volumes (WIPES the warehouse)"
	@echo "  make build        - (re)build the runner image"
	@echo ""
	@echo "Pipeline:"
	@echo "  make init         - apply DDL"
	@echo "  make bronze       - ingest CSVs to MinIO"
	@echo "  make silver       - dedupe + flag + load Postgres"
	@echo "  make gold         - aggregates"
	@echo "  make pipeline     - init + bronze + silver + gold"
	@echo "  make snapshot     - hash silver/gold and print"
	@echo ""
	@echo "Tests:"
	@echo "  make test         - run pytest suite"
	@echo "  make idempotence  - run pipeline twice, compare snapshots"
	@echo "  make anomalies    - print anomaly counts from silver"
	@echo ""
	@echo "Debug:"
	@echo "  make logs         - tail logs"
	@echo "  make ps           - status"
	@echo "  make psql         - psql shell"
	@echo "  make minio-ui     - print MinIO console URL"

up:
	$(COMPOSE) up -d warehouse minio

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

build:
	$(COMPOSE) build runner

init: up build
	$(RUN) init

bronze: up build
	$(RUN) bronze

silver: up build
	$(RUN) silver

gold: up build
	$(RUN) gold

pipeline: up build
	$(RUN) all

snapshot: up build
	$(RUN) snapshot

test: up build
	$(COMPOSE) run --rm --entrypoint pytest runner -v /app/tests

idempotence: pipeline
	@echo "=== run #1 snapshot ==="
	@$(RUN) snapshot > /tmp/g3_snap_1.json
	@cat /tmp/g3_snap_1.json | tail -n 5
	@echo "=== re-running pipeline ==="
	@$(RUN) all > /dev/null
	@echo "=== run #2 snapshot ==="
	@$(RUN) snapshot > /tmp/g3_snap_2.json
	@cat /tmp/g3_snap_2.json | tail -n 5
	@diff -q /tmp/g3_snap_1.json /tmp/g3_snap_2.json \
	  && echo "OK: bit-exact identical state" \
	  || (echo "FAIL: snapshots differ"; diff /tmp/g3_snap_1.json /tmp/g3_snap_2.json; exit 1)

anomalies: up build
	@$(COMPOSE) exec -T warehouse psql -U $${PG_USER:-nafad} -d $${PG_DB:-warehouse} -c "\
SELECT 'transactions_total' AS k, COUNT(*)::text AS v FROM silver.transactions UNION ALL \
SELECT 'clock_skew', COUNT(*)::text FROM silver.transactions WHERE _has_clock_skew UNION ALL \
SELECT 'cross_dc', COUNT(*)::text FROM silver.transactions WHERE _is_cross_dc UNION ALL \
SELECT 'soft_deleted_users', COUNT(*)::text FROM silver.users WHERE _is_deleted UNION ALL \
SELECT 'users_total', COUNT(*)::text FROM silver.users UNION ALL \
SELECT 'accounts_total', COUNT(*)::text FROM silver.accounts UNION ALL \
SELECT 'fees_total', COUNT(*)::text FROM silver.fees;"

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

psql:
	$(COMPOSE) exec warehouse psql -U $${PG_USER:-nafad} -d $${PG_DB:-warehouse}

minio-ui:
	@echo "MinIO console: http://localhost:$${MINIO_CONSOLE_PORT:-9001}  (login: minioadmin / minioadmin123)"
