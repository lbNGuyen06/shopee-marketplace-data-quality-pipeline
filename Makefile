COMPOSE ?= docker compose

.PHONY: postgres-up postgres-down postgres-logs postgres-smoke \
	airflow-build airflow-up airflow-logs airflow-dags

postgres-up:
	$(COMPOSE) up -d --wait postgres

postgres-down:
	$(COMPOSE) down

postgres-logs:
	$(COMPOSE) logs postgres

postgres-smoke:
	$(COMPOSE) exec -T postgres sh -c 'psql \
		-v ON_ERROR_STOP=1 \
		-U "$$POSTGRES_USER" \
		-d "$$POSTGRES_DB" \
		-f /workspace/sql/tests/postgres_smoke_test.sql'

airflow-build:
	$(COMPOSE) --profile airflow build airflow

airflow-up:
	$(COMPOSE) --profile airflow up -d --wait airflow

airflow-logs:
	$(COMPOSE) --profile airflow logs airflow

airflow-dags:
	$(COMPOSE) --profile airflow exec -T airflow airflow dags list
	$(COMPOSE) --profile airflow exec -T airflow \
		airflow dags list-import-errors
	$(COMPOSE) --profile airflow exec -T airflow \
		airflow tasks list shopee_marketplace_quality
