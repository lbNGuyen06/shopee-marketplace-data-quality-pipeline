COMPOSE ?= docker compose

.PHONY: postgres-up postgres-down postgres-logs postgres-smoke

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
