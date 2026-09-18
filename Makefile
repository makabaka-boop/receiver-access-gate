.PHONY: build test test-integration verify-local up down

build:
	go build ./...

test:
	go test ./...

# Requires a running PostgreSQL, e.g. docker compose up -d postgres.
test-integration:
	GATE_TEST_DATABASE_URL="$${GATE_TEST_DATABASE_URL:-postgres://gate:gate@localhost:5432/gate?sslmode=disable}" \
		go test -race -count=1 ./internal/integration/

up:
	docker compose up -d --build

# One-shot acceptance against the two running API containers.
verify:
	docker compose run --rm verify

down:
	docker compose down
