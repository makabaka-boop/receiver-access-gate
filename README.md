# Receiver access gate

Radio-array receivers are watched by several analysts at once (**SHARED**
access) but a retuning engineer must have **EXCLUSIVE** access. This service
is the consistent gate between multiple stateless API processes, backed by
PostgreSQL.

## Rules

| Request  | Succeeds only when                                   |
|----------|------------------------------------------------------|
| SHARED   | no ACTIVE `EXCLUSIVE` grant exists                   |
| EXCLUSIVE| no ACTIVE grant of **any** kind exists               |

- A conflict always returns `409 BUSY` and inserts nothing.
- A successful creation returns a random owner token **once**; only its
  SHA-256 digest is stored.
- Releasing with the correct token moves ACTIVE → RELEASED (replaying the
  release is idempotent); a wrong token returns `403 FORBIDDEN` and changes
  nothing.
- Query responses contain grant id, receiver, mode and status only — never
  the token or its digest.
- The conflict check and the write execute in one database transaction that
  first takes a transaction-scoped advisory lock keyed by the receiver, so
  two API processes can never create double exclusives, and state/token
  validity survive full process restarts.

## API

```
POST /v1/receivers/{receiver}/grants
     body: {"mode": "SHARED" | "EXCLUSIVE"}
     -> 201 {grant_id, receiver, mode, status, owner_token, created_at}
     -> 409 {"error":"BUSY", ...}            (also 400 for bad input)

POST /v1/receivers/{receiver}/grants/{grant_id}/release
     body: {"owner_token": "tkn_..."}
     -> 200 {grant_id, receiver, mode, status, created_at}
     -> 403 {"error":"FORBIDDEN", ...}       (wrong token, nothing changes)
     -> 404 NOT_FOUND                        (unknown grant / receiver)

GET  /v1/receivers/{receiver}/grants
     -> 200 {"receiver": "...", "grants": [{grant_id, mode, status, created_at}, ...]}

GET  /healthz
```

## Configuration

| Env            | Default   | Meaning                       |
|----------------|-----------|-------------------------------|
| `DATABASE_URL` | (required)| PostgreSQL connection string  |
| `LISTEN_ADDR`  | `:8080`   | HTTP listen address           |

## Run with Docker Compose

```sh
docker compose up -d --build                 # postgres + two API processes
curl -s localhost:8081/healthz

docker compose run --rm verify               # one-shot acceptance, exits 0 on success
```

Host ports are overridable:

```sh
PORT_API1=18081 PORT_API2=18082 PORT_POSTGRES=15432 docker compose up -d
```

The `verify` service hits **both** real API containers and checks: the
two-process concurrent exclusive race (exactly one winner among 60
requests), SHARED coexistence and conflict rules, cross-process wrong-token
403 protection, and stable token-free queries. It uses unique receiver names
per run, so it is safe to re-run.

## Local development

Go 1.23+, PostgreSQL 14+ (advisory locks + `hashtextextended`).

```sh
go test ./...
# Real-database integration tests (two in-process HTTP servers, race detector):
GATE_TEST_DATABASE_URL='postgres://user@localhost:5432/gate?sslmode=disable' \
    go test -race -count=1 ./internal/integration/
```

Without `GATE_TEST_DATABASE_URL` the integration package skips. The test
suites cover the two-instance exclusive/mixed races, shared coexistence,
unauthorized release invariants, token non-disclosure, and state/token
persistence through freshly created processes.

## Layout

```
cmd/accessgate/          HTTP service entry point
cmd/verify/              one-shot cross-process acceptance program
internal/store/          PostgreSQL transactions, advisory locking, tokens
internal/httpapi/        HTTP handlers
internal/integration/    Go tests over two real API instances
Dockerfile               multi-stage: api + verify targets
docker-compose.yml       postgres, api1, api2, verify
```
