# quant_execution

Execution layer of the algo trading platform. It loads a watchlist of `(ticker, buy_price)`
entries from `quant_stickynote`, consumes streaming price ticks from Kafka, and when a tick
matches a watchlist buy price it places an order through Alpaca and records the order in a
Postgres `trades` table.

Two modes run **concurrently** as separate processes sharing one code path:

- `--mode paper` — assumes fills, skips the cash check, consumes the paper topic.
- `--mode live` — checks cash via `quant_cash`, submits to Alpaca, and confirms fills.

The mode is a per-process command-line argument, not an environment variable. See `SPEC.md` for
the full design.

## Requirements

- Python 3.12
- A reachable Postgres database (shared; this service owns only the `trades` table)

## Install

```bash
pip install .[dev]
```

## Migrate

```bash
export DATABASE_URL=postgresql+psycopg://app:app@localhost:5432/app
alembic upgrade head
```

Migrations use a service-specific version table (`alembic_version_quant_execution`) and a
Postgres advisory lock so concurrent migrators are safe.

## Run (locally, without containers)

```bash
export DATABASE_URL=postgresql+psycopg://app:app@localhost:5432/app

# Health API
python -m quant_execution

# Executors (separate terminals)
python -m quant_execution.workers.executor --mode paper
python -m quant_execution.workers.executor --mode live
```

## Test / lint / type / security

```bash
pytest -q          # runs with coverage; gate is >= 80%
ruff check .
mypy src
bandit -r src
```

## Docker

```bash
docker build -t quant-execution:dev .
docker compose up
```

The container is managed by `supervisord`, which runs four programs: `migrate` (one-shot),
`api`, `executor_paper`, and `executor_live`. All environment variables are defined inline in
`docker-compose.yml`.

## Health

- `GET /health` — process alive.
- `GET /ready` — returns 200 when the database is reachable **and** the required downstream
  config is present (watchlist URL always; cash URL/account and Alpaca credentials for the live
  executor). Otherwise 503 with the standard error envelope. Readiness never leaks secrets.

## Observability

Executions emit a structured logfmt line per trade, for example:

```
trade_executed symbol=AAPL mode=paper idempotency_key=paper:AAPL:dip:2026-08-28 status=filled duration_ms=3.1
```

Fields include `symbol`, `mode`, `idempotency_key`, `broker_order_id`, `status`, and
`duration_ms`. Logs go to stdout only; the container captures them via `supervisord`.

