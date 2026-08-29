# Quant Execution — Project Specification

**Project**: `quant_execution`
**Layer**: Execution layer of the algo trading platform
**Status**: Specification Document (no development started)
**Version**: 1.0
**Date**: 2026-08-28

Built to the platform **Backend Coding Standards** (Python 3.12, Bottle + Waitress,
SQLAlchemy 2.0, Alembic, Docker, supervisord, Pydantic Settings):
<https://github.com/mayberryjp/coding_standards/blob/main/BACKEND_CODING_STANDARDS.md>

Reference services this project integrates with:
- **Cash API** (`quant_cash`) — account balances + holds: <https://github.com/mayberryjp/quant_cash>
- **Watchlist** (`quant_stickynote`) — tickers with buy prices: <https://github.com/mayberryjp/quant_stickynote>

---

## 1. Overview

### 1.1 Purpose
`quant_execution` is the order-execution layer. It:

1. Loads a **watchlist** of `(ticker, buy_price)` entries from `quant_stickynote` and keeps them
   in memory.
2. Subscribes to a **Kafka** topic of streaming price ticks.
3. When an incoming tick **matches** a watchlist buy price, it **places an order** through
   **Alpaca**.
4. Records every order in a **`trades`** table in Postgres.

### 1.2 Two modes: `paper` and `live` (run concurrently)
**Both modes run at the same time**, side by side, as two separate long-running processes in the
same container. They are the **same executable running the same code path**; the mode is selected
**per process** by a command-line argument — **not** a container-level environment variable (a
single env var would be ambiguous since both run simultaneously):

```
python -m quant_execution.workers.executor --mode paper
python -m quant_execution.workers.executor --mode live
```

The `--mode` argument changes only four things:

| Concern | `--mode paper` | `--mode live` |
|---|---|---|
| Kafka topic consumed | `EXEC_KAFKA_TOPIC_PAPER` | `EXEC_KAFKA_TOPIC_LIVE` |
| Trade record flag | `is_paper = true` | `is_paper = false` |
| Cash check before ordering | **skipped** | **required** (via `quant_cash`) |
| Fill confirmation | **assumed filled** immediately | **polled** from Alpaca, DB updated |

Everything else — watchlist loading, matching logic, order construction, DB persistence,
idempotency, logging, health — is **identical** across modes and shared by both processes.

All other configuration is shared container-level env (topics, DB, API URLs). Only the mode differs
between the two processes, and it is passed as the `--mode` CLI argument.

**supervisord runs both `paper` and `live` processes** concurrently in the same container (two
long-running programs, plus a one-shot migration program and the health API).

### 1.3 Alpaca note
Order execution is done through Alpaca. There is **no Alpaca account yet**, so in practice the
live process cannot actually fill (its Alpaca calls will fail readiness until credentials exist),
while the paper process runs fully. The **full live path is wired** regardless. Note the
terminology distinction:
- **`--mode` (`paper`/`live`)** — this platform's trade-routing mode (topic, cash,
  fill confirmation), selected per process at launch.
- **Alpaca environment** — Alpaca's own base URL / keys (its own paper vs. live endpoints),
  configured separately via `EXEC_ALPACA_*`. Until real keys exist, the Alpaca client points at
  Alpaca's paper base URL.

These two are independent knobs.

---

## 2. Architecture

```
                          ┌─────────────────────────────────────────────┐
                          │            quant_execution container          │
                          │                                              │
 quant_stickynote  ─────► │  Watchlist loader (in-memory ticker→prices)  │
 (GET /sticky-notes)      │        ▲ periodic refresh                     │
                          │        │                                      │
   Kafka (paper topic) ─► │  executor_paper  ─┐                           │
   Kafka (live  topic) ─► │  executor_live   ─┼─► Matching engine         │
                          │                    │         │                 │
                          │                    │         ▼                 │
 quant_cash (live only) ◄─┤  ExecutionService  ├─► Order build + sizing    │
 (balance / hold)         │                    │         │                 │
                          │                    │         ▼                 │
   Alpaca API        ◄────┤  AlpacaBroker  ────┘   Persist Trade (Postgres)│
   (submit / status)      │                                              │
                          │  api (GET /health, /ready)                    │
                          │  migrate (alembic upgrade head, one-shot)     │
                          └─────────────────────────────────────────────┘
```

### 2.1 Processes (supervisord programs)
1. **`migrate`** — runs `alembic upgrade head` once at startup, then exits (`priority=1`,
   `autorestart=false`).
2. **`api`** — Bottle + Waitress health/readiness server (`priority=10`).
3. **`executor_paper`** — Kafka consumer, `command = python -m quant_execution.workers.executor --mode paper` (`priority=20`).
4. **`executor_live`** — Kafka consumer, `command = python -m quant_execution.workers.executor --mode live` (`priority=20`).

> Both executors run **concurrently**. The mode is a **per-process CLI argument** (`--mode`), so the
> two processes share the same container-level environment and differ only by that argument.

### 2.2 Repository layout (per Backend Coding Standards §2)
```
quant_execution/
  .github/workflows/
    ci.yml
    docker-publish.yml
  alembic/
    env.py                     # service-specific version table + advisory lock
    script.py.mako
    versions/
  src/quant_execution/
    api/
      app.py                   # create_app(), Bottle factory
      routes/
        health.py              # GET /health, GET /ready
    clients/
      alpaca_client.py         # AlpacaBroker: submit_order(), get_order()
      cash_client.py           # quant_cash HTTP client (balance, hold)
      watchlist_client.py      # quant_stickynote HTTP client (GET /sticky-notes)
    domain/
      enums.py                 # ExecutionMode, OrderSide, TradeStatus, PositionType
      exceptions.py            # DomainError hierarchy
      matching.py              # WatchlistStore + match rules
      schemas.py               # Pydantic: Tick, OrderIntent, TradeResponse
      services.py              # ExecutionService (mode-agnostic core)
    kafka/
      consumer.py              # consumer factory + graceful shutdown
    repository/
      models.py                # Trade ORM model (Base.metadata)
      trades_repo.py           # parameterized SQL helpers
    workers/
      executor.py              # main() — mode-driven Kafka loop
    config.py                  # Pydantic Settings (env_prefix="EXEC_")
    db.py                      # engine, session_scope(), check_database()
    logging.py                 # configure_logging(), get_logger()
    __init__.py
    __main__.py                # api entrypoint (waitress serve)
  tests/
    conftest.py
    test_health.py
    test_ready.py
    test_matching.py
    test_execution_service.py
    test_migration_smoke.py
  .dockerignore
  .gitignore
  alembic.ini
  docker-compose.yml
  Dockerfile
  pyproject.toml
  README.md
  supervisord.conf
```

---

## 3. Data Model

### 3.1 Ownership
The Postgres database is **shared**. This service **owns and creates the `trades` table** (it does
not exist yet) and only touches its own tables. It uses a **service-specific Alembic version table**
`alembic_version_quant_execution` (never the shared default `alembic_version`).

### 3.2 `trades` table
| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | generated app-side |
| `execution_mode` | VARCHAR(8) | `paper` \| `live` |
| `is_paper` | BOOLEAN NOT NULL | **required flag** for paper vs. live (indexed) |
| `symbol` | VARCHAR(16) NOT NULL | ticker |
| `side` | VARCHAR(8) NOT NULL | `BUY` \| `SELL` (entry side; derived from `position_type`) |
| `position_type` | VARCHAR(8) NOT NULL | `LONG` \| `SHORT` (from watchlist entry) |
| `target_buy_price` | NUMERIC(20,8) NOT NULL | watchlist buy price that armed the trade |
| `trigger_price` | NUMERIC(20,8) NOT NULL | the tick price that matched |
| `quantity` | NUMERIC(20,8) NOT NULL | order size (see §5.3 sizing) |
| `notional` | NUMERIC(20,8) NOT NULL | `quantity * trigger_price` |
| `currency` | CHAR(3) NOT NULL | default `USD` |
| `status` | VARCHAR(24) NOT NULL | see §3.3 lifecycle |
| `broker` | VARCHAR(16) NOT NULL | `alpaca` |
| `broker_order_id` | VARCHAR(64) NULL | Alpaca order id (null until submitted) |
| `filled_quantity` | NUMERIC(20,8) NULL | from Alpaca (live); = quantity (paper) |
| `filled_avg_price` | NUMERIC(20,8) NULL | from Alpaca (live); = trigger_price (paper) |
| `cash_hold_id` | UUID NULL | `quant_cash` hold id (live only) |
| `source_query_id` | VARCHAR(64) NULL | from watchlist entry |
| `trigger_reason` | VARCHAR(255) NULL | from watchlist entry |
| `idempotency_key` | VARCHAR(200) NOT NULL | **UNIQUE**; dedup guard (see §5.4) |
| `kafka_topic` | VARCHAR(128) NULL | provenance |
| `kafka_partition` | INTEGER NULL | provenance |
| `kafka_offset` | BIGINT NULL | provenance |
| `error_code` | VARCHAR(64) NULL | populated on failure |
| `error_detail` | TEXT NULL | populated on failure |
| `created_at` | TIMESTAMPTZ NOT NULL | default now() |
| `submitted_at` | TIMESTAMPTZ NULL | when sent to broker |
| `filled_at` | TIMESTAMPTZ NULL | when confirmed filled |
| `updated_at` | TIMESTAMPTZ NOT NULL | default now(), on update |

Indexes: `idx_trades_symbol`, `idx_trades_status`, `idx_trades_is_paper`,
`idx_trades_created_at`, `uq_trades_idempotency_key` (unique).

> `is_paper` is the explicit required column for "paper or live". `execution_mode` is kept
> alongside as a readable string; both derive from the same flag.

### 3.3 Trade status lifecycle
```
NEW ─► CASH_HELD ─► SUBMITTED ─► FILLED
  │        │            │      └► PARTIALLY_FILLED ─► FILLED
  │        │            └► REJECTED
  │        └► INSUFFICIENT_FUNDS   (live only, terminal)
  └► FAILED  (any unexpected error, terminal)
```
- **paper**: `NEW → SUBMITTED → FILLED` recorded in one transaction (fill assumed). `CASH_HELD`
  and `INSUFFICIENT_FUNDS` never occur in paper.
- **live**: `NEW → CASH_HELD → SUBMITTED`, then a reconciliation loop polls Alpaca and advances to
  `FILLED` / `PARTIALLY_FILLED` / `REJECTED`. On terminal reject/cancel, the cash hold is released.

After an entry fills, the position is **held** until it exits (§5.7):
```
FILLED ─► EXIT_SUBMITTED ─► CLOSED
              │          └► EXIT_PARTIALLY_FILLED ─► CLOSED
              └► EXIT_FAILED ─► (position reopened, retried on next trigger / at close)
```
- **paper** exits record `EXIT_SUBMITTED → CLOSED` immediately (fill assumed).
- **live** exits are confirmed by the same reconciliation loop.

---

## 4. External Integrations

### 4.1 Watchlist — `quant_stickynote`
- `GET /sticky-notes?status=active&limit=...&offset=...` → list of
  `{ symbol, buy_price, position_type, source_query_id, trigger_reason, status }`.
- Loaded fully into an in-memory `WatchlistStore` once at startup, then reloaded once per
  session `EXEC_MARKET_OPEN_LEAD_MINUTES_{PAPER,LIVE}` (default 15) **before** market open (per
  mode, `EXEC_MARKET_OPEN_PAPER` / `EXEC_MARKET_OPEN_LIVE` in `EXEC_MARKET_TIMEZONE`). The startup
  load keeps a restart from running with an empty watchlist; the scheduled reload picks up the
  latest sticky notes right before trading begins. `EXEC_WATCHLIST_REFRESH_SECONDS` is only the
  clock-check cadence of that scheduler, not an unconditional refresh interval; an empty open time
  disables the scheduled reload (startup load only). Refresh is atomic (build new snapshot, swap
  reference).
- A symbol may have multiple entries (different `trigger_reason`); all are armed independently.

### 4.2 Cash — `quant_cash` (live only)
- `GET /accounts/{id}/balance` → `available_balance`.
- `POST /holds` `{ account_id, amount, currency, reason, reference_id }` → reserve funds for the
  order before submitting to the broker.
- `POST /holds/{id}/capture` on confirmed fill; `POST /holds/{id}/release` on reject/cancel.
- Account id is configured via `EXEC_CASH_ACCOUNT_ID`.

### 4.3 Broker — Alpaca
`AlpacaBroker` client wraps:
- `submit_order(symbol, qty, side, type, time_in_force, client_order_id)` → `{ id, status, ... }`.
- `get_order(broker_order_id)` → `{ status, filled_qty, filled_avg_price, ... }` for reconciliation.
- `client_order_id` is set to the trade `idempotency_key` so broker-side dedup aligns with DB dedup.
- Config: `EXEC_ALPACA_BASE_URL`, `EXEC_ALPACA_API_KEY`, `EXEC_ALPACA_API_SECRET`. Missing
  credentials must fail readiness for `live`, but not block `paper`.

---

## 5. Core Behavior

### 5.1 Consume loop (mode-driven, identical code)
```
mode  = args.mode                              # paper | live, from --mode CLI arg
topic = paper_topic if mode == paper else live_topic
group = f"{group_prefix}.{mode}"               # separate consumer groups per mode
for msg in consumer(topic, group):
    tick = parse_tick(msg)                      # symbol, price, ts
    for entry in watchlist.match(tick):         # zero or more armed entries
        execution_service.execute(tick, entry, provenance=msg_meta)
    consumer.commit()                           # at-least-once; idempotency guards dupes
```

The paper streamingchart emits OHLC bars, so the tick parser accepts that shape via aliases
(`ticker`→symbol, `close`→price, `bar_time`→ts) and ignores the remaining bar fields
(`schema_version`, `sequence`, `open`/`high`/`low`, `volume`, `emitted_at`, `is_first`/`is_last`).
The bar **close** is used as the current price. The legacy `{symbol, price, ts}` shape is still
accepted.

### 5.2 Match rule (`domain/matching.py`)
- **LONG** entry arms a **BUY** when `tick.price <= buy_price` (price reached/entry crossed down).
- **SHORT** entry arms a **SELL** when `tick.price >= buy_price`.
- Optional tolerance band `EXEC_PRICE_MATCH_TOLERANCE` (absolute or bps) — default exact threshold.
- Matching is O(1) per tick via a `dict[symbol -> list[entry]]` snapshot.

### 5.3 Order sizing
Config-driven, one rule for both modes:
- `EXEC_ORDER_NOTIONAL_USD` (preferred) → `quantity = notional / trigger_price`, or
- `EXEC_ORDER_QUANTITY` (fixed share count).
Exactly one must be set; validated at startup.

### 5.4 Idempotency (no duplicate orders)
`idempotency_key = f"{execution_mode}:{symbol}:{trigger_reason}:{signal_date}"`
(`signal_date` = UTC date). Enforced by a **unique constraint** on `trades.idempotency_key` and a
pre-insert lookup. The same key is passed to Alpaca as `client_order_id`. Re-processing the same
signal (redelivered Kafka message, refreshed watchlist) is a safe no-op.

### 5.5 Execution algorithm (`ExecutionService.execute`)
```
key = build_idempotency_key(entry, tick)
if trades_repo.exists(key): return            # dedup

trade = Trade(status=NEW, is_paper=(mode==paper), execution_mode=mode, ...)

if mode == live:
    bal = cash_client.get_available_balance(account_id)
    if bal < notional:
        trade.status = INSUFFICIENT_FUNDS; persist(trade); return
    hold = cash_client.place_hold(account_id, notional, reason=key, reference_id=key)
    trade.cash_hold_id = hold.id; trade.status = CASH_HELD

order = alpaca.submit_order(..., client_order_id=key)
trade.broker_order_id = order.id; trade.status = SUBMITTED; trade.submitted_at = now()

if mode == paper:
    # assume executed
    trade.status = FILLED
    trade.filled_quantity = trade.quantity
    trade.filled_avg_price = trade.trigger_price
    trade.filled_at = now()

persist(trade)     # single DB transaction per trade
```
- **live** fills are **not** assumed. A **reconciliation loop** (§5.6) advances live trades.
- All broker/cash calls are bounded with timeout + limited retries; failures set `FAILED` and
  release any hold.

### 5.6 Live reconciliation
A background loop (part of `executor_live`) polls `get_order(broker_order_id)` for trades in
`SUBMITTED`/`PARTIALLY_FILLED`, updates `status`/`filled_*`, and on terminal state:
- `FILLED` → `cash_client.capture_hold(cash_hold_id)`.
- `REJECTED`/`CANCELED` → `cash_client.release_hold(cash_hold_id)`.
Poll interval and max age are config-driven; the loop is idempotent and bounded. The same loop also
confirms live **exit** orders (`EXIT_SUBMITTED`/`EXIT_PARTIALLY_FILLED` → `CLOSED`; terminal reject
reopens the position for retry).

### 5.7 Position lifecycle: hold, target-price exit, and market-close liquidation
The streaming hot path is **in-memory first**. An in-memory `PositionBook` (per-symbol, lock-guarded)
is the source of truth for open positions and holds order state, type, symbol, buy/sell prices,
executed status, and shares. Every durable change is handed to a **non-blocking `AsyncDbWriter`**
(single FIFO queue drained by one background thread, batched into transactions); a full queue drops
the write and logs rather than stalling the tick loop. The entry row is **updated in place** as the
position closes.

- **Watchlist** entries now carry a required `sell_price`. On each tick the service records the
  latest price, opens positions on buy matches (§5.5), and calls `check_exits(tick)`.
- **Target-price exit**: when a streaming price reaches a held position's `sell_price` (LONG:
  `price >= sell_price`; SHORT: `price <= sell_price`), an exit order is submitted for the position's
  `exit_side` (opposite of entry), monitored, and persisted (`EXIT_SUBMITTED` → `CLOSED`).
- **Market-close liquidation**: `EXEC_MARKET_CLOSE_LEAD_MINUTES_{PAPER,LIVE}` (default 15)
  **before** the configured close time (**separate for paper and live**), a per-mode liquidator
  submits immediate market exits for **all** open positions once per day. Empty close time
  disables liquidation for that mode.
- **Restart safety**: on startup, open trades (`CASH_HELD`/`SUBMITTED`/`PARTIALLY_FILLED`/`FILLED`/
  `EXIT_SUBMITTED`/`EXIT_PARTIALLY_FILLED`) are rehydrated into the `PositionBook` and their
  idempotency keys seeded so restarts never re-buy.
- Designed for hundreds of symbols and ticks/second: in-memory matching + claim, non-blocking writes.

---

## 6. Alembic & Race-Condition Safety

Per Backend Coding Standards §9 (shared database):
- **`alembic/env.py`** reads `DATABASE_URL`, sets `version_table="alembic_version_quant_execution"`,
  and restricts autogenerate to this service's tables via `include_object`.
- Migrations run **only** from the supervisord **`migrate`** program (`alembic upgrade head`,
  `priority=1`, `autorestart=false`, `exitcodes=0`). Neither executor process runs migrations.
- **No race conditions**:
  - Only the single one-shot `migrate` program runs migrations; `api`/`executor_*` start after it
    (higher `priority` value).
  - To stay safe under **multiple replicas/containers** hitting the shared DB, `env.py` wraps the
    migration in a **Postgres advisory lock** (`SELECT pg_advisory_lock(<service_const>)` /
    `pg_advisory_unlock`). Concurrent migrators serialize; the second sees head and no-ops.
  - The two executors consume **different topics** and use **different consumer groups**, so they
    never contend on the same partitions. Cross-process duplicate orders are additionally prevented
    by the `idempotency_key` unique constraint.

---

## 7. Configuration (Pydantic Settings, `env_prefix="EXEC_"`)

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — (required) | shared Postgres (SQLAlchemy URL) |
| _mode_ | — (required per process) | `paper` \| `live`, passed as the `--mode` **CLI argument**, not an env var |
| `EXEC_KAFKA_BOOTSTRAP_SERVERS` | — (required) | broker list |
| `EXEC_KAFKA_TOPIC_PAPER` | `ticks.paper` | paper tick topic |
| `EXEC_KAFKA_TOPIC_LIVE` | `ticks.live` | live tick topic |
| `EXEC_KAFKA_GROUP_PREFIX` | `quant-execution` | consumer group prefix (mode appended) |
| `EXEC_WATCHLIST_API_URL` | — (required) | `quant_stickynote` base URL |
| `EXEC_WATCHLIST_REFRESH_SECONDS` | `60` | watchlist scheduler clock-check cadence |
| `EXEC_CASH_API_URL` | — (required for live) | `quant_cash` base URL |
| `EXEC_CASH_ACCOUNT_ID` | — (required for live) | account to check/hold |
| `EXEC_ALPACA_BASE_URL` | Alpaca paper URL | broker endpoint |
| `EXEC_ALPACA_API_KEY` | — | broker key (required for live readiness) |
| `EXEC_ALPACA_API_SECRET` | — | broker secret (required for live readiness) |
| `EXEC_ORDER_NOTIONAL_USD` | — | per-order notional (mutually exclusive with qty) |
| `EXEC_ORDER_QUANTITY` | — | fixed per-order share count |
| `EXEC_PRICE_MATCH_TOLERANCE` | `0` | match band (abs or bps) |
| `EXEC_ALPACA_POLL_SECONDS` | `5` | live reconciliation poll interval |
| `EXEC_MARKET_TIMEZONE` | `America/New_York` | tz for market-open/close evaluation |
| `EXEC_MARKET_OPEN_PAPER` | — | paper open time `HH:MM`; watchlist reloads `LEAD_MINUTES` before (empty disables scheduled reload) |
| `EXEC_MARKET_OPEN_LIVE` | — | live open time `HH:MM`; watchlist reloads `LEAD_MINUTES` before (empty disables scheduled reload) |
| `EXEC_MARKET_OPEN_LEAD_MINUTES_PAPER` | `15` | minutes before paper open to reload the watchlist |
| `EXEC_MARKET_OPEN_LEAD_MINUTES_LIVE` | `15` | minutes before live open to reload the watchlist |
| `EXEC_MARKET_CLOSE_PAPER` | — (required) | paper close time `HH:MM`; positions liquidate `LEAD_MINUTES` before (empty disables liquidation) |
| `EXEC_MARKET_CLOSE_LIVE` | — (required) | live close time `HH:MM`; positions liquidate `LEAD_MINUTES` before (empty disables liquidation) |
| `EXEC_MARKET_CLOSE_LEAD_MINUTES_PAPER` | `15` | minutes before paper close to liquidate open positions |
| `EXEC_MARKET_CLOSE_LEAD_MINUTES_LIVE` | `15` | minutes before live close to liquidate open positions |
| `EXEC_MARKET_CLOSE_CHECK_SECONDS` | `30` | liquidator poll interval |
| `EXEC_DB_WRITER_BATCH_SIZE` | `200` | max writes flushed per transaction |
| `EXEC_DB_WRITER_QUEUE_SIZE` | `100000` | async writer queue capacity (full → drop + log) |
| `EXEC_API_LISTEN_ADDRESS` | `0.0.0.0` | health API bind |
| `EXEC_API_PORT` | `8000` | health API port |
| `EXEC_LOG_LEVEL` | `INFO` | logging |

Per standards §"Additional considerations", all env vars live in `docker-compose.yml` (no `.env`).

---

## 8. API (health only)
Per standards §6, the service exposes:
- `GET /health` → `{ "status": "ok", "service": "quant-execution" }` (process alive).
- `GET /ready` → 200 when DB reachable **and** required downstreams for the running mode are
  configured (watchlist URL always; for live: cash URL/account + Alpaca creds). 503 with the
  standard error envelope otherwise. Never leak secrets in readiness detail.

Error envelope (standards §7): `{"status":"error","code":"...","error":"...","detail":"..."}`.

---

## 9. Progressive Development Slices

Each slice is independently reviewable, passes `ruff`/`mypy`/`bandit`/`pytest`, and keeps
`alembic upgrade head` green.

### SLICE 0 — Scaffolding & standards baseline
**Objective**: Bootable skeleton conforming to Backend Coding Standards.
**Tasks**: repo layout (§2); `pyproject.toml`; `config.py` (Pydantic Settings); `db.py`
(`get_engine`, `session_scope`, `check_database`); `logging.py`; `api/app.py` + `routes/health.py`
with `/health` + `/ready`; `__main__.py` (waitress); `alembic.ini` + `env.py` (service version
table + advisory lock, empty `versions/`); `supervisord.conf` (`migrate`, `api`, `executor_paper`,
`executor_live`); `Dockerfile`; `docker-compose.yml` (image ref, all env inline, no healthcheck);
`.dockerignore`; `.gitignore`; `ci.yml`; `docker-publish.yml`; `README.md`.
**Acceptance**: `docker compose up` starts; `/health` returns ok; `/ready` reflects DB;
`alembic upgrade head` runs from `migrate`; lint/type/security/tests pass.
**Dependencies**: none.

### SLICE 1 — `trades` schema + repository
**Objective**: Create and persist the trades table.
**Tasks**: `repository/models.py` `Trade` (§3.2); first Alembic migration creating `trades` +
indexes + unique idempotency constraint; `repository/trades_repo.py` (`exists_by_idempotency_key`,
`insert`, `get`, `update_status`, `list` — parameterized SQL only); `domain/enums.py`;
`domain/schemas.py` (`TradeResponse`); migration smoke test.
**Acceptance**: migration up/down clean on Postgres; repo CRUD tested; unique constraint blocks
duplicate `idempotency_key`.
**Dependencies**: SLICE 0.

### SLICE 2 — Watchlist store + matching engine
**Objective**: In-memory `(ticker → buy prices)` with match rules.
**Tasks**: `clients/watchlist_client.py` (`GET /sticky-notes` paging); `domain/matching.py`
(`WatchlistStore` snapshot + atomic refresh; LONG `<=` / SHORT `>=` match with tolerance);
refresh loop scaffold; unit tests (match/no-match, multi-entry symbols, tolerance, refresh swap).
**Acceptance**: watchlist loads and refreshes; matching is correct and O(1) per tick; tests cover
edge cases; no network calls in unit tests (client mocked).
**Dependencies**: SLICE 0.

### SLICE 3 — Kafka consumer & executor worker
**Objective**: Mode-driven consume loop wired end-to-end (no ordering yet).
**Tasks**: `kafka/consumer.py` (consumer factory, topic/group by mode, graceful shutdown, at-least-
once commit); `domain/schemas.py` `Tick` parser/validation; `workers/executor.py` `main()` parsing
the `--mode` CLI argument and selecting topic/group from it, driving `WatchlistStore.match`, logging
intents; supervisord `executor_paper`/`executor_live` verified. Poison/malformed messages are logged
and skipped.
**Acceptance**: both executors start under supervisord; consume from correct topic per mode; matched
ticks produce logged `OrderIntent`s; clean shutdown on SIGTERM.
**Dependencies**: SLICE 2.

### SLICE 4 — Order execution core + Alpaca broker (paper path)
**Objective**: Turn matches into persisted trades; paper fills assumed.
**Tasks**: `clients/alpaca_client.py` (`submit_order`, `get_order`, `client_order_id=idempotency_key`,
timeouts/retries); `domain/services.py` `ExecutionService.execute` (§5.5) with sizing (§5.3) and
idempotency (§5.4); paper path records `SUBMITTED → FILLED` in one transaction; wire executor to call
the service; unit tests (paper fill, dedup no-op, sizing by notional and by quantity, broker error →
`FAILED`).
**Acceptance**: paper ticks that match create `trades` rows with `is_paper=true`, `status=FILLED`,
correct sizing; duplicate signals do not create second rows; Alpaca client mocked in tests.
**Dependencies**: SLICE 1, SLICE 3.

### SLICE 5 — Live path: cash checks + fill reconciliation
**Objective**: Complete the live differences (cash hold + real fill confirmation).
**Tasks**: `clients/cash_client.py` (`get_available_balance`, `place_hold`, `capture_hold`,
`release_hold`); live branch in `ExecutionService` (balance check → `INSUFFICIENT_FUNDS`; hold →
`CASH_HELD`; submit → `SUBMITTED`); reconciliation loop in `executor_live` polling Alpaca and
advancing `FILLED`/`PARTIALLY_FILLED`/`REJECTED` with hold capture/release; `/ready` enforces live
downstream config; tests (sufficient/insufficient funds, hold capture on fill, hold release on
reject, partial fills).
**Acceptance**: live path checks cash, places/captures/releases holds correctly, and never assumes
fills; paper path unchanged; both modes share one code path with only the four documented differences.
**Dependencies**: SLICE 4.

### SLICE 6 — Observability, hardening, docs, DoD
**Objective**: Production-readiness.
**Tasks**: structured logs (symbol, mode, idempotency_key, broker_order_id, status, duration_ms);
enrich `/ready`; README runbook (install, migrate, run, test, compose); coverage gate ≥80%;
`test_migration_smoke`, `test_health`, `test_ready`, matching + service tests finalized; CI runs
`ruff`/`mypy`/`bandit`/`alembic upgrade head`/`pytest`.
**Acceptance**: Definition of Done (standards §21) met; CI green; `docker compose up` healthy.
**Dependencies**: SLICE 1–5.

---

## 10. Testing (per standards §16)
- `test_health.py`, `test_ready.py` (happy + DB-down / missing-live-config paths).
- `test_matching.py` — LONG/SHORT thresholds, tolerance, multi-entry, atomic refresh.
- `test_execution_service.py` — paper assume-fill, live cash hold/capture/release, dedup,
  sizing, broker/cash error handling (Alpaca + cash clients mocked).
- `test_migration_smoke.py` — `alembic upgrade head` against Postgres.
- Coverage gate ≥ 80%. Commands: `pytest -q`, `ruff check .`, `mypy src`, `bandit -r src`.

## 11. Definition of Done (per standards §21)
1. `ruff`, `mypy`, `bandit`, `pytest` pass.
2. `alembic upgrade head` succeeds on target Postgres (service version table only).
3. Docker image builds without cloning source; runs as non-root.
4. `docker compose up` starts supervisord with `migrate` + `api` + `executor_paper` +
   `executor_live`; `/health` returns ok.
5. Logs and readiness output are actionable; no secrets leaked.

---

## 12. Open Questions / Assumptions
1. **Tick schema** on Kafka (fields, encoding — JSON assumed: `{symbol, price, ts}`). Confirm.
2. **Order sizing** default (notional vs. fixed quantity) and default value.
3. **Order type/TIF** for Alpaca (assumed market / day). Confirm limit vs. market.
4. **Match semantics** — assumed LONG triggers at `price <= buy_price`. Confirm tolerance policy.
5. **Currency** assumed `USD`.
6. **Sell/exit logic** — this spec covers entry orders armed by the watchlist. Exit/position
   management is assumed out of scope unless the watchlist carries SHORT entries (handled) or a
   separate exit signal is later defined.
```
