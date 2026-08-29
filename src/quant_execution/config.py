"""Service configuration (Pydantic Settings).

All settings are read from environment variables. Service-specific variables use the
``EXEC_`` prefix; shared platform variables (``DATABASE_URL``, ``API_*``) use explicit aliases.

The execution mode (``paper``/``live``) is intentionally NOT here: it is a per-process
command-line argument (``--mode``), because both processes run concurrently in one container.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EXEC_", extra="ignore")

    # Shared platform variables (no EXEC_ prefix).
    database_url: str = Field(..., validation_alias="DATABASE_URL")
    # Containerized service binds all interfaces by design.
    api_listen_address: str = Field(
        "0.0.0.0",  # nosec B104
        validation_alias="API_LISTEN_ADDRESS",
    )
    api_port: int = Field(8000, validation_alias="API_PORT")

    # Database pool.
    db_pool_size: int = 5
    db_max_overflow: int = 10
    # Dedicated Postgres schema for this service in the shared database.
    db_schema: str = "execution"

    # Logging.
    log_level: str = "INFO"

    # Kafka.
    kafka_bootstrap_servers: str = ""
    kafka_topic_paper: str = "ticks.paper"
    kafka_topic_live: str = "ticks.live"
    kafka_group_prefix: str = "quant-execution"

    # Watchlist (quant_stickynote). The sticky note is reloaded once per session at market open
    # (per mode, see ``market_open_paper``/``market_open_live``) plus once at startup so a
    # restart is never left with an empty watchlist. ``watchlist_refresh_seconds`` is the
    # clock-check cadence of that scheduler, not an unconditional refresh interval.
    watchlist_api_url: str = ""
    watchlist_refresh_seconds: int = 60

    # Cash (quant_cash) — required for live mode only.
    cash_api_url: str = ""
    cash_account_id: str = ""

    # Broker (Alpaca).
    alpaca_base_url: str = "https://paper-api.alpaca.markets"
    alpaca_api_key: str = ""
    alpaca_api_secret: str = ""

    # Order sizing — exactly one is used (validated where sizing is applied).
    order_notional_usd: float | None = 1000.0
    order_quantity: float | None = None

    # Matching.
    price_match_tolerance: float = 0.0

    # Live reconciliation.
    alpaca_poll_seconds: int = 5

    # Market schedule. Open/close times are ``HH:MM`` in ``market_timezone``. The open time (per
    # mode) triggers the once-per-session watchlist reload; an empty open time disables the
    # scheduled reload (startup load only). The close time (per mode) auto-liquidates all open
    # positions; empty disables auto-liquidation for that mode. Both executors run concurrently,
    # so each mode has its own open/close. The lead-minutes fire each action that many minutes
    # before its market time (e.g. reload 15m before open, liquidate 15m before close).
    market_timezone: str = "America/New_York"
    market_open_paper: str = ""
    market_open_live: str = ""
    market_open_lead_minutes_paper: int = 15
    market_open_lead_minutes_live: int = 15
    market_close_paper: str = ""
    market_close_live: str = ""
    market_close_lead_minutes_paper: int = 15
    market_close_lead_minutes_live: int = 15
    market_close_check_seconds: int = 30

    # Non-blocking DB writer (in-memory position book is the hot-path source of truth).
    db_writer_batch_size: int = 200
    db_writer_queue_size: int = 100_000

    def missing_readiness_config(self) -> list[str]:
        """Return required downstream settings that are unset (SPEC.md §8).

        The watchlist URL is required for both modes; cash and Alpaca credentials are required
        because the live executor always runs alongside the paper one.
        """
        required = {
            "EXEC_WATCHLIST_API_URL": self.watchlist_api_url,
            "EXEC_ALPACA_API_KEY": self.alpaca_api_key,
            "EXEC_ALPACA_API_SECRET": self.alpaca_api_secret,
            "EXEC_CASH_API_URL": self.cash_api_url,
            "EXEC_CASH_ACCOUNT_ID": self.cash_account_id,
            "EXEC_MARKET_CLOSE_PAPER": self.market_close_paper,
            "EXEC_MARKET_CLOSE_LIVE": self.market_close_live,
        }
        return [name for name, value in required.items() if not value]



settings = Settings()

