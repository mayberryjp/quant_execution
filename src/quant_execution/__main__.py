"""API entrypoint: serve the Bottle app with Waitress.

Run with ``python -m quant_execution``.
"""

from __future__ import annotations

from waitress import serve

from quant_execution.api.app import create_app
from quant_execution.config import settings
from quant_execution.logging import configure_logging, get_logger


def main() -> None:
    configure_logging(settings.log_level)
    log = get_logger("api")
    log.info("starting api on %s:%s", settings.api_listen_address, settings.api_port)
    serve(create_app(), host=settings.api_listen_address, port=settings.api_port)


if __name__ == "__main__":
    main()
