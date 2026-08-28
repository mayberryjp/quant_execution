"""Logging helpers. One format for the whole service, logs to stdout."""

from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def _format_value(value: object) -> str:
    text = str(value)
    if any(ch.isspace() for ch in text) or "=" in text:
        return '"' + text.replace('"', '\\"') + '"'
    return text


def format_event(event: str, /, **fields: object) -> str:
    """Render an event and its fields as a logfmt-style line (``event k=v k=v``).

    ``None`` fields are omitted so log lines only carry populated context.
    """
    parts = [event]
    parts.extend(
        f"{key}={_format_value(value)}" for key, value in fields.items() if value is not None
    )
    return " ".join(parts)

