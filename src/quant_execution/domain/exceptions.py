"""Domain exception hierarchy."""

from __future__ import annotations


class DomainError(Exception):
    """Base class for all domain-level errors raised by this service."""


class ConfigurationError(DomainError):
    """Invalid or missing configuration detected at startup."""


class WatchlistError(DomainError):
    """The watchlist source could not be loaded or parsed."""


class BrokerError(DomainError):
    """The broker rejected a request or returned an unexpected response."""


class CashServiceError(DomainError):
    """The cash service could not be reached or returned an error."""
