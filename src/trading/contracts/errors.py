from __future__ import annotations


class TradingError(Exception):
    """Base for every error this package raises."""


class FetchError(TradingError):
    """A source could not retrieve data for a date it should have had."""


class ParseError(TradingError):
    """A payload could not be parsed. Never raised for a merely empty result."""


class ValidationAbort(TradingError):
    """A batch is so wrong that loading any of it would be unsafe."""
