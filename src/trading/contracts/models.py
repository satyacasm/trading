from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
from pydantic import BaseModel, ConfigDict

from trading.contracts.enums import DataSource, OptionType


def _format_strike(strike: Decimal) -> str:
    """Render a strike canonically: no trailing zeros, no exponent.

    Decimal('24500.00') and Decimal('24500') must produce one key.
    """
    normalized = strike.normalize()
    sign, digits, exponent = normalized.as_tuple()
    if isinstance(exponent, int) and exponent > 0:  # 2.45E+4 -> 24500
        normalized = normalized.quantize(Decimal(1))
    return f"{normalized:f}"


class InstrumentRef(BaseModel):
    """A natural key for one tradable thing, before it has a database id."""

    model_config = ConfigDict(frozen=True)

    exchange: str
    segment: str
    symbol: str
    # Ruling S1 (task-18-brief.md): the CM series (EQ, BE, N2, GB, ...).
    # Distinct series under the same symbol are DIFFERENT securities (an
    # equity and an unrelated NCD can share a ticker) -- see the DHFL case
    # in task-18-brief.md. None for instruments where the source carries no
    # meaningful series (F&O, AMFI, and any pre-existing CM row of that
    # shape), which keeps `canonical_key` byte-identical to its pre-Ruling-S1
    # form whenever series is absent.
    series: str | None = None
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: OptionType | None = None

    @property
    def canonical_key(self) -> str:
        parts = [self.exchange, self.segment, self.symbol]
        if self.series is not None:
            parts.append(self.series)
        if self.expiry is not None:
            parts.append(self.expiry.isoformat())
        if self.strike is not None:
            parts.append(_format_strike(self.strike))
        if self.option_type is not None:
            parts.append(self.option_type.value)
        return ":".join(parts)


class RawPayload(BaseModel):
    """Exactly what a source returned, before anyone interpreted it."""

    model_config = ConfigDict(frozen=True)

    source_key: str
    business_date: date
    content: bytes
    content_hash: str
    fetched_at: datetime
    archive_path: Path
    meta: dict[str, str] = {}


@dataclass(frozen=True)
class NormalizedBatch:
    """Canonical-schema rows for one (source, date), not yet resolved to ids."""

    source: DataSource
    business_date: date
    frame: pl.DataFrame


@dataclass(frozen=True)
class QuarantineRow:
    reason: str
    payload: dict[str, object]


@dataclass(frozen=True)
class ValidationOutcome:
    valid: pl.DataFrame
    quarantined: list[QuarantineRow] = field(default_factory=list)


@dataclass(frozen=True)
class LoadResult:
    rows_written: int
    instruments_created: int
