from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

import polars as pl
from psycopg import Connection

from trading.contracts.models import (
    InstrumentRef,
    LoadResult,
    NormalizedBatch,
    RawPayload,
    ValidationOutcome,
)


@runtime_checkable
class Source(Protocol):
    source_key: str

    def fetch(self, business_date: date) -> RawPayload | None:
        """Return the payload, or None when there is legitimately no data."""
        ...


@runtime_checkable
class Parser(Protocol):
    def can_parse(self, payload: RawPayload) -> bool:
        """True only for payloads this parser owns. Must be mutually exclusive."""
        ...

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        """Source-shaped frame. Raise ParseError on malformed input."""
        ...


@runtime_checkable
class Normalizer(Protocol):
    def normalize(self, frame: pl.DataFrame, payload: RawPayload) -> NormalizedBatch:
        """Emit a frame matching CANONICAL_BAR_SCHEMA exactly."""
        ...


@runtime_checkable
class InstrumentResolver(Protocol):
    def resolve(
        self, refs: set[InstrumentRef], conn: Connection, *, bootstrap: bool = False
    ) -> dict[InstrumentRef, int]:
        """Map natural keys to instrument_ids, creating any that are new."""
        ...


@runtime_checkable
class Validator(Protocol):
    def validate(self, batch: NormalizedBatch) -> ValidationOutcome:
        """Split rows into loadable and quarantined. Never raises for bad rows."""
        ...


@runtime_checkable
class Loader(Protocol):
    def load(self, outcome: ValidationOutcome, conn: Connection) -> LoadResult:
        """Upsert valid rows. Must be idempotent."""
        ...
