"""BSE corporate actions.

The continuity classifier put ~878 of the 1,971 unexplained price steps on
BSE instruments, and none of them could ever be explained: only NSE's feed
had been ingested. BSE publishes the same events in a different shape --
`Purpose` instead of `subject`, "02 Jan 2020" instead of "02-Jan-2020",
"Stock  Split From Rs.10/- to Rs.5/-" instead of "Face Value Split
(Sub-Division) - From Rs 10/- Per Share To Rs 5/- Per Share" -- so it gets
its own patterns rather than a widened NSE set that would fit neither well.

Rows land on `CorporateActionRow` and go through the same
`ingest_corporate_actions` upsert, so both exchanges share one table and one
uniqueness rule.
"""

from __future__ import annotations

import json
import re
from datetime import date
from decimal import Decimal
from fractions import Fraction

import structlog
from psycopg import Connection

from trading.corpactions.ingest import CorporateActionRow, ParseResult

log = structlog.get_logger(__name__)

BSE_CORPORATE_ACTIONS_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"
    "?ddlcategorys=E&ddlindustrys=&scripcode=&segment=0&strType=C"
)
# BSE's API returns 403 to an unadorned client; it wants a browser's headers.
BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
    "Accept": "application/json, text/plain, */*",
}
_SOURCE = "bse_corporate_actions"

_NUM = r"(\d+(?:\.\d+)?)"
# "Bonus issue 1:1" -- 1 new share for every 1 held.
_BONUS_RE = re.compile(rf"(?i)^Bonus\s+issue\s+{_NUM}:{_NUM}$")
# "Stock  Split From Rs.10/- to Rs.5/-" (BSE doubles the space in most rows).
_SPLIT_RE = re.compile(
    rf"(?i)^Stock\s+Split\s+From\s+Rs\.?\s*{_NUM}(?:/-)?\s+to\s+Rs\.?\s*{_NUM}(?:/-)?$"
)
# "Interim Dividend - Rs. - 1.0000"
_DIVIDEND_RE = re.compile(rf"(?i)^(?:Interim|Final|Special)?\s*Dividend\s*-\s*Rs\.?\s*-?\s*{_NUM}$")
_RIGHTS_RE = re.compile(rf"(?i)^Rights\s+issue\s+{_NUM}:{_NUM}\b.*$")
# BSE spells a rights issue without any ratio far more often than with one --
# "Right Issue of Equity Shares", 553 times across the decade. Dilutive either
# way, so the event is recorded and the ratio simply left unknown.
_RIGHTS_NO_RATIO_RE = re.compile(r"(?i)^Right\s+Issue\s+of\s+Equity\s+Shares\b.*$")
_CAPITAL_REDUCTION_RE = re.compile(r"(?i)^(?:Reduction\s+of\s+Capital|Capital\s+Reduction)\b.*$")
# A consolidation is a reverse split: fewer shares, proportionally higher
# price. BSE gives no ratio for it, so it is recorded without one.
_CONSOLIDATION_RE = re.compile(r"(?i)^Consolidation\s+of\s+Shares\b.*$")
_DEMERGER_RE = re.compile(r"(?i)^(?:Scheme Of|Demerger|Amalgamation|Spin.?Off)\b.*$")

_MONTHS = {
    m: i
    for i, m in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        start=1,
    )
}


def _parse_bse_date(value: str | None) -> date | None:
    """BSE spells a date "02 Jan 2020" -- space separated, unlike NSE's dashes."""
    if not value:
        return None
    parts = value.strip().split()
    if len(parts) != 3:
        return None
    day, month, year = parts
    if month[:3].title() not in _MONTHS:
        return None
    try:
        return date(int(year), _MONTHS[month[:3].title()], int(day))
    except ValueError:
        return None


def _classify(purpose: str) -> list[tuple[str, Decimal | None, Decimal | None, Decimal | None]]:
    """Recognise a BSE `Purpose` as zero or more actions.

    Same discipline as the NSE parser: a spelling that has not been observed
    is left unparsed rather than guessed, because a wrong ratio is applied to
    every price before that date and fails silently.
    """
    actions: list[tuple[str, Decimal | None, Decimal | None, Decimal | None]] = []
    for part in re.split(r"/(?!-)|\+|&", purpose):
        segment = " ".join(part.split())  # collapse BSE's doubled spaces
        if m := _BONUS_RE.match(segment):
            new, held = Decimal(m.group(1)), Decimal(m.group(2))
            actions.append(("BONUS", held, held + new, None))
        elif m := _SPLIT_RE.match(segment):
            old_fv, new_fv = Decimal(m.group(1)), Decimal(m.group(2))
            # Canonical share-count ratio reduced to lowest terms (Ruling A7),
            # matching the NSE parser so the same real split from either
            # exchange collides on uq_corp_action instead of being applied twice.
            ratio = Fraction(new_fv) / Fraction(old_fv)
            actions.append(("SPLIT", Decimal(ratio.numerator), Decimal(ratio.denominator), None))
        elif m := _DIVIDEND_RE.match(segment):
            actions.append(("DIVIDEND", None, None, Decimal(m.group(1))))
        elif m := _RIGHTS_RE.match(segment):
            offered, held = Decimal(m.group(1)), Decimal(m.group(2))
            actions.append(("RIGHTS", held, held + offered, None))
        elif _RIGHTS_NO_RATIO_RE.match(segment):
            actions.append(("RIGHTS", None, None, None))
        elif _CAPITAL_REDUCTION_RE.match(segment):
            actions.append(("CAPITAL_REDUCTION", None, None, None))
        elif _CONSOLIDATION_RE.match(segment):
            actions.append(("CONSOLIDATION", None, None, None))
        elif _DEMERGER_RE.match(segment):
            actions.append(("DEMERGER", None, None, None))
    return actions


def _instrument_ids_by_symbol(conn: Connection, symbols: set[str]) -> dict[str, list[int]]:
    """Every existing BSE/CM instrument for each symbol, across series.

    Lookup only -- never creation. A BSE company moves between series (A, B,
    T, XT) over a decade and the action belongs to the company, so it attaches
    to each of its rows; and minting an instrument from a corporate-action
    feed would create a phantom with a guessed series that no price row ever
    lands on.
    """
    if not symbols:
        return {}
    rows = conn.execute(
        "SELECT symbol, instrument_id FROM instruments "
        "WHERE exchange = 'BSE' AND segment = 'CM' AND symbol = ANY(%s)",
        (list(symbols),),
    ).fetchall()
    by_symbol: dict[str, list[int]] = {}
    for symbol, instrument_id in rows:
        by_symbol.setdefault(symbol, []).append(instrument_id)
    return by_symbol


def parse_bse_corporate_actions(payload: bytes, conn: Connection) -> ParseResult:
    """Parse a raw response from `BSE_CORPORATE_ACTIONS_URL` into rows."""
    records: list[dict[str, object]] = json.loads(payload)

    Action = tuple[str, Decimal | None, Decimal | None, Decimal | None]
    classified: list[tuple[dict[str, object], list[Action]]] = []
    skipped = 0
    symbols: set[str] = set()

    for record in records:
        purpose = str(record.get("Purpose", "")).strip()
        actions = _classify(purpose)
        if not actions:
            skipped += 1
            log.debug("bse_corpactions.skipped", purpose=purpose, symbol=record.get("short_name"))
            continue
        symbol = str(record.get("short_name", "")).strip()
        if not symbol:
            skipped += 1
            continue
        symbols.add(symbol)
        classified.append((record, actions))

    by_symbol = _instrument_ids_by_symbol(conn, symbols)

    rows: list[CorporateActionRow] = []
    for record, actions in classified:
        symbol = str(record.get("short_name", "")).strip()
        instrument_ids = by_symbol.get(symbol)
        if not instrument_ids:
            skipped += 1
            log.debug("bse_corpactions.unknown_symbol", symbol=symbol)
            continue
        ex_date = _parse_bse_date(str(record.get("Ex_date")) if record.get("Ex_date") else None)
        if ex_date is None:
            skipped += 1
            log.warning("bse_corpactions.missing_ex_date", symbol=symbol)
            continue
        record_date = _parse_bse_date(str(record.get("RD_Date")) if record.get("RD_Date") else None)
        for action_type, ratio_from, ratio_to, amount in actions:
            for instrument_id in instrument_ids:
                rows.append(
                    CorporateActionRow(
                        instrument_id=instrument_id,
                        action_type=action_type,
                        ex_date=ex_date,
                        record_date=record_date,
                        ratio_from=ratio_from,
                        ratio_to=ratio_to,
                        amount=amount,
                        new_symbol=None,
                        # BSE's feed carries no announcement timestamp, so
                        # these are "always known" under adjust.py's
                        # announced_at IS NULL convention, exactly like NSE's.
                        announced_at=None,
                        source=_SOURCE,
                        raw=record,
                    )
                )

    return ParseResult(rows=rows, skipped=skipped)
