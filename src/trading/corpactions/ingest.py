"""Corporate-actions ingestion.

Two responsibilities live here:

- `ingest_corporate_actions(conn, rows)` -- the fully in-scope interface
  (Ruling A4, task-16 addendum): upsert a batch of already-parsed
  `CorporateActionRow`s into `corporate_actions`, matching `uq_corp_action`'s
  expression index exactly --
  `(instrument_id, action_type, ex_date, (COALESCE(ratio_to, amount, 0)))`
  -- so a repeated ingest of the same action updates in place instead of
  duplicating, while a changed `ratio_to`/`amount` creates a distinct row
  because it participates in that expression.

- `parse_nse_corporate_actions(...)` -- turns a live sample of NSE's
  corporate-actions feed (documented in
  docs/data-formats/eod-source-formats.md §5, fetched live 2026-08-20) into
  `CorporateActionRow`s. NSE does not expose the split/bonus ratio or
  dividend amount as a structured field -- only a free-text `subject`
  string ("Bonus 1:2", "Face Value Split (Sub-Division) - From Rs 10/- Per
  Share To Rs 2/- Per Share", "Dividend - Rs 2 Per Share"). Only the
  patterns actually observed in that live sample are recognised; anything
  else (rights issues, mergers, demergers, symbol changes, "Scheme Of
  Arrangement" variants, ...) is skipped and counted in
  `ParseResult.skipped` rather than guessed -- a guessed ratio would fail
  silently against the real feed, which is worse than not parsing it at
  all.

`announced_at`: NSE's feed carries a `caBroadcastDate` field, used when
present and parseable. Every record in the verified live sample had it
`null`, so in practice every action ingested through this parser is
"always known" under the `announced_at IS NULL` convention documented in
`adjust.py`.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction

import structlog
from psycopg import Connection
from psycopg.types.json import Jsonb

from trading.contracts import InstrumentRef
from trading.resolver.instruments import DbInstrumentResolver

log = structlog.get_logger(__name__)

NSE_CORPORATE_ACTIONS_URL = (
    "https://www.nseindia.com/api/corporates-corporateActions?index=equities"
)
_SOURCE = "nse_corporate_actions"

_MONTH_NUM = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

# Widened against the whole 2016-2026 archive (23,782 real subjects), not
# guessed: every alternative below is a spelling NSE actually uses. See
# tests/corpactions/test_classify.py, where each case is copied verbatim.
_NUM = r"(\d+(?:\.\d+)?)"
# NSE writes an amount as "Rs 2", "Re 1/-", "Rs 2.55" -- the "/-" is
# decoration, not part of the number.
_RUPEES = rf"R[se]\.?\s*{_NUM}(?:/-)?"
# "(Purpose Revised)", "(Revised)" and similar administrative suffixes change
# nothing about the action itself.
_SUFFIX = r"(?:\s*\([^)]*\))?"

# "Bonus 1:2" -> 1 new share for every 2 held. Anchored to its own segment so
# "Scheme Of Arrangement - Bonus Ncrps 4:1" can never match: an NCRPS bonus
# issues preference shares, leaving the equity share count unchanged, so
# applying it would corrupt every price before that date.
_BONUS_RE = re.compile(rf"^Bonus {_NUM}:{_NUM}{_SUFFIX}$")

# "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share"
# "Face Value Split From Rs 10 To Rs 2" / "Fv Splt Frm Rs 10 To Re 1"
_SPLIT_RE = re.compile(
    rf"^(?:Face Value Split|Fv Splt)\s*(?:\(Sub-Division\))?\s*-?\s*(?:From|Frm)\s+"
    rf"{_RUPEES}(?:\s+Per\s+Share)?\s+To\s+{_RUPEES}(?:\s+Per\s+Share)?{_SUFFIX}$"
)

# "Dividend - Rs 2 Per Share", "Interim Dividend Rs 5/- Per Share",
# "Interim Dividend - Rs 2/- Per Share (Purpose Revised)", "Interim Div - Rs 6/-".
_DIVIDEND_RE = re.compile(
    rf"^(?:Interim|Int|Final|Special)?\s*Div(?:idend)?\s*-?\s*{_RUPEES}"
    rf"\s+Per\s+Sh(?:are)?{_SUFFIX}$"
)

# "Rights 2:5 @ Premium Rs 1.17/-" -- 2 new shares offered for every 5 held.
# Dilutive, so the price genuinely moves on the ex-date. Recorded rather than
# dropped; `adjust.py` applies only SPLIT and BONUS, so this informs the
# continuity check without repricing anything.
_RIGHTS_RE = re.compile(rf"^Rights {_NUM}:{_NUM}\b.*$")

# "Scheme Of Arrangement", "Scheme Of Amalgamation", "Demerger". These carve
# real value out of the share price, and this feed carries no ratio for them
# at all -- only the date. The event is recorded so the move is explained;
# inventing a factor would silently reprice a decade of history.
_DEMERGER_RE = re.compile(
    r"(?i)^(?:Composite\s+)?(?:Scheme Of (?:Arrangement|Amalgamation|Demerger)"
    r"|Demerger|Spin.?Off)\b.*$"
)

# "Capital Reduction" -- shares cancelled against accumulated losses, so the
# share count changes and the price moves. Like a demerger, this feed gives
# no ratio, so the event is recorded without one.
_CAPITAL_REDUCTION_RE = re.compile(r"(?i)^(?:Reduction Of Capital|Capital Reduction)\b.*$")


@dataclass(frozen=True)
class CorporateActionRow:
    """One row ready for `ingest_corporate_actions`."""

    instrument_id: int
    action_type: str
    ex_date: date
    record_date: date | None
    ratio_from: Decimal | None
    ratio_to: Decimal | None
    amount: Decimal | None
    new_symbol: str | None
    announced_at: datetime | None
    source: str
    raw: dict[str, object]


@dataclass(frozen=True)
class ParseResult:
    """Output of `parse_nse_corporate_actions`."""

    rows: list[CorporateActionRow]
    skipped: int


_UPSERT = """
    INSERT INTO corporate_actions
        (instrument_id, action_type, ex_date, record_date, ratio_from,
         ratio_to, amount, new_symbol, announced_at, source, raw)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (instrument_id, action_type, ex_date, (COALESCE(ratio_to, amount, 0)))
    DO UPDATE SET
        record_date = EXCLUDED.record_date,
        ratio_from = EXCLUDED.ratio_from,
        ratio_to = EXCLUDED.ratio_to,
        amount = EXCLUDED.amount,
        new_symbol = EXCLUDED.new_symbol,
        announced_at = EXCLUDED.announced_at,
        source = EXCLUDED.source,
        raw = EXCLUDED.raw,
        ingested_at = now()
"""


def _conflict_key(row: CorporateActionRow) -> tuple[int, str, date, Decimal]:
    key_value = row.ratio_to if row.ratio_to is not None else (row.amount or Decimal(0))
    return (row.instrument_id, row.action_type, row.ex_date, key_value)


def ingest_corporate_actions(conn: Connection, rows: Sequence[CorporateActionRow]) -> int:
    """Upsert `rows` into `corporate_actions`. Returns the count written.

    A batch containing two rows with the same `uq_corp_action` key (possible
    if a feed is parsed twice into one call) is deduped before the upsert,
    keeping the last occurrence: Postgres rejects an `ON CONFLICT DO UPDATE`
    that would touch the same arbiter row twice in one command.
    """
    if not rows:
        return 0

    deduped: dict[tuple[int, str, date, Decimal], CorporateActionRow] = {}
    for row in rows:
        deduped[_conflict_key(row)] = row

    payload = [
        (
            r.instrument_id,
            r.action_type,
            r.ex_date,
            r.record_date,
            r.ratio_from,
            r.ratio_to,
            r.amount,
            r.new_symbol,
            r.announced_at,
            r.source,
            Jsonb(r.raw),
        )
        for r in deduped.values()
    ]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT, payload)
    return len(payload)


def _parse_nse_date(value: str | None) -> date | None:
    if value is None or value == "-":
        return None
    day_str, mon_str, year_str = value.split("-")
    return date(int(year_str), _MONTH_NUM[mon_str], int(day_str))


def _parse_broadcast_date(value: object) -> datetime | None:
    if not value or value == "-":
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        log.warning("corpactions.unparseable_broadcast_date", value=value)
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _classify(subject: str) -> list[tuple[str, Decimal | None, Decimal | None, Decimal | None]]:
    """Recognise `subject` as zero or more SPLIT/BONUS/DIVIDEND actions.

    Returns a LIST because NSE packs two genuine corporate actions into one
    subject with a slash -- "Bonus 1:1/Face Value Split (Sub-Division) - From
    Rs 10/- Per Share To Rs 5/- Per Share" is a bonus AND a sub-division, and
    that combination is the largest price move in the whole feed. Recognising
    only the first would leave the other silently unapplied.

    Anything not matching a spelling actually observed in the archive returns
    an empty list rather than a guess: a wrong ratio is applied to every price
    before that date and fails silently, which is worse than not parsing it.
    """
    actions: list[tuple[str, Decimal | None, Decimal | None, Decimal | None]] = []
    # Split on "/", "+" or "&" where they separate two actions, never inside
    # the "/-" that decorates a rupee amount.
    for part in re.split(r"/(?!-)|\+|&", subject):
        segment = part.strip()
        if m := _BONUS_RE.match(segment):
            new, held = Decimal(m.group(1)), Decimal(m.group(2))
            actions.append(("BONUS", held, held + new, None))
        elif m := _SPLIT_RE.match(segment):
            old_fv, new_fv = Decimal(m.group(1)), Decimal(m.group(2))
            # Ruling A7 (task-16 fix round 1): store the canonical share-count
            # ratio, reduced to lowest terms -- not the raw face values. "From
            # Rs 10 To Rs 2" is a 1:5 split, exactly like the migration's own
            # comment (`-- SPLIT 1:5 => from=1, to=5`) and the brief's fixtures
            # say, not (2, 10). `ratio_to` participates in `uq_corp_action`'s
            # uniqueness expression: the unreduced form would let the same
            # real-world split entered once here and once canonically by
            # another source hold two rows and be applied twice. `Fraction`
            # reduces exactly (works for non-integer face values too).
            ratio = Fraction(new_fv) / Fraction(old_fv)
            actions.append(("SPLIT", Decimal(ratio.numerator), Decimal(ratio.denominator), None))
        elif m := _DIVIDEND_RE.match(segment):
            actions.append(("DIVIDEND", None, None, Decimal(m.group(1))))
        elif m := _RIGHTS_RE.match(segment):
            offered, held = Decimal(m.group(1)), Decimal(m.group(2))
            actions.append(("RIGHTS", held, held + offered, None))
        elif _DEMERGER_RE.match(segment):
            actions.append(("DEMERGER", None, None, None))
        elif _CAPITAL_REDUCTION_RE.match(segment):
            actions.append(("CAPITAL_REDUCTION", None, None, None))
    return actions


def parse_nse_corporate_actions(
    payload: bytes, resolver: DbInstrumentResolver, conn: Connection
) -> ParseResult:
    """Parse a raw response body from `NSE_CORPORATE_ACTIONS_URL` into rows.

    Only SPLIT/BONUS/DIVIDEND subjects matching a pattern actually observed
    in a live sample (docs/data-formats/eod-source-formats.md §5) are
    recognised. Everything else is skipped rather than guessed and counted
    in `ParseResult.skipped`. Symbols are resolved to `instrument_id` via the
    same `DbInstrumentResolver` production ingestion uses, as `NSE`/`CM`
    equities (this endpoint's `series` is always `EQ` in the verified
    sample).

    Ruling S1 (task-18-brief.md): `InstrumentRef` now carries `series`, and
    the real `NSE`/`CM` equity instrument this endpoint's rows must land on
    is created with `series="EQ"` (Ruling S1's normalizer requirement). An
    `InstrumentRef` built here without `series="EQ"` would carry a different
    `canonical_key` than that real instrument and resolve to a phantom
    duplicate instead of colliding onto it -- so `series="EQ"` is pinned
    here to match, not left to default to `None`.
    """
    records: list[dict[str, object]] = json.loads(payload)

    classified: list[tuple[dict[str, object], str, Decimal | None, Decimal | None, Decimal | None]]
    classified = []
    skipped = 0
    refs: set[InstrumentRef] = set()

    for record in records:
        subject = str(record.get("subject", ""))
        # One subject can carry two real actions ("Bonus 1:1/Face Value
        # Split ..."), so this fans out rather than taking the first.
        actions = _classify(subject)
        if not actions:
            skipped += 1
            log.debug("corpactions.skipped_subject", subject=subject, symbol=record.get("symbol"))
            continue
        for action_type, ratio_from, ratio_to, amount in actions:
            classified.append((record, action_type, ratio_from, ratio_to, amount))
        refs.add(
            InstrumentRef(exchange="NSE", segment="CM", symbol=str(record["symbol"]), series="EQ")
        )

    if not classified:
        return ParseResult(rows=[], skipped=skipped)

    mapping = resolver.resolve(refs, conn)

    rows: list[CorporateActionRow] = []
    for record, action_type, ratio_from, ratio_to, amount in classified:
        ref = InstrumentRef(exchange="NSE", segment="CM", symbol=str(record["symbol"]), series="EQ")
        ex_date = _parse_nse_date(str(record.get("exDate")) if record.get("exDate") else None)
        if ex_date is None:
            skipped += 1
            log.warning("corpactions.missing_ex_date", symbol=record.get("symbol"))
            continue
        record_date = _parse_nse_date(str(record.get("recDate")) if record.get("recDate") else None)
        rows.append(
            CorporateActionRow(
                instrument_id=mapping[ref],
                action_type=action_type,
                ex_date=ex_date,
                record_date=record_date,
                ratio_from=ratio_from,
                ratio_to=ratio_to,
                amount=amount,
                new_symbol=None,
                announced_at=_parse_broadcast_date(record.get("caBroadcastDate")),
                source=_SOURCE,
                raw=record,
            )
        )
    return ParseResult(rows=rows, skipped=skipped)
