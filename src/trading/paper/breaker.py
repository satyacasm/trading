"""Circuit breaker: auto-pause a portfolio that breaches its declared
`max_daily_loss` or `max_drawdown_pct`.

Today this is a behavioural safeguard against a paper strategy that keeps
digging a hole; once strategies can submit orders unattended it becomes a
runaway-protection mechanism, so its correctness under restart and under
concurrent engine state matters more than its plumbing suggests.

**Equity valuation never invents a price.** `compute_equity` raises
`MissingMark` for any held (non-zero-quantity) position whose instrument
has no entry in `marks`, rather than valuing it at zero (which would
understate equity and could trip the breaker on a position that's fine)
or at cost (which would hide a real loss). This is the project's
no-silent-fallbacks rule applied to the single place a wrong number here
does the most damage: a false "you're fine" or a false "you're paused."

**`peak_equity` survives a restart by construction, not by an explicit
seeding step.** `record_snapshot` never trusts an in-memory peak -- every
call re-reads the most recent `portfolio_equity_snapshots` row for this
`portfolio_id` (`load_peak_equity`) and carries that forward. Because the
peak is always derived from the database, not from process memory, a
restart is transparent to it: the very next evaluation reads the same row
a pre-restart evaluation would have, and `evaluate_breach` (which takes
`peak_equity` as a plain argument) is handed whatever `record_snapshot`
just returned. Nothing in the engine needs to special-case startup. The
alternative -- caching the peak in the engine process and re-seeding it
"specially" at startup -- would still reset to today's equity on every
crash that isn't a clean restart, silently re-arming a drawdown breach;
deriving it fresh every time removes that failure mode entirely rather
than patching around it.

**`day_open_equity` is likewise re-derived every call, never cached.**
`load_day_open_equity` reads the last equity snapshot strictly before the
start of `now`'s Asia/Kolkata calendar day and uses its `equity` as the
day's opening mark; with no such snapshot (a portfolio's first day), it
falls back to `initial_capital` -- the state a freshly funded, untraded
portfolio is in, matching `tests/paper/helpers.py`'s `make_portfolio`.
Nothing about the schema offers a `day_open_equity` column to persist
directly (`portfolio_equity_snapshots` carries `equity`/`peak_equity`/
`drawdown_pct` only), and re-deriving it from history sidesteps the same
restart hazard `peak_equity` would have if it were cached in memory.

**`record_snapshot` runs on every evaluation, not only on breach.** Call
it before `evaluate_breach`, unconditionally. The equity series it writes
is what Phase 3's metrics suite (drawdown depth and duration, rolling
Sharpe, the equity curve itself) reads later -- if it were only written
when a breach happens, that history would be useless for anything else.

**`trip` cancels every non-terminal order, not only OPEN and PENDING.**
The brief's own prose names "OPEN/PENDING", but `PARTIALLY_FILLED` is
exactly as resting and exactly as dangerous -- it can still consume the
rest of its quantity on a future tick. Leaving it alive on a paused
portfolio would defeat the whole point of tripping. Cancelling
OPEN/PENDING/PARTIALLY_FILLED mirrors `trading.paper.api.cancel_order`'s
own non-terminal check (`_TERMINAL_ORDER_STATUSES`), so a portfolio's
orders are held to one consistent notion of "still alive" everywhere in
this codebase, not two different ones depending on who cancels them.

**Cross-boundary requirement: `trip` only writes the database.** It
cannot reach into the engine's in-memory `OpenOrderBook` (this module has
no dependency on `engine.py`, and must not gain one). `trading.paper.
engine.evaluate_breaker_for_portfolio` is what closes that gap: it calls
`trip` and then, on a breach, scans `book` for every order belonging to
this `portfolio_id` and drops them directly -- the same fix already
applied twice elsewhere in this plan for exactly this class of bug (a
DB-only cancel that the in-memory book doesn't hear about until it fills
an order it shouldn't have). `apply_fill`'s `OrderNoLongerFillable`
optimistic-concurrency guard would catch a stale fill at the database
layer as a backstop, but the book is dropped eagerly here rather than
relying on it.

Money/percentage columns (`equity`, `peak_equity`: `NUMERIC(18,4)`;
`drawdown_pct`: `NUMERIC(9,4)`) are quantized in Python before every
write, mirroring `trading.paper.ledger`'s discipline, rather than trusting
Postgres's own storage rounding to agree with whatever a future
pure-Python reader (e.g. a metrics job replaying this table) computes.
`quantize_money`/`quantize_pct` are exported so the *source* value --
`compute_equity`'s raw output, which can carry up to twelve fractional
digits (`positions.quantity` is `NUMERIC(18,8)`, `bars_intraday.close` is
`NUMERIC(18,4)`) -- is quantized exactly once, in `trading.paper.engine.
evaluate_breaker_for_portfolio`, before it is threaded through
`record_snapshot`, `evaluate_breach`, and `trip`. `record_snapshot` also
quantizes defensively on its own inputs (any direct caller, not only the
engine, must get a correctly-scaled row), but the single upstream
quantization is what guarantees `trip`'s `circuit_breaker_events.equity`
and `record_snapshot`'s `portfolio_equity_snapshots.equity` for the same
evaluation are the same number, not two independently-rounded ones.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

from psycopg import Connection

from trading.paper.alerts import enqueue_alert
from trading.paper.enums import OrderStatus
from trading.paper.models import Position

_MONEY_DP = Decimal("0.0001")
_PCT_DP = Decimal("0.0001")

_IST = ZoneInfo("Asia/Kolkata")

_STATUS_ACTIVE = "ACTIVE"
_STATUS_PAUSED = "PAUSED"

REASON_MAX_DAILY_LOSS = "max_daily_loss"
REASON_MAX_DRAWDOWN = "max_drawdown_pct"

# Every order status that still represents live, fillable exposure --
# see the module docstring's "trip cancels every non-terminal order" note.
_CANCELLABLE_ORDER_STATUSES = (
    OrderStatus.OPEN.value,
    OrderStatus.PENDING.value,
    OrderStatus.PARTIALLY_FILLED.value,
)


def quantize_money(value: Decimal) -> Decimal:
    """To 4dp (`NUMERIC(18,4)`'s scale), `ROUND_HALF_UP`. Exported (not
    just used internally by `record_snapshot`) so `trading.paper.engine`
    can quantize `compute_equity`'s raw output once, at the source,
    before threading it through `record_snapshot`, `evaluate_breach`, and
    `trip` -- see the module docstring's quantization paragraph. Fix
    round 1: previously only `record_snapshot` quantized, so `trip` could
    persist a `circuit_breaker_events.equity` with more than 4 fractional
    digits (a position's `quantity` is `NUMERIC(18,8)`, so
    `quantity * mark` can carry up to 12), silently rounded by Postgres's
    own storage rounding on insert instead of by this function -- exactly
    the class of divergence this module documents guarding against.
    """
    return value.quantize(_MONEY_DP, rounding=ROUND_HALF_UP)


def quantize_pct(value: Decimal) -> Decimal:
    """To 4dp (`NUMERIC(9,4)`'s scale), `ROUND_HALF_UP`. Exported for the
    same reason as `quantize_money` -- a `max_drawdown_pct` threshold
    handed to `trip` needs the percentage scale, not the money one."""
    return value.quantize(_PCT_DP, rounding=ROUND_HALF_UP)


class MissingMark(Exception):
    """`compute_equity` was asked to value a non-zero position whose
    instrument has no entry in `marks`. See the module docstring's
    "Equity valuation never invents a price" section -- raised rather
    than defaulting to zero or to cost, both of which would silently
    misrepresent equity in whichever direction is most dangerous."""


def compute_equity(
    cash: Decimal, positions: Sequence[Position], marks: Mapping[int, Decimal]
) -> Decimal:
    """Cash plus every held position, marked to `marks`. Pure.

    A position with `quantity == 0` (fully closed but still present as a
    `positions` row) contributes nothing and needs no mark -- only a held
    position can misprice equity, so only a held position requires one.
    """
    equity = cash
    for position in positions:
        if position.quantity == 0:
            continue
        mark = marks.get(position.instrument_id)
        if mark is None:
            raise MissingMark(
                f"no mark available for instrument_id={position.instrument_id} "
                f"(portfolio_id={position.portfolio_id}, quantity={position.quantity}); "
                "refusing to value a held position at zero (understates equity, "
                "spuriously trips the breaker) or at cost (hides a real loss) -- "
                "failing loudly instead"
            )
        equity += position.quantity * mark
    return equity


def evaluate_breach(
    equity: Decimal,
    day_open_equity: Decimal,
    peak_equity: Decimal,
    max_daily_loss: Decimal | None,
    max_drawdown_pct: Decimal | None,
) -> str | None:
    """The reason this portfolio should be paused, or `None`. Pure.

    Checks daily loss before drawdown; each check is entirely skipped
    when its limit is `None` -- an undeclared limit must never breach.
    The returned string is prefixed with `REASON_MAX_DAILY_LOSS` or
    `REASON_MAX_DRAWDOWN` so a caller (`trading.paper.engine`) can decide
    which of `max_daily_loss`/`max_drawdown_pct` is the `threshold` to
    hand `trip` without re-deriving the check itself.
    """
    if max_daily_loss is not None:
        loss = day_open_equity - equity
        if loss > max_daily_loss:
            return (
                f"{REASON_MAX_DAILY_LOSS}: loss of {loss} from day-open equity "
                f"{day_open_equity} exceeds the limit of {max_daily_loss}"
            )
    if max_drawdown_pct is not None and peak_equity > 0:
        drawdown_pct = (peak_equity - equity) / peak_equity * Decimal(100)
        if drawdown_pct > max_drawdown_pct:
            return (
                f"{REASON_MAX_DRAWDOWN}: drawdown of {drawdown_pct}% from peak equity "
                f"{peak_equity} exceeds the limit of {max_drawdown_pct}%"
            )
    return None


def load_peak_equity(conn: Connection, portfolio_id: int) -> Decimal | None:
    """The most recently recorded `peak_equity` for `portfolio_id`, or
    `None` if it has no snapshot history yet. See the module docstring's
    "peak_equity survives a restart" section -- this is the read that
    makes that true, since it is always re-derived from the database."""
    row = conn.execute(
        "SELECT peak_equity FROM portfolio_equity_snapshots"
        " WHERE portfolio_id = %s ORDER BY ts DESC LIMIT 1",
        (portfolio_id,),
    ).fetchone()
    return Decimal(row[0]) if row is not None else None


def load_day_open_equity(conn: Connection, portfolio_id: int, now: datetime) -> Decimal:
    """The equity daily-loss is measured against for `now`'s Asia/Kolkata
    calendar day. See the module docstring's "day_open_equity is likewise
    re-derived" section for why this is a query, not cached state."""
    day_start = datetime.combine(now.astimezone(_IST).date(), time.min, tzinfo=_IST).astimezone(UTC)
    row = conn.execute(
        "SELECT equity FROM portfolio_equity_snapshots"
        " WHERE portfolio_id = %s AND ts < %s ORDER BY ts DESC LIMIT 1",
        (portfolio_id, day_start),
    ).fetchone()
    if row is not None:
        return Decimal(row[0])
    cap_row = conn.execute(
        "SELECT initial_capital FROM portfolios WHERE portfolio_id = %s", (portfolio_id,)
    ).fetchone()
    assert cap_row is not None, f"portfolio_id={portfolio_id} does not exist"
    return Decimal(cap_row[0])


def record_snapshot(conn: Connection, portfolio_id: int, ts: datetime, equity: Decimal) -> Decimal:
    """Write one `portfolio_equity_snapshots` row for `portfolio_id` at
    `ts`, carrying `peak_equity` forward from the previous snapshot (or
    seeding it from `equity` itself, on the first snapshot ever) and
    deriving `drawdown_pct` from it. Returns the new peak.

    Deliberately does not commit -- the caller owns the transaction
    boundary, matching `trading.paper.ledger.apply_fill`'s convention, so
    a single evaluation can commit `record_snapshot` and a subsequent
    `trip` as one unit.
    """
    equity = quantize_money(equity)
    prior_peak = load_peak_equity(conn, portfolio_id)
    new_peak = quantize_money(equity if prior_peak is None else max(prior_peak, equity))
    drawdown_pct = (
        quantize_pct((new_peak - equity) / new_peak * Decimal(100))
        if new_peak > 0
        else Decimal("0.0000")
    )
    conn.execute(
        "INSERT INTO portfolio_equity_snapshots"
        " (portfolio_id, ts, equity, peak_equity, drawdown_pct)"
        " VALUES (%s, %s, %s, %s, %s)",
        (portfolio_id, ts, equity, new_peak, drawdown_pct),
    )
    return new_peak


def trip(
    conn: Connection, portfolio_id: int, reason: str, equity: Decimal, threshold: Decimal
) -> None:
    """Pause `portfolio_id`, cancel its resting orders, and record why.

    Does not commit (same convention as `record_snapshot`) and does not
    touch the engine's in-memory `OpenOrderBook` -- see the module
    docstring's "Cross-boundary requirement" section for who does.

    Unlike `record_snapshot`, this does *not* quantize `equity`/
    `threshold` itself -- the caller (`trading.paper.engine.
    evaluate_breaker_for_portfolio`) is expected to have already
    quantized `equity` once at the source (`quantize_money`) and
    `threshold` with whichever of `quantize_money`/`quantize_pct`
    matches the breached limit's column scale, precisely so this
    function's `circuit_breaker_events` row and `record_snapshot`'s
    `portfolio_equity_snapshots` row -- both written from the same
    evaluation -- persist the same `equity` value rather than two
    independently-rounded ones.
    """
    conn.execute(
        "UPDATE portfolios SET status = %s WHERE portfolio_id = %s",
        (_STATUS_PAUSED, portfolio_id),
    )
    conn.execute(
        "UPDATE orders SET status = %s, updated_at = now()"
        " WHERE portfolio_id = %s AND status IN (%s, %s, %s)",
        (
            OrderStatus.CANCELLED.value,
            portfolio_id,
            *_CANCELLABLE_ORDER_STATUSES,
        ),
    )
    conn.execute(
        "INSERT INTO circuit_breaker_events (portfolio_id, ts, reason, equity, threshold)"
        " VALUES (%s, now(), %s, %s, %s)",
        (portfolio_id, reason, equity, threshold),
    )
    # Task 10 deliberately left this call out -- Task 11 owns wiring
    # alerting in. enqueue_alert only ever INSERTs (no commit, no
    # network), so it participates in this same uncommitted transaction
    # exactly like the two writes above; the caller's own commit is what
    # makes the pause, the cancels, the event, and this alert one atomic
    # unit.
    enqueue_alert(
        conn,
        "BREACH",
        {
            "portfolio_id": portfolio_id,
            "reason": reason,
            "equity": equity,
            "threshold": threshold,
        },
    )
