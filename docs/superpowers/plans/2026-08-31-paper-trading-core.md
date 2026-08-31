# Paper Trading Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a paper-trading subsystem where an order submitted against a live NSE equity or crypto instrument fills realistically, applies the Indian cost model at contract-note fidelity, and updates a portfolio ledger — plus the §8 behavioural layer (mandatory rationale, circuit breaker, Telegram alerts).

**Architecture:** A standalone `paper_engine` process consumes the existing Redis `ticks:*` fan-out and holds open orders in memory; a `paper_api` router on the existing gateway writes orders and announces them on a control channel. Fills commit atomically (fill + ledger + position + cash + order status in one transaction), then publish. Charge rates live in a dated `charge_schedules` table, never as constants. Telegram goes through a transactional outbox so a third-party outage can never touch the fill path.

**Tech Stack:** Python 3.12 (`uv`) · FastAPI · psycopg (sync `Connection`) · TimescaleDB/Postgres · Redis (`redis.asyncio`) · pydantic v2 · pytest · hypothesis (new dependency, for property tests)

**Spec:** [`docs/superpowers/specs/2026-08-31-paper-trading-core-design.md`](../specs/2026-08-31-paper-trading-core-design.md)

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.12 exactly, managed by `uv`.** Every command is `uv run ...`.
- **Lint/type gate every task:** `uv run ruff check . && uv run ruff format --check . && uv run mypy src` must pass before any commit.
- **Every task ends with a passing `uv run pytest` (default invocation) and a commit.**
- **Money is `Decimal` end to end. Never `float`, anywhere** — not in models, not in intermediate arithmetic, not in test fixtures. Postgres columns are `numeric(18,4)`.
- **Money serialises as JSON numbers, not strings.** Pydantic v2 renders `Decimal` as a string by default; every API model that carries money needs an explicit serializer. This is the Task 7b lesson from the charts/watchlist sub-project — see `src/trading/streaming/market_data_api.py` for the established pattern and copy it.
- **Every FastAPI route is a plain `def`, never `async def`.** psycopg is synchronous; `async def` runs it on the event loop. Commit `5d03a2e` documents the permanent gateway deadlock this caused. `market_data_api.py` is the reference.
- **No silent fallbacks in the charge or fill path.** If a charge cannot be computed correctly the order is rejected; if a tick is malformed it is logged and dropped. Never approximate, never default to zero.
- **Long-only, cash-settled, no leverage or shorting** in this sub-project.
- **A portfolio is single-currency** — INR or USDT, never mixed. No FX conversion anywhere.
- **Tests use the dedicated Redis on port 6380** and the existing guard fixture in `tests/streaming/conftest.py`. No test may touch the live stack.
- **Broker profile is Upstox** — brokerage ₹20/order delivery, ₹20-or-0.1%-whichever-lower intraday, DP ₹20/scrip/day on delivery sells, GST base = brokerage + transaction + demat + IPFT.
- **Slippage is fixed basis points only in this sub-project.** The spec's volume-participation model is deferred, and since partial fills arise *only* from participation, every fill here is a full fill. `orders.filled_quantity` and the `PARTIALLY_FILLED` status are therefore written but never exercised — they exist so participation can be added later without a migration. Do not delete them as dead code, and do not invent a partial-fill path to justify them.

## File Structure

```
src/trading/paper/
  __init__.py
  enums.py        -- Side, OrderType, OrderStatus, Product, TimeInForce,
                     ChargeType, ChargeBasis, Rounding, EntryType
  models.py       -- Portfolio, Order, FillDecision, ChargeBreakdown,
                     Position, ChargeSchedule  (pydantic, Decimal money)
  charges.py      -- load_schedules() + compute_charges()  [PURE calculator]
  fills.py        -- decide_fill()  [PURE fill rules]
  ledger.py       -- apply_fill()  [the one atomic transaction]
  api.py          -- FastAPI router: portfolios, orders, positions
  engine.py       -- paper_engine process: tick loop, control channel, expiry
  breaker.py      -- evaluate_breach()  [PURE] + the 5s driver
  alerts.py       -- enqueue_alert() + alert_worker
migrations/versions/0007_paper_trading_core.py
tests/paper/
  conftest.py, test_charges.py, test_charges_golden.py, test_fills.py,
  test_ledger.py, test_api.py, test_engine.py, test_breaker.py,
  test_alerts.py, test_clock_parity.py, test_migration.py
```

The split is by responsibility, not layer. The three pure modules — `charges.py`, `fills.py`, `breaker.py` — hold every rule that must be provably correct and touch no I/O, so they are exhaustively testable and the Phase 3 backtest engine can call them a million times without Postgres.

---

## Task 1: Migration `0007` — schema, seed user, seed charge schedules

**Files:**
- Create: `migrations/versions/0007_paper_trading_core.py`
- Create: `tests/paper/__init__.py`, `tests/paper/test_migration.py`

**Interfaces:**
- Produces: tables `portfolios`, `orders`, `fills`, `ledger_entries`, `positions`, `portfolio_equity_snapshots`, `circuit_breaker_events`, `alert_deliveries`, `charge_schedules`; one seeded row in `users`; seeded Upstox `charge_schedules` rows.

Read `migrations/versions/0005_watchlist.py` and `tests/streaming/test_watchlist_migration.py` first — follow their structure exactly.

- [ ] **Step 1: Write the failing migration test**

Create `tests/paper/test_migration.py`:

```python
"""Migration 0007 creates the paper-trading schema and seeds charge rates."""

from decimal import Decimal

import pytest
from psycopg import Connection

EXPECTED_TABLES = [
    "portfolios",
    "orders",
    "fills",
    "ledger_entries",
    "positions",
    "portfolio_equity_snapshots",
    "circuit_breaker_events",
    "alert_deliveries",
    "charge_schedules",
]


@pytest.mark.parametrize("table", EXPECTED_TABLES)
def test_migration_creates_table(db_conn: Connection, table: str) -> None:
    row = db_conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s",
        (table,),
    ).fetchone()
    assert row is not None, f"{table} was not created"


def test_seeds_a_single_local_user(db_conn: Connection) -> None:
    row = db_conn.execute("SELECT count(*) FROM users").fetchone()
    assert row is not None
    assert row[0] >= 1


def test_nse_transaction_charge_has_two_dated_regimes(db_conn: Connection) -> None:
    """The 2026-03-01 revision must be seeded as two rows, not one.

    NSE cash transaction charges moved 0.00297% -> 0.00307% effective
    2026-03-01. The backfill spans 2022-2026 and crosses that boundary,
    so a single row would misprice most of the historical period.
    """
    # Filter by product: both DELIVERY and INTRADAY carry both date
    # regimes, so an unfiltered query returns four rows, not two.
    rows = db_conn.execute(
        "SELECT rate, effective_from, effective_to FROM charge_schedules "
        "WHERE exchange='NSE' AND charge_type='EXCHANGE_TXN' "
        "AND product='DELIVERY' "
        "ORDER BY effective_from"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == Decimal("0.0000297")
    assert rows[0][2] is not None, "the older regime must be closed off"
    assert rows[1][0] == Decimal("0.0000307")
    assert rows[1][2] is None, "the current regime must be open-ended"


def test_money_columns_are_numeric_not_float(db_conn: Connection) -> None:
    rows = db_conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name='fills' AND column_name IN "
        "('price','brokerage','stt','exchange_txn','sebi_fee',"
        "'stamp_duty','ipft','gst','dp_charges')"
    ).fetchall()
    assert len(rows) == 9
    for name, dtype in rows:
        assert dtype == "numeric", f"{name} is {dtype}, must be numeric"
```

Add `tests/paper/__init__.py` (empty) and `tests/paper/conftest.py` re-exporting the existing `db_conn` fixture:

```python
"""Paper-trading test fixtures.

`db_conn` comes from the project-level conftest -- the same rolled-back
transaction fixture every other suite uses. Re-exported here so tests in
this package can request it by name.
"""

from tests.conftest import db_conn  # noqa: F401
```

If the project-level fixture lives elsewhere, find it with `grep -rn "def db_conn" tests/` and import from the real location.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/paper/test_migration.py -v`
Expected: FAIL — tables do not exist.

- [ ] **Step 3: Write the migration**

Create `migrations/versions/0007_paper_trading_core.py`. Set `down_revision` to the current head (find it with `uv run alembic heads`).

```python
"""paper trading core

Revision ID: 0007
Revises: 0006
"""

from decimal import Decimal

import sqlalchemy as sa
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "portfolios",
        sa.Column("portfolio_id", sa.BigInteger, primary_key=True),
        sa.Column("user_id", sa.BigInteger, sa.ForeignKey("users.user_id"), nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("base_currency", sa.Text, nullable=False, server_default="INR"),
        sa.Column("initial_capital", sa.Numeric(18, 4), nullable=False),
        sa.Column("cash_balance", sa.Numeric(18, 4), nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="ACTIVE"),
        sa.Column("max_daily_loss", sa.Numeric(18, 4), nullable=True),
        sa.Column("max_drawdown_pct", sa.Numeric(9, 4), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("user_id", "name", name="uq_portfolio_name"),
        sa.CheckConstraint("cash_balance >= 0", name="ck_no_negative_cash"),
    )

    op.create_table(
        "orders",
        sa.Column("order_id", sa.BigInteger, primary_key=True),
        sa.Column("portfolio_id", sa.BigInteger,
                  sa.ForeignKey("portfolios.portfolio_id"), nullable=False),
        sa.Column("instrument_id", sa.BigInteger,
                  sa.ForeignKey("instruments.instrument_id"), nullable=False),
        sa.Column("side", sa.Text, nullable=False),
        sa.Column("order_type", sa.Text, nullable=False),
        sa.Column("quantity", sa.Numeric(18, 8), nullable=False),
        sa.Column("filled_quantity", sa.Numeric(18, 8), nullable=False,
                  server_default="0"),
        sa.Column("limit_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("product", sa.Text, nullable=False),
        sa.Column("time_in_force", sa.Text, nullable=False, server_default="DAY"),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("rationale", sa.Text, nullable=False),
        sa.Column("rejection_reason", sa.Text, nullable=True),
        sa.Column("idempotency_key", sa.Text, nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("idempotency_key", name="uq_order_idempotency"),
        sa.CheckConstraint("quantity > 0", name="ck_order_qty_positive"),
        sa.CheckConstraint("length(rationale) > 0", name="ck_rationale_present"),
    )
    op.create_index("ix_orders_open", "orders", ["status", "instrument_id"])

    op.create_table(
        "fills",
        sa.Column("fill_id", sa.BigInteger, primary_key=True),
        sa.Column("order_id", sa.BigInteger, sa.ForeignKey("orders.order_id"),
                  nullable=False),
        sa.Column("quantity", sa.Numeric(18, 8), nullable=False),
        sa.Column("price", sa.Numeric(18, 4), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("tick_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("brokerage", sa.Numeric(18, 4), nullable=False),
        sa.Column("stt", sa.Numeric(18, 4), nullable=False),
        sa.Column("exchange_txn", sa.Numeric(18, 4), nullable=False),
        sa.Column("sebi_fee", sa.Numeric(18, 4), nullable=False),
        sa.Column("stamp_duty", sa.Numeric(18, 4), nullable=False),
        sa.Column("ipft", sa.Numeric(18, 4), nullable=False),
        sa.Column("gst", sa.Numeric(18, 4), nullable=False),
        sa.Column("dp_charges", sa.Numeric(18, 4), nullable=False),
        sa.Column("total_charges", sa.Numeric(18, 4), nullable=False),
        sa.Index("ix_fills_order", "order_id"),
    )

    op.create_table(
        "ledger_entries",
        sa.Column("entry_id", sa.BigInteger, primary_key=True),
        sa.Column("portfolio_id", sa.BigInteger,
                  sa.ForeignKey("portfolios.portfolio_id"), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("entry_type", sa.Text, nullable=False),
        sa.Column("amount", sa.Numeric(18, 4), nullable=False),
        sa.Column("fill_id", sa.BigInteger, sa.ForeignKey("fills.fill_id"),
                  nullable=True),
        sa.Column("balance_after", sa.Numeric(18, 4), nullable=False),
        sa.Index("ix_ledger_portfolio_ts", "portfolio_id", "ts"),
    )

    op.create_table(
        "positions",
        sa.Column("portfolio_id", sa.BigInteger,
                  sa.ForeignKey("portfolios.portfolio_id"), primary_key=True),
        sa.Column("instrument_id", sa.BigInteger,
                  sa.ForeignKey("instruments.instrument_id"), primary_key=True),
        sa.Column("quantity", sa.Numeric(18, 8), nullable=False),
        sa.Column("avg_cost", sa.Numeric(18, 4), nullable=False),
        sa.Column("realised_pnl", sa.Numeric(18, 4), nullable=False,
                  server_default="0"),
    )

    op.create_table(
        "portfolio_equity_snapshots",
        sa.Column("portfolio_id", sa.BigInteger,
                  sa.ForeignKey("portfolios.portfolio_id"), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("peak_equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("drawdown_pct", sa.Numeric(9, 4), nullable=False),
        sa.PrimaryKeyConstraint("portfolio_id", "ts"),
    )

    op.create_table(
        "circuit_breaker_events",
        sa.Column("event_id", sa.BigInteger, primary_key=True),
        sa.Column("portfolio_id", sa.BigInteger,
                  sa.ForeignKey("portfolios.portfolio_id"), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("threshold", sa.Numeric(18, 4), nullable=False),
    )

    op.create_table(
        "alert_deliveries",
        sa.Column("delivery_id", sa.BigInteger, primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("payload", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="PENDING"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text, nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Index("ix_alerts_pending", "status", "created_at"),
    )

    op.create_table(
        "charge_schedules",
        sa.Column("schedule_id", sa.BigInteger, primary_key=True),
        sa.Column("broker", sa.Text, nullable=False),
        sa.Column("exchange", sa.Text, nullable=False),
        sa.Column("asset_class", sa.Text, nullable=False),
        sa.Column("product", sa.Text, nullable=False),
        sa.Column("charge_type", sa.Text, nullable=False),
        sa.Column("basis", sa.Text, nullable=False),
        sa.Column("applies_to_side", sa.Text, nullable=False),
        sa.Column("rate", sa.Numeric(18, 10), nullable=False),
        sa.Column("cap", sa.Numeric(18, 4), nullable=True),
        sa.Column("rounding", sa.Text, nullable=False),
        sa.Column("gst_base_types", sa.Text, nullable=True),
        sa.Column("effective_from", sa.Date, nullable=False),
        sa.Column("effective_to", sa.Date, nullable=True),
        sa.Column("source_note", sa.Text, nullable=False),
        sa.CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_charge_range",
        ),
        sa.Index("ix_charge_lookup", "broker", "exchange", "asset_class",
                 "product", "effective_from"),
    )

    _seed(op.get_bind())


def _seed(conn) -> None:  # noqa: ANN001 - alembic bind
    conn.execute(
        sa.text(
            "INSERT INTO users (email) VALUES (:e) ON CONFLICT (email) DO NOTHING"
        ),
        {"e": "local@paper.trading"},
    )

    # Rates verified 2026-08-31 against upstox.com/brokerage-charges/,
    # zerodha.com/charges/, and the NSE transaction-charge circular.
    # Percentage rates are stored as fractions: 0.1% -> 0.001.
    src_up = "https://upstox.com/brokerage-charges/ (checked 2026-08-31)"
    src_nse = "NSE circular FA64232; revision effective 2026-03-01"

    rows = [
        # (product, charge_type, basis, side, rate, cap, rounding,
        #  gst_base, eff_from, eff_to, source)
        ("DELIVERY", "BROKERAGE", "FLAT_PER_ORDER", "BOTH",
         "20", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("INTRADAY", "BROKERAGE", "PERCENT_OF_TURNOVER", "BOTH",
         "0.001", "20", "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("DELIVERY", "STT", "PERCENT_OF_TURNOVER", "BOTH",
         "0.001", None, "NEAREST_RUPEE", None, "2024-10-01", None, src_up),
        ("INTRADAY", "STT", "PERCENT_OF_TURNOVER", "SELL",
         "0.00025", None, "NEAREST_RUPEE", None, "2024-10-01", None, src_up),
        ("DELIVERY", "EXCHANGE_TXN", "PERCENT_OF_TURNOVER", "BOTH",
         "0.0000297", None, "TWO_DECIMALS", None, "2024-10-01", "2026-03-01", src_nse),
        ("DELIVERY", "EXCHANGE_TXN", "PERCENT_OF_TURNOVER", "BOTH",
         "0.0000307", None, "TWO_DECIMALS", None, "2026-03-01", None, src_nse),
        ("INTRADAY", "EXCHANGE_TXN", "PERCENT_OF_TURNOVER", "BOTH",
         "0.0000297", None, "TWO_DECIMALS", None, "2024-10-01", "2026-03-01", src_nse),
        ("INTRADAY", "EXCHANGE_TXN", "PERCENT_OF_TURNOVER", "BOTH",
         "0.0000307", None, "TWO_DECIMALS", None, "2026-03-01", None, src_nse),
        ("DELIVERY", "SEBI_FEE", "PERCENT_OF_TURNOVER", "BOTH",
         "0.000001", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("INTRADAY", "SEBI_FEE", "PERCENT_OF_TURNOVER", "BOTH",
         "0.000001", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("DELIVERY", "STAMP_DUTY", "PERCENT_OF_TURNOVER", "BUY",
         "0.00015", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("INTRADAY", "STAMP_DUTY", "PERCENT_OF_TURNOVER", "BUY",
         "0.00003", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("DELIVERY", "IPFT", "PERCENT_OF_TURNOVER", "BOTH",
         "0.0000001", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("INTRADAY", "IPFT", "PERCENT_OF_TURNOVER", "BOTH",
         "0.0000001", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("DELIVERY", "DP_CHARGES", "FLAT_PER_SCRIP_PER_DAY", "SELL",
         "20", None, "TWO_DECIMALS", None, "2024-10-01", None, src_up),
        ("DELIVERY", "GST", "PERCENT_OF_CHARGES", "BOTH",
         "0.18", None, "TWO_DECIMALS",
         "BROKERAGE,EXCHANGE_TXN,DP_CHARGES,IPFT", "2024-10-01", None, src_up),
        ("INTRADAY", "GST", "PERCENT_OF_CHARGES", "BOTH",
         "0.18", None, "TWO_DECIMALS",
         "BROKERAGE,EXCHANGE_TXN,IPFT", "2024-10-01", None, src_up),
    ]
    stmt = sa.text(
        "INSERT INTO charge_schedules (broker, exchange, asset_class, product,"
        " charge_type, basis, applies_to_side, rate, cap, rounding,"
        " gst_base_types, effective_from, effective_to, source_note)"
        " VALUES ('UPSTOX','NSE','EQUITY', :p, :ct, :b, :s, :r, :cap, :rnd,"
        " :gst, :ef, :et, :src)"
    )
    for p, ct, b, s, r, cap, rnd, gst, ef, et, src in rows:
        conn.execute(stmt, {
            "p": p, "ct": ct, "b": b, "s": s, "r": Decimal(r),
            "cap": Decimal(cap) if cap else None, "rnd": rnd, "gst": gst,
            "ef": ef, "et": et, "src": src,
        })

    # Crypto: Binance spot taker fee. TDS is a separate, optional charge
    # type so it stays visible rather than baked into the fee.
    src_bin = "https://www.binance.com/en/fee/schedule (checked 2026-08-31)"
    crypto = sa.text(
        "INSERT INTO charge_schedules (broker, exchange, asset_class, product,"
        " charge_type, basis, applies_to_side, rate, cap, rounding,"
        " gst_base_types, effective_from, effective_to, source_note)"
        " VALUES ('BINANCE','BINANCE','CRYPTO','DELIVERY', :ct,"
        " 'PERCENT_OF_TURNOVER', :s, :r, NULL, 'TWO_DECIMALS', NULL,"
        " '2024-01-01', NULL, :src)"
    )
    conn.execute(crypto, {"ct": "BROKERAGE", "s": "BOTH",
                          "r": Decimal("0.001"), "src": src_bin})


def downgrade() -> None:
    for t in ("charge_schedules", "alert_deliveries", "circuit_breaker_events",
              "portfolio_equity_snapshots", "positions", "ledger_entries",
              "fills", "orders", "portfolios"):
        op.drop_table(t)
```

- [ ] **Step 4: Run the migration and the test**

Run: `uv run alembic upgrade head && uv run pytest tests/paper/test_migration.py -v`
Expected: PASS, 12 tests.

- [ ] **Step 5: Verify the full gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add migrations/versions/0007_paper_trading_core.py tests/paper/
git commit -m "feat(paper): schema and dated Upstox charge schedules"
```

---

## Task 2: Enums and domain models

**Files:**
- Create: `src/trading/paper/__init__.py`, `src/trading/paper/enums.py`, `src/trading/paper/models.py`
- Create: `tests/paper/test_models.py`

**Interfaces:**
- Produces: `Side`, `OrderType`, `OrderStatus`, `Product`, `TimeInForce`, `ChargeType`, `ChargeBasis`, `Rounding`, `EntryType` (all `StrEnum`); models `ChargeSchedule`, `Order`, `FillDecision`, `ChargeBreakdown`, `Position`, `Portfolio`.

Follow `src/trading/contracts/enums.py` for enum style and `src/trading/streaming/models.py` for pydantic style.

- [ ] **Step 1: Write the failing test**

Create `tests/paper/test_models.py`:

```python
import json
from decimal import Decimal

from trading.paper.enums import ChargeType, OrderStatus, Product, Side
from trading.paper.models import ChargeBreakdown


def test_charge_breakdown_totals_its_components() -> None:
    b = ChargeBreakdown(
        brokerage=Decimal("20.00"), stt=Decimal("131.00"),
        exchange_txn=Decimal("4.02"), sebi_fee=Decimal("0.13"),
        stamp_duty=Decimal("19.65"), ipft=Decimal("0.01"),
        gst=Decimal("4.35"), dp_charges=Decimal("0.00"),
    )
    assert b.total == Decimal("179.16")


def test_money_serialises_as_json_number_not_string() -> None:
    """Task 7b lesson: pydantic v2 renders Decimal as a string by default,
    which broke the frontend's arithmetic. Money must be a JSON number."""
    b = ChargeBreakdown(
        brokerage=Decimal("20.00"), stt=Decimal("0"), exchange_txn=Decimal("0"),
        sebi_fee=Decimal("0"), stamp_duty=Decimal("0"), ipft=Decimal("0"),
        gst=Decimal("0"), dp_charges=Decimal("0"),
    )
    payload = json.loads(b.model_dump_json())
    assert isinstance(payload["brokerage"], (int, float))
    assert payload["brokerage"] == 20.0


def test_enums_are_string_valued() -> None:
    assert Side.BUY == "BUY"
    assert Product.DELIVERY == "DELIVERY"
    assert OrderStatus.PENDING == "PENDING"
    assert ChargeType.STT == "STT"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/paper/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: trading.paper`.

- [ ] **Step 3: Write the enums**

Create `src/trading/paper/__init__.py` (empty) and `src/trading/paper/enums.py`:

```python
"""Enumerations for the paper-trading subsystem.

StrEnum throughout so values round-trip through Postgres `text` columns
and JSON without conversion, matching `trading.contracts.enums`.
"""

from __future__ import annotations

from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class Product(StrEnum):
    DELIVERY = "DELIVERY"
    INTRADAY = "INTRADAY"


class TimeInForce(StrEnum):
    DAY = "DAY"
    GTC = "GTC"


class ChargeType(StrEnum):
    BROKERAGE = "BROKERAGE"
    STT = "STT"
    EXCHANGE_TXN = "EXCHANGE_TXN"
    SEBI_FEE = "SEBI_FEE"
    STAMP_DUTY = "STAMP_DUTY"
    IPFT = "IPFT"
    GST = "GST"
    DP_CHARGES = "DP_CHARGES"
    TDS = "TDS"


class ChargeBasis(StrEnum):
    PERCENT_OF_TURNOVER = "PERCENT_OF_TURNOVER"
    FLAT_PER_ORDER = "FLAT_PER_ORDER"
    FLAT_PER_SCRIP_PER_DAY = "FLAT_PER_SCRIP_PER_DAY"
    PERCENT_OF_CHARGES = "PERCENT_OF_CHARGES"


class Rounding(StrEnum):
    NEAREST_RUPEE = "NEAREST_RUPEE"
    TWO_DECIMALS = "TWO_DECIMALS"


class EntryType(StrEnum):
    FILL = "FILL"
    CHARGE = "CHARGE"
    DEPOSIT = "DEPOSIT"
```

- [ ] **Step 4: Write the models**

Create `src/trading/paper/models.py`:

```python
"""Pydantic models for the paper-trading subsystem.

Money is Decimal everywhere -- never float. `MoneyModel` pins the JSON
encoding to numbers rather than strings; pydantic v2's default renders
Decimal as a string, which is the defect Task 7b fixed for the market-data
API and which would silently break arithmetic in any consumer.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_serializer

from trading.paper.enums import (
    ChargeBasis,
    ChargeType,
    OrderStatus,
    OrderType,
    Product,
    Rounding,
    Side,
    TimeInForce,
)

_MONEY_FIELDS = (
    "brokerage", "stt", "exchange_txn", "sebi_fee", "stamp_duty",
    "ipft", "gst", "dp_charges",
)


class ChargeBreakdown(BaseModel):
    """Every statutory charge on one fill, itemised.

    Stored per component rather than as a total because §8's cost-drag
    report needs the breakdown and it cannot be reconstructed from a lump
    sum afterwards.
    """

    model_config = ConfigDict(frozen=True)

    brokerage: Decimal
    stt: Decimal
    exchange_txn: Decimal
    sebi_fee: Decimal
    stamp_duty: Decimal
    ipft: Decimal
    gst: Decimal
    dp_charges: Decimal

    @property
    def total(self) -> Decimal:
        return (
            self.brokerage + self.stt + self.exchange_txn + self.sebi_fee
            + self.stamp_duty + self.ipft + self.gst + self.dp_charges
        )

    @field_serializer(*_MONEY_FIELDS)
    def _money_as_number(self, v: Decimal) -> float:
        return float(v)


class ChargeSchedule(BaseModel):
    """One dated charge rule. Rates are data, not constants -- NSE cash
    transaction charges changed on 2026-03-01 and the backfill spans that
    boundary."""

    model_config = ConfigDict(frozen=True)

    broker: str
    exchange: str
    asset_class: str
    product: Product
    charge_type: ChargeType
    basis: ChargeBasis
    applies_to_side: str
    rate: Decimal
    cap: Decimal | None
    rounding: Rounding
    gst_base_types: tuple[ChargeType, ...] = ()
    effective_from: date
    effective_to: date | None
    source_note: str


class Order(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: int
    portfolio_id: int
    instrument_id: int
    side: Side
    order_type: OrderType
    quantity: Decimal
    filled_quantity: Decimal
    limit_price: Decimal | None
    product: Product
    time_in_force: TimeInForce
    status: OrderStatus
    rationale: str
    submitted_at: datetime

    @property
    def remaining(self) -> Decimal:
        return self.quantity - self.filled_quantity


class FillDecision(BaseModel):
    """The pure fill rules' output: fill this much at this price, caused by
    the price event at `tick_ts`."""

    model_config = ConfigDict(frozen=True)

    quantity: Decimal
    price: Decimal
    tick_ts: datetime


class Position(BaseModel):
    model_config = ConfigDict(frozen=True)

    portfolio_id: int
    instrument_id: int
    quantity: Decimal
    avg_cost: Decimal
    realised_pnl: Decimal


class Portfolio(BaseModel):
    model_config = ConfigDict(frozen=True)

    portfolio_id: int
    user_id: int
    name: str
    base_currency: str
    initial_capital: Decimal
    cash_balance: Decimal
    status: str
    max_daily_loss: Decimal | None
    max_drawdown_pct: Decimal | None
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/paper/test_models.py -v`
Expected: PASS, 3 tests.

- [ ] **Step 6: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/ tests/paper/test_models.py
git commit -m "feat(paper): enums and domain models with numeric money encoding"
```

---

## Task 3: The charge calculator

**Files:**
- Create: `src/trading/paper/charges.py`
- Create: `tests/paper/test_charges.py`

**Interfaces:**
- Consumes: `ChargeSchedule`, `ChargeBreakdown`, enums (Task 2).
- Produces:
  - `load_schedules(conn: Connection, broker: str, exchange: str, asset_class: str, product: Product, on: date) -> list[ChargeSchedule]`
  - `compute_charges(schedules: Sequence[ChargeSchedule], side: Side, quantity: Decimal, price: Decimal) -> ChargeBreakdown` — **pure**, no I/O
  - `MissingChargeSchedule(Exception)`

- [ ] **Step 1: Write the failing tests**

Create `tests/paper/test_charges.py`:

```python
from datetime import date
from decimal import Decimal

import pytest

from trading.paper.charges import (
    MissingChargeSchedule,
    compute_charges,
    load_schedules,
)
from trading.paper.enums import (
    ChargeBasis,
    ChargeType,
    Product,
    Rounding,
    Side,
)
from trading.paper.models import ChargeSchedule


def _sched(charge_type, basis, side, rate, *, cap=None,
           rounding=Rounding.TWO_DECIMALS, gst_base=(), product=Product.DELIVERY):
    return ChargeSchedule(
        broker="UPSTOX", exchange="NSE", asset_class="EQUITY", product=product,
        charge_type=charge_type, basis=basis, applies_to_side=side,
        rate=Decimal(rate), cap=Decimal(cap) if cap else None,
        rounding=rounding, gst_base_types=gst_base,
        effective_from=date(2024, 10, 1), effective_to=None, source_note="test",
    )


def test_percent_of_turnover_charge() -> None:
    s = [_sched(ChargeType.STT, ChargeBasis.PERCENT_OF_TURNOVER, "BOTH", "0.001",
                rounding=Rounding.NEAREST_RUPEE)]
    b = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1310.50"))
    # 100 * 1310.50 = 131050 turnover; 0.1% = 131.05 -> nearest rupee = 131
    assert b.stt == Decimal("131")


def test_flat_per_order_charge() -> None:
    s = [_sched(ChargeType.BROKERAGE, ChargeBasis.FLAT_PER_ORDER, "BOTH", "20")]
    b = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1310.50"))
    assert b.brokerage == Decimal("20.00")


def test_percentage_brokerage_is_capped() -> None:
    """Upstox intraday: Rs 20 or 0.1%, whichever is LOWER."""
    s = [_sched(ChargeType.BROKERAGE, ChargeBasis.PERCENT_OF_TURNOVER, "BOTH",
                "0.001", cap="20", product=Product.INTRADAY)]
    big = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1310.50"))
    assert big.brokerage == Decimal("20.00")  # 0.1% = 131.05, capped
    small = compute_charges(s, Side.BUY, Decimal("1"), Decimal("500"))
    assert small.brokerage == Decimal("0.50")  # 0.1% of 500, under the cap


def test_side_specific_charge_skips_wrong_side() -> None:
    """Stamp duty is buy-side only; intraday STT is sell-side only."""
    s = [_sched(ChargeType.STAMP_DUTY, ChargeBasis.PERCENT_OF_TURNOVER, "BUY",
                "0.00015")]
    assert compute_charges(s, Side.BUY, Decimal("10"), Decimal("100")).stamp_duty > 0
    assert compute_charges(s, Side.SELL, Decimal("10"), Decimal("100")).stamp_duty == 0


def test_gst_base_excludes_stt_and_stamp_duty() -> None:
    """The classic silent error: GST is levied on brokerage + transaction +
    demat + IPFT, never on STT or stamp duty."""
    s = [
        _sched(ChargeType.BROKERAGE, ChargeBasis.FLAT_PER_ORDER, "BOTH", "20"),
        _sched(ChargeType.STT, ChargeBasis.PERCENT_OF_TURNOVER, "BOTH", "0.001"),
        _sched(ChargeType.STAMP_DUTY, ChargeBasis.PERCENT_OF_TURNOVER, "BUY",
               "0.00015"),
        _sched(ChargeType.GST, ChargeBasis.PERCENT_OF_CHARGES, "BOTH", "0.18",
               gst_base=(ChargeType.BROKERAGE,)),
    ]
    b = compute_charges(s, Side.BUY, Decimal("100"), Decimal("1000"))
    assert b.gst == Decimal("3.60")  # 18% of brokerage 20 only


def test_dp_charge_is_flat_and_sell_side_only() -> None:
    s = [_sched(ChargeType.DP_CHARGES, ChargeBasis.FLAT_PER_SCRIP_PER_DAY,
                "SELL", "20")]
    assert compute_charges(s, Side.SELL, Decimal("5"), Decimal("100")).dp_charges == Decimal("20.00")
    assert compute_charges(s, Side.BUY, Decimal("5"), Decimal("100")).dp_charges == Decimal("0")


def test_empty_schedule_list_raises_rather_than_returning_zero() -> None:
    """A missing schedule must fail loudly. Treating it as zero yields a
    P&L that looks fine and is systematically optimistic."""
    with pytest.raises(MissingChargeSchedule):
        compute_charges([], Side.BUY, Decimal("10"), Decimal("100"))


def test_load_schedules_picks_the_regime_in_force_on_that_date(db_conn) -> None:
    """NSE transaction charges moved 0.00297% -> 0.00307% on 2026-03-01."""
    before = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY",
                            Product.DELIVERY, date(2026, 2, 28))
    after = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY",
                           Product.DELIVERY, date(2026, 3, 1))
    rate_before = next(s.rate for s in before if s.charge_type == ChargeType.EXCHANGE_TXN)
    rate_after = next(s.rate for s in after if s.charge_type == ChargeType.EXCHANGE_TXN)
    assert rate_before == Decimal("0.0000297")
    assert rate_after == Decimal("0.0000307")


def test_load_schedules_returns_exactly_one_row_per_charge_type(db_conn) -> None:
    """Overlapping date ranges would double-charge; the loader must never
    return two rows of the same charge type for one date."""
    got = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY",
                         Product.DELIVERY, date(2026, 6, 1))
    types = [s.charge_type for s in got]
    assert len(types) == len(set(types)), f"duplicate charge types: {types}"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/paper/test_charges.py -v`
Expected: FAIL — `ModuleNotFoundError: trading.paper.charges`.

- [ ] **Step 3: Implement the calculator**

Create `src/trading/paper/charges.py`:

```python
"""The Indian cost model.

`compute_charges` is pure -- no DB access, no clock -- so it is
exhaustively testable and Phase 3's backtest engine can call it a million
times without touching Postgres. Schedules are loaded separately and
passed in.

Rates are data, not constants: NSE cash transaction charges were revised
0.00297% -> 0.00307% effective 2026-03-01, and the intraday backfill
spans that boundary, so a hardcoded rate would misprice most of the
historical period.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from psycopg import Connection

from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding, Side
from trading.paper.models import ChargeBreakdown, ChargeSchedule

_TWO_DP = Decimal("0.01")
_ONE = Decimal("1")


class MissingChargeSchedule(Exception):
    """No charge schedule covers this instrument, product, and date.

    Raised rather than defaulting to zero: a silently-zero charge produces
    a P&L that looks correct and is systematically optimistic, which is the
    most dangerous failure mode in this subsystem.
    """


def load_schedules(
    conn: Connection,
    broker: str,
    exchange: str,
    asset_class: str,
    product: Product,
    on: date,
) -> list[ChargeSchedule]:
    """Every charge rule in force for this combination on `on`."""
    rows = conn.execute(
        "SELECT broker, exchange, asset_class, product, charge_type, basis,"
        " applies_to_side, rate, cap, rounding, gst_base_types,"
        " effective_from, effective_to, source_note"
        " FROM charge_schedules"
        " WHERE broker=%s AND exchange=%s AND asset_class=%s AND product=%s"
        "   AND effective_from <= %s"
        "   AND (effective_to IS NULL OR effective_to > %s)",
        (broker, exchange, asset_class, product.value, on, on),
    ).fetchall()
    return [
        ChargeSchedule(
            broker=r[0], exchange=r[1], asset_class=r[2], product=Product(r[3]),
            charge_type=ChargeType(r[4]), basis=ChargeBasis(r[5]),
            applies_to_side=r[6], rate=r[7], cap=r[8], rounding=Rounding(r[9]),
            gst_base_types=tuple(
                ChargeType(t) for t in (r[10].split(",") if r[10] else [])
            ),
            effective_from=r[11], effective_to=r[12], source_note=r[13],
        )
        for r in rows
    ]


def _round(value: Decimal, rounding: Rounding) -> Decimal:
    if rounding is Rounding.NEAREST_RUPEE:
        return value.quantize(_ONE, rounding=ROUND_HALF_UP)
    return value.quantize(_TWO_DP, rounding=ROUND_HALF_UP)


def _applies(schedule: ChargeSchedule, side: Side) -> bool:
    return schedule.applies_to_side in ("BOTH", side.value)


def compute_charges(
    schedules: Sequence[ChargeSchedule],
    side: Side,
    quantity: Decimal,
    price: Decimal,
) -> ChargeBreakdown:
    """Itemised charges for one fill. Pure.

    GST is computed last, over the named subset of charge types its own
    schedule row declares -- never as a multiplier on the total, because
    it excludes STT and stamp duty and the included set differs between
    brokers.
    """
    if not schedules:
        raise MissingChargeSchedule(
            "no charge schedule covers this fill; refusing to compute a "
            "silently-zero cost"
        )

    turnover = quantity * price
    amounts: dict[ChargeType, Decimal] = dict.fromkeys(ChargeType, Decimal("0"))
    gst_schedule: ChargeSchedule | None = None

    for s in schedules:
        if s.charge_type is ChargeType.GST:
            gst_schedule = s
            continue
        if not _applies(s, side):
            continue

        if s.basis is ChargeBasis.PERCENT_OF_TURNOVER:
            raw = turnover * s.rate
        elif s.basis in (
            ChargeBasis.FLAT_PER_ORDER,
            ChargeBasis.FLAT_PER_SCRIP_PER_DAY,
        ):
            raw = s.rate
        else:
            continue  # PERCENT_OF_CHARGES only ever applies to GST

        if s.cap is not None:
            raw = min(raw, s.cap)
        amounts[s.charge_type] = _round(raw, s.rounding)

    if gst_schedule is not None and _applies(gst_schedule, side):
        base = sum(
            (amounts[t] for t in gst_schedule.gst_base_types), Decimal("0")
        )
        amounts[ChargeType.GST] = _round(base * gst_schedule.rate,
                                         gst_schedule.rounding)

    return ChargeBreakdown(
        brokerage=amounts[ChargeType.BROKERAGE],
        stt=amounts[ChargeType.STT],
        exchange_txn=amounts[ChargeType.EXCHANGE_TXN],
        sebi_fee=amounts[ChargeType.SEBI_FEE],
        stamp_duty=amounts[ChargeType.STAMP_DUTY],
        ipft=amounts[ChargeType.IPFT],
        gst=amounts[ChargeType.GST],
        dp_charges=amounts[ChargeType.DP_CHARGES],
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/paper/test_charges.py -v`
Expected: PASS, 10 tests.

- [ ] **Step 5: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/charges.py tests/paper/test_charges.py
git commit -m "feat(paper): dated Indian charge calculator"
```

---

## Task 4: Golden tests against a real contract note

**Files:**
- Create: `tests/paper/test_charges_golden.py`
- Create: `tests/paper/fixtures/contract_notes.py`

**Interfaces:**
- Consumes: `compute_charges`, `load_schedules` (Task 3).
- Produces: nothing importable — this task is validation.

**Ground truth.** Prefer a real Upstox contract note for an executed trade. If none is available, use Upstox's published brokerage calculator at <https://upstox.com/brokerage-calculator/> as the oracle: enter the same quantity, price, and product, and record its itemised output. A calculator oracle is weaker than a contract note — it shares the broker's *published* model rather than what was actually billed — and the fixture must say which source it used.

- [ ] **Step 1: Record the fixture**

Create `tests/paper/fixtures/contract_notes.py`:

```python
"""Ground-truth charge figures for the golden tests.

SOURCE: replace this line with either
  "Upstox contract note, order <id>, <date>"  (preferred), or
  "Upstox brokerage calculator, checked <date>"  (fallback oracle).

Every value below is the broker's own figure, not ours. If our calculator
disagrees with these, our calculator is wrong.

The numbers currently here are placeholders derived from the seeded rate
table and MUST be replaced with real broker output before this task is
considered done -- a golden test that grades itself against our own
arithmetic proves nothing.
"""

from decimal import Decimal

DELIVERY_BUY = {
    "source": "REPLACE ME",
    "quantity": Decimal("100"),
    "price": Decimal("1310.50"),
    "product": "DELIVERY",
    "side": "BUY",
    "expected": {
        "brokerage": Decimal("20.00"),
        "stt": Decimal("131"),
        "exchange_txn": Decimal("4.02"),
        "sebi_fee": Decimal("0.13"),
        "stamp_duty": Decimal("19.66"),
        "ipft": Decimal("0.01"),
        "gst": Decimal("4.33"),
        "dp_charges": Decimal("0.00"),
    },
}

DELIVERY_SELL = {
    "source": "REPLACE ME",
    "quantity": Decimal("100"),
    "price": Decimal("1350.00"),
    "product": "DELIVERY",
    "side": "SELL",
    "expected": {
        "brokerage": Decimal("20.00"),
        "stt": Decimal("135"),
        "exchange_txn": Decimal("4.14"),
        "sebi_fee": Decimal("0.14"),
        "stamp_duty": Decimal("0.00"),
        "ipft": Decimal("0.01"),
        "gst": Decimal("7.94"),
        "dp_charges": Decimal("20.00"),
    },
}
```

- [ ] **Step 2: Write the golden test**

Create `tests/paper/test_charges_golden.py`:

```python
"""Golden tests: our calculator must reproduce the broker's own figures.

This is the only honest validation of an Indian charge stack. If these
fail, the calculator is wrong -- do not adjust the fixtures to match.
"""

from datetime import date
from decimal import Decimal

import pytest

from trading.paper.charges import compute_charges, load_schedules
from trading.paper.enums import Product, Side
from tests.paper.fixtures.contract_notes import DELIVERY_BUY, DELIVERY_SELL


@pytest.mark.parametrize("note", [DELIVERY_BUY, DELIVERY_SELL],
                         ids=["delivery_buy", "delivery_sell"])
def test_matches_broker_contract_note(db_conn, note) -> None:
    assert note["source"] != "REPLACE ME", (
        "record a real Upstox contract note or calculator output first -- "
        "grading the calculator against our own arithmetic proves nothing"
    )
    schedules = load_schedules(
        db_conn, "UPSTOX", "NSE", "EQUITY",
        Product(note["product"]), date(2026, 6, 1),
    )
    got = compute_charges(
        schedules, Side(note["side"]), note["quantity"], note["price"],
    )
    for field, expected in note["expected"].items():
        actual = getattr(got, field)
        assert actual == expected, (
            f"{field}: ours {actual} vs broker {expected} "
            f"(source: {note['source']})"
        )


def test_total_matches_sum_of_components(db_conn) -> None:
    schedules = load_schedules(db_conn, "UPSTOX", "NSE", "EQUITY",
                               Product.DELIVERY, date(2026, 6, 1))
    got = compute_charges(schedules, Side.BUY, Decimal("100"), Decimal("1310.50"))
    assert got.total == sum(
        [got.brokerage, got.stt, got.exchange_txn, got.sebi_fee,
         got.stamp_duty, got.ipft, got.gst, got.dp_charges],
        Decimal("0"),
    )
```

- [ ] **Step 3: Run and reconcile**

Run: `uv run pytest tests/paper/test_charges_golden.py -v`

This will fail on the `REPLACE ME` guard. Obtain real broker figures, replace both fixtures including the `source` line, and re-run. When ours and the broker's disagree, **fix `charges.py`, never the fixture** — the mismatch is telling you something real about rounding order or the GST base.

- [ ] **Step 4: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add tests/paper/test_charges_golden.py tests/paper/fixtures/
git commit -m "test(paper): golden charge tests against broker ground truth"
```

---

## Task 5: Pure fill rules

**Files:**
- Create: `src/trading/paper/fills.py`
- Create: `tests/paper/test_fills.py`

**Interfaces:**
- Consumes: `Order`, `FillDecision`, `Side`, `OrderType` (Task 2).
- Produces: `decide_fill(order: Order, tick_price: Decimal, tick_ts: datetime, slippage_bps: Decimal) -> FillDecision | None` — **pure**.

- [ ] **Step 1: Write the failing tests**

Create `tests/paper/test_fills.py`:

```python
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce
from trading.paper.fills import decide_fill
from trading.paper.models import Order

T0 = datetime(2026, 8, 31, 6, 0, 0, tzinfo=UTC)
BPS = Decimal("10")  # 10 bps


def _order(**kw) -> Order:
    base = dict(
        order_id=1, portfolio_id=1, instrument_id=1, side=Side.BUY,
        order_type=OrderType.MARKET, quantity=Decimal("10"),
        filled_quantity=Decimal("0"), limit_price=None,
        product=Product.DELIVERY, time_in_force=TimeInForce.DAY,
        status=OrderStatus.OPEN, rationale="test", submitted_at=T0,
    )
    base.update(kw)
    return Order(**base)


def test_market_buy_fills_with_adverse_slippage() -> None:
    d = decide_fill(_order(), Decimal("100"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("100.10")  # +10bps, against the buyer
    assert d.quantity == Decimal("10")


def test_market_sell_slippage_is_also_adverse() -> None:
    d = decide_fill(_order(side=Side.SELL), Decimal("100"),
                    T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("99.90")


def test_limit_buy_does_not_fill_above_the_limit() -> None:
    o = _order(order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    assert decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS) is None


def test_limit_buy_fills_at_the_limit_not_the_better_tick_price() -> None:
    """Deliberate conservatism: assuming price improvement manufactures
    free money on every limit order, and Phase 3 would inherit it."""
    o = _order(order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    d = decide_fill(o, Decimal("97"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("99")


def test_limit_sell_fills_at_the_limit_when_crossed() -> None:
    o = _order(side=Side.SELL, order_type=OrderType.LIMIT,
               limit_price=Decimal("101"))
    d = decide_fill(o, Decimal("105"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("101")


def test_limit_orders_take_no_slippage() -> None:
    """Slippage models uncertainty about the traded price. A limit fill's
    price is known by construction."""
    o = _order(order_type=OrderType.LIMIT, limit_price=Decimal("99"))
    d = decide_fill(o, Decimal("98"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.price == Decimal("99")


def test_never_fills_on_a_tick_older_than_the_order() -> None:
    """The anti-lookahead invariant, asserted at the source."""
    assert decide_fill(_order(), Decimal("100"), T0 - timedelta(seconds=1), BPS) is None


def test_fills_only_the_remaining_quantity() -> None:
    o = _order(quantity=Decimal("10"), filled_quantity=Decimal("4"))
    d = decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS)
    assert d is not None
    assert d.quantity == Decimal("6")


def test_terminal_orders_never_fill() -> None:
    for status in (OrderStatus.FILLED, OrderStatus.CANCELLED,
                   OrderStatus.REJECTED, OrderStatus.EXPIRED):
        o = _order(status=status)
        assert decide_fill(o, Decimal("100"), T0 + timedelta(seconds=1), BPS) is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/paper/test_fills.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

Create `src/trading/paper/fills.py`:

```python
"""Pure fill rules: given an order and one price event, fill or not.

No I/O, no clock, no DB. This is the module Phase 3's backtest engine
reuses unchanged -- it is fed bars instead of ticks, which is what §6's
"one engine, two clock speeds" means in practice.
"""

from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from trading.paper.enums import OrderStatus, OrderType, Side
from trading.paper.models import FillDecision, Order

_TWO_DP = Decimal("0.01")
_BPS = Decimal("10000")

_FILLABLE = (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING)


def decide_fill(
    order: Order,
    tick_price: Decimal,
    tick_ts: datetime,
    slippage_bps: Decimal,
) -> FillDecision | None:
    """Whether this price event fills this order, and at what price."""
    if order.status not in _FILLABLE:
        return None
    if order.remaining <= 0:
        return None

    # Anti-lookahead: a price that printed before the order existed can
    # never have filled it.
    if tick_ts < order.submitted_at:
        return None

    if order.order_type is OrderType.MARKET:
        # Slippage always moves against the order.
        drift = tick_price * slippage_bps / _BPS
        price = tick_price + drift if order.side is Side.BUY else tick_price - drift
        price = price.quantize(_TWO_DP, rounding=ROUND_HALF_UP)
    else:
        limit = order.limit_price
        if limit is None:
            return None
        crossed = (
            tick_price <= limit if order.side is Side.BUY else tick_price >= limit
        )
        if not crossed:
            return None
        # Fill at the limit, not at the better tick price. Real venues
        # sometimes grant improvement; assuming it here would flatter every
        # limit order and every backtest built on this engine.
        price = limit

    return FillDecision(quantity=order.remaining, price=price, tick_ts=tick_ts)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/paper/test_fills.py -v`
Expected: PASS, 9 tests.

- [ ] **Step 5: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/fills.py tests/paper/test_fills.py
git commit -m "feat(paper): pure fill rules with anti-lookahead guard"
```

---

## Task 6: The atomic ledger write

**Files:**
- Create: `src/trading/paper/ledger.py`
- Create: `tests/paper/test_ledger.py`
- Modify: `pyproject.toml` (add `hypothesis` to dev dependencies)

**Interfaces:**
- Consumes: `Order`, `FillDecision`, `ChargeBreakdown` (Tasks 2, 3, 5).
- Produces:
  - `apply_fill(conn, order, decision, charges) -> int` (returns `fill_id`)
  - `replay_portfolio(conn, portfolio_id) -> tuple[Decimal, dict[int, Position]]`

- [ ] **Step 1: Add hypothesis**

```bash
uv add --dev hypothesis
```

- [ ] **Step 2: Write the failing tests**

Create `tests/paper/test_ledger.py`:

```python
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from trading.paper.enums import OrderStatus, Side
from trading.paper.ledger import apply_fill, replay_portfolio
from tests.paper.helpers import make_order, make_portfolio, simple_charges


def test_buy_decreases_cash_by_notional_plus_charges(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    order = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    charges = simple_charges(brokerage=Decimal("20"))
    apply_fill(db_conn, order, decision_at(Decimal("100")), charges)

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert cash == Decimal("100000") - Decimal("1000") - Decimal("20")


def test_sell_increases_cash_by_notional_minus_charges(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    buy = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, buy, decision_at(Decimal("100")), simple_charges())
    sell = make_order(db_conn, pid, side=Side.SELL, quantity=Decimal("10"))
    apply_fill(db_conn, sell, decision_at(Decimal("110")), simple_charges(
        brokerage=Decimal("20")))

    cash = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert cash == Decimal("100000") - Decimal("1000") + Decimal("1100") - Decimal("20")


def test_position_average_cost_after_two_buys(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    for price in (Decimal("100"), Decimal("120")):
        o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
        apply_fill(db_conn, o, decision_at(price), simple_charges())
    qty, avg = db_conn.execute(
        "SELECT quantity, avg_cost FROM positions WHERE portfolio_id=%s", (pid,)
    ).fetchone()
    assert qty == Decimal("20")
    assert avg == Decimal("110")


def test_sell_records_realised_pnl(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    b = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, b, decision_at(Decimal("100")), simple_charges())
    s = make_order(db_conn, pid, side=Side.SELL, quantity=Decimal("4"))
    apply_fill(db_conn, s, decision_at(Decimal("130")), simple_charges())
    realised = db_conn.execute(
        "SELECT realised_pnl FROM positions WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0]
    assert realised == Decimal("120")  # 4 * (130 - 100)


def test_order_status_advances_in_the_same_transaction(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, o, decision_at(Decimal("100")), simple_charges())
    status, filled = db_conn.execute(
        "SELECT status, filled_quantity FROM orders WHERE order_id=%s",
        (o.order_id,),
    ).fetchone()
    assert status == OrderStatus.FILLED
    assert filled == Decimal("10")


def test_buy_exceeding_cash_is_refused(db_conn) -> None:
    pid = make_portfolio(db_conn, cash=Decimal("500"))
    o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    with pytest.raises(Exception):
        apply_fill(db_conn, o, decision_at(Decimal("100")), simple_charges())


@settings(max_examples=25, deadline=None)
@given(
    prices=st.lists(
        st.decimals(min_value=Decimal("1"), max_value=Decimal("500"), places=2),
        min_size=1, max_size=8,
    )
)
def test_replay_reproduces_the_cached_cash_and_positions(db_conn, prices) -> None:
    """The invariant that earns `cash_balance` and `positions` their place
    as caches: replaying every fill must reproduce them exactly."""
    pid = make_portfolio(db_conn, cash=Decimal("1000000"))
    for p in prices:
        o = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("1"))
        apply_fill(db_conn, o, decision_at(Decimal(p)), simple_charges())

    cached_cash, cached_pos = db_conn.execute(
        "SELECT cash_balance FROM portfolios WHERE portfolio_id=%s", (pid,)
    ).fetchone()[0], _positions(db_conn, pid)
    replayed_cash, replayed_pos = replay_portfolio(db_conn, pid)
    assert replayed_cash == cached_cash
    assert replayed_pos == cached_pos
```

Create `tests/paper/helpers.py` with `make_portfolio`, `make_order`,
`simple_charges`, `decision_at`, and `_positions`. Each inserts the minimal
row and returns the id or model; `simple_charges` builds a `ChargeBreakdown`
with all fields zero except those passed. Import `decision_at` and
`_positions` into the test module.

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/paper/test_ledger.py -v`
Expected: FAIL — module not found.

- [ ] **Step 4: Implement**

Create `src/trading/paper/ledger.py`:

```python
"""The one atomic write: a fill and everything it implies.

`fills` and `ledger_entries` are the source of truth; `cash_balance` and
`positions` are caches maintained here in the same transaction. The cache
is only acceptable because `replay_portfolio` can prove it never drifted.

`apply_fill` deliberately does NOT commit. The caller owns the transaction
boundary -- that is what lets the engine commit fill, ledger, position,
cash, and order status as one unit, and what lets tests roll back.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from psycopg import Connection

from trading.paper.enums import EntryType, OrderStatus, Side
from trading.paper.models import ChargeBreakdown, FillDecision, Order, Position


def apply_fill(
    conn: Connection,
    order: Order,
    decision: FillDecision,
    charges: ChargeBreakdown,
) -> int:
    """Record a fill and update ledger, position, cash, and order status."""
    notional = decision.quantity * decision.price
    total_charges = charges.total
    # Charges always leave the account, whichever side the trade is.
    delta = (
        -(notional + total_charges)
        if order.side is Side.BUY
        else notional - total_charges
    )

    fill_row = conn.execute(
        "INSERT INTO fills (order_id, quantity, price, filled_at, tick_ts,"
        " brokerage, stt, exchange_txn, sebi_fee, stamp_duty, ipft, gst,"
        " dp_charges, total_charges)"
        " VALUES (%s,%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        " RETURNING fill_id",
        (
            order.order_id, decision.quantity, decision.price, decision.tick_ts,
            charges.brokerage, charges.stt, charges.exchange_txn,
            charges.sebi_fee, charges.stamp_duty, charges.ipft, charges.gst,
            charges.dp_charges, total_charges,
        ),
    ).fetchone()
    assert fill_row is not None
    fill_id = int(fill_row[0])

    # The ck_no_negative_cash constraint enforces the floor in the database,
    # so an over-spend raises here rather than silently going negative.
    cash_row = conn.execute(
        "UPDATE portfolios SET cash_balance = cash_balance + %s"
        " WHERE portfolio_id = %s RETURNING cash_balance",
        (delta, order.portfolio_id),
    ).fetchone()
    assert cash_row is not None
    balance_after = cash_row[0]

    conn.execute(
        "INSERT INTO ledger_entries (portfolio_id, ts, entry_type, amount,"
        " fill_id, balance_after) VALUES (%s, now(), %s, %s, %s, %s)",
        (order.portfolio_id, EntryType.FILL.value, delta, fill_id, balance_after),
    )

    _apply_position(conn, order, decision)

    filled = order.filled_quantity + decision.quantity
    status = (
        OrderStatus.FILLED
        if filled >= order.quantity
        else OrderStatus.PARTIALLY_FILLED
    )
    conn.execute(
        "UPDATE orders SET filled_quantity = %s, status = %s, updated_at = now()"
        " WHERE order_id = %s",
        (filled, status.value, order.order_id),
    )
    return fill_id


def _apply_position(
    conn: Connection, order: Order, decision: FillDecision
) -> None:
    """Weighted-average cost on increase; realised P&L on decrease.

    Long-only in this slice, so a sell can only reduce an existing position
    -- the API rejects a sell with no position behind it.
    """
    row = conn.execute(
        "SELECT quantity, avg_cost, realised_pnl FROM positions"
        " WHERE portfolio_id=%s AND instrument_id=%s FOR UPDATE",
        (order.portfolio_id, order.instrument_id),
    ).fetchone()

    if order.side is Side.BUY:
        if row is None:
            conn.execute(
                "INSERT INTO positions (portfolio_id, instrument_id, quantity,"
                " avg_cost, realised_pnl) VALUES (%s,%s,%s,%s,0)",
                (order.portfolio_id, order.instrument_id,
                 decision.quantity, decision.price),
            )
            return
        qty, avg, _ = row
        new_qty = qty + decision.quantity
        # Weighted average over the *gross* traded price. Charges are a cash
        # cost, not part of the position's cost basis -- folding them in here
        # would double-count them against realised P&L on the way out.
        new_avg = ((qty * avg) + (decision.quantity * decision.price)) / new_qty
        conn.execute(
            "UPDATE positions SET quantity=%s, avg_cost=%s"
            " WHERE portfolio_id=%s AND instrument_id=%s",
            (new_qty, new_avg, order.portfolio_id, order.instrument_id),
        )
        return

    assert row is not None, "sell with no position; the API must reject this"
    qty, avg, realised = row
    new_qty = qty - decision.quantity
    gain = (decision.price - avg) * decision.quantity
    conn.execute(
        "UPDATE positions SET quantity=%s, realised_pnl=%s"
        " WHERE portfolio_id=%s AND instrument_id=%s",
        (new_qty, realised + gain, order.portfolio_id, order.instrument_id),
    )


def replay_portfolio(
    conn: Connection, portfolio_id: int
) -> tuple[Decimal, dict[int, Position]]:
    """Recompute cash and positions from `fills` alone.

    The invariant that earns the caches their place: this must reproduce
    `portfolios.cash_balance` and every `positions` row exactly.
    """
    initial = conn.execute(
        "SELECT initial_capital FROM portfolios WHERE portfolio_id=%s",
        (portfolio_id,),
    ).fetchone()
    assert initial is not None
    cash = Decimal(initial[0])

    rows = conn.execute(
        "SELECT o.instrument_id, o.side, f.quantity, f.price, f.total_charges"
        " FROM fills f JOIN orders o ON o.order_id = f.order_id"
        " WHERE o.portfolio_id = %s ORDER BY f.fill_id",
        (portfolio_id,),
    ).fetchall()

    positions: dict[int, Position] = {}
    for instrument_id, side, quantity, price, total_charges in rows:
        notional = quantity * price
        if side == Side.BUY:
            cash -= notional + total_charges
            held = positions.get(instrument_id)
            if held is None:
                positions[instrument_id] = Position(
                    portfolio_id=portfolio_id, instrument_id=instrument_id,
                    quantity=quantity, avg_cost=price,
                    realised_pnl=Decimal("0"),
                )
            else:
                new_qty = held.quantity + quantity
                positions[instrument_id] = held.model_copy(update={
                    "quantity": new_qty,
                    "avg_cost": ((held.quantity * held.avg_cost)
                                 + (quantity * price)) / new_qty,
                })
        else:
            cash += notional - total_charges
            held = positions[instrument_id]
            positions[instrument_id] = held.model_copy(update={
                "quantity": held.quantity - quantity,
                "realised_pnl": held.realised_pnl
                + (price - held.avg_cost) * quantity,
            })

    return cash, positions
```

Note the cost-basis decision made explicit in `_apply_position`: charges are
a cash cost, not part of the position's average cost. Folding them into
`avg_cost` would double-count them — once as cash out, again as a smaller
realised gain on exit.

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/paper/test_ledger.py -v`
Expected: PASS.

- [ ] **Step 6: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/ledger.py tests/paper/test_ledger.py tests/paper/helpers.py pyproject.toml uv.lock
git commit -m "feat(paper): atomic fill-to-ledger write with replay invariant"
```

---

## Task 7: The order API

**Files:**
- Create: `src/trading/paper/api.py`
- Modify: `src/trading/streaming/gateway.py` (mount the router)
- Create: `tests/paper/test_api.py`

**Interfaces:**
- Consumes: models, `load_schedules`, `MissingChargeSchedule`.
- Produces: `router` — `POST /portfolios`, `GET /portfolios`, `POST /orders`, `DELETE /orders/{order_id}`, `GET /portfolios/{id}/positions`.

**Every route is a plain `def`.** Read `src/trading/streaming/market_data_api.py` and copy its structure, its `Depends(get_db_connection)` usage, and its money-serialisation pattern.

- [ ] **Step 1: Write the failing tests**

Cover: submitting a valid order returns 201 with status `PENDING`; an order with empty `rationale` is rejected 422; an order exceeding cash is rejected 400 with a reason; an order for an instrument with no charge schedule is rejected 400 naming the missing schedule (never accepted with zero charges); a repeated `idempotency_key` returns the original order rather than creating a second; cancelling an `OPEN` order sets `CANCELLED`; cancelling a `FILLED` order returns 409; and `inspect.iscoroutinefunction` is False for every route endpoint on the router.

That last test is the guard against re-introducing `5d03a2e`:

```python
import inspect

from trading.paper.api import router


def test_no_route_is_a_coroutine_function() -> None:
    """psycopg is synchronous; an `async def` route runs it on the event
    loop and deadlocks the gateway under concurrency (see 5d03a2e)."""
    for route in router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        assert not inspect.iscoroutinefunction(endpoint), (
            f"{endpoint.__name__} must be a plain def"
        )
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/paper/test_api.py -v`

- [ ] **Step 3: Implement the router and mount it**

Validation order in `POST /orders`, all before the row is written: portfolio exists and is `ACTIVE`; instrument exists; quantity positive; `rationale` non-empty; market open per `trading_calendar` for the instrument's exchange (crypto is 24/7 — skip the calendar check when `asset_class == "CRYPTO"`); sufficient cash for a buy or sufficient position for a sell; and a `charge_schedules` row exists for this instrument, product, and today. On success, insert with `status=PENDING` and publish `{"action": "new", "order_id": ...}` to the Redis `orders:control` channel.

In `gateway.py`, add `app.include_router(paper_api.router)` beside the existing `market_data_api` mount.

- [ ] **Step 4: Run to verify pass, then gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/api.py src/trading/streaming/gateway.py tests/paper/test_api.py
git commit -m "feat(paper): order and portfolio API"
```

---

## Task 8: The `paper_engine` process

**Files:**
- Create: `src/trading/paper/engine.py`
- Create: `tests/paper/test_engine.py`

**Interfaces:**
- Consumes: `decide_fill`, `compute_charges`, `load_schedules`, `apply_fill`.
- Produces: `run_engine(redis, conn_factory, max_ticks=None)` — `max_ticks` is the test seam, matching `crypto_ingestor.run_ingestion_loop`.

Read `src/trading/streaming/crypto_ingestor.py` and `bar_aggregator.py` first — reuse their reconnect/backoff shape and their per-message exception containment.

- [ ] **Step 1: Write the failing tests**

Cover: an open order in the DB is loaded into memory on startup and `PENDING` is promoted to `OPEN`; a tick on a subscribed instrument fills a matching market order and writes exactly one `fills` row; a tick on an unrelated instrument fills nothing; a malformed tick payload is logged and dropped without killing the loop (the `5373207` containment pattern); a `DAY` order is swept to `EXPIRED` at session close; and a second identical tick does not double-fill an already-`FILLED` order.

- [ ] **Step 2: Run to verify failure, then implement**

The loop: `psubscribe("ticks:*")` plus `subscribe("orders:control")`; open orders held as `dict[int, list[Order]]` keyed by `instrument_id`; on each tick, iterate that instrument's orders and call `decide_fill`; on a decision, open a connection, `load_schedules`, `compute_charges`, `apply_fill`, `conn.commit()`, then publish to `fills:{portfolio_id}`. Wrap each tick's handling in `try/except Exception` with a `log.warning` — one bad tick must never kill the loop.

- [ ] **Step 3: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/engine.py tests/paper/test_engine.py
git commit -m "feat(paper): paper_engine fill loop over the tick stream"
```

---

## Task 9: Clock-parity test — the "one engine, two clock speeds" proof

**Files:**
- Create: `tests/paper/test_clock_parity.py`

**Interfaces:** consumes `decide_fill` only. No production code changes.

This task exists to prove §6's central claim before anything is built on it.

- [ ] **Step 1: Write the test**

Build a synthetic price path as ticks, and the 1-minute bars that path aggregates to. Feed the same resting limit order through `decide_fill` twice — once per tick, once per bar (using each bar's high for a sell trigger and low for a buy trigger). Assert:

1. If the tick path fills, the bar path fills too (coarser data never misses a crossing that finer data caught).
2. **The bar path's fill price is never better than the tick path's** — better for a buy means lower, for a sell means higher. If coarse data yields a better price, lookahead is leaking in.

- [ ] **Step 2: Run, then gate and commit**

```bash
uv run pytest tests/paper/test_clock_parity.py -v
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add tests/paper/test_clock_parity.py
git commit -m "test(paper): bar-driven fills never beat tick-driven fills"
```

---

## Task 10: Circuit breaker

**Files:**
- Create: `src/trading/paper/breaker.py`
- Create: `tests/paper/test_breaker.py`
- Modify: `src/trading/paper/engine.py` (drive it on a 5s timer and after each fill)

**Interfaces:**
- Produces:
  - `compute_equity(cash: Decimal, positions: Sequence[Position], marks: Mapping[int, Decimal]) -> Decimal` — pure
  - `evaluate_breach(equity, day_open_equity, peak_equity, max_daily_loss, max_drawdown_pct) -> str | None` — pure, returns the reason or `None`
  - `record_snapshot(conn, portfolio_id, ts, equity) -> Decimal` — writes a `portfolio_equity_snapshots` row, carrying `peak_equity` forward from the previous snapshot and deriving `drawdown_pct`; returns the new peak
  - `trip(conn, portfolio_id, reason, equity, threshold) -> None`

- [ ] **Step 1: Write the failing tests**

Cover: equity is cash plus marked positions; a loss inside the limit does not breach; a loss exceeding `max_daily_loss` breaches with that reason; a drawdown from peak exceeding `max_drawdown_pct` breaches; `None` limits never breach; `trip` sets the portfolio to `PAUSED`, cancels its `OPEN` and `PENDING` orders, and writes a `circuit_breaker_events` row; and a position with no mark available raises rather than being valued at zero.

**`trip` does NOT enqueue an alert in this task.** `alerts.enqueue_alert` does not exist until Task 11, which owns wiring alerting into both the engine and the breaker. Do not create a stub for it here.

Also cover `record_snapshot` specifically, because `peak_equity` is the only piece of breaker state that must survive a restart:

- a first snapshot sets `peak_equity` to the current equity and `drawdown_pct` to zero;
- a higher equity raises `peak_equity`;
- a lower equity leaves `peak_equity` unchanged and reports a positive `drawdown_pct`;
- and after a restart, the peak is read back from the last snapshot rather than reset — otherwise a drawdown breach silently rearms itself every time the engine bounces.

- [ ] **Step 2: Run to verify failure, implement, verify pass**

- [ ] **Step 3: Wire into the engine**

Evaluate on a 5-second timer and immediately after every fill. Worst-case detection lag is 5 seconds by design — per-tick evaluation would mean ~107 evaluations a second for a threshold that moves in minutes.

Each evaluation calls `record_snapshot` before `evaluate_breach`, so the equity curve is persisted whether or not a breach occurs. That series is what Phase 3's metrics suite (drawdown depth and duration, rolling Sharpe, the equity curve itself) reads later — if it is only written on breach, the history is useless.

On restart, seed `peak_equity` from the most recent snapshot rather than from current equity.

- [ ] **Step 4: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/breaker.py src/trading/paper/engine.py tests/paper/test_breaker.py
git commit -m "feat(paper): portfolio circuit breaker"
```

---

## Task 11: Telegram alerts via transactional outbox

**Files:**
- Create: `src/trading/paper/alerts.py`
- Create: `tests/paper/test_alerts.py`
- Modify: `src/trading/config.py` (add `telegram_bot_token`, `telegram_chat_id`)

**Interfaces:**
- Produces:
  - `enqueue_alert(conn, kind: str, payload: dict) -> None` — writes an `alert_deliveries` row, no network
  - `run_alert_worker(conn_factory, sender, max_batches=None)` — drains `PENDING` with retry/backoff

- [ ] **Step 1: Write the failing tests**

Cover: `enqueue_alert` writes a `PENDING` row and makes **no network call**; the worker sends pending rows and marks them `SENT` with `sent_at`; a sender raising leaves the row `PENDING`, increments `attempts`, and records `last_error`; a row exceeding max attempts moves to `FAILED` and stops being retried; and — the important one — **a sender that always raises does not roll back or affect the fill that enqueued the alert.**

The last test is the point of the whole pattern:

```python
def test_telegram_failure_never_affects_the_fill(db_conn) -> None:
    """The outbox exists so a third-party outage cannot reach the fill
    path. The fill is committed; only the notification is late."""
    pid = make_portfolio(db_conn, cash=Decimal("100000"))
    order = make_order(db_conn, pid, side=Side.BUY, quantity=Decimal("10"))
    apply_fill(db_conn, order, decision_at(Decimal("100")), simple_charges())
    enqueue_alert(db_conn, "FILL", {"order_id": order.order_id})

    def always_fails(_payload: str) -> None:
        raise RuntimeError("telegram is down")

    run_alert_worker(lambda: db_conn, always_fails, max_batches=1)

    status = db_conn.execute(
        "SELECT status FROM orders WHERE order_id=%s", (order.order_id,)
    ).fetchone()[0]
    assert status == OrderStatus.FILLED, "the fill must survive a failed alert"
    delivery = db_conn.execute(
        "SELECT status, attempts FROM alert_deliveries"
    ).fetchone()
    assert delivery[0] == "PENDING"
    assert delivery[1] == 1
```

- [ ] **Step 2: Run to verify failure, implement, verify pass**

The `sender` is injected so tests never touch the network. The real sender posts to the Telegram Bot API; if `telegram_bot_token` is unset, the worker logs once and idles rather than crashing — an unconfigured bot is not an error.

- [ ] **Step 3: Wire `enqueue_alert` into the engine and breaker**

Called inside the same transaction as the fill and the trip. Task 10 deliberately left `trip` without alerting, so add the call here and add the assertion that a trip enqueues a `BREACH` alert — that test belongs to this task, not Task 10.

- [ ] **Step 4: Gate and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src && uv run pytest
git add src/trading/paper/alerts.py src/trading/paper/engine.py src/trading/paper/breaker.py src/trading/config.py tests/paper/test_alerts.py
git commit -m "feat(paper): telegram alerts via transactional outbox"
```

---

## Task 12: End-to-end live verification (controller-run)

**Files:** none — this runs the shipped stack and records evidence, matching the evidentiary standard of `docs/verification/` and this project's prior live-verification tasks.

Controller-run, not delegated: it needs a human eye on real market data.

- [ ] **Step 1:** Bring up the stack: `docker compose up -d`, `uv run alembic upgrade head`, then the gateway, `crypto_ingestor`, `bar_aggregator`, `upstox_ingestor` (during an NSE session), and `uv run python -m trading.paper.engine`.
- [ ] **Step 2:** Create a portfolio with ₹1,000,000 virtual capital via `POST /portfolios`.
- [ ] **Step 3:** Submit a market buy for 100 RELIANCE, `DELIVERY`, with a rationale. Confirm it fills within a tick or two, and that cash decreases by notional plus charges.
- [ ] **Step 4:** Compare the fill's itemised charges against Upstox's brokerage calculator for the same quantity, price, and product. Record both. **Any mismatch is a bug in the calculator, not in the note.**
- [ ] **Step 5:** Sell the position and confirm the DP charge appears on the delivery sell and STT does not (delivery STT applies to both sides — confirm it does appear, and that DP does not appear on a comparable intraday sell).
- [ ] **Step 6:** Submit a limit buy far below market. Confirm it rests as `OPEN`, does not fill, and sweeps to `EXPIRED` at session close.
- [ ] **Step 7:** Set `max_daily_loss` low, force a loss, confirm the portfolio pauses, open orders cancel, and a Telegram alert arrives.
- [ ] **Step 8:** Run `replay_portfolio` against the live portfolio and confirm it reproduces `cash_balance` and `positions` exactly.
- [ ] **Step 9:** Record in this plan's Completion Notes: symbols and quantities used, the charge comparison table (ours vs broker, per line), screenshots, whether NSE was open, and any issues found — fixed inline if small, or filed as a new discovered-live task the way Tasks 8/9/10 were added to the intraday-backfill plan.
