"""Paper trading core: portfolios, orders, fills, ledger, positions, equity
snapshots, circuit-breaker events, an alert outbox, and dated charge
schedules.

Part of the paper-trading sub-project (.superpowers/sdd/2026-08-31-
paper-trading-core/). `charge_schedules` is dated (`effective_from` /
`effective_to`) rather than a set of constants: NSE cash transaction
charges were revised 0.00297% -> 0.00307% effective 2026-03-01, and this
project's historical candle backfill spans 2022-2026, crossing that
boundary, so a single flat rate would misprice most of the historical
period. Seeds one local user and the Upstox/NSE + Binance charge rows
current as of 2026-08-31.

Revision ID: 0007
Revises: 0006
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import Connection

# revision identifiers, used by Alembic.
revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


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
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("user_id", "name", name="uq_portfolio_name"),
        sa.CheckConstraint("cash_balance >= 0", name="ck_no_negative_cash"),
    )

    op.create_table(
        "orders",
        sa.Column("order_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "portfolio_id",
            sa.BigInteger,
            sa.ForeignKey("portfolios.portfolio_id"),
            nullable=False,
        ),
        sa.Column(
            "instrument_id",
            sa.BigInteger,
            sa.ForeignKey("instruments.instrument_id"),
            nullable=False,
        ),
        sa.Column("side", sa.Text, nullable=False),
        sa.Column("order_type", sa.Text, nullable=False),
        sa.Column("quantity", sa.Numeric(18, 8), nullable=False),
        sa.Column("filled_quantity", sa.Numeric(18, 8), nullable=False, server_default="0"),
        sa.Column("limit_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("product", sa.Text, nullable=False),
        sa.Column("time_in_force", sa.Text, nullable=False, server_default="DAY"),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("rationale", sa.Text, nullable=False),
        sa.Column("rejection_reason", sa.Text, nullable=True),
        sa.Column("idempotency_key", sa.Text, nullable=False),
        sa.Column(
            "submitted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_order_idempotency"),
        sa.CheckConstraint("quantity > 0", name="ck_order_qty_positive"),
        sa.CheckConstraint("length(rationale) > 0", name="ck_rationale_present"),
    )
    op.create_index("ix_orders_open", "orders", ["status", "instrument_id"])

    op.create_table(
        "fills",
        sa.Column("fill_id", sa.BigInteger, primary_key=True),
        sa.Column("order_id", sa.BigInteger, sa.ForeignKey("orders.order_id"), nullable=False),
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
        sa.Column(
            "portfolio_id",
            sa.BigInteger,
            sa.ForeignKey("portfolios.portfolio_id"),
            nullable=False,
        ),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("entry_type", sa.Text, nullable=False),
        sa.Column("amount", sa.Numeric(18, 4), nullable=False),
        sa.Column("fill_id", sa.BigInteger, sa.ForeignKey("fills.fill_id"), nullable=True),
        sa.Column("balance_after", sa.Numeric(18, 4), nullable=False),
        sa.Index("ix_ledger_portfolio_ts", "portfolio_id", "ts"),
    )

    op.create_table(
        "positions",
        sa.Column(
            "portfolio_id",
            sa.BigInteger,
            sa.ForeignKey("portfolios.portfolio_id"),
            primary_key=True,
        ),
        sa.Column(
            "instrument_id",
            sa.BigInteger,
            sa.ForeignKey("instruments.instrument_id"),
            primary_key=True,
        ),
        sa.Column("quantity", sa.Numeric(18, 8), nullable=False),
        sa.Column("avg_cost", sa.Numeric(18, 4), nullable=False),
        sa.Column("realised_pnl", sa.Numeric(18, 4), nullable=False, server_default="0"),
    )

    op.create_table(
        "portfolio_equity_snapshots",
        sa.Column(
            "portfolio_id",
            sa.BigInteger,
            sa.ForeignKey("portfolios.portfolio_id"),
            nullable=False,
        ),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("peak_equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("drawdown_pct", sa.Numeric(9, 4), nullable=False),
        sa.PrimaryKeyConstraint("portfolio_id", "ts"),
    )

    op.create_table(
        "circuit_breaker_events",
        sa.Column("event_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "portfolio_id",
            sa.BigInteger,
            sa.ForeignKey("portfolios.portfolio_id"),
            nullable=False,
        ),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("equity", sa.Numeric(18, 4), nullable=False),
        sa.Column("threshold", sa.Numeric(18, 4), nullable=False),
    )

    op.create_table(
        "alert_deliveries",
        sa.Column("delivery_id", sa.BigInteger, primary_key=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
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
        sa.Index(
            "ix_charge_lookup",
            "broker",
            "exchange",
            "asset_class",
            "product",
            "effective_from",
        ),
    )

    _seed(op.get_bind())


def _seed(conn: Connection) -> None:
    conn.execute(
        sa.text("INSERT INTO users (email) VALUES (:e) ON CONFLICT (email) DO NOTHING"),
        {"e": "local@paper.trading"},
    )

    # Rates verified 2026-08-31 against upstox.com/brokerage-charges/,
    # zerodha.com/charges/, and the NSE transaction-charge circular.
    # Percentage rates are stored as fractions: 0.1% -> 0.001.
    src_up = "https://upstox.com/brokerage-charges/ (checked 2026-08-31)"
    src_nse = "NSE circular FA64232; revision effective 2026-03-01"

    # (product, charge_type, basis, side, rate, cap, rounding,
    #  gst_base, eff_from, eff_to, source)
    ChargeRow = tuple[str, str, str, str, str, str | None, str, str | None, str, str | None, str]
    rows: list[ChargeRow] = [
        (
            "DELIVERY",
            "BROKERAGE",
            "FLAT_PER_ORDER",
            "BOTH",
            "20",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "INTRADAY",
            "BROKERAGE",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.001",
            "20",
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "DELIVERY",
            "STT",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.001",
            None,
            "NEAREST_RUPEE",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "INTRADAY",
            "STT",
            "PERCENT_OF_TURNOVER",
            "SELL",
            "0.00025",
            None,
            "NEAREST_RUPEE",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "DELIVERY",
            "EXCHANGE_TXN",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.0000297",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            "2026-03-01",
            src_nse,
        ),
        (
            "DELIVERY",
            "EXCHANGE_TXN",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.0000307",
            None,
            "TWO_DECIMALS",
            None,
            "2026-03-01",
            None,
            src_nse,
        ),
        (
            "INTRADAY",
            "EXCHANGE_TXN",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.0000297",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            "2026-03-01",
            src_nse,
        ),
        (
            "INTRADAY",
            "EXCHANGE_TXN",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.0000307",
            None,
            "TWO_DECIMALS",
            None,
            "2026-03-01",
            None,
            src_nse,
        ),
        (
            "DELIVERY",
            "SEBI_FEE",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.000001",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "INTRADAY",
            "SEBI_FEE",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.000001",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "DELIVERY",
            "STAMP_DUTY",
            "PERCENT_OF_TURNOVER",
            "BUY",
            "0.00015",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "INTRADAY",
            "STAMP_DUTY",
            "PERCENT_OF_TURNOVER",
            "BUY",
            "0.00003",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "DELIVERY",
            "IPFT",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.000000001",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "INTRADAY",
            "IPFT",
            "PERCENT_OF_TURNOVER",
            "BOTH",
            "0.000000001",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "DELIVERY",
            "DP_CHARGES",
            "FLAT_PER_SCRIP_PER_DAY",
            "SELL",
            "20",
            None,
            "TWO_DECIMALS",
            None,
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "DELIVERY",
            "GST",
            "PERCENT_OF_CHARGES",
            "BOTH",
            "0.18",
            None,
            "TWO_DECIMALS",
            "BROKERAGE,EXCHANGE_TXN,DP_CHARGES,IPFT",
            "2024-10-01",
            None,
            src_up,
        ),
        (
            "INTRADAY",
            "GST",
            "PERCENT_OF_CHARGES",
            "BOTH",
            "0.18",
            None,
            "TWO_DECIMALS",
            "BROKERAGE,EXCHANGE_TXN,IPFT",
            "2024-10-01",
            None,
            src_up,
        ),
    ]
    stmt = sa.text(
        "INSERT INTO charge_schedules (broker, exchange, asset_class, product,"
        " charge_type, basis, applies_to_side, rate, cap, rounding,"
        " gst_base_types, effective_from, effective_to, source_note)"
        " VALUES ('UPSTOX','NSE','EQUITY', :p, :ct, :b, :s, :r, :cap, :rnd,"
        " :gst, :ef, :et, :src)"
    )
    for p, ct, b, s, r, cap, rnd, gst, ef, et, src in rows:
        params: dict[str, Any] = {
            "p": p,
            "ct": ct,
            "b": b,
            "s": s,
            "r": Decimal(r),
            "cap": Decimal(cap) if cap else None,
            "rnd": rnd,
            "gst": gst,
            "ef": ef,
            "et": et,
            "src": src,
        }
        conn.execute(stmt, params)

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
    conn.execute(
        crypto,
        {"ct": "BROKERAGE", "s": "BOTH", "r": Decimal("0.001"), "src": src_bin},
    )


def downgrade() -> None:
    for t in (
        "charge_schedules",
        "alert_deliveries",
        "circuit_breaker_events",
        "portfolio_equity_snapshots",
        "positions",
        "ledger_entries",
        "fills",
        "orders",
        "portfolios",
    ):
        op.drop_table(t)
