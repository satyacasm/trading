"""Seeding perpetual contracts into the instrument master."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from trading.sources.binance_futures import PerpContractSpec
from trading.streaming.seed_perp_instruments import (
    PERP_UNIVERSE,
    perp_canonical_keys,
    seed_perp_instruments,
)


def _spec(symbol: str = "BTCUSDT", tick: str = "0.10") -> PerpContractSpec:
    return PerpContractSpec(
        symbol=symbol,
        base_asset=symbol.removesuffix("USDT"),
        quote_asset="USDT",
        tick_size=Decimal(tick),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("50"),
        liquidation_fee=Decimal("0.0125"),
    )


def test_a_perp_is_a_different_instrument_from_its_spot_pair(db_conn) -> None:
    """BTC-USDT spot and BTC-USDT perp are separate tradable things with
    separate prices, separate costs and separate positions. They must not
    collide on `canonical_key`, or seeding one would silently overwrite the
    other's asset class."""
    from trading.streaming.seed_instruments import seed_crypto_instruments

    spot = seed_crypto_instruments(db_conn, pairs=["BTC-USDT"])["BTC-USDT"]
    perp = seed_perp_instruments(db_conn, [_spec()], on=date(2026, 9, 5))["BTC-USDT"]
    assert spot != perp

    asset_class, segment, exchange = db_conn.execute(
        "SELECT asset_class, segment, exchange FROM instruments WHERE instrument_id = %s",
        (perp,),
    ).fetchone()
    assert (asset_class, segment, exchange) == ("PERP", "PERP", "BINANCE_FUTURES")


def test_the_symbol_is_normalised_to_the_platform_convention(db_conn) -> None:
    """Binance says BTCUSDT; every other instrument in this database says
    BTC-USDT. One convention, applied at the boundary."""
    seed_perp_instruments(db_conn, [_spec()], on=date(2026, 9, 5))
    symbol = db_conn.execute(
        "SELECT symbol FROM instruments WHERE asset_class='PERP' AND segment='PERP'"
        " ORDER BY instrument_id DESC LIMIT 1"
    ).fetchone()[0]
    assert symbol == "BTC-USDT"


def test_the_filters_land_in_the_specs_table(db_conn) -> None:
    iid = seed_perp_instruments(db_conn, [_spec()], on=date(2026, 9, 5))["BTC-USDT"]
    row = db_conn.execute(
        "SELECT tick_size, step_size, min_qty, min_notional, liquidation_fee"
        " FROM perp_contract_specs WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    assert row == (
        Decimal("0.100000000000"),
        Decimal("0.001000000000"),
        Decimal("0.001000000000"),
        Decimal("50.0000"),
        Decimal("0.012500"),
    )


def test_reseeding_the_same_day_is_idempotent(db_conn) -> None:
    first = seed_perp_instruments(db_conn, [_spec()], on=date(2026, 9, 5))
    second = seed_perp_instruments(db_conn, [_spec()], on=date(2026, 9, 5))
    assert first == second
    count = db_conn.execute(
        "SELECT count(*) FROM perp_contract_specs WHERE instrument_id = %s",
        (first["BTC-USDT"],),
    ).fetchone()[0]
    assert count == 1


def test_a_revised_filter_opens_a_new_dated_row_and_closes_the_old(db_conn) -> None:
    """Binance revises tick sizes. Overwriting would make an order accepted
    last March unreconstructable against last March's rules."""
    iid = seed_perp_instruments(db_conn, [_spec(tick="0.10")], on=date(2026, 9, 5))["BTC-USDT"]
    seed_perp_instruments(db_conn, [_spec(tick="0.50")], on=date(2026, 9, 8))

    rows = db_conn.execute(
        "SELECT effective_from, effective_to, tick_size FROM perp_contract_specs"
        " WHERE instrument_id = %s ORDER BY effective_from",
        (iid,),
    ).fetchall()
    assert [(r[0], r[1]) for r in rows] == [
        (date(2026, 9, 5), date(2026, 9, 8)),
        (date(2026, 9, 8), None),
    ]
    assert rows[1][2] == Decimal("0.500000000000")


def test_the_universe_is_a_deliberate_list_not_everything_binance_lists() -> None:
    """Binance lists ~500 perpetuals. Seeding all of them puts hundreds of
    illiquid contracts in a master that every instrument query scans, for a
    single-user platform that will trade a handful."""
    assert "BTCUSDT" in PERP_UNIVERSE
    assert "ETHUSDT" in PERP_UNIVERSE
    assert len(PERP_UNIVERSE) < 30


def test_canonical_keys_resolve_without_touching_the_database() -> None:
    keys = perp_canonical_keys(["BTCUSDT"])
    assert keys == ["BINANCE_FUTURES:PERP:BTC-USDT"]


def test_seeding_a_contract_outside_the_universe_is_refused(db_conn) -> None:
    with pytest.raises(ValueError, match="SHIBUSDT"):
        seed_perp_instruments(db_conn, [_spec(symbol="SHIBUSDT")], on=date(2026, 9, 5))


def test_margin_tiers_land_against_the_right_instrument(db_conn) -> None:
    from decimal import Decimal as D

    from trading.sources.binance_margin_tiers import MarginTier
    from trading.streaming.seed_perp_margin_tiers import seed_margin_tiers

    iid = seed_perp_instruments(db_conn, [_spec()], on=date(2026, 9, 5))["BTC-USDT"]
    written = seed_margin_tiers(
        db_conn,
        [
            MarginTier("BTCUSDT", D("0"), D("50000"), D("125"), D("0.004"), D("0")),
            # Outside the seeded universe: the endpoint returns every
            # contract Binance lists, which is not a reason to widen ours.
            MarginTier("SHIBUSDT", D("0"), D("50000"), D("75"), D("0.01"), D("0")),
        ],
        on=date(2026, 9, 5),
    )
    assert written == 1
    row = db_conn.execute(
        "SELECT maintenance_rate, max_leverage FROM perp_margin_tiers WHERE instrument_id=%s",
        (iid,),
    ).fetchone()
    assert row == (Decimal("0.004000"), Decimal("125.00"))
