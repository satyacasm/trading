"""Which option contracts the chain recorder subscribes to.

The recorder's whole value is that it accrues history nobody can sell back
to you later, so the universe it picks is the thing that decides what you
will and will not be able to backtest in six months.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from trading.recorder.universe import UpstoxInstrument, select_chain


def _option(
    strike: str, option_type: str, expiry: date = date(2026, 9, 10), underlying: str = "NIFTY"
) -> UpstoxInstrument:
    return UpstoxInstrument(
        instrument_key=f"NSE_FO|{underlying}{strike}{option_type}{expiry:%d%m}",
        segment="NSE_FO",
        underlying_symbol=underlying,
        underlying_key="NSE_INDEX|Nifty 50",
        instrument_type=option_type,
        expiry=expiry,
        strike=Decimal(strike),
        lot_size=75,
        trading_symbol=f"{underlying} {strike} {option_type}",
    )


def _chain(expiry: date = date(2026, 9, 10), step: int = 50) -> list[UpstoxInstrument]:
    return [
        _option(str(24000 + n * step), option_type, expiry)
        for n in range(-40, 41)
        for option_type in ("CE", "PE")
    ]


def test_it_takes_the_strikes_around_the_anchor_in_both_option_types() -> None:
    selection = select_chain(
        _chain(),
        underlying="NIFTY",
        anchor=Decimal("24391.10"),
        anchor_date=date(2026, 9, 4),
        today=date(2026, 9, 7),
        strikes=2,
        expiries=1,
    )
    # 24391.10 rounds to the 24400 strike; two either side, calls and puts.
    assert sorted({r.strike for r in selection.contracts}) == [
        Decimal("24300"),
        Decimal("24350"),
        Decimal("24400"),
        Decimal("24450"),
        Decimal("24500"),
    ]
    assert {r.instrument_type for r in selection.contracts} == {"CE", "PE"}
    assert len(selection.contracts) == 10


def test_the_strike_step_comes_from_the_data_not_from_a_constant() -> None:
    """NIFTY steps 50, BANKNIFTY 100, and a step that is assumed rather than
    measured silently records a window a fraction of the intended width the
    first time an exchange revises one."""
    selection = select_chain(
        _chain(step=100),
        underlying="NIFTY",
        anchor=Decimal("24000"),
        anchor_date=date(2026, 9, 4),
        today=date(2026, 9, 7),
        strikes=1,
        expiries=1,
    )
    assert sorted({r.strike for r in selection.contracts}) == [
        Decimal("23900"),
        Decimal("24000"),
        Decimal("24100"),
    ]


def test_it_records_the_underlying_itself_alongside_the_options() -> None:
    """Reconstructing a held position's strike relative to spot, and computing
    IV from premiums at all, both need the spot series. Recording a chain
    without its underlying leaves a chain that cannot be interpreted."""
    selection = select_chain(
        _chain(),
        underlying="NIFTY",
        anchor=Decimal("24400"),
        anchor_date=date(2026, 9, 4),
        today=date(2026, 9, 7),
        strikes=1,
        expiries=1,
    )
    assert "NSE_INDEX|Nifty 50" in selection.instrument_keys
    assert selection.instrument_keys[0] == "NSE_INDEX|Nifty 50"


def test_expired_contracts_are_never_selected() -> None:
    past = _chain(expiry=date(2026, 9, 3))
    future = _chain(expiry=date(2026, 9, 10))
    selection = select_chain(
        past + future,
        underlying="NIFTY",
        anchor=Decimal("24400"),
        anchor_date=date(2026, 9, 4),
        today=date(2026, 9, 7),
        strikes=1,
        expiries=1,
    )
    assert {r.expiry for r in selection.contracts} == {date(2026, 9, 10)}


def test_it_takes_the_nearest_expiries_in_order() -> None:
    chain = (
        _chain(expiry=date(2026, 9, 10))
        + _chain(expiry=date(2026, 9, 17))
        + _chain(expiry=date(2026, 9, 24))
    )
    selection = select_chain(
        chain,
        underlying="NIFTY",
        anchor=Decimal("24400"),
        anchor_date=date(2026, 9, 4),
        today=date(2026, 9, 7),
        strikes=1,
        expiries=2,
    )
    assert selection.expiries == (date(2026, 9, 10), date(2026, 9, 17))


def test_an_underlying_with_no_live_contracts_is_an_error_not_an_empty_list() -> None:
    """Silently recording nothing is the failure mode that costs a day of
    history and is noticed a month later."""
    with pytest.raises(LookupError, match="BANKNIFTY"):
        select_chain(
            _chain(),
            underlying="BANKNIFTY",
            anchor=Decimal("58000"),
            anchor_date=date(2026, 9, 4),
            today=date(2026, 9, 7),
            strikes=1,
            expiries=1,
        )


def test_resolving_several_underlyings_keeps_each_ones_spot_and_drops_repeats() -> None:
    """Two underlyings can share an index key (NIFTY options and a NIFTY
    weekly both anchor on Nifty 50). Subscribing twice to one key wastes a
    slot against the feed's subscription cap for no extra data."""
    from trading.recorder.universe import flatten_keys

    first = select_chain(
        _chain(),
        underlying="NIFTY",
        anchor=Decimal("24400"),
        anchor_date=date(2026, 9, 4),
        today=date(2026, 9, 7),
        strikes=1,
        expiries=1,
    )
    keys = flatten_keys([first, first])
    assert keys[0] == "NSE_INDEX|Nifty 50"
    assert len(keys) == len(set(keys))
    assert len(keys) == 1 + 6


def test_the_token_comes_from_the_environment_or_the_settings_file() -> None:
    """An unattended 09:10 job has no shell to source `.env` in, and the
    failure mode of getting this wrong is a recorder that exits with
    "no token" on a Monday morning nobody is watching.
    """
    from trading.recorder.__main__ import resolve_token

    class _Settings:
        upstox_access_token = None
        upstox_analytics_token = "from-dotenv"

    # The environment wins, so a one-off run can override without editing
    # the file every service on this machine reads.
    assert resolve_token(_Settings(), {"UPSTOX_ACCESS_TOKEN": "from-env"}) == "from-env"
    assert resolve_token(_Settings(), {}) == "from-dotenv"
    assert resolve_token(_Settings(), {"UPSTOX_ANALYTICS_TOKEN": ""}) == "from-dotenv"

    class _Empty:
        upstox_access_token = None
        upstox_analytics_token = None

    assert resolve_token(_Empty(), {}) is None


def test_a_run_started_after_the_close_records_nothing_unless_asked() -> None:
    """A laptop woken at 18:00 fires the missed 09:10 job. Without this the
    recorder would write a five-minute session file for a market that shut
    hours ago -- every evening, forever, each one indistinguishable from a
    real session until you open it."""
    from datetime import UTC, datetime

    from trading.recorder.__main__ import session_close

    during = datetime(2026, 9, 7, 5, 0, tzinfo=UTC)  # 10:30 IST
    assert session_close(during, allow_after_close=False) == datetime(2026, 9, 7, 10, 0, tzinfo=UTC)

    after = datetime(2026, 9, 7, 12, 30, tzinfo=UTC)  # 18:00 IST
    assert session_close(after, allow_after_close=False) is None

    # The manual-test escape hatch stays, but has to be asked for.
    forced = session_close(after, allow_after_close=True)
    assert forced is not None and forced > after
