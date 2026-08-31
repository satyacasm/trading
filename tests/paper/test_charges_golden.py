"""Golden tests: our calculator must reproduce the broker's own figures.

This is the only honest validation of an Indian charge stack. If these
fail, the calculator is wrong -- do not adjust the fixtures to match.

The two contract-note tests are marked `golden` and excluded from the
default `uv run pytest` run (see pyproject.toml) because no real broker
figures are recorded yet -- see `tests/paper/fixtures/contract_notes.py`.
Run them explicitly with `uv run pytest -m golden`; until real figures are
filled in, that run fails loudly on the `REPLACE ME` guard below rather
than silently skipping.
"""

from datetime import date
from decimal import Decimal

import pytest

from tests.paper.fixtures.contract_notes import DELIVERY_BUY, DELIVERY_SELL
from trading.paper.charges import compute_charges, load_schedules
from trading.paper.enums import Product, Side


@pytest.mark.golden
@pytest.mark.parametrize(
    "note", [DELIVERY_BUY, DELIVERY_SELL], ids=["delivery_buy", "delivery_sell"]
)
def test_matches_broker_contract_note(db_conn, note) -> None:
    assert note["source"] != "REPLACE ME", (
        "record a real Upstox contract note or calculator output first -- "
        "grading the calculator against our own arithmetic proves nothing"
    )
    schedules = load_schedules(
        db_conn,
        "UPSTOX",
        "NSE",
        "EQUITY",
        Product(note["product"]),
        date(2026, 6, 1),
    )
    got = compute_charges(
        schedules,
        Side(note["side"]),
        note["quantity"],
        note["price"],
    )
    for field, expected in note["expected"].items():
        actual = getattr(got, field)
        assert actual == expected, (
            f"{field}: ours {actual} vs broker {expected} (source: {note['source']})"
        )


def test_total_matches_sum_of_components(db_conn) -> None:
    schedules = load_schedules(
        db_conn, "UPSTOX", "NSE", "EQUITY", Product.DELIVERY, date(2026, 6, 1)
    )
    got = compute_charges(schedules, Side.BUY, Decimal("100"), Decimal("1310.50"))
    assert got.total == sum(
        [
            got.brokerage,
            got.stt,
            got.exchange_txn,
            got.sebi_fee,
            got.stamp_duty,
            got.ipft,
            got.gst,
            got.dp_charges,
        ],
        Decimal("0"),
    )
