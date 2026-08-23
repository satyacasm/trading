"""Subject-string recognition, widened against the real archive.

Task 16 deliberately recognised only the patterns present in one live sample,
and was right to: a guessed ratio fails silently. That sample was narrow.
Backfilling a decade produced 23,782 real subjects, and measuring against
them found 21 bonuses, 36 splits and 2,021 dividends going unrecognised on
formatting alone. Every string in this file is copied verbatim from that
archive.

A missed SPLIT is the expensive case: the price halves, nothing explains it,
and any backtest crossing that date reads a routine sub-division as a 50%
loss.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.corpactions.ingest import _classify


def _only(subject: str) -> tuple[str | None, Decimal | None, Decimal | None, Decimal | None]:
    actions = _classify(subject)
    assert len(actions) == 1, f"expected exactly one action, got {actions}"
    return actions[0]


@pytest.mark.parametrize(
    "subject",
    [
        "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share",
        "Face Value Split From Rs 10/- Per Share To Rs 2/- Per Share",
        "Face Value Split From Rs 10 To Rs 2",
        "Face Value Split (Sub-Division) - From Rs10/- Per Share To Rs 2/- Per Share",
    ],
)
def test_split_spellings_all_reduce_to_one_for_five(subject: str):
    """Four ways NSE writes the same 1:5 sub-division."""
    assert _only(subject) == ("SPLIT", Decimal(1), Decimal(5), None)


def test_split_from_rupees_two_to_rupee_one_is_one_for_two():
    assert _only("Face Value Split From Rs 2 To Re 1") == ("SPLIT", Decimal(1), Decimal(2), None)


@pytest.mark.parametrize(
    ("subject", "amount"),
    [
        ("Dividend - Rs 2 Per Share", Decimal("2")),
        ("Interim Dividend - Rs 2.55 Per Share", Decimal("2.55")),
        ("Dividend - Re 0.70  Per Share", Decimal("0.70")),
        ("Dividend - Re  0.50 Per Share", Decimal("0.50")),
        ("Dividend - Rs 6 Per Sh", Decimal("6")),
        ("Final Dividend - Rs 3 Per Share", Decimal("3")),
    ],
)
def test_dividend_spellings(subject: str, amount: Decimal):
    action, _from, _to, got = _only(subject)
    assert (action, got) == ("DIVIDEND", amount)


def test_a_combined_bonus_and_split_yields_both_actions():
    """NSE packs two real corporate actions into one subject with a slash.
    Recognising only the first would leave the other silently unapplied --
    and this shape is the largest price move in the whole feed."""
    actions = _classify(
        "Bonus 1:1/Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 5/- Per Share"
    )
    assert ("BONUS", Decimal(1), Decimal(2), None) in actions
    assert ("SPLIT", Decimal(1), Decimal(2), None) in actions
    assert len(actions) == 2


def test_plain_bonus_still_works():
    assert _only("Bonus 1:2") == ("BONUS", Decimal(2), Decimal(3), None)


@pytest.mark.parametrize(
    "subject",
    [
        "Buy Back",
        "Rights 2:5 @ Premium Rs 1.17/-",
        "Distribution - Rs 1.1482 Per Unit As Interest",
        # A bonus of preference shares (NCRPS) is NOT an equity bonus: the
        # equity share count does not change, so applying it would corrupt
        # every price before that date.
        "Scheme Of Arrangement - Bonus Ncrps 4:1",
        "Scheme Of Arrangement",
        " Interest Payment",
    ],
)
def test_subjects_that_must_never_be_guessed_at(subject: str):
    assert _classify(subject) == []
