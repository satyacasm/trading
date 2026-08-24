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
        # A buy-back is a tender offer -- no ex-date price adjustment.
        "Buy Back",
        # Per UNIT, not per share: an InvIT/REIT distribution, not a dividend
        # on an equity share.
        "Distribution - Rs 1.1482 Per Unit As Interest",
        " Interest Payment",
        "Annual General Meeting",
    ],
)
def test_subjects_that_are_not_price_events(subject: str):
    assert _classify(subject) == []


@pytest.mark.parametrize(
    ("subject", "expected_type"),
    [
        ("Rights 2:5 @ Premium Rs 1.17/-", "RIGHTS"),
        ("Scheme Of Arrangement", "DEMERGER"),
        ("Scheme Of Arrangement - Bonus Ncrps 4:1", "DEMERGER"),
    ],
)
def test_real_price_events_are_recorded_never_read_as_a_share_ratio(
    subject: str, expected_type: str
):
    """These genuinely move the price, so dropping them leaves the move
    unexplained. They are recorded as their own type -- and critically, never
    as a BONUS or SPLIT, because `adjust.py` applies only those two and would
    reprice a decade of history from a ratio this feed never gave us.

    "Scheme Of Arrangement - Bonus Ncrps 4:1" is the case that matters: a
    bonus of preference shares leaves the equity share count untouched."""
    actions = _classify(subject)
    assert [a[0] for a in actions] == [expected_type]
    assert all(a[0] not in ("BONUS", "SPLIT") for a in actions)


# ---------------------------------------------------------------------------
# Second widening. Categorising all 11,644 still-unrecognised subjects showed
# most were not exotic events at all -- they were spellings of things already
# parsed. 1,366 dividends differ only by "/-" after the amount, a missing
# dash, or a "(Purpose Revised)" suffix; 6,932 "Annual General Meeting" rows
# genuinely carry no price event, but 120+ of them carry a real dividend
# after a slash; and a dozen more splits hide behind "Fv Splt Frm".
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("subject", "amount"),
    [
        ("Dividend - Re 1/- Per Share", Decimal("1")),
        ("Interim Dividend Rs 5/- Per Share", Decimal("5")),
        ("Interim Dividend - Rs 2/- Per Share (Purpose Revised)", Decimal("2")),
        ("Interim Dividend Re 1/- Per Share (Purpose Revised)", Decimal("1")),
        ("Interim Div - Rs 6/- Per Share", Decimal("6")),
    ],
)
def test_dividend_spellings_with_slash_dash_and_suffixes(subject: str, amount: Decimal):
    action, _f, _t, got = _only(subject)
    assert (action, got) == ("DIVIDEND", amount)


@pytest.mark.parametrize(
    "subject",
    ["Fv Splt Frm Rs 10 To Rs 2", "Fv Splt Frm Rs 10 To Re 1"],
)
def test_abbreviated_split_spelling_is_recognised(subject: str):
    action, ratio_from, _to, _a = _only(subject)
    assert action == "SPLIT"
    assert ratio_from == Decimal(1)


def test_a_meeting_carrying_a_dividend_yields_the_dividend():
    """6,932 subjects are a bare meeting notice with no price event, but the
    slash form hides a real dividend behind one."""
    assert _only("Annual General Meeting/Dividend - Re 1/- Per Share") == (
        "DIVIDEND",
        None,
        None,
        Decimal("1"),
    )


def test_a_plus_also_separates_two_actions():
    actions = _classify(
        "Interim Div - Rs 6/- Per Share + Face Value Split (Sub-Division) - "
        "From Rs 10/- Per Share To Rs 2/- Per Share"
    )
    assert ("DIVIDEND", None, None, Decimal("6")) in actions
    assert ("SPLIT", Decimal(1), Decimal(5), None) in actions


def test_rights_are_recorded_with_their_ratio():
    """270 rights issues dilute the share count, so the price genuinely moves.
    The ratio is in the subject, so the event is recorded rather than dropped
    -- the adjuster only applies SPLIT and BONUS, so this informs the
    continuity check without silently repricing anything."""
    action, ratio_from, ratio_to, _a = _only("Rights 2:5 @ Premium Rs 1.17/-")
    assert (action, ratio_from, ratio_to) == ("RIGHTS", Decimal(5), Decimal(7))


def test_a_demerger_is_recorded_even_though_its_ratio_is_unknowable():
    """132 demergers carve real value out of the share price, but this feed
    gives no ratio at all -- only the date. Recording the event explains the
    move; inventing a factor would silently reprice a decade."""
    action, ratio_from, ratio_to, amount = _only("Scheme Of Arrangement")
    assert action == "DEMERGER"
    assert (ratio_from, ratio_to, amount) == (None, None, None)


def test_a_bare_meeting_is_still_not_an_action():
    assert _classify("Annual General Meeting") == []
    assert _classify("Extra Ordinary General Meeting") == []


# ---------------------------------------------------------------------------
# Third pass over the residue. After the second widening 9,227 subjects were
# still unrecognised, but 8,637 of those are meetings, bond interest payments
# and buy-backs -- none of which move an equity price on the ex-date. Of the
# 590 that remained, these are the ones that genuinely do.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "subject",
    ["Scheme Of Demerger", "Composite Scheme Of Arrangement", "Scheme of Arrangement"],
)
def test_more_demerger_spellings(subject: str):
    assert [a[0] for a in _classify(subject)] == ["DEMERGER"]


def test_capital_reduction_is_a_price_event():
    """Cancelling shares against accumulated losses changes the share count,
    so the price moves. No ratio in the subject, so the event is recorded
    without one, exactly like a demerger."""
    action, ratio_from, ratio_to, amount = _only("Capital Reduction")
    assert action == "CAPITAL_REDUCTION"
    assert (ratio_from, ratio_to, amount) == (None, None, None)


@pytest.mark.parametrize(
    ("subject", "amount"),
    [
        ("Dividend Re.0.50 Per Share", Decimal("0.50")),
        ("Int Dividend - Rs 0.75 Per Share", Decimal("0.75")),
    ],
)
def test_more_dividend_spellings(subject: str, amount: Decimal):
    action, _f, _t, got = _only(subject)
    assert (action, got) == ("DIVIDEND", amount)


def test_an_ampersand_also_separates_two_actions():
    actions = _classify("Dividend - Rs 8.35 Per Share & Special Dividend - Rs 3.35 Per Share")
    assert sorted(a[3] for a in actions) == [Decimal("3.35"), Decimal("8.35")]


@pytest.mark.parametrize("subject", ["Interim Dividend", "Dividend"])
def test_a_dividend_with_no_amount_stays_unparsed(subject: str):
    """The feed gives no number at all. Recording a dividend of zero, or
    guessing one, would both be worse than leaving it out -- and it is
    reported in the run summary rather than dropped in silence."""
    assert _classify(subject) == []


def test_a_per_unit_distribution_is_not_an_equity_dividend():
    """InvIT and REIT distributions are per UNIT and mix interest, dividend
    and return of capital. Treating them as a share dividend would be wrong
    on both the instrument and the amount."""
    assert _classify("Distribution - Rs 3 Per Unit") == []
