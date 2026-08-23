import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError, RawPayload
from trading.parsers.amfi_history import AmfiNavHistoryParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "amfi" / "navhistory.txt"


def _synthetic_payload(content: str, tmp_path: Path) -> RawPayload:
    """Build a RawPayload from in-memory content, not a fixture file.

    Used for cases (e.g. a whitespace-only ISIN field) that the committed
    fixtures don't happen to contain and which must not be edited to add.
    """
    raw = content.encode("utf-8")
    return RawPayload(
        source_key="amfi_nav_history",
        business_date=date(2019, 3, 14),
        content=raw,
        content_hash=hashlib.sha256(raw).hexdigest(),
        fetched_at=datetime.now(UTC),
        archive_path=tmp_path / "synthetic_navhistory.txt",
    )


@pytest.fixture
def parser() -> AmfiNavHistoryParser:
    return AmfiNavHistoryParser()


def test_extracts_only_data_rows(parser: AmfiNavHistoryParser) -> None:
    """Section headers and AMC names must not become rows."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav_history", date(2019, 3, 14)))
    assert frame.height > 0
    assert frame["scheme_code"].str.contains(r"^\d+$").all()


def test_missing_isin_empty_field_becomes_null(parser: AmfiNavHistoryParser) -> None:
    """The historical format encodes an absent ISIN as an empty field (never a
    literal '-'). Both isin_reinvest and isin_growth must normalise to null,
    with exact counts derived from the fixture (Finding 1 / Ruling P1)."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav_history", date(2019, 3, 14)))
    assert frame["isin_reinvest"].null_count() == 30
    assert frame["isin_growth"].null_count() == 12
    assert not frame["isin_reinvest"].str.contains(r"^$").any()
    assert not frame["isin_growth"].str.contains(r"^$").any()


def test_missing_isin_whitespace_becomes_null(parser: AmfiNavHistoryParser, tmp_path: Path) -> None:
    """A whitespace-only ISIN field (post-strip, empty) must normalise to
    null, the same as an already-empty field (Ruling P1)."""
    content = (
        "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;"
        "ISIN Div Reinvestment;Net Asset Value;Repurchase Price;"
        "Sale Price;Date\n"
        "Open Ended Schemes ( Growth )\n"
        "Some Mutual Fund\n"
        "120373;Some Scheme - Growth;INF515L01AJ6;   ;74.2258;;;14-Mar-2019\n"
    )
    frame = parser.parse(_synthetic_payload(content, tmp_path))
    assert frame.height == 1
    assert frame["isin_reinvest"][0] is None


def test_rejects_a_csv_file(parser: AmfiNavHistoryParser, tmp_path: Path) -> None:
    bad = tmp_path / "x.csv"
    bad.write_bytes(b"TradDt,BizDt,Sgmt\n2026-08-13,2026-08-13,CM\n")
    payload = make_payload(bad, "amfi_nav_history", date(2019, 3, 14))
    assert parser.can_parse(payload) is False
    with pytest.raises(ParseError):
        parser.parse(payload)


# ---------------------------------------------------------------------------
# AMFI reordered this report's columns (F1, task-17-report.md). Both layouts
# carry 8 fields, so positional unpacking parsed the new one without error
# while writing the Plan value into the ISIN column and the ISIN into the NAV
# column. Values are therefore read by column NAME, and both layouts are
# supported -- the archived bytes are the source of truth for
# check_idempotency, and old-format files may already sit in data/raw.
# ---------------------------------------------------------------------------

FIXTURE_V2 = Path(__file__).parent.parent / "fixtures" / "amfi" / "navhistory_v2.txt"

CURRENT_HEADER = (
    "Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;"
    "ISIN Div Reinvestment;Net Asset Value;Date"
)


def test_can_parse_accepts_the_current_header(parser: AmfiNavHistoryParser) -> None:
    assert parser.can_parse(make_payload(FIXTURE_V2, "amfi_nav_history", date(2019, 3, 14)))


def test_parses_the_current_column_layout(parser: AmfiNavHistoryParser) -> None:
    frame = parser.parse(make_payload(FIXTURE_V2, "amfi_nav_history", date(2019, 3, 14)))
    assert frame.height == 64
    assert frame["scheme_code"].str.contains(r"^\d+$").all()


def test_both_layouts_parse_to_identical_frames(parser: AmfiNavHistoryParser) -> None:
    """The two fixtures are the same 64 schemes on the same date, captured in
    AMFI's old and current column orders. Anything but equality means the
    reorder is leaking into the parsed values."""
    old = parser.parse(make_payload(FIXTURE, "amfi_nav_history", date(2019, 3, 14)))
    new = parser.parse(make_payload(FIXTURE_V2, "amfi_nav_history", date(2019, 3, 14)))
    assert old.equals(new)


def test_current_layout_does_not_shift_isin_and_nav(parser: AmfiNavHistoryParser) -> None:
    """The exact corruption positional unpacking would cause: 'Plan' landing
    in isin_growth and the ISIN landing in nav, with no error raised."""
    frame = parser.parse(make_payload(FIXTURE_V2, "amfi_nav_history", date(2019, 3, 14)))
    isins = frame["isin_growth"].drop_nulls()
    assert isins.len() > 0
    assert isins.str.contains(r"^INF[0-9A-Z]+$").all()
    assert frame["nav"].str.contains(r"^\d+(\.\d+)?$").all()
    assert frame["nav_date"].str.contains(r"^\d{2}-[A-Z][a-z]{2}-\d{4}$").all()


def test_a_header_missing_a_required_column_is_rejected(
    parser: AmfiNavHistoryParser, tmp_path: Path
) -> None:
    """A future reorder that DROPS a column we depend on must fail loudly
    rather than silently yielding nulls."""
    content = (
        "Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;"
        "ISIN Div Reinvestment;Date\n"
        "\nOpen Ended Schemes ( Growth )\n\nSahara Mutual Fund\n"
        "120373;SOME FUND;;;INF515L01AJ6;;14-Mar-2019\n"
    )
    with pytest.raises(ParseError, match="Net Asset Value"):
        parser.parse(_synthetic_payload(content, tmp_path))


def test_dash_isin_becomes_null_in_the_current_layout(
    parser: AmfiNavHistoryParser, tmp_path: Path
) -> None:
    """AMFI encodes an absent ISIN as a literal '-' as well as an empty field,
    in BOTH columns. Rows copied verbatim from a live 2019-03-14 fetch
    (portal.amfiindia.com, lines 2243 and 12144) -- the committed fixtures
    happen to contain only the empty-field spelling, so this case would
    otherwise go untested and a literal '-' would reach the ISIN column.
    """
    content = (
        f"{CURRENT_HEADER}\n"
        "\nOpen Ended Schemes ( Growth )\n\nICICI Prudential Mutual Fund\n"
        "145399;ICICI Prudential Ultra Short Term Fund - Daily IDCW;Regular Plan;"
        "Daily IDCW;-;INF109KC1ND7;10.0014;14-Mar-2019\n"
        "118804;Nippon India Annual Interval Fund - Series I;;;INF204K01B81;-;"
        "18.9501;14-Mar-2019\n"
    )
    frame = parser.parse(_synthetic_payload(content, tmp_path))
    assert frame["isin_growth"].to_list() == [None, "INF204K01B81"]
    assert frame["isin_reinvest"].to_list() == ["INF109KC1ND7", None]
