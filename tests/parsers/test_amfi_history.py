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
