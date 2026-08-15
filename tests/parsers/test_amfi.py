from datetime import date
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError
from trading.parsers.amfi import AmfiNavParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "amfi" / "navall.txt"


@pytest.fixture
def parser() -> AmfiNavParser:
    return AmfiNavParser()


def test_extracts_only_data_rows(parser: AmfiNavParser) -> None:
    """Section headers and AMC names must not become rows."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert frame.height > 0
    assert frame["scheme_code"].str.contains(r"^\d+$").all()


def test_scheme_type_and_amc_are_carried_down(parser: AmfiNavParser) -> None:
    """Every data row inherits the section it appeared under."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert frame["amc_name"].null_count() == 0
    assert frame["scheme_type"].null_count() == 0
    assert frame["scheme_type"].str.starts_with("Open Ended").any()


def test_missing_isin_dash_becomes_null(parser: AmfiNavParser) -> None:
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert not frame["isin_reinvest"].str.contains(r"^-$").any()


def test_date_column_is_preserved_verbatim(parser: AmfiNavParser) -> None:
    """DD-Mon-YYYY; conversion is the normalizer's job, not the parser's."""
    frame = parser.parse(make_payload(FIXTURE, "amfi_nav", date(2026, 8, 13)))
    assert frame["nav_date"].str.contains(r"^\d{2}-[A-Za-z]{3}-\d{4}$").all()


def test_rejects_a_csv_file(parser: AmfiNavParser, tmp_path: Path) -> None:
    bad = tmp_path / "x.csv"
    bad.write_bytes(b"TradDt,BizDt,Sgmt\n2026-08-13,2026-08-13,CM\n")
    payload = make_payload(bad, "amfi_nav", date(2026, 8, 13))
    assert parser.can_parse(payload) is False
    with pytest.raises(ParseError):
        parser.parse(payload)
