from datetime import date
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError
from trading.parsers.udiff import UdiffParser

FIXTURES = Path(__file__).parent.parent / "fixtures" / "udiff"


@pytest.fixture
def parser() -> UdiffParser:
    return UdiffParser()


def test_parses_zipped_nse_cm(parser: UdiffParser) -> None:
    payload = make_payload(FIXTURES / "nse_cm_udiff.zip", "nse_cm_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    assert frame.height == 50
    assert frame["Sgmt"].unique().to_list() == ["CM"]
    assert frame["Src"].unique().to_list() == ["NSE"]


def test_parses_plain_csv_bse_despite_crlf(parser: UdiffParser) -> None:
    """Finding F1: BSE differs from NSE only by line endings."""
    payload = make_payload(FIXTURES / "bse_cm_udiff.csv", "bse_cm_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    assert frame.height == 50
    assert frame["Src"].unique().to_list() == ["BSE"]
    assert not frame.columns[-1].endswith("\r")


def test_untraded_option_rows_are_kept(parser: UdiffParser) -> None:
    """Finding F2: OHLC=0 with a real close is a valid untraded contract."""
    payload = make_payload(FIXTURES / "nse_fo_udiff.zip", "nse_fo_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    untraded = frame.filter((frame["OpnPric"] == "0.00") & (frame["ClsPric"] != "0.00"))
    assert untraded.height > 0, "untraded contracts were dropped"


def test_all_34_columns_are_present(parser: UdiffParser) -> None:
    payload = make_payload(FIXTURES / "nse_fo_udiff.zip", "nse_fo_udiff", date(2026, 8, 13))
    frame = parser.parse(payload)
    assert frame.width == 34
    assert frame.columns[0] == "TradDt"
    assert frame.columns[28] == "NewBrdLotQty"


def test_rejects_a_file_with_a_foreign_header(parser: UdiffParser, tmp_path: Path) -> None:
    bad = tmp_path / "bad.csv"
    bad.write_bytes(b"SYMBOL,SERIES,OPEN\nX,EQ,1\n")
    payload = make_payload(bad, "nse_cm_udiff", date(2026, 8, 13))
    assert parser.can_parse(payload) is False
    with pytest.raises(ParseError):
        parser.parse(payload)
