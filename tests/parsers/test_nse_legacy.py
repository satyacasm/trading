from datetime import date
from pathlib import Path

import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError
from trading.parsers.nse_legacy import NseLegacyCmParser

FIXTURE = Path(__file__).parent.parent / "fixtures" / "nse_legacy" / "cm_legacy.zip"


@pytest.fixture
def parser() -> NseLegacyCmParser:
    return NseLegacyCmParser()


def test_parses_the_legacy_format(parser: NseLegacyCmParser) -> None:
    frame = parser.parse(make_payload(FIXTURE, "nse_cm_legacy", date(2019, 3, 14)))
    assert frame.height == 50
    assert frame["SYMBOL"][0] == "20MICRONS"
    assert frame["TIMESTAMP"][0] == "14-MAR-2019"


def test_trailing_empty_column_is_dropped(parser: NseLegacyCmParser) -> None:
    """Every legacy line ends with a comma; the 14th field is not data."""
    frame = parser.parse(make_payload(FIXTURE, "nse_cm_legacy", date(2019, 3, 14)))
    assert frame.width == 13
    assert frame.columns[-1] == "ISIN"


def test_rejects_a_udiff_file(parser: NseLegacyCmParser, tmp_path: Path) -> None:
    bad = tmp_path / "udiff.csv"
    bad.write_bytes(b"TradDt,BizDt,Sgmt,Src\n2026-08-13,2026-08-13,CM,NSE\n")
    payload = make_payload(bad, "nse_cm_legacy", date(2019, 3, 14))
    assert parser.can_parse(payload) is False
    with pytest.raises(ParseError):
        parser.parse(payload)
