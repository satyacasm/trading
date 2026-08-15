"""Registry of parser conformance cases.

Each parser task appends exactly one ParserCase here. The contract suite
then runs every case against every parser, which is what enforces
mutual exclusivity of can_parse().
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading.contracts import Parser
from trading.parsers.nse_legacy import NseLegacyCmParser
from trading.parsers.udiff import UdiffParser

FIXTURE_ROOT = Path(__file__).parent.parent / "fixtures"


@dataclass(frozen=True)
class ParserCase:
    name: str
    parser: Parser
    fixture: Path  # the file this parser owns
    source_key: str
    exact_rows: int  # NOT a floor — silently dropped rows must fail
    required_columns: tuple[str, ...]
    golden_row_index: int  # a row whose values are asserted verbatim
    golden_row: dict[str, str]  # column -> expected str(value); catches column swaps


PARSER_CASES: list[ParserCase] = []

PARSER_CASES.append(
    ParserCase(
        name="udiff",
        parser=UdiffParser(),
        fixture=FIXTURE_ROOT / "udiff" / "nse_fo_udiff.zip",
        source_key="nse_fo_udiff",
        exact_rows=50,
        required_columns=("TradDt", "Sgmt", "FinInstrmTp", "ClsPric", "NewBrdLotQty"),
        golden_row_index=0,
        # Real values read off the live 2026-08-13 NSE F&O file. UndrlygPric and
        # NewBrdLotQty are included deliberately: they are the two fields finding F3
        # depends on, and a column-shift would corrupt them silently.
        golden_row={
            "TradDt": "2026-08-13",
            "Sgmt": "FO",
            "Src": "NSE",
            "FinInstrmTp": "STO",
            "TckrSymb": "ABCAPITAL",
            "XpryDt": "2026-10-27",
            "StrkPric": "430.00",
            "OptnTp": "CE",
            "ClsPric": "19.45",
            "NewBrdLotQty": "3100",
            "UndrlygPric": "407.70",
        },
    )
)

PARSER_CASES.append(
    ParserCase(
        name="nse_legacy",
        parser=NseLegacyCmParser(),
        fixture=FIXTURE_ROOT / "nse_legacy" / "cm_legacy.zip",
        source_key="nse_cm_legacy",
        exact_rows=50,
        required_columns=("SYMBOL", "SERIES", "CLOSE", "TIMESTAMP", "ISIN"),
        golden_row_index=0,
        # Real values from the live 2019-03-14 file. OPEN/HIGH/LOW/CLOSE are all
        # asserted because a transposition among them is the classic legacy-parser
        # bug and no other test in the suite would see it.
        golden_row={
            "SYMBOL": "20MICRONS",
            "SERIES": "EQ",
            "OPEN": "39.5",
            "HIGH": "40",
            "LOW": "38.5",
            "CLOSE": "38.95",
            "TIMESTAMP": "14-MAR-2019",
            "ISIN": "INE144J01027",
        },
    )
)
