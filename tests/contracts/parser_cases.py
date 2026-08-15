"""Registry of parser conformance cases.

Each parser task appends exactly one ParserCase here. The contract suite
then runs every case against every parser, which is what enforces
mutual exclusivity of can_parse().
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading.contracts import Parser
from trading.parsers.amfi import AmfiNavParser
from trading.parsers.amfi_history import AmfiNavHistoryParser
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

PARSER_CASES.append(
    ParserCase(
        name="amfi",
        parser=AmfiNavParser(),
        fixture=FIXTURE_ROOT / "amfi" / "navall.txt",
        source_key="amfi_nav",
        # Set this to the exact data-row count your trimmed fixture produces.
        exact_rows=40,
        required_columns=("scheme_code", "nav", "nav_date", "amc_name", "scheme_type"),
        golden_row_index=0,
        # Real values from the live file. amc_name is asserted because it is
        # *carried down* from a section header rather than read off the row —
        # if the stateful scan is wrong, this is the field that shows it.
        golden_row={
            "scheme_code": "119551",
            "isin_growth": "INF209KA12Z1",
            "scheme_name": ("Aditya Birla Sun Life Banking & PSU Debt Fund  - DIRECT - IDCW"),
            "nav": "107.2564",
            "nav_date": "13-Aug-2026",
            "amc_name": "Aditya Birla Sun Life Mutual Fund",
        },
    )
)

PARSER_CASES.append(
    ParserCase(
        name="amfi_history",
        parser=AmfiNavHistoryParser(),
        fixture=FIXTURE_ROOT / "amfi" / "navhistory.txt",
        source_key="amfi_nav_history",
        exact_rows=64,
        required_columns=("scheme_code", "nav", "nav_date", "amc_name", "scheme_type"),
        golden_row_index=0,
        # Real values from the live 2019-03-14 historical report. scheme_name is
        # asserted because it moves from 4th to 2nd position between the two AMFI
        # formats — a parser that reuses the latest-format offsets puts the ISIN
        # here, and this is the assertion that catches it.
        golden_row={
            "scheme_code": "120373",
            "scheme_name": "SAHARA BANKING & FINANCIAL SERVICES FUND- GROWTH - Direct",
            "isin_growth": "INF515L01AJ6",
            "nav": "74.2258",
            "nav_date": "14-Mar-2019",
            "scheme_type": "Open Ended Schemes ( Growth )",
            "amc_name": "Sahara Mutual Fund",
        },
    )
)
