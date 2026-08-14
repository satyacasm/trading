"""Registry of parser conformance cases.

Each parser task appends exactly one ParserCase here. The contract suite
then runs every case against every parser, which is what enforces
mutual exclusivity of can_parse().
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from trading.contracts import Parser

FIXTURE_ROOT = Path(__file__).parent.parent / "fixtures"


@dataclass(frozen=True)
class ParserCase:
    name: str
    parser: Parser
    fixture: Path  # the file this parser owns
    source_key: str
    min_rows: int  # sanity floor for the trimmed fixture
    required_columns: tuple[str, ...]


PARSER_CASES: list[ParserCase] = []
