from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from trading.contracts import ParseError, RawPayload

from .parser_cases import PARSER_CASES, ParserCase


def make_payload(path: Path, source_key: str, business_date: date) -> RawPayload:
    content = path.read_bytes()
    return RawPayload(
        source_key=source_key,
        business_date=business_date,
        content=content,
        content_hash=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime.now(UTC),
        archive_path=path,
    )


def _ids(cases: list[ParserCase]) -> list[str]:
    return [c.name for c in cases]


@pytest.fixture(params=PARSER_CASES, ids=_ids(PARSER_CASES))
def case(request: pytest.FixtureRequest) -> ParserCase:
    return request.param


def test_can_parse_accepts_its_own_fixture(case: ParserCase) -> None:
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    assert case.parser.can_parse(payload) is True


def test_can_parse_rejects_every_other_parsers_fixture(case: ParserCase) -> None:
    """Mutual exclusivity. Without this, dispatch silently picks the wrong parser."""
    for other in PARSER_CASES:
        if other.name == case.name:
            continue
        payload = make_payload(other.fixture, other.source_key, date(2026, 8, 13))
        assert case.parser.can_parse(payload) is False, (
            f"{case.name} claims to parse {other.name}'s fixture"
        )


def test_parse_returns_the_declared_columns(case: ParserCase) -> None:
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    frame = case.parser.parse(payload)
    missing = set(case.required_columns) - set(frame.columns)
    assert not missing, f"{case.name} did not emit {sorted(missing)}"


def test_parse_returns_exactly_the_expected_rows(case: ParserCase) -> None:
    """Exact, not a floor. A floor cannot catch silently dropped rows."""
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    assert case.parser.parse(payload).height == case.exact_rows


def test_parse_maps_columns_correctly_on_a_known_row(case: ParserCase) -> None:
    """Assert real values from a real file.

    Column-name and row-count checks cannot distinguish a correct parser from
    one that returns the right shape full of garbage, or one with open/close
    transposed. This is the only test that reads what is actually in a cell.
    """
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    frame = case.parser.parse(payload)
    row = frame.row(case.golden_row_index, named=True)
    for column, expected in case.golden_row.items():
        assert str(row[column]) == expected, (
            f"{case.name}: column {column!r} was {row[column]!r}, expected {expected!r}"
        )


@pytest.mark.parametrize(
    "garbage",
    [b"\x00\xff\xfe\x01" * 64, b"col_a,col_b\n1,2\n", b"   \n\n  \n"],
    ids=["binary", "foreign-csv", "whitespace"],
)
def test_can_parse_never_raises_on_hostile_input(
    case: ParserCase, garbage: bytes, tmp_path: Path
) -> None:
    """ParserRegistry.select calls can_parse on EVERY registered parser.

    One parser that raises on a foreign payload breaks dispatch for all of
    them, so this property has to be enforced in code, not just documented.
    """
    path = tmp_path / "hostile.bin"
    path.write_bytes(garbage)
    payload = make_payload(path, case.source_key, date(2026, 8, 13))
    assert case.parser.can_parse(payload) is False


def test_parse_is_pure(case: ParserCase) -> None:
    """Same bytes twice must give the same frame. Catches hidden state."""
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    assert case.parser.parse(payload).equals(case.parser.parse(payload))


def test_parse_raises_on_empty_input(case: ParserCase, tmp_path: Path) -> None:
    empty = tmp_path / "empty.bin"
    empty.write_bytes(b"")
    payload = make_payload(empty, case.source_key, date(2026, 8, 13))
    with pytest.raises(ParseError):
        case.parser.parse(payload)


def test_parse_raises_on_truncated_input(case: ParserCase, tmp_path: Path) -> None:
    """A half-downloaded file must fail loudly, never return partial rows."""
    truncated = tmp_path / "truncated.bin"
    truncated.write_bytes(case.fixture.read_bytes()[:40])
    payload = make_payload(truncated, case.source_key, date(2026, 8, 13))
    with pytest.raises(ParseError):
        case.parser.parse(payload)


def test_at_least_one_parser_is_registered() -> None:
    """Guards against the suite silently passing with zero cases."""
    assert PARSER_CASES, "no parser registered a ParserCase"
