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


def test_parse_returns_enough_rows(case: ParserCase) -> None:
    payload = make_payload(case.fixture, case.source_key, date(2026, 8, 13))
    assert case.parser.parse(payload).height >= case.min_rows


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
