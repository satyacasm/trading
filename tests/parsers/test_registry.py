from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from tests.contracts.test_parser_contract import make_payload
from trading.contracts import ParseError, RawPayload
from trading.parsers.registry import ParserRegistry


class _NeverOwns:
    """A stub parser that never claims a payload."""

    def can_parse(self, payload: RawPayload) -> bool:
        return False

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        raise ParseError("never should have been asked to parse")


class _AlwaysOwnsA:
    """A stub parser that always claims a payload."""

    def can_parse(self, payload: RawPayload) -> bool:
        return True

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        return pl.DataFrame({"parser": ["a"]})


class _AlwaysOwnsB:
    """A second, distinctly-named stub parser that always claims a payload."""

    def can_parse(self, payload: RawPayload) -> bool:
        return True

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        return pl.DataFrame({"parser": ["b"]})


class _RaisesInCanParse:
    """A defective stub parser: can_parse violates its contract and raises."""

    def can_parse(self, payload: RawPayload) -> bool:
        raise RuntimeError("this parser is broken")

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        raise ParseError("never should have been asked to parse")


@pytest.fixture
def payload(tmp_path: Path) -> RawPayload:
    path = tmp_path / "whatever.bin"
    path.write_bytes(b"irrelevant bytes")
    return make_payload(path, "some_source", date(2026, 8, 13))


def test_select_raises_when_no_parser_matches(payload: RawPayload) -> None:
    registry = ParserRegistry([_NeverOwns(), _NeverOwns()])
    with pytest.raises(ParseError) as exc_info:
        registry.select(payload)
    message = str(exc_info.value)
    assert payload.source_key in message
    assert str(payload.business_date) in message


def test_select_returns_the_single_matching_parser(payload: RawPayload) -> None:
    winner = _AlwaysOwnsA()
    registry = ParserRegistry([_NeverOwns(), winner, _NeverOwns()])
    assert registry.select(payload) is winner


def test_select_raises_naming_the_colliding_parsers_on_ambiguous_match(
    payload: RawPayload,
) -> None:
    registry = ParserRegistry([_AlwaysOwnsA(), _AlwaysOwnsB()])
    with pytest.raises(ParseError) as exc_info:
        registry.select(payload)
    message = str(exc_info.value)
    assert "_AlwaysOwnsA" in message
    assert "_AlwaysOwnsB" in message


def test_select_is_not_taken_down_by_a_parser_that_raises_in_can_parse(
    payload: RawPayload,
) -> None:
    """One misbehaving parser must not break dispatch for the others.

    can_parse is contractually required never to raise (enforced by the
    parser conformance suite for every registered parser), but ParserRegistry
    additionally defends against a defective parser: a can_parse that raises
    is treated as "does not own this payload" rather than propagating and
    aborting evaluation of every parser that follows it in the list.
    """
    winner = _AlwaysOwnsA()
    registry = ParserRegistry([_RaisesInCanParse(), winner])
    assert registry.select(payload) is winner
