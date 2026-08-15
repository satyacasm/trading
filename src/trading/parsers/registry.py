from __future__ import annotations

from trading.contracts import ParseError, Parser, RawPayload


class ParserRegistry:
    """Selects the one parser that owns a payload."""

    def __init__(self, parsers: list[Parser]) -> None:
        self._parsers = parsers

    def select(self, payload: RawPayload) -> Parser:
        matches = [p for p in self._parsers if p.can_parse(payload)]
        if not matches:
            raise ParseError(f"no parser accepts {payload.source_key} {payload.business_date}")
        if len(matches) > 1:
            names = ", ".join(type(p).__name__ for p in matches)
            raise ParseError(f"ambiguous payload; multiple parsers matched: {names}")
        return matches[0]
