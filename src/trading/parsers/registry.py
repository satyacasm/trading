from __future__ import annotations

from trading.contracts import ParseError, Parser, RawPayload


class ParserRegistry:
    """Selects the one parser that owns a payload."""

    def __init__(self, parsers: list[Parser]) -> None:
        self._parsers = parsers

    def select(self, payload: RawPayload) -> Parser:
        matches: list[Parser] = []
        for p in self._parsers:
            try:
                owns_payload = p.can_parse(payload)
            except Exception:
                # can_parse is contractually required never to raise (enforced
                # by the parser conformance suite for every registered
                # parser), but a defective parser must not be allowed to take
                # dispatch down for every parser that follows it in the list.
                # Treat a raising can_parse as "does not own this payload".
                continue
            if owns_payload:
                matches.append(p)
        if not matches:
            raise ParseError(f"no parser accepts {payload.source_key} {payload.business_date}")
        if len(matches) > 1:
            names = ", ".join(type(p).__name__ for p in matches)
            raise ParseError(f"ambiguous payload; multiple parsers matched: {names}")
        return matches[0]
