from __future__ import annotations

import io

import polars as pl

from trading.contracts import ParseError, RawPayload
from trading.parsers.udiff import _extract_csv, _header_of

LEGACY_COLUMNS: tuple[str, ...] = (
    "SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE",
    "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN",
)


class NseLegacyCmParser:
    """Parses NSE cash bhavcopy from before the UDiFF migration."""

    def can_parse(self, payload: RawPayload) -> bool:
        try:
            header = _header_of(_extract_csv(payload.content))
        except ParseError:
            return False
        # A trailing comma yields a final empty field; tolerate it.
        trimmed = header[:-1] if header and header[-1] == "" else header
        return trimmed == LEGACY_COLUMNS

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        if not self.can_parse(payload):
            raise ParseError("header is not NSE legacy CM")
        csv_bytes = _extract_csv(payload.content)
        try:
            frame = pl.read_csv(
                io.BytesIO(csv_bytes.replace(b"\r\n", b"\n")),
                schema_overrides={c: pl.String for c in LEGACY_COLUMNS},
                has_header=True,
                truncate_ragged_lines=True,
            )
        except Exception as exc:
            raise ParseError(f"unreadable legacy csv: {exc}") from exc
        frame = frame.select([c for c in frame.columns if c in LEGACY_COLUMNS])
        if frame.height == 0:
            raise ParseError("legacy file has a header but no rows")
        return frame
