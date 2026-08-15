from __future__ import annotations

import polars as pl

from trading.contracts import ParseError, RawPayload

EXPECTED_HEADER = "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment"
SCHEME_TYPE_PREFIXES = ("Open Ended", "Close Ended", "Interval Fund")
FIELD_COUNT = 6  # five semicolons


class AmfiNavParser:
    """Scans AMFI's hierarchical NAV file.

    The file interleaves data rows with blank lines, scheme-type headers and
    AMC names, so it cannot be read as a CSV (finding F4).
    """

    def can_parse(self, payload: RawPayload) -> bool:
        head = payload.content[:200].decode("utf-8", errors="replace")
        return head.startswith(EXPECTED_HEADER)

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        if not self.can_parse(payload):
            raise ParseError("not an AMFI NAVAll file")

        text = payload.content.decode("utf-8", errors="replace")
        rows: list[dict[str, str | None]] = []
        scheme_type: str | None = None
        amc_name: str | None = None

        for raw_line in text.splitlines()[1:]:  # skip the header
            line = raw_line.strip()
            if not line:
                continue
            if ";" not in line:
                if line.startswith(SCHEME_TYPE_PREFIXES):
                    scheme_type = line
                else:
                    amc_name = line
                continue
            fields = line.split(";")
            if len(fields) != FIELD_COUNT:
                continue  # defensive: unexpected shape is not data
            code, isin_g, isin_r, name, nav, nav_date = (f.strip() for f in fields)
            rows.append(
                {
                    "scheme_code": code,
                    "isin_growth": None if isin_g == "-" else isin_g,
                    "isin_reinvest": None if isin_r == "-" else isin_r,
                    "scheme_name": name,
                    "nav": nav,
                    "nav_date": nav_date,
                    "scheme_type": scheme_type,
                    "amc_name": amc_name,
                }
            )

        if not rows:
            raise ParseError("AMFI file contained no data rows")

        return pl.DataFrame(
            rows,
            schema={
                "scheme_code": pl.String(),
                "isin_growth": pl.String(),
                "isin_reinvest": pl.String(),
                "scheme_name": pl.String(),
                "nav": pl.String(),
                "nav_date": pl.String(),
                "scheme_type": pl.String(),
                "amc_name": pl.String(),
            },
        )
