from __future__ import annotations

import polars as pl

from trading.contracts import ParseError, RawPayload

EXPECTED_HEADER = "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment"
SCHEME_TYPE_PREFIXES = ("Open Ended", "Close Ended", "Interval Fund")
FIELD_COUNT = 8  # seven semicolons


class AmfiNavHistoryParser:
    """Scans AMFI's historical NAV report (DownloadNAVHistoryReport_Po.aspx).

    Same hierarchical, semicolon-delimited shape as the latest NAVAll.txt
    format (blank lines, scheme-type headers, AMC names interleaved with
    data rows), but with 8 fields instead of 6 and Scheme Name in the 2nd
    position instead of the 4th. The header text differs from the latest
    format by exactly one space (`ISIN Div Payout/ISIN Growth` vs
    `ISIN Div Payout/ ISIN Growth`), which is the can_parse discriminator.
    """

    def can_parse(self, payload: RawPayload) -> bool:
        head = payload.content[:200].decode("utf-8", errors="replace")
        return head.startswith(EXPECTED_HEADER)

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        if not self.can_parse(payload):
            raise ParseError("not an AMFI NAV history file")

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
            (
                code,
                name,
                isin_g,
                isin_r,
                nav,
                _repurchase_price,
                _sale_price,
                nav_date,
            ) = (f.strip() for f in fields)
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
            raise ParseError("AMFI history file contained no data rows")

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
