from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from trading.contracts import ParseError, RawPayload

SCHEME_TYPE_PREFIXES = ("Open Ended", "Close Ended", "Interval Fund")

# The header cell that discriminates this report from NAVAll.txt (the latest
# snapshot), whose otherwise-similar header spells the same column with an
# extra space: "ISIN Div Payout/ ISIN Growth". Matching on cells rather than
# a prefix keeps the two parsers mutually exclusive regardless of how many
# columns either format grows.
ISIN_GROWTH_COLUMN = "ISIN Div Payout/ISIN Growth"

# Output field -> the header names AMFI has used for it. Values are located by
# NAME, never by position: AMFI reordered this report in August 2026 (F1,
# task-17-report.md) from
#   Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;
#   Net Asset Value;Repurchase Price;Sale Price;Date
# to
#   Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;
#   ISIN Div Reinvestment;Net Asset Value;Date
# Both carry exactly 8 fields, so the previous positional unpacking parsed the
# new layout WITHOUT error while writing the Plan value into the ISIN column
# and the ISIN into the NAV column. A name-based map cannot shift silently,
# and it keeps both layouts readable -- which matters because archived raw
# bytes are what check_idempotency re-parses, and old-format files may already
# sit under data/raw/amfi_nav_history/.
#
# Columns present in one layout but not the other -- Repurchase Price and Sale
# Price (old), Plan and Option (current) -- are deliberately not mapped. The
# canonical schema has no slot for prices we do not use, and AMFI's scheme_code
# already distinguishes Direct from Regular and Growth from Dividend, so Plan
# and Option carry no identity we don't already hold.
COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "scheme_code": ("Scheme Code",),
    "scheme_name": ("Scheme Name", "NAV Name"),
    "isin_growth": (ISIN_GROWTH_COLUMN,),
    "isin_reinvest": ("ISIN Div Reinvestment",),
    "nav": ("Net Asset Value",),
    "nav_date": ("Date",),
}


def _header_cells(content: bytes) -> list[str]:
    head = content[:400].decode("utf-8", errors="replace").splitlines()
    return [c.strip() for c in head[0].split(";")] if head else []


def _column_index(cells: Sequence[str]) -> dict[str, int]:
    """Map each output field to its column index, or raise naming what is
    missing. Silence here would mean a whole column of nulls."""
    index: dict[str, int] = {}
    for field, aliases in COLUMN_ALIASES.items():
        position = next((cells.index(a) for a in aliases if a in cells), None)
        if position is None:
            raise ParseError(
                f"AMFI NAV history header is missing a required column: "
                f"{' / '.join(aliases)} (header was {';'.join(cells)!r})"
            )
        index[field] = position
    return index


class AmfiNavHistoryParser:
    """Scans AMFI's historical NAV report (DownloadNAVHistoryReport_Po.aspx).

    Hierarchical, semicolon-delimited: blank lines and scheme-type headers and
    AMC names interleaved with data rows. Data rows are located by column name
    against the file's own header, so both of AMFI's published column orders
    parse to identical output and an unrecognised reorder fails loudly.
    """

    def can_parse(self, payload: RawPayload) -> bool:
        cells = _header_cells(payload.content)
        return bool(cells) and cells[0] == "Scheme Code" and ISIN_GROWTH_COLUMN in cells

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        if not self.can_parse(payload):
            raise ParseError("not an AMFI NAV history file")

        text = payload.content.decode("utf-8", errors="replace")
        lines = text.splitlines()
        header = [c.strip() for c in lines[0].split(";")]
        index = _column_index(header)

        rows: list[dict[str, str | None]] = []
        scheme_type: str | None = None
        amc_name: str | None = None

        for raw_line in lines[1:]:
            line = raw_line.strip()
            if not line:
                continue
            if ";" not in line:
                if line.startswith(SCHEME_TYPE_PREFIXES):
                    scheme_type = line
                else:
                    amc_name = line
                continue
            fields = [f.strip() for f in line.split(";")]
            if len(fields) != len(header):
                continue  # defensive: a row that does not match its own header is not data

            def cell(field: str, _fields: list[str] = fields) -> str:
                return _fields[index[field]]

            def isin(field: str) -> str | None:
                # AMFI spells an absent ISIN both ways -- an empty field and a
                # literal "-" -- in both ISIN columns. Verified against a live
                # 2019-03-14 fetch, which carries six "-" ISINs.
                value = cell(field)
                return None if value in ("-", "") else value

            rows.append(
                {
                    "scheme_code": cell("scheme_code"),
                    "isin_growth": isin("isin_growth"),
                    "isin_reinvest": isin("isin_reinvest"),
                    "scheme_name": cell("scheme_name"),
                    "nav": cell("nav"),
                    "nav_date": cell("nav_date"),
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
