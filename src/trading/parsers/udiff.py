from __future__ import annotations

import io
import zipfile

import polars as pl

from trading.contracts import ParseError, RawPayload

ZIP_MAGIC = b"PK\x03\x04"

UDIFF_COLUMNS: tuple[str, ...] = (
    "TradDt",
    "BizDt",
    "Sgmt",
    "Src",
    "FinInstrmTp",
    "FinInstrmId",
    "ISIN",
    "TckrSymb",
    "SctySrs",
    "XpryDt",
    "FininstrmActlXpryDt",
    "StrkPric",
    "OptnTp",
    "FinInstrmNm",
    "OpnPric",
    "HghPric",
    "LwPric",
    "ClsPric",
    "LastPric",
    "PrvsClsgPric",
    "UndrlygPric",
    "SttlmPric",
    "OpnIntrst",
    "ChngInOpnIntrst",
    "TtlTradgVol",
    "TtlTrfVal",
    "TtlNbOfTxsExctd",
    "SsnId",
    "NewBrdLotQty",
    "Rmks",
    "Rsvd1",
    "Rsvd2",
    "Rsvd3",
    "Rsvd4",
)


def _extract_csv(content: bytes) -> bytes:
    """Return CSV bytes whether the payload is zipped (NSE) or plain (BSE)."""
    if not content.startswith(ZIP_MAGIC):
        return content
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise ParseError("zip contains no .csv member")
            return archive.read(names[0])
    except zipfile.BadZipFile as exc:
        raise ParseError("payload is not a readable zip") from exc


def _header_of(csv_bytes: bytes) -> tuple[str, ...]:
    first = csv_bytes.split(b"\n", 1)[0].replace(b"\r", b"")
    return tuple(first.decode("utf-8", errors="replace").split(","))


class UdiffParser:
    """Parses the UDiFF bhavcopy shared by NSE CM, NSE FO and BSE CM (finding F1)."""

    def can_parse(self, payload: RawPayload) -> bool:
        try:
            return _header_of(_extract_csv(payload.content)) == UDIFF_COLUMNS
        except ParseError:
            return False

    def parse(self, payload: RawPayload) -> pl.DataFrame:
        if not payload.content:
            raise ParseError("empty payload")
        csv_bytes = _extract_csv(payload.content)
        if _header_of(csv_bytes) != UDIFF_COLUMNS:
            raise ParseError("header is not UDiFF")
        try:
            frame = pl.read_csv(
                io.BytesIO(csv_bytes.replace(b"\r\n", b"\n")),
                schema_overrides={c: pl.String for c in UDIFF_COLUMNS},
                has_header=True,
                truncate_ragged_lines=False,
            )
        except Exception as exc:
            raise ParseError(f"unreadable UDiFF csv: {exc}") from exc
        if frame.height == 0:
            raise ParseError("UDiFF file has a header but no rows")
        return frame
