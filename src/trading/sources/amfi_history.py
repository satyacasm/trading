from __future__ import annotations

from datetime import UTC, date, datetime

from trading.config import get_settings
from trading.contracts import RawPayload
from trading.sources.http import ArchivingClient

URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={d}&todt={d}"

# strftime("%b") is locale-dependent; a non-English locale would silently
# 404 every historical day rather than raise. Spell the mapping out instead.
_MONTH_ABBR = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


class AmfiNavHistorySource:
    """Fetches AMFI's per-date historical NAV report.

    Unlike `AmfiNavSource` (the latest-snapshot feed, which cannot answer for
    a past date), this endpoint accepts `frmdt`/`todt` query parameters and,
    with both set to the same date, returns exactly that day's NAVs. This is
    the genuinely per-date source and is what a historical backfill should
    use.

    Note: this is a different schema from NAVAll.txt (8 semicolon-delimited
    fields vs. 6, different column order, different ISIN header text) even
    though both are AMFI NAV files — see docs/data-formats/eod-source-formats.md
    §4. Parsing that distinction is a later task's job; this layer only
    archives the raw bytes.
    """

    source_key = "amfi_nav_history"

    def __init__(self, client: ArchivingClient | None = None):
        self._client = client or ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(self, business_date: date) -> RawPayload | None:
        d = f"{business_date:%d}-{_MONTH_ABBR[business_date.month]}-{business_date:%Y}"
        url = URL.format(d=d)
        name = (
            f"{self.source_key}/{business_date:%Y}/{business_date:%m}/"
            f"{business_date.isoformat()}.txt"
        )
        result = self._client.get(url, archive_name=name, prime=None)
        if result is None:
            return None
        body, path, digest = result
        return RawPayload(
            source_key=self.source_key,
            business_date=business_date,
            content=body,
            content_hash=digest,
            fetched_at=datetime.now(UTC),
            archive_path=path,
            meta={"url": url},
        )
