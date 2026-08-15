from __future__ import annotations

from datetime import UTC, date, datetime

from trading.config import get_settings
from trading.contracts import RawPayload
from trading.sources.http import ArchivingClient

NSE_PRIME = "https://www.nseindia.com"
URL = (
    "https://nsearchives.nseindia.com/content/historical/EQUITIES/"
    "{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip"
)

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


class NseLegacyCmSource:
    source_key = "nse_cm_legacy"

    def __init__(self, client: ArchivingClient | None = None):
        self._client = client or ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(self, business_date: date) -> RawPayload | None:
        mon = _MONTH_ABBR[business_date.month].upper()
        url = URL.format(
            YYYY=business_date.strftime("%Y"),
            MON=mon,
            DD=business_date.strftime("%d"),
        )
        name = (
            f"{self.source_key}/{business_date:%Y}/{business_date:%m}/"
            f"{business_date.isoformat()}.zip"
        )
        result = self._client.get(
            url, archive_name=name, headers={"Referer": f"{NSE_PRIME}/"}, prime=NSE_PRIME
        )
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
