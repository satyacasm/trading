from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

from trading.config import get_settings
from trading.contracts import RawPayload
from trading.sources.http import ArchivingClient

NSE_PRIME = "https://www.nseindia.com"
URL = "https://nsearchives.nseindia.com/content/{seg}/BhavCopy_NSE_{SEG}_0_0_0_{ymd}_F_0000.csv.zip"


class NseUdiffSource:
    def __init__(self, segment: Literal["cm", "fo"], client: ArchivingClient | None = None):
        self._segment = segment
        self.source_key = f"nse_{segment}_udiff"
        self._client = client or ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(self, business_date: date) -> RawPayload | None:
        ymd = business_date.strftime("%Y%m%d")
        url = URL.format(seg=self._segment, SEG=self._segment.upper(), ymd=ymd)
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
