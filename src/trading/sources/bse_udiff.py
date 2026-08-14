from __future__ import annotations

from datetime import UTC, date, datetime

from trading.config import get_settings
from trading.contracts import RawPayload
from trading.sources.http import ArchivingClient

BSE_REFERER = "https://www.bseindia.com/"
URL = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{ymd}_F_0000.CSV"


class BseUdiffSource:
    source_key = "bse_cm_udiff"

    def __init__(self, client: ArchivingClient | None = None):
        self._client = client or ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(self, business_date: date) -> RawPayload | None:
        ymd = business_date.strftime("%Y%m%d")
        url = URL.format(ymd=ymd)
        name = (
            f"{self.source_key}/{business_date:%Y}/{business_date:%m}/"
            f"{business_date.isoformat()}.csv"
        )
        result = self._client.get(
            url, archive_name=name, headers={"Referer": BSE_REFERER}, prime=None
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
