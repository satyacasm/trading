from __future__ import annotations

from datetime import UTC, date, datetime

from trading.config import get_settings
from trading.contracts import RawPayload
from trading.sources.http import ArchivingClient

URL = "https://portal.amfiindia.com/spages/NAVAll.txt"


class AmfiNavSource:
    """Fetches AMFI's daily NAV file.

    The file carries the *latest* NAV per scheme rather than one date's worth
    of history, so every call hits the same URL regardless of business_date.
    """

    source_key = "amfi_nav"

    def __init__(self, client: ArchivingClient | None = None):
        self._client = client or ArchivingClient(root=get_settings().raw_archive_root)

    def fetch(self, business_date: date) -> RawPayload | None:
        name = (
            f"{self.source_key}/{business_date:%Y}/{business_date:%m}/"
            f"{business_date.isoformat()}.txt"
        )
        result = self._client.get(URL, archive_name=name, prime=None)
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
            meta={"url": URL},
        )
