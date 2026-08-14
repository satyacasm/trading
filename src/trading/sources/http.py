from __future__ import annotations

import hashlib
import time
from pathlib import Path

import httpx
import structlog

from trading.contracts import FetchError

log = structlog.get_logger(__name__)

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)


class ArchivingClient:
    """Fetches a URL, archives the exact bytes, and returns them with a digest.

    Archiving before parsing is what lets a parser bug found in month four be
    fixed by re-reading disk instead of re-downloading 2,500 files.
    """

    def __init__(
        self,
        root: Path,
        *,
        transport: httpx.BaseTransport | None = None,
        attempts: int = 3,
        backoff_seconds: float = 2.0,
        timeout: float = 60.0,
    ) -> None:
        self._root = root
        self._attempts = attempts
        self._backoff = backoff_seconds
        self._client = httpx.Client(
            transport=transport,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": BROWSER_UA},
        )

    def get(
        self,
        url: str,
        *,
        archive_name: str,
        headers: dict[str, str] | None = None,
        prime: str | None = None,
    ) -> tuple[bytes, Path, str] | None:
        """Return (body, archive_path, sha256), or None on a clean 404."""
        if prime is not None:
            self._client.get(prime)  # populates cookies; failures are non-fatal

        last: Exception | None = None
        for attempt in range(1, self._attempts + 1):
            try:
                response = self._client.get(url, headers=headers or {})
            except httpx.HTTPError as exc:
                last = exc
            else:
                if response.status_code == 404:
                    log.info("source.absent", url=url)
                    return None
                if response.status_code == 200:
                    if not response.content:
                        raise FetchError(f"empty body from {url}")
                    return self._archive(response.content, archive_name)
                last = FetchError(f"HTTP {response.status_code} from {url}")

            if attempt < self._attempts:
                time.sleep(self._backoff * attempt)

        raise FetchError(f"failed after {self._attempts} attempts: {url}") from last

    def _archive(self, body: bytes, name: str) -> tuple[bytes, Path, str]:
        path = self._root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return body, path, hashlib.sha256(body).hexdigest()
