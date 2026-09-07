"""Async HTTP to the gateway. The only way this package reaches data.

Never psycopg: the routes enforce market hours, sufficient cash, contract
filters, the charge model and the breaker, and a second path to the
database would fork all of it.
"""

from __future__ import annotations

from typing import Any, cast

import httpx
import structlog

log = structlog.get_logger(__name__)

_TIMEOUT_SECONDS = 60.0


class GatewayRefusal(Exception):
    """The platform said no, and said why. Not a failure of the platform."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class GatewayUnavailable(Exception):
    """The platform could not answer. Distinct from a refusal on purpose.

    An agent should adapt to a refusal and must not adapt to a crash: a
    strategy rewritten around a 500 is a strategy fitted to a bug.
    """


class GatewayClient:
    """One method per HTTP verb, with the retry policy the spec requires."""

    def __init__(self, base_url: str, http: httpx.AsyncClient) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http

    @classmethod
    def open(cls, base_url: str) -> GatewayClient:
        return cls(base_url, httpx.AsyncClient(timeout=_TIMEOUT_SECONDS))

    async def aclose(self) -> None:
        await self._http.aclose()

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    @staticmethod
    def _decode(response: httpx.Response) -> Any:
        if response.status_code >= 500:
            raise GatewayUnavailable(
                f"gateway returned {response.status_code} for {response.request.url.path}"
            )
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail")
            except ValueError:
                detail = None
            raise GatewayRefusal(response.status_code, str(detail or response.text))
        if not response.content:
            return None
        return response.json()

    async def get(self, path: str, params: dict[str, object] | None = None) -> Any:
        """Retried once: a read is idempotent, so a dropped connection
        costs nothing to repeat."""
        # httpx's own params type is narrower than the interface this method
        # exposes (query values are always the JSON-primitive types the
        # gateway accepts); the cast documents that boundary rather than
        # widening it.
        query = cast("dict[str, Any] | None", params)
        for attempt in (1, 2):
            try:
                return self._decode(await self._http.get(self._url(path), params=query))
            except httpx.HTTPError as error:
                if attempt == 2:
                    raise GatewayUnavailable(f"GET {path} failed: {error}") from error
                log.warning("mcp.gateway.read_retry", path=path, error=str(error))
        raise AssertionError("unreachable")

    async def post(self, path: str, body: dict[str, object]) -> Any:
        """Never retried. A timed-out write may already have happened, and
        re-sending it is how one decision becomes two positions. Callers
        that need certainty read the state back instead."""
        try:
            return self._decode(await self._http.post(self._url(path), json=body))
        except httpx.HTTPError as error:
            raise GatewayUnavailable(f"POST {path} failed: {error}") from error

    async def delete(self, path: str) -> Any:
        """Never retried, for the reason `post` gives."""
        try:
            return self._decode(await self._http.delete(self._url(path)))
        except httpx.HTTPError as error:
            raise GatewayUnavailable(f"DELETE {path} failed: {error}") from error
