from __future__ import annotations

import pytest

from trading.mcp.serve_http import BearerTokenMiddleware, bearer_token


@pytest.mark.anyio
async def test_middleware_extracts_a_bearer_token_into_the_context() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    scope = {"type": "http", "headers": [(b"authorization", b"Bearer secret-a")]}
    await BearerTokenMiddleware(app)(scope, None, None)
    assert seen == ["secret-a"]


@pytest.mark.anyio
async def test_middleware_is_case_insensitive_about_the_scheme() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    scope = {"type": "http", "headers": [(b"Authorization", b"bearer secret-a")]}
    await BearerTokenMiddleware(app)(scope, None, None)
    assert seen == ["secret-a"]


@pytest.mark.anyio
async def test_a_request_with_no_authorization_header_carries_no_token() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    await BearerTokenMiddleware(app)({"type": "http", "headers": []}, None, None)
    assert seen == [None]


@pytest.mark.anyio
async def test_a_non_bearer_authorization_header_carries_no_token() -> None:
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    scope = {"type": "http", "headers": [(b"authorization", b"Basic abc")]}
    await BearerTokenMiddleware(app)(scope, None, None)
    assert seen == [None]


@pytest.mark.anyio
async def test_one_requests_token_does_not_leak_into_the_next() -> None:
    # Each request must resolve its own scope. A leaked token is a
    # request trading another agent's book.
    seen: list[str | None] = []

    async def app(scope: dict, receive: object, send: object) -> None:
        seen.append(bearer_token.get())

    middleware = BearerTokenMiddleware(app)
    await middleware({"type": "http", "headers": [(b"authorization", b"Bearer a")]}, None, None)
    await middleware({"type": "http", "headers": []}, None, None)
    assert seen == ["a", None]
