from __future__ import annotations

import pytest

from trading.mcp.session import AgentSession, SessionRefused, SessionStore


def test_a_known_token_resolves_to_its_portfolio() -> None:
    store = SessionStore({"secret-a": 1, "secret-b": 2})
    assert store.resolve("secret-a") == AgentSession(token="secret-a", portfolio_id=1)
    assert store.resolve("secret-b").portfolio_id == 2


def test_an_unknown_token_is_refused() -> None:
    store = SessionStore({"secret-a": 1})
    with pytest.raises(SessionRefused):
        store.resolve("secret-c")


def test_a_missing_token_is_refused_when_there_is_no_stdio_scope() -> None:
    store = SessionStore({"secret-a": 1})
    with pytest.raises(SessionRefused):
        store.resolve(None)


def test_a_missing_token_falls_back_to_the_stdio_portfolio() -> None:
    # get_access_token() returns None over stdio; the subprocess is
    # already inside the trust boundary, so scope comes from config.
    store = SessionStore({"secret-a": 1}, stdio_portfolio_id=7)
    assert store.resolve(None).portfolio_id == 7
    assert store.resolve(None).token is None


def test_the_refusal_never_repeats_the_token_back() -> None:
    # A rejected credential must not be echoed into logs or an agent's
    # transcript.
    store = SessionStore({"secret-a": 1})
    with pytest.raises(SessionRefused) as excinfo:
        store.resolve("hunter2")
    assert "hunter2" not in str(excinfo.value)


def test_settings_parse_comma_separated_token_pairs() -> None:
    store = SessionStore.from_pairs("alpha:1, beta:2", stdio_portfolio_id=None)
    assert store.resolve("alpha").portfolio_id == 1
    assert store.resolve("beta").portfolio_id == 2


def test_an_empty_token_setting_yields_a_store_that_refuses_everything() -> None:
    store = SessionStore.from_pairs("", stdio_portfolio_id=None)
    with pytest.raises(SessionRefused):
        store.resolve("alpha")


def test_a_malformed_token_pair_is_rejected_at_construction() -> None:
    # Failing at startup rather than on the first order: a typo here
    # otherwise surfaces as an authentication failure mid-session.
    with pytest.raises(ValueError):
        SessionStore.from_pairs("alpha-1", stdio_portfolio_id=None)
    with pytest.raises(ValueError):
        SessionStore.from_pairs("alpha:notanumber", stdio_portfolio_id=None)


def test_two_tokens_resolve_to_different_portfolios() -> None:
    # The scope guarantee only holds if distinct tokens actually yield
    # distinct portfolios rather than both resolving to whichever value
    # a sloppy implementation defaults to.
    store = SessionStore.from_pairs("alpha:1, beta:2", stdio_portfolio_id=None)
    alpha = store.resolve("alpha")
    beta = store.resolve("beta")
    assert alpha.portfolio_id != beta.portfolio_id


def test_from_settings_reads_mcp_tokens_and_stdio_portfolio_id() -> None:
    from trading.config import Settings

    settings = Settings(
        _env_file=None,
        database_url="postgresql://u:p@localhost:5432/db",
        redis_url="redis://localhost:6379/0",
        mcp_tokens="alpha:1, beta:2",
        mcp_stdio_portfolio_id=9,
    )
    store = SessionStore.from_settings(settings)
    assert store.resolve("alpha").portfolio_id == 1
    assert store.resolve(None).portfolio_id == 9
