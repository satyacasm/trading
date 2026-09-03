from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local"), env_file_encoding="utf-8", extra="ignore"
    )

    database_url: str
    redis_url: str
    data_root: Path = Path("./data")

    upstox_api_key: str | None = None
    upstox_api_secret: str | None = None
    upstox_analytics_token: str | None = None
    upstox_access_token: str | None = None
    dhan_client_id: str | None = None
    dhan_access_token: str | None = None

    # `paper_engine`'s market-order slippage, in basis points, always moved
    # against the order (trading.paper.fills.decide_fill). 5 bps is
    # deliberately conservative -- roughly ₹0.65 on RELIANCE at 1310, and
    # wider than typical Binance spot spreads on BTC -- because
    # understating your edge is the safe direction to err (this project's
    # correctness doctrine). Tunable per-deployment without a code change;
    # trading.paper.engine.validate_slippage_bps rejects a negative value
    # at startup, whatever supplies it.
    paper_slippage_bps: Decimal = Decimal("5")

    # Passed to `docker run --runtime` for every strategy sandbox container
    # (trading.agent_contract.sandbox). None means "whatever the daemon
    # defaults to", which is `runc` even on a host where gVisor is
    # installed -- pointing DOCKER_CONTEXT at a gVisor-capable daemon is
    # therefore NOT enough on its own. Set this to "runsc" to actually get
    # kernel isolation. Left unset by default because a machine without
    # gVisor would fail every sandbox run rather than degrade.
    strategy_sandbox_runtime: str | None = None

    # Passed to `docker --context`, naming which daemon runs the sandbox.
    # Named explicitly rather than inherited from the ambient
    # DOCKER_CONTEXT so that one configuration controls the whole sandbox:
    # gVisor lives on a particular daemon, and a process that sets the
    # runtime but inherits a different daemon would run strategies on one
    # that has no runsc at all. None leaves the environment to decide.
    strategy_sandbox_docker_context: str | None = None

    # Telegram Bot API credentials for `trading.paper.alerts`' outbox worker
    # (Task 11). Both optional: an unconfigured bot is a deliberate
    # configuration state for this personal, single-user system, not an
    # error -- `run_alert_worker` idles rather than crashing when either is
    # unset (`build_telegram_sender` returns None, which is the signal it
    # checks). Not validated against each other at startup (a bot token with
    # no chat id, or vice versa, is still "unconfigured" as far as sending
    # is concerned) because nothing downstream needs one without the other.
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None

    @property
    def raw_archive_root(self) -> Path:
        return self.data_root / "raw"

    @property
    def recordings_root(self) -> Path:
        return self.data_root / "recordings"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
