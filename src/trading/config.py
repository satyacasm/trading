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

    @property
    def raw_archive_root(self) -> Path:
        return self.data_root / "raw"

    @property
    def recordings_root(self) -> Path:
        return self.data_root / "recordings"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
