from __future__ import annotations

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
    dhan_client_id: str | None = None
    dhan_access_token: str | None = None

    @property
    def raw_archive_root(self) -> Path:
        return self.data_root / "raw"

    @property
    def recordings_root(self) -> Path:
        return self.data_root / "recordings"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
