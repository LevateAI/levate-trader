"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic import AnyUrl, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from `.env` and process environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    hyperliquid_private_key: str = Field(..., min_length=1)
    hyperliquid_account_address: str = Field(..., min_length=1)
    hyperliquid_testnet: bool = True

    supabase_url: AnyUrl
    supabase_service_key: str = Field(..., min_length=1)

    discord_webhook_url: str | None = None
    sms_alerts_enabled: bool = True
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""
    twilio_to_number: str = ""

    max_daily_loss_pct: float = 5.0
    max_weekly_loss_pct: float = 10.0
    max_drawdown_pct: float = 20.0

    strategies_enabled: str = "cme_gap_fill,rsi_mean_reversion"
    starting_balance_usd: float = 1000.0
    close_positions_on_shutdown: bool = False
    log_level: str = "INFO"

    @field_validator("max_daily_loss_pct", "max_weekly_loss_pct", "max_drawdown_pct")
    @classmethod
    def validate_pct(cls, value: float) -> float:
        """Validate that configured risk percentages are positive."""
        if value <= 0:
            raise ValueError("risk percentages must be positive")
        return value

    @property
    def enabled_strategy_names(self) -> list[str]:
        """Return enabled strategy names as normalized identifiers."""
        return [name.strip() for name in self.strategies_enabled.split(",") if name.strip()]

    @property
    def daily_loss_fraction(self) -> float:
        """Configured daily loss limit as a fraction."""
        return self.max_daily_loss_pct / 100

    @property
    def weekly_loss_fraction(self) -> float:
        """Configured weekly loss limit as a fraction."""
        return self.max_weekly_loss_pct / 100

    @property
    def drawdown_fraction(self) -> float:
        """Configured all-time drawdown limit as a fraction."""
        return self.max_drawdown_pct / 100


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once per process."""
    return Settings()
