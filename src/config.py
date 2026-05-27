"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import AnyUrl, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ExecutionMode = Literal["paper_sim", "testnet_real", "mainnet_real"]
MAINNET_REAL_DISABLED_MESSAGE = (
    "MAINNET REAL MONEY EXECUTION IS DISABLED. "
    "Set EXECUTION_MODE=paper_sim or testnet_real."
)


class Settings(BaseSettings):
    """Application settings loaded from `.env` and process environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    execution_mode: ExecutionMode = "paper_sim"
    hyperliquid_private_key: str | None = Field(default=None, min_length=1)
    hyperliquid_account_address: str | None = Field(default=None, min_length=1)
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
    paper_slippage_bps: float = 5.0
    paper_max_pending_orders: int = 10
    scalp_mode_enabled: bool = True
    scalp_max_hold_minutes: int = 15
    scalp_cooldown_seconds: int = 600
    close_positions_on_shutdown: bool = False
    log_level: str = "INFO"

    @field_validator("max_daily_loss_pct", "max_weekly_loss_pct", "max_drawdown_pct")
    @classmethod
    def validate_pct(cls, value: float) -> float:
        """Validate that configured risk percentages are positive."""
        if value <= 0:
            raise ValueError("risk percentages must be positive")
        return value

    @field_validator("scalp_max_hold_minutes", "scalp_cooldown_seconds")
    @classmethod
    def validate_scalp_positive_ints(cls, value: int) -> int:
        """Validate positive scalp runtime limits."""
        if value <= 0:
            raise ValueError("scalp runtime limits must be positive")
        return value

    @field_validator("hyperliquid_private_key", "hyperliquid_account_address", mode="before")
    @classmethod
    def empty_credential_to_none(cls, value: str | None) -> str | None:
        """Treat empty optional credential env vars as unset."""
        if value == "":
            return None
        return value

    @field_validator("paper_slippage_bps")
    @classmethod
    def validate_slippage(cls, value: float) -> float:
        """Validate paper slippage."""
        if value < 0:
            raise ValueError("paper slippage cannot be negative")
        return value

    @field_validator("paper_max_pending_orders")
    @classmethod
    def validate_max_pending_orders(cls, value: int) -> int:
        """Validate pending paper order cap."""
        if value <= 0:
            raise ValueError("paper_max_pending_orders must be positive")
        return value

    @model_validator(mode="after")
    def validate_execution_mode(self) -> "Settings":
        """Validate mode-specific credential requirements and safety guards."""
        if self.execution_mode == "mainnet_real":
            raise ValueError(MAINNET_REAL_DISABLED_MESSAGE)
        if self.execution_mode == "testnet_real":
            if not self.hyperliquid_private_key or not self.hyperliquid_account_address:
                raise ValueError(
                    "HYPERLIQUID_PRIVATE_KEY and HYPERLIQUID_ACCOUNT_ADDRESS "
                    "are required when EXECUTION_MODE=testnet_real"
                )
        return self

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
