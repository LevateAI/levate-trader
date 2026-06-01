"""Configuration for the standalone Polymarket paper bot."""

from __future__ import annotations

from pydantic import AnyUrl, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PolymarketSettings(BaseSettings):
    """Runtime settings for the Polymarket read-only paper module."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    polymarket_account_id: str = "polymarket_crypto"
    polymarket_display_name: str = "Polymarket Crypto Arb"
    polymarket_starting_balance_usd: float = 500.0
    polymarket_poll_interval_sec: float = 2.0
    polymarket_market_refresh_sec: int = 300
    polymarket_stale_threshold_sec: int = 20
    polymarket_max_markets: int = 10
    polymarket_market_keywords: str = "bitcoin,btc,ethereum,eth"
    polymarket_fee_rate_crypto: float = 0.07

    polymarket_gamma_url: str = "https://gamma-api.polymarket.com"
    polymarket_clob_url: str = "https://clob.polymarket.com"
    coinbase_exchange_url: str = "https://api.exchange.coinbase.com"

    supabase_url: AnyUrl
    supabase_service_key: str = Field(..., min_length=1)
    log_level: str = "INFO"

    @field_validator(
        "polymarket_starting_balance_usd",
        "polymarket_poll_interval_sec",
        "polymarket_market_refresh_sec",
        "polymarket_stale_threshold_sec",
        "polymarket_max_markets",
    )
    @classmethod
    def validate_positive_numbers(cls, value: float | int) -> float | int:
        """Validate positive numeric settings."""
        if value <= 0:
            raise ValueError("Polymarket numeric settings must be positive")
        return value

    @field_validator("polymarket_fee_rate_crypto")
    @classmethod
    def validate_fee_rate(cls, value: float) -> float:
        """Validate fee rate coefficient."""
        if value < 0:
            raise ValueError("Polymarket fee rate cannot be negative")
        return value

    @property
    def market_keywords(self) -> list[str]:
        """Return normalized keywords used for crypto market discovery."""
        return [
            keyword.strip().lower()
            for keyword in self.polymarket_market_keywords.split(",")
            if keyword.strip()
        ]

