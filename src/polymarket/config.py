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

    polymarket_account_id: str = "btc_5m"
    polymarket_display_name: str = "Polymarket Crypto Arb"
    polymarket_horizon: str = "5m"
    polymarket_assets: str = "BTC,ETH,SOL,XRP"
    polymarket_starting_balance_usd: float = 500.0
    polymarket_poll_interval_sec: float = 2.0
    polymarket_market_refresh_sec: int = 300
    polymarket_stale_threshold_sec: int = 20
    polymarket_max_markets: int = 16
    polymarket_market_keywords: str = "bitcoin,btc,ethereum,eth,solana,sol,xrp,ripple"
    polymarket_fee_rate_crypto: float = 0.072
    polymarket_strategies_enabled: str = "multi_outcome_sum_arb,latency_arb,ev_gated"
    polymarket_sum_arb_threshold: float = 0.02
    polymarket_sum_arb_max_account_pct: float = 0.10
    polymarket_sum_arb_max_stake_usd: float = 50.0
    polymarket_latency_edge_threshold: float = 0.05
    polymarket_latency_max_account_pct: float = 0.05
    polymarket_latency_max_stake_usd: float = 25.0
    polymarket_ev_min_edge: float = 0.04
    polymarket_ev_stake_usd: float = 30.0
    polymarket_ev_fee_band_low: float = 0.45
    polymarket_ev_fee_band_high: float = 0.55
    polymarket_vol_lambda: float = 0.97
    polymarket_vol_nu: float = 4.0
    polymarket_vol_sample_sec: float = 2.0
    polymarket_vol_window_sec: int = 900
    polymarket_strategy_cooldown_sec: int = 300
    stale_limit_seconds: int = 120
    watchdog_interval_seconds: int = 15

    polymarket_gamma_url: str = "https://gamma-api.polymarket.com"
    polymarket_clob_url: str = "https://clob.polymarket.com"
    polymarket_web_url: str = "https://polymarket.com"
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
        "stale_limit_seconds",
        "watchdog_interval_seconds",
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

    @field_validator("polymarket_horizon")
    @classmethod
    def validate_horizon(cls, value: str) -> str:
        """Validate supported Polymarket short-duration horizons."""
        normalized = value.strip().lower()
        if normalized not in {"5m", "15m"}:
            raise ValueError("POLYMARKET_HORIZON must be '5m' or '15m'")
        return normalized

    @field_validator(
        "polymarket_sum_arb_threshold",
        "polymarket_sum_arb_max_account_pct",
        "polymarket_latency_edge_threshold",
        "polymarket_latency_max_account_pct",
        "polymarket_sum_arb_max_stake_usd",
        "polymarket_latency_max_stake_usd",
        "polymarket_ev_min_edge",
        "polymarket_ev_stake_usd",
        "polymarket_ev_fee_band_low",
        "polymarket_ev_fee_band_high",
    )
    @classmethod
    def validate_strategy_fractions(cls, value: float) -> float:
        """Validate non-negative strategy fraction settings."""
        if value < 0:
            raise ValueError("Polymarket strategy fractions cannot be negative")
        return value

    @field_validator("polymarket_vol_nu", "polymarket_vol_sample_sec")
    @classmethod
    def validate_positive_strategy_numbers(cls, value: float) -> float:
        """Validate positive EV model settings."""
        if value <= 0:
            raise ValueError("Polymarket EV model settings must be positive")
        return value

    @field_validator("polymarket_vol_lambda")
    @classmethod
    def validate_vol_lambda(cls, value: float) -> float:
        """Validate EWMA lambda."""
        if not 0 < value < 1:
            raise ValueError("POLYMARKET_VOL_LAMBDA must be between 0 and 1")
        return value

    @property
    def market_keywords(self) -> list[str]:
        """Return normalized keywords used for crypto market discovery."""
        return [
            keyword.strip().lower()
            for keyword in self.polymarket_market_keywords.split(",")
            if keyword.strip()
        ]

    @property
    def enabled_strategy_names(self) -> list[str]:
        """Return enabled Polymarket strategies as normalized identifiers."""
        return [
            name.strip()
            for name in self.polymarket_strategies_enabled.split(",")
            if name.strip()
        ]

    @property
    def enabled_asset_symbols(self) -> tuple[str, ...]:
        """Return enabled crypto assets as uppercase symbols."""
        assets = tuple(
            asset.strip().upper()
            for asset in self.polymarket_assets.split(",")
            if asset.strip()
        )
        supported = {"BTC", "ETH", "SOL", "XRP"}
        unsupported = sorted(set(assets) - supported)
        if unsupported:
            raise ValueError(f"Unsupported POLYMARKET_ASSETS values: {unsupported}")
        if not assets:
            raise ValueError("POLYMARKET_ASSETS must include at least one asset")
        return assets
