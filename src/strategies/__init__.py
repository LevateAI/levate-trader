"""Strategy registry."""

from src.strategies.base import Strategy
from src.strategies.cme_gap_fill import CmeGapFillStrategy
from src.strategies.rsi_mean_reversion import RsiMeanReversionStrategy

STRATEGY_REGISTRY: dict[str, type[Strategy]] = {
    CmeGapFillStrategy.name: CmeGapFillStrategy,
    RsiMeanReversionStrategy.name: RsiMeanReversionStrategy,
}

__all__ = ["STRATEGY_REGISTRY", "Strategy"]
