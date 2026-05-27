"""Strategy registry."""

from src.strategies.base import Strategy
from src.strategies.book_imbalance import BookImbalanceStrategy
from src.strategies.cme_gap_fill import CmeGapFillStrategy
from src.strategies.micro_rsi_scalp import MicroRsiScalpStrategy
from src.strategies.rsi_mean_reversion import RsiMeanReversionStrategy
from src.strategies.volume_fade import VolumeFadeStrategy

STRATEGY_REGISTRY: dict[str, type[Strategy]] = {
    CmeGapFillStrategy.name: CmeGapFillStrategy,
    RsiMeanReversionStrategy.name: RsiMeanReversionStrategy,
    MicroRsiScalpStrategy.name: MicroRsiScalpStrategy,
    BookImbalanceStrategy.name: BookImbalanceStrategy,
    VolumeFadeStrategy.name: VolumeFadeStrategy,
}

__all__ = ["STRATEGY_REGISTRY", "Strategy"]
