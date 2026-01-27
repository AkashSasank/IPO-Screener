from dataclasses import dataclass
from enum import Enum
from typing import Optional

from chittorgarh.utils.transformer.imputer import ImputerPolicy
from chittorgarh.utils.transformer.normalizer import NormalizationPolicy
from chittorgarh.utils.transformer.outlier import OutlierPolicy


class Metric(Enum):
    AMOUNT = "amount"  # absolute monetary value (₹)
    PRICE = "price"  # per-share price (₹)
    PERCENTAGE = "percentage"  # %
    RATIO = "ratio"  # unitless ratio
    TIMES = "times"  # x times
    COUNT = "count"  # number of shares / units
    DATE = "date"  # calendar date
    TEXT = "text"  # descriptive text


@dataclass(frozen=True)
class ColumnStrategy:
    imputer: ImputerPolicy
    outlier: OutlierPolicy
    normalization: NormalizationPolicy

    # Optional tuning knobs (per column)
    pctl_low: float = 0.01
    pctl_high: float = 0.99
    iqr_k: float = 1.5
    hard_min: Optional[float] = None
    hard_max: Optional[float] = None


# -------------------------
# Strategy defaults (opinionated + safe)
# -------------------------

DEFAULT_AMOUNT_STRATEGY = ColumnStrategy(
    imputer=ImputerPolicy.MEDIAN,
    outlier=OutlierPolicy.IQR_FILTER,
    normalization=NormalizationPolicy.LOG1P,
    pctl_low=0.01,
    pctl_high=0.99,
)

DEFAULT_PRICE_STRATEGY = ColumnStrategy(
    imputer=ImputerPolicy.MEDIAN,
    outlier=OutlierPolicy.PCTL_FILTER,
    normalization=NormalizationPolicy.LOG1P,  # keep scale unless you decide otherwise
    pctl_low=0.01,
    pctl_high=0.99,
)

DEFAULT_PERCENT_STRATEGY = ColumnStrategy(
    imputer=ImputerPolicy.MEDIAN,
    outlier=OutlierPolicy.PCTL_FILTER,
    normalization=NormalizationPolicy.ROBUST_Z,
    pctl_low=0.01,
    pctl_high=0.99,
)

DEFAULT_RATIO_STRATEGY = ColumnStrategy(
    imputer=ImputerPolicy.MEDIAN,
    outlier=OutlierPolicy.PCTL_CLIP,
    normalization=NormalizationPolicy.ROBUST_Z,
    pctl_low=0.01,
    pctl_high=0.99,
)

# For TEXT/DATE: untouched
DEFAULT_TEXT_DATE_STRATEGY = ColumnStrategy(
    imputer=ImputerPolicy.NONE,
    outlier=OutlierPolicy.NONE,
    normalization=NormalizationPolicy.NONE,
)


def default_strategy_for(metric: Metric) -> ColumnStrategy:
    if metric == Metric.AMOUNT:
        return DEFAULT_AMOUNT_STRATEGY
    if metric == Metric.PRICE:
        return DEFAULT_PRICE_STRATEGY
    if metric == Metric.PERCENTAGE:
        return DEFAULT_PERCENT_STRATEGY
    if metric in (Metric.RATIO, Metric.TIMES):
        return DEFAULT_RATIO_STRATEGY
    if metric in (Metric.TEXT, Metric.DATE):
        return DEFAULT_TEXT_DATE_STRATEGY
    return DEFAULT_TEXT_DATE_STRATEGY
