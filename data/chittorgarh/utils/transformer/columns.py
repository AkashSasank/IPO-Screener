from enum import Enum
from typing import Dict, List

from chittorgarh.utils.transformer.imputer import ImputerPolicy
from chittorgarh.utils.transformer.normalizer import NormalizationPolicy
from chittorgarh.utils.transformer.outlier import OutlierPolicy
from chittorgarh.utils.transformer.strategy import (DEFAULT_AMOUNT_STRATEGY,
                                                    DEFAULT_PRICE_STRATEGY,
                                                    DEFAULT_TEXT_DATE_STRATEGY,
                                                    ColumnStrategy, Metric)


class IPOColumn(Enum):
    # ---------- Financials ----------
    # COLUMN = ("column_name", "Description of the column", Metric.TYPE, ColumnStrategy)
    ASSETS = (
        "assets",
        "Total assets of the company",
        Metric.AMOUNT,
        DEFAULT_AMOUNT_STRATEGY,
    )
    NET_WORTH = (
        "net_worth",
        "Shareholders’ equity",
        Metric.AMOUNT,
        DEFAULT_AMOUNT_STRATEGY,
    )
    TOTAL_DEBT = (
        "total_debt",
        "Total borrowings",
        Metric.AMOUNT,
        DEFAULT_AMOUNT_STRATEGY,
    )
    REVENUE = ("revenue", "Operating revenue", Metric.AMOUNT, DEFAULT_AMOUNT_STRATEGY)
    EBITDA = (
        "ebitda",
        "Operating profit before depreciation and tax",
        Metric.AMOUNT,
        DEFAULT_AMOUNT_STRATEGY,
    )
    PAT = ("pat", "Profit after tax", Metric.AMOUNT, DEFAULT_AMOUNT_STRATEGY)

    EBITDA_MARGIN = (
        "ebitda_margin",
        "EBITDA as % of revenue",
        Metric.PERCENTAGE,
        # margins can be negative; widen bounds
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    PAT_MARGIN = (
        "pat_margin",
        "PAT as % of revenue",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    EPS = ("eps", "Earnings per share", Metric.PRICE, DEFAULT_PRICE_STRATEGY)

    ROE = (
        "roe",
        "Return on equity",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    ROCE = (
        "roce",
        "Return on capital employed",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    ROA = (
        "roa",
        "Return on assets",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    DEBT_TO_EQUITY = (
        "debt_to_equity",
        "Debt to equity ratio",
        Metric.RATIO,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    MARKET_CAPITALISATION = (
        "market_capitalisation",
        "Market value of equity",
        Metric.AMOUNT,
        DEFAULT_AMOUNT_STRATEGY,
    )
    ENTERPRISE_VALUE = (
        "enterprise_value",
        "Firm value including debt",
        Metric.AMOUNT,
        DEFAULT_AMOUNT_STRATEGY,
    )

    EV_EBITDA = (
        "ev_ebitda",
        "Enterprise value to EBITDA multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    PE_MULTIPLE = (
        "pe_multiple",
        "Price to earnings multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    PB_MULTIPLE = (
        "pb_multiple",
        "Price to book multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    NAV = ("nav", "Net asset value per share", Metric.PRICE, DEFAULT_PRICE_STRATEGY)

    # ---------- Company ----------
    COMPANY = (
        "company",
        "Name of issuing company",
        Metric.TEXT,
        DEFAULT_TEXT_DATE_STRATEGY,
    )

    # ---------- GMP ----------
    # GMP can be negative; don't log-normalize; cap tails
    IPO_OPEN_GMP = (
        "ipo_open_gmp",
        "GMP at IPO opening",
        Metric.PRICE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.NONE,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    IPO_CLOSE_GMP = (
        "ipo_close_gmp",
        "GMP at IPO close",
        Metric.PRICE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.NONE,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    IPO_ALLOTMENT_GMP = (
        "ipo_allotment_gmp",
        "GMP at allotment",
        Metric.PRICE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.NONE,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    IPO_LISTING_GMP = (
        "ipo_listing_gmp",
        "GMP on listing day",
        Metric.PRICE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.NONE,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    # ---------- IPO Metadata ----------
    IPO_CATEGORY = (
        "ipo_category",
        "Mainboard or SME",
        Metric.TEXT,
        DEFAULT_TEXT_DATE_STRATEGY,
    )
    EXCHANGE = ("exchange", "Listing exchange", Metric.TEXT, DEFAULT_TEXT_DATE_STRATEGY)
    ISSUE_TYPE = (
        "issue_type",
        "Fresh issue / OFS",
        Metric.TEXT,
        DEFAULT_TEXT_DATE_STRATEGY,
    )

    IPO_SIZE = (
        "ipo_size",
        "Total IPO issue size",
        Metric.AMOUNT,
        DEFAULT_AMOUNT_STRATEGY,
    )
    ISSUE_PRICE = (
        "issue_price",
        "IPO issue price",
        Metric.PRICE,
        DEFAULT_PRICE_STRATEGY,
    )
    FACE_VALUE = (
        "face_value",
        "Face value per share",
        Metric.PRICE,
        DEFAULT_PRICE_STRATEGY,
    )

    # ---------- Promoters ----------
    PRE_ISSUE_PROMOTER_HOLDING = (
        "pre_issue_promoter_holding",
        "Promoter holding before IPO",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )
    POST_ISSUE_PROMOTER_HOLDING = (
        "post_issue_promoter_holding",
        "Promoter holding after IPO",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.MEDIAN,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    # ---------- Dates ----------
    DRHP_DATE = (
        "dhrp_date",
        "DRHP filing date",
        Metric.DATE,
        DEFAULT_TEXT_DATE_STRATEGY,
    )
    OPEN_DATE = ("open_date", "IPO open date", Metric.DATE, DEFAULT_TEXT_DATE_STRATEGY)
    CLOSE_DATE = (
        "close_date",
        "IPO close date",
        Metric.DATE,
        DEFAULT_TEXT_DATE_STRATEGY,
    )
    ALLOTMENT_DATE = (
        "allotment_date",
        "Allotment date",
        Metric.DATE,
        DEFAULT_TEXT_DATE_STRATEGY,
    )
    LISTING_DATE = (
        "listing_date",
        "Listing date",
        Metric.DATE,
        DEFAULT_TEXT_DATE_STRATEGY,
    )

    # ---------- Listing ----------
    OBJECT_OF_ISSUE = (
        "object_of_issue",
        "Use of IPO proceeds",
        Metric.TEXT,
        DEFAULT_TEXT_DATE_STRATEGY,
    )
    LISTING_PRICE = (
        "listing_price",
        "Listing price",
        Metric.PRICE,
        DEFAULT_TEXT_DATE_STRATEGY,
    )  # label; don't touch
    LISTING_GAIN = (
        "listing_gain",
        "Listing day gain/loss",
        Metric.PRICE,
        DEFAULT_TEXT_DATE_STRATEGY,
    )  # label; don't touch
    CURRENT_MARKET_PRICE = (
        "current_market_price",
        "Latest market price",
        Metric.PRICE,
        DEFAULT_PRICE_STRATEGY,
    )

    # ---------- Subscription ----------
    # Subscription/multiples: sparse; fill 0 but keep missing indicator
    SUBSCRIPTION_TOTAL = (
        "subscription",
        "Overall subscription multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
            hard_min=0.0,
            hard_max=None,
        ),
    )
    ALLOCATION_TOTAL_IPO_SUBSCRIPTION = (
        "allocation_total_ipo_subscription",
        "Total shares allocated",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    SUBSCRIPTION_QIB = (
        "subscription_qib",
        "QIB subscription multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
            hard_min=0.0,
            hard_max=None,
        ),
    )
    ALLOCATION_QIB = (
        "allocation_qib",
        "Shares allocated to QIBs",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    SUBSCRIPTION_RETAIL_INVESTORS = (
        "subscription_retail_investors",
        "Retail subscription multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
            hard_min=0.0,
            hard_max=None,
        ),
    )
    ALLOCATION_RETAIL_INVESTORS = (
        "allocation_retail_investors",
        "Shares allocated to retail investors",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    SUBSCRIPTION_EMPLOYEES = (
        "subscription_employees",
        "Employee subscription multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
            hard_min=0.0,
            hard_max=None,
        ),
    )
    ALLOCATION_EMPLOYEES = (
        "allocation_employees",
        "Shares allocated to employees",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    # ---------- NIIs ----------
    SUBSCRIPTION_SNII_BELOW_10L = (
        "subscription_snii_bids_below_10l",
        "Small NII subscription multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
            hard_min=0.0,
            hard_max=None,
        ),
    )
    ALLOCATION_SNII_BELOW_10L = (
        "allocation_snii_bids_below_10l",
        "Shares allocated to small NIIs",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    SUBSCRIPTION_BNII_ABOVE_10L = (
        "subscription_bnii_bids_above_10l",
        "Big NII subscription multiple",
        Metric.TIMES,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            pctl_low=0.01,
            pctl_high=0.99,
            hard_min=0.0,
            hard_max=None,
        ),
    )
    ALLOCATION_BNII_ABOVE_10L = (
        "allocation_bnii_bids_above_10l",
        "Shares allocated to big NIIs",
        Metric.PERCENTAGE,
        ColumnStrategy(
            imputer=ImputerPolicy.ZERO,
            outlier=OutlierPolicy.CLIP_THEN_PCTL_CLIP,
            normalization=NormalizationPolicy.ROBUST_Z,
            hard_min=0.0,
            hard_max=100.0,
            pctl_low=0.01,
            pctl_high=0.99,
        ),
    )

    def __init__(
        self, column: str, description: str, metric: Metric, strategy: ColumnStrategy
    ):
        self._column = column
        self._description = description
        self._metric = metric
        self._strategy = strategy

    @property
    def col(self) -> str:
        return self._column

    @property
    def description(self) -> str:
        return self._description

    @property
    def metric(self) -> Metric:
        return self._metric

    @property
    def strategy(self) -> ColumnStrategy:
        return self._strategy

    # ---------- helpers ----------

    @staticmethod
    def get_field_names(metric: Metric) -> List[str]:
        return [c.col for c in IPOColumn if c.metric == metric]

    @staticmethod
    def get_by_imputer(policy: ImputerPolicy) -> List[str]:
        return [c.col for c in IPOColumn if c.strategy.imputer == policy]

    @staticmethod
    def get_by_outlier(policy: OutlierPolicy) -> List[str]:
        return [c.col for c in IPOColumn if c.strategy.outlier == policy]

    @staticmethod
    def get_by_normalization(policy: NormalizationPolicy) -> List[str]:
        return [c.col for c in IPOColumn if c.strategy.normalization == policy]

    @staticmethod
    def strategy_map() -> Dict[str, ColumnStrategy]:
        return {c.col: c.strategy for c in IPOColumn}

    @staticmethod
    def list():
        return [col for col in IPOColumn]
