from enum import Enum
from typing import List, Tuple


class Metric(Enum):
    AMOUNT = "amount"  # absolute monetary value (₹)
    PRICE = "price"  # per-share price (₹)
    PERCENTAGE = "percentage"  # %
    RATIO = "ratio"  # unitless ratio
    TIMES = "times"  # x times
    COUNT = "count"  # number of shares / units
    DATE = "date"  # calendar date
    TEXT = "text"  # descriptive text


class IPOColumn(Enum):
    # ---------- Financials ----------
    ASSETS = ("assets", "Total assets of the company", Metric.AMOUNT)
    NET_WORTH = ("net_worth", "Shareholders’ equity", Metric.AMOUNT)
    TOTAL_DEBT = ("total_debt", "Total borrowings", Metric.AMOUNT)
    REVENUE = ("revenue", "Operating revenue", Metric.AMOUNT)
    EBITDA = ("ebitda", "Operating profit before depreciation and tax", Metric.AMOUNT)
    PAT = ("pat", "Profit after tax", Metric.AMOUNT)

    EBITDA_MARGIN = ("ebitda_margin", "EBITDA as % of revenue", Metric.PERCENTAGE)
    PAT_MARGIN = ("pat_margin", "PAT as % of revenue", Metric.PERCENTAGE)

    EPS = ("eps", "Earnings per share", Metric.PRICE)
    ROE = ("roe", "Return on equity", Metric.PERCENTAGE)
    ROCE = ("roce", "Return on capital employed", Metric.PERCENTAGE)
    ROA = ("roa", "Return on assets", Metric.PERCENTAGE)

    DEBT_TO_EQUITY = ("debt_to_equity", "Debt to equity ratio", Metric.RATIO)

    MARKET_CAPITALISATION = (
        "market_capitalisation",
        "Market value of equity",
        Metric.AMOUNT,
    )
    ENTERPRISE_VALUE = ("enterprise_value", "Firm value including debt", Metric.AMOUNT)

    EV_EBITDA = ("ev_ebitda", "Enterprise value to EBITDA multiple", Metric.TIMES)
    PE_MULTIPLE = ("pe_multiple", "Price to earnings multiple", Metric.TIMES)
    PB_MULTIPLE = ("pb_multiple", "Price to book multiple", Metric.TIMES)

    NAV = ("nav", "Net asset value per share", Metric.PRICE)

    # ---------- Company ----------
    COMPANY = ("company", "Name of issuing company", Metric.TEXT)

    # ---------- GMP ----------
    IPO_OPEN_GMP = ("ipo_open_gmp", "GMP at IPO opening", Metric.PRICE)
    IPO_CLOSE_GMP = ("ipo_close_gmp", "GMP at IPO close", Metric.PRICE)
    IPO_ALLOTMENT_GMP = ("ipo_allotment_gmp", "GMP at allotment", Metric.PRICE)
    IPO_LISTING_GMP = ("ipo_listing_gmp", "GMP on listing day", Metric.PRICE)

    # ---------- IPO Metadata ----------
    IPO_CATEGORY = ("ipo_category", "Mainboard or SME", Metric.TEXT)
    EXCHANGE = ("exchange", "Listing exchange", Metric.TEXT)
    ISSUE_TYPE = ("issue_type", "Fresh issue / OFS", Metric.TEXT)

    IPO_SIZE = ("ipo_size", "Total IPO issue size", Metric.AMOUNT)
    ISSUE_PRICE = ("issue_price", "IPO issue price", Metric.PRICE)
    FACE_VALUE = ("face_value", "Face value per share", Metric.PRICE)

    # ---------- Promoters ----------
    PRE_ISSUE_PROMOTER_HOLDING = (
        "pre_issue_promoter_holding",
        "Promoter holding before IPO",
        Metric.PERCENTAGE,
    )
    POST_ISSUE_PROMOTER_HOLDING = (
        "post_issue_promoter_holding",
        "Promoter holding after IPO",
        Metric.PERCENTAGE,
    )

    # ---------- Dates ----------
    DRHP_DATE = ("dhrp_date", "DRHP filing date", Metric.DATE)
    OPEN_DATE = ("open_date", "IPO open date", Metric.DATE)
    CLOSE_DATE = ("close_date", "IPO close date", Metric.DATE)
    ALLOTMENT_DATE = ("allotment_date", "Allotment date", Metric.DATE)
    LISTING_DATE = ("listing_date", "Listing date", Metric.DATE)

    # ---------- Listing ----------
    OBJECT_OF_ISSUE = ("object_of_issue", "Use of IPO proceeds", Metric.TEXT)
    LISTING_PRICE = ("listing_price", "Listing price", Metric.PRICE)
    LISTING_GAIN = ("listing_gain", "Listing day gain/loss", Metric.PRICE)
    CURRENT_MARKET_PRICE = ("current_market_price", "Latest market price", Metric.PRICE)

    # ---------- Subscription ----------
    SUBSCRIPTION_TOTAL = ("subscription", "Overall subscription multiple", Metric.TIMES)
    ALLOCATION_TOTAL_IPO_SUBSCRIPTION = (
        "allocation_total_ipo_subscription",
        "Total shares allocated",
        Metric.COUNT,
    )

    SUBSCRIPTION_QIB = ("subscription_qib", "QIB subscription multiple", Metric.TIMES)
    ALLOCATION_QIB = ("allocation_qib", "Shares allocated to QIBs", Metric.COUNT)

    SUBSCRIPTION_RETAIL_INVESTORS = (
        "subscription_retail_investors",
        "Retail subscription multiple",
        Metric.TIMES,
    )
    ALLOCATION_RETAIL_INVESTORS = (
        "allocation_retail_investors",
        "Shares allocated to retail investors",
        Metric.COUNT,
    )

    SUBSCRIPTION_EMPLOYEES = (
        "subscription_employees",
        "Employee subscription multiple",
        Metric.TIMES,
    )
    ALLOCATION_EMPLOYEES = (
        "allocation_employees",
        "Shares allocated to employees",
        Metric.COUNT,
    )

    # ---------- NIIs ----------
    SUBSCRIPTION_SNII_BELOW_10L = (
        "subscription_snii_bids_below_10l",
        "Small NII subscription multiple",
        Metric.TIMES,
    )
    ALLOCATION_SNII_BELOW_10L = (
        "allocation_snii_bids_below_10l",
        "Shares allocated to small NIIs",
        Metric.COUNT,
    )

    SUBSCRIPTION_BNII_ABOVE_10L = (
        "subscription_bnii_bids_above_10l",
        "Big NII subscription multiple",
        Metric.TIMES,
    )
    ALLOCATION_BNII_ABOVE_10L = (
        "allocation_bnii_bids_above_10l",
        "Shares allocated to big NIIs",
        Metric.COUNT,
    )

    def __init__(self, column: str, description: str, metric: Metric):
        self._column = column
        self._description = description
        self._metric = metric

    @property
    def col(self) -> str:
        return self._column

    @property
    def description(self) -> str:
        return self._description

    @property
    def metric(self) -> Metric:
        return self._metric

    @staticmethod
    def get_field_names( metric: Metric) -> List[str]:
        """
        Get all field names for a given metric type.
        """
        return [col.col for col in IPOColumn if col.metric == metric]
