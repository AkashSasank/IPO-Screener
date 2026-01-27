import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Protocol

import dask.dataframe as dd
import pandas as pd


# --------------------------- Utils ---------------------------
class Parser:
    _PLACEHOLDER_TOKENS = {"[●]", "[•]", "—", "-", "NA", "N/A", "null", "None", ""}

    _ORDINAL_SUFFIX_RX = re.compile(r"(\d+)(st|nd|rd|th)\b", re.IGNORECASE)

    _CURRENCY_RX = re.compile(r"[₹,\s]")  # remove ₹, commas, extra spaces
    _MONEY_UNIT_RX = re.compile(r"\b(cr|cr\.|crore|lakh|lakhs)\b", re.IGNORECASE)

    _LISTLIKE_PREFIX_RX = re.compile(r"^\s*\[.*\]\s*$", re.DOTALL)

    def _normalize_missing(self, x: object) -> Optional[str]:
        if x is None:
            return None
        s = str(x).strip()
        if s in self._PLACEHOLDER_TOKENS:
            return None
        return s

    def _strip_ordinal_suffix(self, s: str) -> str:
        # "31st March 2015" -> "31 March 2015"
        return self._ORDINAL_SUFFIX_RX.sub(r"\1", s)

    def parse_indian_money_to_number(self, x: object) -> Optional[float]:
        """
        Normalizes amounts expressed like:
          - "₹ 356.19 Cr."
          - "₹ 6,997.28 Cr."
          - "356.19 Cr."
          - "₹ 25.5 Lakh"
          - "1,617.30"
          - "473.80"
        Output is a float in "Crore units" if a unit is present, else a float as-is.

        Notes:
          - If "Cr" is present: returns number in crores
          - If "Lakh" is present: converts lakhs -> crores (divide by 100)
          - If no unit: just parses numeric
        """
        s = self._normalize_missing(x)
        if s is None:
            return None

        s_low = s.lower()
        unit = None
        m = self._MONEY_UNIT_RX.search(s_low)
        if m:
            unit = m.group(1).lower()

        # remove currency symbols, commas, spaces
        s_num = self._CURRENCY_RX.sub("", s_low)
        # remove words like 'cr.' 'crore' 'lakh(s)'
        s_num = self._MONEY_UNIT_RX.sub("", s_num).strip()

        # sometimes string still has weird dots
        if not s_num:
            return None

        try:
            val = float(s_num)
        except ValueError:
            return None

        if unit in {"lakh", "lakhs"}:
            return val / 100.0  # lakh -> crore
        # cr/crore -> already in crores
        return val

    def parse_number(self, x: object) -> Optional[float]:
        """
        Parses generic numbers like:
          - "1,617.30"
          - "115"
          - "-3.20"
          - "15.93"
          - "4"
        """
        s = self._normalize_missing(x)
        if s is None:
            return None
        s = s.replace(",", "").strip()
        try:
            return float(s)
        except ValueError:
            return None

    def parse_percentage(self, x: object) -> Optional[float]:
        """
        Parses:
          - "100%"
          - "7.14%"
          - "0.43"   (already numeric % in some files)
        Returns float percentage value (e.g., 7.14).
        """
        s = self._normalize_missing(x)
        if s is None:
            return None
        s = s.strip()
        if s.endswith("%"):
            s = s[:-1].strip()
        s = s.replace(",", "")
        try:
            return float(s)
        except ValueError:
            return None

    def parse_date(self, x: object) -> Optional[str]:
        """
        Parses dates like:
          - "31st March 2015"
          - "10th December 2025"
          - "2025-12-10" (if present)
        Returns ISO string "YYYY-MM-DD" or None.
        """
        s = self._normalize_missing(x)
        if s is None:
            return None

        s = self._strip_ordinal_suffix(s).strip()

        # common formats seen in your csvs
        fmts = [
            "%d %B %Y",  # 31 March 2015
            "%d %b %Y",  # 31 Mar 2015
            "%Y-%m-%d",  # 2015-03-31
            "%d/%m/%Y",  # 31/03/2015
            "%d-%m-%Y",  # 31-03-2015
        ]
        for f in fmts:
            try:
                return datetime.strptime(s, f).date().isoformat()
            except ValueError:
                pass

        # last resort: pandas parser
        try:
            dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
            if pd.isna(dt):
                return None
            return dt.date().isoformat()
        except Exception:
            return None

    def normalize_company_slug(self, x: object) -> Optional[str]:
        """
        Keeps your existing slug style; also normalizes spaces/hyphens if any.
        """
        s = self._normalize_missing(x)
        if s is None:
            return None
        s = s.strip().lower()
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^a-z0-9_]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s or None

    def parse_text(self, x: object) -> Optional[str]:
        s = self._normalize_missing(x)
        if s is None:
            return None
        return s.strip()

    def parse_listlike_text(self, x: object) -> Optional[str]:
        """
        For object_of_issue which often looks like a stringified Python list.
        We keep it as text for now, but normalize whitespace.
        """
        s = self._normalize_missing(x)
        if s is None:
            return None
        s = re.sub(r"\s+", " ", s).strip()
        return s


# --------------------------- Cleaner strategies ---------------------------


@dataclass(frozen=True)
class MapCleaner:
    fn: callable

    def clean(self, s: dd.Series) -> dd.Series:
        return s.map(self.fn, meta=("x", "object"))


class MoneyCroreCleaner(MapCleaner):
    pass


class NumberCleaner(MapCleaner):
    pass


class PercentCleaner(MapCleaner):
    pass


class DateCleaner(MapCleaner):
    pass


class CompanyCleaner(MapCleaner):
    pass


class TextCleaner(MapCleaner):
    pass


class ListTextCleaner(MapCleaner):
    pass


# --------------------------- Strategy factory ---------------------------


class CleanerFactory:
    """
    Strategy pattern:
      - `get(col)` returns the appropriate cleaner strategy for that column.
      - You can add/override mappings as your schema evolves.
    """

    def __init__(self) -> None:
        # Base strategies
        parser = Parser()
        self.money = MoneyCroreCleaner(parser.parse_indian_money_to_number)
        self.num = NumberCleaner(parser.parse_number)
        self.pct = PercentCleaner(parser.parse_percentage)
        self.date = DateCleaner(parser.parse_date)
        self.company = CompanyCleaner(parser.normalize_company_slug)
        self.text = TextCleaner(parser.parse_text)
        self.listtext = ListTextCleaner(parser.parse_listlike_text)

        # Column -> strategy mapping (built using your 5 CSVs)
        self._by_col: Dict[str, MapCleaner] = {
            # --- common key ---
            "company": self.company,
            # --- financials.csv (mostly numeric; amounts/ratios/%/multiples) ---
            "assets": self.num,
            "net_worth": self.num,
            "total_debt": self.num,
            "revenue": self.num,
            "ebitda": self.num,
            "pat": self.num,
            "ebitda_margin": self.num,  # in your file it's 4.86 etc (already % value)
            "pat_margin": self.num,  # already % value
            "eps": self.num,
            "roe": self.num,  # already % value
            "roce": self.num,  # already % value
            "roa": self.num,  # already % value
            "debt_to_equity": self.num,  # ratio
            "market_capitalisation": self.num,  # already numeric here; in ipo_information it is ₹...Cr
            "enterprise_value": self.num,
            "ev_ebitda": self.num,  # multiple
            "pb_multiple": self.num,  # multiple
            "pe_multiple": self.num,  # multiple
            "nav": self.num,
            # --- gmp.csv (prices/premiums) ---
            "ipo_open_gmp": self.num,
            "ipo_close_gmp": self.num,
            "ipo_allotment_gmp": self.num,
            "ipo_listing_gmp": self.num,
            # --- ipo_information.csv (mix of text + money + dates) ---
            "ipo_category": self.text,
            "exchange": self.text,
            "issue_type": self.text,
            "ipo_size": self.money,  # "₹ 356.19 Cr."
            "issue_price": self.money,  # "₹ 115.00"
            "subscription": self.num,  # "0.77"
            "pre_issue_promoter_holding": self.num,  # sometimes blank
            "post_issue_promoter_holding": self.num,  # sometimes blank
            "dhrp_date": self.date,
            "open_date": self.date,
            "close_date": self.date,
            "allotment_date": self.date,  # may be [●]
            "listing_date": self.date,
            "object_of_issue": self.listtext,
            # NOTE: market_capitalisation + pe_multiple appear here too, but formatted as money/text sometimes
            # We'll handle with "auto override" below.
            # --- performance_report.csv (price + % gains) ---
            "face_value": self.num,
            "listing_price": self.num,
            "listing_gain": self.num,
            "current_market_price": self.num,
            # --- subscription.csv (mix: placeholders, % allocations, subscription multiples) ---
            "subscription_employees": self.num,  # often placeholder
            "subscription_qib": self.num,
            "subscription_retail_investors": self.num,
            "subscription_non_institutional_buyers": self.num,
            "subscription_snii_bids_below_10l": self.num,
            "subscription_bnii_bids_above_10l": self.num,
            "subscription_anchor_investors": self.num,
            "subscription_market_maker": self.num,
            "subscription_retail_individual_investors_riis": self.num,
            "subscription_shareholders": self.num,
            # allocations in your sample are in % strings like "100%", "7.14%"
            "allocation_total_ipo_subscription": self.pct,
            "allocation_qib": self.pct,
            "allocation_retail_investors": self.pct,
            "allocation_snii_bids_below_10l": self.pct,
            "allocation_employees": self.pct,
            "allocation_anchor_investors": self.pct,
            "allocation_non_institutional_buyers": self.pct,
            "allocation_bnii_bids_above_10l": self.pct,
            "allocation_market_maker": self.pct,
            "allocation_retail_individual_investors_riis": self.pct,
            "allocation_shareholders": self.pct,
        }

    def get(self, col: str) -> MapCleaner:
        """
        Switch cleaner strategy based on column name (col()).
        Defaults:
          - allocation_* -> PercentCleaner
          - subscription_* -> NumberCleaner
          - *_date -> DateCleaner
          - otherwise -> TextCleaner
        """
        if col in self._by_col:
            return self._by_col[col]

        if col.startswith("allocation_"):
            return self.pct

        if col.startswith("subscription_"):
            return self.num

        if col.endswith("_date"):
            return self.date

        # A few columns are polymorphic across files (e.g., market_capitalisation, issue_price)
        # If your pipeline reads from multiple sources, you can choose to always coerce these:
        if col in {"market_capitalisation", "issue_price", "ipo_size"}:
            return self.money

        return self.text
