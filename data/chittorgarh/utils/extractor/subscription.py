# Assuming you already have:
# class Extractor: ...
# (same base as your other extractors)
import re

from bs4 import BeautifulSoup

from data.utils.base import Extractor

# Assuming you already have:
# class Extractor: ...


class IPOSubscriptionExtractor(Extractor):
    """
    Extracts:
      - Category-wise Subscription (times) -> flattened columns
      - Category-wise Allocation Size (%)  -> flattened columns

    Output example:
    {
      "company": "...",
      "subscription_anchor_investors": "1",
      "subscription_qib_ex_anchor": "1.06",
      "subscription_non_institutional_buyers": "5.73",
      "subscription_bnii_bids_above_10l": "5.25",
      "subscription_snii_bids_below_10l": "6.68",
      "subscription_retail_individual_investors_riis": "19.04",
      "allocation_anchor_investors": "39.88%",
      ...
    }
    """

    def __init__(self, debug: bool = False) -> None:
        self.debug = debug

    def _log(self, *args):
        if self.debug:
            print("[IPO-SUB-ALLOC-FLAT]", *args)

    @staticmethod
    def _read_file(filepath: str) -> str:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "")).strip()

    @staticmethod
    def _norm_header(s: str) -> str:
        return IPOSubscriptionExtractor._clean(s).lower()

    @staticmethod
    def _norm_category_text(cat: str) -> str:
        c = IPOSubscriptionExtractor._clean(cat)
        c = c.replace("\xa0", " ")
        c = re.sub(r"[\*]+", "", c)  # remove ** markers
        c = re.sub(r"^\s*[-–•]+", "", c).strip()
        c = re.sub(r"\s+", " ", c).strip()
        return c

    @staticmethod
    def _slugify(s: str) -> str:
        """
        Convert arbitrary category label to safe snake_case slug:
        - lowercase
        - remove currency symbols etc
        - keep alnum, replace others with underscore
        - collapse underscores
        """
        s = s.lower()
        s = s.replace("&", " and ")
        s = re.sub(r"[₹]", "", s)
        s = re.sub(r"[^a-z0-9]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    def _canonical_category_key(self, category_label: str) -> str:
        """
        Map noisy category names to stable slugs.
        Priority:
          1) Known pattern-based mapping (anchor/qib/nii/retail/bnii/snii)
          2) fallback slugify(category_label)
        """
        raw = self._norm_category_text(category_label)
        low = raw.lower()

        # Strong, common buckets on IPO pages
        # Anchor
        if "anchor" in low:
            return "anchor_investors"

        # QIB
        if "qib" in low:
            if "ex" in low and "anchor" in low:
                return "qib_ex_anchor"
            return "qib"

        # Retail / RII
        if "retail" in low or "rii" in low:
            # keep detail if present
            # e.g. "Retail Individual Investors (RIIs)" -> retail_individual_investors_riis
            return self._slugify(raw)

        # NII variants
        if (
            "non-institutional" in low
            or "non institutional" in low
            or re.search(r"\bnii\b", low)
        ):
            # bNII / sNII often appear as separate lines
            if "bnii" in low or (
                "above" in low
                and (
                    "10l" in low or "10 l" in low or "10lac" in low or "10 lakh" in low
                )
            ):
                return self._slugify(raw)
            if "snii" in low or (
                "below" in low
                and (
                    "10l" in low or "10 l" in low or "10lac" in low or "10 lakh" in low
                )
            ):
                return self._slugify(raw)
            # otherwise aggregate NII
            return "non_institutional_buyers"

        # Employee / shareholder / others if any
        if "employee" in low:
            return "employees"
        if "shareholder" in low:
            return "shareholders"

        # Fallback: stable slug of label
        return self._slugify(raw)

    def _find_header_row(self, table):
        """
        Scans first few rows to find a plausible header row.
        Needed because some pages have an extra border/header row.
        """
        rows = table.find_all("tr")
        for i, tr in enumerate(rows[:6]):
            cells = tr.find_all(["th", "td"])
            headers = [self._clean(c.get_text()) for c in cells]
            if not headers:
                continue

            # Prefer row containing Category
            if any("category" in h.lower() for h in headers) and len(headers) >= 2:
                return i, headers

            joined = " | ".join(h.lower() for h in headers)
            if "subscription" in joined or "size" in joined or "%" in joined:
                return i, headers

        return None, None

    def _extract_subscription_times_flat(self, soup: BeautifulSoup) -> dict:
        """
        Returns flattened: subscription_<category_key> -> subscription_times
        """
        out = {}
        for table in soup.find_all("table"):
            hdr_idx, headers = self._find_header_row(table)
            if headers is None:
                continue

            norm_headers = [self._norm_header(h) for h in headers]
            has_category = any("category" in h for h in norm_headers)
            has_sub_times = any(
                ("subscription" in h and "time" in h) for h in norm_headers
            )
            if not (has_category and has_sub_times):
                continue

            self._log("Matched subscription table headers:", headers)

            cat_i = next(
                (i for i, h in enumerate(norm_headers) if "category" in h), None
            )
            sub_i = next(
                (
                    i
                    for i, h in enumerate(norm_headers)
                    if ("subscription" in h and "time" in h)
                ),
                None,
            )
            if cat_i is None or sub_i is None:
                continue

            rows = table.find_all("tr")
            for tr in rows[hdr_idx + 1 :]:
                tds = tr.find_all(["td", "th"])
                if not tds:
                    continue
                cells = [self._clean(td.get_text()) for td in tds]
                if len(cells) <= max(cat_i, sub_i):
                    continue

                category = self._norm_category_text(cells[cat_i])
                if not category:
                    continue
                if category.lower() in {
                    "total",
                    "total subscription",
                    "total ipo subscription",
                }:
                    continue

                sub_times = self._clean(cells[sub_i])
                if not sub_times:
                    continue

                key = self._canonical_category_key(category)
                out[f"subscription_{key}"] = sub_times

            if out:
                break

        return out

    def _extract_allocation_pct_flat(self, soup: BeautifulSoup) -> dict:
        """
        Returns flattened: allocation_<category_key> -> allocation_percentage
        """
        out = {}
        for table in soup.find_all("table"):
            hdr_idx, headers = self._find_header_row(table)
            if headers is None:
                continue

            norm_headers = [self._norm_header(h) for h in headers]
            has_category = any("category" in h for h in norm_headers)

            pct_candidates = []
            for i, h in enumerate(norm_headers):
                # typical: "Size (%)"
                if "%" in h and ("size" in h or "allocation" in h or "issue" in h):
                    pct_candidates.append(i)

            if not (has_category and pct_candidates):
                continue

            pct_i = pct_candidates[0]
            cat_i = next(
                (i for i, h in enumerate(norm_headers) if "category" in h), None
            )
            if cat_i is None:
                continue

            self._log("Matched allocation table headers:", headers)

            rows = table.find_all("tr")
            for tr in rows[hdr_idx + 1 :]:
                tds = tr.find_all(["td", "th"])
                if not tds:
                    continue
                cells = [self._clean(td.get_text()) for td in tds]
                if len(cells) <= max(cat_i, pct_i):
                    continue

                category = self._norm_category_text(cells[cat_i])
                if not category:
                    continue
                if category.lower() in {"total", "total allocation"}:
                    continue

                pct = self._clean(cells[pct_i])
                if not pct:
                    continue

                key = self._canonical_category_key(category)
                out[f"allocation_{key}"] = pct

            if out:
                break

        return out

    def extract(self, filepath: str) -> dict:
        try:
            html = self._read_file(filepath)
            soup = BeautifulSoup(html, "html.parser")

            flat = {}
            flat.update(self._extract_subscription_times_flat(soup))
            flat.update(self._extract_allocation_pct_flat(soup))

            flat["company"] = filepath.split("/")[-1].split(".")[0]
            return flat
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return {}
