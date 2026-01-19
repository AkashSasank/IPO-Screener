import re

from bs4 import BeautifulSoup

from data.utils.base import Extractor


class IPOGMPTagsExtractor(Extractor):
    """
    Extract Grey Market Premium (GMP) values for:
      - IPO Open
      - IPO Close
      - IPO Allotment
      - IPO Listing

    Built for Chittorgarh 'IPO Day-wise GMP Trend' table template where:
      - First column has date + optional badge (Open/Close/Allotment/Listing)
      - GMP column contains ₹ value plus arrow img etc.
    """

    def __init__(self, debug: bool = False) -> None:
        self.debug = debug

        # output fields -> badge labels we search for
        self.targets = {
            "ipo_open_gmp": "Open",
            "ipo_close_gmp": "Close",
            "ipo_allotment_gmp": "Allotment",
            "ipo_listing_gmp": "Listing",
        }

        # Accept ₹, &#8377; etc. Extract first signed/decimal number
        self._gmp_number_rx = re.compile(r"([+-]?\d+(?:\.\d+)?)")

    # ---------- utils ----------

    def _log(self, *args):
        if self.debug:
            print("[GMP-EXTRACTOR]", *args)

    @staticmethod
    def _read_file(filepath: str) -> str:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "")).strip()

    def _parse_gmp(self, text: str):
        """
        Extracts numeric GMP like '55' from strings like '₹55  <img ...>'
        """
        if not text:
            return None
        m = self._gmp_number_rx.search(text)
        return m.group(1) if m else None

    @staticmethod
    def _norm_header(h: str) -> str:
        return re.sub(r"\s+", " ", (h or "")).strip().lower()

    # ---------- core logic ----------

    def _find_gmp_table(self, soup: BeautifulSoup):
        """
        Prefer the specific Day-wise GMP Trend table by:
          1) h2 contains 'gmp trend'
          2) or table headers contain 'gmp date' and 'gmp'
        """
        # (1) Look for the h2 used in this template
        for h2 in soup.find_all(["h2", "h3"]):
            h2_txt = self._clean(h2.get_text(" ", strip=True)).lower()
            if "gmp" in h2_txt and "trend" in h2_txt:
                # next table after the heading
                tbl = h2.find_next("table")
                if tbl:
                    self._log("Selected GMP table via heading:", h2_txt)
                    return tbl

        # (2) Fallback: scan tables by headers
        for i, tbl in enumerate(soup.find_all("table")):
            thead = tbl.find("thead")
            if not thead:
                continue
            hdr_row = thead.find("tr")
            if not hdr_row:
                continue
            headers = [
                self._norm_header(th.get_text(" ", strip=True))
                for th in hdr_row.find_all(["th", "td"])
            ]
            if "gmp date" in headers and "gmp" in headers:
                self._log(
                    f"Selected GMP table via headers (table #{i}) headers={headers}"
                )
                return tbl

        return None

    def _extract_data(self, html_content: str) -> dict:
        soup = BeautifulSoup(html_content, "html.parser")
        result = {k: None for k in self.targets}

        gmp_table = self._find_gmp_table(soup)
        if not gmp_table:
            self._log("No GMP table detected")
            return result

        # Resolve column indices using headers (robust to reordering)
        hdr_row = None
        if gmp_table.find("thead"):
            hdr_row = (
                gmp_table.find("thead").find("tr") if gmp_table.find("thead") else None
            )
        if not hdr_row:
            hdr_row = gmp_table.find("tr")

        headers_raw = [
            self._clean(h.get_text(" ", strip=True))
            for h in hdr_row.find_all(["th", "td"])
        ]
        headers_norm = [self._norm_header(h) for h in headers_raw]
        self._log("Headers:", headers_raw)

        def _idx(name: str, default: int):
            name = name.lower()
            try:
                return headers_norm.index(name)
            except ValueError:
                return default

        date_col_idx = _idx("gmp date", 0)
        gmp_col_idx = _idx("gmp", 2)
        self._log(f"Using date_col_idx={date_col_idx}, gmp_col_idx={gmp_col_idx}")

        # Iterate rows in tbody if present; else all trs minus header row
        body_rows = []
        tbody = gmp_table.find("tbody")
        if tbody:
            body_rows = tbody.find_all("tr")
        else:
            body_rows = gmp_table.find_all("tr")[1:]

        for row_idx, tr in enumerate(body_rows, start=1):
            tds = tr.find_all("td")
            if len(tds) <= max(date_col_idx, gmp_col_idx):
                continue

            date_cell = tds[date_col_idx]
            gmp_cell = tds[gmp_col_idx]

            # Primary signal: badge text in first cell
            badge_texts = [
                self._clean(b.get_text(" ", strip=True))
                for b in date_cell.select("span.badge, span[class*='badge'], div.badge")
            ]

            date_text = self._clean(date_cell.get_text(" ", strip=True))
            gmp_text = self._clean(gmp_cell.get_text(" ", strip=True))
            gmp_value = self._parse_gmp(gmp_text)

            self._log(
                f"Row {row_idx}: date_text='{date_text}' badges={badge_texts} gmp_text='{gmp_text}' parsed={gmp_value}"
            )

            # Skip if we couldn't parse any number
            if gmp_value is None:
                continue

            # Determine which event this row represents
            # 1) badge match (preferred)
            event_label = None
            for lbl in self.targets.values():
                if any(
                    re.search(rf"\b{re.escape(lbl)}\b", bt, re.IGNORECASE)
                    for bt in badge_texts
                ):
                    event_label = lbl
                    break

            # 2) fallback: search in first cell text if badge missing
            if not event_label:
                for lbl in self.targets.values():
                    if re.search(rf"\b{re.escape(lbl)}\b", date_text, re.IGNORECASE):
                        event_label = lbl
                        break

            if not event_label:
                continue

            # Store into the corresponding output field (first match wins)
            for field, lbl in self.targets.items():
                if lbl.lower() == event_label.lower() and result[field] is None:
                    result[field] = gmp_value
                    self._log(f"Matched {lbl} -> {gmp_value}")
                    break

            # Early exit if all captured
            if all(result[k] is not None for k in self.targets):
                break

        return result

    def extract(self, filepath: str) -> dict:
        html_content = self._read_file(filepath)
        result = self._extract_data(html_content)
        result["company"] = filepath.split("/")[-1].split(".")[0]
        return result
