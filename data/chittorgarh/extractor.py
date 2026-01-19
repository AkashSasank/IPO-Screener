import os
import re

from bs4 import BeautifulSoup
from utils.base import Extractor


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


class IPOSubscriptionExtractor(Extractor):
    """Class to extract IPO subscription and share allocation data from HTML files."""

    def __init__(self, debug: bool = False):
        """
        Initialize the extractor.

        Args:
            debug: Enable debug logging
        """
        self.debug = debug

    def _log(self, *args):
        """Debug logging utility."""
        if self.debug:
            print("[IPO-SUBSCRIPTION-EXTRACTOR]", *args)

    @staticmethod
    def _read_file(filepath: str) -> str:
        """
        Read HTML file content.

        Args:
            filepath: Path to HTML file

        Returns:
            HTML content as string
        """
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _clean(s: str) -> str:
        """Clean and normalize text."""
        return re.sub(r"\s+", " ", (s or "")).strip()

    def _extract_subscription_data(self, html_content: str) -> dict:
        """
        Extract category-wise IPO subscription data from tables.

        Args:
            html_content: HTML content as string

        Returns:
            dict with subscription data by category
        """
        soup = BeautifulSoup(html_content, "html.parser")
        subscription_data = {}
        tables = soup.find_all("table")

        for table in tables:
            table_text = self._clean(table.get_text()).lower()

            # Check if this is a subscription table
            if "subscription" in table_text and "category" in table_text:
                self._log("Found subscription table")

                rows = table.find_all("tr")
                headers = [
                    self._clean(h.get_text()) for h in rows[0].find_all(["th", "td"])
                ]

                self._log(f"Headers: {headers}")

                # Find column indices
                col_indices = {
                    "category": None,
                    "subscription_times": None,
                    "shares_offered": None,
                    "shares_applied": None,
                    "amount_offered": None,
                    "amount_applied": None,
                }

                for idx, header in enumerate(headers):
                    header_lower = header.lower()
                    if "category" in header_lower:
                        col_indices["category"] = idx
                    elif "subscription" in header_lower and "times" in header_lower:
                        col_indices["subscription_times"] = idx
                    elif "shares" in header_lower and "offered" in header_lower:
                        col_indices["shares_offered"] = idx
                    elif "shares" in header_lower and (
                        "applied" in header_lower or "bid" in header_lower
                    ):
                        col_indices["shares_applied"] = idx
                    elif "amount" in header_lower and "offered" in header_lower:
                        col_indices["amount_offered"] = idx
                    elif "amount" in header_lower and (
                        "applied" in header_lower or "bid" in header_lower
                    ):
                        col_indices["amount_applied"] = idx

                self._log(f"Column indices: {col_indices}")

                # Extract data rows
                for row_idx, tr in enumerate(rows[1:], start=1):
                    cells = [
                        self._clean(td.get_text()) for td in tr.find_all(["td", "th"])
                    ]

                    if not cells or len(cells) < 2:
                        continue

                    category = (
                        cells[col_indices["category"]].strip()
                        if col_indices["category"] is not None
                        else None
                    )

                    if not category or category.lower() in [
                        "total",
                        "total subscription",
                    ]:
                        continue

                    row_data = {"category": category}

                    if col_indices["subscription_times"] is not None and col_indices[
                        "subscription_times"
                    ] < len(cells):
                        row_data["subscription_times"] = cells[
                            col_indices["subscription_times"]
                        ]

                    if col_indices["shares_offered"] is not None and col_indices[
                        "shares_offered"
                    ] < len(cells):
                        row_data["shares_offered"] = cells[
                            col_indices["shares_offered"]
                        ]

                    if col_indices["shares_applied"] is not None and col_indices[
                        "shares_applied"
                    ] < len(cells):
                        row_data["shares_applied"] = cells[
                            col_indices["shares_applied"]
                        ]

                    if col_indices["amount_offered"] is not None and col_indices[
                        "amount_offered"
                    ] < len(cells):
                        row_data["amount_offered"] = cells[
                            col_indices["amount_offered"]
                        ]

                    if col_indices["amount_applied"] is not None and col_indices[
                        "amount_applied"
                    ] < len(cells):
                        row_data["amount_applied"] = cells[
                            col_indices["amount_applied"]
                        ]

                    subscription_data[category] = row_data
                    self._log(f"Row {row_idx}: {row_data}")

        return subscription_data

    def _extract_allocation_data(self, html_content: str) -> dict:
        """
        Extract category-wise share allocation data from tables.

        Args:
            html_content: HTML content as string

        Returns:
            dict with allocation data by category
        """
        soup = BeautifulSoup(html_content, "html.parser")
        allocation_data = {}
        tables = soup.find_all("table")

        for table in tables:
            table_text = self._clean(table.get_text()).lower()

            # Check if this is an allocation table
            if "allocation" in table_text and "category" in table_text:
                self._log("Found allocation table")

                rows = table.find_all("tr")
                headers = [
                    self._clean(h.get_text()) for h in rows[0].find_all(["th", "td"])
                ]

                self._log(f"Headers: {headers}")

                # Find column indices
                col_indices = {
                    "category": None,
                    "shares_offered": None,
                    "amount": None,
                    "size_percentage": None,
                }

                for idx, header in enumerate(headers):
                    header_lower = header.lower()
                    if "category" in header_lower:
                        col_indices["category"] = idx
                    elif "shares" in header_lower and "offered" in header_lower:
                        col_indices["shares_offered"] = idx
                    elif "amount" in header_lower and "%" not in header_lower:
                        col_indices["amount"] = idx
                    elif "%" in header_lower or "size" in header_lower:
                        col_indices["size_percentage"] = idx

                self._log(f"Column indices: {col_indices}")

                # Extract data rows
                for row_idx, tr in enumerate(rows[1:], start=1):
                    cells = [
                        self._clean(td.get_text()) for td in tr.find_all(["td", "th"])
                    ]

                    if not cells or len(cells) < 2:
                        continue

                    category = (
                        cells[col_indices["category"]].strip()
                        if col_indices["category"] is not None
                        else None
                    )

                    if not category or category.lower() in [
                        "total",
                        "total allocation",
                    ]:
                        continue

                    row_data = {"category": category}

                    if col_indices["shares_offered"] is not None and col_indices[
                        "shares_offered"
                    ] < len(cells):
                        row_data["shares_offered"] = cells[
                            col_indices["shares_offered"]
                        ]

                    if col_indices["amount"] is not None and col_indices[
                        "amount"
                    ] < len(cells):
                        row_data["amount"] = cells[col_indices["amount"]]

                    if col_indices["size_percentage"] is not None and col_indices[
                        "size_percentage"
                    ] < len(cells):
                        row_data["size_percentage"] = cells[
                            col_indices["size_percentage"]
                        ]

                    allocation_data[category] = row_data
                    self._log(f"Row {row_idx}: {row_data}")

        return allocation_data

    def _extract_data(self, html_content: str) -> dict:
        """
        Extract all IPO subscription and allocation data.

        Args:
            html_content: HTML content as string

        Returns:
            dict with subscription and allocation data
        """
        subscription = self._extract_subscription_data(html_content)
        allocation = self._extract_allocation_data(html_content)

        return {"subscription": subscription, "allocation": allocation}

    def extract(self, filepath: str) -> dict:
        """
        Main extraction method that orchestrates all operations.

        Args:
            filepath: Path to HTML file

        Returns:
            dict with IPO subscription and allocation data
        """
        try:
            html_content = self._read_file(filepath)
            result = self._extract_data(html_content)
            result["company"] = filepath.split("/")[-1].split(".")[0]
            return result
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return {}


class IPOPerformanceExtractor(Extractor):
    """Class to extract IPO performance data from an HTML file."""

    def __init__(self):
        """
        Initialize the extractor.
        """
        self.patterns = {
            "face_value": r"Face\s*(?:Value|value)[:\s]+₹?\s*([\d.]+)",
            "issue_price": r"Issue\s*(?:Price|price)[:\s]+₹?\s*([\d.]+)",
            "listing_price": r"Listing\s*(?:Price|price)[:\s]+₹?\s*([\d.]+)",
            "listing_gain": r"Listing\s*(?:Gain|gain)\s*\([^)]*\)\s*([+-]?[\d.]+)\s*(%)?",
        }

    def _extract_data(self, html_content: str) -> dict:
        """
        Extract IPO performance data from HTML content.

        Args:
            html_content: HTML content as string

        Returns:
            dict with performance metrics
        """
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        performance_data = {
            "face_value": None,
            "issue_price": None,
            "listing_price": None,
            "listing_gain": None,
            "current_market_price": None,
        }

        for key, pattern in self.patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                performance_data[key] = match.group(1)

        return performance_data

    @staticmethod
    def _read_file(filepath: str) -> str:
        """
        Read HTML file content.

        Returns:
            HTML content as string
        """
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    def extract(self, filepath: str) -> dict:
        """
        Main extraction method that orchestrates all operations.

        Returns:
            dict with IPO performance data
        """
        try:
            html_content = self._read_file(filepath)
            result = self._extract_data(html_content)
            result["company"] = filepath.split("/")[-1].split(".")[0]
            return result
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return {}


class IPOFinancialsExtractor(Extractor):
    """Class to extract IPO financial data from HTML files."""

    def __init__(self):
        """
        Initialize the extractor with patterns for financial metrics.
        """
        self.patterns = {
            "assets": r"(?:Total\s+)?Assets?\s*[:\s]+₹?\s*([\d,.]+)",
            "net_worth": r"Net\s+Worth\s*[:\s]+₹?\s*([\d,.]+)",
            "total_debt": r"Total\s+Debt\s*[:\s]+₹?\s*([\d,.]+)",
            "revenue": r"(?:Total\s+)?Revenue\s*[:\s]+₹?\s*([\d,.]+)",
            "ebitda": r"EBITDA\s*[:\s]+₹?\s*([\d,.]+)",
            "pat": r"PAT\s*[:\s]+₹?\s*([\d,.]+)",
            "ebitda_margin": r"EBITDA\s+margin\s*[:\s(%]+\s*([\d.]+)",
            "pat_margin": r"PAT\s+margin\s*[:\s(%]+\s*([\d.]+)",
            "eps": r"EPS\s*\(₹\)\s*([+-]?[\d.]+)",
            "roe": r"ROE\s*\(%\)\s*([+-]?[\d.]+)",
            "roce": r"ROCE\s*\(%\)\s*([+-]?[\d.]+)",
            "roa": r"ROA\s*\(%\)\s*([+-]?[\d.]+)",
            "debt_to_equity": r"Debt\s+to\s+[Ee]quity\s*[:\s(x]+\s*([\d.]+)",
            "market_capitalisation": r"Market\s+Capitalisation?\s*[:\s]+₹?\s*([\d,.]+)",
            "ev_ebitda": r"EV\s*/\s*EBITDA\s*\(?times\)?\s*[:\s]+([+-]?[\d.]+)",
            "pb_multiple": r"P\s*/\s*B\s*\(?times\)?\s*[:\s]+([+-]?[\d.]+)",
            "nav": r"NAV\s*\(₹\)\s*[:\s]+([+-]?[\d.]+)",
            "enterprise_value": r"Enterprise\s+Value\s*\(EV\)\s*\(₹\s*Cr\.\)\s*:\s*([\d,.]+)",
            "pe_multiple": r"PE\s+Multiple\s*\(times\)\s*:\s*([\d.]+)",
        }

    def _extract_table_data(self, html_content: str) -> dict:
        """
        Extract financial data from tables in HTML content.

        Args:
            html_content: HTML content as string

        Returns:
            dict with table-based financial data
        """
        soup = BeautifulSoup(html_content, "html.parser")
        table_data = {}
        tables = soup.find_all("table")

        for table in tables:
            table_text = soup.get_text(separator=" ", strip=True).lower()
            rows = table.find_all("tr")

            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    for key in self.patterns.keys():
                        if key.replace("_", " ") in label:
                            table_data[key] = value
                            break

        return table_data

    def _extract_data(self, html_content: str) -> dict:
        """
        Extract IPO financial data from HTML content.

        Args:
            html_content: HTML content as string

        Returns:
            dict with financial metrics
        """
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        financial_data = {key: None for key in self.patterns.keys()}

        # Extract using regex patterns
        for key, pattern in self.patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                financial_data[key] = match.group(1)

        # If no matches found, try extracting from tables
        if not any(financial_data.values()):
            table_data = self._extract_table_data(html_content)
            financial_data.update(table_data)

        return financial_data

    @staticmethod
    def _read_file(filepath: str) -> str:
        """
        Read HTML file content.

        Args:
            filepath: Path to HTML file

        Returns:
            HTML content as string
        """
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    def extract(self, filepath: str) -> dict:
        """
        Main extraction method that orchestrates all operations.

        Args:
            filepath: Path to HTML file

        Returns:
            dict with IPO financial data
        """
        try:
            html_content = self._read_file(filepath)
            result = self._extract_data(html_content)
            result["company"] = filepath.split("/")[-1].split(".")[0]
            return result
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return {}


class IPOInformationExtractor(Extractor):
    """Class to extract IPO information from HTML files."""

    @staticmethod
    def _read_file(filepath: str) -> str:
        """Read HTML file content."""
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _clean(s: str) -> str:
        """Clean and normalize text."""
        return re.sub(r"\s+", " ", (s or "")).strip()

    def _extract_object_of_issue(self, html_content: str) -> list:
        """
        Extract Object of Issue from HTML.

        Args:
            html_content: HTML content as string

        Returns:
            list of objects
        """
        soup = BeautifulSoup(html_content, "html.parser")
        objects = []

        # Look for sections with "Object of Issue" heading
        for element in soup.find_all(["h2", "h3", "h4", "strong", "b"]):
            element_text = self._clean(element.get_text()).lower()

            if "object" in element_text and "issue" in element_text:

                # Get parent and find following list or paragraphs
                parent = element.parent
                current = element.next_sibling

                while current:
                    if isinstance(current, str):
                        text = self._clean(current)
                        if text and len(text) > 10 and not text.startswith("<"):
                            objects.append(text)
                            if len(objects) > 10:
                                break
                    elif hasattr(current, "name"):
                        if current.name in ["ul", "ol"]:
                            for li in current.find_all("li"):
                                text = self._clean(li.get_text())
                                if text and len(text) > 5:
                                    objects.append(text)
                            break
                        elif current.name in ["h2", "h3", "h4"]:
                            break
                        elif current.name == "p":
                            text = self._clean(current.get_text())
                            if text and len(text) > 10:
                                objects.append(text)

                    current = current.next_sibling if current else None

                if objects:
                    break

        return objects[:10]  # Limit to 10 items

    def _extract_from_text(self, html_content: str) -> dict:
        """
        Extract IPO information from HTML using refined case-sensitive regex patterns.

        Args:
            html_content: HTML content as string

        Returns:
            dict with extracted fields
        """
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        field_patterns = {
            "ipo_category": r"IPO\s+Category\s*:\s*([^:\n]+?)(?:\s+(?:Exchange|Issue|IPO\s+Size)|$)",
            "exchange": r"Exchange\s*:\s*([^:\n]+?)(?:\s+(?:Issue\s+Type|IPO\s+Size)|$)",
            "issue_type": r"Issue\s+Type\s*:\s*([^:\n]+?)(?:\s+(?:IPO\s+Size|Issue\s+Price)|$)",
            "ipo_size": r"IPO\s+Size\s*:\s*([^:\n]+?)(?:\s+(?:Issue\s+Price|Market\s+Capitalisation)|$)",
            "issue_price": r"Issue\s+Price\s*:\s*([^:\n]+?)(?:\s+(?:Market\s+Capitalisation|PE\s+multiple)|$)",
            "market_capitalisation": r"Market\s+Capitalisation\s*:\s*([^:\n]+?)(?:\s+(?:PE\s+multiple|Subscription)|$)",
            "pe_multiple": r"PE\s+multiple\s*:\s*([^:\n]+?)(?:\s+(?:Subscription|Pre\s+Issue)|$)",
            "subscription": r"Subscription\s*:\s*([^:\n]+?)(?:\s+(?:Pre\s+Issue|Post\s+Issue|times)|$)",
            "pre_issue_promoter_holding": r"Pre\s+Issue\s+Promoter\s+Holding\s*:\s*([^:\n]+?)(?:\s+(?:Post\s+Issue|%)|$)",
            "post_issue_promoter_holding": r"Post\s+Issue\s+Promoter\s+Holding\s*:\s*([^:\n%]+?)(?:%|$)",
        }

        data = {}
        for field, pattern in field_patterns.items():
            match = re.search(pattern, text)
            if match:
                value = self._clean(match.group(1)).strip()
                # Remove trailing field names or invalid characters
                value = re.sub(
                    r"(?:Read|Financial|Information|Documents|Key|Highlights).*$",
                    "",
                    value,
                ).strip()
                data[field] = value
            else:
                data[field] = None

        return data

    def _extract_dates_from_text(self, html_content: str) -> dict:
        """
        Extract dates from HTML using refined case-sensitive regex patterns.

        Args:
            html_content: HTML content as string

        Returns:
            dict with dates
        """
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        date_patterns = {
            "dhrp_date": r"Date\s+of\s+DRHP\s*:\s*([^:\n]+?)(?:\s+(?:IPO\s+Open|Initiation)|$)",
            "open_date": r"IPO\s+Open\s+Date\s*:\s*([^:\n]+?)(?:\s+(?:IPO\s+Close|Initiation)|$)",
            "close_date": r"IPO\s+Close\s+Date\s*:\s*([^:\n]+?)(?:\s+(?:IPO\s+Allotment|Initiation)|$)",
            "allotment_date": r"IPO\s+Allotment\s+Date\s*:\s*([^:\n]+?)(?:\s+(?:IPO\s+Listing|Initiation|Refund)|$)",
            "listing_date": r"""
                (?ix)
                IPO\s+Listing\s+Date\s*:\s*
                (?:<[^>]*>\s*)*
                (\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+\d{4})
            """,
        }

        dates = {}
        for date_field, pattern in date_patterns.items():
            match = re.search(pattern, text)
            if match:
                value = self._clean(match.group(1)).strip()
                # Remove trailing junk
                value = re.sub(
                    r"(?:Initiation|Refund|Read|Documents|Financial).*$", "", value
                ).strip()
                dates[date_field] = value
            else:
                dates[date_field] = None

        return dates

    def _extract_data(self, html_content: str) -> dict:
        """
        Extract all IPO information data.

        Args:
            html_content: HTML content as string

        Returns:
            dict with IPO information
        """
        data = self._extract_from_text(html_content)
        dates = self._extract_dates_from_text(html_content)

        if dates:
            data = data | dates

        objects = self._extract_object_of_issue(html_content)
        if objects:
            data["object_of_issue"] = objects

        return data

    def extract(self, filepath: str) -> dict:
        """
        Main extraction method.

        Args:
            filepath: Path to HTML file

        Returns:
            dict with IPO information data
        """
        try:
            html_content = self._read_file(filepath)
            result = self._extract_data(html_content)
            result["company"] = filepath.split("/")[-1].split(".")[0]
            return result
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return {}


# extractor = IPOInformationExtractor()
# path = "/Users/akash/PycharmProjects/IPO-Screener/webscrapper/data/dataset/chittorgarh/raw/html/mainboard/ipo_information/"
# for f in os.listdir(path):
#     filepath = os.path.join(path, f)
#     res = extractor.extract(filepath)
#     print(res)


class IPOSections:
    IPO_INFORMATION = "ipo_information"
    IPO_FINANCIALS = "financials"
    IPO_PERFORMANCE = "performance_report"
    IPO_GMP_TAGS = "gmp"
    IPO_SUBSCRIPTION = "subscription"
    IPO_PEERS = "peers"
    IPO_REVIEW = "review"


class ExtractorContext:
    """Strategy to select appropriate extractor based on IPO section."""

    def __init__(self):
        self.strategy: Extractor = Extractor()

    def set_extractor(self, section: str):
        if section == IPOSections.IPO_INFORMATION:
            self.strategy = IPOInformationExtractor()
        elif section == IPOSections.IPO_FINANCIALS:
            self.strategy = IPOFinancialsExtractor()
        elif section == IPOSections.IPO_PERFORMANCE:
            self.strategy = IPOPerformanceExtractor()
        elif section == IPOSections.IPO_GMP_TAGS:
            self.strategy = IPOGMPTagsExtractor()
        elif section == IPOSections.IPO_SUBSCRIPTION:
            self.strategy = IPOSubscriptionExtractor()
        # elif section == IPOSections.IPO_PEERS:
        #     self.strategy = None  # Placeholder for IPOPeersExtractor
        # elif section == IPOSections.IPO_REVIEW:
        #     self.strategy = None  # Placeholder for IPOReviewExtractor
        else:
            raise Exception(f"IPO section {section} not supported")

    def extract(self, filepath: str) -> dict:
        response = self.strategy.extract(filepath)
        return response


extractor = IPOGMPTagsExtractor()
path = "/Users/akash/PycharmProjects/IPO-Screener/webscrapper/data/dataset/chittorgarh/raw/html/sme/gmp/"
for i, f in enumerate(os.listdir(path)):
    print(i)
    filepath = os.path.join(path, f)
    res = extractor.extract(filepath)
    print(res)
