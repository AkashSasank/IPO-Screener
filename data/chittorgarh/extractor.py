import re
from bs4 import BeautifulSoup
from utils.base import Extractor
import os

class IPOPerformanceExtractor(Extractor):
    """Class to extract IPO performance data from an HTML file."""

    def __init__(self):
        """
        Initialize the extractor.
        """
        self.patterns = {
            'face_value': r'Face\s*(?:Value|value)[:\s]+₹?\s*([\d.]+)',
            'issue_price': r'Issue\s*(?:Price|price)[:\s]+₹?\s*([\d.]+)',
            'listing_price': r'Listing\s*(?:Price|price)[:\s]+₹?\s*([\d.]+)',
            'listing_gain': r'Listing\s*(?:Gain|gain)\s*\([^)]*\)\s*([+-]?[\d.]+)\s*(%)?',
        }

    def _extract_data(self, html_content: str) -> dict:
        """
        Extract IPO performance data from HTML content.

        Args:
            html_content: HTML content as string

        Returns:
            dict with performance metrics
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text(separator=" ", strip=True)

        performance_data = {
            'face_value': None,
            'issue_price': None,
            'listing_price': None,
            'listing_gain': None,
            'current_market_price': None
        }

        for key, pattern in self.patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                performance_data[key] = match.group(1)

        return performance_data
    @staticmethod
    def _read_file(filepath:str) -> str:
        """
        Read HTML file content.

        Returns:
            HTML content as string
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()

    def extract(self, filepath:str) -> dict:
        """
        Main extraction method that orchestrates all operations.

        Returns:
            dict with IPO performance data
        """
        try:
            html_content = self._read_file(filepath)
            result = self._extract_data(html_content)
            result['filename'] = filepath
            return result
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return {}
import re
from bs4 import BeautifulSoup

class IPOGMPTagsExtractor(Extractor):
    """
    DEBUG VERSION
    Extract Grey Market Premium (GMP) values for:
      - IPO Open
      - IPO Close
      - IPO Allotment
      - IPO Listing
    """

    def __init__(self, debug: bool = False) -> None:
        self.debug = debug

        self.targets = {
            "ipo_open_gmp": "Open",
            "ipo_close_gmp": "Close",
            "ipo_allotment_gmp": "Allotment",
            "ipo_listing_gmp": "Listing",
        }

        self._gmp_number_rx = re.compile(r"(?:₹\s*)?([+-]?\d+(?:\.\d+)?)")

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
        m = self._gmp_number_rx.search(text)
        return m.group(1) if m else None

    # ---------- core logic ----------

    def _extract_data(self, html_content: str) -> dict:
        soup = BeautifulSoup(html_content, "html.parser")

        result = {k: None for k in self.targets}
        tables = soup.find_all("table")

        self._log(f"Found {len(tables)} tables")

        gmp_table = None

        # 1️⃣ identify GMP table
        for i, table in enumerate(tables):
            table_text = self._clean(table.get_text(" ", strip=True)).lower()
            if "gmp" in table_text and "ipo price" in table_text:
                gmp_table = table
                self._log(f"Selected table #{i} as GMP table")
                break

        if not gmp_table:
            self._log("❌ No GMP table detected")
            return result

        # 2️⃣ inspect header row
        header_row = gmp_table.find("tr")
        headers = [self._clean(h.get_text()) for h in header_row.find_all(["th", "td"])]

        self._log("Table headers:", headers)

        try:
            date_col_idx = headers.index("GMP Date")
        except ValueError:
            date_col_idx = 0
            self._log("⚠️ 'GMP Date' header not found, defaulting to column 0")

        try:
            gmp_col_idx = headers.index("GMP")
        except ValueError:
            gmp_col_idx = 2
            self._log("⚠️ 'GMP' header not found, defaulting to column 2")

        self._log(f"Using date_col_idx={date_col_idx}, gmp_col_idx={gmp_col_idx}")

        # 3️⃣ iterate rows
        for row_idx, tr in enumerate(gmp_table.find_all("tr")[1:], start=1):
            tds = tr.find_all("td")
            if len(tds) <= max(date_col_idx, gmp_col_idx):
                continue

            date_cell = tds[date_col_idx]
            gmp_cell = tds[gmp_col_idx]

            # extract badge text (Open / Close / Allotment / Listing)
            badges = [
                self._clean(b.get_text())
                for b in date_cell.find_all(["span", "div"])
            ]

            date_text = self._clean(date_cell.get_text(" ", strip=True))
            gmp_text = self._clean(gmp_cell.get_text(" ", strip=True))
            gmp_value = self._parse_gmp(gmp_text)

            self._log(
                f"Row {row_idx}:",
                f"date_text='{date_text}'",
                f"badges={badges}",
                f"gmp_text='{gmp_text}'",
                f"parsed_gmp={gmp_value}",
            )

            if not gmp_value:
                continue

            row_context = " ".join([date_text] + badges)

            for field, label in self.targets.items():
                if result[field] is not None:
                    continue

                if re.search(rf"\b{label}\b", row_context, re.IGNORECASE):
                    result[field] = gmp_value
                    self._log(f"✅ Matched {label} → {gmp_value}")

        return result

    def extract(self, filepath: str) -> dict:
        html_content = self._read_file(filepath)
        result = self._extract_data(html_content)
        result["filename"] = filepath
        return result

class IPOFinancialsExtractor(Extractor):
    """Class to extract IPO financial data from HTML files."""

    def __init__(self):
        """
        Initialize the extractor with patterns for financial metrics.
        """
        self.patterns = {
        'assets': r'(?:Total\s+)?Assets?\s*[:\s]+₹?\s*([\d,.]+)',
        'net_worth': r'Net\s+Worth\s*[:\s]+₹?\s*([\d,.]+)',
        'total_debt': r'Total\s+Debt\s*[:\s]+₹?\s*([\d,.]+)',
        'revenue': r'(?:Total\s+)?Revenue\s*[:\s]+₹?\s*([\d,.]+)',
        'ebitda': r'EBITDA\s*[:\s]+₹?\s*([\d,.]+)',
        'pat': r'PAT\s*[:\s]+₹?\s*([\d,.]+)',
        'ebitda_margin': r'EBITDA\s+margin\s*[:\s(%]+\s*([\d.]+)',
        'pat_margin': r'PAT\s+margin\s*[:\s(%]+\s*([\d.]+)',
        'eps': r'EPS\s*\(₹\)\s*([+-]?[\d.]+)',
        'roe': r'ROE\s*\(%\)\s*([+-]?[\d.]+)',
        'roce': r'ROCE\s*\(%\)\s*([+-]?[\d.]+)',
        'roa': r'ROA\s*\(%\)\s*([+-]?[\d.]+)',
        'debt_to_equity': r'Debt\s+to\s+[Ee]quity\s*[:\s(x]+\s*([\d.]+)',
        'market_capitalisation': r'Market\s+Capitalisation?\s*[:\s]+₹?\s*([\d,.]+)',
        'ev_ebitda': r'EV\s*/\s*EBITDA\s*\(?times\)?\s*[:\s]+([+-]?[\d.]+)',
        'pb_multiple': r'P\s*/\s*B\s*\(?times\)?\s*[:\s]+([+-]?[\d.]+)',
        'nav': r'NAV\s*\(₹\)\s*[:\s]+([+-]?[\d.]+)',
        'enterprise_value': r'Enterprise\s+Value\s*\(EV\)\s*\(₹\s*Cr\.\)\s*:\s*([\d,.]+)',
        'pe_multiple': r'PE\s+Multiple\s*\(times\)\s*:\s*([\d.]+)',

        }

    def _extract_table_data(self, html_content: str) -> dict:
        """
        Extract financial data from tables in HTML content.

        Args:
            html_content: HTML content as string

        Returns:
            dict with table-based financial data
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        table_data = {}
        tables = soup.find_all('table')

        for table in tables:
            table_text = soup.get_text(separator=" ", strip=True).lower()
            rows = table.find_all('tr')

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    for key in self.patterns.keys():
                        if key.replace('_', ' ') in label:
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
        soup = BeautifulSoup(html_content, 'html.parser')
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
        with open(filepath, 'r', encoding='utf-8') as f:
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
            result['filename'] = filepath
            return result
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return {}

extractor = IPOFinancialsExtractor()
path = "/Users/akash/PycharmProjects/IPO-Screener/webscrapper/data/dataset/chittorgarh/raw/html/mainboard/financials/"
for f in os.listdir(path):
    filepath = os.path.join(path, f)
    res = extractor.extract(filepath)
    print(res)