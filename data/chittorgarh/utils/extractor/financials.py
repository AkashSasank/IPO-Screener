import re

from bs4 import BeautifulSoup

from data.utils.base import Extractor


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
