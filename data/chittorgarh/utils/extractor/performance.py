import re

from bs4 import BeautifulSoup

from data.utils.base import Extractor


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
            # "current_market_price": None,
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
