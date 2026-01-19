import re

from bs4 import BeautifulSoup

from data.utils.base import Extractor


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
