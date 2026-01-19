import re

from bs4 import BeautifulSoup

from data.utils.base import Extractor


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
