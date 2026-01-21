from data.chittorgarh.utils.extractor.financials import IPOFinancialsExtractor
from data.chittorgarh.utils.extractor.gmp import IPOGMPTagsExtractor
from data.chittorgarh.utils.extractor.information import \
    IPOInformationExtractor
from data.chittorgarh.utils.extractor.performance import \
    IPOPerformanceExtractor
from data.chittorgarh.utils.extractor.subscription import \
    IPOSubscriptionExtractor
from data.utils.base import Extractor


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
        self.strategy: Extractor = None

    def set_extractor(self, section: IPOSections):
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
