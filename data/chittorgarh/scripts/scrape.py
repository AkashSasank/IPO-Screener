import sys

from data.chittorgarh.utils.scraper import ChittorgarhScraper


def scrape(config_path: str):
    """Scrape data from Chittorgarh website based on the provided configuration."""
    scraper = ChittorgarhScraper(config_path)
    scraper.scrape()


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) != 1:
        print("Usage: python scrape.py <config_path>")
        sys.exit(1)
    scrape(args[0])
