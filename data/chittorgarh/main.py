from scraper import ChittorgarhScraper

config_path = "./config.local.json"
scraper = ChittorgarhScraper(config=config_path)
scraper.scrape()
