import os

from data.chittorgarh.scripts import clean, extract, scrape
from data.chittorgarh.utils.transformer import DataTransformer
from data.utils.config import parse_config

# scrape("./config.local.json")
# extract("./config.local.json")
# clean("./config.local.json")
# config = parse_config("./config.local.json")
# from data.chittorgarh.utils.extractor.subscription import \
#     IPOSubscriptionExtractor
#
# data_dir = os.path.join(
#     config["dataset_root"], "raw", "html", "mainboard", "subscription"
# )
# extractor = IPOSubscriptionExtractor()
# for file in os.listdir(data_dir):
#     data = extractor.extract(os.path.join(data_dir, file))
#     print(data)
# data_dir = os.path.join(
#     config["dataset_root"], "raw", "html", "sme", "subscription"
# )
# extractor = IPOSubscriptionExtractor()
# for file in os.listdir(data_dir):
#     data = extractor.extract(os.path.join(data_dir, file))
#     print(data)


trans = DataTransformer(
    config_path="/Users/akash/PycharmProjects/IPO-Screener/webscrapper/data/chittorgarh/config.local.json"
)
print(trans.combined())
