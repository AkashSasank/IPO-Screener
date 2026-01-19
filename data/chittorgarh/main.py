from data.chittorgarh.scripts import extract
import os
from data.utils.config import parse_config

config = parse_config("./config.local.json")
from data.chittorgarh.utils.extractor.subscription import \
    IPOSubscriptionExtractor
data_dir = os.path.join(config["dataset_root"], "raw", "html", "mainboard", "subscription")
extractor = IPOSubscriptionExtractor()
for file in os.listdir(data_dir):
    data = extractor.extract(
        os.path.join(data_dir, file)
)
    print(data)