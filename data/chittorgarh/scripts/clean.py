import os

from data.chittorgarh.utils.transformer.cleaner import Cleaner
from data.utils.config import parse_config


def clean(config_path: str):
    config = parse_config(config_path)
    cleaner = Cleaner()
    dataset_root = config["dataset_root"]
    raw_input_data_dir = os.path.join(dataset_root, "raw", "csv")
    cleaned_output_data_dir = os.path.join(dataset_root, "processed", "csv")

    sections = config["sections"]
    segments = config["segments"]

    for segment in segments:
        segment_input_dir = os.path.join(raw_input_data_dir, segment)
        segment_output_dir = os.path.join(cleaned_output_data_dir, segment)
        os.makedirs(segment_output_dir, exist_ok=True)
        for section in sections:
            input_filename = os.path.join(segment_input_dir, f"{section}.csv")
            output_filename = os.path.join(segment_output_dir, f"{section}.csv")
            cleaner.clean(input_filename, output_filename)
            print(f"Cleaned {section} for {segment} in {output_filename}")
