import csv
import os

from data.chittorgarh.utils.extractor import ExtractorContext
from data.utils.config import parse_config

SENTINEL = None


def extract(config_path: str):
    # YOUR PRODUCER LOGIC (kept)
    config = parse_config(config_path)
    extractor = ExtractorContext()

    dataset_root = config["dataset_root"]
    raw_input_data_dir = os.path.join(dataset_root, "raw", "html")
    raw_output_data_dir = os.path.join(dataset_root, "raw", "csv")

    sections = config["sections"]
    segments = config["segments"]

    for segment in segments:
        segment_input_dir = os.path.join(raw_input_data_dir, segment)
        segment_output_dir = os.path.join(raw_output_data_dir, segment)
        os.makedirs(segment_output_dir, exist_ok=True)
        for section in sections:
            extractor.set_extractor(section)
            input_dir = os.path.join(segment_input_dir, section)
            output_file = os.path.join(segment_output_dir, f"{section}.csv")
            gen = data_extractor(input_dir, extractor)
            first_row = next(gen)
            fieldnames = list(first_row.keys())
            print(f"Writing {section} for {segment} in {output_file}")
            with open(output_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in gen:
                    writer.writerow(row)


def data_extractor(input_dir: str, extractor: ExtractorContext):
    for file in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file)
        data = extractor.extract(file_path)
        yield data
