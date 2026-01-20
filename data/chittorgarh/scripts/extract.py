import csv
import os
import sys
import tempfile
from typing import Dict, Iterable

from data.chittorgarh.utils.extractor import ExtractorContext
from data.utils.config import parse_config


class DynamicCSVWriter:
    """
    CSV writer that dynamically expands columns when new keys appear.
    Rewrites the file ONLY when schema changes.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.fieldnames = []
        self._rows_written = 0

    def _rewrite_with_new_header(self, new_fieldnames: list[str]):
        """
        Rewrite existing CSV with expanded header.
        """
        tmp_fd, tmp_path = tempfile.mkstemp()
        os.close(tmp_fd)

        with open(self.filepath, "r", newline="") as src, open(
            tmp_path, "w", newline=""
        ) as dst:

            reader = csv.DictReader(src)
            writer = csv.DictWriter(dst, fieldnames=new_fieldnames)
            writer.writeheader()

            for row in reader:
                writer.writerow(row)

        os.replace(tmp_path, self.filepath)
        self.fieldnames = new_fieldnames

    def write_row(self, row: Dict):
        """
        Write a row, expanding columns if required.
        """
        row_keys = set(row.keys())

        # First row ever → initialize file
        if not self.fieldnames:
            self.fieldnames = list(row_keys)
            with open(self.filepath, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
                writer.writerow(row)
            self._rows_written += 1
            return

        # Check for new columns
        new_cols = row_keys - set(self.fieldnames)
        if new_cols:
            new_fieldnames = self.fieldnames + sorted(new_cols)
            self._rewrite_with_new_header(new_fieldnames)

        # Append row
        with open(self.filepath, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writerow(row)

        self._rows_written += 1


def extract(config_path: str):
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
            print(f"Writing {section} for {segment} in {output_file}")
            writer = DynamicCSVWriter(output_file)
            for row in gen:
                writer.write_row(row)


def data_extractor(input_dir: str, extractor: ExtractorContext):
    for file in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file)
        data = extractor.extract(file_path)
        yield data


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) != 1:
        print("Usage: python extract.py <config_path>")
        sys.exit(1)
    extract(args[0])
