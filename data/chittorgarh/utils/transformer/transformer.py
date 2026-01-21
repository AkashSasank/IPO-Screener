import os

import dask.dataframe as dd

from data.utils.config import parse_config


class DataTransformer:
    def __init__(self, config_path: str):
        self.config = parse_config(config_path)
        self.common_field = "company"
        self.dataset = {}
        self.__combine_datasets()

    def __combine_datasets(self):
        dataset_root = self.config["dataset_root"]
        segments = self.config["segments"]
        out_files = []

        output_root = os.path.join(dataset_root, "processed", "csv", "combined")
        os.makedirs(output_root, exist_ok=True)
        for segment in segments:
            input_path = os.path.join(
                dataset_root,
                "processed",
                "csv",
                segment,
            )

            output_file = os.path.join(output_root, f"{segment}.csv")
            self.dataset[segment] = output_file
            out_files.append(output_file)
            files = sorted(os.listdir(input_path))
            df = dd.read_csv(os.path.join(input_path, files[0]))
            for file in files[1:]:
                file_path = os.path.join(input_path, file)
                print(f"Combining data from {file_path} into {output_file}")
                df = df.merge(dd.read_csv(file_path), on=self.common_field, how="left")
            df.drop_duplicates(subset=self.common_field, inplace=True)
            df.to_csv(output_file, index=False, single_file=True)
            print(f"Combined data saved to {output_file}")

        print("Combining segments...")
        output_file = os.path.join(
            dataset_root, "processed", "csv", "combined", "combined.csv"
        )
        self.dataset["combined"] = output_file
        df = dd.concat([dd.read_csv(file) for file in out_files], axis=0)
        df.drop_duplicates(subset=[self.common_field, "open_date"], inplace=True)
        df.to_csv(output_file, index=False, single_file=True)
        print(f"Combined data saved to {output_file}")

    def combined(self) -> dd.DataFrame:
        return dd.read_csv(self.dataset["combined"])

    def sme(self) -> dd.DataFrame:
        return dd.read_csv(self.dataset["sme"])

    def mainboard(self) -> dd.DataFrame:
        return dd.read_csv(self.dataset["mainboard"])
