import dask.dataframe as dd

from chittorgarh.utils.transformer.parser import CleanerFactory


class Cleaner:
    def __init__(self):
        self.cleaner_factory = CleanerFactory()

    def clean_df(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """
        Applies per-column cleaners using the strategy factory.
        Only cleans columns that exist in the given dataframe.
        """
        out = ddf
        for col in out.columns:
            cleaner = self.cleaner_factory.get(col)
            out[col] = cleaner.clean(out[col])
        return out

    def clean(self, input_csv: str, output_csv: str) -> str:
        """
        Reads input CSV, applies cleaning, writes to output CSV.
        Returns output CSV path.
        """
        ddf = dd.read_csv(input_csv, dtype="object")
        cleaned_ddf = self.clean_df(ddf)
        cleaned_ddf.to_csv(output_csv, single_file=True, index=False)
        return output_csv
