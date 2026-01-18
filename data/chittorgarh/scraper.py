import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import pandas as pd
import requests
from chittorgarh.fetcher import ChittorgarhFetcher, IPOGmpTableFetcher
from utils.config import parse_config


class ChittorgarhScraper:
    """
    A scraper class to scrape data from Chittorgarh.
    """

    def __init__(self, config: str):
        self.fetcher = ChittorgarhFetcher()
        self.gmp_fetcher = IPOGmpTableFetcher()
        self.config = parse_config(config)
        self.base_url = self.config["base_url"]
        self.base_data_files = []

    def scrape(self, outputs_base_dir: str = "../dataset/chittorgarh/raw"):
        print("Scraping Chittorgarh...")
        print("Getting base data...")
        self._get_base_data(os.path.join(outputs_base_dir, "csv"))
        print("Downloading pages...")
        self._get_pages(os.path.join(outputs_base_dir, "html"))
        print("Done scraping")

    def _get_base_data(self, outputs_dir: str):
        """
        Download base data with page info such as urls and names
        :return:
        """
        for source in self.config["segments"]:
            print(f"Getting data for {source}")
            page_url = self.config["segmentsAPI"][source]["page_url"].format(
                base_url=self.base_url
            )
            api_url = self.config["segmentsAPI"][source]["api_url"].format(
                base_url=self.base_url
            )
            params = self.config["segmentsAPI"][source]["params"]
            n_pages = self.config["segmentsAPI"][source]["n_pages"]
            fields = self.config["segmentsAPI"][source]["fields"]

            rows, failed_urls = self.fetcher.fetch(
                page_url=page_url,
                api_url=api_url,
                params=params,
                n_pages=n_pages,
                fields=fields,
            )
            if failed_urls:
                print(f"Warning: Failed to fetch some URLs: {len(failed_urls)} items")

            df = self.build_dataframe_from_rows(rows)

            try:
                csv_path = self.save_dataframe_as_csv(df, source, outputs_dir)
                self.base_data_files.append(csv_path)
                print(f"Saved {len(df)} rows to {csv_path}")
            except Exception as e:
                print(f"Failed to save CSV for {source}: {e}")

            print("Total rows fetched:", len(df))
            if not df.empty:
                print(df.head().to_string())

    def _get_pages(
        self,
        outputs_dir: str,
        base_data_files: list[str] = None,
        max_workers: int = 5,
    ):
        """
        Download html pages concurrently using ThreadPoolExecutor.
        Uses background threads to prevent blocking.

        :param outputs_dir: Output directory for HTML files
        :param base_data_files: List of CSV files to process
        :param max_workers: Maximum number of concurrent threads (default: 5)
        """
        if not base_data_files:
            base_data_files = self.base_data_files

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for file in base_data_files:
                self._submit_file_tasks(executor, futures, file, outputs_dir)

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in background thread: {e}")

    def _submit_file_tasks(
        self, executor: ThreadPoolExecutor, futures: list, file: str, outputs_dir: str
    ):
        """Submit page fetch tasks for a single CSV file."""
        df = pd.read_csv(file)
        category = file.split("/")[-1].split(".")[0]
        for row in df.itertuples():
            self._submit_row_tasks(executor, futures, row, outputs_dir, category)

    def _submit_row_tasks(
        self,
        executor: ThreadPoolExecutor,
        futures: list,
        row,
        outputs_dir: str,
        category: str,
    ):
        """Submit page fetch tasks for a single row."""
        for section in self.config["sections"]:
            out_dir = os.path.join(outputs_dir, category, section)
            os.makedirs(out_dir, exist_ok=True)
            page_info = self.config["sectionsAPI"][section]

            if page_info.get("path"):
                future = executor.submit(
                    self._fetch_and_save_page,
                    slug=row.chittorgarh_slug,
                    id=row.id,
                    company_name=row.company_name,
                    out_dir=out_dir,
                    url_pattern=page_info["path"],
                    url=None,
                )
                futures.append(future)

            if page_info.get("key") == "investor_gain" and pd.notna(row.investor_gain):
                future = executor.submit(
                    self._fetch_and_save_page,
                    slug=row.chittorgarh_slug,
                    id=row.id,
                    company_name=row.company_name,
                    out_dir=out_dir,
                    url_pattern=None,
                    url=row.investor_gain,
                )
                futures.append(future)

    def _fetch_and_save_page(
        self,
        slug: str,
        id: int,
        company_name: str,
        out_dir: str,
        url_pattern: str = None,
        url: str = None,  # Happens when we need to go to an external page. Eg:investor gain
    ):
        """Fetch a page and save it to disk (runs in background thread)."""
        path = (
            os.path.join(
                out_dir,
                company_name.lower().replace(" ", "_"),
            )
            + ".html"
        )
        if os.path.exists(path):
            print(f"File {path} already exists, skipping.")
            return
        # TODO: Make this dynamic. Right now only GMP pages are external
        if url:
            print(f"Fetching URL: {url}")
            page = self.gmp_fetcher.extract_gmp(url)
        else:
            page = self._get_page(slug=slug, id=id, url_pattern=url_pattern)
        if page:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    if isinstance(page, str):
                        f.write(page)
                    else:
                        f.write(page.text)
                print(f"Saved page to {path}")
            except Exception as e:
                print(f"Failed to save {path}: {e}")

    def _get_page(self, slug: str, id: int, url_pattern: str):
        """Build URL and fetch the page."""
        url = url_pattern.format(chittorgarh_slug=slug, id=id, base_url=self.base_url)
        try:
            print(f"Fetching {url}")
            page = requests.get(url)
            return page
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
        return None

    @staticmethod
    def build_dataframe_from_rows(rows: List[Dict]) -> pd.DataFrame:
        """Build a pandas DataFrame from list of dict."""
        try:
            return pd.DataFrame(rows)
        except Exception:
            return pd.DataFrame.from_records(rows)

    @staticmethod
    def save_dataframe_as_csv(
        df: pd.DataFrame,
        name: str,
        outputs_dir: str,
    ) -> str:
        """Save DataFrame to outputs/{name}.csv, return the path.

        Creates the outputs directory if it doesn't exist. Raises the original
        exception on failure.
        """
        os.makedirs(outputs_dir, exist_ok=True)
        csv_path = os.path.join(outputs_dir, f"{name}.csv")
        df.to_csv(csv_path, index=False)
        return csv_path


if __name__ == "__main__":
    scraper = ChittorgarhScraper(config="./config.json")
    scraper.scrape()
