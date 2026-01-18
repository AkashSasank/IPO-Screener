import time
import traceback
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright


class ChittorgarhFetcher:
    """
    Fetcher class to get data from Chittorgarh website using Playwright.
    1) Opens the main page to establish session/cookies.
    2) Builds paginated API URLs.
    3) Fetches data from each API URL with appropriate headers.
    4) Returns combined data from all pages.
    5) Introduces delay between requests to avoid rate limiting.
    6) Uses real browser User-Agent for requests.
    """

    @staticmethod
    def __build_page_urls(api_url: str, base_params: dict, n_pages: int) -> list[str]:
        """Build full URLs for pages 1..n with correct draw/start/_ mapping."""
        length = int(base_params.get("length", 10))
        urls = []

        for page_num in range(1, n_pages + 1):
            p = base_params.copy()
            p["draw"] = str(page_num)
            p["start"] = str((page_num - 1) * length)
            p["_"] = str(
                int(time.time() * 1000) + page_num
            )  # tiny bump to avoid duplicates

            qs = urlencode(p, doseq=True)
            urls.append(f"{api_url}?{qs}")

        return urls

    def fetch(
        self,
        page_url: str,
        api_url: str,
        params: dict,
        fields: list[str] = None,
        n_pages: int = 5,
        delay_s: float = 0.2,
    ):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            # Establish session/cookies
            page.goto(page_url, wait_until="networkidle")

            # Use real UA to match browser
            ua = page.evaluate("() => navigator.userAgent")

            headers = {
                "accept": "application/json, text/javascript, */*; q=0.01",
                "x-requested-with": "XMLHttpRequest",
                "referer": page_url,
                "user-agent": ua,
            }

            failed_url = []
            # 1) Build all URLs first

            print("Building URLs...")
            urls = self.__build_page_urls(api_url, params, n_pages)

            # 2) Call them
            all_rows = []
            for i, u in enumerate(urls, start=1):
                try:
                    resp = context.request.get(u, headers=headers)
                    print(f"Fetch page {i}: status={resp.status}")

                    if not resp.ok:
                        print(resp.text()[:500])
                        failed_url.append(u)
                        continue

                    payload = resp.json()
                    rows = payload.get("data", {})
                    if rows:
                        for data in rows:
                            if fields:
                                filtered_rows = {
                                    k: data[k] for k in fields if k in data
                                }
                                all_rows.append(filtered_rows)
                            else:
                                all_rows.append(data)
                    if delay_s:
                        time.sleep(delay_s)
                except Exception as e:
                    print(f"Failed to fetch page {i}: {e}")
                    failed_url.append(u)
                    traceback.print_exc()

            browser.close()

        return all_rows, failed_url


class IPOGmpTableFetcher:
    """
    Loads a page fully (JS + background XHR) and returns the final rendered HTML.
    Designed to avoid cross-origin frame access issues.
    """

    def __init__(
        self,
        headless: bool = True,
        wait_after_networkidle_ms: int = 4000,
        debug: bool = False,
    ) -> None:
        self.headless = headless
        self.wait_after_networkidle_ms = wait_after_networkidle_ms
        self.debug = debug

    def _log(self, *args) -> None:
        if self.debug:
            print("[PAGE-HTML]", *args)

    def extract_gmp(self, page_url: str, timeout_ms: int = 60000) -> str:
        """
        Loads the page and returns the final rendered HTML (after JS execution).

        Args:
            page_url: page to load
            timeout_ms: navigation timeout

        Returns:
            Rendered HTML as string
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/143.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                viewport={"width": 1366, "height": 768},
            )

            page = context.new_page()
            page.set_default_navigation_timeout(timeout_ms)

            self._log("Goto:", page_url)

            # 1) Load everything that the page itself triggers
            page.goto(page_url, wait_until="load")

            # 2) Many sites fire late XHRs after networkidle; give it a buffer
            if self.wait_after_networkidle_ms > 0:
                self._log("Extra wait(ms):", self.wait_after_networkidle_ms)
                page.wait_for_timeout(self.wait_after_networkidle_ms)

            # 3) Return the final DOM HTML
            html = page.content()
            browser.close()
            return html
