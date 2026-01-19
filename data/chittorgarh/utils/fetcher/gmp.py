import random
import re
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote, urlparse

from playwright.sync_api import sync_playwright


class IPOGmpTableFetcher:
    """
    Resolve redirect → extract ipo_id from Investorgain URL →
    call webnodejs GMP API with rate-limit aware retries.
    """

    GMP_API_TEMPLATE = (
        "https://webnodejs.investorgain.com/cloud/ipo/ipo-gmp-read/{ipo_id}/true?v={v}"
    )

    def __init__(
        self,
        headless: bool = True,
        wait_after_load_ms: int = 500,
        debug: bool = False,
        # Rate-limit / retry controls
        max_retries: int = 6,
        base_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
        min_gap_between_calls_s: float = 0.4,  # local throttle (per instance)
    ) -> None:
        self.headless = headless
        self.wait_after_load_ms = wait_after_load_ms
        self.debug = debug

        self.max_retries = max_retries
        self.base_backoff_s = base_backoff_s
        self.max_backoff_s = max_backoff_s
        self.min_gap_between_calls_s = min_gap_between_calls_s

        self._last_api_call_ts: float = 0.0

    def _log(self, *args) -> None:
        if self.debug:
            print("[GMP-FETCH]", *args)

    @staticmethod
    def _extract_ipo_id_from_url(url: str) -> str:
        p = urlparse(url)
        path = unquote(p.path or "")
        segs = [s for s in path.split("/") if s]

        for s in reversed(segs):
            if s.isdigit():
                return s

        m = re.search(r"(\d+)(?:/)?$", path)
        if m:
            return m.group(1)

        raise ValueError(f"Could not extract ipo_id from url={url} path={path}")

    @staticmethod
    def _build_v() -> str:
        return datetime.now().strftime("%H-%M")

    def _throttle(self) -> None:
        """
        Simple per-instance throttle so you don't spam the API even with no retries.
        If you have threads, you should ALSO implement a shared limiter outside this class.
        """
        now = time.time()
        gap = now - self._last_api_call_ts
        if gap < self.min_gap_between_calls_s:
            sleep_s = self.min_gap_between_calls_s - gap
            time.sleep(sleep_s)
        self._last_api_call_ts = time.time()

    def _api_get_with_retry(
        self,
        context,
        api_url: str,
        referer_url: str,
        timeout_ms: int,
    ) -> Dict[str, Any]:
        """
        Retries on:
          - HTTP 429
          - transient 5xx
          - JSON payloads that include ['msg','error'] instead of data
        """
        headers = {
            "accept": "application/json, text/plain, */*",
            "origin": "https://www.investorgain.com",
            # IMPORTANT: use the actual page as referer, not just homepage
            "referer": referer_url,
        }

        for attempt in range(self.max_retries + 1):
            self._throttle()

            resp = context.request.get(api_url, headers=headers, timeout=timeout_ms)

            status = resp.status
            retry_after = resp.headers.get("retry-after")

            # Try parse JSON no matter what; some errors come as JSON with 200 too
            data: Optional[Dict[str, Any]] = None
            try:
                data = resp.json()
            except Exception:
                data = None

            # Success shape
            if resp.ok and isinstance(data, dict) and "ipoGmpTable" in data:
                return data

            # Detect error-shape JSON (your second error case)
            api_error = None
            if isinstance(data, dict) and "error" in data and "msg" in data:
                api_error = str(data.get("error"))

            should_retry = False

            # Hard rate-limit
            if status == 429:
                should_retry = True

            # Transient server errors
            if 500 <= status <= 599:
                should_retry = True

            # Sometimes they return 200 but with {msg:0,error:"..."} or missing table
            if resp.ok and api_error:
                # Often rate limit / WAF messages land here
                should_retry = True

            if not should_retry:
                # Fail fast with helpful detail
                if isinstance(data, dict):
                    raise RuntimeError(
                        f"GMP API unexpected payload (status={status}). "
                        f"Keys={list(data.keys())} url={api_url}"
                    )
                raise RuntimeError(
                    f"GMP API failed (status={status}) and non-JSON response. url={api_url}"
                )

            # Compute backoff
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except Exception:
                    sleep_s = self.base_backoff_s
            else:
                # exponential backoff + jitter
                sleep_s = min(self.base_backoff_s * (2**attempt), self.max_backoff_s)
                sleep_s = sleep_s * (0.7 + random.random() * 0.6)  # jitter 0.7x..1.3x

            self._log(
                f"Retrying attempt={attempt+1}/{self.max_retries} "
                f"status={status} api_error={api_error} sleep={sleep_s:.2f}s"
            )
            time.sleep(sleep_s)

        # If we exit loop, retries exhausted
        if isinstance(data, dict):
            raise RuntimeError(
                f"GMP API retries exhausted. last_status={status} keys={list(data.keys())} url={api_url}"
            )
        raise RuntimeError(
            f"GMP API retries exhausted. last_status={status} url={api_url}"
        )

    def fetch_gmp_table(self, page_url: str, timeout_ms: int = 60000) -> str:
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
            page.goto(page_url, wait_until="domcontentloaded")

            if self.wait_after_load_ms:
                page.wait_for_timeout(self.wait_after_load_ms)

            final_url = page.url
            self._log("Final URL:", final_url)

            # Prefer final URL, fallback to input URL (in case of CF challenge)
            try:
                ipo_id = self._extract_ipo_id_from_url(final_url)
            except ValueError:
                ipo_id = self._extract_ipo_id_from_url(page_url)

            v = self._build_v()
            api_url = self.GMP_API_TEMPLATE.format(ipo_id=ipo_id, v=v)

            self._log("ipo_id:", ipo_id)
            self._log("API URL:", api_url)

            data = self._api_get_with_retry(
                context=context,
                api_url=api_url,
                referer_url=final_url if "investorgain.com" in final_url else page_url,
                timeout_ms=timeout_ms,
            )

            browser.close()
            return data["ipoGmpTable"]
