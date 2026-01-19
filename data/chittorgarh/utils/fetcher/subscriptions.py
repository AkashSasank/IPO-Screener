import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


@dataclass
class FetchResult:
    url: str
    final_url: str
    status: Optional[int]
    html: str
    title: str
    captured_json: List[Tuple[str, dict]]  # (url, json)


class SubscriptionFetcher:
    """
    Loads a JS-heavy page and returns fully rendered HTML.
    Also optionally captures JSON/XHR responses for debugging or API extraction.
    """

    def __init__(
        self,
        headless: bool = True,
        timeout_ms: int = 45_000,
        slow_mo_ms: int = 0,
        locale: str = "en-US",
        timezone_id: str = "Asia/Kolkata",
        user_agent: Optional[str] = None,
        block_resources: bool = True,
        capture_json: bool = True,
        debug: bool = False,
    ) -> None:
        self.headless = headless
        self.timeout_ms = timeout_ms
        self.slow_mo_ms = slow_mo_ms
        self.locale = locale
        self.timezone_id = timezone_id
        self.user_agent = user_agent
        self.block_resources = block_resources
        self.capture_json = capture_json
        self.debug = debug

    def _log(self, *args) -> None:
        if self.debug:
            print("[PW-FETCHER]", *args)

    def fetch(
        self,
        url: str,
        wait_selector: Optional[str] = None,
        wait_text_regex: Optional[str] = None,
        extra_wait_ms: int = 1_000,
        retries: int = 2,
    ) -> FetchResult:
        """
        Strategy:
        1) goto(url) with domcontentloaded
        2) wait for network to go idle-ish
        3) optional: wait for selector or text
        4) small extra wait (for late hydration)
        5) return page.content()
        """
        last_err: Optional[Exception] = None

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless, slow_mo=self.slow_mo_ms)

            # NOTE: if you need a persistent session/cookies, switch to launch_persistent_context
            context_kwargs = dict(
                locale=self.locale,
                timezone_id=self.timezone_id,
                viewport={"width": 1366, "height": 768},
            )
            if self.user_agent:
                context_kwargs["user_agent"] = self.user_agent

            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            page.set_default_timeout(self.timeout_ms)

            # Optional: block images/fonts to speed up. For HTML extraction, you usually don’t need them.
            if self.block_resources:

                def route_handler(route, request):
                    rtype = request.resource_type
                    if rtype in ("image", "media", "font"):
                        return route.abort()
                    return route.continue_()

                page.route("**/*", route_handler)

            captured_json: List[Tuple[str, dict]] = []

            # Optional: capture JSON responses from XHR/fetch
            if self.capture_json:

                def on_response(resp):
                    try:
                        ct = (resp.headers.get("content-type") or "").lower()
                        if "application/json" in ct:
                            data = resp.json()
                            captured_json.append((resp.url, data))
                            self._log("captured json:", resp.url)
                    except Exception:
                        # ignore parse/cors/stream errors
                        pass

                page.on("response", on_response)

            for attempt in range(retries + 1):
                try:
                    self._log(f"goto attempt {attempt+1}/{retries+1}:", url)
                    resp = page.goto(url, wait_until="domcontentloaded")

                    # Some sites do lots of background calls; "networkidle" can be too strict.
                    # We'll do a soft settle: wait a bit and also try networkidle with a short timeout.
                    try:
                        page.wait_for_load_state("networkidle", timeout=10_000)
                    except PlaywrightTimeoutError:
                        self._log("networkidle timeout (soft-ignored)")

                    if wait_selector:
                        self._log("waiting selector:", wait_selector)
                        page.wait_for_selector(wait_selector, state="visible")
                    if wait_text_regex:
                        self._log("waiting text regex:", wait_text_regex)
                        rx = re.compile(wait_text_regex, re.IGNORECASE)
                        page.wait_for_function(
                            """(pattern) => new RegExp(pattern, 'i').test(document.body.innerText)""",
                            wait_text_regex,
                        )

                    if extra_wait_ms:
                        page.wait_for_timeout(extra_wait_ms)

                    html = page.content()
                    title = page.title()
                    final_url = page.url
                    status = resp.status if resp else None

                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        html=html,
                        title=title,
                        captured_json=captured_json,
                    )

                except Exception as e:
                    last_err = e
                    self._log("error:", repr(e))
                    # small backoff
                    time.sleep(1.0 + attempt * 0.75)

            # If we exhausted retries
            context.close()
            browser.close()
            raise RuntimeError(
                f"Failed to fetch after retries: {url}. Last error: {last_err!r}"
            )
