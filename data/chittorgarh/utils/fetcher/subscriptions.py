import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


@dataclass
class FetchResult:
    url: str
    final_url: str
    status: Optional[int]
    html: str
    title: str
    captured_json: List[Tuple[str, Any]]


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

    @staticmethod
    def _looks_like_json(text: str) -> bool:
        t = text.lstrip()
        return t.startswith("{") or t.startswith("[")

    def fetch(
        self,
        url: str,
        wait_selector: Optional[str] = None,
        wait_text_regex: Optional[str] = None,
        extra_wait_ms: int = 500,
        retries: int = 2,
        # NEW: make “data loaded” explicit
        wait_for_table: bool = True,
        table_min_rows: int = 1,
        placeholder_token: Optional[str] = "[●]",
        subscription_panel_selector: str = "div.panel-box",
        table_selector: str = "div.panel-box table",
        auto_scroll: bool = True,
    ) -> FetchResult:
        """
        Robust strategy:
        1) goto(url)
        2) wait for DOM + JS settle
        3) wait for *data-ready* signal (table rows OR placeholder gone)
        4) return page.content()
        """

        last_err: Optional[Exception] = None

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                slow_mo=self.slow_mo_ms,
                # If you face bot-detection, these flags often help (not a silver bullet):
                args=["--disable-blink-features=AutomationControlled"],
            )

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

            # Speed: block non-essential assets (keep scripts + xhr!)
            if self.block_resources:

                def route_handler(route, request):
                    rtype = request.resource_type
                    if rtype in ("image", "media", "font"):
                        return route.abort()
                    return route.continue_()

                page.route("**/*", route_handler)

            captured_json: List[Tuple[str, Any]] = []

            # Log XHR/fetch errors (very useful for “table is empty” debugging)
            def on_request_failed(req):
                if req.resource_type in ("xhr", "fetch"):
                    self._log("XHR FAILED:", req.method, req.url, "->", req.failure)

            page.on("requestfailed", on_request_failed)

            def on_response(resp):
                # Capture JSON-ish payloads from XHR/fetch
                if not self.capture_json:
                    return
                try:
                    if resp.request.resource_type not in ("xhr", "fetch"):
                        return

                    ct = (resp.headers.get("content-type") or "").lower()

                    # Some sites send JSON as text/plain or even text/html
                    if "application/json" in ct:
                        data = resp.json()
                        captured_json.append((resp.url, data))
                        self._log("captured json:", resp.status, resp.url)
                        return

                    # Fallback: attempt to parse “JSON-looking” text
                    txt = resp.text()
                    if self._looks_like_json(txt):
                        try:
                            data = resp.json()  # playwright will parse if possible
                        except Exception:
                            # last resort: keep the raw text
                            data = txt
                        captured_json.append((resp.url, data))
                        self._log("captured json-ish:", resp.status, resp.url)
                except Exception:
                    pass

            page.on("response", on_response)

            for attempt in range(retries + 1):
                try:
                    self._log(f"goto attempt {attempt+1}/{retries+1}:", url)

                    resp = page.goto(url, wait_until="domcontentloaded")

                    # Let JS settle (don’t rely solely on networkidle)
                    try:
                        page.wait_for_load_state("networkidle", timeout=12_000)
                    except PlaywrightTimeoutError:
                        self._log("networkidle timeout (soft-ignored)")

                    # Optional: user-provided waits
                    if wait_selector:
                        self._log("waiting selector:", wait_selector)
                        page.wait_for_selector(wait_selector, state="visible")

                    if wait_text_regex:
                        self._log("waiting text regex:", wait_text_regex)
                        page.wait_for_function(
                            """(pattern) => new RegExp(pattern, 'i').test(document.body.innerText)""",
                            wait_text_regex,
                        )

                    # Auto-scroll helps if the table loads on viewport / intersection observer
                    if auto_scroll:
                        page.evaluate(
                            """() => new Promise(resolve => {
                                let y = 0;
                                const step = 600;
                                const max = Math.max(document.body.scrollHeight, 3000);
                                const timer = setInterval(() => {
                                    window.scrollTo(0, y);
                                    y += step;
                                    if (y >= max) {
                                        clearInterval(timer);
                                        window.scrollTo(0, 0);
                                        resolve(true);
                                    }
                                }, 120);
                            })"""
                        )

                    # NEW: Wait until the table is truly populated / placeholders replaced
                    if wait_for_table:
                        self._log("waiting for table data to appear...")
                        page.wait_for_function(
                            """({tableSel, panelSel, minRows, token}) => {
                                const tbl = document.querySelector(tableSel);
                                const panel = document.querySelector(panelSel);

                                // Condition A: tbody rows exist and have non-empty text
                                if (tbl) {
                                    const rows = tbl.querySelectorAll("tbody tr");
                                    if (rows && rows.length >= minRows) {
                                        // ensure not just empty <td>
                                        const hasText = Array.from(rows).some(r => (r.innerText || "").trim().length > 0);
                                        if (hasText) return true;
                                    }
                                }

                                // Condition B: placeholder token removed from the subscription panel
                                if (token && panel) {
                                    const t = (panel.innerText || "");
                                    if (t && !t.includes(token)) return true;
                                }

                                return false;
                            }""",
                            {
                                "tableSel": table_selector,
                                "panelSel": subscription_panel_selector,
                                "minRows": table_min_rows,
                                "token": placeholder_token,
                            },
                            timeout=self.timeout_ms,
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

                    # Backoff increases odds of surviving rate-limit / transient failures
                    time.sleep(2.0 + attempt * 1.25)

                    # On retry, a clean reload can help
                    try:
                        page.goto("about:blank")
                    except Exception:
                        pass

            context.close()
            browser.close()
            raise RuntimeError(
                f"Failed to fetch after retries: {url}. Last error: {last_err!r}"
            )
