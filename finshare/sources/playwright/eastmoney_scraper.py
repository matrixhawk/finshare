"""EastMoney web scrapers — snapshot and stock list fallback."""
from __future__ import annotations
import logging
import re
from typing import Any, Optional
from finshare.sources.playwright.base_scraper import BaseScraper
from finshare.sources.playwright.browser_pool import get_page

logger = logging.getLogger(__name__)

class EastMoneySnapshotScraper(BaseScraper):
    source_name = "eastmoney_playwright"
    timeout = 15000

    def fetch(self, code: str) -> Optional[dict]:
        prefix = "sh" if code.startswith("6") else "sz"
        url = f"https://quote.eastmoney.com/{prefix}{code}.html"
        try:
            with get_page(timeout=self.timeout) as page:
                page.goto(url, wait_until="networkidle", timeout=self.timeout)
                page.wait_for_selector("#price9", timeout=10000)
                return self._extract_snapshot(page, code)
        except Exception as e:
            logger.warning(f"[EastMoneySnapshot] scrape {code} failed: {e}")
            return None

    def _extract_snapshot(self, page: Any, code: str) -> dict:
        def _text(sel):
            el = page.query_selector(sel)
            return el.inner_text().strip() if el else ""
        def _float(sel):
            t = _text(sel).replace(",", "").replace("%", "")
            try: return float(t)
            except (ValueError, TypeError): return 0.0

        return {
            "code": code, "name": _text("#name"),
            "price": _float("#price9"), "change": _float("#km1"),
            "change_pct": _float("#km2"), "open": _float("#gt1"),
            "high": _float("#zt1"), "low": _float("#zd1"),
            "prev_close": _float("#zrsp1"),
            "volume": _float("#lt1"), "amount": _float("#cjje"),
        }

    def _extract(self, page: Any) -> list[dict]:
        return []

class EastMoneyStockListScraper(BaseScraper):
    source_name = "eastmoney_list_playwright"
    timeout = 30000
    URL = "https://quote.eastmoney.com/center/gridlist.html#hs_a_board"
    WAIT_SELECTOR = "table.table_wrapper"
    NEXT_SELECTOR = "a.next"

    def fetch(self) -> list[dict]:
        return self._fetch_paginated(self.URL, self.WAIT_SELECTOR, self.NEXT_SELECTOR, max_pages=100)

    def _extract(self, page: Any) -> list[dict]:
        results = []
        rows = page.query_selector_all("table.table_wrapper tbody tr")
        for row in rows:
            cells = row.query_selector_all("td")
            if len(cells) < 3:
                continue
            code = cells[1].inner_text().strip()
            name = cells[2].inner_text().strip()
            if code and re.match(r"^\d{6}$", code):
                exchange = "SH" if code.startswith("6") else "SZ"
                results.append({"code": code, "name": name, "exchange": exchange, "sec_type": "stock"})
        return results
