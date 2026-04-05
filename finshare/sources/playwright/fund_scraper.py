"""Fund web scrapers — NAV history and fund info from eastmoney."""
from __future__ import annotations
import logging
import re
from typing import Any, Optional
from finshare.sources.playwright.base_scraper import BaseScraper
from finshare.sources.playwright.browser_pool import get_page

logger = logging.getLogger(__name__)

class FundNavScraper(BaseScraper):
    source_name = "fund_nav_playwright"
    timeout = 20000

    def fetch(self, code: str, max_pages: int = 10) -> list[dict]:
        url = f"https://fund.eastmoney.com/f10/jjjz_{code}.html"
        all_navs = []
        try:
            with get_page(timeout=self.timeout) as page:
                page.goto(url, wait_until="networkidle", timeout=self.timeout)
                page.wait_for_selector("#jztable", timeout=10000)
                for _ in range(max_pages):
                    navs = self._extract_nav_table(page)
                    if not navs:
                        break
                    all_navs.extend(navs)
                    next_btn = page.query_selector("#pagebar a:has-text('下一页')")
                    if next_btn and "disabled" not in (next_btn.get_attribute("class") or ""):
                        next_btn.click()
                        page.wait_for_load_state("networkidle", timeout=10000)
                        page.wait_for_timeout(300)
                    else:
                        break
        except Exception as e:
            logger.warning(f"[FundNavScraper] scrape {code} failed after {len(all_navs)} rows: {e}")
        logger.info(f"[FundNavScraper] {code}: {len(all_navs)} NAV records")
        return all_navs

    def _extract_nav_table(self, page: Any) -> list[dict]:
        results = []
        rows = page.query_selector_all("#jztable table tbody tr")
        for row in rows:
            cells = row.query_selector_all("td")
            if len(cells) < 4:
                continue
            date_str = cells[0].inner_text().strip()
            nav = cells[1].inner_text().strip()
            total_nav = cells[2].inner_text().strip()
            change = cells[3].inner_text().strip()
            try:
                results.append({
                    "date": date_str,
                    "nav": float(nav),
                    "total_nav": float(total_nav),
                    "change_pct": float(change.replace("%", "")) if change and change != "—" else 0,
                })
            except (ValueError, TypeError):
                continue
        return results

    def _extract(self, page: Any) -> list[dict]:
        return self._extract_nav_table(page)

class FundInfoScraper(BaseScraper):
    source_name = "fund_info_playwright"
    timeout = 15000

    def fetch(self, code: str) -> Optional[dict]:
        url = f"https://fund.eastmoney.com/{code}.html"
        try:
            with get_page(timeout=self.timeout) as page:
                page.goto(url, wait_until="networkidle", timeout=self.timeout)
                page.wait_for_selector(".fundDetail-tit", timeout=10000)
                name_el = page.query_selector(".fundDetail-tit")
                name = name_el.inner_text().strip() if name_el else ""
                info = {"code": code, "name": name}
                info_rows = page.query_selector_all(".infoOfFund tr")
                for row in info_rows:
                    cells = row.query_selector_all("td")
                    for cell in cells:
                        text = cell.inner_text().strip()
                        if "类型" in text:
                            info["type"] = text.split("：")[-1].strip() if "：" in text else ""
                        elif "规模" in text:
                            match = re.search(r"([\d.]+)亿", text)
                            if match:
                                info["size"] = float(match.group(1))
                        elif "管理人" in text:
                            info["manager"] = text.split("：")[-1].strip() if "：" in text else ""
                return info
        except Exception as e:
            logger.warning(f"[FundInfoScraper] scrape {code} failed: {e}")
            return None

    def _extract(self, page: Any) -> list[dict]:
        return []
