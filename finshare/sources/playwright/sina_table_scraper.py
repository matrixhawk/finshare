"""配置驱动的新浪财经通用表格抓取器。"""

from __future__ import annotations

import logging
from typing import Any, Optional

from finshare.sources.playwright.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class SinaFinanceTableScraper(BaseScraper):
    """配置驱动的新浪财经表格抓取器。"""

    source_name = "playwright_sina"

    def __init__(
        self,
        url: str,
        column_map: dict[int, str],
        wait_selector: str = "#dataTable",
        row_selector: str = "#dataTable tbody tr",
        cell_selector: str = "td",
        next_selector: Optional[str] = None,
        timeout: int = 30000,
    ):
        self.url = url
        self.column_map = column_map
        self.wait_selector = wait_selector
        self.row_selector = row_selector
        self.cell_selector = cell_selector
        self.next_selector = next_selector
        self.timeout = timeout

    def fetch(self, max_pages: int = 10) -> list[dict]:
        if self.next_selector:
            return self._fetch_paginated(
                self.url,
                wait_selector=self.wait_selector,
                next_selector=self.next_selector,
                max_pages=max_pages,
            )
        return self._fetch_page(self.url, wait_selector=self.wait_selector)

    def _extract(self, page: Any) -> list[dict]:
        rows = page.query_selector_all(self.row_selector)
        max_col_index = max(self.column_map.keys()) if self.column_map else 0
        results = []

        for row in rows:
            cells = row.query_selector_all(self.cell_selector)
            if len(cells) <= max_col_index:
                continue

            record = {}
            for col_index, field_name in self.column_map.items():
                text = cells[col_index].inner_text().strip()
                record[field_name] = text
            results.append(record)

        return results
