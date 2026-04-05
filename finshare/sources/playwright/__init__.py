"""Playwright-based web scraping — last-resort fallback when all API sources fail."""

from finshare.sources.playwright.browser_pool import get_page, is_available
from finshare.sources.playwright.eastmoney_table_scraper import EastMoneyTableScraper
from finshare.sources.playwright.sina_table_scraper import SinaFinanceTableScraper

__all__ = ["get_page", "is_available", "EastMoneyTableScraper", "SinaFinanceTableScraper"]
