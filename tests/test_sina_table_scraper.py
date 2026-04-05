import pytest
from unittest.mock import MagicMock, patch


class TestSinaFinanceTableScraper:
    def test_init_stores_config(self):
        from finshare.sources.playwright.sina_table_scraper import SinaFinanceTableScraper

        scraper = SinaFinanceTableScraper(
            url="https://finance.sina.com.cn/stock/sl/",
            column_map={0: "code", 1: "name"},
            wait_selector="#dataTable",
        )
        assert scraper.url == "https://finance.sina.com.cn/stock/sl/"
        assert scraper.column_map == {0: "code", 1: "name"}
        assert scraper.wait_selector == "#dataTable"

    def test_extract_parses_table_rows(self):
        from finshare.sources.playwright.sina_table_scraper import SinaFinanceTableScraper

        scraper = SinaFinanceTableScraper(
            url="https://example.com",
            column_map={0: "industry", 1: "net_inflow"},
        )

        mock_page = MagicMock()

        mock_cell_0 = MagicMock()
        mock_cell_0.inner_text.return_value = "银行"
        mock_cell_1 = MagicMock()
        mock_cell_1.inner_text.return_value = "1234567890"

        mock_row = MagicMock()
        mock_row.query_selector_all.return_value = [mock_cell_0, mock_cell_1]

        mock_page.query_selector_all.return_value = [mock_row]

        result = scraper._extract(mock_page)
        assert len(result) == 1
        assert result[0] == {"industry": "银行", "net_inflow": "1234567890"}

    def test_extract_skips_rows_with_insufficient_cells(self):
        from finshare.sources.playwright.sina_table_scraper import SinaFinanceTableScraper

        scraper = SinaFinanceTableScraper(
            url="https://example.com",
            column_map={0: "a", 1: "b", 2: "c"},
        )

        mock_page = MagicMock()
        mock_cell = MagicMock()
        mock_cell.inner_text.return_value = "x"
        mock_row = MagicMock()
        mock_row.query_selector_all.return_value = [mock_cell]
        mock_page.query_selector_all.return_value = [mock_row]

        result = scraper._extract(mock_page)
        assert len(result) == 0

    def test_fetch_delegates_to_fetch_page(self):
        from finshare.sources.playwright.sina_table_scraper import SinaFinanceTableScraper

        scraper = SinaFinanceTableScraper(
            url="https://example.com",
            column_map={0: "code"},
        )

        with patch.object(scraper, "_fetch_page", return_value=[{"code": "001"}]) as mock_fp:
            result = scraper.fetch()
            mock_fp.assert_called_once_with("https://example.com", wait_selector="#dataTable")
            assert result == [{"code": "001"}]
