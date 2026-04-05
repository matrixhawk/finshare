"""Integration test: verify all Plan B public APIs exist and return correct types."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestPlanBPublicAPI:
    """Verify all new public functions are importable and callable."""

    def test_concept_list_importable(self):
        from finshare.stock.concept import get_concept_list
        assert callable(get_concept_list)

    def test_concept_constituents_importable(self):
        from finshare.stock.concept import get_concept_constituents
        assert callable(get_concept_constituents)

    def test_concept_money_flow_importable(self):
        from finshare.stock.concept import get_concept_money_flow
        assert callable(get_concept_money_flow)

    def test_earnings_calendar_importable(self):
        from finshare.stock.earnings import get_earnings_calendar
        assert callable(get_earnings_calendar)

    def test_earnings_preannouncement_importable(self):
        from finshare.stock.earnings import get_earnings_preannouncement
        assert callable(get_earnings_preannouncement)

    def test_market_overview_importable(self):
        from finshare.stock.sentiment import get_market_overview
        assert callable(get_market_overview)

    def test_margin_trading_summary_importable(self):
        from finshare.stock.sentiment import get_margin_trading_summary
        assert callable(get_margin_trading_summary)

    def test_fear_greed_index_importable(self):
        from finshare.stock.sentiment import get_fear_greed_index
        assert callable(get_fear_greed_index)


class TestFearGreedEndToEnd:
    """End-to-end test: overview + margin + north flow → fear/greed index."""

    def test_full_pipeline(self):
        from finshare.stock.sentiment import get_fear_greed_index

        overview = pd.DataFrame([{
            "date": "2026-04-04", "up_count": 3200, "down_count": 1500,
            "limit_up": 80, "limit_down": 10,
            "total_amount": 1_200_000_000_000, "avg_turnover": 1.5,
        }])
        margin = pd.DataFrame([{"date": "2026-04-04", "margin_buy": 50_000_000_000}])
        turnover_hist = [1.2] * 20

        result = get_fear_greed_index(overview, margin, 5_000_000_000, turnover_hist)
        assert isinstance(result, dict)
        assert 0 <= result["index_value"] <= 100
        assert result["level"] in ["极度恐惧", "恐惧", "中性", "贪婪", "极度贪婪"]
        assert "components" in result
        assert result["warmup"] is False

    def test_cold_start(self):
        from finshare.stock.sentiment import get_fear_greed_index

        overview = pd.DataFrame([{
            "date": "2026-04-04", "up_count": 3200, "down_count": 1500,
            "limit_up": 80, "limit_down": 10,
            "total_amount": 1_200_000_000_000, "avg_turnover": 1.5,
        }])
        margin = pd.DataFrame([{"date": "2026-04-04", "margin_buy": 50_000_000_000}])

        result = get_fear_greed_index(overview, margin, 0, [1.2, 1.3])
        assert result["index_value"] is None
        assert result["level"] == "数据不足"


class TestSinaExtendedMethods:
    """Verify Sina has the new methods."""

    def test_sina_has_concept_list(self):
        from finshare.sources.sina_source import SinaDataSource
        source = SinaDataSource()
        assert hasattr(source, "get_concept_list")

    def test_sina_has_minutely_data(self):
        from finshare.sources.sina_source import SinaDataSource
        source = SinaDataSource()
        assert hasattr(source, "get_minutely_data")


class TestEastMoneyExtendedMethods:
    """Verify EastMoney has all new methods."""

    def test_eastmoney_has_concept_list(self):
        from finshare.sources.eastmoney_source import EastMoneyDataSource
        source = EastMoneyDataSource()
        assert hasattr(source, "get_concept_list")

    def test_eastmoney_has_money_flow_stock(self):
        from finshare.sources.eastmoney_source import EastMoneyDataSource
        source = EastMoneyDataSource()
        assert hasattr(source, "get_money_flow_stock")

    def test_eastmoney_has_earnings_calendar(self):
        from finshare.sources.eastmoney_source import EastMoneyDataSource
        source = EastMoneyDataSource()
        assert hasattr(source, "get_earnings_calendar")

    def test_eastmoney_has_market_overview(self):
        from finshare.sources.eastmoney_source import EastMoneyDataSource
        source = EastMoneyDataSource()
        assert hasattr(source, "get_market_overview")

    def test_eastmoney_has_margin_summary(self):
        from finshare.sources.eastmoney_source import EastMoneyDataSource
        source = EastMoneyDataSource()
        assert hasattr(source, "get_margin_trading_summary")
