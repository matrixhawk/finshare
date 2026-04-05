import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestSentimentClient:
    def test_get_market_overview(self):
        from finshare.stock.sentiment.client import SentimentClient
        client = SentimentClient()
        mock_json = {"data": {"diff": [{"f104": 3200, "f105": 1500, "f106": 80, "f107": 10}]}}
        with patch.object(client, "_request", return_value=mock_json):
            df = client.get_market_overview()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "up_count" in df.columns
        assert "down_count" in df.columns
        assert "limit_up" in df.columns
        assert "limit_down" in df.columns

    def test_get_market_overview_empty(self):
        from finshare.stock.sentiment.client import SentimentClient
        client = SentimentClient()
        with patch.object(client, "_request", return_value=None):
            df = client.get_market_overview()
        assert len(df) == 0

    def test_get_margin_trading_summary(self):
        from finshare.stock.sentiment.client import SentimentClient
        client = SentimentClient()
        mock_json = {
            "result": {"data": [
                {"TRADE_DATE": "2026-04-04T00:00:00", "BUY_AMT": 50000000000,
                 "FIN_BALANCE": 1800000000000, "SL_SELL_VOL": 1000000,
                 "SL_BALANCE": 50000000000},
            ]}
        }
        with patch.object(client, "_request_datacenter", return_value=mock_json):
            df = client.get_margin_trading_summary()
        assert len(df) == 1
        assert "margin_buy" in df.columns
        assert "margin_balance" in df.columns
        assert df.iloc[0]["date"] == "2026-04-04"

    def test_get_margin_trading_summary_empty(self):
        from finshare.stock.sentiment.client import SentimentClient
        client = SentimentClient()
        with patch.object(client, "_request_datacenter", return_value=None):
            df = client.get_margin_trading_summary()
        assert len(df) == 0


class TestFearGreedCalculator:
    def _make_overview(self, **kwargs):
        defaults = {"date": "2026-04-04", "up_count": 3200, "down_count": 1500,
                     "limit_up": 80, "limit_down": 10,
                     "total_amount": 1_200_000_000_000, "avg_turnover": 1.5}
        defaults.update(kwargs)
        return pd.DataFrame([defaults])

    def _make_margin(self, **kwargs):
        defaults = {"date": "2026-04-04", "margin_buy": 50_000_000_000}
        defaults.update(kwargs)
        return pd.DataFrame([defaults])

    def test_calculate_with_full_data(self):
        from finshare.stock.sentiment.fear_greed import FearGreedCalculator
        calc = FearGreedCalculator()
        turnover_hist = [1.2] * 20
        result = calc.calculate(self._make_overview(), self._make_margin(), 5_000_000_000, turnover_hist)
        assert "index_value" in result
        assert "level" in result
        assert "components" in result
        assert 0 <= result["index_value"] <= 100
        assert result["warmup"] is False

    def test_cold_start_insufficient_data(self):
        from finshare.stock.sentiment.fear_greed import FearGreedCalculator
        calc = FearGreedCalculator()
        result = calc.calculate(self._make_overview(), self._make_margin(), 0.0, [1.2, 1.3])
        assert result["index_value"] is None
        assert result["level"] == "数据不足"

    def test_warmup_mode(self):
        from finshare.stock.sentiment.fear_greed import FearGreedCalculator
        calc = FearGreedCalculator()
        result = calc.calculate(self._make_overview(), self._make_margin(), 0.0, [1.2] * 10)
        assert result["warmup"] is True
        assert result["index_value"] is not None

    def test_level_classification(self):
        from finshare.stock.sentiment.fear_greed import FearGreedCalculator
        calc = FearGreedCalculator()
        assert calc._classify_level(10) == "极度恐惧"
        assert calc._classify_level(30) == "恐惧"
        assert calc._classify_level(50) == "中性"
        assert calc._classify_level(70) == "贪婪"
        assert calc._classify_level(90) == "极度贪婪"

    def test_extreme_values(self):
        from finshare.stock.sentiment.fear_greed import FearGreedCalculator
        calc = FearGreedCalculator()
        # Very bullish: all up, heavy north flow, high margin
        overview = self._make_overview(up_count=4500, down_count=200, limit_up=150, limit_down=2,
                                       total_amount=2_000_000_000_000, avg_turnover=3.0)
        margin = self._make_margin(margin_buy=200_000_000_000)
        result = calc.calculate(overview, margin, 20_000_000_000, [1.2] * 20)
        assert result["index_value"] >= 70  # should be greedy


class TestSentimentPublicAPI:
    def test_get_market_overview_function(self):
        from finshare.stock.sentiment import get_market_overview
        with patch("finshare.stock.sentiment._get_client") as mock_get:
            mock_client = MagicMock()
            mock_client.get_market_overview.return_value = pd.DataFrame({"up_count": [3200]})
            mock_get.return_value = mock_client
            df = get_market_overview()
            assert len(df) == 1

    def test_get_margin_trading_summary_function(self):
        from finshare.stock.sentiment import get_margin_trading_summary
        with patch("finshare.stock.sentiment._get_client") as mock_get:
            mock_client = MagicMock()
            mock_client.get_margin_trading_summary.return_value = pd.DataFrame({"margin_buy": [50e9]})
            mock_get.return_value = mock_client
            df = get_margin_trading_summary()
            assert len(df) == 1

    def test_get_fear_greed_index_function(self):
        from finshare.stock.sentiment import get_fear_greed_index
        overview = pd.DataFrame([{"up_count": 3200, "down_count": 1500, "limit_up": 80, "limit_down": 10,
                                   "total_amount": 1.2e12, "avg_turnover": 1.5}])
        margin = pd.DataFrame([{"margin_buy": 50e9}])
        result = get_fear_greed_index(overview, margin, 5e9, [1.2] * 20)
        assert isinstance(result, dict)
        assert 0 <= result["index_value"] <= 100
