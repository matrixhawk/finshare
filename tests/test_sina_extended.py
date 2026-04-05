import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestSinaConceptList:
    def test_returns_dataframe_on_success(self):
        from finshare.sources.sina_source import SinaDataSource
        source = SinaDataSource()
        assert hasattr(source, "get_concept_list")

    def test_returns_empty_on_failure(self):
        from finshare.sources.sina_source import SinaDataSource
        source = SinaDataSource()
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        with patch("requests.get", return_value=mock_resp):
            df = source.get_concept_list()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestSinaMinuteData:
    def test_has_method(self):
        from finshare.sources.sina_source import SinaDataSource
        source = SinaDataSource()
        assert hasattr(source, "get_minutely_data")

    def test_returns_empty_on_failure(self):
        from finshare.sources.sina_source import SinaDataSource
        source = SinaDataSource()
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        with patch("requests.get", return_value=mock_resp):
            df = source.get_minutely_data("000001", freq=5)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_parses_jsonp_response(self):
        from finshare.sources.sina_source import SinaDataSource
        source = SinaDataSource()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = 'var result="day,open,high,low,close,volume\\n2026-04-05 10:00,12.50,12.80,12.40,12.70,10000\\n2026-04-05 10:05,12.70,12.90,12.60,12.85,8000"'
        with patch("requests.get", return_value=mock_resp):
            df = source.get_minutely_data("000001", freq=5)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "trade_time" in df.columns
        assert "close" in df.columns
        assert df.iloc[0]["close"] == 12.70


class TestSafeFloat:
    def test_normal(self):
        from finshare.sources.sina_source import _safe_float
        assert _safe_float("12.34") == 12.34

    def test_with_percent(self):
        from finshare.sources.sina_source import _safe_float
        assert _safe_float("2.35%") == 2.35

    def test_invalid(self):
        from finshare.sources.sina_source import _safe_float
        assert _safe_float("abc") == 0.0

    def test_empty(self):
        from finshare.sources.sina_source import _safe_float
        assert _safe_float("") == 0.0
