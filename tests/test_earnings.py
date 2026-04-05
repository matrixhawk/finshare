import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestEarningsClient:
    def test_get_earnings_calendar(self):
        from finshare.stock.earnings.client import EarningsClient
        client = EarningsClient()
        mock_response = {
            "result": {
                "data": [
                    {"SECURITY_CODE": "000001", "SECURITY_NAME_ABBR": "平安银行",
                     "REPORT_DATE": "2026-03-31", "REPORT_TYPE": "一季报",
                     "UPDATE_DATE": "2026-04-15"},
                ]
            }
        }
        with patch.object(client, "_request", return_value=mock_response):
            df = client.get_earnings_calendar("2026-04-15")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "code" in df.columns
        assert "name" in df.columns
        assert "report_date" in df.columns
        assert "report_type" in df.columns
        assert df.iloc[0]["code"] == "000001"

    def test_get_earnings_preannouncement(self):
        from finshare.stock.earnings.client import EarningsClient
        client = EarningsClient()
        mock_response = {
            "result": {
                "data": [
                    {"REPORT_DATE": "2026-03-31", "PREDICT_TYPE": "预增",
                     "PREDICT_CONTENT": "净利润同比增长50%-80%",
                     "NOTICE_DATE": "2026-01-20"},
                ]
            }
        }
        with patch.object(client, "_request", return_value=mock_response):
            df = client.get_earnings_preannouncement("000001")
        assert len(df) == 1
        assert "pre_type" in df.columns
        assert df.iloc[0]["pre_type"] == "预增"

    def test_empty_response_calendar(self):
        from finshare.stock.earnings.client import EarningsClient
        client = EarningsClient()
        with patch.object(client, "_request", return_value=None):
            df = client.get_earnings_calendar("2026-04-15")
        assert len(df) == 0

    def test_empty_response_preannouncement(self):
        from finshare.stock.earnings.client import EarningsClient
        client = EarningsClient()
        with patch.object(client, "_request", return_value=None):
            df = client.get_earnings_preannouncement("000001")
        assert len(df) == 0


class TestEarningsPublicAPI:
    def test_get_earnings_calendar_function(self):
        from finshare.stock.earnings import get_earnings_calendar
        with patch("finshare.stock.earnings._get_client") as mock_get:
            mock_client = MagicMock()
            mock_client.get_earnings_calendar.return_value = pd.DataFrame({"code": ["000001"]})
            mock_get.return_value = mock_client
            df = get_earnings_calendar("2026-04-15")
            assert len(df) == 1

    def test_get_earnings_preannouncement_function(self):
        from finshare.stock.earnings import get_earnings_preannouncement
        with patch("finshare.stock.earnings._get_client") as mock_get:
            mock_client = MagicMock()
            mock_client.get_earnings_preannouncement.return_value = pd.DataFrame({"pre_type": ["预增"]})
            mock_get.return_value = mock_client
            df = get_earnings_preannouncement("000001")
            assert len(df) == 1
