import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestConceptClient:
    def test_get_concept_list_returns_dataframe(self):
        from finshare.stock.concept.client import ConceptClient
        client = ConceptClient()
        mock_response = {
            "data": {"diff": [
                {"f12": "BK0493", "f14": "新能源", "f3": 2.35, "f62": 1000000, "f184": 5.2},
                {"f12": "BK0655", "f14": "芯片", "f3": -1.20, "f62": -500000, "f184": -2.1},
            ]}
        }
        with patch.object(client, "_request", return_value=mock_response):
            df = client.get_concept_list()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "board_code" in df.columns
        assert "board_name" in df.columns
        assert "change_pct" in df.columns
        assert "net_inflow" in df.columns
        assert "net_inflow_ratio" in df.columns
        assert df.iloc[0]["board_name"] == "新能源"

    def test_get_concept_constituents_returns_dataframe(self):
        from finshare.stock.concept.client import ConceptClient
        client = ConceptClient()
        mock_response = {
            "data": {"diff": [
                {"f12": "000001", "f14": "平安银行"},
                {"f12": "600036", "f14": "招商银行"},
            ]}
        }
        with patch.object(client, "_request", return_value=mock_response):
            df = client.get_concept_constituents("BK0493")
        assert len(df) == 2
        assert "fs_code" in df.columns
        assert df.iloc[1]["fs_code"] == "600036.SH"

    def test_get_concept_money_flow_returns_dataframe(self):
        from finshare.stock.concept.client import ConceptClient
        client = ConceptClient()
        mock_response = {
            "data": {"diff": [
                {"f14": "新能源", "f62": 5000000, "f184": 3.2, "f3": 1.5},
            ]}
        }
        with patch.object(client, "_request", return_value=mock_response):
            df = client.get_concept_money_flow()
        assert len(df) == 1
        assert "concept" in df.columns
        assert "net_inflow" in df.columns

    def test_get_concept_list_returns_empty_on_failure(self):
        from finshare.stock.concept.client import ConceptClient
        client = ConceptClient()
        with patch.object(client, "_request", return_value=None):
            df = client.get_concept_list()
        assert len(df) == 0

    def test_caching(self):
        from finshare.stock.concept.client import ConceptClient
        client = ConceptClient()
        mock_response = {
            "data": {"diff": [{"f12": "BK0001", "f14": "测试", "f3": 1.0, "f62": 100, "f184": 0.5}]}
        }
        with patch.object(client, "_request", return_value=mock_response) as mock_req:
            client.get_concept_list()
            client.get_concept_list()
            assert mock_req.call_count == 1


class TestConceptPublicAPI:
    def test_get_concept_list_function(self):
        from finshare.stock.concept import get_concept_list
        with patch("finshare.stock.concept._get_client") as mock_get:
            mock_client = MagicMock()
            mock_client.get_concept_list.return_value = pd.DataFrame({"board_name": ["AI"]})
            mock_get.return_value = mock_client
            df = get_concept_list()
            assert len(df) == 1

    def test_get_concept_constituents_function(self):
        from finshare.stock.concept import get_concept_constituents
        with patch("finshare.stock.concept._get_client") as mock_get:
            mock_client = MagicMock()
            mock_client.get_concept_constituents.return_value = pd.DataFrame({"fs_code": ["000001.SZ"]})
            mock_get.return_value = mock_client
            df = get_concept_constituents("BK0493")
            assert len(df) == 1

    def test_get_concept_money_flow_function(self):
        from finshare.stock.concept import get_concept_money_flow
        with patch("finshare.stock.concept._get_client") as mock_get:
            mock_client = MagicMock()
            mock_client.get_concept_money_flow.return_value = pd.DataFrame({"concept": ["AI"]})
            mock_get.return_value = mock_client
            df = get_concept_money_flow()
            assert len(df) == 1
