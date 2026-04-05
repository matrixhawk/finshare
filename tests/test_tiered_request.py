import pandas as pd
from unittest.mock import MagicMock, patch
from finshare.sources.resilience.smart_router import DataType, SourceType, SourceTier, SourcePreference


class TestTieredRequest:
    def setup_method(self):
        from finshare.sources.manager import DataSourceManager
        self.manager = DataSourceManager.__new__(DataSourceManager)
        self.manager.sources = {}
        self.manager.source_status = {}
        self.manager._playwright_sources = {}

    def test_tiered_request_returns_from_first_api_source(self):
        mock_source = MagicMock()
        mock_source.get_concept_list.return_value = pd.DataFrame({"board_name": ["新能源"]})
        self.manager.sources["eastmoney"] = mock_source

        prefs_api = [SourcePreference(source=SourceType.EASTMONEY, priority=1, timeout=10.0, tier=SourceTier.API)]
        prefs_scraper = []

        with patch("finshare.sources.manager.get_router") as mock_router:
            mock_router.return_value.get_tiered_sources.return_value = (prefs_api, prefs_scraper)
            result = self.manager._tiered_request(
                data_type=DataType.CONCEPT_LIST,
                method_name="get_concept_list",
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_tiered_request_falls_through_to_scraper_on_api_failure(self):
        mock_api_source = MagicMock()
        mock_api_source.get_concept_list.side_effect = Exception("API down")
        self.manager.sources["eastmoney"] = mock_api_source

        mock_pw_source = MagicMock()
        mock_pw_source.get_concept_list.return_value = pd.DataFrame({"board_name": ["芯片"]})
        mock_pw_source.is_available.return_value = True
        self.manager._playwright_sources["playwright_eastmoney"] = mock_pw_source

        prefs_api = [SourcePreference(source=SourceType.EASTMONEY, priority=1, timeout=10.0, tier=SourceTier.API)]
        prefs_scraper = [SourcePreference(source=SourceType.PLAYWRIGHT_EASTMONEY, priority=10, timeout=30.0, tier=SourceTier.SCRAPER)]

        with patch("finshare.sources.manager.get_router") as mock_router:
            mock_router.return_value.get_tiered_sources.return_value = (prefs_api, prefs_scraper)
            result = self.manager._tiered_request(
                data_type=DataType.CONCEPT_LIST,
                method_name="get_concept_list",
            )

        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0]["board_name"] == "芯片"

    def test_tiered_request_returns_none_when_all_fail(self):
        mock_source = MagicMock()
        mock_source.get_concept_list.side_effect = Exception("fail")
        self.manager.sources["eastmoney"] = mock_source

        prefs_api = [SourcePreference(source=SourceType.EASTMONEY, priority=1, timeout=10.0, tier=SourceTier.API)]
        prefs_scraper = []

        with patch("finshare.sources.manager.get_router") as mock_router:
            mock_router.return_value.get_tiered_sources.return_value = (prefs_api, prefs_scraper)
            result = self.manager._tiered_request(
                data_type=DataType.CONCEPT_LIST,
                method_name="get_concept_list",
            )

        assert result is None

    def test_tiered_request_passes_args_and_kwargs(self):
        mock_source = MagicMock()
        mock_source.get_money_flow_stock.return_value = pd.DataFrame({"main_net": [100]})
        self.manager.sources["eastmoney"] = mock_source

        prefs_api = [SourcePreference(source=SourceType.EASTMONEY, priority=1, timeout=10.0, tier=SourceTier.API)]

        with patch("finshare.sources.manager.get_router") as mock_router:
            mock_router.return_value.get_tiered_sources.return_value = (prefs_api, [])
            result = self.manager._tiered_request(
                data_type=DataType.MONEY_FLOW_STOCK,
                method_name="get_money_flow_stock",
                args=("000001",),
            )

        mock_source.get_money_flow_stock.assert_called_once_with("000001")
        assert len(result) == 1

    def test_tiered_request_records_metrics(self):
        mock_source = MagicMock()
        mock_source.get_concept_list.return_value = pd.DataFrame({"board_name": ["A"]})
        self.manager.sources["eastmoney"] = mock_source

        prefs_api = [SourcePreference(source=SourceType.EASTMONEY, priority=1, timeout=10.0, tier=SourceTier.API)]

        with patch("finshare.sources.manager.get_router") as mock_router:
            mock_router.return_value.get_tiered_sources.return_value = (prefs_api, [])
            with patch("finshare.sources.manager.get_metrics_recorder") as mock_metrics:
                self.manager._tiered_request(
                    data_type=DataType.CONCEPT_LIST,
                    method_name="get_concept_list",
                    collector_name="ConceptCollector",
                )
                mock_metrics.return_value.record.assert_called_once()
                recorded = mock_metrics.return_value.record.call_args[0][0]
                assert recorded.collector_name == "ConceptCollector"
                assert recorded.source_used == "eastmoney"
                assert recorded.success is True
