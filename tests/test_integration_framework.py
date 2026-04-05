"""Smoke test: tiered routing chain SmartRouter → Manager → Source (mocked)."""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from finshare.sources.resilience.smart_router import (
    SmartRouter,
    DataType,
    SourceType,
    SourceTier,
    SourcePreference,
    set_router,
    get_router,
)
from finshare.metrics import get_metrics_recorder


class TestIntegrationTieredRouting:
    def setup_method(self):
        router = SmartRouter(preferences={
            DataType.CONCEPT_LIST: [
                SourcePreference(source=SourceType.EASTMONEY, priority=1, timeout=10.0, tier=SourceTier.API),
                SourcePreference(source=SourceType.SINA, priority=2, timeout=10.0, tier=SourceTier.API),
                SourcePreference(source=SourceType.PLAYWRIGHT_EASTMONEY, priority=10, timeout=30.0, tier=SourceTier.SCRAPER),
            ],
        })
        set_router(router)

    def test_api_success_no_scraper_called(self):
        from finshare.sources.manager import DataSourceManager

        manager = DataSourceManager.__new__(DataSourceManager)
        manager.source_status = {}

        mock_em = MagicMock()
        mock_em.get_concept_list.return_value = pd.DataFrame({"board_name": ["AI"]})
        manager.sources = {"eastmoney": mock_em}

        mock_pw = MagicMock()
        manager._playwright_sources = {"playwright_eastmoney": mock_pw}

        result = manager._tiered_request(
            data_type=DataType.CONCEPT_LIST,
            method_name="get_concept_list",
            collector_name="integration_test",
        )

        assert len(result) == 1
        mock_pw.get_concept_list.assert_not_called()

    def test_api_fail_scraper_called(self):
        from finshare.sources.manager import DataSourceManager

        manager = DataSourceManager.__new__(DataSourceManager)
        manager.source_status = {}

        mock_em = MagicMock()
        mock_em.get_concept_list.side_effect = Exception("API down")
        mock_sina = MagicMock()
        mock_sina.get_concept_list.side_effect = Exception("Sina down too")
        manager.sources = {"eastmoney": mock_em, "sina": mock_sina}

        mock_pw = MagicMock()
        mock_pw.is_available.return_value = True
        mock_pw.get_concept_list.return_value = pd.DataFrame({"board_name": ["robotics"]})
        manager._playwright_sources = {"playwright_eastmoney": mock_pw}

        result = manager._tiered_request(
            data_type=DataType.CONCEPT_LIST,
            method_name="get_concept_list",
            collector_name="integration_test",
        )

        assert len(result) == 1
        assert result.iloc[0]["board_name"] == "robotics"

    def test_metrics_recorded_on_success(self):
        from finshare.sources.manager import DataSourceManager

        manager = DataSourceManager.__new__(DataSourceManager)
        manager.source_status = {}

        mock_em = MagicMock()
        mock_em.get_concept_list.return_value = pd.DataFrame({"board_name": ["AI"]})
        manager.sources = {"eastmoney": mock_em}
        manager._playwright_sources = {}

        recorder = get_metrics_recorder()
        before_count = len(recorder.get_recent(collector_name="metrics_test"))

        manager._tiered_request(
            data_type=DataType.CONCEPT_LIST,
            method_name="get_concept_list",
            collector_name="metrics_test",
        )

        after = recorder.get_recent(collector_name="metrics_test")
        assert len(after) == before_count + 1
        assert after[-1].success is True
        assert after[-1].source_used == "eastmoney"
        assert after[-1].source_tier == "api"
