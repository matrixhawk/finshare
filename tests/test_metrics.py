import pytest
from finshare.metrics import CollectMetrics, MetricsRecorder


class TestCollectMetrics:
    def test_create_success_metrics(self):
        m = CollectMetrics(
            collector_name="ConceptCollector",
            source_used="eastmoney",
            source_tier="api",
            duration_ms=123,
            records_count=50,
            success=True,
        )
        assert m.collector_name == "ConceptCollector"
        assert m.success is True
        assert m.error_message == ""
        assert m.fallback_count == 0
        assert m.timestamp != ""

    def test_create_failure_metrics(self):
        m = CollectMetrics(
            collector_name="ConceptCollector",
            source_used="playwright_eastmoney",
            source_tier="scraper",
            duration_ms=5000,
            records_count=0,
            success=False,
            error_message="timeout",
            fallback_count=2,
        )
        assert m.success is False
        assert m.fallback_count == 2


class TestMetricsRecorder:
    def setup_method(self):
        self.recorder = MetricsRecorder()

    def test_record_and_get_recent(self):
        m = CollectMetrics(
            collector_name="TestCollector",
            source_used="eastmoney",
            source_tier="api",
            duration_ms=100,
            records_count=10,
            success=True,
        )
        self.recorder.record(m)
        recent = self.recorder.get_recent(collector_name="TestCollector", limit=10)
        assert len(recent) == 1
        assert recent[0].collector_name == "TestCollector"

    def test_get_recent_respects_limit(self):
        for i in range(5):
            m = CollectMetrics(
                collector_name="TestCollector",
                source_used="eastmoney",
                source_tier="api",
                duration_ms=100,
                records_count=10,
                success=True,
            )
            self.recorder.record(m)
        recent = self.recorder.get_recent(collector_name="TestCollector", limit=3)
        assert len(recent) == 3

    def test_get_source_hit_stats(self):
        for source in ["eastmoney", "eastmoney", "sina"]:
            m = CollectMetrics(
                collector_name="TestCollector",
                source_used=source,
                source_tier="api",
                duration_ms=100,
                records_count=10,
                success=True,
            )
            self.recorder.record(m)
        stats = self.recorder.get_source_hit_stats(collector_name="TestCollector")
        assert stats["eastmoney"] == 2
        assert stats["sina"] == 1
