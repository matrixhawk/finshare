"""采集指标记录，用于可观测性和告警。"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class CollectMetrics:
    """单次采集的指标快照。"""

    collector_name: str
    source_used: str
    source_tier: str  # "api" or "scraper"
    duration_ms: int
    records_count: int
    success: bool
    error_message: str = ""
    fallback_count: int = 0
    timestamp: str = field(default="")

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


class MetricsRecorder:
    """线程安全的内存指标记录器。"""

    def __init__(self, max_history: int = 1000):
        self._lock = threading.Lock()
        self._history: list[CollectMetrics] = []
        self._max_history = max_history

    def record(self, metrics: CollectMetrics) -> None:
        with self._lock:
            self._history.append(metrics)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history:]

    def get_recent(
        self, collector_name: str | None = None, limit: int = 50
    ) -> list[CollectMetrics]:
        with self._lock:
            items = self._history
            if collector_name:
                items = [m for m in items if m.collector_name == collector_name]
            return items[-limit:]

    def get_source_hit_stats(
        self, collector_name: str | None = None
    ) -> dict[str, int]:
        with self._lock:
            items = self._history
            if collector_name:
                items = [m for m in items if m.collector_name == collector_name]
            counts: dict[str, int] = defaultdict(int)
            for m in items:
                counts[m.source_used] += 1
            return dict(counts)


# Global singleton
_recorder = MetricsRecorder()


def get_metrics_recorder() -> MetricsRecorder:
    return _recorder
