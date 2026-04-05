"""Phase 1a: 快速失败切源 — 验证 retry/cooldown/manager 改造效果"""
import time
from unittest.mock import patch, MagicMock

from finshare.sources.resilience.retry_handler import RetryHandler, RetryConfig
from finshare.sources.resilience.smart_cooldown import SmartCooldown, CooldownConfig


# ---- Task 1: Retry 快速失败 ----

def test_fast_retry_config_defaults():
    config = RetryConfig()
    assert config.max_retries == 1, "最多重试 1 次"
    assert config.base_delay <= 1.0, "基础延迟不超过 1 秒"
    assert config.max_delay <= 3.0, "最大延迟不超过 3 秒"


def test_fast_retry_delay_sequence():
    handler = RetryHandler(RetryConfig())
    d0 = handler.calculate_delay(0)
    assert d0 <= 2.0, f"首次重试延迟应 ≤ 2s，实际 {d0:.1f}s"


def test_retry_gives_up_fast():
    handler = RetryHandler(RetryConfig())
    call_count = 0

    def failing_func():
        nonlocal call_count
        call_count += 1
        raise ConnectionError("Remote end closed connection")

    t0 = time.time()
    try:
        handler.execute(failing_func)
    except ConnectionError:
        pass
    elapsed = time.time() - t0

    assert call_count == 2, f"应该尝试 2 次（1 原始 + 1 重试），实际 {call_count}"
    assert elapsed < 5.0, f"总耗时应 < 5s，实际 {elapsed:.1f}s"


# ---- Task 2: Cooldown 快速恢复 ----

def test_connection_error_short_cooldown():
    mgr = SmartCooldown()
    seconds = mgr.enter_cooldown("test_conn", "connection_error")
    assert seconds <= 15, f"连接错误冷却应 ≤ 15s，实际 {seconds}s"


def test_timeout_short_cooldown():
    mgr = SmartCooldown()
    seconds = mgr.enter_cooldown("test_timeout", "timeout")
    assert seconds <= 10, f"超时冷却应 ≤ 10s，实际 {seconds}s"


def test_rate_limit_keeps_long_cooldown():
    mgr = SmartCooldown()
    seconds = mgr.enter_cooldown("test_rate", "rate_limit", http_status=429)
    assert seconds >= 60, f"429 冷却应 ≥ 60s，实际 {seconds}s"


# ---- Task 3: Manager 短冷却 ----

def test_manager_failure_short_cooldown():
    from finshare.sources.manager import DataSourceManager
    mgr = DataSourceManager()

    mgr._record_source_failure("eastmoney", "ConnectionError: timeout")

    status = mgr.source_status.get("eastmoney", {})
    if status.get("cool_down_until"):
        from datetime import datetime
        remaining = (status["cool_down_until"] - datetime.now()).total_seconds()
        assert remaining < 120, f"冷却时间应 < 120s，实际 {remaining:.0f}s"


def test_manager_failed_source_unavailable():
    from finshare.sources.manager import DataSourceManager
    mgr = DataSourceManager()

    mgr._record_source_failure("eastmoney", "ConnectionError")
    assert not mgr._is_source_available("eastmoney"), "失败的源应立即不可用"
