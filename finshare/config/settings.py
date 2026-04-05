"""
finshare Config Module

配置管理模块
"""

import os
from typing import List


class SmartCooldownConfig:
    """智能冷却配置 — 连接类错误快速恢复，服务端限制保持长冷却"""

    def __init__(self):
        # 冷却策略（秒）
        self.cooldown_timeout = 5               # 超时：5s 后重试
        self.cooldown_connection_error = 10     # 连接错误：10s 后重试
        self.cooldown_rate_limit = 120          # 429限流：2 分钟
        self.cooldown_forbidden = 300           # 403禁止：5 分钟
        self.cooldown_service_unavailable = 30  # 503不可用：30s
        self.cooldown_default = 15              # 默认：15s

        # 连续失败累积
        self.max_failure_multiplier = 3.0       # 最大累积倍率


class RetryConfig:
    """重试配置 — 快速失败，让 Manager 切源"""

    def __init__(self):
        self.max_retries = 1                   # 最多重试 1 次（快速放弃）
        self.retry_base_delay = 1.0            # 基础延迟 1 秒
        self.retry_max_delay = 3.0             # 最大延迟 3 秒
        self.retry_backoff_factor = 2.0        # 指数退避因子


class HealthProbeConfig:
    """健康探测配置"""

    def __init__(self):
        self.probe_interval = 300            # 探测间隔（秒），默认5分钟
        self.probe_timeout = 10               # 探测超时（秒）
        self.success_threshold = 1             # 连续成功次数阈值


class LoggingConfig:
    """日志配置"""

    def __init__(self):
        self.log_dir = os.path.join(os.path.expanduser("~"), ".finshare", "logs")
        self.log_level = "INFO"
        self.log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        )
        self.rotation = "10 MB"
        self.retention = "30 days"
        self.enable_remote_logging = False
        self.remote_log_url = None


class DataSourceConfig:
    """数据源配置"""

    def __init__(self):
        # 默认数据源优先级（用于未匹配的市场，以及数据源初始化）
        # 注意：这里需要包含所有可能的数据源用于初始化
        self.source_priority = ["yahoo", "eastmoney", "tencent", "sina", "tdx", "baostock"]
        self.timeout = 30
        self.request_timeout = 30  # 请求超时时间（秒）
        self.retry_times = 3
        self.request_interval = 0.1  # 请求间隔（秒）
        self.max_workers = 5  # 最大并发数
        # 旧版配置，保留兼容（已废弃，使用 SmartCooldownConfig）
        self.failure_cooldown_hours = 24  # 数据源失败后的冷却时间（小时）

        # 按市场类型配置数据源优先级
        # 不同市场使用不同的数据源，以发挥各数据源的优势
        self.market_source_priority = {
            "US": ["yahoo", "eastmoney"],  # 美股：Yahoo Finance 优先，东方财富备选
            "HK": ["eastmoney"],            # 港股：东方财富
            "SH": ["eastmoney", "baostock", "tencent", "sina"],  # 上海A股
            "SZ": ["eastmoney", "baostock", "tencent", "sina"],  # 深圳A股
            "BJ": ["eastmoney", "tencent"],  # 北京A股
        }

    def get_source_priority(self, code: str) -> List[str]:
        """
        根据股票代码获取对应的数据源优先级

        Args:
            code: 股票代码（如 600519, AAPL.US, 00700.HK）

        Returns:
            数据源优先级列表
        """
        # 简单判断市场类型
        code_upper = code.upper() if code else ""

        if ".US" in code_upper or code_upper.startswith("US") or (code_upper.isalpha() and len(code_upper) <= 5):
            return self.market_source_priority.get("US", self.source_priority)
        elif ".HK" in code_upper or code_upper.startswith("HK"):
            return self.market_source_priority.get("HK", self.source_priority)
        elif code_upper.startswith("SH") or (len(code_upper) >= 1 and code_upper[0] in ["5", "6"]):
            return self.market_source_priority.get("SH", self.source_priority)
        elif code_upper.startswith("SZ") or (len(code_upper) >= 1 and code_upper[0] in ["0", "1", "2", "3"]):
            return self.market_source_priority.get("SZ", self.source_priority)
        elif code_upper.startswith("BJ") or (len(code_upper) >= 1 and code_upper[:2] in ["8", "9"]):
            return self.market_source_priority.get("BJ", self.source_priority)

        return self.source_priority


class Config:
    """全局配置"""

    def __init__(self):
        self.timeout = 30
        self.logging = LoggingConfig()
        self.data_source = DataSourceConfig()
        self.smart_cooldown = SmartCooldownConfig()
        self.retry = RetryConfig()
        self.health_probe = HealthProbeConfig()

    def get(self, key, default=None):
        """获取配置项"""
        return getattr(self, key, default)


# 全局配置实例
config = Config()

__all__ = [
    "Config",
    "config",
    "SmartCooldownConfig",
    "RetryConfig",
    "HealthProbeConfig",
]
