"""
BaseClient - 数据客户端公共基类

提供HTTP请求、频率限制、User-Agent轮换、智能冷却、指数退避重试、
内存缓存（含 stale 降级）等公共能力，
供 IndexClient、IndustryClient、ValuationClient 等继承使用。
"""

import time
import random
import threading
import requests
from typing import Optional, Dict, Callable, Any

from finshare.logger import logger
from finshare.sources.resilience import cooldown_manager, retry_handler
from finshare.sources.resilience.retry_handler import RetryHandler, RetryConfig
from finshare.cache.cache import get_cache

# 快速重试配置 — 用于实时行情等低延迟场景
FAST_RETRY_HANDLER = RetryHandler(
    RetryConfig(max_retries=1, base_delay=1.0, max_delay=5.0)
)


class BaseClient:
    """数据客户端公共基类 — 提供HTTP请求、弹性机制、缓存"""

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    # 类级别线程安全频率限制
    _last_request_time: Dict[str, float] = {}
    _rate_limit_lock = threading.Lock()

    def __init__(self, source_name: str, request_interval: float = 0.5):
        self.source_name = source_name
        self.session = requests.Session()
        self.request_interval = request_interval
        self._cooldown_mgr = cooldown_manager
        self._retry_handler = retry_handler
        self._fast_retry_handler = FAST_RETRY_HANDLER
        self._cache = get_cache("memory")

    def get_random_user_agent(self) -> str:
        """随机获取一个 User-Agent"""
        return random.choice(self.USER_AGENTS)

    def _rate_limit(self):
        """线程安全的频率限制"""
        with self._rate_limit_lock:
            last_time = self._last_request_time.get(self.source_name, 0)
            elapsed = time.time() - last_time
            if elapsed < self.request_interval:
                sleep_time = self.request_interval - elapsed
                self._last_request_time[self.source_name] = time.time() + sleep_time
            else:
                sleep_time = 0
                self._last_request_time[self.source_name] = time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)

    def _make_request(
        self,
        url: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        timeout: int = 30,
        fast: bool = False,
    ) -> Optional[Dict]:
        """
        发送 HTTP GET 请求（带重试 + 智能冷却）

        Args:
            url: 请求地址
            params: 查询参数
            headers: 额外请求头
            timeout: 超时时间（秒）
            fast: True 使用快速重试（1次/1秒），适用于实时行情
        """
        # 1. 检查冷却
        if self._cooldown_mgr.is_in_cooldown(self.source_name):
            logger.debug(f"[{self.source_name}] 处于冷却中，跳过请求")
            return None

        # 2. 频率限制
        self._rate_limit()

        # 3. 构造请求头
        request_headers = {
            "User-Agent": self.get_random_user_agent(),
            "Accept": "application/json, text/plain, */*",
        }
        if headers:
            request_headers.update(headers)

        # 4. 带重试执行请求
        handler = self._fast_retry_handler if fast else self._retry_handler
        try:
            result = handler.execute(
                self._do_request, url, params, request_headers, timeout
            )
            self._cooldown_mgr.record_success(self.source_name)
            return result
        except (requests.RequestException, ValueError) as e:
            http_status = getattr(getattr(e, "response", None), "status_code", None)
            # 429/403/503 already recorded by _do_request, skip to avoid double counting
            if http_status not in (429, 403, 503):
                error_type = self._classify_error(str(e), http_status)
                self._cooldown_mgr.record_failure(
                    self.source_name, error_type, http_status
                )
            return None

    def _do_request(
        self, url: str, params: Optional[Dict], headers: Dict, timeout: int
    ) -> Optional[Dict]:
        """执行单次 HTTP 请求（带逐状态冷却）"""
        response = self.session.get(
            url, params=params, headers=headers, timeout=timeout
        )

        # 逐状态冷却 — 重试期间立即对特定错误进入冷却
        if response.status_code == 429:
            self._cooldown_mgr.record_failure(self.source_name, "rate_limit", 429)
            raise requests.HTTPError(response=response)
        if response.status_code == 403:
            self._cooldown_mgr.record_failure(self.source_name, "forbidden", 403)
            raise requests.HTTPError(response=response)
        if response.status_code == 503:
            self._cooldown_mgr.record_failure(
                self.source_name, "service_unavailable", 503
            )
            raise requests.HTTPError(response=response)
        if response.status_code >= 400:
            raise requests.HTTPError(response=response)

        try:
            return response.json()
        except ValueError as e:
            raise ValueError(f"[{self.source_name}] JSON 解析失败") from e

    def _classify_error(self, reason: str, http_status: Optional[int] = None) -> str:
        """根据错误信息分类错误类型"""
        if http_status == 429:
            return "rate_limit"
        if http_status == 403:
            return "forbidden"
        if http_status == 503:
            return "service_unavailable"
        reason_lower = reason.lower()
        if "timeout" in reason_lower:
            return "timeout"
        if "connection" in reason_lower:
            return "connection_error"
        return "default"

    def _cached_request(
        self,
        cache_key: str,
        ttl: int,
        fetch_fn: Callable[[], Any],
    ) -> Any:
        """
        带缓存 + stale 降级的请求。

        流程: 命中缓存 → 返回
              未命中 → fetch_fn()
                → 成功（非空）→ 写缓存 → 返回
                → 失败或空 → 有过期缓存? → 返回过期数据
                                          → 返回 None
        """
        import pandas as pd

        # 1. 尝试命中缓存
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        # 2. 网络请求
        result = fetch_fn()

        # 3. 判断结果是否有效（None 和空 DataFrame 都视为失败）
        is_valid = result is not None
        if is_valid and isinstance(result, pd.DataFrame) and result.empty:
            is_valid = False

        if is_valid:
            self._cache.set(cache_key, result, ttl=ttl)
            return result
        else:
            stale = self._cache.get_stale(cache_key)
            if stale is not None:
                logger.warning(
                    f"[{self.source_name}] 返回过期缓存: {cache_key}"
                )
                return stale
            return None

    def _ensure_full_code(self, code: str) -> str:
        """
        将股票代码标准化为 000001.SZ 格式

        支持输入格式:
        - 000001.SZ / 600519.SH  (已是标准格式，直接返回)
        - SZ000001 / SH600519     (前缀格式)
        - 000001 / 600519         (纯数字，自动识别市场)

        市场判断规则（首位数字）:
        - 6 / 5 → .SH
        - 0 / 1 / 2 / 3 → .SZ
        - 4 / 8 → .BJ
        - 9 → .SH
        """
        if not code:
            return code

        code = code.strip().upper()

        if "." in code:
            return code

        prefix_map = {"SZ": "SZ", "SH": "SH", "BJ": "BJ"}
        for prefix, market in prefix_map.items():
            if code.startswith(prefix):
                num_code = code[len(prefix):]
                return f"{num_code}.{market}"

        if code.isdigit():
            first = code[0]
            if first in ("6", "5", "9"):
                return f"{code}.SH"
            elif first in ("0", "1", "2", "3"):
                return f"{code}.SZ"
            elif first in ("4", "8"):
                return f"{code}.BJ"

        return code

    def close(self):
        """关闭 HTTP Session"""
        self.session.close()
