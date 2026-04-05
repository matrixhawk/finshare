"""概念板块 API 客户端。

通过东财 push2 API 获取概念板块列表、成分股和资金流数据。
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://82.push2.eastmoney.com/api/qt/clist/get"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com",
    "Accept": "application/json, text/plain, */*",
}
_CACHE_TTL = 24 * 3600


class ConceptClient:
    """概念板块客户端，含请求缓存。"""

    def __init__(self, cache_ttl: int = _CACHE_TTL):
        self._cache: dict[str, tuple[float, pd.DataFrame]] = {}
        self._cache_ttl = cache_ttl

    def _request(self, params: dict) -> Optional[dict]:
        try:
            resp = requests.get(_BASE_URL, params=params, headers=_HEADERS, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[ConceptClient] request failed: {e}")
            return None

    def _get_cached(self, key: str) -> Optional[pd.DataFrame]:
        if key in self._cache:
            ts, df = self._cache[key]
            if time.time() - ts < self._cache_ttl:
                return df
            del self._cache[key]
        return None

    def _set_cached(self, key: str, df: pd.DataFrame) -> None:
        self._cache[key] = (time.time(), df)

    def get_concept_list(self) -> pd.DataFrame:
        """获取概念板块列表（含涨跌幅和资金流）。

        Returns:
            DataFrame with columns: board_code, board_name, change_pct, net_inflow, net_inflow_ratio
        """
        cached = self._get_cached("concept_list")
        if cached is not None:
            return cached

        params = {
            "pn": "1", "pz": "500", "po": "1", "np": "1", "fltt": "2", "invt": "2",
            "fs": "m:90+t:3+f:!50", "fields": "f2,f3,f12,f14,f62,f184",
        }
        data = self._request(params)
        if not data or "data" not in data or not data["data"]:
            return pd.DataFrame()
        diff = data["data"].get("diff", [])
        if not diff:
            return pd.DataFrame()
        records = [
            {
                "board_code": str(item.get("f12", "")),
                "board_name": str(item.get("f14", "")),
                "change_pct": float(item.get("f3", 0)),
                "net_inflow": float(item.get("f62", 0)),
                "net_inflow_ratio": float(item.get("f184", 0)),
            }
            for item in diff
        ]
        df = pd.DataFrame(records)
        self._set_cached("concept_list", df)
        return df

    def get_concept_constituents(self, board_code: str) -> pd.DataFrame:
        """获取概念板块成分股。

        Args:
            board_code: 板块代码，如 "BK0493"
        Returns:
            DataFrame with columns: fs_code, name
        """
        cache_key = f"concept_constituents_{board_code}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        params = {
            "pn": "1", "pz": "1000", "po": "1", "np": "1", "fltt": "2", "invt": "2",
            "fs": f"b:{board_code}+f:!50", "fields": "f12,f14",
        }
        data = self._request(params)
        if not data or "data" not in data or not data["data"]:
            return pd.DataFrame()
        diff = data["data"].get("diff", [])
        if not diff:
            return pd.DataFrame()
        records = []
        for item in diff:
            code = str(item.get("f12", ""))
            name = str(item.get("f14", ""))
            if code.startswith("6"):
                fs_code = f"{code}.SH"
            elif code.startswith(("0", "3")):
                fs_code = f"{code}.SZ"
            else:
                fs_code = code
            records.append({"fs_code": fs_code, "name": name})
        df = pd.DataFrame(records)
        self._set_cached(cache_key, df)
        return df

    def get_concept_money_flow(self) -> pd.DataFrame:
        """获取概念板块资金流。

        Returns:
            DataFrame with columns: concept, net_inflow, net_inflow_ratio, change_rate
        """
        cached = self._get_cached("concept_money_flow")
        if cached is not None:
            return cached
        params = {
            "pn": "1", "pz": "500", "po": "1", "np": "1", "fltt": "2", "invt": "2",
            "fs": "m:90+t:3+f:!50", "fields": "f3,f14,f62,f184",
        }
        data = self._request(params)
        if not data or "data" not in data or not data["data"]:
            return pd.DataFrame()
        diff = data["data"].get("diff", [])
        if not diff:
            return pd.DataFrame()
        records = [
            {
                "concept": str(item.get("f14", "")),
                "net_inflow": float(item.get("f62", 0)),
                "net_inflow_ratio": float(item.get("f184", 0)),
                "change_rate": float(item.get("f3", 0)),
            }
            for item in diff
        ]
        df = pd.DataFrame(records)
        self._set_cached("concept_money_flow", df)
        return df
