"""市场情绪数据客户端。"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_PUSH2_URL = "https://82.push2.eastmoney.com/api/qt/ulist.np/get"
_DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com",
}


class SentimentClient:
    """市场情绪数据客户端。"""

    def _request(self, url: str, params: dict) -> Optional[dict]:
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[SentimentClient] request failed: {e}")
            return None

    def _request_datacenter(self, params: dict) -> Optional[dict]:
        return self._request(_DATACENTER_URL, params)

    def get_market_overview(self) -> pd.DataFrame:
        """获取市场概览（涨跌家数、涨停跌停数）。

        Returns:
            DataFrame: date, up_count, down_count, limit_up, limit_down
        """
        params = {
            "fltt": "2",
            "fields": "f104,f105,f106,f107",
            "secids": "1.000001,0.399001",
        }
        data = self._request(_PUSH2_URL, params)
        if not data or "data" not in data or not data["data"]:
            return pd.DataFrame()
        diff = data["data"].get("diff", [])
        if not diff:
            return pd.DataFrame()
        up_count = sum(int(d.get("f104", 0)) for d in diff)
        down_count = sum(int(d.get("f105", 0)) for d in diff)
        limit_up = sum(int(d.get("f106", 0)) for d in diff)
        limit_down = sum(int(d.get("f107", 0)) for d in diff)
        today = date.today().isoformat()
        return pd.DataFrame([{
            "date": today,
            "up_count": up_count,
            "down_count": down_count,
            "limit_up": limit_up,
            "limit_down": limit_down,
        }])

    def get_margin_trading_summary(self) -> pd.DataFrame:
        """获取全市场融资融券汇总。

        Returns:
            DataFrame: date, margin_buy, margin_balance, short_sell_volume, short_balance
        """
        params = {
            "reportName": "RPTA_MUTUAL_MARKETSTAT",
            "columns": "ALL",
            "pageSize": "5",
            "pageNumber": "1",
            "sortColumns": "TRADE_DATE",
            "sortTypes": "-1",
        }
        data = self._request_datacenter(params)
        if not data or "result" not in data or not data["result"]:
            return pd.DataFrame()
        rows = data["result"].get("data", [])
        if not rows:
            return pd.DataFrame()
        records = [
            {
                "date": row.get("TRADE_DATE", "")[:10],
                "margin_buy": float(row.get("BUY_AMT", 0) or 0),
                "margin_balance": float(row.get("FIN_BALANCE", 0) or 0),
                "short_sell_volume": float(row.get("SL_SELL_VOL", 0) or 0),
                "short_balance": float(row.get("SL_BALANCE", 0) or 0),
            }
            for row in rows
        ]
        return pd.DataFrame(records)
