"""财报日历 API 客户端。"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com",
}


class EarningsClient:
    """财报日历客户端。"""

    def _request(self, params: dict) -> Optional[dict]:
        try:
            resp = requests.get(_BASE_URL, params=params, headers=_HEADERS, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"[EarningsClient] request failed: {e}")
            return None

    def get_earnings_calendar(self, date: str) -> pd.DataFrame:
        """获取某日的财报披露日历。

        Args:
            date: 日期，格式 YYYY-MM-DD
        Returns:
            DataFrame: code, name, report_date, report_type
        """
        params = {
            "reportName": "RPT_PUBLIC_BS_APPOIN",
            "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,REPORT_DATE,REPORT_TYPE,UPDATE_DATE",
            "filter": f"(UPDATE_DATE='{date}')",
            "pageSize": "500",
            "pageNumber": "1",
            "sortColumns": "SECURITY_CODE",
            "sortTypes": "1",
        }
        data = self._request(params)
        if not data or "result" not in data or not data["result"]:
            return pd.DataFrame()
        rows = data["result"].get("data", [])
        if not rows:
            return pd.DataFrame()
        records = [
            {
                "code": row.get("SECURITY_CODE", ""),
                "name": row.get("SECURITY_NAME_ABBR", ""),
                "report_date": row.get("REPORT_DATE", ""),
                "report_type": row.get("REPORT_TYPE", ""),
            }
            for row in rows
        ]
        return pd.DataFrame(records)

    def get_earnings_preannouncement(self, code: str) -> pd.DataFrame:
        """获取个股业绩预告。

        Args:
            code: 股票代码（6位数字）
        Returns:
            DataFrame: report_period, pre_type, pre_profit_range, announce_date
        """
        params = {
            "reportName": "RPT_PUBLIC_OP_NEWPREDICT",
            "columns": "REPORT_DATE,PREDICT_TYPE,PREDICT_CONTENT,NOTICE_DATE",
            "filter": f'(SECURITY_CODE="{code}")',
            "pageSize": "20",
            "pageNumber": "1",
            "sortColumns": "NOTICE_DATE",
            "sortTypes": "-1",
        }
        data = self._request(params)
        if not data or "result" not in data or not data["result"]:
            return pd.DataFrame()
        rows = data["result"].get("data", [])
        if not rows:
            return pd.DataFrame()
        records = [
            {
                "report_period": row.get("REPORT_DATE", ""),
                "pre_type": row.get("PREDICT_TYPE", ""),
                "pre_profit_range": row.get("PREDICT_CONTENT", ""),
                "announce_date": row.get("NOTICE_DATE", ""),
            }
            for row in rows
        ]
        return pd.DataFrame(records)
