"""财报日历数据接口。"""

from __future__ import annotations

import pandas as pd

_client = None


def _get_client():
    global _client
    if _client is None:
        from finshare.stock.earnings.client import EarningsClient
        _client = EarningsClient()
    return _client


def get_earnings_calendar(date: str) -> pd.DataFrame:
    """获取某日的财报披露日历。"""
    return _get_client().get_earnings_calendar(date)


def get_earnings_preannouncement(code: str) -> pd.DataFrame:
    """获取个股业绩预告。"""
    return _get_client().get_earnings_preannouncement(code)
