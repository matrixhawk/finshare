"""市场情绪数据接口。"""

from __future__ import annotations

from typing import Any

import pandas as pd

_client = None


def _get_client():
    global _client
    if _client is None:
        from finshare.stock.sentiment.client import SentimentClient
        _client = SentimentClient()
    return _client


def get_market_overview() -> pd.DataFrame:
    """获取市场概览（涨跌家数、涨停跌停数）。"""
    return _get_client().get_market_overview()


def get_margin_trading_summary() -> pd.DataFrame:
    """获取全市场融资融券汇总。"""
    return _get_client().get_margin_trading_summary()


def get_fear_greed_index(
    overview: pd.DataFrame,
    margin: pd.DataFrame,
    north_flow: float,
    turnover_history: list[float],
) -> dict[str, Any]:
    """计算恐贪指数。"""
    from finshare.stock.sentiment.fear_greed import FearGreedCalculator
    return FearGreedCalculator().calculate(overview, margin, north_flow, turnover_history)
