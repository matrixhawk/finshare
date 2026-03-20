"""
申万行业分析数据接口 - thin wrapper around IndustryClient
"""

import pandas as pd
from typing import Optional
from finshare.stock.industry.client import IndustryClient


_client = None


def _get_client() -> IndustryClient:
    """获取客户端单例"""
    global _client
    if _client is None:
        _client = IndustryClient()
    return _client


def get_sw_industry_analysis(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    level: int = 1,
) -> pd.DataFrame:
    """
    获取申万行业指数 K 线分析数据。

    Args:
        start_date: 开始日期，格式 "20260301"
        end_date: 结束日期，格式 "20260320"
        level: 申万行业级别 (1/2/3)，默认 1

    Returns:
        DataFrame 包含列:
        - industry_code: 行业代码
        - industry_name: 行业名称
        - trade_date: 交易日期
        - close: 收盘价
        - change_pct: 涨跌幅
        - pe: None（东财K线API不提供）
        - pb: None（东财K线API不提供）
        - dividend_yield: None（东财K线API不提供）

    Example:
        >>> df = get_sw_industry_analysis(start_date="20260301", end_date="20260320")
        >>> print(df.head())
    """
    return _get_client().get_sw_industry_analysis(
        start_date=start_date,
        end_date=end_date,
        level=level,
    )
