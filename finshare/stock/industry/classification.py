"""
行业分类数据接口 - thin wrappers around IndustryClient
"""

import pandas as pd
from finshare.stock.industry.client import IndustryClient


_client = None


def _get_client() -> IndustryClient:
    """获取客户端单例"""
    global _client
    if _client is None:
        _client = IndustryClient()
    return _client


def get_industry_list() -> pd.DataFrame:
    """
    获取东财行业板块列表。

    Returns:
        DataFrame 包含列:
        - board_code: 板块代码
        - board_name: 板块名称
        - change_pct: 涨跌幅

    Example:
        >>> df = get_industry_list()
        >>> print(df.head())
    """
    return _get_client().get_industry_list()


def get_industry_constituents(board_name: str) -> pd.DataFrame:
    """
    获取东财行业成分股列表。

    Args:
        board_name: 板块名称，如 "银行"

    Returns:
        DataFrame 包含列:
        - fs_code: 股票代码，格式 "000001.SZ"
        - name: 股票简称

    Example:
        >>> df = get_industry_constituents("银行")
        >>> print(df.head())
    """
    return _get_client().get_industry_constituents(board_name)


def get_sw_industry_list(level: int = 3) -> pd.DataFrame:
    """
    获取申万行业列表。

    Args:
        level: 申万行业级别 (1/2/3)，默认 3

    Returns:
        DataFrame 包含列:
        - industry_code: 行业代码
        - industry_name: 行业名称

    Example:
        >>> df = get_sw_industry_list(level=1)
        >>> print(df.head())
    """
    return _get_client().get_sw_industry_list(level=level)


def get_sw_industry_constituents(industry_code: str) -> pd.DataFrame:
    """
    获取申万行业成分股。

    Args:
        industry_code: 申万行业代码

    Returns:
        DataFrame 包含列:
        - fs_code: 股票代码，格式 "000001.SZ"
        - name: 股票简称

    Example:
        >>> df = get_sw_industry_constituents("801780")
        >>> print(df.head())
    """
    return _get_client().get_sw_industry_constituents(industry_code)
