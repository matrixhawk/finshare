"""
特色数据模块

提供资金流向、龙虎榜、融资融券、大宗交易、质押、解禁等中国特色数据。
"""

from finshare.stock.feature.moneyflow import get_money_flow, get_money_flow_industry
from finshare.stock.feature.lhb import get_lhb, get_lhb_detail
from finshare.stock.feature.margin import get_margin, get_margin_detail
from finshare.stock.feature.alt_data import (
    get_block_trade,
    get_pledge_ratio,
    get_restricted_release,
    get_macro_pmi,
    get_macro_shibor,
    get_stock_news,
    get_stock_info,
    get_insider_trade,
    get_analyst_forecast,
    get_rating_change,
)

__all__ = [
    "get_money_flow",
    "get_money_flow_industry",
    "get_lhb",
    "get_lhb_detail",
    "get_margin",
    "get_margin_detail",
    "get_block_trade",
    "get_pledge_ratio",
    "get_restricted_release",
    "get_macro_pmi",
    "get_macro_shibor",
    "get_stock_news",
    "get_stock_info",
    "get_insider_trade",
    "get_analyst_forecast",
    "get_rating_change",
]
