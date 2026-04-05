"""概念板块数据接口。"""

from __future__ import annotations

import pandas as pd

_client = None


def _get_client():
    global _client
    if _client is None:
        from finshare.stock.concept.client import ConceptClient
        _client = ConceptClient()
    return _client


def get_concept_list() -> pd.DataFrame:
    """获取概念板块列表（含涨跌幅和资金流）。"""
    return _get_client().get_concept_list()


def get_concept_constituents(board_code: str) -> pd.DataFrame:
    """获取概念板块成分股。"""
    return _get_client().get_concept_constituents(board_code)


def get_concept_money_flow() -> pd.DataFrame:
    """获取概念板块资金流。"""
    return _get_client().get_concept_money_flow()
