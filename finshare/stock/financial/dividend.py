"""
分红送股数据接口 — 多源容灾

优先级: 东方财富 → BaoStock
"""

import pandas as pd
from finshare.logger import logger

_EMPTY_COLUMNS = [
    "fs_code", "ann_date", "divident_date", "cash_div",
    "stock_div", "stock_ratio", "bonus_ratio"
]


def get_dividend(code: str) -> pd.DataFrame:
    """
    获取分红送股数据（多源容灾）

    Args:
        code: 股票代码 (000001.SZ, 600519.SH, 或纯6位)

    Returns:
        DataFrame with: fs_code, ann_date, divident_date, cash_div,
        stock_div, stock_ratio, bonus_ratio
    """
    # Source 1: 东方财富
    try:
        from finshare.stock.financial.client import FinancialClient
        client = FinancialClient()
        df = client.get_dividend(code)
        if df is not None and not df.empty:
            return df
        logger.debug(f"东方财富分红数据为空: {code}，切换 BaoStock")
    except Exception as e:
        logger.warning(f"东方财富分红数据失败: {code}，{e}，切换 BaoStock")

    # Source 2: BaoStock
    try:
        df = _get_from_baostock(code)
        if df is not None and not df.empty:
            logger.info(f"[BaoStock] 分红数据获取成功: {code}, {len(df)}条")
            return df
    except Exception as e:
        logger.warning(f"BaoStock 分红数据也失败: {code}, {e}")

    return pd.DataFrame(columns=_EMPTY_COLUMNS)


def _get_from_baostock(code: str) -> pd.DataFrame:
    """从 BaoStock 获取分红数据（query_dividend_data）。"""
    import baostock as bs
    from datetime import date

    clean = code.replace(".SH", "").replace(".SZ", "").replace(".ss", "")
    if len(clean) > 6:
        clean = clean[-6:]
    bs_code = f"sh.{clean}" if clean.startswith(("6", "9")) else f"sz.{clean}"

    bs.login()
    try:
        records = []
        current_year = date.today().year

        # Fetch last 5 years of dividends
        for year in range(current_year - 4, current_year + 1):
            rs = bs.query_dividend_data(code=bs_code, year=str(year), yearType="report")
            if rs.error_code != "0":
                continue
            while rs.next():
                row = dict(zip(rs.fields, rs.get_row_data()))
                cash_str = row.get("dividCashPsBeforeTax", "0")
                stock_str = row.get("dividStocksPs", "0")
                records.append({
                    "fs_code": code,
                    "ann_date": (row.get("dividPlanAnnounceDate") or "").replace("-", ""),
                    "divident_date": (row.get("dividPayDate") or row.get("dividOperateDate") or "").replace("-", ""),
                    "cash_div": _safe_float(cash_str),
                    "stock_div": _safe_float(stock_str),
                    "stock_ratio": _safe_float(stock_str) / 10 if _safe_float(stock_str) > 0 else 0,
                    "bonus_ratio": 0,
                })

        if not records:
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        df = pd.DataFrame(records)
        df = df.drop_duplicates(subset=["ann_date", "divident_date"], keep="last")
        return df.sort_values("ann_date", ascending=False).reset_index(drop=True)
    finally:
        bs.logout()


def _safe_float(val) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
