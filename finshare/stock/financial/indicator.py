"""
财务指标数据接口 — 多源容灾

优先级: 东方财富 → BaoStock
东方财富: 数据全(EPS/ROE/毛利率/净利率/资产负债率等)
BaoStock: 免费无限制，覆盖 EPS/ROE/毛利率/净利率
"""

import pandas as pd
from typing import Optional
from finshare.logger import logger

_EMPTY_COLUMNS = [
    "fs_code", "ann_date", "report_date", "eps", "roe",
    "gross_margin", "netprofit_margin", "current_ratio", "quick_ratio",
    "debt_to_assets"
]


def get_financial_indicator(
    code: str,
    ann_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    获取财务指标数据（多源容灾）

    优先东方财富，失败切 BaoStock。

    Args:
        code: 股票代码 (000001.SZ, 600519.SH, 或纯6位)
        ann_date: 公告日期 (YYYYMMDD)

    Returns:
        DataFrame with: fs_code, ann_date, report_date, eps, roe,
        gross_margin, netprofit_margin, current_ratio, quick_ratio, debt_to_assets
    """
    # Source 1: 东方财富
    try:
        from finshare.stock.financial.client import FinancialClient
        client = FinancialClient()
        df = client.get_financial_indicator(code, ann_date)
        if df is not None and not df.empty:
            return df
        logger.debug(f"东方财富财务指标为空: {code}，切换 BaoStock")
    except Exception as e:
        logger.warning(f"东方财富财务指标失败: {code}，{e}，切换 BaoStock")

    # Source 2: BaoStock
    try:
        df = _get_from_baostock(code)
        if df is not None and not df.empty:
            logger.info(f"[BaoStock] 财务指标获取成功: {code}, {len(df)}条")
            return df
    except Exception as e:
        logger.warning(f"BaoStock 财务指标也失败: {code}, {e}")

    return pd.DataFrame(columns=_EMPTY_COLUMNS)


def _get_from_baostock(code: str) -> pd.DataFrame:
    """从 BaoStock 获取财务指标（query_profit_data）。"""
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

        for year in range(current_year - 2, current_year + 1):
            for quarter in range(1, 5):
                rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
                if rs.error_code != "0":
                    continue
                while rs.next():
                    row = dict(zip(rs.fields, rs.get_row_data()))
                    records.append({
                        "fs_code": code,
                        "ann_date": (row.get("pubDate") or "").replace("-", ""),
                        "report_date": (row.get("statDate") or "").replace("-", ""),
                        "eps": _safe_float(row.get("epsTTM")),
                        "roe": _safe_float(row.get("roeAvg")),
                        "gross_margin": _safe_float(row.get("gpMargin")),
                        "netprofit_margin": _safe_float(row.get("npMargin")),
                        "current_ratio": 0,
                        "quick_ratio": 0,
                        "debt_to_assets": 0,
                    })

        if not records:
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        df = pd.DataFrame(records)
        df = df.drop_duplicates(subset=["report_date"], keep="last")
        return df.sort_values("report_date", ascending=False).reset_index(drop=True)
    finally:
        bs.logout()


def _safe_float(val) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
