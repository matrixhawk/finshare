"""
包含: 大宗交易、质押比例、限售解禁、宏观指标、个股新闻、个股信息、
      高管增减持、分析师预测、评级变动

所有请求走 FeatureClient，享受 rate_limit + UA 轮换 + 超时控制。
"""

import re

import pandas as pd
from typing import Optional

from finshare.stock.feature.client import FeatureClient
from finshare.logger import logger

# 全局客户端
_client = None


def _get_client() -> FeatureClient:
    global _client
    if _client is None:
        _client = FeatureClient()
    return _client


# ============================================================
# 东方财富 datacenter API 通用分页获取
# ============================================================

def _fetch_datacenter(
    report_name: str,
    columns: str = "ALL",
    filter_expr: str = "",
    sort_columns: str = "",
    sort_types: str = "-1",
    page_size: int = 500,
    max_pages: int = 20,
) -> list[dict]:
    """东方财富 datacenter API 分页获取。返回原始 data list。"""
    client = _get_client()
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    all_data = []

    for page in range(1, max_pages + 1):
        params = {
            "reportName": report_name,
            "columns": columns,
            "pageNumber": str(page),
            "pageSize": str(page_size),
            "sortColumns": sort_columns,
            "sortTypes": sort_types,
            "source": "WEB",
            "client": "WEB",
        }
        if filter_expr:
            params["filter"] = filter_expr

        data = client._make_request(url, params)
        if not data:
            break

        result = data.get("result")
        if not result:
            break

        items = result.get("data") or []
        if not items:
            break

        all_data.extend(items)

        total = result.get("count", 0)
        total_pages = (total + page_size - 1) // page_size if total > 0 else 1
        if page >= total_pages:
            break

    return all_data


# ============================================================
# 1. 大宗交易 (替代 ak.stock_dzjy_mrtj)
# ============================================================

def get_block_trade(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """大宗交易每日明细

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame: 证券代码, 证券简称, 交易日期, 折溢率, 成交总额, ...
    """
    from datetime import datetime, timedelta

    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

    s_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
    e_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

    data = _fetch_datacenter(
        report_name="RPT_DATA_BLOCKTRADE",
        columns="TRADE_DATE,SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,CHANGE_RATE,"
                "CLOSE_PRICE,DEAL_PRICE,PREMIUM_RATIO,DEAL_VOLUME,DEAL_AMT,"
                "TURNOVER_RATE,BUYER_NAME,SELLER_NAME",
        filter_expr=f"(SECURITY_TYPE_WEB=1)(TRADE_DATE>='{s_fmt}')(TRADE_DATE<='{e_fmt}')",
        sort_columns="TRADE_DATE",
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    col_map = {
        "TRADE_DATE": "交易日期",
        "SECURITY_CODE": "证券代码",
        "SECURITY_NAME_ABBR": "证券简称",
        "PREMIUM_RATIO": "折溢率",
        "DEAL_AMT": "成交总额",
        "DEAL_VOLUME": "成交量",
        "CLOSE_PRICE": "收盘价",
        "DEAL_PRICE": "成交价",
        "BUYER_NAME": "买方营业部",
        "SELLER_NAME": "卖方营业部",
    }
    df = df.rename(columns=col_map)
    if "交易日期" in df.columns:
        df["交易日期"] = pd.to_datetime(df["交易日期"], errors="coerce").dt.date
    logger.info(f"获取大宗交易: {len(df)} 条")
    return df


# ============================================================
# 2. 质押比例 (替代 ak.stock_gpzy_pledge_ratio_em)
# ============================================================

def get_pledge_ratio(date: str = None) -> pd.DataFrame:
    """上市公司股权质押比例

    Args:
        date: 日期 YYYYMMDD (可选)

    Returns:
        DataFrame: 股票代码, 股票简称, 质押比例, 质押市值, ...
    """
    data = _fetch_datacenter(
        report_name="RPT_CSDC_LIST",
        sort_columns="PLEDGE_RATIO",
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    col_map = {
        "SECURITY_CODE": "股票代码",
        "SECURITY_NAME_ABBR": "股票简称",
        "PLEDGE_RATIO": "质押比例",
        "PLEDGE_TOTAL_NUM": "质押笔数",
        "PLEDGE_TOTAL_MARKET_CAP": "质押市值",
        "CLOSE_PRICE": "收盘价",
        "TRADE_DATE": "交易日期",
    }
    df = df.rename(columns=col_map)
    logger.info(f"获取质押比例: {len(df)} 条")
    return df


# ============================================================
# 3. 限售解禁 (替代 ak.stock_restricted_release_queue_em)
# ============================================================

def get_restricted_release(code: str = None) -> pd.DataFrame:
    """限售解禁数据

    Args:
        code: 股票代码 (可选, 不传获取全部)

    Returns:
        DataFrame: 股票代码, 股票简称, 解禁日期, 解禁数量, 解禁市值
    """
    filter_expr = ""
    if code:
        filter_expr = f'(SECURITY_CODE="{code}")'

    data = _fetch_datacenter(
        report_name="RPT_LIFT_STAGE",
        sort_columns="FREE_DATE",
        filter_expr=filter_expr,
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    col_map = {
        "SECURITY_CODE": "股票代码",
        "SECURITY_NAME_ABBR": "股票简称",
        "FREE_DATE": "解禁日期",
        "FREE_SHARES_QUANTITY": "解禁数量",
        "FREE_MARKET_CAP": "解禁市值",
        "FREE_RATIO": "解禁比例",
    }
    df = df.rename(columns=col_map)
    logger.info(f"获取限售解禁: {len(df)} 条")
    return df


# ============================================================
# 4. 宏观数据 (替代 ak.macro_china_pmi_yearly / macro_china_shibor_all)
# ============================================================

def get_macro_pmi() -> pd.DataFrame:
    """中国 PMI 数据"""
    data = _fetch_datacenter(
        report_name="RPT_ECONOMY_PMI",
        sort_columns="REPORT_DATE",
    )
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    logger.info(f"获取 PMI: {len(df)} 条")
    return df


def get_macro_shibor() -> pd.DataFrame:
    """SHIBOR 利率数据"""
    data = _fetch_datacenter(
        report_name="RPT_ECONOMY_SHIBOR",
        sort_columns="REPORT_DATE",
        max_pages=5,
    )
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    logger.info(f"获取 SHIBOR: {len(df)} 条")
    return df


# ============================================================
# 5. 个股新闻 (替代 ak.stock_news_em)
# ============================================================

def get_stock_news(code: str, count: int = 100) -> pd.DataFrame:
    """个股新闻（最近 N 条）

    Args:
        code: 股票代码 (6位数字)
        count: 获取条数

    Returns:
        DataFrame: 新闻标题, 发布时间, 文章来源, 新闻链接
    """
    client = _get_client()
    # 东方财富搜索 API (JSONP)
    url = "https://search-api-web.eastmoney.com/search/jsonp"

    # 纯6位代码
    pure_code = code.split(".")[0] if "." in code else code

    params = {
        "cb": "jQuery_callback",
        "param": f'{{"uid":"","keyword":"{pure_code}","type":["cmsArticleWebOld"],'
                 f'"client":"web","clientType":"web","clientVersion":"curr",'
                 f'"param":{{"cmsArticleWebOld":{{"searchScope":"default","sort":"default",'
                 f'"pageIndex":1,"pageSize":{count},"preTag":"<em>","postTag":"</em>"}}}}}}',
    }

    data = client._make_request(url, params)

    # JSONP 响应需要特殊处理
    if isinstance(data, str):
        # 提取 JSONP 中的 JSON
        import json
        match = re.search(r'jQuery_callback\((.*)\)', data)
        if match:
            try:
                data = json.loads(match.group(1))
            except Exception:
                return pd.DataFrame()
        else:
            return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    try:
        articles = (
            data.get("result", {})
            .get("cmsArticleWebOld", {})
            .get("list", [])
        )

        if not articles:
            return pd.DataFrame()

        rows = []
        for a in articles:
            rows.append({
                "新闻标题": a.get("title", "").replace("<em>", "").replace("</em>", ""),
                "发布时间": a.get("date", ""),
                "文章来源": a.get("mediaName", ""),
                "新闻链接": a.get("url", ""),
            })

        df = pd.DataFrame(rows)
        logger.info(f"获取个股新闻: {pure_code}, {len(df)} 条")
        return df

    except Exception as e:
        logger.warning(f"解析新闻失败: {e}")
        return pd.DataFrame()


# ============================================================
# 6. 个股信息 (替代 ak.stock_individual_info_em)
# ============================================================

def get_stock_info(code: str) -> pd.DataFrame:
    """个股基本信息（行业、总市值、流通市值等）

    Args:
        code: 股票代码 (6位数字)

    Returns:
        DataFrame: item, value 两列
    """
    client = _get_client()
    url = "https://push2.eastmoney.com/api/qt/stock/get"

    pure_code = code.split(".")[0] if "." in code else code
    market_code = 1 if pure_code.startswith("6") else 0

    params = {
        "fltt": "2",
        "invt": "2",
        "fields": "f57,f58,f84,f85,f127,f116,f117,f189,f43",
        "secid": f"{market_code}.{pure_code}",
    }

    data = client._make_request(url, params)
    if not data or "data" not in data:
        return pd.DataFrame()

    try:
        d = data["data"]
        code_name_map = {
            "f57": "股票代码",
            "f58": "股票简称",
            "f84": "总股本",
            "f85": "流通股",
            "f127": "行业",
            "f116": "总市值",
            "f117": "流通市值",
            "f189": "上市时间",
            "f43": "最新",
        }

        rows = []
        for k, label in code_name_map.items():
            if k in d and d[k] is not None:
                rows.append({"item": label, "value": d[k]})

        df = pd.DataFrame(rows)
        logger.info(f"获取个股信息: {pure_code}")
        return df

    except Exception as e:
        logger.warning(f"解析个股信息失败: {e}")
        return pd.DataFrame()


# ============================================================
# 7. 高管增减持 (替代 ak.stock_inner_trade_xq)
# 注意: 雪球需要 cookie，改用东方财富的高管持股变动 API
# ============================================================

def get_insider_trade() -> pd.DataFrame:
    """高管增减持（东方财富数据源）

    Returns:
        DataFrame: 股票代码, 股票名称, 变动日期, 变动股数, 成交均价, ...
    """
    data = _fetch_datacenter(
        report_name="RPT_EXECUTIVE_HOLD_CHANGE",
        sort_columns="END_DATE",
        page_size=500,
        max_pages=5,
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    col_map = {
        "SECURITY_CODE": "股票代码",
        "SECURITY_NAME_ABBR": "股票名称",
        "END_DATE": "变动日期",
        "CHANGE_SHARES": "变动股数",
        "AVERAGE_PRICE": "成交均价",
        "EXECUTIVE_NAME": "高管姓名",
        "CHANGE_REASON_EXPLAIN": "变动原因",
    }
    df = df.rename(columns=col_map)
    if "变动日期" in df.columns:
        df["变动日期"] = pd.to_datetime(df["变动日期"], errors="coerce").dt.date
    logger.info(f"获取高管增减持: {len(df)} 条")
    return df


# ============================================================
# 8. 分析师预测 (替代 ak.stock_profit_forecast_ths)
# 改用东方财富的盈利预测 API，不依赖同花顺 HTML 解析
# ============================================================

def get_analyst_forecast(code: str) -> pd.DataFrame:
    """个股盈利预测（东方财富数据源）

    Args:
        code: 股票代码 (6位数字)

    Returns:
        DataFrame: 年度, 预测机构数, 均值(EPS), 最小值, 最大值
    """
    pure_code = code.split(".")[0] if "." in code else code

    data = _fetch_datacenter(
        report_name="RPT_WEB_RESPREDICT",
        filter_expr=f'(SECURITY_CODE="{pure_code}")',
        sort_columns="PREDICT_YEAR",
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    col_map = {
        "PREDICT_YEAR": "年度",
        "PREDICT_ORG_NUM": "预测机构数",
        "PREDICT_EPS_MEAN": "均值",
        "PREDICT_EPS_MIN": "最小值",
        "PREDICT_EPS_MAX": "最大值",
        "PREDICT_PE": "预测PE",
        "PREDICT_NETPROFIT_MEAN": "预测净利润均值",
    }
    df = df.rename(columns=col_map)
    logger.info(f"获取分析师预测: {pure_code}, {len(df)} 条")
    return df


# ============================================================
# 9. 评级变动 (替代 ak.stock_rank_forecast_cninfo)
# 改用东方财富的机构评级 API，不依赖巨潮
# ============================================================

def get_rating_change(date: str = None) -> pd.DataFrame:
    """机构评级变动

    Args:
        date: 日期 YYYYMMDD (可选)

    Returns:
        DataFrame: 股票代码, 股票简称, 最新评级, 评级变动, 目标价, ...
    """
    from datetime import datetime

    if not date:
        date = datetime.now().strftime("%Y%m%d")

    d_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}"

    data = _fetch_datacenter(
        report_name="RPT_WEB_RESPREDICT_DETAIL",
        filter_expr=f"(REPORT_DATE>='{d_fmt}')",
        sort_columns="REPORT_DATE",
        max_pages=5,
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    col_map = {
        "SECURITY_CODE": "股票代码",
        "SECURITY_NAME_ABBR": "股票简称",
        "RATING_NAME": "最新评级",
        "RATING_CHANGE": "评级变动",
        "TARGET_PRICE": "目标价",
        "ORG_NAME": "研究机构",
        "RESEARCHER": "研究员",
        "REPORT_DATE": "报告日期",
    }
    df = df.rename(columns=col_map)
    logger.info(f"获取评级变动: {len(df)} 条")
    return df
