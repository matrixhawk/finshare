"""
IndexClient - 指数成分股 + PE/PB 估值历史客户端

提供指数成分股查询（东方财富）和估值历史数据（乐估乐股）。
"""

import pandas as pd
from typing import Optional

from finshare.stock.base_client import BaseClient
from finshare.logger import logger


# 指数名称 → 中证指数代码映射
INDEX_CODE_MAP = {
    "上证指数": "000001",
    "沪深300": "000300",
    "中证500": "000905",
    "中证1000": "000852",
    "创业板指": "399006",
    "上证50": "000016",
    "深证成指": "399001",
    "科创50": "000688",
    "中证全指": "000985",
    "中证800": "000906",
}

# 旧版乐估乐股符号映射（兼容）
LG_SYMBOL_MAP = {
    name: f"{code}.XSHG" if code.startswith(("000", "9")) else f"{code}.XSHE"
    for name, code in INDEX_CODE_MAP.items()
}

# Also support lookup by code (without exchange suffix)
_CODE_TO_LG = {v.split(".")[0]: v for v in LG_SYMBOL_MAP.values()}


class IndexClient(BaseClient):
    """指数成分股 + PE/PB 估值历史客户端"""

    CLIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    CSINDEX_PERF_URL = "https://www.csindex.com.cn/csindex-home/perf/index-perf"

    # Cache TTLs (seconds)
    TTL_CONSTITUENTS = 86400  # 24h — quarterly rebalance
    TTL_VALUATION = 3600       # 1h  — daily update

    def __init__(self):
        super().__init__("eastmoney_index")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_secid(self, index_code: str) -> str:
        """
        将指数代码转换为 EastMoney secid 格式。

        首位 0/3/9 → 0.{code}，其余 → 1.{code}
        """
        code = index_code.strip()
        first = code[0] if code else ""
        if first in ("0", "3", "9"):
            return f"0.{code}"
        return f"1.{code}"

    def _resolve_index_code(self, symbol: str) -> Optional[str]:
        """
        将中文名称或代码映射到纯数字指数代码。

        支持：
        - 中文名称，如 "沪深300"
        - 纯数字代码，如 "000300"
        - 已包含交易所后缀，如 "000300.XSHG"
        """
        # Direct map by Chinese name
        if symbol in INDEX_CODE_MAP:
            return INDEX_CODE_MAP[symbol]

        # Already contains exchange suffix
        if "." in symbol:
            return symbol.split(".")[0]

        # Numeric code directly
        code = symbol.strip()
        if code in _CODE_TO_LG:
            return code

        logger.warning(f"[eastmoney_index] 无法映射指数代码: {symbol}")
        return None

    def _resolve_lg_symbol(self, symbol: str) -> Optional[str]:
        """兼容旧接口：将中文名称或代码映射到乐估乐股 symbol。"""
        code = self._resolve_index_code(symbol)
        if not code:
            return None
        return _CODE_TO_LG.get(code)

    # ------------------------------------------------------------------
    # Private fetch methods
    # ------------------------------------------------------------------

    def _fetch_index_constituents(self, index_code: str) -> Optional[pd.DataFrame]:
        """从东方财富获取指数成分股"""
        secid = self._to_secid(index_code)
        logger.debug(f"获取指数成分股: {index_code} (secid={secid})")

        params = {
            "fs": f"b:{secid}",
            "fields": "f12,f14",
            "pz": 10000,
        }
        headers = {"Referer": "https://quote.eastmoney.com/"}
        data = self._make_request(self.CLIST_URL, params=params, headers=headers)

        if not data:
            return None

        try:
            data_obj = data.get("data") or {}
            diff = data_obj.get("diff", []) if isinstance(data_obj, dict) else []
            if isinstance(diff, dict):
                diff = list(diff.values())
            if not diff:
                logger.warning(f"[eastmoney_index] 成分股列表为空: {index_code}")
                return None

            records = []
            for item in diff:
                raw_code = str(item.get("f12", ""))
                name = item.get("f14", "")
                fs_code = self._ensure_full_code(raw_code)
                records.append({"fs_code": fs_code, "name": name})

            df = pd.DataFrame(records)
            logger.info(f"获取指数成分股成功: {index_code}, {len(df)}只")
            return df
        except Exception as e:
            logger.error(f"解析指数成分股失败: {e}")
            return None

    def _fetch_index_constituents_csindex(self, index_code: str) -> Optional[pd.DataFrame]:
        """备用源：中证指数官网获取成分股"""
        headers = {
            "Referer": "https://www.csindex.com.cn/",
            "User-Agent": self.get_random_user_agent(),
        }
        try:
            response = self.session.get(
                f"https://www.csindex.com.cn/csindex-home/index-info/index-stocks?indexCode={index_code}",
                headers=headers, timeout=15
            )
            if response.status_code != 200:
                return None
            data = response.json()
            items = data.get("data", [])
            if not items:
                return None
            records = []
            for item in items:
                code = str(item.get("securityCode", ""))
                name = item.get("securityNameAbbr", "")
                fs_code = self._ensure_full_code(code)
                records.append({"fs_code": fs_code, "name": name})
            df = pd.DataFrame(records)
            logger.info(f"CSIndex 备用源获取成分股成功: {index_code}, {len(df)}只")
            return df
        except Exception as e:
            logger.warning(f"CSIndex 备用源失败: {e}")
            return None

    def _fetch_index_constituents_baostock(self, index_code: str) -> Optional[pd.DataFrame]:
        """备用源：BaoStock 获取指数成分股（支持沪深300/中证500）"""
        try:
            import baostock as bs
            bs.login()
            try:
                query_map = {
                    "000300": bs.query_hs300_stocks,
                    "000905": bs.query_zz500_stocks,
                }
                query_fn = query_map.get(index_code)
                if query_fn is None:
                    logger.debug(f"[baostock] 不支持的指数成分股查询: {index_code}")
                    return None

                rs = query_fn()
                if rs.error_code != '0':
                    logger.warning(f"[baostock] 查询失败: {rs.error_msg}")
                    return None

                records = []
                while rs.next():
                    row = rs.get_row_data()
                    # row = [updateDate, code("sh.600000"), code_name]
                    if len(row) >= 3:
                        raw_code = row[1]  # "sh.600000"
                        code = raw_code.split(".")[-1] if "." in raw_code else raw_code
                        fs_code = self._ensure_full_code(code)
                        records.append({"fs_code": fs_code, "name": row[2]})

                if not records:
                    return None

                df = pd.DataFrame(records)
                logger.info(f"[baostock] 获取指数成分股成功: {index_code}, {len(df)}只")
                return df
            finally:
                bs.logout()
        except ImportError:
            logger.debug("[baostock] baostock 未安装")
            return None
        except Exception as e:
            logger.warning(f"[baostock] 获取指数成分股失败: {e}")
            return None

    def _fetch_csindex_perf(self, index_code: str, start_date: str = "20100101") -> Optional[pd.DataFrame]:
        """
        从中证指数官网获取指数行情+PE历史数据。

        CSIndex API 返回字段包括 peg (即PE) 和收盘价等。

        Returns:
            包含 date, close, pe 的 DataFrame，或 None。
        """
        import datetime as dt

        logger.debug(f"获取CSIndex指数行情: {index_code}")

        end_date = dt.date.today().strftime("%Y%m%d")
        params = {
            "indexCode": index_code,
            "startDate": start_date,
            "endDate": end_date,
        }
        headers = {"Referer": "https://www.csindex.com.cn/"}

        data = self._make_request(self.CSINDEX_PERF_URL, params=params, headers=headers)

        if not data:
            return None

        try:
            # CSIndex returns {"code": "200", "data": [...]}
            items = data.get("data", []) if isinstance(data, dict) else data

            if not items:
                logger.warning(f"[eastmoney_index] CSIndex 数据为空: {index_code}")
                return None

            records = []
            for item in items:
                records.append({
                    "date": pd.to_datetime(str(item.get("tradeDate", "")), format="%Y%m%d"),
                    "close": item.get("close"),
                    "pe": item.get("peg"),  # CSIndex 的 peg 字段实际是 PE
                })

            df = pd.DataFrame(records)
            logger.info(f"获取CSIndex指数行情成功: {index_code}, {len(df)}条")
            return df

        except Exception as e:
            logger.error(f"解析CSIndex指数行情失败: {e}")
            return None

    def _fetch_index_pe(self, symbol: str) -> Optional[pd.DataFrame]:
        """从中证指数官网获取指数 PE 历史"""
        index_code = self._resolve_index_code(symbol)
        if not index_code:
            return None

        logger.debug(f"获取指数PE: {symbol} -> {index_code}")

        raw_df = self._fetch_csindex_perf(index_code)
        if raw_df is None or raw_df.empty:
            return None

        try:
            df = pd.DataFrame({
                "date": raw_df["date"],
                "index_val": raw_df["close"],
                "pe": raw_df["pe"],
                "pe_ttm": raw_df["pe"],  # CSIndex 仅提供一种 PE
            })
            logger.info(f"获取指数PE成功: {symbol}, {len(df)}条")
            return df
        except Exception as e:
            logger.error(f"解析指数PE失败: {e}")
            return None

    def _fetch_index_pb(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        获取指数 PB 历史。

        注意: 中证指数官网免费API不提供PB数据。
        返回 None 并记录警告。如需PB数据请接入付费数据源。
        """
        logger.warning(
            f"[eastmoney_index] 指数PB历史数据暂不可用 (中证指数官网不提供PB): {symbol}。"
            " 如需PB数据请接入 Wind / Choice 等付费数据源。"
        )
        return None

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_index_constituents(self, index_code: str) -> pd.DataFrame:
        """获取指数成分股列表

        数据源优先级: 东方财富 → 中证指数官网 → BaoStock
        带24小时缓存 + stale fallback。
        """
        cache_key = f"index_cons:{index_code}"
        result = self._cached_request(
            cache_key, self.TTL_CONSTITUENTS,
            lambda: self._fetch_index_constituents(index_code)
        )
        if result is not None:
            return result

        # Fallback 1: CSIndex
        logger.warning(f"东财指数成分股失败，尝试中证指数备用源: {index_code}")
        backup = self._fetch_index_constituents_csindex(index_code)
        if backup is not None and not backup.empty:
            self._cache.set(cache_key, backup, ttl=self.TTL_CONSTITUENTS)
            return backup

        # Fallback 2: BaoStock
        logger.warning(f"中证指数备用源也失败，尝试 BaoStock: {index_code}")
        bao = self._fetch_index_constituents_baostock(index_code)
        if bao is not None and not bao.empty:
            self._cache.set(cache_key, bao, ttl=self.TTL_CONSTITUENTS)
            return bao

        return pd.DataFrame(columns=["fs_code", "name"])

    def get_index_pe(self, symbol: str) -> pd.DataFrame:
        """获取指数 PE 历史（带缓存，数据源: 中证指数官网）"""
        index_code = self._resolve_index_code(symbol)
        if not index_code:
            return pd.DataFrame(columns=["date", "index_val", "pe", "pe_ttm"])
        cache_key = f"index_pe:{index_code}"
        result = self._cached_request(
            cache_key, self.TTL_VALUATION,
            lambda: self._fetch_index_pe(symbol)
        )
        return result if result is not None else pd.DataFrame(columns=["date", "index_val", "pe", "pe_ttm"])

    def get_index_pb(self, symbol: str) -> pd.DataFrame:
        """获取指数 PB 历史（暂不可用 - 中证指数官网不提供 PB）"""
        index_code = self._resolve_index_code(symbol)
        if not index_code:
            return pd.DataFrame(columns=["date", "index_val", "pb"])
        cache_key = f"index_pb:{index_code}"
        result = self._cached_request(
            cache_key, self.TTL_VALUATION,
            lambda: self._fetch_index_pb(symbol)
        )
        return result if result is not None else pd.DataFrame(columns=["date", "index_val", "pb"])
