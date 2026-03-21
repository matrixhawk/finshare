"""
IndustryClient - 行业分类 + 成分股 + 申万行业分析客户端

提供东财行业板块列表、成分股查询，以及申万一/二/三级行业数据。
"""

import pandas as pd
from typing import Optional

from finshare.stock.base_client import BaseClient
from finshare.logger import logger


# FS filter codes for different board types
_FS_MAP = {
    "dc": "m:90+t:2+f:!50",    # 东财行业
    "sw1": "m:90+t:1+f:!50",   # 申万一级
    "sw2": "m:90+t:3+f:!50",   # 申万二级
    "sw3": "m:90+t:4+f:!50",   # 申万三级
}

_LEVEL_TO_FS = {1: "sw1", 2: "sw2", 3: "sw3"}


class IndustryClient(BaseClient):
    """行业分类 + 成分股 + 申万行业分析客户端"""

    CLIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
    KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

    _COMMON_PARAMS = {
        "pn": 1,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
    }

    _HEADERS = {"Referer": "https://quote.eastmoney.com/"}

    # Cache TTLs (seconds)
    TTL_LIST = 86400          # 24h — rarely changes
    TTL_CONSTITUENTS = 86400  # 24h
    TTL_ANALYSIS = 3600       # 1h  — daily update

    def __init__(self):
        super().__init__("eastmoney_industry")

    # ------------------------------------------------------------------
    # DRY helpers
    # ------------------------------------------------------------------

    def _board_request(self, fs: str, pz: str = "2000") -> Optional[dict]:
        """共用的板块列表请求逻辑"""
        params = {
            **self._COMMON_PARAMS,
            "fs": fs,
            "fields": "f12,f14,f3",
            "pz": pz,
        }
        return self._make_request(self.CLIST_URL, params=params, headers=self._HEADERS)

    def _board_cons_request(self, board_code: str) -> Optional[dict]:
        """共用的板块成分股请求逻辑"""
        params = {
            **self._COMMON_PARAMS,
            "fs": f"b:{board_code}+f:!50",
            "fields": "f12,f14",
            "pz": "5000",
        }
        return self._make_request(self.CLIST_URL, params=params, headers=self._HEADERS)

    def _parse_board_list(self, data: Optional[dict], rename_cols: bool = False) -> pd.DataFrame:
        """
        解析板块列表响应。

        Args:
            data: API 响应 JSON
            rename_cols: True 时将 board_code/board_name 重命名为 industry_code/industry_name

        Returns:
            DataFrame
        """
        if rename_cols:
            empty = pd.DataFrame(columns=["industry_code", "industry_name"])
        else:
            empty = pd.DataFrame(columns=["board_code", "board_name", "change_pct"])

        if not data:
            return empty

        try:
            data_obj = data.get("data") or {}
            diff = data_obj.get("diff", []) if isinstance(data_obj, dict) else []
            if isinstance(diff, dict):
                diff = list(diff.values())

            if not diff:
                return empty

            records = []
            for item in diff:
                records.append({
                    "board_code": str(item.get("f12", "")),
                    "board_name": item.get("f14", ""),
                    "change_pct": item.get("f3"),
                })

            df = pd.DataFrame(records)

            if rename_cols:
                df = df.rename(columns={"board_code": "industry_code", "board_name": "industry_name"})
                df = df[["industry_code", "industry_name"]]

            return df

        except Exception as e:
            logger.error(f"[{self.source_name}] 解析板块列表失败: {e}")
            return empty

    def _parse_constituents(self, data: Optional[dict]) -> pd.DataFrame:
        """解析成分股响应"""
        empty = pd.DataFrame(columns=["fs_code", "name"])

        if not data:
            return empty

        try:
            data_obj = data.get("data") or {}
            diff = data_obj.get("diff", []) if isinstance(data_obj, dict) else []
            if isinstance(diff, dict):
                diff = list(diff.values())

            if not diff:
                return empty

            records = []
            for item in diff:
                raw_code = str(item.get("f12", ""))
                name = item.get("f14", "")
                fs_code = self._ensure_full_code(raw_code)
                records.append({"fs_code": fs_code, "name": name})

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[{self.source_name}] 解析成分股失败: {e}")
            return empty

    # ------------------------------------------------------------------
    # Fetch helpers (return None on failure for _cached_request)
    # ------------------------------------------------------------------

    def _fetch_industry_list(self) -> Optional[pd.DataFrame]:
        """获取东财行业板块列表（主接口）"""
        logger.debug("获取东财行业板块列表")
        data = self._board_request(_FS_MAP["dc"])
        df = self._parse_board_list(data)
        if df.empty:
            return None
        logger.info(f"获取东财行业板块列表成功: {len(df)}个")
        return df

    def _fetch_industry_list_backup(self) -> Optional[pd.DataFrame]:
        """备用端点：push2.eastmoney.com 行业板块"""
        try:
            backup_url = "https://push2.eastmoney.com/api/qt/clist/get"
            params = {
                "fs": "m:90+t:2",
                "fields": "f12,f14,f3",
                "pn": 1, "pz": 2000, "fltt": 2,
            }
            headers = {"Referer": "https://data.eastmoney.com/"}
            data = self._make_request(backup_url, params=params, headers=headers)
            df = self._parse_board_list(data)
            if df.empty:
                return None
            logger.info(f"行业列表备用端点成功: {len(df)}个")
            return df
        except Exception as e:
            logger.warning(f"行业列表备用端点失败: {e}")
            return None

    def _fetch_industry_constituents(self, board_name: str) -> Optional[pd.DataFrame]:
        """获取东财行业成分股（内部逻辑）"""
        # First look up board_code by name
        board_list = self.get_industry_list()
        if board_list.empty:
            logger.warning(f"[{self.source_name}] 无法获取板块列表，无法查找: {board_name}")
            return None

        matched = board_list[board_list["board_name"] == board_name]
        if matched.empty:
            logger.warning(f"[{self.source_name}] 未找到板块: {board_name}")
            return None

        board_code = matched.iloc[0]["board_code"]
        logger.debug(f"获取东财行业成分股: {board_name} (board_code={board_code})")

        data = self._board_cons_request(board_code)
        df = self._parse_constituents(data)
        if df.empty:
            return None
        logger.info(f"获取东财行业成分股成功: {board_name}, {len(df)}只")
        return df

    def _fetch_sw_industry_list(self, level: int) -> Optional[pd.DataFrame]:
        """获取申万行业列表（内部逻辑）"""
        fs_key = _LEVEL_TO_FS.get(level)
        if not fs_key:
            return None

        logger.debug(f"获取申万{level}级行业列表")
        data = self._board_request(_FS_MAP[fs_key])
        df = self._parse_board_list(data, rename_cols=True)
        if df.empty:
            return None
        logger.info(f"获取申万{level}级行业列表成功: {len(df)}个")
        return df

    def _fetch_sw_industry_constituents(self, industry_code: str) -> Optional[pd.DataFrame]:
        """获取申万行业成分股（内部逻辑）"""
        logger.debug(f"获取申万行业成分股: {industry_code}")
        data = self._board_cons_request(industry_code)
        df = self._parse_constituents(data)
        if df.empty:
            return None
        logger.info(f"获取申万行业成分股成功: {industry_code}, {len(df)}只")
        return df

    def _fetch_sw_industry_analysis(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        level: int,
    ) -> Optional[pd.DataFrame]:
        """获取申万行业指数 K 线分析数据（内部逻辑）"""
        # Get industry list for this level
        industry_list = self.get_sw_industry_list(level=level)
        if industry_list.empty:
            logger.warning(f"[{self.source_name}] 申万{level}级行业列表为空")
            return None

        all_records = []

        for _, row in industry_list.iterrows():
            code = row["industry_code"]
            name = row["industry_name"]

            params = {
                "secid": f"90.{code}",
                "klt": 101,   # daily
                "fqt": 1,
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            }
            if start_date:
                params["beg"] = start_date
            if end_date:
                params["end"] = end_date

            data = self._make_request(self.KLINE_URL, params=params, headers=self._HEADERS)

            if not data:
                continue

            try:
                klines = data.get("data", {})
                if isinstance(klines, dict):
                    klines = klines.get("klines", [])

                if not klines:
                    continue

                for kline in klines:
                    parts = kline.split(",")
                    if len(parts) < 2:
                        continue
                    # f51=date, f53=close, f57=change_pct
                    trade_date = parts[0] if len(parts) > 0 else None
                    close = float(parts[2]) if len(parts) > 2 and parts[2] else None
                    change_pct = float(parts[6]) if len(parts) > 6 and parts[6] else None

                    all_records.append({
                        "industry_code": code,
                        "industry_name": name,
                        "trade_date": trade_date,
                        "close": close,
                        "change_pct": change_pct,
                        "pe": None,
                        "pb": None,
                        "dividend_yield": None,
                    })

            except Exception as e:
                logger.warning(f"[{self.source_name}] 解析 {code} K线失败: {e}")
                continue

        if not all_records:
            return None

        df = pd.DataFrame(all_records)
        logger.info(f"获取申万{level}级行业分析成功: {len(industry_list)}个行业, {len(df)}条记录")
        return df

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_industry_list(self) -> pd.DataFrame:
        """
        获取东财行业板块列表。

        Returns:
            DataFrame 包含列:
            - board_code: 板块代码
            - board_name: 板块名称
            - change_pct: 涨跌幅
        """
        cache_key = "industry_list:dc"
        result = self._cached_request(
            cache_key, self.TTL_LIST,
            self._fetch_industry_list
        )
        if result is None:
            logger.warning("东财行业板块主接口失败，尝试备用端点")
            backup = self._fetch_industry_list_backup()
            if backup is not None and not backup.empty:
                self._cache.set(cache_key, backup, ttl=30)
                return backup
            return pd.DataFrame(columns=["board_code", "board_name", "change_pct"])
        return result

    def get_industry_constituents(self, board_name: str) -> pd.DataFrame:
        """
        获取东财行业成分股列表。

        Args:
            board_name: 板块名称，如 "银行"

        Returns:
            DataFrame 包含列:
            - fs_code: 股票代码，格式 "000001.SZ"
            - name: 股票简称
        """
        cache_key = f"industry_cons:{board_name}"
        result = self._cached_request(
            cache_key, self.TTL_CONSTITUENTS,
            lambda: self._fetch_industry_constituents(board_name)
        )
        return result if result is not None else pd.DataFrame(columns=["fs_code", "name"])

    def get_sw_industry_list(self, level: int = 3) -> pd.DataFrame:
        """
        获取申万行业列表。

        Args:
            level: 申万行业级别 (1/2/3)

        Returns:
            DataFrame 包含列:
            - industry_code: 行业代码
            - industry_name: 行业名称
        """
        empty = pd.DataFrame(columns=["industry_code", "industry_name"])

        fs_key = _LEVEL_TO_FS.get(level)
        if not fs_key:
            logger.warning(f"[{self.source_name}] 不支持的申万行业级别: {level}")
            return empty

        cache_key = f"sw_industry_list:{level}"
        result = self._cached_request(
            cache_key, self.TTL_LIST,
            lambda: self._fetch_sw_industry_list(level)
        )
        return result if result is not None else empty

    def get_sw_industry_constituents(self, industry_code: str) -> pd.DataFrame:
        """
        获取申万行业成分股。

        Args:
            industry_code: 申万行业代码

        Returns:
            DataFrame 包含列:
            - fs_code: 股票代码，格式 "000001.SZ"
            - name: 股票简称
        """
        cache_key = f"sw_cons:{industry_code}"
        result = self._cached_request(
            cache_key, self.TTL_CONSTITUENTS,
            lambda: self._fetch_sw_industry_constituents(industry_code)
        )
        return result if result is not None else pd.DataFrame(columns=["fs_code", "name"])

    def get_sw_industry_analysis(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        level: int = 1,
    ) -> pd.DataFrame:
        """
        获取申万行业指数 K 线分析数据。

        Args:
            start_date: 开始日期，格式 "20260301"
            end_date: 结束日期，格式 "20260320"
            level: 申万行业级别 (1/2/3)

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
        """
        cache_key = f"sw_analysis:{level}:{start_date}:{end_date}"
        result = self._cached_request(
            cache_key, self.TTL_ANALYSIS,
            lambda: self._fetch_sw_industry_analysis(start_date, end_date, level)
        )
        return result if result is not None else pd.DataFrame(columns=[
            "industry_code", "industry_name", "trade_date",
            "close", "change_pct", "pe", "pb", "dividend_yield",
        ])
