# sources/yahoo_source.py
"""
Yahoo Finance 数据源实现

支持美股历史数据、实时快照和分钟线数据
使用 yfinance 库作为主要数据获取方式
"""

import time
from datetime import date, datetime
from typing import List, Optional, Dict

from finshare.models.data_models import HistoricalData, SnapshotData, AdjustmentType, MarketType, MinuteData
from finshare.logger import logger
from finshare.sources.base_source import BaseDataSource

# 延迟导入 yfinance
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    yf = None  # type: ignore


class YahooFinanceDataSource(BaseDataSource):
    """Yahoo Finance 数据源实现"""

    def __init__(self):
        super().__init__("yahoo")
        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance 未安装，无法使用 Yahoo Finance 数据源")

    def _convert_code_format(self, code: str) -> str:
        """
        转换代码格式为 Yahoo Finance 格式

        支持输入:
        - AAPL.US -> AAPL
        - USAAPL -> AAPL
        - AAPL -> AAPL (保持不变)
        """
        # 移除 .US 后缀
        if ".US" in code:
            return code.replace(".US", "")

        # 移除 US 前缀
        if code.startswith("US"):
            return code[2:]

        # 美股纯字母代码直接返回
        if code.isalpha() and len(code) <= 5:
            return code

        return code

    def _get_market_type(self, code: str) -> MarketType:
        """根据代码判断市场类型"""
        full_code = self._ensure_full_code(code)

        if full_code.startswith("US") or ".US" in full_code:
            return MarketType.US

        # 纯字母代码默认为美股
        clean_code = self._convert_code_format(full_code)
        if clean_code.isalpha():
            return MarketType.US

        return MarketType.US

    def get_historical_data(
        self,
        code: str,
        start_date: date,
        end_date: date,
        adjustment: AdjustmentType = AdjustmentType.NONE,
    ) -> List[HistoricalData]:
        """获取 Yahoo Finance 历史数据"""
        if not YFINANCE_AVAILABLE:
            logger.warning("yfinance 未安装，无法获取历史数据")
            return []

        try:
            symbol = self._convert_code_format(code)

            # yfinance 不支持复权参数
            if adjustment != AdjustmentType.NONE:
                logger.debug(f"Yahoo Finance (yfinance) 不支持复权类型 {adjustment}，返回原始数据")

            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date)

            if df.empty:
                logger.warning(f"Yahoo Finance 历史数据为空: {symbol}")
                return []

            return self._parse_yahoo_historical_data(df, symbol)

        except Exception as e:
            error_msg = f"获取 Yahoo Finance 历史数据失败 {code}: {e}"
            logger.error(error_msg)
            return []

    def _parse_yahoo_historical_data(self, df, symbol: str) -> List[HistoricalData]:
        """解析 yfinance 历史数据格式"""
        try:
            historical_list = []

            for idx, row in df.iterrows():
                trade_date = idx.date()

                open_price = float(row['Open'])
                high = float(row['High'])
                low = float(row['Low'])
                close = float(row['Close'])
                volume = float(row['Volume'])

                # yfinance 返回的就是未调整的价格
                adjusted_close = close

                historical_list.append(
                    HistoricalData(
                        code=f"{symbol}.US",
                        trade_date=trade_date,
                        open_price=open_price,
                        high_price=high,
                        low_price=low,
                        close_price=close,
                        adjusted_close=adjusted_close,
                        volume=volume,
                        amount=0,  # yfinance 不提供成交额
                    )
                )

            return historical_list

        except Exception as e:
            logger.error(f"解析 Yahoo 历史数据失败: {e}")
            return []

    def get_snapshot_data(self, code: str) -> Optional[SnapshotData]:
        """获取 Yahoo Finance 实时快照数据"""
        if not YFINANCE_AVAILABLE:
            logger.warning("yfinance 未安装，无法获取快照数据")
            return None

        try:
            symbol = self._convert_code_format(code)

            ticker = yf.Ticker(symbol)
            info = ticker.info

            if not info or 'regularMarketPrice' not in info:
                logger.warning(f"Yahoo Finance 快照数据为空: {symbol}")
                return None

            return self._parse_yahoo_snapshot(info, code)

        except Exception as e:
            error_msg = f"获取 Yahoo Finance 快照数据失败 {code}: {e}"
            logger.error(error_msg)
            return None

    def _parse_yahoo_snapshot(self, info: dict, code: str) -> Optional[SnapshotData]:
        """解析 Yahoo Finance 快照数据格式"""
        try:
            last_price = info.get('regularMarketPrice')
            if last_price is None:
                return None

            previous_close = info.get('previousClose', info.get('chartPreviousClose', last_price))
            open_price = info.get('regularMarketOpen', last_price)
            day_high = info.get('regularMarketDayHigh', last_price)
            day_low = info.get('regularMarketDayLow', last_price)
            volume = info.get('regularMarketVolume', 0)

            # 涨跌额和涨跌幅
            change = last_price - previous_close if previous_close else 0
            pct_change = (change / previous_close * 100) if previous_close else 0

            # 成交额估算
            amount = last_price * volume if volume else 0

            full_code = self._ensure_full_code(code)

            return SnapshotData(
                code=full_code,
                timestamp=datetime.now(),
                last_price=last_price,
                day_open=open_price,
                day_high=day_high,
                day_low=day_low,
                prev_close=previous_close,
                volume=volume,
                amount=amount,
                market=MarketType.US,
                data_source="yahoo",
            )

        except Exception as e:
            logger.error(f"解析 Yahoo 快照数据失败: {e}")
            return None

    def get_minutely_data(
        self,
        code: str,
        start: datetime,
        end: datetime,
        frequency: str = "5",
    ) -> List[MinuteData]:
        """获取 Yahoo Finance 分钟线数据"""
        if not YFINANCE_AVAILABLE:
            logger.warning("yfinance 未安装，无法获取分钟数据")
            return []

        try:
            symbol = self._convert_code_format(code)

            # 转换频率
            freq_map = {
                "1": "1m",
                "5": "5m",
                "15": "15m",
                "30": "30m",
                "60": "60m",
            }
            interval = freq_map.get(frequency, "5m")

            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start, end=end, interval=interval)

            if df.empty:
                logger.warning(f"Yahoo Finance 分钟数据为空: {symbol}")
                return []

            return self._parse_yahoo_minutely_data(df, code, frequency)

        except Exception as e:
            error_msg = f"获取 Yahoo Finance 分钟数据失败 {code}: {e}"
            logger.error(error_msg)
            return []

    def _parse_yahoo_minutely_data(
        self, df, code: str, frequency: str
    ) -> List[MinuteData]:
        """解析 Yahoo Finance 分钟线数据格式"""
        try:
            minute_list = []

            for idx, row in df.iterrows():
                open_price = float(row['Open'])
                high = float(row['High'])
                low = float(row['Low'])
                close = float(row['Close'])
                volume = float(row['Volume'])

                # 估算成交额
                amount = close * volume

                trade_time = idx.strftime("%Y%m%d%H%M%S")

                full_code = self._ensure_full_code(code)

                minute_list.append(
                    MinuteData(
                        fs_code=full_code,
                        trade_time=trade_time,
                        open=open_price,
                        high=high,
                        low=low,
                        close=close,
                        volume=volume,
                        amount=amount,
                        frequency=frequency,
                        data_source="yahoo",
                    )
                )

            return minute_list

        except Exception as e:
            logger.error(f"解析 Yahoo 分钟数据失败: {e}")
            return []

    def get_batch_snapshots(self, codes: List[str]) -> Dict[str, SnapshotData]:
        """批量获取 Yahoo Finance 快照数据"""
        results = {}

        for code in codes:
            snapshot = self.get_snapshot_data(code)
            if snapshot:
                results[code] = snapshot
            # 避免请求过快
            time.sleep(0.5)

        return results
