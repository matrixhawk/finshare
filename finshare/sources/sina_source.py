# sources/sina_source.py
"""
新浪财经数据源实现

注意：新浪数据源仅用于获取实时快照数据，不支持历史K线数据。
历史数据请使用东方财富或腾讯数据源。
"""

import re
import time
from datetime import date, datetime
from typing import List, Optional, Dict

import requests
import pandas as pd

from finshare.sources.base_source import BaseDataSource
from finshare.models.data_models import HistoricalData, SnapshotData, AdjustmentType, MarketType
from finshare.logger import logger


def _safe_float(val: str, default: float = 0.0) -> float:
    """Convert string to float safely."""
    try:
        return float(str(val).replace("%", "").replace(",", ""))
    except (ValueError, TypeError):
        return default


class SinaDataSource(BaseDataSource):
    """
    新浪财经数据源实现

    仅支持实时快照数据获取，不支持历史K线。
    成交量单位：股 -> 手（÷100）
    """

    def __init__(self):
        super().__init__("sina")
        self.base_url = "https://hq.sinajs.cn"

        # 新浪API特定headers（必须设置正确的Referer避免403）
        self.headers = {
            "Referer": "https://finance.sina.com.cn/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Host": "hq.sinajs.cn",
        }
        # 更新session的headers
        self.session.headers.update(self.headers)

    def get_historical_data(
        self,
        code: str,
        start_date: date,
        end_date: date,
        adjustment: AdjustmentType = AdjustmentType.NONE,
    ) -> List[HistoricalData]:
        """
        新浪数据源不支持历史K线数据

        请使用东方财富或腾讯数据源获取历史K线。
        """
        raise NotImplementedError(
            "新浪数据源不支持历史K线数据，请使用 EastMoneyDataSource 或 TencentDataSource"
        )

    def get_snapshot_data(self, code: str) -> Optional[SnapshotData]:
        """
        获取新浪实时快照数据

        返回数据单位：
        - 价格: 元
        - 成交量: 手
        - 成交额: 元
        """
        try:
            full_code = self._ensure_full_code(code)
            market_code = self._convert_code_format(full_code)
            url = f"{self.base_url}/list={market_code}"

            response_data = self._make_request(url)

            if not response_data:
                return None

            snapshot = self._parse_sina_snapshot(response_data, full_code)

            if snapshot:
                logger.debug(f"新浪快照数据获取成功: {full_code}")

            return snapshot

        except Exception as e:
            error_msg = f"获取新浪快照数据失败 {code}: {e}"
            logger.error(error_msg)
            return None

    def get_batch_snapshots(self, codes: List[str]) -> Dict[str, SnapshotData]:
        """
        批量获取新浪快照数据

        返回数据单位：
        - 价格: 元
        - 成交量: 手
        - 成交额: 元
        """
        results = {}
        full_codes = [self._ensure_full_code(code) for code in codes]

        # 新浪API支持批量查询，最大80个代码
        max_batch_size = 80

        for i in range(0, len(full_codes), max_batch_size):
            batch = full_codes[i : i + max_batch_size]
            market_codes = [self._convert_code_format(code) for code in batch]

            query_str = ",".join(market_codes)
            url = f"{self.base_url}/list={query_str}"

            try:
                response_data = self._make_request(url)

                if response_data:
                    batch_results = self._parse_sina_batch_snapshots(response_data, batch)
                    results.update(batch_results)
                    logger.debug(f"新浪批量快照获取: {len(batch_results)}/{len(batch)} 成功")

                time.sleep(5)

            except Exception as e:
                logger.error(f"新浪批量快照获取失败: {e}")

        return results

    def _parse_sina_snapshot(self, content: str, code: str) -> Optional[SnapshotData]:
        """
        解析新浪快照数据格式

        新浪原始数据成交量单位为股，这里转换为手（÷100）
        """
        try:
            full_code = self._ensure_full_code(code)

            # 新浪格式: var hq_str_sh600000="浦发银行,12.34,12.35,...";
            pattern = r'var hq_str_(?:sh|sz|bj)(\d+)=["\'](.*?)["\'];'
            matches = re.findall(pattern, content)

            if not matches:
                return None

            for match_code, data_str in matches:
                clean_target = full_code.replace("SH", "").replace("SZ", "").replace("BJ", "")
                if match_code != clean_target:
                    continue

                data_parts = data_str.split(",")
                if len(data_parts) < 30:
                    continue

                # 解析字段
                open_price = float(data_parts[1]) if data_parts[1] else 0.0
                pre_close = float(data_parts[2]) if data_parts[2] else 0.0
                current_price = float(data_parts[3]) if data_parts[3] else 0.0
                high_price = float(data_parts[4]) if data_parts[4] else 0.0
                low_price = float(data_parts[5]) if data_parts[5] else 0.0
                bid_price = float(data_parts[6]) if data_parts[6] else 0.0
                ask_price = float(data_parts[7]) if data_parts[7] else 0.0
                # 新浪成交量单位为股，转换为手（÷100）
                volume_shares = float(data_parts[8]) if data_parts[8] else 0.0
                volume = volume_shares / 100  # 股 -> 手
                amount = float(data_parts[9]) if data_parts[9] else 0.0

                # 买一到买五
                bid_prices = []
                bid_volumes = []
                for i in range(10, 20, 2):
                    if i < len(data_parts) and data_parts[i]:
                        bid_prices.append(float(data_parts[i]))
                        # 买卖盘量也是股，转换为手
                        bid_volumes.append(float(data_parts[i + 1]) / 100)

                # 卖一到卖五
                ask_prices = []
                ask_volumes = []
                for i in range(20, 30, 2):
                    if i < len(data_parts) and data_parts[i]:
                        ask_prices.append(float(data_parts[i]))
                        ask_volumes.append(float(data_parts[i + 1]) / 100)

                # 日期和时间
                date_str = data_parts[30] if len(data_parts) > 30 else ""
                time_str = data_parts[31] if len(data_parts) > 31 else ""

                if date_str and time_str:
                    try:
                        timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                    except:
                        timestamp = datetime.now()
                else:
                    timestamp = datetime.now()

                snapshot = SnapshotData(
                    code=full_code,
                    timestamp=timestamp,
                    last_price=current_price,
                    volume=volume,  # 已转换为手
                    amount=amount,
                    bid1_price=bid_prices[0] if bid_prices else current_price,
                    ask1_price=ask_prices[0] if ask_prices else current_price,
                    bid1_volume=bid_volumes[0] if bid_volumes else 0,
                    ask1_volume=ask_volumes[0] if ask_volumes else 0,
                    day_high=high_price,
                    day_low=low_price,
                    day_open=open_price,
                    prev_close=pre_close,
                    is_trading=current_price > 0,
                    market=self._get_market_type(full_code),
                    data_source=self.source_name,
                )

                return snapshot

        except Exception as e:
            logger.error(f"解析新浪快照数据失败: {e}")

        return None

    def _parse_sina_batch_snapshots(
        self, content: str, codes: List[str]
    ) -> Dict[str, SnapshotData]:
        """解析新浪批量快照数据"""
        results = {}
        lines = content.strip().split(";\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            pattern = r'var hq_str_(sh|sz|bj)(\d+)=["\'](.*?)["\']'
            match = re.search(pattern, line)

            if not match:
                continue

            market_prefix = match.group(1)
            code_num = match.group(2)

            # 构造标准代码
            if market_prefix == "sh":
                full_code = f"SH{code_num}"
            elif market_prefix == "sz":
                full_code = f"SZ{code_num}"
            elif market_prefix == "bj":
                full_code = f"BJ{code_num}"
            else:
                continue

            # 只处理请求的代码
            if full_code not in codes:
                for request_code in codes:
                    if (
                        request_code.replace("SH", "").replace("SZ", "").replace("BJ", "")
                        == code_num
                    ):
                        full_code = request_code
                        break
                else:
                    continue

            snapshot = self._parse_sina_snapshot(line, full_code)
            if snapshot:
                results[full_code] = snapshot

        return results

    def get_concept_list(self) -> pd.DataFrame:
        """获取新浪概念板块列表。"""
        try:
            resp = requests.get(
                "https://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"},
                timeout=10,
            )
            if resp.status_code != 200:
                return pd.DataFrame()
            text = resp.text
            # Sina returns JS variable assignments with arrays
            items = re.findall(r'"([^"]+)","([^"]*)"', text)
            if not items:
                return pd.DataFrame()
            records = []
            for i in range(0, len(items), 2):
                if i + 1 < len(items):
                    name = items[i][0] if items[i] else ""
                    pct = items[i][1] if items[i] else "0"
                    if name:
                        records.append({"board_name": name, "change_pct": _safe_float(pct)})
            if not records:
                # Fallback: try simpler pattern
                simple_items = re.findall(r'\[?"([^"]{2,20}?)","(-?[\d.]+%?)"', text)
                for name, pct in simple_items:
                    records.append({"board_name": name, "change_pct": _safe_float(pct)})
            return pd.DataFrame(records) if records else pd.DataFrame()
        except Exception as e:
            logger.warning(f"[SinaDataSource] get_concept_list failed: {e}")
            return pd.DataFrame()

    def get_minutely_data(self, code: str, start: str = None, end: str = None, freq: int = 5) -> pd.DataFrame:
        """获取新浪分钟K线数据。"""
        try:
            sina_code = self._convert_code_format(code)
            resp = requests.get(
                "https://quotes.sina.cn/cn/api/jsonp.php/var/InnerExtensionService.getMinKLine",
                params={"symbol": sina_code, "scale": str(freq), "datalen": "240"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            if resp.status_code != 200:
                return pd.DataFrame()
            text = resp.text
            # Parse JSONP: var="day,open,high,low,close,volume\n..."
            match = re.search(r'="(.+)"', text, re.DOTALL)
            if not match:
                return pd.DataFrame()
            csv_data = match.group(1)
            lines = csv_data.strip().split("\\n")
            if len(lines) < 2:
                return pd.DataFrame()
            records = []
            for line in lines[1:]:  # skip header
                parts = line.split(",")
                if len(parts) < 6:
                    continue
                records.append({
                    "trade_time": parts[0],
                    "open": _safe_float(parts[1]),
                    "high": _safe_float(parts[2]),
                    "low": _safe_float(parts[3]),
                    "close": _safe_float(parts[4]),
                    "volume": _safe_float(parts[5]),
                })
            return pd.DataFrame(records)
        except Exception as e:
            logger.warning(f"[SinaDataSource] get_minutely_data failed: {e}")
            return pd.DataFrame()

    def _convert_code_format(self, code: str) -> str:
        """转换代码格式为新浪API格式"""
        full_code = self._ensure_full_code(code)
        clean_code = full_code.replace("SH", "").replace("SZ", "").replace("BJ", "").replace("HK", "").replace("US", "")

        if full_code.startswith("HK"):
            return f"hk{clean_code}"
        elif full_code.startswith("US"):
            return f"us{clean_code}"
        elif full_code.startswith("SH") or (clean_code and clean_code[0] in ["6", "5"]):
            return f"sh{clean_code}"
        elif full_code.startswith("SZ") or (clean_code and clean_code[0] in ["0", "1", "2", "3"]):
            return f"sz{clean_code}"
        elif full_code.startswith("BJ") or (
            clean_code and clean_code[0] == "9" and not clean_code.startswith("90")
        ):
            return f"bj{clean_code}"
        else:
            return code

    def _get_market_type(self, code: str) -> MarketType:
        """根据代码判断市场类型"""
        full_code = self._ensure_full_code(code)

        if full_code.startswith("SH"):
            return MarketType.SH
        elif full_code.startswith("SZ"):
            return MarketType.SZ
        elif full_code.startswith("BJ"):
            return MarketType.BJ
        elif full_code.startswith("HK"):
            return MarketType.HK
        else:
            clean_code = full_code.replace("SH", "").replace("SZ", "").replace("BJ", "")
            if clean_code and clean_code[0].isdigit():
                first_digit = clean_code[0]
                if first_digit in ["6", "5"]:
                    return MarketType.SH
                elif first_digit in ["0", "1", "2", "3"]:
                    return MarketType.SZ
                elif first_digit == "9":
                    if clean_code.startswith("90"):
                        return MarketType.SH
                    else:
                        return MarketType.BJ

        return MarketType.SH
