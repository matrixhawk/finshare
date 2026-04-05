"""恐贪指数计算器。

合成因子：
- 涨跌停比 (25%) — limit_up / (limit_up + limit_down)
- 融资买入占比 (20%) — margin_buy / total_amount
- 北向资金净流入 (20%) — normalized around ±100亿
- 换手率偏离度 (20%) — avg_turnover vs historical mean
- 市场宽度 (15%) — up_count / (up_count + down_count)

Cold start: <5 days → "数据不足", 5-19 days → warmup mode, ≥20 days → full
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_MIN_HISTORY_DAYS = 5
_FULL_HISTORY_DAYS = 20


class FearGreedCalculator:
    """恐贪指数计算器。"""

    def calculate(
        self,
        overview: pd.DataFrame,
        margin: pd.DataFrame,
        north_flow: float,
        turnover_history: list[float],
    ) -> dict[str, Any]:
        """计算恐贪指数。

        Args:
            overview: 当日市场概览 (up_count, down_count, limit_up, limit_down, total_amount, avg_turnover)
            margin: 当日融资融券 (margin_buy)
            north_flow: 北向资金净流入（元）
            turnover_history: 最近 N 天的平均换手率列表

        Returns:
            dict: index_value (0-100 or None), level (str), components (dict), warmup (bool)
        """
        if len(turnover_history) < _MIN_HISTORY_DAYS:
            return {"index_value": None, "level": "数据不足", "components": {}, "warmup": False}

        warmup = len(turnover_history) < _FULL_HISTORY_DAYS

        try:
            row = overview.iloc[0]
            up = int(row.get("up_count", 0))
            down = int(row.get("down_count", 0))
            limit_up = int(row.get("limit_up", 0))
            limit_down = int(row.get("limit_down", 0))
            total_amount = float(row.get("total_amount", 0))
            avg_turnover = float(row.get("avg_turnover", 0))
        except (IndexError, KeyError):
            return {"index_value": None, "level": "数据不足", "components": {}, "warmup": False}

        margin_buy = 0.0
        if not margin.empty:
            margin_buy = float(margin.iloc[0].get("margin_buy", 0))

        components = {}

        # 1. 涨跌停比 (25%)
        if limit_up + limit_down > 0:
            components["limit_ratio"] = min(limit_up / (limit_up + limit_down) * 100, 100)
        else:
            components["limit_ratio"] = 50.0

        # 2. 融资买入占比 (20%) — typical range 5%-15%, normalize to 0-100
        if total_amount > 0 and margin_buy > 0:
            margin_ratio = margin_buy / total_amount
            components["margin_ratio"] = min(max((margin_ratio - 0.05) / 0.10 * 100, 0), 100)
        else:
            components["margin_ratio"] = 50.0

        # 3. 北向资金 (20%) — ±100亿 → 0-100
        north_norm = (north_flow / 10_000_000_000 + 1) / 2 * 100
        components["north_flow"] = min(max(north_norm, 0), 100)

        # 4. 换手率偏离度 (20%)
        avg_hist = sum(turnover_history) / len(turnover_history)
        if avg_hist > 0:
            deviation = (avg_turnover - avg_hist) / avg_hist
            components["turnover_deviation"] = min(max((deviation + 0.5) / 1.0 * 100, 0), 100)
        else:
            components["turnover_deviation"] = 50.0

        # 5. 市场宽度 (15%)
        if up + down > 0:
            components["market_breadth"] = min(up / (up + down) * 100, 100)
        else:
            components["market_breadth"] = 50.0

        index_value = round(
            components["limit_ratio"] * 0.25
            + components["margin_ratio"] * 0.20
            + components["north_flow"] * 0.20
            + components["turnover_deviation"] * 0.20
            + components["market_breadth"] * 0.15,
            1,
        )
        index_value = min(max(index_value, 0), 100)
        level = self._classify_level(index_value)

        return {"index_value": index_value, "level": level, "components": components, "warmup": warmup}

    def _classify_level(self, value: float) -> str:
        if value <= 20:
            return "极度恐惧"
        elif value <= 40:
            return "恐惧"
        elif value <= 60:
            return "中性"
        elif value <= 80:
            return "贪婪"
        else:
            return "极度贪婪"
