"""
Integration tests for finshare.stock.industry module.
"""

import pytest
import pandas as pd

from finshare.stock.industry import (
    get_industry_list,
    get_industry_constituents,
    get_sw_industry_list,
    get_sw_industry_constituents,
    get_sw_industry_analysis,
)


class TestIndustry:
    @pytest.mark.integration
    def test_industry_list(self):
        df = get_industry_list()
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "board_name" in df.columns
            assert "board_code" in df.columns
            assert len(df) > 20

    @pytest.mark.integration
    def test_industry_list_columns(self):
        df = get_industry_list()
        assert isinstance(df, pd.DataFrame)
        # Even empty DataFrame should have correct columns
        assert "board_code" in df.columns
        assert "board_name" in df.columns
        assert "change_pct" in df.columns

    @pytest.mark.integration
    def test_industry_cons(self):
        df = get_industry_constituents("银行")
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "fs_code" in df.columns
            assert "name" in df.columns
            # Verify code format
            sample = df.iloc[0]["fs_code"]
            assert "." in sample

    @pytest.mark.integration
    def test_industry_cons_invalid(self):
        df = get_industry_constituents("不存在的行业XXX")
        assert isinstance(df, pd.DataFrame)
        assert "fs_code" in df.columns
        assert "name" in df.columns

    @pytest.mark.integration
    def test_sw_list_level1(self):
        df = get_sw_industry_list(level=1)
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "industry_code" in df.columns
            assert "industry_name" in df.columns
            assert len(df) > 20

    @pytest.mark.integration
    def test_sw_list_level2(self):
        df = get_sw_industry_list(level=2)
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "industry_code" in df.columns

    @pytest.mark.integration
    def test_sw_list_level3(self):
        df = get_sw_industry_list(level=3)
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "industry_code" in df.columns

    @pytest.mark.integration
    def test_sw_cons(self):
        # First get a valid code from SW level 1
        sw_list = get_sw_industry_list(level=1)
        if sw_list.empty:
            pytest.skip("申万行业列表为空，跳过成分股测试")

        code = sw_list.iloc[0]["industry_code"]
        df = get_sw_industry_constituents(code)
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "fs_code" in df.columns
            assert "name" in df.columns

    @pytest.mark.integration
    def test_sw_analysis(self):
        df = get_sw_industry_analysis(start_date="20260301", end_date="20260320")
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "industry_code" in df.columns
            assert "industry_name" in df.columns
            assert "trade_date" in df.columns
            assert "close" in df.columns
            assert "change_pct" in df.columns
            assert "pe" in df.columns
            assert "pb" in df.columns
            assert "dividend_yield" in df.columns

    @pytest.mark.integration
    def test_sw_analysis_columns_always_present(self):
        """Even empty result should have correct columns"""
        df = get_sw_industry_analysis(start_date="20260301", end_date="20260320")
        assert isinstance(df, pd.DataFrame)
        expected_cols = [
            "industry_code", "industry_name", "trade_date",
            "close", "change_pct", "pe", "pb", "dividend_yield",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"
