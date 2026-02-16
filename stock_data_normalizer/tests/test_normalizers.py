"""
单元测试 - 归一化器

测试各数据源归一化器的功能。
"""

import pytest
from datetime import datetime

from stock_data_normalizer import (
    StockQuote,
    DataSource,
    normalize,
    normalize_batch,
    NormalizerRegistry,
    EastMoneyNormalizer,
    TencentNormalizer,
    SinaNormalizer,
    AkShareNormalizer,
)


class TestNormalizerRegistry:
    """归一化器注册表测试"""

    def test_all_sources_registered(self):
        """测试所有数据源都已注册"""
        for source in DataSource:
            assert NormalizerRegistry.is_registered(source)

    def test_get_normalizer(self):
        """测试获取归一化器"""
        for source in DataSource:
            normalizer = NormalizerRegistry.get(source)
            assert normalizer.source == source

    def test_list_sources(self):
        """测试列出数据源"""
        sources = NormalizerRegistry.list_sources()
        assert len(sources) == 7


class TestEastMoneyNormalizer:
    """东方财富归一化器测试"""

    def setup_method(self):
        self.normalizer = EastMoneyNormalizer()

    def test_normalize_basic(self):
        """测试基础归一化"""
        raw_data = {
            "f12": "000001",
            "f14": "平安银行",
            "f2": 10.5,
            "f3": 2.5,
        }
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "000001"
        assert quote.name == "平安银行"
        assert quote.price == 10.5
        assert quote.change_percent == 2.5
        assert quote.source == DataSource.EASTMONEY

    def test_normalize_full(self):
        """测试完整数据归一化"""
        raw_data = {
            "f12": "600000",
            "f14": "浦发银行",
            "f2": 10.5,
            "f3": 2.5,
            "f5": 1000000,
            "f6": 10500000,
            "f8": 2.5,
            "f15": 10.8,
            "f16": 10.2,
            "f17": 10.3,
            "f18": 10.0,
            "f20": 2000,
            "f21": 2500,
        }
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "600000"
        assert quote.price == 10.5
        assert quote.volume == 1000000
        assert quote.amount == 10500000
        assert quote.turnover_rate == 2.5
        assert quote.high == 10.8
        assert quote.low == 10.2
        assert quote.open == 10.3
        assert quote.pre_close == 10.0
        assert quote.market_cap == 2000
        assert quote.total_cap == 2500
        assert quote.market == "SH"
        assert quote.board == "沪A"

    def test_normalize_chinext(self):
        """测试创业板股票"""
        raw_data = {"f12": "300001", "f14": "特锐德", "f2": 25.0}
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "300001"
        assert quote.is_chinext is True
        assert quote.board == "创业板"
        assert quote.market == "SZ"

    def test_normalize_kcb(self):
        """测试科创板股票"""
        raw_data = {"f12": "688001", "f14": "华兴源创", "f2": 50.0}
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "688001"
        assert quote.is_kcb is True
        assert quote.board == "科创板"

    def test_normalize_st(self):
        """测试 ST 股票"""
        raw_data = {"f12": "000001", "f14": "ST某某", "f2": 5.0}
        quote = self.normalizer.normalize(raw_data)

        assert quote.is_st is True

    def test_normalize_suspended(self):
        """测试停牌股票"""
        raw_data = {"f12": "000001", "f14": "测试股", "f2": 0, "f5": 0}
        quote = self.normalizer.normalize(raw_data)

        assert quote.suspended is True

    def test_normalize_batch(self):
        """测试批量归一化"""
        raw_list = [
            {"f12": "000001", "f14": "平安银行", "f2": 10.5},
            {"f12": "000002", "f14": "万科A", "f2": 15.0},
            {"f12": "000003", "f14": "无效股", "f2": 0},  # 无效数据
        ]
        quotes = self.normalizer.normalize_batch(raw_list, skip_invalid=False)

        assert len(quotes) == 3

    def test_normalize_batch_skip_invalid(self):
        """测试批量归一化并跳过无效数据"""
        raw_list = [
            {"f12": "000001", "f14": "平安银行", "f2": 10.5},
            {"f12": "000002", "f14": "万科A", "f2": 0, "f5": 0},  # 停牌
            {"f12": "000003", "f14": "无效股", "f2": 0},  # 无效
        ]
        quotes = self.normalizer.normalize_batch(raw_list, skip_invalid=True)

        # 只有一条有效数据
        assert len(quotes) == 1
        assert quotes[0].code == "000001"


class TestTencentNormalizer:
    """腾讯财经归一化器测试"""

    def setup_method(self):
        self.normalizer = TencentNormalizer()

    def test_normalize_eastmoney_format(self):
        """测试东财格式数据"""
        raw_data = {"f12": "000001", "f14": "平安银行", "f2": 10.5}
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "000001"
        assert quote.price == 10.5
        assert quote.source == DataSource.TENCENT

    def test_normalize_standard_format(self):
        """测试标准格式数据"""
        raw_data = {
            "code": "000001",
            "name": "平安银行",
            "price": 10.5,
            "sector": "银行",
        }
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "000001"
        assert quote.sector == "银行"

    def test_normalize_with_market_board(self):
        """测试包含市场和板块的数据"""
        raw_data = {
            "f12": "000001",
            "f14": "平安银行",
            "f2": 10.5,
            "market": 0,
            "market_board": "深A",
            "sector": "银行",
        }
        quote = self.normalizer.normalize(raw_data)

        assert quote.market == "SZ"
        assert quote.board == "深A"
        assert quote.sector == "银行"


class TestSinaNormalizer:
    """新浪财经归一化器测试"""

    def setup_method(self):
        self.normalizer = SinaNormalizer()

    def test_normalize(self):
        """测试基础归一化"""
        raw_data = {
            "f12": "000001",
            "f14": "平安银行",
            "f2": 10.5,
            "f3": 2.5,
            "f4": 10.8,  # 新浪的 f4 是最高价
        }
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "000001"
        assert quote.price == 10.5
        assert quote.high == 10.8
        assert quote.source == DataSource.SINA


class TestAkShareNormalizer:
    """AKShare 归一化器测试"""

    def setup_method(self):
        self.normalizer = AkShareNormalizer()

    def test_normalize(self):
        """测试基础归一化"""
        raw_data = {
            "f12": "000001",
            "f14": "平安银行",
            "f2": 10.5,
            "is_chinext": False,
        }
        quote = self.normalizer.normalize(raw_data)

        assert quote.code == "000001"
        assert quote.source == DataSource.AKSHARE


class TestConvenienceFunctions:
    """便捷函数测试"""

    def test_normalize_single(self):
        """测试单条归一化便捷函数"""
        raw_data = {"f12": "000001", "f14": "平安银行", "f2": 10.5}
        quote = normalize(DataSource.EASTMONEY, raw_data)

        assert quote.code == "000001"
        assert quote.source == DataSource.EASTMONEY

    def test_normalize_batch_func(self):
        """测试批量归一化便捷函数"""
        raw_list = [
            {"f12": "000001", "f14": "平安银行", "f2": 10.5},
            {"f12": "000002", "f14": "万科A", "f2": 15.0},
        ]
        quotes = normalize_batch(DataSource.TENCENT, raw_list)

        assert len(quotes) == 2
        assert quotes[0].source == DataSource.TENCENT
