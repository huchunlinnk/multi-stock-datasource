"""
单元测试 - 数据模型

测试 StockQuote 模型的创建、验证和转换功能。
"""

import pytest
from datetime import datetime
import json

from stock_data_normalizer import StockQuote, DataSource


class TestStockQuote:
    """StockQuote 模型测试"""

    # ==================== 创建测试 ====================

    def test_create_minimal(self):
        """测试最小化创建"""
        quote = StockQuote(code="000001", source=DataSource.TENCENT)
        assert quote.code == "000001"
        assert quote.source == DataSource.TENCENT
        assert quote.price == 0.0
        assert quote.name == ""

    def test_create_full(self):
        """测试完整创建"""
        quote = StockQuote(
            code="600000",
            name="浦发银行",
            price=10.5,
            open=10.3,
            high=10.8,
            low=10.2,
            pre_close=10.0,
            change_amount=0.5,
            change_percent=5.0,
            volume=1000000,
            amount=10500000,
            turnover_rate=2.5,
            market_cap=2000,
            total_cap=2500,
            market="SH",
            board="沪A",
            sector="银行",
            is_st=False,
            is_chinext=False,
            is_kcb=False,
            suspended=False,
            source=DataSource.EASTMONEY,
        )

        assert quote.code == "600000"
        assert quote.name == "浦发银行"
        assert quote.price == 10.5
        assert quote.market == "SH"
        assert quote.sector == "银行"

    def test_create_with_different_sources(self):
        """测试使用不同数据源创建"""
        for source in DataSource:
            quote = StockQuote(code="000001", source=source)
            assert quote.source == source

    # ==================== 验证测试 ====================

    def test_validate_code_valid(self):
        """测试有效代码验证"""
        valid_codes = ["000001", "600000", "300001", "688001", "430001"]
        for code in valid_codes:
            quote = StockQuote(code=code, source=DataSource.TENCENT)
            assert quote.code == code

    def test_validate_code_invalid(self):
        """测试无效代码验证"""
        invalid_codes = ["12345", "1234567", "abcdef", "", "12.34"]
        for code in invalid_codes:
            with pytest.raises(ValueError):
                StockQuote(code=code, source=DataSource.TENCENT)

    def test_validate_market(self):
        """测试市场代码标准化"""
        test_cases = [
            ("SH", "SH"),
            ("上海", "SH"),
            ("沪", "SH"),
            ("1", "SH"),
            ("SZ", "SZ"),
            ("深圳", "SZ"),
            ("深", "SZ"),
            ("0", "SZ"),
            ("BJ", "BJ"),
            ("北京", "BJ"),
        ]
        for input_val, expected in test_cases:
            quote = StockQuote(
                code="000001",
                market=input_val,
                source=DataSource.TENCENT
            )
            assert quote.market == expected

    def test_detect_board_from_code(self):
        """测试从代码推断板块"""
        test_cases = [
            ("600000", "沪A"),
            ("000001", "深A"),
            ("300001", "创业板"),
            ("688001", "科创板"),
            ("430001", "北交所"),
        ]
        for code, expected_board in test_cases:
            quote = StockQuote(code=code, source=DataSource.TENCENT)
            assert quote.board == expected_board

    # ==================== 方法测试 ====================

    def test_is_valid(self):
        """测试数据有效性检查"""
        # 有效数据
        valid_quote = StockQuote(
            code="000001",
            price=10.5,
            source=DataSource.TENCENT
        )
        assert valid_quote.is_valid() is True

        # 无效数据 - 价格为0
        invalid_quote1 = StockQuote(
            code="000001",
            price=0,
            source=DataSource.TENCENT
        )
        assert invalid_quote1.is_valid() is False

    def test_calculate_completeness(self):
        """测试完整性计算"""
        # 最小数据
        minimal = StockQuote(code="000001", source=DataSource.TENCENT)
        assert minimal.calculate_completeness() < 0.5

        # 完整数据
        full = StockQuote(
            code="000001",
            name="平安银行",
            price=10.5,
            volume=1000000,
            amount=10000000,
            turnover_rate=2.5,
            market_cap=2000,
            high=10.8,
            low=10.2,
            open=10.3,
            pre_close=10.0,
            sector="银行",
            market="SZ",
            board="深A",
            source=DataSource.TENCENT
        )
        assert full.calculate_completeness() > 0.8

    def test_to_dict(self):
        """测试转换为字典"""
        quote = StockQuote(
            code="000001",
            name="平安银行",
            price=10.5,
            source=DataSource.TENCENT
        )
        data = quote.to_dict()

        assert isinstance(data, dict)
        assert data["code"] == "000001"
        assert data["name"] == "平安银行"
        assert data["price"] == 10.5
        assert data["source"] == "tencent"

    def test_to_json(self):
        """测试转换为 JSON"""
        quote = StockQuote(
            code="000001",
            name="平安银行",
            price=10.5,
            source=DataSource.TENCENT
        )
        json_str = quote.to_json()

        assert isinstance(json_str, str)
        data = json.loads(json_str)
        assert data["code"] == "000001"

    def test_from_dict(self):
        """测试从字典创建"""
        data = {
            "code": "000001",
            "name": "平安银行",
            "price": 10.5,
            "source": "tencent",
        }
        quote = StockQuote.from_dict(data)

        assert quote.code == "000001"
        assert quote.name == "平安银行"
        assert quote.price == 10.5
        assert quote.source == DataSource.TENCENT

    def test_from_json(self):
        """测试从 JSON 创建"""
        json_str = '{"code": "000001", "name": "平安银行", "price": 10.5, "source": "tencent"}'
        quote = StockQuote.from_json(json_str)

        assert quote.code == "000001"
        assert quote.price == 10.5

    def test_serialization_roundtrip(self):
        """测试序列化往返"""
        original = StockQuote(
            code="000001",
            name="平安银行",
            price=10.5,
            volume=1000000,
            source=DataSource.TENCENT
        )

        # 通过字典
        restored = StockQuote.from_dict(original.to_dict())
        assert restored.code == original.code
        assert restored.price == original.price

        # 通过 JSON
        restored2 = StockQuote.from_json(original.to_json())
        assert restored2.code == original.code


class TestDataSource:
    """DataSource 枚举测试"""

    def test_all_sources_exist(self):
        """测试所有数据源都存在"""
        expected_sources = [
            "eastmoney", "tencent", "sina",
            "akshare", "baostock", "joinquant", "tushare"
        ]
        for source in expected_sources:
            assert hasattr(DataSource, source.upper())

    def test_list_all(self):
        """测试获取所有数据源"""
        all_sources = DataSource.list_all()
        assert len(all_sources) == 7
        assert "tencent" in all_sources
        assert "eastmoney" in all_sources

    def test_source_value(self):
        """测试数据源值"""
        assert DataSource.TENCENT.value == "tencent"
        assert DataSource.EASTMONEY.value == "eastmoney"
