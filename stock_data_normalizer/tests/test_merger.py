"""
单元测试 - 数据合并器

测试多源数据合并功能。
"""

import pytest
from datetime import datetime

from stock_data_normalizer import (
    StockQuote,
    DataSource,
    DataMerger,
    merge_quotes,
)


class TestDataMerger:
    """数据合并器测试"""

    def setup_method(self):
        self.merger = DataMerger()

    def _create_quote(
        self,
        code: str,
        name: str,
        price: float,
        source: DataSource,
        **kwargs
    ) -> StockQuote:
        """创建测试用 StockQuote"""
        return StockQuote(
            code=code,
            name=name,
            price=price,
            source=source,
            **kwargs
        )

    # ==================== 基础合并测试 ====================

    def test_merge_empty(self):
        """测试空数据合并"""
        result = self.merger.merge([])
        assert result == []

    def test_merge_single_source(self):
        """测试单数据源合并"""
        quotes = [
            self._create_quote("000001", "平安银行", 10.5, DataSource.TENCENT),
            self._create_quote("000002", "万科A", 15.0, DataSource.TENCENT),
        ]
        result = self.merger.merge([quotes])

        assert len(result) == 2
        assert result[0].code == "000001"
        assert result[1].code == "000002"

    def test_merge_multiple_sources(self):
        """测试多数据源合并"""
        tencent = [
            self._create_quote("000001", "平安银行", 10.5, DataSource.TENCENT),
            self._create_quote("000002", "万科A", 15.0, DataSource.TENCENT),
        ]
        eastmoney = [
            self._create_quote("000001", "平安银行", 10.6, DataSource.EASTMONEY),
            self._create_quote("000003", "测试股", 20.0, DataSource.EASTMONEY),
        ]

        result = self.merger.merge([tencent, eastmoney])

        # 应该有3只股票 (000001, 000002, 000003)
        assert len(result) == 3

        # 检查 000001 使用了腾讯数据（权重更高）
        quote_001 = next(q for q in result if q.code == "000001")
        assert quote_001.source == DataSource.TENCENT
        assert quote_001.price == 10.5

    def test_merge_deduplication(self):
        """测试去重"""
        # 同一只股票在多个源中
        quotes1 = [
            self._create_quote("000001", "平安银行", 10.5, DataSource.TENCENT),
        ]
        quotes2 = [
            self._create_quote("000001", "平安银行", 10.6, DataSource.EASTMONEY),
        ]
        quotes3 = [
            self._create_quote("000001", "平安银行", 10.4, DataSource.SINA),
        ]

        result = self.merger.merge([quotes1, quotes2, quotes3])

        # 只应该有一只股票
        assert len(result) == 1
        assert result[0].code == "000001"

    # ==================== 质量评分测试 ====================

    def test_quality_score_calculation(self):
        """测试质量分计算"""
        # 完整数据
        full_quote = self._create_quote(
            "000001", "平安银行", 10.5, DataSource.TENCENT,
            volume=1000000,
            amount=10000000,
            turnover_rate=2.5,
            market_cap=2000,
            high=10.8,
            low=10.2,
            sector="银行",
        )

        # 最小数据
        minimal_quote = self._create_quote(
            "000002", "测试股", 15.0, DataSource.SINA
        )

        # 合并时应该选择质量更高的数据
        result = self.merger.merge([[full_quote], [minimal_quote]])

        # 两个都应该保留（不同代码）
        assert len(result) == 2

    def test_source_weight_priority(self):
        """测试数据源权重优先级"""
        # 腾讯权重最高
        tencent_quote = self._create_quote("000001", "平安银行", 10.5, DataSource.TENCENT)
        sina_quote = self._create_quote("000001", "平安银行", 10.5, DataSource.SINA)

        # 两数据相同，应该选择腾讯
        result = self.merger.merge([[tencent_quote], [sina_quote]])

        assert len(result) == 1
        assert result[0].source == DataSource.TENCENT

    # ==================== 字段补充测试 ====================

    def test_enrich_missing_fields(self):
        """测试补充缺失字段"""
        # 腾讯有价格但没有市值
        tencent_quote = self._create_quote(
            "000001", "平安银行", 10.5, DataSource.TENCENT,
            sector="银行",
        )

        # 东财有市值但没有行业
        eastmoney_quote = self._create_quote(
            "000001", "平安银行", 10.6, DataSource.EASTMONEY,
            market_cap=2000,
        )

        result = self.merger.merge([[tencent_quote], [eastmoney_quote]], enrich=True)

        assert len(result) == 1
        quote = result[0]

        # 应该使用腾讯数据（权重高）
        assert quote.source == DataSource.TENCENT
        # 应该从东财补充市值
        assert quote.market_cap == 2000

    # ==================== 统计信息测试 ====================

    def test_get_statistics(self):
        """测试获取统计信息"""
        quotes1 = [
            self._create_quote("000001", "平安银行", 10.5, DataSource.TENCENT),
            self._create_quote("600000", "浦发银行", 10.0, DataSource.TENCENT),
        ]
        quotes2 = [
            self._create_quote("000001", "平安银行", 10.6, DataSource.EASTMONEY),
            self._create_quote("300001", "特锐德", 25.0, DataSource.EASTMONEY),
        ]

        stats = self.merger.get_statistics([quotes1, quotes2])

        assert stats["total_records"] == 4
        assert stats["unique_stocks"] == 3
        assert "tencent" in stats["sources"]
        assert "eastmoney" in stats["sources"]

    # ==================== 自定义配置测试 ====================

    def test_custom_source_weights(self):
        """测试自定义数据源权重"""
        # 自定义权重：东财 > 腾讯
        custom_weights = {
            DataSource.EASTMONEY: 1.0,
            DataSource.TENCENT: 0.5,
        }
        merger = DataMerger(source_weights=custom_weights)

        tencent_quote = self._create_quote("000001", "平安银行", 10.5, DataSource.TENCENT)
        eastmoney_quote = self._create_quote("000001", "平安银行", 10.6, DataSource.EASTMONEY)

        result = merger.merge([[tencent_quote], [eastmoney_quote]])

        # 应该选择东财
        assert result[0].source == DataSource.EASTMONEY


class TestMergeQuotesFunction:
    """merge_quotes 便捷函数测试"""

    def test_basic_merge(self):
        """测试基础合并"""
        quotes1 = [
            StockQuote(code="000001", name="平安银行", price=10.5, source=DataSource.TENCENT),
        ]
        quotes2 = [
            StockQuote(code="000002", name="万科A", price=15.0, source=DataSource.EASTMONEY),
        ]

        result = merge_quotes([quotes1, quotes2])

        assert len(result) == 2


class TestEdgeCases:
    """边界情况测试"""

    def setup_method(self):
        self.merger = DataMerger()

    def test_merge_with_invalid_quotes(self):
        """测试包含无效数据的合并"""
        quotes = [
            StockQuote(code="000001", name="平安银行", price=10.5, source=DataSource.TENCENT),
            StockQuote(code="000002", name="无效股", price=0, source=DataSource.TENCENT),  # 无效
        ]
        result = self.merger.merge([quotes])

        # 两条数据都会被保留（合并器不负责过滤无效数据）
        assert len(result) == 2

    def test_merge_large_dataset(self):
        """测试大数据量合并"""
        # 创建1000只股票
        quotes1 = [
            StockQuote(
                code=f"{i:06d}",
                name=f"股票{i}",
                price=10.0 + i * 0.01,
                source=DataSource.TENCENT
            )
            for i in range(1000)
        ]
        quotes2 = [
            StockQuote(
                code=f"{i:06d}",
                name=f"股票{i}",
                price=10.0 + i * 0.02,
                source=DataSource.EASTMONEY
            )
            for i in range(500, 1500)  # 重叠500只
        ]

        result = self.merger.merge([quotes1, quotes2])

        # 应该有1500只唯一股票
        assert len(result) == 1500

    def test_merge_same_source(self):
        """测试相同数据源的合并"""
        quotes1 = [
            StockQuote(code="000001", name="平安银行", price=10.5, source=DataSource.TENCENT),
        ]
        quotes2 = [
            StockQuote(code="000001", name="平安银行", price=10.6, source=DataSource.TENCENT),
        ]

        result = self.merger.merge([quotes1, quotes2])

        # 只保留一只
        assert len(result) == 1
