"""
股票数据归一化器 (Stock Data Normalizer)

一个用于多数据源股票数据归一化的 Python 模块。

功能特点:
- 支持 6+ 个主流数据源（东方财富、腾讯、新浪、AKShare 等）
- 统一的数据模型 (StockQuote)
- 自动数据质量评估
- 多源数据智能合并
- 完善的类型提示和文档

快速开始:
    from stock_data_normalizer import StockQuote, DataSource, normalize

    # 归一化单条数据
    raw_data = {"f12": "000001", "f14": "平安银行", "f2": 10.5, "f3": 2.5}
    quote = normalize(DataSource.EASTMONEY, raw_data)
    print(f"{quote.code} {quote.name}: ¥{quote.price}")

    # 批量归一化
    from stock_data_normalizer import normalize_batch
    quotes = normalize_batch(DataSource.TENCENT, raw_list)

    # 多源合并
    from stock_data_normalizer import DataMerger
    merger = DataMerger()
    merged = merger.merge([tencent_quotes, eastmoney_quotes])

GitHub: https://github.com/your-repo/stock-data-normalizer
License: MIT
"""

__version__ = "1.0.0"
__author__ = "AI Stocker Team"
__license__ = "MIT"

# 核心模型
from .models import (
    StockQuote,
    DataSource,
    detect_market,
    detect_board,
    is_chinext,
    is_kcb,
    is_st,
)

# 基础类和工具
from .base import (
    BaseNormalizer,
    NormalizerRegistry,
    normalize,
    normalize_batch,
    EASTMONEY_FIELD_MAP,
)

# 归一化器
from .normalizers import (
    EastMoneyNormalizer,
    TencentNormalizer,
    SinaNormalizer,
    AkShareNormalizer,
    BaoStockNormalizer,
    JoinQuantNormalizer,
    TushareNormalizer,
)

# 数据合并
from .services import (
    DataMerger,
    merge_quotes,
)


__all__ = [
    # 版本信息
    "__version__",
    "__author__",
    "__license__",
    # 核心模型
    "StockQuote",
    "DataSource",
    # 工具函数
    "detect_market",
    "detect_board",
    "is_chinext",
    "is_kcb",
    "is_st",
    # 基础类
    "BaseNormalizer",
    "NormalizerRegistry",
    "EASTMONEY_FIELD_MAP",
    # 便捷函数
    "normalize",
    "normalize_batch",
    "merge_quotes",
    # 归一化器
    "EastMoneyNormalizer",
    "TencentNormalizer",
    "SinaNormalizer",
    "AkShareNormalizer",
    "BaoStockNormalizer",
    "JoinQuantNormalizer",
    "TushareNormalizer",
    # 数据合并
    "DataMerger",
]
