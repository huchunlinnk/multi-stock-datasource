"""
股票数据归一化器基类

提供数据归一化的通用方法和接口定义。
所有具体的数据源归一化器都继承自这个基类。
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Type, Callable
import logging
import time
from datetime import datetime

from .models import StockQuote, DataSource


logger = logging.getLogger(__name__)


# 东方财富字段映射表 (行业约定格式)
EASTMONEY_FIELD_MAP = {
    "f12": "code",           # 股票代码
    "f14": "name",           # 股票名称
    "f2": "price",           # 当前价
    "f3": "change_percent",  # 涨跌幅%
    "f4": "change_amount",   # 涨跌额
    "f5": "volume",          # 成交量
    "f6": "amount",          # 成交额
    "f8": "turnover_rate",   # 换手率%
    "f15": "high",           # 最高价
    "f16": "low",            # 最低价
    "f17": "open",           # 开盘价
    "f18": "pre_close",      # 昨收价
    "f20": "market_cap",     # 流通市值(亿)
    "f21": "total_cap",      # 总市值(亿)
}


class BaseNormalizer(ABC):
    """
    数据归一化器基类

    子类需要实现:
    - source 属性: 返回数据源类型
    - normalize 方法: 将原始数据转换为 StockQuote

    提供的方法:
    - normalize_batch: 批量归一化
    - _get_float/_get_int/_get_str: 安全获取字段值
    - _detect_market/_detect_board: 市场和板块推断
    """

    # 子类可以覆盖这个映射表
    FIELD_MAP = EASTMONEY_FIELD_MAP

    @property
    @abstractmethod
    def source(self) -> DataSource:
        """返回数据源类型"""
        pass

    @abstractmethod
    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        """
        将原始数据转换为 StockQuote

        Args:
            raw_data: 原始数据字典

        Returns:
            StockQuote: 标准化的股票行情

        Raises:
            ValueError: 数据格式错误
        """
        pass

    def normalize_batch(
        self,
        raw_list: List[Dict[str, Any]],
        skip_invalid: bool = True,
        calculate_quality: bool = True,
    ) -> List[StockQuote]:
        """
        批量归一化数据

        Args:
            raw_list: 原始数据列表
            skip_invalid: 是否跳过无效数据
            calculate_quality: 是否计算数据质量分

        Returns:
            List[StockQuote]: 归一化后的数据列表
        """
        results = []
        errors = []

        for i, raw in enumerate(raw_list):
            try:
                quote = self.normalize(raw)

                # 检查数据有效性
                if skip_invalid and not quote.is_valid():
                    continue

                # 计算数据质量分
                if calculate_quality:
                    quote.quality_score = quote.calculate_completeness()

                results.append(quote)

            except Exception as e:
                code = self._extract_code_safe(raw)
                errors.append({"index": i, "code": code, "error": str(e)})
                logger.debug(f"[{self.source.value}] 归一化失败 {code}: {e}")

        if errors:
            logger.warning(
                f"[{self.source.value}] 批量归一化: 成功 {len(results)}, "
                f"失败 {len(errors)}"
            )
        else:
            logger.info(
                f"[{self.source.value}] 批量归一化完成: {len(results)} 条"
            )

        return results

    # ==================== 辅助方法 ====================

    def _get_float(
        self,
        data: Dict[str, Any],
        *keys: str,
        default: float = 0.0
    ) -> float:
        """
        安全获取浮点数值

        支持多个备选键名，按顺序尝试获取。

        Args:
            data: 数据字典
            *keys: 键名列表（按优先级）
            default: 默认值

        Returns:
            float: 浮点数值
        """
        for key in keys:
            val = data.get(key)
            if val is not None and val != "" and val != "-":
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return default

    def _get_int(
        self,
        data: Dict[str, Any],
        *keys: str,
        default: int = 0
    ) -> int:
        """安全获取整数值"""
        for key in keys:
            val = data.get(key)
            if val is not None and val != "" and val != "-":
                try:
                    return int(float(val))
                except (ValueError, TypeError):
                    continue
        return default

    def _get_str(
        self,
        data: Dict[str, Any],
        *keys: str,
        default: str = ""
    ) -> str:
        """安全获取字符串值"""
        for key in keys:
            val = data.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
        return default

    def _extract_code(self, data: Dict[str, Any]) -> str:
        """
        从数据中提取股票代码

        按优先级尝试: f12 > code > symbol > secu_code

        Args:
            data: 数据字典

        Returns:
            str: 股票代码

        Raises:
            ValueError: 无法提取代码
        """
        code = (
            data.get("f12") or
            data.get("code") or
            data.get("symbol") or
            data.get("secu_code") or
            ""
        )
        code = str(code).strip()

        # 移除可能的前缀 (如 sh, sz)
        if len(code) > 6:
            for prefix in ["sh", "sz", "bj", "SH", "SZ", "BJ"]:
                if code.lower().startswith(prefix):
                    code = code[2:]
                    break

        if not code:
            raise ValueError("无法提取股票代码")

        return code

    def _extract_code_safe(self, data: Dict[str, Any]) -> str:
        """安全提取代码（不抛出异常）"""
        try:
            return self._extract_code(data)
        except ValueError:
            return "unknown"

    def _extract_name(self, data: Dict[str, Any]) -> str:
        """从数据中提取股票名称"""
        name = (
            data.get("f14") or
            data.get("name") or
            data.get("stock_name") or
            data.get("证券名称") or
            ""
        )
        return str(name).strip()

    def _detect_market(self, code: str) -> str:
        """根据代码推断市场"""
        if code.startswith("6"):
            return "SH"
        elif code.startswith(("0", "3")):
            return "SZ"
        elif code.startswith(("8", "4")):
            return "BJ"
        return ""

    def _detect_board(self, code: str) -> str:
        """根据代码推断板块"""
        if code.startswith(("688", "689")):
            return "科创板"
        elif code.startswith(("300", "301")):
            return "创业板"
        elif code.startswith(("8", "4")):
            return "北交所"
        elif code.startswith("6"):
            return "沪A"
        else:
            return "深A"

    def _is_chinext(self, code: str) -> bool:
        """判断是否为创业板"""
        return code.startswith(("300", "301"))

    def _is_kcb(self, code: str) -> bool:
        """判断是否为科创板"""
        return code.startswith(("688", "689"))

    def _is_st(self, name: str) -> bool:
        """判断是否为ST股票"""
        return "ST" in name.upper() if name else False


class NormalizerRegistry:
    """
    归一化器注册表

    用于管理和获取各种数据源的归一化器。
    """

    _registry: Dict[DataSource, Type[BaseNormalizer]] = {}

    @classmethod
    def register(cls, source: DataSource) -> Callable:
        """
        注册归一化器的装饰器

        Usage:
            @NormalizerRegistry.register(DataSource.TENCENT)
            class TencentNormalizer(BaseNormalizer):
                ...
        """
        def decorator(normalizer_class: Type[BaseNormalizer]) -> Type[BaseNormalizer]:
            cls._registry[source] = normalizer_class
            return normalizer_class
        return decorator

    @classmethod
    def get(cls, source: DataSource) -> BaseNormalizer:
        """
        获取指定数据源的归一化器实例

        Args:
            source: 数据源类型

        Returns:
            BaseNormalizer: 归一化器实例

        Raises:
            ValueError: 不支持的数据源
        """
        if source not in cls._registry:
            raise ValueError(
                f"不支持的数据源: {source}. "
                f"支持的来源: {list(cls._registry.keys())}"
            )
        return cls._registry[source]()

    @classmethod
    def list_sources(cls) -> List[DataSource]:
        """获取所有已注册的数据源"""
        return list(cls._registry.keys())

    @classmethod
    def is_registered(cls, source: DataSource) -> bool:
        """检查数据源是否已注册"""
        return source in cls._registry


def normalize(source: DataSource, raw_data: Dict[str, Any]) -> StockQuote:
    """
    便捷函数：归一化单条数据

    Args:
        source: 数据源类型
        raw_data: 原始数据

    Returns:
        StockQuote: 归一化后的数据
    """
    normalizer = NormalizerRegistry.get(source)
    return normalizer.normalize(raw_data)


def normalize_batch(
    source: DataSource,
    raw_list: List[Dict[str, Any]],
    **kwargs
) -> List[StockQuote]:
    """
    便捷函数：批量归一化数据

    Args:
        source: 数据源类型
        raw_list: 原始数据列表
        **kwargs: 传递给 normalize_batch 的参数

    Returns:
        List[StockQuote]: 归一化后的数据列表
    """
    normalizer = NormalizerRegistry.get(source)
    return normalizer.normalize_batch(raw_list, **kwargs)
