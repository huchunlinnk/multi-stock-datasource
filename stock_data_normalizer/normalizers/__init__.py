"""
各数据源归一化器模块

包含:
- EastMoneyNormalizer: 东方财富
- TencentNormalizer: 腾讯财经
- SinaNormalizer: 新浪财经
- AkShareNormalizer: AKShare
- BaoStockNormalizer: BaoStock
- JoinQuantNormalizer: 聚宽
"""

from ..base import BaseNormalizer, NormalizerRegistry, EASTMONEY_FIELD_MAP
from ..models import StockQuote, DataSource
from datetime import datetime
from typing import Dict, Any
import logging


logger = logging.getLogger(__name__)


# ==================== 东方财富归一化器 ====================

@NormalizerRegistry.register(DataSource.EASTMONEY)
class EastMoneyNormalizer(BaseNormalizer):
    """
    东方财富数据归一化器

    东方财富 API 返回的字段格式:
    - f12: 股票代码
    - f14: 股票名称
    - f2: 当前价
    - f3: 涨跌幅
    - f5: 成交量
    - f6: 成交额
    - f8: 换手率
    - f15: 最高价
    - f16: 最低价
    - f17: 开盘价
    - f18: 昨收价
    - f20: 流通市值
    - f21: 总市值
    """

    @property
    def source(self) -> DataSource:
        return DataSource.EASTMONEY

    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)

        return StockQuote(
            code=code,
            name=name,
            price=self._get_float(raw_data, "f2"),
            change_percent=self._get_float(raw_data, "f3"),
            volume=self._get_int(raw_data, "f5"),
            amount=self._get_float(raw_data, "f6"),
            turnover_rate=self._get_float(raw_data, "f8"),
            high=self._get_float(raw_data, "f15"),
            low=self._get_float(raw_data, "f16"),
            open=self._get_float(raw_data, "f17"),
            pre_close=self._get_float(raw_data, "f18"),
            market_cap=self._get_float(raw_data, "f20"),
            total_cap=self._get_float(raw_data, "f21"),
            market=self._detect_market(code),
            board=self._detect_board(code),
            sector="",
            is_st=self._is_st(name),
            is_chinext=self._is_chinext(code),
            is_kcb=self._is_kcb(code),
            suspended=self._is_suspended(raw_data),
            source=self.source,
            fetched_at=datetime.now(),
        )

    def _is_suspended(self, raw_data: Dict[str, Any]) -> bool:
        """判断是否停牌"""
        price = self._get_float(raw_data, "f2")
        volume = self._get_int(raw_data, "f5")
        return price == 0 and volume == 0


# ==================== 腾讯财经归一化器 ====================

@NormalizerRegistry.register(DataSource.TENCENT)
class TencentNormalizer(BaseNormalizer):
    """
    腾讯财经数据归一化器

    腾讯 API 返回的数据格式:
    - 可能是原始的 ~ 分隔格式（需要解析）
    - 也可能是已归一化的格式（包含 code, name, sector）

    特点:
    - 数据稳定可靠
    - 包含行业分类信息
    """

    @property
    def source(self) -> DataSource:
        return DataSource.TENCENT

    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)

        # 提取价格信息（兼容多种字段名）
        price = self._get_float(raw_data, "f2", "price")
        high = self._get_float(raw_data, "f15", "high", "f4")
        low = self._get_float(raw_data, "f16", "low", "f34")
        open_price = self._get_float(raw_data, "f17", "open", "f5")
        pre_close = self._get_float(raw_data, "f18", "pre_close")

        # 涨跌信息
        change_percent = self._get_float(raw_data, "f3", "change_percent")

        # 成交信息
        volume = self._get_int(raw_data, "f5", "volume")
        amount = self._get_float(raw_data, "f6", "amount")
        turnover_rate = self._get_float(raw_data, "f8", "turnover_rate")

        # 市值信息
        market_cap = self._get_float(raw_data, "f20", "market_cap")
        total_cap = self._get_float(raw_data, "f21", "total_cap")

        # 市场信息（腾讯可能已提供）
        market = self._get_str(raw_data, "market")
        if market:
            if isinstance(market, int) or (isinstance(market, str) and market.isdigit()):
                market = "SZ" if int(market) == 0 else "SH"
        else:
            market = self._detect_market(code)

        # 板块和行业
        board = self._get_str(raw_data, "market_board", "board") or self._detect_board(code)
        sector = self._get_str(raw_data, "sector", "industry")

        # 创业板标记（腾讯可能已提供）
        is_chinext_flag = raw_data.get("is_chinext")
        if isinstance(is_chinext_flag, str):
            is_chinext_flag = is_chinext_flag.lower() == "true"
        else:
            is_chinext_flag = self._is_chinext(code)

        return StockQuote(
            code=code,
            name=name,
            price=price,
            open=open_price,
            high=high,
            low=low,
            pre_close=pre_close,
            change_percent=change_percent,
            volume=volume,
            amount=amount,
            turnover_rate=turnover_rate,
            market_cap=market_cap,
            total_cap=total_cap,
            market=market,
            board=board,
            sector=sector,
            is_st=self._is_st(name),
            is_chinext=bool(is_chinext_flag),
            is_kcb=self._is_kcb(code),
            suspended=(price == 0 and volume == 0),
            source=self.source,
            fetched_at=datetime.now(),
        )


# ==================== 新浪财经归一化器 ====================

@NormalizerRegistry.register(DataSource.SINA)
class SinaNormalizer(BaseNormalizer):
    """新浪财经数据归一化器"""

    @property
    def source(self) -> DataSource:
        return DataSource.SINA

    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)

        # 新浪的 f4 是最高价
        price = self._get_float(raw_data, "f2")
        high = self._get_float(raw_data, "f4")
        low = self._get_float(raw_data, "f16")
        open_price = self._get_float(raw_data, "f15")
        pre_close = self._get_float(raw_data, "f18")

        change_percent = self._get_float(raw_data, "f3")
        volume = self._get_int(raw_data, "f5")
        amount = self._get_float(raw_data, "f6")

        return StockQuote(
            code=code,
            name=name,
            price=price,
            open=open_price,
            high=high,
            low=low,
            pre_close=pre_close,
            change_percent=change_percent,
            volume=volume,
            amount=amount,
            turnover_rate=self._get_float(raw_data, "f8"),
            market=self._detect_market(code),
            board=self._detect_board(code),
            sector="",
            is_st=self._is_st(name),
            is_chinext=self._is_chinext(code),
            is_kcb=self._is_kcb(code),
            suspended=(price == 0 and volume == 0),
            source=self.source,
            fetched_at=datetime.now(),
        )


# ==================== AKShare 归一化器 ====================

@NormalizerRegistry.register(DataSource.AKSHARE)
class AkShareNormalizer(BaseNormalizer):
    """AKShare 数据归一化器"""

    @property
    def source(self) -> DataSource:
        return DataSource.AKSHARE

    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)

        price = self._get_float(raw_data, "f2")
        high = self._get_float(raw_data, "f4")
        low = self._get_float(raw_data, "f16")
        open_price = self._get_float(raw_data, "f15")
        pre_close = self._get_float(raw_data, "f18")

        change_percent = self._get_float(raw_data, "f3")
        volume = self._get_int(raw_data, "f5")
        amount = self._get_float(raw_data, "f6")

        # AKShare 可能已提供创业板标记
        is_chinext_flag = raw_data.get("is_chinext")
        if isinstance(is_chinext_flag, str):
            is_chinext_flag = is_chinext_flag.lower() == "true"
        else:
            is_chinext_flag = self._is_chinext(code)

        return StockQuote(
            code=code,
            name=name,
            price=price,
            open=open_price,
            high=high,
            low=low,
            pre_close=pre_close,
            change_percent=change_percent,
            volume=volume,
            amount=amount,
            turnover_rate=self._get_float(raw_data, "f8"),
            market=self._detect_market(code),
            board=self._detect_board(code),
            sector="",
            is_st=self._is_st(name),
            is_chinext=bool(is_chinext_flag),
            is_kcb=self._is_kcb(code),
            suspended=(price == 0 and volume == 0),
            source=self.source,
            fetched_at=datetime.now(),
        )


# ==================== BaoStock 归一化器 ====================

@NormalizerRegistry.register(DataSource.BAOSTOCK)
class BaoStockNormalizer(BaseNormalizer):
    """BaoStock 数据归一化器"""

    @property
    def source(self) -> DataSource:
        return DataSource.BAOSTOCK

    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)

        return StockQuote(
            code=code,
            name=name,
            price=self._get_float(raw_data, "f2"),
            change_percent=self._get_float(raw_data, "f3"),
            volume=self._get_int(raw_data, "f5"),
            amount=self._get_float(raw_data, "f6"),
            turnover_rate=self._get_float(raw_data, "f8"),
            high=self._get_float(raw_data, "f15"),
            low=self._get_float(raw_data, "f16"),
            open=self._get_float(raw_data, "f17"),
            pre_close=self._get_float(raw_data, "f18"),
            market_cap=self._get_float(raw_data, "f20"),
            total_cap=self._get_float(raw_data, "f21"),
            market=self._detect_market(code),
            board=self._detect_board(code),
            sector="",
            is_st=self._is_st(name),
            is_chinext=self._is_chinext(code),
            is_kcb=self._is_kcb(code),
            suspended=(self._get_float(raw_data, "f2") == 0 and
                      self._get_int(raw_data, "f5") == 0),
            source=self.source,
            fetched_at=datetime.now(),
        )


# ==================== 聚宽归一化器 ====================

@NormalizerRegistry.register(DataSource.JOINQUANT)
class JoinQuantNormalizer(BaseNormalizer):
    """聚宽数据归一化器"""

    @property
    def source(self) -> DataSource:
        return DataSource.JOINQUANT

    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)

        return StockQuote(
            code=code,
            name=name,
            price=self._get_float(raw_data, "f2"),
            change_percent=self._get_float(raw_data, "f3"),
            volume=self._get_int(raw_data, "f5"),
            amount=self._get_float(raw_data, "f6"),
            turnover_rate=self._get_float(raw_data, "f8"),
            high=self._get_float(raw_data, "f15"),
            low=self._get_float(raw_data, "f16"),
            open=self._get_float(raw_data, "f17"),
            pre_close=self._get_float(raw_data, "f18"),
            market_cap=self._get_float(raw_data, "f20"),
            total_cap=self._get_float(raw_data, "f21"),
            market=self._detect_market(code),
            board=self._detect_board(code),
            sector="",
            is_st=self._is_st(name),
            is_chinext=self._is_chinext(code),
            is_kcb=self._is_kcb(code),
            suspended=(self._get_float(raw_data, "f2") == 0 and
                      self._get_int(raw_data, "f5") == 0),
            source=self.source,
            fetched_at=datetime.now(),
        )


# ==================== Tushare 归一化器 ====================

@NormalizerRegistry.register(DataSource.TUSHARE)
class TushareNormalizer(BaseNormalizer):
    """
    Tushare 数据归一化器

    Tushare 返回的是 DataFrame 格式，需要转换为字典。
    字段名与东财格式类似。
    """

    @property
    def source(self) -> DataSource:
        return DataSource.TUSHARE

    def normalize(self, raw_data: Dict[str, Any]) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)

        return StockQuote(
            code=code,
            name=name,
            price=self._get_float(raw_data, "f2", "close", "price"),
            change_percent=self._get_float(raw_data, "f3", "pct_chg"),
            volume=self._get_int(raw_data, "f5", "vol", "volume"),
            amount=self._get_float(raw_data, "f6", "amount"),
            turnover_rate=self._get_float(raw_data, "f8", "turnover_rate"),
            high=self._get_float(raw_data, "f15", "high"),
            low=self._get_float(raw_data, "f16", "low"),
            open=self._get_float(raw_data, "f17", "open"),
            pre_close=self._get_float(raw_data, "f18", "pre_close"),
            market_cap=self._get_float(raw_data, "f20", "circ_mv"),
            total_cap=self._get_float(raw_data, "f21", "total_mv"),
            market=self._detect_market(code),
            board=self._detect_board(code),
            sector=self._get_str(raw_data, "industry", "sector"),
            is_st=self._is_st(name),
            is_chinext=self._is_chinext(code),
            is_kcb=self._is_kcb(code),
            suspended=(self._get_float(raw_data, "f2", "close", "price") == 0),
            source=self.source,
            fetched_at=datetime.now(),
        )


# 导出所有归一化器
__all__ = [
    "EastMoneyNormalizer",
    "TencentNormalizer",
    "SinaNormalizer",
    "AkShareNormalizer",
    "BaoStockNormalizer",
    "JoinQuantNormalizer",
    "TushareNormalizer",
]
