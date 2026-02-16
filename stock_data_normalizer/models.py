"""
股票数据归一化器 - 统一数据模型

这是一个可独立使用的 Python 模块，用于：
1. 定义统一的股票行情数据结构
2. 支持多个数据源的数据归一化
3. 提供数据质量评估和合并功能

GitHub: https://github.com/your-repo/stock-data-normalizer
License: MIT
Author: AI Stocker Team

使用示例:
    from stock_data_normalizer import StockQuote, DataSource, normalize

    # 归一化单条数据
    raw_data = {"f12": "000001", "f14": "平安银行", "f2": 10.5}
    quote = normalize(DataSource.EASTMONEY, raw_data)

    # 批量归一化
    quotes = normalize_batch(DataSource.TENCENT, raw_list)
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
import json


__version__ = "1.0.0"
__author__ = "AI Stocker Team"


class DataSource(str, Enum):
    """
    支持的数据来源枚举

    每个数据源都有其特点:
    - TENCENT: 腾讯财经，数据稳定，有行业分类
    - EASTMONEY: 东方财富，数据全面
    - SINA: 新浪财经，轻量备用
    - AKSHARE: 开源库，数据规范
    - BAOSTOCK: 免费 API
    - JOINQUANT: 聚宽平台
    - TUSHARE: 需要 Token
    """
    EASTMONEY = "eastmoney"
    TENCENT = "tencent"
    SINA = "sina"
    AKSHARE = "akshare"
    BAOSTOCK = "baostock"
    JOINQUANT = "joinquant"
    TUSHARE = "tushare"

    @classmethod
    def list_all(cls) -> List[str]:
        """获取所有支持的数据源列表"""
        return [s.value for s in cls]


class StockQuote(BaseModel):
    """
    统一股票行情数据模型

    这是核心数据结构，所有数据源的数据都会归一化到这个格式。

    字段说明:
    - 基础信息: code, name
    - 价格信息: price, open, high, low, pre_close
    - 涨跌信息: change_amount, change_percent
    - 成交信息: volume, amount, turnover_rate
    - 市值信息: market_cap, total_cap
    - 市场信息: market, board, sector
    - 状态标记: is_st, is_chinext, is_kcb, suspended
    - 元数据: source, fetched_at, quality_score

    示例:
        quote = StockQuote(
            code="000001",
            name="平安银行",
            price=10.5,
            source=DataSource.TENCENT
        )
    """

    # ==================== 基础信息 ====================
    code: str = Field(..., description="股票代码 (6位数字)")
    name: str = Field(default="", description="股票名称")

    # ==================== 价格信息 ====================
    price: float = Field(default=0.0, description="当前价格", ge=0)
    open: float = Field(default=0.0, description="开盘价", ge=0)
    high: float = Field(default=0.0, description="最高价", ge=0)
    low: float = Field(default=0.0, description="最低价", ge=0)
    pre_close: float = Field(default=0.0, description="昨收价", ge=0)

    # ==================== 涨跌信息 ====================
    change_amount: float = Field(default=0.0, description="涨跌额")
    change_percent: float = Field(default=0.0, description="涨跌幅(%)")

    # ==================== 成交信息 ====================
    volume: int = Field(default=0, description="成交量(手)", ge=0)
    amount: float = Field(default=0.0, description="成交额(元)", ge=0)
    turnover_rate: float = Field(default=0.0, description="换手率(%)", ge=0)

    # ==================== 市值信息 ====================
    market_cap: float = Field(default=0.0, description="流通市值(亿元)", ge=0)
    total_cap: float = Field(default=0.0, description="总市值(亿元)", ge=0)

    # ==================== 市场信息 ====================
    market: str = Field(default="", description="市场: SH/SZ/BJ")
    board: str = Field(default="", description="板块: 主板/创业板/科创板/北交所")
    sector: str = Field(default="", description="行业分类")

    # ==================== 状态标记 ====================
    is_st: bool = Field(default=False, description="是否ST股票")
    is_chinext: bool = Field(default=False, description="是否创业板")
    is_kcb: bool = Field(default=False, description="是否科创板")
    suspended: bool = Field(default=False, description="是否停牌")

    # ==================== 元数据 ====================
    source: DataSource = Field(..., description="数据来源")
    source_raw: Optional[Dict[str, Any]] = Field(
        default=None,
        description="原始数据(用于调试)",
        exclude=True  # 序列化时排除，避免数据过大
    )
    fetched_at: datetime = Field(
        default_factory=datetime.now,
        description="数据获取时间"
    )
    quality_score: float = Field(
        default=1.0,
        description="数据质量分(0-1)",
        ge=0,
        le=1
    )

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        use_enum_values = False

    @validator("code")
    def validate_code(cls, v: str) -> str:
        """验证股票代码格式"""
        if not v:
            raise ValueError("股票代码不能为空")
        v = str(v).strip()
        if not v.isdigit() or len(v) != 6:
            raise ValueError(f"股票代码必须是6位数字: {v}")
        return v

    @validator("market", pre=True)
    def validate_market(cls, v) -> str:
        """标准化市场代码"""
        if not v:
            return ""
        v = str(v).upper()
        market_map = {
            "SH": "SH", "上海": "SH", "沪": "SH", "1": "SH",
            "SZ": "SZ", "深圳": "SZ", "深": "SZ", "0": "SZ",
            "BJ": "BJ", "北京": "BJ", "北": "BJ",
        }
        return market_map.get(v, v)

    @validator("board", pre=True, always=True)
    def detect_board_from_code(cls, v, values) -> str:
        """如果未提供板块，从代码推断"""
        if v:
            return str(v)

        code = values.get("code", "")
        if not code:
            return ""

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

    def calculate_completeness(self) -> float:
        """
        计算数据完整性分数

        基于重要字段的填充情况计算 0-1 之间的分数。
        分数越高表示数据越完整。

        Returns:
            float: 完整性分数 (0-1)
        """
        field_weights = {
            "price": 0.15,
            "volume": 0.10,
            "amount": 0.10,
            "turnover_rate": 0.08,
            "market_cap": 0.08,
            "high": 0.05,
            "low": 0.05,
            "open": 0.05,
            "pre_close": 0.05,
            "sector": 0.10,
            "market": 0.05,
            "board": 0.04,
            "name": 0.05,
            "change_percent": 0.05,
        }

        score = 0.0
        for field, weight in field_weights.items():
            value = getattr(self, field, None)
            if self._is_valid_field_value(value):
                score += weight

        return round(min(score, 1.0), 2)

    @staticmethod
    def _is_valid_field_value(value: Any) -> bool:
        """判断字段值是否有效"""
        if value is None:
            return False
        if isinstance(value, (int, float)) and value != 0:
            return True
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, bool):
            return True
        return False

    def is_valid(self) -> bool:
        """
        检查数据是否有效

        有效数据的判断标准:
        1. 股票代码格式正确
        2. 价格大于 0

        Returns:
            bool: 数据是否有效
        """
        if self.price <= 0:
            return False
        if not self.code or len(self.code) != 6 or not self.code.isdigit():
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典格式

        Returns:
            Dict: 可序列化的字典
        """
        return {
            "code": self.code,
            "name": self.name,
            "price": self.price,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "pre_close": self.pre_close,
            "change_amount": self.change_amount,
            "change_percent": self.change_percent,
            "volume": self.volume,
            "amount": self.amount,
            "turnover_rate": self.turnover_rate,
            "market_cap": self.market_cap,
            "total_cap": self.total_cap,
            "market": self.market,
            "board": self.board,
            "sector": self.sector,
            "is_st": self.is_st,
            "is_chinext": self.is_chinext,
            "is_kcb": self.is_kcb,
            "suspended": self.suspended,
            "source": self.source.value,
            "quality_score": self.quality_score,
            "fetched_at": self.fetched_at.isoformat(),
        }

    def to_json(self) -> str:
        """转换为 JSON 字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StockQuote":
        """
        从字典创建实例

        Args:
            data: 字典数据

        Returns:
            StockQuote: 实例
        """
        data = data.copy()

        # 处理 source 字段
        if isinstance(data.get("source"), str):
            data["source"] = DataSource(data["source"])

        # 处理 fetched_at 字段
        if isinstance(data.get("fetched_at"), str):
            data["fetched_at"] = datetime.fromisoformat(data["fetched_at"])

        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "StockQuote":
        """从 JSON 字符串创建实例"""
        return cls.from_dict(json.loads(json_str))


# ==================== 工具函数 ====================

def detect_market(code: str) -> str:
    """根据股票代码推断市场"""
    if code.startswith("6"):
        return "SH"
    elif code.startswith(("0", "3")):
        return "SZ"
    elif code.startswith(("8", "4")):
        return "BJ"
    return ""


def detect_board(code: str) -> str:
    """根据股票代码推断板块"""
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


def is_chinext(code: str) -> bool:
    """判断是否为创业板"""
    return code.startswith(("300", "301"))


def is_kcb(code: str) -> bool:
    """判断是否为科创板"""
    return code.startswith(("688", "689"))


def is_st(name: str) -> bool:
    """判断是否为ST股票"""
    return "ST" in name.upper() if name else False
