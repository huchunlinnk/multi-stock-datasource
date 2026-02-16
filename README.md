# Stock Data Normalizer

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-48%20passed-success)](stock_data_normalizer/tests/)

**一个用于多数据源股票数据归一化的 Python 模块**

[English](#english) | [中文](#中文)

</div>

---

## 中文

### 功能特点

- ✅ **支持 7 个主流数据源**：东方财富、腾讯财经、新浪财经、AKShare、BaoStock、聚宽、Tushare
- ✅ **统一数据模型**：所有数据源归一化为 `StockQuote` 格式
- ✅ **数据质量评估**：自动计算数据完整性和质量分数
- ✅ **多源数据合并**：智能合并多个数据源，选择最优数据
- ✅ **字段自动补充**：从其他数据源补充缺失字段
- ✅ **零外部依赖**：仅依赖 pydantic

### 安装

```bash
pip install stock-data-normalizer
```

或从源码安装：

```bash
git clone https://github.com/your-username/stock-data-normalizer.git
cd stock-data-normalizer
pip install -e .
```

### 快速开始

#### 1. 单条数据归一化

```python
from stock_data_normalizer import StockQuote, DataSource, normalize

# 东方财富格式的原始数据
raw_data = {
    "f12": "000001",    # 股票代码
    "f14": "平安银行",   # 股票名称
    "f2": 10.5,         # 当前价
    "f3": 2.5,          # 涨跌幅
    "f5": 1000000,      # 成交量
    "f6": 10500000,     # 成交额
}

# 归一化为统一格式
quote = normalize(DataSource.EASTMONEY, raw_data)

print(f"{quote.code} {quote.name}: ¥{quote.price} ({quote.change_percent:+.2f}%)")
# 输出: 000001 平安银行: ¥10.5 (+2.50%)
```

#### 2. 批量归一化

```python
from stock_data_normalizer import normalize_batch, DataSource

raw_list = [
    {"f12": "000001", "f14": "平安银行", "f2": 10.5},
    {"f12": "000002", "f14": "万科A", "f2": 15.0},
    {"f12": "600000", "f14": "浦发银行", "f2": 10.0},
]

quotes = normalize_batch(DataSource.TENCENT, raw_list)
for q in quotes:
    print(f"{q.code} {q.name}: ¥{q.price}")
```

#### 3. 多源数据合并

```python
from stock_data_normalizer import DataMerger, DataSource, normalize_batch

# 从不同数据源获取数据
tencent_data = [...]  # 腾讯数据
eastmoney_data = [...]  # 东财数据
akshare_data = [...]  # AKShare 数据

# 归一化
tencent_quotes = normalize_batch(DataSource.TENCENT, tencent_data)
eastmoney_quotes = normalize_batch(DataSource.EASTMONEY, eastmoney_data)
akshare_quotes = normalize_batch(DataSource.AKSHARE, akshare_data)

# 合并多源数据
merger = DataMerger()
merged = merger.merge([tencent_quotes, eastmoney_quotes, akshare_quotes])

# 查看合并统计
stats = merger.get_statistics([tencent_quotes, eastmoney_quotes, akshare_quotes])
print(f"合并后: {stats['unique_stocks']} 只唯一股票")
print(f"数据来源: {stats['sources']}")
```

#### 4. 数据模型

```python
from stock_data_normalizer import StockQuote, DataSource

# 创建股票行情
quote = StockQuote(
    code="000001",
    name="平安银行",
    price=10.5,
    open=10.3,
    high=10.8,
    low=10.2,
    pre_close=10.0,
    change_percent=5.0,
    volume=1000000,
    amount=10500000,
    turnover_rate=2.5,
    market_cap=2000.0,  # 亿
    market="SZ",
    board="深A",
    sector="银行",
    source=DataSource.TENCENT,
)

# 数据有效性检查
if quote.is_valid():
    print(f"数据有效，完整性: {quote.calculate_completeness():.2%}")

# 序列化
data = quote.to_dict()  # 转为字典
json_str = quote.to_json()  # 转为 JSON

# 反序列化
restored = StockQuote.from_dict(data)
restored = StockQuote.from_json(json_str)
```

### 数据源字段映射

各数据源的字段会自动映射到统一的 `StockQuote` 格式：

| 字段 | 说明 | 东财 | 腾讯 | 新浪 |
|------|------|------|------|------|
| code | 股票代码 | f12 | f12/code | f12 |
| name | 股票名称 | f14 | f14/name | f14 |
| price | 当前价 | f2 | f2 | f2 |
| change_percent | 涨跌幅 | f3 | f3 | f3 |
| volume | 成交量 | f5 | f5 | f5 |
| amount | 成交额 | f6 | f6 | f6 |
| high | 最高价 | f15 | f15 | f4 |
| low | 最低价 | f16 | f16 | f16 |
| open | 开盘价 | f17 | f17 | f15 |
| pre_close | 昨收价 | f18 | f18 | f18 |
| turnover_rate | 换手率 | f8 | f8 | f8 |
| market_cap | 流通市值 | f20 | f20 | - |

### 数据质量评分

合并器会根据以下因素计算数据质量分数：

- **数据源权重** (40%)：腾讯 > 东财 > AKShare > ...
- **数据完整性** (40%)：字段填充程度
- **数据新鲜度** (20%)：获取时间

```python
for quote in merged:
    print(f"{quote.code}: 质量分={quote.quality_score:.2f}")
```

### 自定义配置

```python
from stock_data_normalizer import DataMerger, DataSource

# 自定义数据源权重
custom_weights = {
    DataSource.EASTMONEY: 1.0,  # 东财优先
    DataSource.TENCENT: 0.9,
}

merger = DataMerger(source_weights=custom_weights)
```

### 添加新的数据源

```python
from stock_data_normalizer import BaseNormalizer, DataSource, NormalizerRegistry

@NormalizerRegistry.register(DataSource.CUSTOM)  # 需要先在 DataSource 中添加
class CustomNormalizer(BaseNormalizer):
    @property
    def source(self) -> DataSource:
        return DataSource.CUSTOM

    def normalize(self, raw_data: dict) -> StockQuote:
        code = self._extract_code(raw_data)
        name = self._extract_name(raw_data)
        # ... 自定义字段映射
        return StockQuote(
            code=code,
            name=name,
            # ...
            source=self.source,
        )
```

---

## English

### Features

- ✅ **7 Data Sources Supported**: EastMoney, Tencent, Sina, AKShare, BaoStock, JoinQuant, Tushare
- ✅ **Unified Data Model**: All sources normalized to `StockQuote` format
- ✅ **Data Quality Assessment**: Automatic completeness and quality scoring
- ✅ **Multi-source Merging**: Intelligent merge with best data selection
- ✅ **Field Enrichment**: Missing fields filled from other sources
- ✅ **Zero External Dependencies**: Only requires pydantic

### Installation

```bash
pip install stock-data-normalizer
```

### Quick Start

```python
from stock_data_normalizer import StockQuote, DataSource, normalize, DataMerger

# Normalize single record
raw = {"f12": "000001", "f14": "Ping An Bank", "f2": 10.5}
quote = normalize(DataSource.EASTMONEY, raw)

# Batch normalize
quotes = normalize_batch(DataSource.TENCENT, raw_list)

# Merge multi-source data
merger = DataMerger()
merged = merger.merge([source1_quotes, source2_quotes])
```

---

## API Reference

### StockQuote

| Field | Type | Description |
|-------|------|-------------|
| code | str | Stock code (6 digits) |
| name | str | Stock name |
| price | float | Current price |
| open | float | Open price |
| high | float | High price |
| low | float | Low price |
| pre_close | float | Previous close |
| change_amount | float | Change amount |
| change_percent | float | Change percent |
| volume | int | Volume (lots) |
| amount | float | Amount (CNY) |
| turnover_rate | float | Turnover rate |
| market_cap | float | Circulation market cap (100M) |
| total_cap | float | Total market cap (100M) |
| market | str | Market (SH/SZ/BJ) |
| board | str | Board type |
| sector | str | Industry sector |
| source | DataSource | Data source |
| quality_score | float | Quality score (0-1) |

### DataSource Enum

```python
class DataSource(str, Enum):
    EASTMONEY = "eastmoney"
    TENCENT = "tencent"
    SINA = "sina"
    AKSHARE = "akshare"
    BAOSTOCK = "baostock"
    JOINQUANT = "joinquant"
    TUSHARE = "tushare"
```

---

## Development

### Run Tests

```bash
# Install dev dependencies
pip install pytest

# Run tests
pytest stock_data_normalizer/tests/ -v

# Run with coverage
pytest stock_data_normalizer/tests/ -v --cov=stock_data_normalizer
```

### Project Structure

```
stock-data-normalizer/
├── stock_data_normalizer/
│   ├── __init__.py          # Package entry
│   ├── models.py            # StockQuote model
│   ├── base.py              # BaseNormalizer & Registry
│   ├── normalizers/         # Data source normalizers
│   │   └── __init__.py
│   ├── services/            # Data merger
│   │   └── __init__.py
│   └── tests/               # Test suite
├── README.md
├── LICENSE
├── pyproject.toml
└── setup.py
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## Acknowledgments

- [AKShare](https://github.com/akfamily/akshare) - Open source financial data interface
- [Tushare](https://tushare.pro/) - Financial data interface
- All data providers for their public APIs
