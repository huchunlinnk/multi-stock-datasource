# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-02-16

### Added
- Initial release
- Support for 7 data sources:
  - EastMoney (东方财富)
  - Tencent Finance (腾讯财经)
  - Sina Finance (新浪财经)
  - AKShare
  - BaoStock
  - JoinQuant (聚宽)
  - Tushare
- Unified `StockQuote` data model
- `BaseNormalizer` abstract class for custom normalizers
- `NormalizerRegistry` for automatic normalizer registration
- `DataMerger` for intelligent multi-source data merging
- Data quality scoring and completeness calculation
- Field enrichment from multiple sources
- Comprehensive test suite (48 tests)
- Type hints for all public APIs
- Support for both Pydantic v1 and v2

### Features
- `normalize()` - Single record normalization
- `normalize_batch()` - Batch normalization
- `merge_quotes()` - Multi-source data merging
- `StockQuote.to_dict()` / `StockQuote.from_dict()` - Serialization
- `StockQuote.to_json()` / `StockQuote.from_json()` - JSON serialization
- `StockQuote.is_valid()` - Data validation
- `StockQuote.calculate_completeness()` - Completeness scoring

### Documentation
- Comprehensive README with examples
- API reference
- Chinese and English documentation
