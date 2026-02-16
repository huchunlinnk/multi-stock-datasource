# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-02-16

### Added
- **RotatingCacheService**: 轮询数据源缓存服务
  - 每次请求自动切换数据源，防止单一数据源被限流
  - 失败时先重试当前数据源，再切换到下一个
  - 支持自定义缓存后端 (CacheBackend 接口)
  - 内置内存缓存后端 (MemoryCacheBackend)
- CacheBackend 抽象类，支持 Redis/Memcached/内存等多种后端

### Changed
- 版本号更新到 1.2.0

## [1.1.0] - 2026-02-16

### Added
- Non-trading hours cache fallback mechanism
- Integration tests for all 7 data sources
- Cross-source data consistency validation
- Redis cache fallback for EastMoney and Sina APIs
- Full integration test with 100% match rate validation

### Changed
- Improved test coverage for non-trading hours scenarios
- Enhanced cache key compatibility across different Redis formats

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
