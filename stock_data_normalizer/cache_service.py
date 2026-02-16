"""
轮询数据源缓存服务

核心策略：
1. 每次缓存更新只使用一个数据源，避免并行调用导致限流
2. 轮询切换数据源，分散请求压力
3. 失败时先重试当前数据源，再切换到下一个
4. 归一化后统一格式存储

使用示例:
    from stock_data_normalizer import RotatingCacheService, DataSource

    # 使用默认的内存缓存
    service = RotatingCacheService()

    # 或使用自定义缓存
    class MyCacheBackend:
        def get(self, key): ...
        def set(self, key, value, ttl=None): ...

    service = RotatingCacheService(cache_backend=MyCacheBackend())

    # 获取数据（自动轮询数据源）
    quotes, source = await service.fetch(limit=1000)

    # 更新缓存
    result = await service.update_cache(limit=1000)
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

from .models import StockQuote
from .base import DataSource, NormalizerRegistry
from .services import DataMerger

logger = logging.getLogger(__name__)


class CacheBackend(ABC):
    """缓存后端抽象类"""

    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        """获取缓存"""
        pass

    @abstractmethod
    def set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        """设置缓存"""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """删除缓存"""
        pass


class MemoryCacheBackend(CacheBackend):
    """内存缓存后端（用于测试）"""

    def __init__(self):
        self._cache: Dict[str, tuple] = {}  # {key: (value, expire_time)}

    def get(self, key: str) -> Optional[str]:
        import time
        if key in self._cache:
            value, expire_time = self._cache[key]
            if expire_time is None or time.time() < expire_time:
                return value
            del self._cache[key]
        return None

    def set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        import time
        expire_time = time.time() + ttl if ttl else None
        self._cache[key] = (value, expire_time)
        return True

    def delete(self, key: str) -> bool:
        if key in self._cache:
            del self._cache[key]
        return True


class RotatingCacheService:
    """
    轮询数据源缓存服务

    核心逻辑:
    1. 维护一个数据源轮询索引
    2. 每次更新时使用下一个数据源
    3. 当前数据源失败时，先重试，再切换
    4. 归一化数据后统一存储
    """

    # 数据源列表（按优先级排序）
    DEFAULT_SOURCES = [
        DataSource.TENCENT,
        DataSource.EASTMONEY,
        DataSource.AKSHARE,
        DataSource.BAOSTOCK,
        DataSource.JOINQUANT,
        DataSource.SINA,
    ]

    # 缓存Key前缀
    CACHE_PREFIX = "stock_data_normalizer"

    # 轮询索引存储Key
    ROTATION_INDEX_KEY = "rotation_index"

    # 默认TTL（秒）
    DEFAULT_TTL = 600

    def __init__(
        self,
        cache_backend: Optional[CacheBackend] = None,
        sources: Optional[List[DataSource]] = None,
        source_fetchers: Optional[Dict[DataSource, Callable]] = None,
    ):
        """
        初始化轮询缓存服务

        Args:
            cache_backend: 缓存后端，默认使用内存缓存
            sources: 数据源列表，默认使用全部6个
            source_fetchers: 数据源获取函数映射
        """
        self.cache = cache_backend or MemoryCacheBackend()
        self.sources = sources or self.DEFAULT_SOURCES
        self.source_fetchers = source_fetchers or {}
        self.merger = DataMerger()

    def _get_rotation_index(self) -> int:
        """获取当前轮询索引"""
        try:
            index = self.cache.get(self.ROTATION_INDEX_KEY)
            return int(index) if index else 0
        except:
            return 0

    def _set_rotation_index(self, index: int):
        """设置当前轮询索引"""
        self.cache.set(self.ROTATION_INDEX_KEY, str(index), ttl=86400 * 7)

    def _advance_index(self) -> int:
        """推进轮询索引并返回新索引"""
        current = self._get_rotation_index()
        next_index = (current + 1) % len(self.sources)
        self._set_rotation_index(next_index)
        return next_index

    def register_fetcher(self, source: DataSource, fetcher: Callable):
        """
        注册数据源获取函数

        Args:
            source: 数据源类型
            fetcher: 异步获取函数，签名为 async def fetch(limit: int) -> List[Dict]
        """
        self.source_fetchers[source] = fetcher

    async def _fetch_from_source(
        self,
        source: DataSource,
        limit: int = 2000
    ) -> List[Dict]:
        """从指定数据源获取原始数据"""
        if source not in self.source_fetchers:
            logger.warning(f"数据源 {source.value} 未注册获取函数")
            return []

        try:
            fetcher = self.source_fetchers[source]
            raw_data = await fetcher(limit=limit)

            if raw_data:
                logger.info(f"[{source.value}] 获取到 {len(raw_data)} 条原始数据")
            return raw_data or []

        except Exception as e:
            logger.error(f"[{source.value}] 获取数据失败: {e}")
            return []

    async def fetch(
        self,
        limit: int = 2000,
        max_sources: int = 6,
        retry_per_source: int = 2
    ) -> tuple[List[StockQuote], Optional[DataSource]]:
        """
        使用轮询策略获取数据

        策略：
        1. 每次请求都从下一个数据源开始
        2. 当前数据源失败时，先重试 retry_per_source 次
        3. 重试仍失败，才切换到下一个数据源
        4. 最多尝试 max_sources 个数据源

        Args:
            limit: 获取数量限制
            max_sources: 最多尝试多少个数据源
            retry_per_source: 每个数据源的重试次数

        Returns:
            (股票列表, 使用的数据源)
        """
        start_index = self._advance_index()

        for source_attempt in range(min(max_sources, len(self.sources))):
            current_index = (start_index + source_attempt) % len(self.sources)
            source = self.sources[current_index]

            # 对当前数据源进行重试
            for retry in range(retry_per_source):
                raw_data = await self._fetch_from_source(source, limit)

                if raw_data:
                    normalizer = NormalizerRegistry.get(source)
                    quotes = normalizer.normalize_batch(raw_data, skip_invalid=True)

                    if quotes:
                        logger.info(f"[{source.value}] 归一化后 {len(quotes)} 条数据，成功")
                        self._set_rotation_index(current_index)
                        return quotes, source

                # 重试前等待
                if retry < retry_per_source - 1:
                    await asyncio.sleep(0.5)
                    logger.warning(f"[{source.value}] 第{retry + 1}次重试...")

            logger.warning(f"[{source.value}] 重试{retry_per_source}次后仍失败，切换到下一个数据源")
            self._advance_index()

        logger.error("所有数据源尝试失败")
        return [], None

    async def update_cache(
        self,
        limit: int = 2000,
        ttl: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        获取数据并更新缓存

        Args:
            limit: 获取数量限制
            ttl: 缓存过期时间（秒）

        Returns:
            更新结果统计
        """
        import json

        result = {
            "success": False,
            "source": None,
            "count": 0,
            "cached": 0,
            "timestamp": datetime.now().isoformat(),
        }

        quotes, source = await self.fetch(limit, **kwargs)

        if not quotes:
            return result

        result["source"] = source.value if source else None
        result["count"] = len(quotes)
        ttl = ttl or self.DEFAULT_TTL
        cached = 0

        # 缓存单只股票
        for quote in quotes:
            try:
                key = f"{self.CACHE_PREFIX}:quote:{quote.code}"
                self.cache.set(key, quote.to_json(), ttl=ttl)
                cached += 1
            except Exception as e:
                logger.debug(f"缓存写入失败 {quote.code}: {e}")

        # 缓存股票列表
        try:
            list_key = f"{self.CACHE_PREFIX}:stock_list:all"
            list_data = {
                "data": [q.to_dict() for q in quotes],
                "source": source.value if source else None,
                "count": len(quotes),
                "cached_at": datetime.now().isoformat(),
            }
            self.cache.set(list_key, json.dumps(list_data, ensure_ascii=False), ttl=ttl)
        except Exception as e:
            logger.error(f"列表缓存写入失败: {e}")

        result["cached"] = cached
        result["success"] = cached > 0

        logger.info(f"缓存更新完成: 来源={source.value if source else 'N/A'}, 数量={cached}")
        return result

    def get_stock_list(self, max_age: int = 600) -> Optional[List[Dict]]:
        """获取缓存的股票列表"""
        import json
        list_key = f"{self.CACHE_PREFIX}:stock_list:all"

        try:
            cached = self.cache.get(list_key)
            if cached:
                data = json.loads(cached)
                # 注意：内存缓存不检查过期时间，由缓存后端负责
                return data.get("data", [])
        except Exception as e:
            logger.error(f"获取缓存失败: {e}")

        return None

    def get_stock_quote(self, code: str) -> Optional[Dict]:
        """获取单只股票缓存"""
        import json
        key = f"{self.CACHE_PREFIX}:quote:{code}"

        try:
            cached = self.cache.get(key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            logger.debug(f"获取股票缓存失败 {code}: {e}")

        return None

    def get_rotation_status(self) -> Dict[str, Any]:
        """获取轮询状态"""
        current_index = self._get_rotation_index()
        next_source = self.sources[(current_index + 1) % len(self.sources)]

        return {
            "current_index": current_index,
            "current_source": self.sources[current_index].value,
            "next_source": next_source.value,
            "total_sources": len(self.sources),
            "sources": [s.value for s in self.sources],
        }
