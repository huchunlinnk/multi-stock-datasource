"""
多源数据合并服务

功能:
1. 合并来自多个数据源的数据
2. 按股票代码去重
3. 基于质量评分选择最佳数据
4. 从其他数据源补充缺失字段
"""

from typing import List, Dict, Optional, Tuple, Any
import logging

from ..models import StockQuote, DataSource


logger = logging.getLogger(__name__)


# 数据源权重配置
DEFAULT_SOURCE_WEIGHTS: Dict[DataSource, float] = {
    DataSource.TENCENT: 1.0,       # 腾讯最稳定，有行业数据
    DataSource.EASTMONEY: 0.9,     # 东财数据全面
    DataSource.AKSHARE: 0.85,      # AKShare 数据规范
    DataSource.BAOSTOCK: 0.75,     # BaoStock 备用
    DataSource.SINA: 0.70,         # 新浪备用
    DataSource.JOINQUANT: 0.70,    # 聚宽底层是腾讯
    DataSource.TUSHARE: 0.65,      # Tushare 需Token
}

# 字段权重配置（用于计算完整性）
DEFAULT_FIELD_WEIGHTS: Dict[str, float] = {
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


class DataMerger:
    """
    多源数据合并器

    核心功能:
    1. 将多个数据源的股票列表合并
    2. 对同一股票的多源数据进行质量评分
    3. 选择质量最高的数据作为主数据
    4. 从其他数据源补充缺失字段

    使用示例:
        merger = DataMerger()
        merged = merger.merge([tencent_quotes, eastmoney_quotes])
    """

    def __init__(
        self,
        source_weights: Optional[Dict[DataSource, float]] = None,
        field_weights: Optional[Dict[str, float]] = None,
    ):
        """
        初始化合并器

        Args:
            source_weights: 数据源权重，None 使用默认值
            field_weights: 字段权重，None 使用默认值
        """
        self.source_weights = source_weights or DEFAULT_SOURCE_WEIGHTS
        self.field_weights = field_weights or DEFAULT_FIELD_WEIGHTS

    def merge(
        self,
        quotes_list: List[List[StockQuote]],
        enrich: bool = True,
    ) -> List[StockQuote]:
        """
        合并多源数据

        Args:
            quotes_list: 来自多个数据源的股票列表
            enrich: 是否从其他数据源补充缺失字段

        Returns:
            List[StockQuote]: 合并后的股票列表
        """
        if not quotes_list:
            return []

        # 按股票代码分组
        grouped: Dict[str, List[StockQuote]] = {}
        total_count = 0

        for quotes in quotes_list:
            if not quotes:
                continue
            for quote in quotes:
                if quote.code not in grouped:
                    grouped[quote.code] = []
                grouped[quote.code].append(quote)
                total_count += 1

        logger.info(f"数据合并: {total_count} 条 -> {len(grouped)} 只唯一股票")

        # 对每只股票选择最佳数据
        merged = []
        for code, candidates in grouped.items():
            best = self._select_best(candidates)
            if best:
                # 尝试从其他数据源补充缺失字段
                if enrich and len(candidates) > 1:
                    best = self._enrich_from_others(best, candidates)
                merged.append(best)

        logger.info(f"合并完成: {len(merged)} 只股票")
        return merged

    def _select_best(self, candidates: List[StockQuote]) -> Optional[StockQuote]:
        """
        从多个候选数据中选择最佳的一个

        评分标准:
        - 数据源权重 (40%)
        - 数据完整性 (40%)
        - 数据新鲜度 (20%)

        Args:
            candidates: 同一股票的多个数据源数据

        Returns:
            StockQuote: 最佳数据
        """
        if not candidates:
            return None

        if len(candidates) == 1:
            candidates[0].quality_score = self._calculate_score(candidates[0])
            return candidates[0]

        # 计算每个候选的得分
        scored_candidates = [
            (quote, self._calculate_score(quote))
            for quote in candidates
        ]

        # 选择得分最高的
        best, best_score = max(scored_candidates, key=lambda x: x[1])
        best.quality_score = best_score

        return best

    def _calculate_score(self, quote: StockQuote) -> float:
        """
        计算数据质量得分

        Args:
            quote: 股票行情数据

        Returns:
            float: 质量得分 (0-1)
        """
        # 数据源权重 (40%)
        source_weight = self.source_weights.get(quote.source, 0.5)

        # 数据完整性 (40%)
        completeness = self._calculate_completeness(quote)

        # 新鲜度 (20%) - 所有数据都是刚获取的
        freshness = 1.0

        return source_weight * 0.4 + completeness * 0.4 + freshness * 0.2

    def _calculate_completeness(self, quote: StockQuote) -> float:
        """计算数据完整性分数"""
        score = 0.0

        for field, weight in self.field_weights.items():
            value = getattr(quote, field, None)
            if self._is_valid_value(value):
                score += weight

        return min(score, 1.0)

    @staticmethod
    def _is_valid_value(value) -> bool:
        """判断值是否有效"""
        if value is None:
            return False
        if isinstance(value, (int, float)) and value != 0:
            return True
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, bool):
            return True
        return False

    def _enrich_from_others(
        self,
        best: StockQuote,
        candidates: List[StockQuote]
    ) -> StockQuote:
        """
        从其他数据源补充缺失字段

        Args:
            best: 当前最佳数据
            candidates: 所有候选数据

        Returns:
            StockQuote: 补充后的数据
        """
        # 按数据源权重排序
        sorted_candidates = sorted(
            candidates,
            key=lambda q: self.source_weights.get(q.source, 0.5),
            reverse=True
        )

        # 需要补充的字段
        enrich_fields = [
            ("sector", str),
            ("market_cap", float),
            ("total_cap", float),
            ("turnover_rate", float),
            ("high", float),
            ("low", float),
            ("open", float),
            ("pre_close", float),
        ]

        for field, field_type in enrich_fields:
            current_value = getattr(best, field, None)

            # 如果当前值为空或0，尝试从其他源获取
            if not current_value or current_value == 0 or current_value == "":
                for candidate in sorted_candidates:
                    candidate_value = getattr(candidate, field, None)
                    if candidate_value and candidate_value != 0 and candidate_value != "":
                        setattr(best, field, candidate_value)
                        break

        # 重新计算质量分
        best.quality_score = self._calculate_score(best)

        return best

    def get_statistics(
        self,
        quotes_list: List[List[StockQuote]]
    ) -> Dict[str, Any]:
        """
        获取合并统计信息

        Args:
            quotes_list: 来自多个数据源的股票列表

        Returns:
            Dict: 统计信息
        """
        stats = {
            "sources": {},
            "total_records": 0,
            "unique_stocks": 0,
            "by_market": {"SH": 0, "SZ": 0, "BJ": 0, "": 0},
            "by_board": {},
            "avg_quality_score": 0.0,
        }

        seen_codes = set()
        all_quotes = []

        for quotes in quotes_list:
            if not quotes:
                continue

            source_name = quotes[0].source.value if quotes else "unknown"
            stats["sources"][source_name] = len(quotes)
            stats["total_records"] += len(quotes)

            for quote in quotes:
                all_quotes.append(quote)

                if quote.code not in seen_codes:
                    seen_codes.add(quote.code)

                    # 按市场统计
                    market = quote.market or ""
                    if market in stats["by_market"]:
                        stats["by_market"][market] += 1
                    else:
                        stats["by_market"][""] += 1

                    # 按板块统计
                    board = quote.board or "unknown"
                    stats["by_board"][board] = stats["by_board"].get(board, 0) + 1

        stats["unique_stocks"] = len(seen_codes)

        # 计算平均质量分
        if all_quotes:
            stats["avg_quality_score"] = round(
                sum(q.quality_score for q in all_quotes) / len(all_quotes),
                2
            )

        return stats


# 便捷函数
def merge_quotes(
    quotes_list: List[List[StockQuote]],
    source_weights: Optional[Dict[DataSource, float]] = None,
) -> List[StockQuote]:
    """
    便捷函数：合并多源数据

    Args:
        quotes_list: 来自多个数据源的股票列表
        source_weights: 数据源权重

    Returns:
        List[StockQuote]: 合并后的股票列表
    """
    merger = DataMerger(source_weights=source_weights)
    return merger.merge(quotes_list)
