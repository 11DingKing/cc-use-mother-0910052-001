import threading
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import logging
from app.config import db_session_scope
from app.mappers.stock_mapper import StockMapper
from app.data.fetcher import FetchResult
from app.data.akshare_fetcher import AKShareFetcher
from app.data.yahoo_fetcher import YahooFetcher
from app.chan.models import RawCandle
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.utils.coverage import compute_missing_ranges, is_fully_covered
from app.middleware.exception_handler import DataFetchException
logger = logging.getLogger(__name__)
class StockService:
    """行情查询服务。

    缓存一致性约定：
    - 命中：请求区间 [start, end] 被 cache_coverage 表中的覆盖区间完整
      包含时才算命中，直接返回缓存内容；区间内有零星数据不算命中。
    - 补齐：未覆盖部分按集合差计算出缺失子区间，逐段向上游请求，
      K线与覆盖记录在同一事务内写入（要么同时生效，要么同时回滚）。
    - 失败：任一缺失子区间所有数据源都失败时抛出 DataFetchException，
      不会返回残缺数据，也不会留下半成功的缓存状态。
    - 并发：同一 (stock_code, period) 的查询通过单飞锁串行，并发请求
      共享同一次抓取结果，不会重复调用上游或重复写入。
    """

    # 类级单飞锁注册表：所有实例（行情接口/分析/回测各自持有的
    # StockService）共享，保证跨实例的并发协调。
    _locks_guard = threading.Lock()
    _range_locks: Dict[Tuple[str, str], threading.Lock] = {}

    def __init__(self):
        self.fetchers = self._init_fetchers()
    def _init_fetchers(self):
        fetchers = {}
        try:
            akshare = AKShareFetcher()
            if akshare.is_available():
                fetchers['akshare'] = akshare
        except Exception as e:
            logger.warning(f'AKShare not available: {e}')
        try:
            yahoo = YahooFetcher()
            if yahoo.is_available():
                fetchers['yahoo'] = yahoo
        except Exception as e:
            logger.warning(f'Yahoo Finance not available: {e}')
        return fetchers

    @classmethod
    def _lock_for(cls, stock_code: str, period: str) -> threading.Lock:
        key = (stock_code, period)
        with cls._locks_guard:
            lock = cls._range_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                cls._range_locks[key] = lock
            return lock

    def get_candles(self, stock_code, period, start_date=None, end_date=None, use_cache=True):
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        if not use_cache:
            # 无缓存查询：保持原有行为，直接向上游请求全区间并透传结果
            result = self._fetch_from_source(stock_code, period, start_date, end_date)
            if not result.success:
                raise DataFetchException(message=result.error_message or 'Failed', stock_code=stock_code, source=result.source)
            if result.candles:
                try:
                    self._save_to_cache(result.candles, stock_code, period, coverage=(start_date, end_date), source=result.source)
                except Exception as e:
                    logger.warning(f'Cache write error: {e}')
            return result.candles
        # 单飞：同一 (code, period) 的并发请求串行，后到者直接命中
        # 先到者刚写入的覆盖区间，共享抓取结果而不重复调用上游。
        with self._lock_for(stock_code, period):
            cached = self._get_covered_candles(stock_code, period, start_date, end_date)
            if cached is not None:
                return cached
            self._fill_missing(stock_code, period, start_date, end_date)
            cached = self._get_covered_candles(stock_code, period, start_date, end_date)
            if cached is None:
                # 补齐后仍不完整：说明持久化未生效，明确失败而不是返回残缺数据
                raise DataFetchException(
                    message='Cache fill incomplete: range is not fully covered after fetch',
                    stock_code=stock_code,
                    source='cache',
                )
            return cached

    def _get_covered_candles(self, stock_code, period, start_date, end_date):
        """区间被完整覆盖时返回缓存K线列表，否则返回 None。"""
        with db_session_scope() as session:
            mapper = StockMapper(session)
            coverages = mapper.get_coverages(stock_code, period)
            intervals = [(c.range_start, c.range_end) for c in coverages]
            if not is_fully_covered(start_date, end_date, intervals):
                return None
            candles = mapper.get_candles(stock_code, period, start_date, end_date)
            return mapper.to_raw_candles(candles)

    def _fill_missing(self, stock_code, period, start_date, end_date):
        """按集合差计算缺失子区间并逐段补齐；任一失败则明确抛出异常。"""
        with db_session_scope() as session:
            mapper = StockMapper(session)
            intervals = [(c.range_start, c.range_end) for c in mapper.get_coverages(stock_code, period)]
        missing = compute_missing_ranges(start_date, end_date, intervals)
        for m_start, m_end in missing:
            logger.info(f'Filling missing range [{m_start}, {m_end}] for {stock_code} {period}')
            result = self._fetch_from_source(stock_code, period, m_start, m_end)
            if not result.success:
                raise DataFetchException(
                    message=result.error_message or 'Failed',
                    stock_code=stock_code,
                    source=result.source,
                    details={'missing_range': [m_start.isoformat(), m_end.isoformat()]},
                )
            try:
                self._save_to_cache(result.candles, stock_code, period, coverage=(m_start, m_end), source=result.source)
            except Exception as e:
                # 写入失败则缓存状态未变（事务已回滚），明确失败而非让
                # 调用方误以为数据已经可用
                raise DataFetchException(
                    message=f'Cache write failed: {e}',
                    stock_code=stock_code,
                    source='cache',
                    details={'missing_range': [m_start.isoformat(), m_end.isoformat()]},
                )

    def _save_to_cache(self, candles, stock_code, period, coverage=None, source=None):
        """K线与覆盖记录在同一事务内写入，保证不会出现半成功状态。"""
        with db_session_scope() as session:
            mapper = StockMapper(session)
            entities = mapper.from_raw_candles(candles, stock_code, period)
            mapper.upsert_candles(entities)
            if coverage is not None:
                mapper.add_coverage(
                    stock_code, period, coverage[0], coverage[1],
                    source=source, candle_count=len(candles),
                )

    def _fetch_from_source(self, stock_code, period, start_date, end_date):
        # 保持既有降级顺序：纯数字代码 akshare 优先，否则 yahoo 优先
        sources = ['akshare', 'yahoo'] if stock_code[0].isdigit() else ['yahoo', 'akshare']
        last_error = None
        empty_confirmed = False
        empty_source = None
        for source_name in sources:
            fetcher = self.fetchers.get(source_name)
            if not fetcher:
                continue
            try:
                result = fetcher.fetch_candles(stock_code, period, start_date, end_date)
                if result.success and result.candles:
                    return result
                if result.success:
                    # 数据源明确应答"该区间无数据"（与请求失败可区分）
                    empty_confirmed = True
                    empty_source = source_name
                else:
                    last_error = result.error_message or last_error
            except Exception as e:
                last_error = str(e)
        if empty_confirmed and last_error is None:
            # 所有可用数据源一致确认无数据：这是可缓存的确定结论
            return FetchResult([], stock_code, period, start_date, end_date, empty_source or 'none', True)
        return FetchResult([], stock_code, period, start_date, end_date, 'none', False, last_error)
    def fetch_and_update(self, stock_code, period, start_date=None, end_date=None):
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        cache_error = None
        with self._lock_for(stock_code, period):
            result = self._fetch_from_source(stock_code, period, start_date, end_date)
            if result.success:
                try:
                    self._save_to_cache(result.candles, stock_code, period, coverage=(start_date, end_date), source=result.source)
                except Exception as e:
                    # 抓取成功但持久化失败：缓存未更新（事务已回滚），
                    # 如实上报失败，不让调用方误以为数据已经可用
                    logger.warning(f'Cache write error: {e}')
                    cache_error = str(e)
        success = result.success and cache_error is None
        if cache_error is not None:
            error = f'Cache write failed: {cache_error}'
        elif not result.success:
            error = result.error_message
        else:
            error = None
        return {'stock_code': stock_code, 'period': period, 'start_date': start_date.isoformat(), 'end_date': end_date.isoformat(), 'success': success, 'count': len(result.candles), 'source': result.source, 'error': error}
