from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import logging
import threading
from app.config import db_session_scope
from app.mappers.stock_mapper import StockMapper
from app.data.fetcher import FetchResult
from app.data.akshare_fetcher import AKShareFetcher
from app.data.yahoo_fetcher import YahooFetcher
from app.chan.models import RawCandle
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.middleware.exception_handler import DataFetchException

logger = logging.getLogger(__name__)

# 进程级单飞锁：同一 (stock_code, period) 的缓存填充串行执行，
# 后到的并发请求在锁内重新检查覆盖区间，直接共享首次拉取的结果，
# 避免重复调用上游与并发写冲突。锁只活在进程内，重启后由持久化的
# 覆盖区间决定行为，结果依然确定。
_RANGE_LOCKS: Dict[Tuple[str, str], threading.RLock] = {}
_RANGE_LOCKS_GUARD = threading.Lock()


def _get_range_lock(stock_code: str, period: str) -> threading.RLock:
    key = (stock_code, period)
    with _RANGE_LOCKS_GUARD:
        lock = _RANGE_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _RANGE_LOCKS[key] = lock
        return lock


def _subtract_ranges(
    start: datetime,
    end: datetime,
    intervals: List[Tuple[datetime, datetime]],
) -> List[Tuple[datetime, datetime]]:
    """计算 [start, end] 中未被覆盖区间并集包含的缺失子区间。"""
    if start == end:
        # 零宽区间：单点未被任何覆盖区间包含时，该点即为缺失
        for iv_start, iv_end in intervals:
            if iv_start <= start <= iv_end:
                return []
        return [(start, end)]
    gaps = []
    cursor = start
    for iv_start, iv_end in sorted(intervals):
        if iv_end < cursor:
            continue
        if iv_start > cursor:
            gaps.append((cursor, min(iv_start, end)))
        cursor = max(cursor, iv_end)
        if cursor >= end:
            break
    if cursor < end:
        gaps.append((cursor, end))
    return gaps


class StockService:
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

    def get_candles(self, stock_code, period, start_date=None, end_date=None, use_cache=True):
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        if use_cache:
            return self._get_candles_cached(stock_code, period, start_date, end_date)
        result = self._fetch_from_source(stock_code, period, start_date, end_date)
        if not result.success:
            raise DataFetchException(message=result.error_message or 'Failed', stock_code=stock_code, source=result.source)
        if result.candles:
            self._save_to_cache(result.candles, stock_code, period, start_date, end_date, result.source)
        return result.candles

    def _get_candles_cached(self, stock_code, period, start_date, end_date):
        """缓存优先查询：只有请求区间被覆盖区间完全包含才算命中。

        缺失的子区间按可解释的策略向上游补齐（任一数据源成功应答即记录
        覆盖，含明确返回空的情况）；所有数据源都失败则明确抛出
        DataFetchException，绝不静默返回残缺数据。
        """
        lock = _get_range_lock(stock_code, period)
        with lock:
            coverage, cached = self._read_cache(stock_code, period, start_date, end_date)
            gaps = _subtract_ranges(start_date, end_date, coverage)
            if not gaps:
                return cached
            candles = list(cached)
            for gap_start, gap_end in gaps:
                result = self._fetch_gap(stock_code, period, gap_start, gap_end)
                if result.candles:
                    candles.extend(result.candles)
                self._save_to_cache(result.candles, stock_code, period, gap_start, gap_end, result.source)
            return self._merge_candles(candles, start_date, end_date)

    @staticmethod
    def _merge_candles(candles, start_date, end_date):
        """合并缓存与补齐数据：按时间戳去重、排序并裁剪到请求区间。"""
        by_ts = {}
        for c in candles:
            if c.timestamp < start_date or c.timestamp > end_date:
                continue
            by_ts[c.timestamp] = c
        return [by_ts[ts] for ts in sorted(by_ts)]

    def _read_cache(self, stock_code, period, start_date, end_date):
        """返回 (覆盖区间列表, 请求区间内已缓存的K线)。读取失败视为无覆盖。"""
        try:
            with db_session_scope() as session:
                mapper = StockMapper(session)
                coverage = [(c.start_time, c.end_time) for c in mapper.get_coverage(stock_code, period)]
                rows = mapper.get_candles(stock_code, period, start_date, end_date)
                return coverage, mapper.to_raw_candles(rows)
        except Exception as e:
            logger.warning(f'Cache read error: {e}')
            return [], []

    def _save_to_cache(self, candles, stock_code, period, start_time, end_time, source=None):
        """同一事务写入K线与覆盖区间。

        原子性保证：要么"数据+覆盖"同时落库，要么整体回滚——进程崩溃或
        并发冲突都不会留下"有覆盖无数据"或"部分写入"的污染。写失败仅记录
        日志：本次调用方已拿到完整数据，未记录的区间下次会重新拉取。
        """
        lock = _get_range_lock(stock_code, period)
        with lock:
            try:
                with db_session_scope() as session:
                    mapper = StockMapper(session)
                    if candles:
                        entities = mapper.from_raw_candles(candles, stock_code, period)
                        mapper.upsert_candles(entities)
                    mapper.add_coverage(stock_code, period, start_time, end_time, source)
            except Exception as e:
                logger.error(f'Cache write error: {e}')

    @staticmethod
    def _source_order(stock_code):
        return ['akshare', 'yahoo'] if stock_code[0].isdigit() else ['yahoo', 'akshare']

    def _fetch_gap(self, stock_code, period, gap_start, gap_end):
        """按既有降级顺序抓取缺失区间。

        任一数据源成功返回数据即采用；数据源成功但返回空视为该区间无
        交易数据（同样计入覆盖）；全部数据源失败则明确抛出异常，且不
        写入任何覆盖记录，后续请求会重试该区间。
        """
        last_error = None
        empty_result = None
        for source_name in self._source_order(stock_code):
            fetcher = self.fetchers.get(source_name)
            if not fetcher:
                continue
            try:
                result = fetcher.fetch_candles(stock_code, period, gap_start, gap_end)
            except Exception as e:
                last_error = str(e)
                continue
            if not result.success:
                last_error = result.error_message
                continue
            if result.candles:
                return result
            if empty_result is None:
                empty_result = result
        if empty_result is not None:
            return empty_result
        raise DataFetchException(
            message=last_error or 'Failed',
            stock_code=stock_code,
            source=None,
            details={'gap_start': gap_start.isoformat(), 'gap_end': gap_end.isoformat()},
        )

    def _fetch_from_source(self, stock_code, period, start_date, end_date):
        sources = self._source_order(stock_code)
        last_error = None
        for source_name in sources:
            fetcher = self.fetchers.get(source_name)
            if not fetcher:
                continue
            try:
                result = fetcher.fetch_candles(stock_code, period, start_date, end_date)
                if result.success and result.candles:
                    return result
                last_error = result.error_message
            except Exception as e:
                last_error = str(e)
        return FetchResult([], stock_code, period, start_date, end_date, 'none', False, last_error)

    def fetch_and_update(self, stock_code, period, start_date=None, end_date=None):
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        result = self._fetch_from_source(stock_code, period, start_date, end_date)
        if result.success and result.candles:
            self._save_to_cache(result.candles, stock_code, period, start_date, end_date, result.source)
        return {'stock_code': stock_code, 'period': period, 'start_date': start_date.isoformat(), 'end_date': end_date.isoformat(), 'success': result.success, 'count': len(result.candles), 'source': result.source, 'error': result.error_message}
