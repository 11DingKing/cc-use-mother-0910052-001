"""缓存一致性测试。

覆盖以下场景：
- 完整区间命中：请求区间被覆盖区间完全包含时不访问上游；
- 部分缓存不误判为命中：缓存只有零星数据时必须向上游补齐；
- 重叠区间增量补齐：只抓取缺失子区间；
- 上游异常：明确失败且不留下覆盖记录，恢复后重试成功；
- 空结果区间：上游明确返回空也计入覆盖，不重复打上游；
- 并发相同/重叠区间：共享首次拉取结果，无重复写入；
- 进程恢复：覆盖区间持久化，新实例直接命中；
- 兼容性：use_cache=False、数据源降级顺序、fetch_and_update。
"""

import threading
import time
from datetime import datetime, timedelta
from typing import List, Optional

import pytest

from app.chan.models import RawCandle
from app.config import db_session_scope, get_engine
from app.data.fetcher import DataFetcher, FetchResult
from app.entities.stock import Base as StockBase
from app.entities.stock import StockCandle, StockCandleCoverage
from app.mappers.stock_mapper import StockMapper
from app.middleware.exception_handler import DataFetchException
from app.services.stock_service import StockService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def biz_days(start: datetime, end: datetime) -> List[datetime]:
    """业务模块说明。"""
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def make_candles(start: datetime, end: datetime, base: float = 100.0) -> List[RawCandle]:
    """为 [start, end] 内每个工作日生成一根日线。"""
    candles = []
    for i, d in enumerate(biz_days(start, end)):
        price = base + i
        candles.append(RawCandle(
            timestamp=d,
            open=price,
            high=price + 2.0,
            low=price - 2.0,
            close=price + 1.0,
            volume=1000.0 + i,
        ))
    return candles


class ControllableFetcher(DataFetcher):
    """可控制行为的上游数据源：记录调用、可注入延迟与故障。"""

    def __init__(
        self,
        name: str = "akshare",
        candles: Optional[List[RawCandle]] = None,
        error: Optional[Exception] = None,
        delay: float = 0.0,
    ):
        self._name = name
        self.candles = list(candles or [])
        self.error = error
        self.delay = delay
        self.calls = []
        self._lock = threading.Lock()

    @property
    def source_name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return True

    def call_count(self) -> int:
        with self._lock:
            return len(self.calls)

    def requested_ranges(self):
        with self._lock:
            return [(c[2], c[3]) for c in self.calls]

    def fetch_candles(self, stock_code, period, start_date=None, end_date=None):
        with self._lock:
            self.calls.append((stock_code, period, start_date, end_date))
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        result = [
            c for c in self.candles
            if (start_date is None or c.timestamp >= start_date)
            and (end_date is None or c.timestamp <= end_date)
        ]
        return FetchResult(
            candles=list(result),
            stock_code=stock_code,
            period=period,
            start_time=start_date,
            end_time=end_date,
            source=self._name,
            success=True,
        )


def make_service(fetchers: dict) -> StockService:
    """构造注入 mock 数据源的 StockService。"""
    service = StockService()
    service.fetchers = fetchers
    return service


def read_coverage(stock_code: str, period: str):
    with db_session_scope() as session:
        mapper = StockMapper(session)
        return [(c.start_time, c.end_time) for c in mapper.get_coverage(stock_code, period)]


def read_cached_candles(stock_code: str, period: str):
    with db_session_scope() as session:
        mapper = StockMapper(session)
        return [(c.timestamp, c.open, c.close) for c in mapper.get_candles(stock_code, period)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

JAN1 = datetime(2024, 1, 1)
JAN31 = datetime(2024, 1, 31)
FEB1 = datetime(2024, 2, 1)
FEB29 = datetime(2024, 2, 29)
MAR1 = datetime(2024, 3, 1)
MAR31 = datetime(2024, 3, 31)
JUN30 = datetime(2024, 6, 30)


@pytest.fixture(autouse=True)
def setup_database():
    """每个测试使用干净的K线与覆盖区间表。"""
    engine = get_engine()
    StockBase.metadata.drop_all(bind=engine)
    StockBase.metadata.create_all(bind=engine)
    yield
    StockBase.metadata.drop_all(bind=engine)


# ---------------------------------------------------------------------------
# 完整区间命中
# ---------------------------------------------------------------------------

class TestFullCoverageHit:
    """业务模块说明。"""

    def test_first_query_fetches_and_caches(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        result = service.get_candles("600000", "daily", JAN1, MAR31)

        assert [c.timestamp for c in result] == [c.timestamp for c in candles]
        assert fetcher.call_count() == 1
        # 覆盖区间记录为完整请求区间
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

    def test_subrange_hit_without_upstream(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})
        service.get_candles("600000", "daily", JAN1, MAR31)
        assert fetcher.call_count() == 1

        # 子区间完全命中，不再访问上游
        result = service.get_candles("600000", "daily", FEB1, FEB29)
        assert fetcher.call_count() == 1
        expected = [c for c in candles if FEB1 <= c.timestamp <= FEB29]
        assert [c.timestamp for c in result] == [c.timestamp for c in expected]

    def test_same_range_hit_without_upstream(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        first = service.get_candles("600000", "daily", JAN1, MAR31)
        second = service.get_candles("600000", "daily", JAN1, MAR31)

        assert fetcher.call_count() == 1
        assert [c.timestamp for c in first] == [c.timestamp for c in second]


# ---------------------------------------------------------------------------
# 部分缓存不再误判为命中（核心缺陷）
# ---------------------------------------------------------------------------

class TestPartialCacheNotAHit:
    """业务模块说明。"""

    def test_scattered_cache_rows_do_not_satisfy_range_query(self):
        # 模拟历史遗留：缓存里只有零星几根数据，且没有覆盖记录
        full = make_candles(JAN1, MAR31)
        scattered = full[5:8]
        with db_session_scope() as session:
            mapper = StockMapper(session)
            mapper.upsert_candles(mapper.from_raw_candles(scattered, "sh600000", "daily"))

        fetcher = ControllableFetcher(candles=full)
        service = make_service({"akshare": fetcher})

        result = service.get_candles("600000", "daily", JAN1, MAR31)

        # 必须访问上游并返回完整序列，而不是静默返回零星数据
        assert fetcher.call_count() >= 1
        assert [c.timestamp for c in result] == [c.timestamp for c in full]

    def test_larger_range_after_small_range_cached(self):
        # 先缓存小区间，再查大区间：旧实现会直接返回小区间的残缺数据
        candles = make_candles(JAN1, JUN30)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        small = service.get_candles("600000", "daily", FEB1, FEB29)
        assert len(small) == len(biz_days(FEB1, FEB29))

        result = service.get_candles("600000", "daily", JAN1, JUN30)
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]

        # 再次查询大区间：完全命中，不再访问上游
        calls_before = fetcher.call_count()
        again = service.get_candles("600000", "daily", JAN1, JUN30)
        assert fetcher.call_count() == calls_before
        assert [c.timestamp for c in again] == [c.timestamp for c in candles]


# ---------------------------------------------------------------------------
# 重叠区间增量补齐
# ---------------------------------------------------------------------------

class TestIncrementalGapFill:
    """业务模块说明。"""

    def test_only_missing_tail_is_fetched(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        service.get_candles("600000", "daily", JAN1, FEB29)
        assert fetcher.call_count() == 1

        result = service.get_candles("600000", "daily", JAN1, MAR31)
        assert fetcher.call_count() == 2

        # 第二次只请求缺失的尾部 (FEB29, MAR31]
        _, second_range = fetcher.requested_ranges()
        assert second_range[0] == FEB29
        assert second_range[1] == MAR31

        assert [c.timestamp for c in result] == [c.timestamp for c in candles]
        # 覆盖区间合并为一段
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

    def test_missing_head_and_tail_filled(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        service.get_candles("600000", "daily", FEB1, FEB29)
        result = service.get_candles("600000", "daily", JAN1, MAR31)

        # 头部 [JAN1, FEB1) 与尾部 (FEB29, MAR31] 各补一次
        assert fetcher.call_count() == 3
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

    def test_gap_fill_merges_with_cached_data(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        service.get_candles("600000", "daily", JAN1, JAN31)
        service.get_candles("600000", "daily", MAR1, MAR31)
        result = service.get_candles("600000", "daily", JAN1, MAR31)

        # 只补中间缺口 (JAN31, MAR1)
        assert fetcher.call_count() == 3
        gap_range = fetcher.requested_ranges()[-1]
        assert gap_range == (JAN31, MAR1)
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]


# ---------------------------------------------------------------------------
# 上游异常：明确失败、不留污染、恢复后重试
# ---------------------------------------------------------------------------

class TestUpstreamFailure:
    """业务模块说明。"""

    def test_failure_raises_and_records_nothing(self):
        fetcher = ControllableFetcher(error=ConnectionError("upstream down"))
        service = make_service({"akshare": fetcher})

        with pytest.raises(DataFetchException):
            service.get_candles("600000", "daily", JAN1, MAR31)

        # 不留任何缓存与覆盖记录
        assert read_coverage("sh600000", "daily") == []
        assert read_cached_candles("sh600000", "daily") == []

    def test_retry_after_recovery_succeeds(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles, error=ConnectionError("upstream down"))
        service = make_service({"akshare": fetcher})

        with pytest.raises(DataFetchException):
            service.get_candles("600000", "daily", JAN1, MAR31)

        # 上游恢复后重试成功，且数据完整
        fetcher.error = None
        result = service.get_candles("600000", "daily", JAN1, MAR31)
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

    def test_failed_gap_does_not_corrupt_existing_coverage(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        service.get_candles("600000", "daily", JAN1, FEB29)

        # 扩展区间时上游故障：明确失败
        fetcher.error = ConnectionError("upstream down")
        with pytest.raises(DataFetchException):
            service.get_candles("600000", "daily", JAN1, MAR31)

        # 既有覆盖区间不受影响，仍可命中
        fetcher.error = None
        calls_before = fetcher.call_count()
        result = service.get_candles("600000", "daily", JAN1, FEB29)
        assert fetcher.call_count() == calls_before
        expected = [c for c in candles if c.timestamp <= FEB29]
        assert [c.timestamp for c in result] == [c.timestamp for c in expected]

    def test_fallback_to_second_source_on_failure(self):
        # A股代码规范化后带前缀，降级顺序为 yahoo -> akshare
        candles = make_candles(JAN1, MAR31)
        yahoo = ControllableFetcher(name="yahoo", error=ConnectionError("yahoo down"))
        akshare = ControllableFetcher(name="akshare", candles=candles)
        service = make_service({"yahoo": yahoo, "akshare": akshare})

        result = service.get_candles("600000", "daily", JAN1, MAR31)

        assert yahoo.call_count() == 1
        assert akshare.call_count() == 1
        assert len(result) == len(candles)

    def test_digit_leading_code_prefers_akshare(self):
        # 港股代码保持数字开头，降级顺序为 akshare -> yahoo
        candles = make_candles(JAN1, MAR31)
        akshare = ControllableFetcher(name="akshare", error=ConnectionError("akshare down"))
        yahoo = ControllableFetcher(name="yahoo", candles=candles)
        service = make_service({"akshare": akshare, "yahoo": yahoo})

        result = service.get_candles("00700", "daily", JAN1, MAR31)

        assert akshare.call_count() == 1
        assert yahoo.call_count() == 1
        assert len(result) == len(candles)


# ---------------------------------------------------------------------------
# 空结果区间
# ---------------------------------------------------------------------------

class TestEmptyRangeCoverage:
    """业务模块说明。"""

    def test_empty_upstream_result_is_covered(self):
        fetcher = ControllableFetcher(candles=[])
        service = make_service({"akshare": fetcher})

        result = service.get_candles("600000", "daily", JAN1, MAR31)
        assert result == []
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

        # 已确认无数据的区间不再重复请求上游
        again = service.get_candles("600000", "daily", JAN1, MAR31)
        assert again == []
        assert fetcher.call_count() == 1


# ---------------------------------------------------------------------------
# 并发行为
# ---------------------------------------------------------------------------

class TestConcurrency:
    """业务模块说明。"""

    def test_concurrent_same_range_shares_single_fetch(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles, delay=0.2)
        service = make_service({"akshare": fetcher})

        results = [None] * 8
        errors = [None] * 8

        def worker(i):
            try:
                results[i] = service.get_candles("600000", "daily", JAN1, MAR31)
            except Exception as e:  # pragma: no cover
                errors[i] = e

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(e is None for e in errors)
        # 上游只被调用一次，所有请求共享同一完整结果
        assert fetcher.call_count() == 1
        expected_ts = [c.timestamp for c in candles]
        for r in results:
            assert [c.timestamp for c in r] == expected_ts

        # 数据库无重复行，覆盖区间恰为一段
        rows = read_cached_candles("sh600000", "daily")
        assert len(rows) == len(candles)
        assert len({r[0] for r in rows}) == len(rows)
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

    def test_concurrent_overlapping_ranges(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles, delay=0.1)
        service = make_service({"akshare": fetcher})

        results = {}

        def worker(key, start, end):
            results[key] = service.get_candles("600000", "daily", start, end)

        t1 = threading.Thread(target=worker, args=("a", JAN1, FEB29))
        t2 = threading.Thread(target=worker, args=("b", FEB1, MAR31))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # 两个请求都拿到各自完整区间
        expected_a = [c for c in candles if c.timestamp <= FEB29]
        expected_b = [c for c in candles if c.timestamp >= FEB1]
        assert [c.timestamp for c in results["a"]] == [c.timestamp for c in expected_a]
        assert [c.timestamp for c in results["b"]] == [c.timestamp for c in expected_b]

        # 串行化补齐：每个区间只抓一次，且数据库无重复
        assert fetcher.call_count() == 2
        rows = read_cached_candles("sh600000", "daily")
        assert len(rows) == len({r[0] for r in rows})
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

        # 合并后的完整区间直接命中
        result = service.get_candles("600000", "daily", JAN1, MAR31)
        assert fetcher.call_count() == 2
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]

    def test_concurrent_duplicate_writes_do_not_conflict(self):
        # 上游返回带重复时间戳的数据，并发写入也不应产生唯一键冲突
        candles = make_candles(JAN1, MAR31)
        duplicated = candles + list(candles)  # 每根出现两次
        fetcher = ControllableFetcher(candles=duplicated, delay=0.05)
        service = make_service({"akshare": fetcher})

        errors = []

        def worker():
            try:
                service.get_candles("600000", "daily", JAN1, MAR31)
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        rows = read_cached_candles("sh600000", "daily")
        assert len(rows) == len(candles)


# ---------------------------------------------------------------------------
# 进程恢复后的确定性
# ---------------------------------------------------------------------------

class TestProcessRecovery:
    """业务模块说明。"""

    def test_new_service_instance_hits_persisted_coverage(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})
        service.get_candles("600000", "daily", JAN1, MAR31)
        assert fetcher.call_count() == 1

        # 模拟进程重启：全新实例，上游若被调用会抛异常
        failing = ControllableFetcher(error=AssertionError("upstream must not be called"))
        recovered = make_service({"akshare": failing})

        result = recovered.get_candles("600000", "daily", JAN1, MAR31)
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]

        sub = recovered.get_candles("600000", "daily", FEB1, FEB29)
        expected = [c for c in candles if FEB1 <= c.timestamp <= FEB29]
        assert [c.timestamp for c in sub] == [c.timestamp for c in expected]

    def test_uncovered_tail_after_recovery_fetches_incrementally(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})
        service.get_candles("600000", "daily", JAN1, FEB29)

        # 重启后查询更大区间：只补缺失尾部
        recovered_fetcher = ControllableFetcher(candles=candles)
        recovered = make_service({"akshare": recovered_fetcher})
        result = recovered.get_candles("600000", "daily", JAN1, MAR31)

        assert recovered_fetcher.call_count() == 1
        assert recovered_fetcher.requested_ranges()[0] == (FEB29, MAR31)
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]


# ---------------------------------------------------------------------------
# 兼容性：无缓存查询与 fetch_and_update
# ---------------------------------------------------------------------------

class TestCompatibility:
    """业务模块说明。"""

    def test_use_cache_false_bypasses_cache_read(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        service.get_candles("600000", "daily", JAN1, MAR31)
        assert fetcher.call_count() == 1

        # use_cache=False 强制回源
        result = service.get_candles("600000", "daily", JAN1, MAR31, use_cache=False)
        assert fetcher.call_count() == 2
        assert [c.timestamp for c in result] == [c.timestamp for c in candles]

    def test_use_cache_false_failure_raises(self):
        fetcher = ControllableFetcher(error=ConnectionError("down"))
        service = make_service({"akshare": fetcher})

        with pytest.raises(DataFetchException):
            service.get_candles("600000", "daily", JAN1, MAR31, use_cache=False)

    def test_fetch_and_update_records_coverage(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        result = service.fetch_and_update("600000", "daily", JAN1, MAR31)

        assert result["success"] is True
        assert result["count"] == len(candles)
        assert result["stock_code"] == "sh600000"
        assert read_coverage("sh600000", "daily") == [(JAN1, MAR31)]

        # 之后的缓存查询直接命中
        cached = service.get_candles("600000", "daily", JAN1, MAR31)
        assert fetcher.call_count() == 1
        assert len(cached) == len(candles)

    def test_fetch_and_update_failure_reports_error(self):
        fetcher = ControllableFetcher(error=ConnectionError("down"))
        service = make_service({"akshare": fetcher})

        result = service.fetch_and_update("600000", "daily", JAN1, MAR31)

        assert result["success"] is False
        assert read_coverage("sh600000", "daily") == []


# ---------------------------------------------------------------------------
# 分析与回测调用方式兼容
# ---------------------------------------------------------------------------

class TestAnalysisIntegration:
    """业务模块说明。"""

    def test_analysis_gets_complete_data_across_ranges(self):
        from unittest.mock import patch
        from app.services.analysis_service import AnalysisService

        candles = make_candles(JAN1, JUN30)
        fetcher = ControllableFetcher(candles=candles)
        stock_service = make_service({"akshare": fetcher})

        with patch.object(AnalysisService, "stock_service", stock_service):
            service = AnalysisService()
            first = service.run_analysis("600000", "daily", JAN1, MAR31)
            second = service.run_analysis("600000", "daily", JAN1, JUN30)

        # 第二次分析使用的是补齐后的完整数据，而非第一次的缓存残段
        assert first["candle_count"] == len(biz_days(JAN1, MAR31))
        assert second["candle_count"] == len(biz_days(JAN1, JUN30))
        assert second["candle_count"] > first["candle_count"]


# ---------------------------------------------------------------------------
# 区间减法与零宽区间
# ---------------------------------------------------------------------------

class TestRangeMath:
    """业务模块说明。"""

    def test_subtract_ranges_cases(self):
        from app.services.stock_service import _subtract_ranges

        # 无覆盖 -> 整个区间为缺失
        assert _subtract_ranges(JAN1, MAR31, []) == [(JAN1, MAR31)]
        # 完全覆盖 -> 无缺失
        assert _subtract_ranges(JAN1, MAR31, [(JAN1, MAR31)]) == []
        assert _subtract_ranges(JAN1, MAR31, [(JAN1, MAR31), (JAN1, FEB1)]) == []
        # 头部缺失
        assert _subtract_ranges(JAN1, MAR31, [(FEB1, MAR31)]) == [(JAN1, FEB1)]
        # 尾部缺失
        assert _subtract_ranges(JAN1, MAR31, [(JAN1, FEB29)]) == [(FEB29, MAR31)]
        # 中间缺口
        assert _subtract_ranges(JAN1, MAR31, [(JAN1, JAN31), (MAR1, MAR31)]) == [(JAN31, MAR1)]
        # 超出请求范围的覆盖被裁剪
        assert _subtract_ranges(FEB1, FEB29, [(JAN1, MAR31)]) == []

    def test_zero_width_range(self):
        from app.services.stock_service import _subtract_ranges

        # 零宽区间：未覆盖即缺失该点
        assert _subtract_ranges(FEB1, FEB1, []) == [(FEB1, FEB1)]
        assert _subtract_ranges(FEB1, FEB1, [(JAN1, MAR31)]) == []
        assert _subtract_ranges(FEB1, FEB1, [(FEB1, FEB1)]) == []

    def test_zero_width_query_fetches_single_day(self):
        candles = make_candles(JAN1, MAR31)
        fetcher = ControllableFetcher(candles=candles)
        service = make_service({"akshare": fetcher})

        day = datetime(2024, 2, 5)  # 周一
        result = service.get_candles("600000", "daily", day, day)
        assert fetcher.call_count() == 1
        assert [c.timestamp for c in result] == [day]

        # 再次查询命中
        again = service.get_candles("600000", "daily", day, day)
        assert fetcher.call_count() == 1
        assert [c.timestamp for c in again] == [day]


# ---------------------------------------------------------------------------
# API 级别的可观察行为
# ---------------------------------------------------------------------------

class TestAPICacheBehavior:
    """业务模块说明。"""

    @pytest.fixture
    def api_client(self):
        from fastapi.testclient import TestClient
        from app.main import app
        from app.controllers import stock_controller

        fetcher = ControllableFetcher(candles=make_candles(JAN1, JUN30))
        original = stock_controller.stock_service.fetchers
        stock_controller.stock_service.fetchers = {"akshare": fetcher}
        try:
            with TestClient(app) as client:
                yield client, fetcher
        finally:
            stock_controller.stock_service.fetchers = original

    def test_candles_endpoint_full_then_cached(self, api_client):
        client, fetcher = api_client

        resp = client.get(
            "/api/stocks/600000/candles",
            params={"period": "daily", "start_date": "2024-01-01", "end_date": "2024-03-31"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == len(biz_days(JAN1, MAR31))
        assert fetcher.call_count() == 1

        # 被覆盖的子区间直接命中，不再访问上游
        resp = client.get(
            "/api/stocks/600000/candles",
            params={"period": "daily", "start_date": "2024-02-01", "end_date": "2024-02-29"},
        )
        assert resp.status_code == 200
        assert resp.json()["count"] == len(biz_days(FEB1, FEB29))
        assert fetcher.call_count() == 1

    def test_candles_endpoint_expands_range_incrementally(self, api_client):
        client, fetcher = api_client

        client.get(
            "/api/stocks/600000/candles",
            params={"period": "daily", "start_date": "2024-01-01", "end_date": "2024-02-29"},
        )
        resp = client.get(
            "/api/stocks/600000/candles",
            params={"period": "daily", "start_date": "2024-01-01", "end_date": "2024-03-31"},
        )
        assert resp.status_code == 200
        # 返回完整大区间，且上游只补了缺失尾部
        assert resp.json()["count"] == len(biz_days(JAN1, MAR31))
        assert fetcher.call_count() == 2
        assert fetcher.requested_ranges()[-1] == (FEB29, MAR31)

    def test_candles_endpoint_upstream_error_is_explicit(self, api_client):
        client, fetcher = api_client
        fetcher.error = ConnectionError("upstream down")

        resp = client.get(
            "/api/stocks/600000/candles",
            params={"period": "daily", "start_date": "2024-01-01", "end_date": "2024-03-31"},
        )
        assert resp.status_code == 502
        body = resp.json()
        assert body["error"]["code"] == "DATA_FETCH_ERROR"

        # 失败后不留覆盖记录：上游恢复再请求可成功
        fetcher.error = None
        resp = client.get(
            "/api/stocks/600000/candles",
            params={"period": "daily", "start_date": "2024-01-01", "end_date": "2024-03-31"},
        )
        assert resp.status_code == 200
        assert resp.json()["count"] == len(biz_days(JAN1, MAR31))
