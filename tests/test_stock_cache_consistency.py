"""行情-缓存-回退-持久化链路的一致性测试。

覆盖场景：
- 完整区间命中：覆盖后不再调用上游
- 部分缓存 + 更大范围查询：不得返回残缺序列，只补齐缺失子区间
- 重叠/带缺口的区间：按集合差增量补齐
- 上游异常：明确失败，不留污染，恢复后重试可用
- 并发请求：单飞共享结果，上游只调用一次
- 进程恢复：覆盖记录持久化，重启后确定性命中
- 兼容：股票代码规范化、数据源降级顺序、无缓存查询、空数据应答
"""

import threading
import time
from datetime import datetime, timedelta
from typing import List, Optional

import pytest

import app.config as config
from app.chan.models import RawCandle
from app.data.fetcher import DataFetcher, FetchResult
from app.middleware.exception_handler import DataFetchException
from app.mappers.stock_mapper import StockMapper
from app.services.stock_service import StockService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def d(month: int, day: int) -> datetime:
    return datetime(2024, month, day)


def _daily_candles(start: datetime, end: datetime) -> List[RawCandle]:
    """为 [start, end] 每个自然日生成一根有效K线（确定性内容）。"""
    candles = []
    day = start
    while day <= end:
        candles.append(RawCandle(
            timestamp=day,
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000.0,
        ))
        day += timedelta(days=1)
    return candles


class StubFetcher(DataFetcher):
    """可控的假数据源：记录调用、可模拟失败/空应答/延迟。"""

    def __init__(self, name: str = "yahoo", fail: bool = False,
                 empty: bool = False, delay: float = 0.0):
        self._name = name
        self.fail = fail
        self.empty = empty
        self.delay = delay
        self.calls = []
        self._mu = threading.Lock()

    @property
    def source_name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return True

    def fetch_candles(self, stock_code, period, start_date=None, end_date=None):
        with self._mu:
            self.calls.append((start_date, end_date))
        if self.delay:
            time.sleep(self.delay)
        if self.fail:
            raise RuntimeError("upstream boom")
        if self.empty:
            return FetchResult([], stock_code, period, start_date, end_date,
                               self._name, True)
        candles = _daily_candles(start_date, end_date)
        return FetchResult(candles, stock_code, period, start_date, end_date,
                           self._name, True)

    @property
    def call_count(self) -> int:
        with self._mu:
            return len(self.calls)


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    """每个测试使用独立的临时 SQLite 文件。"""
    db_file = tmp_path / "cache_test.db"
    monkeypatch.setattr(config, "DATABASE_URL", f"sqlite:///{db_file}")
    monkeypatch.setattr(config, "_engine", None)
    monkeypatch.setattr(config, "_SessionLocal", None)
    config.init_database()
    yield db_file


@pytest.fixture
def stub():
    return StubFetcher("yahoo")


@pytest.fixture
def service(temp_db, stub):
    svc = StockService()
    # 两个数据源键都指向同一个 stub，调用计数集中在一处
    svc.fetchers = {"yahoo": stub, "akshare": stub}
    return svc


def _db_counts():
    """返回 (K线行数, 覆盖记录行数)，用于断言没有残留污染。"""
    with config.db_session_scope() as session:
        from app.entities.stock import StockCandle, CacheCoverage
        candle_rows = session.query(StockCandle).count()
        coverage_rows = session.query(CacheCoverage).count()
    return candle_rows, coverage_rows


def _coverages(code: str, period: str = "daily"):
    with config.db_session_scope() as session:
        mapper = StockMapper(session)
        return [(c.range_start, c.range_end) for c in mapper.get_coverages(code, period)]


# ---------------------------------------------------------------------------
# 完整区间命中
# ---------------------------------------------------------------------------

class TestFullCoverageHit:
    def test_second_identical_query_served_from_cache(self, service, stub):
        first = service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert len(first) == 31
        assert stub.call_count == 1

        second = service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert stub.call_count == 1  # 不再调用上游
        assert [c.timestamp for c in second] == [c.timestamp for c in first]

    def test_sub_range_of_coverage_hits_cache(self, service, stub):
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        sub = service.get_candles("600000", "daily", d(1, 5), d(1, 20))
        assert stub.call_count == 1
        assert len(sub) == 16
        assert sub[0].timestamp == d(1, 5)
        assert sub[-1].timestamp == d(1, 20)

    def test_normalized_code_shares_coverage(self, service, stub):
        # 同一股票的不同写法（600000 / sh600000）规范化后共享覆盖
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        service.get_candles("sh600000", "daily", d(1, 1), d(1, 31))
        assert stub.call_count == 1


# ---------------------------------------------------------------------------
# 部分缓存 + 更大范围：不得返回残缺序列
# ---------------------------------------------------------------------------

class TestPartialCacheDoesNotShortCircuit:
    def test_larger_range_triggers_incremental_fill(self, service, stub):
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert stub.call_count == 1

        # 缓存里只有 1 月的数据，查询 1-3 月必须补齐而不是直接返回 1 月
        candles = service.get_candles("600000", "daily", d(1, 1), d(3, 31))
        assert stub.call_count == 2
        assert candles[0].timestamp == d(1, 1)
        assert candles[-1].timestamp == d(3, 31)
        assert len(candles) == 91  # 31 + 29 + 31（2024 闰年）

        # 第二次大范围查询完整命中，不再调用上游
        again = service.get_candles("600000", "daily", d(1, 1), d(3, 31))
        assert stub.call_count == 2
        assert [c.timestamp for c in again] == [c.timestamp for c in candles]

    def test_only_missing_tail_is_fetched(self, service, stub):
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        service.get_candles("600000", "daily", d(1, 1), d(3, 31))
        # 第二次抓取的是缺失的尾部区间，而不是全量重抓
        fill_start, fill_end = stub.calls[1]
        assert fill_start <= d(2, 1)
        assert fill_end == d(3, 31)

    def test_gap_between_ranges_filled_incrementally(self, service, stub):
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        service.get_candles("600000", "daily", d(3, 1), d(3, 31))
        assert stub.call_count == 2

        # 跨两个已缓存区间的查询：只补齐中间缺口（2 月）
        candles = service.get_candles("600000", "daily", d(1, 1), d(3, 31))
        assert stub.call_count == 3
        gap_start, gap_end = stub.calls[2]
        assert gap_start == d(1, 31)  # 缺口从 1 月覆盖端点开始
        assert gap_end == d(3, 1)     # 到 3 月覆盖起点结束
        assert candles[0].timestamp == d(1, 1)
        assert candles[-1].timestamp == d(3, 31)
        assert len(candles) == 91

        # 覆盖区间合并为一段
        assert _coverages("sh600000") == [(d(1, 1), d(3, 31))]

    def test_coverage_merges_overlapping_fetches(self, service, stub):
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        service.get_candles("600000", "daily", d(1, 15), d(2, 15))
        assert _coverages("sh600000") == [(d(1, 1), d(2, 15))]


# ---------------------------------------------------------------------------
# 上游异常：明确失败、不留污染、恢复后可用
# ---------------------------------------------------------------------------

class TestUpstreamFailure:
    def test_failure_raises_and_leaves_no_pollution(self, service, stub):
        stub.fail = True
        with pytest.raises(DataFetchException):
            service.get_candles("600000", "daily", d(1, 1), d(1, 31))

        candle_rows, coverage_rows = _db_counts()
        assert candle_rows == 0
        assert coverage_rows == 0

        # 上游恢复后重试成功（失败尝试没有留下"已覆盖"假象）
        stub.fail = False
        candles = service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert len(candles) == 31
        assert _coverages("sh600000") == [(d(1, 1), d(1, 31))]

    def test_failure_result_also_raises(self, temp_db):
        """数据源返回 success=False 时同样明确失败。"""
        class FailingFetcher(StubFetcher):
            def fetch_candles(self, stock_code, period, start_date=None, end_date=None):
                return FetchResult([], stock_code, period, start_date, end_date,
                                   self._name, False, "source exploded")

        svc = StockService()
        svc.fetchers = {"yahoo": FailingFetcher("yahoo")}
        with pytest.raises(DataFetchException):
            svc.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert _db_counts() == (0, 0)

    def test_cache_write_failure_is_explicit_and_atomic(self, temp_db, stub, monkeypatch):
        """持久化失败：明确报错，且K线与覆盖记录一起回滚，不留半成功状态。"""
        def boom(self, entities):
            raise RuntimeError("disk full")

        monkeypatch.setattr(StockMapper, "upsert_candles", boom)
        svc = StockService()
        svc.fetchers = {"yahoo": stub, "akshare": stub}
        with pytest.raises(DataFetchException):
            svc.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert _db_counts() == (0, 0)


# ---------------------------------------------------------------------------
# 并发：单飞共享结果
# ---------------------------------------------------------------------------

class TestConcurrency:
    def _run_threads(self, fn, n=8):
        results = [None] * n
        errors = [None] * n

        def worker(i):
            try:
                results[i] = fn()
            except Exception as e:  # noqa: BLE001
                errors[i] = e

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results, errors

    def test_concurrent_same_range_single_upstream_call(self, service, stub):
        stub.delay = 0.2  # 放大竞态窗口
        results, errors = self._run_threads(
            lambda: service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        )
        assert all(e is None for e in errors)
        assert stub.call_count == 1  # 单飞：上游只被调用一次
        expected = [c.timestamp for c in results[0]]
        assert len(expected) == 31
        for r in results[1:]:
            assert [c.timestamp for c in r] == expected

    def test_concurrent_overlapping_ranges(self, service, stub):
        stub.delay = 0.1
        ranges = [(d(1, 1), d(1, 31)), (d(1, 15), d(2, 15))]
        results, errors = self._run_threads(
            lambda: service.get_candles("600000", "daily", *ranges[0])
        )
        results2, errors2 = self._run_threads(
            lambda: service.get_candles("600000", "daily", *ranges[1])
        )
        assert all(e is None for e in errors + errors2)
        # 两个区间各自完整
        assert results[0][0].timestamp == d(1, 1)
        assert results[0][-1].timestamp == d(1, 31)
        assert results2[0][0].timestamp == d(1, 15)
        assert results2[0][-1].timestamp == d(2, 15)
        # 合起来覆盖 [1-1, 2-15]，且没有重复抓取已覆盖部分
        assert _coverages("sh600000") == [(d(1, 1), d(2, 15))]
        assert stub.call_count <= 2

    def test_concurrent_mixed_hit_and_miss(self, service, stub):
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert stub.call_count == 1
        stub.delay = 0.1
        # 一批命中缓存，一批触发更大范围补齐
        hit_results, hit_errors = self._run_threads(
            lambda: service.get_candles("600000", "daily", d(1, 5), d(1, 20))
        )
        miss_results, miss_errors = self._run_threads(
            lambda: service.get_candles("600000", "daily", d(1, 1), d(2, 29))
        )
        assert all(e is None for e in hit_errors + miss_errors)
        assert all(len(r) == 16 for r in hit_results)
        assert all(len(r) == 60 for r in miss_results)  # 31 + 29
        # 大范围补齐只发生一次
        assert stub.call_count == 2


# ---------------------------------------------------------------------------
# 进程恢复后的确定性
# ---------------------------------------------------------------------------

class TestProcessRecovery:
    def test_new_engine_and_service_hit_persisted_cache(self, temp_db, stub, monkeypatch):
        svc1 = StockService()
        svc1.fetchers = {"yahoo": stub, "akshare": stub}
        before = svc1.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert stub.call_count == 1

        # 模拟进程重启：丢弃引擎/会话工厂，新建服务实例
        monkeypatch.setattr(config, "_engine", None)
        monkeypatch.setattr(config, "_SessionLocal", None)
        stub2 = StubFetcher("yahoo")
        svc2 = StockService()
        svc2.fetchers = {"yahoo": stub2, "akshare": stub2}

        after = svc2.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert stub2.call_count == 0  # 覆盖记录已持久化，无需再访上游
        assert [c.timestamp for c in after] == [c.timestamp for c in before]
        assert [c.close for c in after] == [c.close for c in before]


# ---------------------------------------------------------------------------
# 兼容性：降级顺序、无缓存查询、空数据应答、fetch_and_update
# ---------------------------------------------------------------------------

class TestCompatibility:
    def test_fallback_order_us_style_code_yahoo_first(self, temp_db):
        """sh 前缀代码：yahoo 优先，yahoo 失败降级 akshare。"""
        yahoo = StubFetcher("yahoo", fail=True)
        akshare = StubFetcher("akshare")
        svc = StockService()
        svc.fetchers = {"yahoo": yahoo, "akshare": akshare}

        candles = svc.get_candles("600000", "daily", d(1, 1), d(1, 10))
        assert len(candles) == 10
        assert yahoo.call_count == 1   # 先尝试 yahoo
        assert akshare.call_count == 1 # 失败后降级

    def test_fallback_order_digit_code_akshare_first(self, temp_db):
        """无前缀数字代码：akshare 优先，成功则不触达 yahoo。"""
        yahoo = StubFetcher("yahoo")
        akshare = StubFetcher("akshare")
        svc = StockService()
        svc.fetchers = {"yahoo": yahoo, "akshare": akshare}

        candles = svc.get_candles("200001", "daily", d(1, 1), d(1, 10))
        assert len(candles) == 10
        assert akshare.call_count == 1
        assert yahoo.call_count == 0

    def test_no_cache_query_bypasses_cache(self, service, stub):
        service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert stub.call_count == 1
        # use_cache=False 仍然直接访问上游
        fresh = service.get_candles("600000", "daily", d(1, 1), d(1, 31), use_cache=False)
        assert stub.call_count == 2
        assert len(fresh) == 31

    def test_empty_upstream_answer_is_cached(self, temp_db):
        """所有数据源一致确认无数据：返回空列表并记录覆盖，后续不再访问上游。"""
        stub = StubFetcher("yahoo", empty=True)
        svc = StockService()
        svc.fetchers = {"yahoo": stub, "akshare": stub}

        assert svc.get_candles("600000", "daily", d(1, 1), d(1, 31)) == []
        calls_after_first = stub.call_count
        assert svc.get_candles("600000", "daily", d(1, 1), d(1, 31)) == []
        assert stub.call_count == calls_after_first  # 第二次完整命中，不再访问上游
        assert _coverages("sh600000") == [(d(1, 1), d(1, 31))]

    def test_fetch_and_update_records_coverage(self, service, stub):
        result = service.fetch_and_update("600000", "daily", d(1, 1), d(1, 31))
        assert result["success"] is True
        assert result["count"] == 31
        assert stub.call_count == 1

        # fetch_and_update 写入的覆盖对 get_candles 可见
        candles = service.get_candles("600000", "daily", d(1, 1), d(1, 31))
        assert len(candles) == 31
        assert stub.call_count == 1

    def test_fetch_and_update_reports_cache_write_failure(
        self, temp_db, stub, monkeypatch
    ):
        """抓取成功但持久化失败：如实上报失败，不伪装成已可用。"""
        def boom(self, entities):
            raise RuntimeError("disk full")

        monkeypatch.setattr(StockMapper, "upsert_candles", boom)
        svc = StockService()
        svc.fetchers = {"yahoo": stub, "akshare": stub}
        result = svc.fetch_and_update("600000", "daily", d(1, 1), d(1, 31))
        assert result["success"] is False
        assert "Cache write failed" in result["error"]
        assert _db_counts() == (0, 0)


# ---------------------------------------------------------------------------
# API 级验证
# ---------------------------------------------------------------------------

class TestAPIConsistency:
    def test_candles_endpoint_deterministic_across_calls(
        self, temp_db, stub, monkeypatch
    ):
        from fastapi.testclient import TestClient
        from app.main import app
        from app.controllers import stock_controller

        monkeypatch.setattr(
            stock_controller.stock_service, "fetchers",
            {"yahoo": stub, "akshare": stub},
        )
        with TestClient(app) as client:
            params = {"start_date": "2024-01-01", "end_date": "2024-01-31"}
            r1 = client.get("/api/stocks/600000/candles", params=params)
            assert r1.status_code == 200
            r2 = client.get("/api/stocks/600000/candles", params=params)
            assert r2.status_code == 200

        assert stub.call_count == 1
        body1, body2 = r1.json(), r2.json()
        assert body1["count"] == 31
        assert body1["candles"] == body2["candles"]

    def test_candles_endpoint_upstream_failure_is_explicit(
        self, temp_db, monkeypatch
    ):
        from fastapi.testclient import TestClient
        from app.main import app
        from app.controllers import stock_controller

        failing = StubFetcher("yahoo", fail=True)
        monkeypatch.setattr(
            stock_controller.stock_service, "fetchers",
            {"yahoo": failing, "akshare": failing},
        )
        with TestClient(app) as client:
            r = client.get(
                "/api/stocks/600000/candles",
                params={"start_date": "2024-01-01", "end_date": "2024-01-31"},
            )
        assert r.status_code == 502
        assert r.json()["error"]["code"] == "DATA_FETCH_ERROR"
        assert _db_counts() == (0, 0)
