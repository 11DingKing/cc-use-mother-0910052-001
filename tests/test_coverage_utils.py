"""区间覆盖纯函数测试：命中判定与缺失区间计算。"""

from datetime import datetime, timedelta

from app.utils.coverage import (
    merge_intervals,
    compute_missing_ranges,
    is_fully_covered,
)


def d(day: int) -> datetime:
    return datetime(2024, 1, 1) + timedelta(days=day)


class TestMergeIntervals:
    def test_empty(self):
        assert merge_intervals([]) == []

    def test_overlapping_merged(self):
        merged = merge_intervals([(d(0), d(10)), (d(5), d(15))])
        assert merged == [(d(0), d(15))]

    def test_touching_merged(self):
        # 端点相接（前一段 end == 后一段 start）视为连续
        merged = merge_intervals([(d(0), d(10)), (d(10), d(20))])
        assert merged == [(d(0), d(20))]

    def test_disjoint_kept_separate(self):
        merged = merge_intervals([(d(0), d(10)), (d(12), d(20))])
        assert merged == [(d(0), d(10)), (d(12), d(20))]

    def test_unsorted_input(self):
        merged = merge_intervals([(d(12), d(20)), (d(0), d(10)), (d(9), d(13))])
        assert merged == [(d(0), d(20))]

    def test_contained_interval_absorbed(self):
        merged = merge_intervals([(d(0), d(30)), (d(5), d(10))])
        assert merged == [(d(0), d(30))]


class TestComputeMissingRanges:
    def test_no_coverage_everything_missing(self):
        assert compute_missing_ranges(d(0), d(10), []) == [(d(0), d(10))]

    def test_full_coverage_nothing_missing(self):
        assert compute_missing_ranges(d(0), d(10), [(d(0), d(10))]) == []

    def test_coverage_wider_than_query(self):
        assert compute_missing_ranges(d(3), d(7), [(d(0), d(10))]) == []

    def test_left_gap(self):
        missing = compute_missing_ranges(d(0), d(10), [(d(4), d(10))])
        assert missing == [(d(0), d(4))]

    def test_right_gap(self):
        missing = compute_missing_ranges(d(0), d(10), [(d(0), d(6))])
        assert missing == [(d(6), d(10))]

    def test_middle_gap(self):
        missing = compute_missing_ranges(d(0), d(20), [(d(0), d(5)), (d(10), d(20))])
        assert missing == [(d(5), d(10))]

    def test_multiple_gaps(self):
        missing = compute_missing_ranges(
            d(0), d(30), [(d(5), d(10)), (d(20), d(25))]
        )
        assert missing == [(d(0), d(5)), (d(10), d(20)), (d(25), d(30))]

    def test_query_outside_coverage(self):
        missing = compute_missing_ranges(d(40), d(50), [(d(0), d(10))])
        assert missing == [(d(40), d(50))]

    def test_single_point_covered(self):
        assert compute_missing_ranges(d(5), d(5), [(d(0), d(10))]) == []

    def test_single_point_not_covered(self):
        assert compute_missing_ranges(d(15), d(15), [(d(0), d(10))]) == [(d(15), d(15))]

    def test_touching_coverage_is_complete(self):
        # 两段相接的覆盖合起来完整覆盖查询区间
        assert compute_missing_ranges(d(0), d(20), [(d(0), d(10)), (d(10), d(20))]) == []

    def test_invalid_range(self):
        assert compute_missing_ranges(d(10), d(0), []) == []


class TestIsFullyCovered:
    def test_covered(self):
        assert is_fully_covered(d(0), d(10), [(d(0), d(10))]) is True

    def test_not_covered(self):
        assert is_fully_covered(d(0), d(10), [(d(0), d(9))]) is False

    def test_no_coverage(self):
        assert is_fully_covered(d(0), d(10), []) is False
