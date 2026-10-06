"""区间覆盖计算。

缓存命中的判定不依赖"表里有没有行"，而依赖"请求区间是否已被
已抓取的覆盖区间完整包含"。本模块提供纯函数实现，便于确定性测试。

区间约定：闭区间 [start, end]。两个区间在端点相接（前一区间的 end
等于后一区间的 start）时视为连续，可以合并。
"""

from datetime import datetime
from typing import Iterable, List, Tuple

Interval = Tuple[datetime, datetime]


def merge_intervals(intervals: Iterable[Interval]) -> List[Interval]:
    """合并相互重叠或端点相接的区间，返回按起点排序的不相交区间列表。"""
    sorted_ivs = sorted((s, e) for s, e in intervals if s <= e)
    merged: List[Interval] = []
    for s, e in sorted_ivs:
        if merged and s <= merged[-1][1]:
            if e > merged[-1][1]:
                merged[-1] = (merged[-1][0], e)
        else:
            merged.append((s, e))
    return merged


def compute_missing_ranges(
    start: datetime,
    end: datetime,
    coverages: Iterable[Interval],
) -> List[Interval]:
    """计算 [start, end] 中未被 coverages 覆盖的部分。

    返回按起点排序的缺失闭区间列表；空列表表示完整覆盖（缓存命中）。
    """
    if start > end:
        return []

    merged = merge_intervals(coverages)

    # 单点区间的特判：该点被任一覆盖区间包含即视为覆盖
    if start == end:
        for iv_start, iv_end in merged:
            if iv_start <= start <= iv_end:
                return []
        return [(start, end)]

    missing: List[Interval] = []
    cursor = start  # 已确认覆盖到的位置（含该点）
    for iv_start, iv_end in merged:
        if iv_end < cursor:
            continue
        if iv_start > end:
            break
        if iv_start > cursor:
            missing.append((cursor, iv_start))
        if iv_end > cursor:
            cursor = iv_end
        if cursor >= end:
            break
    if cursor < end:
        missing.append((cursor, end))
    return missing


def is_fully_covered(
    start: datetime,
    end: datetime,
    coverages: Iterable[Interval],
) -> bool:
    """[start, end] 是否被 coverages 完整覆盖。"""
    return not compute_missing_ranges(start, end, coverages)
