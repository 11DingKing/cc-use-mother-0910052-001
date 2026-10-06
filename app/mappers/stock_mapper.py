"""业务模块说明。"""

from datetime import datetime
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.entities.stock import StockCandle, CacheCoverage
from app.chan.models import RawCandle


class StockMapper:
    """业务模块说明。"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, candle: StockCandle) -> StockCandle:
        """业务模块说明。"""
        self.session.add(candle)
        self.session.flush()
        return candle
    
    def create_batch(self, candles: List[StockCandle]) -> int:
        """业务模块说明。"""
        self.session.add_all(candles)
        self.session.flush()
        return len(candles)
    
    def get_by_id(self, candle_id: int) -> Optional[StockCandle]:
        """业务模块说明。"""
        return self.session.query(StockCandle).filter(
            StockCandle.id == candle_id
        ).first()
    
    def get_candles(
        self,
        stock_code: str,
        period: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[StockCandle]:
        """业务模块说明。"""
        query = self.session.query(StockCandle).filter(
            and_(
                StockCandle.stock_code == stock_code,
                StockCandle.period == period,
            )
        )
        
        if start_time:
            query = query.filter(StockCandle.timestamp >= start_time)
        if end_time:
            query = query.filter(StockCandle.timestamp <= end_time)
        
        query = query.order_by(StockCandle.timestamp.asc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_latest_candle(
        self,
        stock_code: str,
        period: str,
    ) -> Optional[StockCandle]:
        """业务模块说明。"""
        return self.session.query(StockCandle).filter(
            and_(
                StockCandle.stock_code == stock_code,
                StockCandle.period == period,
            )
        ).order_by(StockCandle.timestamp.desc()).first()
    
    def update(self, candle: StockCandle) -> StockCandle:
        """业务模块说明。"""
        self.session.merge(candle)
        self.session.flush()
        return candle
    
    def delete_by_stock_period(
        self,
        stock_code: str,
        period: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> int:
        """业务模块说明。"""
        query = self.session.query(StockCandle).filter(
            and_(
                StockCandle.stock_code == stock_code,
                StockCandle.period == period,
            )
        )
        
        if start_time:
            query = query.filter(StockCandle.timestamp >= start_time)
        if end_time:
            query = query.filter(StockCandle.timestamp <= end_time)
        
        count = query.delete(synchronize_session=False)
        self.session.flush()
        return count
    
    def upsert_candles(self, candles: List[StockCandle]) -> int:
        """幂等写入K线。

        SQLite 下使用 ON CONFLICT DO UPDATE，并发或重复写入不会产生
        唯一约束冲突；其他方言回退到逐行查询后插入/更新。
        """
        if not candles:
            return 0

        bind = self.session.get_bind()
        if bind is not None and bind.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            rows = [
                {
                    "stock_code": c.stock_code,
                    "period": c.period,
                    "timestamp": c.timestamp,
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.volume,
                    "amount": c.amount,
                    "is_limit_up": c.is_limit_up,
                    "is_limit_down": c.is_limit_down,
                    "is_suspended": c.is_suspended,
                }
                for c in candles
            ]
            stmt = sqlite_insert(StockCandle).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["stock_code", "period", "timestamp"],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                    "amount": stmt.excluded.amount,
                    "is_limit_up": stmt.excluded.is_limit_up,
                    "is_limit_down": stmt.excluded.is_limit_down,
                    "is_suspended": stmt.excluded.is_suspended,
                },
            )
            self.session.execute(stmt)
            self.session.flush()
            return len(rows)

        count = 0
        for candle in candles:
            existing = self.session.query(StockCandle).filter(
                and_(
                    StockCandle.stock_code == candle.stock_code,
                    StockCandle.period == candle.period,
                    StockCandle.timestamp == candle.timestamp,
                )
            ).first()

            if existing:
                existing.open = candle.open
                existing.high = candle.high
                existing.low = candle.low
                existing.close = candle.close
                existing.volume = candle.volume
                existing.amount = candle.amount
                existing.is_limit_up = candle.is_limit_up
                existing.is_limit_down = candle.is_limit_down
                existing.is_suspended = candle.is_suspended
            else:
                self.session.add(candle)
            count += 1

        self.session.flush()
        return count

    def get_coverages(
        self,
        stock_code: str,
        period: str,
    ) -> List[CacheCoverage]:
        """读取 (stock_code, period) 的全部覆盖区间，按起点升序。"""
        return self.session.query(CacheCoverage).filter(
            and_(
                CacheCoverage.stock_code == stock_code,
                CacheCoverage.period == period,
            )
        ).order_by(CacheCoverage.range_start.asc()).all()

    def add_coverage(
        self,
        stock_code: str,
        period: str,
        range_start: datetime,
        range_end: datetime,
        source: Optional[str] = None,
        candle_count: int = 0,
    ) -> CacheCoverage:
        """记录一段已抓取区间，并与重叠或相接的既有区间合并。

        必须在调用方的事务内使用（与K线写入同事务提交），保证
        "K线"与"覆盖事实"要么同时生效、要么同时回滚。
        """
        overlapping = self.session.query(CacheCoverage).filter(
            and_(
                CacheCoverage.stock_code == stock_code,
                CacheCoverage.period == period,
                CacheCoverage.range_start <= range_end,
                CacheCoverage.range_end >= range_start,
            )
        ).all()

        merged_start = min([range_start] + [c.range_start for c in overlapping])
        merged_end = max([range_end] + [c.range_end for c in overlapping])

        for old in overlapping:
            self.session.delete(old)

        coverage = CacheCoverage(
            stock_code=stock_code,
            period=period,
            range_start=merged_start,
            range_end=merged_end,
            source=source,
            candle_count=candle_count,
        )
        self.session.add(coverage)
        self.session.flush()
        return coverage
    
    def to_raw_candles(self, candles: List[StockCandle]) -> List[RawCandle]:
        """业务模块说明。"""
        return [
            RawCandle(
                timestamp=c.timestamp,
                open=c.open,
                high=c.high,
                low=c.low,
                close=c.close,
                volume=c.volume,
            )
            for c in candles
        ]
    
    def from_raw_candles(
        self,
        candles: List[RawCandle],
        stock_code: str,
        period: str,
    ) -> List[StockCandle]:
        """业务模块说明。"""
        return [
            StockCandle(
                stock_code=stock_code,
                period=period,
                timestamp=c.timestamp,
                open=c.open,
                high=c.high,
                low=c.low,
                close=c.close,
                volume=c.volume,
            )
            for c in candles
        ]
    
    def count_candles(self, stock_code: str, period: str) -> int:
        """业务模块说明。"""
        return self.session.query(StockCandle).filter(
            and_(
                StockCandle.stock_code == stock_code,
                StockCandle.period == period,
            )
        ).count()
