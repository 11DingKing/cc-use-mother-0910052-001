"""业务模块说明。"""

from datetime import datetime
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.entities.stock import StockCandle, StockCandleCoverage
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
        """幂等写入K线，并发安全。

        - 批次内先按 (stock_code, period, timestamp) 去重，避免同事务唯一键冲突；
        - SQLite 使用 INSERT ... ON CONFLICT DO UPDATE，避免"先查后插"在并发下
          触发唯一约束冲突；
        - 其他数据库回退为逐行查询后写入（由唯一约束兜底，冲突时整事务回滚，
          不会留下部分写入）。
        """
        candles = self._dedupe_candles(candles)
        if not candles:
            return 0

        dialect_name = self.session.bind.dialect.name if self.session.bind else ""
        if dialect_name == "sqlite":
            return self._upsert_candles_sqlite(candles)
        return self._upsert_candles_generic(candles)

    @staticmethod
    def _dedupe_candles(candles: List[StockCandle]) -> List[StockCandle]:
        """业务模块说明。"""
        seen = {}
        for candle in candles:
            key = (candle.stock_code, candle.period, candle.timestamp)
            seen[key] = candle  # 同键保留最后一条
        return list(seen.values())

    def _upsert_candles_sqlite(self, candles: List[StockCandle]) -> int:
        """业务模块说明。"""
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
                "is_limit_up": c.is_limit_up or 0,
                "is_limit_down": c.is_limit_down or 0,
                "is_suspended": c.is_suspended or 0,
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

    def _upsert_candles_generic(self, candles: List[StockCandle]) -> int:
        """业务模块说明。"""
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

    # ------------------------------------------------------------------
    # 覆盖区间（缓存完整性记录）
    # ------------------------------------------------------------------

    def get_coverage(
        self,
        stock_code: str,
        period: str,
    ) -> List[StockCandleCoverage]:
        """按开始时间升序返回该股票+周期的全部覆盖区间。"""
        return self.session.query(StockCandleCoverage).filter(
            and_(
                StockCandleCoverage.stock_code == stock_code,
                StockCandleCoverage.period == period,
            )
        ).order_by(StockCandleCoverage.start_time.asc()).all()

    def add_coverage(
        self,
        stock_code: str,
        period: str,
        start_time: datetime,
        end_time: datetime,
        source: Optional[str] = None,
    ) -> StockCandleCoverage:
        """记录一段覆盖区间，并与相邻/重叠的既有区间合并。

        必须在写入对应K线的同一事务内调用，保证"有覆盖必有数据、
        无覆盖必重新拉取"，进程崩溃也不会留下只有覆盖没有数据的污染。
        """
        overlapping = self.session.query(StockCandleCoverage).filter(
            and_(
                StockCandleCoverage.stock_code == stock_code,
                StockCandleCoverage.period == period,
                StockCandleCoverage.start_time <= end_time,
                StockCandleCoverage.end_time >= start_time,
            )
        ).all()

        merged_start = min([start_time] + [c.start_time for c in overlapping])
        merged_end = max([end_time] + [c.end_time for c in overlapping])

        for old in overlapping:
            self.session.delete(old)

        coverage = StockCandleCoverage(
            stock_code=stock_code,
            period=period,
            start_time=merged_start,
            end_time=merged_end,
            source=source,
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
