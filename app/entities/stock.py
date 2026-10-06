"""业务模块说明。"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StockCandle(Base):
    """业务模块说明。"""
    __tablename__ = "stock_candles"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False, index=True)  # daily, 60min, 30min
    timestamp = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False, default=0.0)
    amount = Column(Float, nullable=True)  # 成交额（可选）
    
    # 数据质量标记
    is_limit_up = Column(Integer, default=0)    # 涨停标记
    is_limit_down = Column(Integer, default=0)  # 跌停标记
    is_suspended = Column(Integer, default=0)   # 停牌标记
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('stock_code', 'period', 'timestamp', name='uix_stock_period_time'),
        Index('ix_stock_code_period', 'stock_code', 'period'),
        Index('ix_stock_code_timestamp', 'stock_code', 'timestamp'),
    )
    
    def __repr__(self):
        return (
            f"<StockCandle(code={self.stock_code}, period={self.period}, "
            f"time={self.timestamp}, O={self.open}, H={self.high}, "
            f"L={self.low}, C={self.close})>"
        )
    
    def to_dict(self) -> dict:
        """业务模块说明。"""
        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "is_limit_up": bool(self.is_limit_up),
            "is_limit_down": bool(self.is_limit_down),
            "is_suspended": bool(self.is_suspended),
        }


class StockCandleCoverage(Base):
    """K线缓存覆盖区间。

    语义：区间 [start_time, end_time] 内的数据已经完整向上游请求过
    （包括上游明确返回空的情况），缓存中保存了上游当时提供的全部数据。
    只有请求区间被覆盖区间的并集完全包含时，缓存才算命中。
    """
    __tablename__ = "stock_candle_coverage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    source = Column(String(20), nullable=True)  # 提供数据的数据源

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('ix_coverage_code_period', 'stock_code', 'period'),
    )

    def __repr__(self):
        return (
            f"<StockCandleCoverage(code={self.stock_code}, period={self.period}, "
            f"range=[{self.start_time}, {self.end_time}], source={self.source})>"
        )
