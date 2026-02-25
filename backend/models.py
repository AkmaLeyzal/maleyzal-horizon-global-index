"""
Pydantic models for the MHGI API.
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ConstituentInfo(BaseModel):
    ticker: str
    name: str
    sector: str
    price: float = 0.0
    change_percent: float = 0.0
    market_cap: float = 0.0
    free_float_market_cap: float = 0.0
    free_float_factor: float = 0.0
    weight: float = 0.0
    shares_outstanding: float = 0.0
    volume: int = 0


class IndexValue(BaseModel):
    timestamp: datetime
    value: float
    change: float = 0.0
    change_percent: float = 0.0
    high: float = 0.0
    low: float = 0.0
    open: float = 0.0
    previous_close: float = 0.0
    total_market_cap: float = 0.0
    total_free_float_market_cap: float = 0.0


class IndexHistoryPoint(BaseModel):
    timestamp: datetime
    value: float
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None


class IndexSnapshot(BaseModel):
    index: IndexValue
    constituents: list[ConstituentInfo]


class IndexMeta(BaseModel):
    name: str
    full_name: str
    base_value: float
    base_date: str
    currency: str
    description: str
    num_constituents: int
