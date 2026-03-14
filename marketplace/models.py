"""
Database models for Orders and Trades.

Order lifecycle: pending → filled | partially_filled | cancelled
A single buy order can generate multiple trades (if it matches several sells).
"""

from sqlalchemy import Column, Integer, Float, String, DateTime, Enum as SAEnum, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
import enum

from .database import Base


class OrderStatus(str, enum.Enum):
    """Order lifecycle states."""
    PENDING          = "pending"
    FILLED          = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED        = "cancelled"


class OrderType(str, enum.Enum):
    """Buy or sell."""
    BUY  = "buy"
    SELL = "sell"


class Order(Base):
    __tablename__ = "orders"

    id                 = Column(Integer, primary_key=True, index=True, autoincrement=True)
    node_id            = Column(String(50), nullable=False, index=True)        # e.g. "Delhi_00", "Noida_14"
    order_type         = Column(SAEnum(OrderType), nullable=False)             # "buy" or "sell"
    quantity_kwh       = Column(Float, nullable=False)                          # Original quantity requested
    remaining_kwh      = Column(Float, nullable=False)                          # What's left to fill (starts = quantity_kwh)
    price_per_kwh      = Column(Float, nullable=False)                          # Limit price in ₹
    status             = Column(SAEnum(OrderStatus), default=OrderStatus.PENDING, nullable=False)
    created_at         = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at         = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                                 onupdate=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return (f"<Order id={self.id} {self.order_type.value} {self.remaining_kwh}/{self.quantity_kwh} kWh "
                f"@ ₹{self.price_per_kwh} [{self.status.value}]>")


class Trade(Base):
    __tablename__ = "trades"

    id                 = Column(Integer, primary_key=True, index=True, autoincrement=True)
    buyer_node_id      = Column(String(50), nullable=False, index=True)
    seller_node_id     = Column(String(50), nullable=False, index=True)
    buyer_order_id     = Column(Integer, ForeignKey("orders.id"), nullable=False)
    seller_order_id    = Column(Integer, ForeignKey("orders.id"), nullable=False)
    quantity_kwh       = Column(Float, nullable=False)                          # Actual traded quantity
    price_per_kwh      = Column(Float, nullable=False)                          # Clearing price (midpoint)
    total_cost         = Column(Float, nullable=False)                          # quantity × price
    executed_at        = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships for easy querying
    buyer_order  = relationship("Order", foreign_keys=[buyer_order_id])
    seller_order = relationship("Order", foreign_keys=[seller_order_id])

    def __repr__(self):
        return (f"<Trade {self.buyer_node_id}←{self.seller_node_id} "
                f"{self.quantity_kwh} kWh @ ₹{self.price_per_kwh}>")
