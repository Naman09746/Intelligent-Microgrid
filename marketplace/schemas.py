"""
Pydantic schemas for API request/response validation.
Separates API contract from database internals.
"""

from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional, List


# ── Grid pricing constants (₹/kWh for Northern India urban residential) ──
GRID_BUY_PRICE  = 8.50   # What a home pays to buy from utility grid
GRID_SELL_PRICE  = 3.00   # What a home receives selling back to grid (net metering)
# P2P trades should happen between these bounds — both parties benefit


class OrderCreate(BaseModel):
    """POST /orders — what a node sends to place an order."""
    node_id: str = Field(..., min_length=1, max_length=50,
                         description="Home identifier, e.g. 'Delhi_00'",
                         json_schema_extra={"examples": ["Delhi_00", "Noida_14", "Chandigarh_07"]})
    order_type: str = Field(..., pattern="^(buy|sell)$",
                            description="Must be 'buy' or 'sell'")
    quantity_kwh: float = Field(..., gt=0, le=50.0,
                                description="Energy quantity in kWh. Max 50 kWh per order.")
    price_per_kwh: float = Field(..., gt=0, le=20.0,
                                  description="Limit price in ₹/kWh.")

    @field_validator('price_per_kwh')
    @classmethod
    def price_must_be_reasonable(cls, v):
        """Warn if price is outside grid bounds (still allow it)."""
        # We don't reject — the LLM agent might have strategic reasons
        # But log a warning in production
        return round(v, 2)

    @field_validator('quantity_kwh')
    @classmethod
    def quantity_precision(cls, v):
        """Round to 4 decimal places for consistency."""
        return round(v, 4)


class OrderResponse(BaseModel):
    """Response after posting an order."""
    id: int
    node_id: str
    order_type: str
    quantity_kwh: float
    remaining_kwh: float
    price_per_kwh: float
    status: str
    created_at: datetime

    class Config:
        from_attributes = True


class TradeResponse(BaseModel):
    """A single executed trade."""
    id: int
    buyer_node_id: str
    seller_node_id: str
    buyer_order_id: int
    seller_order_id: int
    quantity_kwh: float
    price_per_kwh: float
    total_cost: float
    executed_at: datetime

    class Config:
        from_attributes = True


class MarketSnapshot(BaseModel):
    """GET /orders — current order book state for LLM agent consumption."""
    pending_buy_orders: List[OrderResponse]
    pending_sell_orders: List[OrderResponse]
    total_buy_volume_kwh: float
    total_sell_volume_kwh: float
    best_buy_price: Optional[float] = None     # Highest pending buy
    best_sell_price: Optional[float] = None     # Lowest pending sell
    spread: Optional[float] = None              # best_buy - best_sell (if both exist)


class MarketStats(BaseModel):
    """GET /stats — aggregate market statistics."""
    total_trades: int
    total_volume_kwh: float
    total_value_inr: float
    average_price_per_kwh: Optional[float] = None
    total_pending_orders: int
    active_nodes: int                           # Unique node_ids with pending orders
    grid_buy_price: float = GRID_BUY_PRICE      # Reference for comparison
    grid_sell_price: float = GRID_SELL_PRICE
