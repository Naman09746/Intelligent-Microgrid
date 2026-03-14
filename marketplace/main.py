"""
P2P Energy Marketplace — FastAPI Application

Endpoints:
  POST   /orders              — Submit a buy or sell order
  GET    /orders              — Market snapshot (pending orders + spread)
  GET    /orders/{order_id}   — Check status of a specific order
  DELETE /orders/{order_id}   — Cancel a pending order
  GET    /trades              — All trade history (paginated)
  GET    /trades/node/{node}  — Trades for a specific node
  GET    /trades/recent       — Last N trades
  GET    /stats               — Aggregate market statistics
  GET    /health              — Health check

Run: uvicorn marketplace.main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from typing import List, Optional
from contextlib import asynccontextmanager

from .database import get_db, init_db
from .models import Order, Trade, OrderStatus, OrderType
from .schemas import (
    OrderCreate, OrderResponse, TradeResponse,
    MarketSnapshot, MarketStats,
    GRID_BUY_PRICE, GRID_SELL_PRICE
)
from .order_book import OrderBook


# ── App lifecycle ──
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    init_db()
    yield

app = FastAPI(
    title="⚡ Microgrid P2P Energy Marketplace",
    description="Double auction marketplace for 75 household nodes across 5 North Indian cities.",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS (allow dashboard and other frontends) ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════
#  ORDERS
# ══════════════════════════════════════════════════════════════

@app.post("/orders", response_model=dict, status_code=201,
          summary="Submit a buy or sell order")
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    """
    Submit a limit order to the marketplace.
    If a matching counterparty exists, a trade is executed immediately.
    """
    book = OrderBook(db)
    result = book.add_order(
        node_id=order.node_id,
        order_type=order.order_type,
        quantity_kwh=order.quantity_kwh,
        price_per_kwh=order.price_per_kwh,
    )

    # Use model_validate for both order and trades
    order_resp = OrderResponse.model_validate(result["order"])
    trade_resps = [TradeResponse.model_validate(t) for t in result["trades"]]

    return {
        "order": order_resp.model_dump(),
        "trades": [t.model_dump(mode="json") for t in trade_resps],
        "matched": len(trade_resps) > 0,
    }


@app.get("/orders", response_model=MarketSnapshot,
         summary="Get current order book snapshot")
def get_market_snapshot(db: Session = Depends(get_db)):
    """Returns all pending orders grouped by buy/sell, with spread info."""
    book = OrderBook(db)
    pending = book.get_pending_orders()

    buy_orders = [OrderResponse.model_validate(o) for o in pending["buys"]]
    sell_orders = [OrderResponse.model_validate(o) for o in pending["sells"]]

    total_buy = sum(o.remaining_kwh for o in buy_orders)
    total_sell = sum(o.remaining_kwh for o in sell_orders)
    best_buy = buy_orders[0].price_per_kwh if buy_orders else None
    best_sell = sell_orders[0].price_per_kwh if sell_orders else None
    spread = round(best_buy - best_sell, 2) if (best_buy and best_sell) else None

    return MarketSnapshot(
        pending_buy_orders=buy_orders,
        pending_sell_orders=sell_orders,
        total_buy_volume_kwh=round(total_buy, 4),
        total_sell_volume_kwh=round(total_sell, 4),
        best_buy_price=best_buy,
        best_sell_price=best_sell,
        spread=spread,
    )


@app.get("/orders/{order_id}", response_model=OrderResponse,
         summary="Get order by ID")
def get_order(order_id: int, db: Session = Depends(get_db)):
    """Check the status of a specific order."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return OrderResponse.model_validate(order)


@app.delete("/orders/{order_id}", response_model=OrderResponse,
            summary="Cancel a pending order")
def cancel_order(order_id: int, db: Session = Depends(get_db)):
    """Cancel a pending or partially filled order."""
    book = OrderBook(db)
    order = book.cancel_order(order_id)
    if not order:
        raise HTTPException(
            status_code=404,
            detail=f"Order {order_id} not found or already filled/cancelled"
        )
    return OrderResponse.model_validate(order)


# ══════════════════════════════════════════════════════════════
#  TRADES
# ══════════════════════════════════════════════════════════════

@app.get("/trades", response_model=List[TradeResponse],
         summary="Get trade history")
def get_trades(
    skip: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    db: Session = Depends(get_db)
):
    """All executed trades, newest first. Supports pagination."""
    trades = (
        db.query(Trade)
        .order_by(Trade.executed_at.desc())
        .offset(skip).limit(limit)
        .all()
    )
    return [TradeResponse.model_validate(t) for t in trades]


@app.get("/trades/node/{node_id}", response_model=List[TradeResponse],
         summary="Get trades for a specific node")
def get_node_trades(node_id: str, db: Session = Depends(get_db)):
    """All trades where the node was buyer or seller."""
    trades = (
        db.query(Trade)
        .filter((Trade.buyer_node_id == node_id) | (Trade.seller_node_id == node_id))
        .order_by(Trade.executed_at.desc())
        .all()
    )
    return [TradeResponse.model_validate(t) for t in trades]


@app.get("/trades/recent", response_model=List[TradeResponse],
         summary="Get most recent trades")
def get_recent_trades(
    n: int = Query(10, ge=1, le=100, description="Number of recent trades"),
    db: Session = Depends(get_db)
):
    """Last N trades for dashboard ticker."""
    trades = db.query(Trade).order_by(Trade.executed_at.desc()).limit(n).all()
    return [TradeResponse.model_validate(t) for t in trades]


# ══════════════════════════════════════════════════════════════
#  STATS & HEALTH
# ══════════════════════════════════════════════════════════════

@app.get("/stats", response_model=MarketStats,
         summary="Aggregate market statistics")
def get_stats(db: Session = Depends(get_db)):
    """Overall marketplace metrics for dashboard and monitoring."""
    total_trades = db.query(func.count(Trade.id)).scalar() or 0
    total_volume = db.query(func.sum(Trade.quantity_kwh)).scalar() or 0.0
    total_value = db.query(func.sum(Trade.total_cost)).scalar() or 0.0
    avg_price = db.query(func.avg(Trade.price_per_kwh)).scalar()

    pending_count = (
        db.query(func.count(Order.id))
        .filter(Order.status.in_([OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]))
        .scalar() or 0
    )
    active_nodes = (
        db.query(func.count(distinct(Order.node_id)))
        .filter(Order.status.in_([OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]))
        .scalar() or 0
    )

    return MarketStats(
        total_trades=total_trades,
        total_volume_kwh=round(total_volume, 4),
        total_value_inr=round(total_value, 2),
        average_price_per_kwh=round(avg_price, 2) if avg_price else None,
        total_pending_orders=pending_count,
        active_nodes=active_nodes,
    )


@app.get("/health", summary="Health check")
def health_check():
    """Simple health check for monitoring."""
    return {"status": "healthy", "service": "microgrid-marketplace", "version": "1.0.0"}
