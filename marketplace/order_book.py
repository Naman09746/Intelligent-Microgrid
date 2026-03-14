"""
Double Auction Order Book — Matching Engine

Algorithm:
1. New order arrives
2. If BUY: scan sell orders (cheapest first). Match if sell_price <= buy_price.
3. If SELL: scan buy orders (highest first). Match if buy_price >= sell_price.
4. Trade price = midpoint of buyer and seller limit prices.
5. Handle partial fills: if order not fully consumed, keep remainder in book.
6. Repeat until no more matches possible.

This is the same algorithm used by EPEX SPOT (European Power Exchange)
and IEX (Indian Energy Exchange) for continuous intraday energy trading.
"""

from sqlalchemy.orm import Session
from sqlalchemy import and_
from .models import Order, Trade, OrderStatus, OrderType
from datetime import datetime, timezone
from typing import List, Optional


class OrderBook:
    """Continuous double auction matching engine for P2P energy trading."""

    def __init__(self, db: Session):
        self.db = db

    def add_order(self, node_id: str, order_type: str, quantity_kwh: float,
                  price_per_kwh: float) -> dict:
        """
        Add a new order to the book and attempt matching.

        Returns dict with:
          - "order": the created Order object
          - "trades": list of Trade objects (may be empty if no match)
        """
        # 1. Create the order in DB
        new_order = Order(
            node_id=node_id,
            order_type=OrderType(order_type),
            quantity_kwh=quantity_kwh,
            remaining_kwh=quantity_kwh,
            price_per_kwh=price_per_kwh,
            status=OrderStatus.PENDING,
        )
        self.db.add(new_order)
        self.db.flush()  # Get the auto-generated ID without committing

        # 2. Attempt matching
        trades = self._match(new_order)

        # 3. Commit everything (order + any trades) in one transaction
        self.db.commit()
        self.db.refresh(new_order)

        return {"order": new_order, "trades": trades}

    def _match(self, incoming: Order) -> List[Trade]:
        """
        Core matching loop.
        Matches incoming order against the opposite side of the book.
        """
        trades = []

        while incoming.remaining_kwh > 0.0001:  # Float tolerance
            # Find best counterparty
            counter = self._find_best_counter(incoming)
            if counter is None:
                break  # No match possible

            # Calculate trade parameters
            trade_qty = round(min(incoming.remaining_kwh, counter.remaining_kwh), 4)
            trade_price = round((incoming.price_per_kwh + counter.price_per_kwh) / 2, 2)

            # Determine buyer and seller
            if incoming.order_type == OrderType.BUY:
                buyer_order, seller_order = incoming, counter
            else:
                buyer_order, seller_order = counter, incoming

            # Create trade record
            trade = Trade(
                buyer_node_id=buyer_order.node_id,
                seller_node_id=seller_order.node_id,
                buyer_order_id=buyer_order.id,
                seller_order_id=seller_order.id,
                quantity_kwh=trade_qty,
                price_per_kwh=trade_price,
                total_cost=round(trade_qty * trade_price, 2),
            )
            self.db.add(trade)

            # Update remaining quantities
            incoming.remaining_kwh = round(incoming.remaining_kwh - trade_qty, 4)
            counter.remaining_kwh = round(counter.remaining_kwh - trade_qty, 4)

            # Update order statuses
            self._update_status(incoming)
            self._update_status(counter)

            # Explicitly flush changes to ensure subsequent queries see the updated state
            # This is crucial for the counterparty search logic to work correctly
            self.db.flush()

            trades.append(trade)

        return trades

    def _find_best_counter(self, incoming: Order) -> Optional[Order]:
        """
        Find the best matching order on the opposite side.

        For a BUY order  → find cheapest SELL where sell_price <= buy_price
        For a SELL order → find highest BUY where buy_price >= sell_price
        """
        if incoming.order_type == OrderType.BUY:
            # Best sell = cheapest first, then oldest (FIFO for same price)
            return (
                self.db.query(Order)
                .filter(and_(
                    Order.order_type == OrderType.SELL,
                    Order.status.in_([OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]),
                    Order.remaining_kwh > 0.0001,
                    Order.price_per_kwh <= incoming.price_per_kwh,
                    Order.node_id != incoming.node_id,  # Can't trade with yourself
                ))
                .order_by(Order.price_per_kwh.asc(), Order.created_at.asc())
                .first()
            )
        else:
            # Best buy = highest first, then oldest (FIFO for same price)
            return (
                self.db.query(Order)
                .filter(and_(
                    Order.order_type == OrderType.BUY,
                    Order.status.in_([OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]),
                    Order.remaining_kwh > 0.0001,
                    Order.price_per_kwh >= incoming.price_per_kwh,
                    Order.node_id != incoming.node_id,
                ))
                .order_by(Order.price_per_kwh.desc(), Order.created_at.asc())
                .first()
            )

    def _update_status(self, order: Order):
        """Update order status based on remaining quantity."""
        if order.remaining_kwh <= 0.0001:
            order.remaining_kwh = 0.0
            order.status = OrderStatus.FILLED
        elif order.remaining_kwh < order.quantity_kwh:
            order.status = OrderStatus.PARTIALLY_FILLED
        # else: still PENDING

    def cancel_order(self, order_id: int) -> Optional[Order]:
        """Cancel a pending order. Returns the order if found, None otherwise."""
        order = (
            self.db.query(Order)
            .filter(and_(
                Order.id == order_id,
                Order.status.in_([OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED])
            ))
            .first()
        )
        if order:
            order.status = OrderStatus.CANCELLED
            self.db.commit()
            self.db.refresh(order)
        return order

    def get_pending_orders(self) -> dict:
        """Get current order book state grouped by side."""
        buys = (
            self.db.query(Order)
            .filter(and_(
                Order.order_type == OrderType.BUY,
                Order.status.in_([OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]),
                Order.remaining_kwh > 0.0001,
            ))
            .order_by(Order.price_per_kwh.desc())
            .all()
        )
        sells = (
            self.db.query(Order)
            .filter(and_(
                Order.order_type == OrderType.SELL,
                Order.status.in_([OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]),
                Order.remaining_kwh > 0.0001,
            ))
            .order_by(Order.price_per_kwh.asc())
            .all()
        )
        return {"buys": buys, "sells": sells}
