
from __future__ import annotations

from decimal import Decimal
from heapq import nlargest, nsmallest
import math

from config import MAX_BOOK_LEVELS_PER_SIDE, STORED_BOOK_DEPTH


def to_price_tick(price: str, tick_size: str) -> int:
    ratio = Decimal(price) / Decimal(tick_size)
    integral = ratio.to_integral_value()
    if ratio != integral:
        raise ValueError(f"Price {price} is not aligned to tick size {tick_size}")
    tick = int(integral)
    if not 0 < tick <= 2**31 - 1:
        raise ValueError(f"Price tick {tick} is outside signed int32")
    return tick


class LocalOrderBook:
    def __init__(self, symbol: str, tick_size: str):
        self.symbol = symbol
        self.tick_size = tick_size
        self.bids: dict[int, float] = {}
        self.asks: dict[int, float] = {}
        self.update_id: int | None = None

    @property
    def live(self) -> bool:
        return self.update_id is not None

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = self._parse_snapshot_side(snapshot["bids"])
        self.asks = self._parse_snapshot_side(snapshot["asks"])
        self.update_id = int(snapshot["lastUpdateId"])
        self._trim(self.bids, bids=True)
        self._trim(self.asks, bids=False)

    def _parse_snapshot_side(self, levels: list[list[str]]) -> dict[int, float]:
        side = {}
        for price, quantity in levels:
            tick = to_price_tick(price, self.tick_size)
            qty = float(quantity)
            if qty < 0 or not math.isfinite(qty):
                raise ValueError("snapshot contains an invalid quantity")
            if qty > 0:
                side[tick] = qty
        return side

    @staticmethod
    def _trim(side: dict[int, float], bids: bool) -> None:
        excess = len(side) - MAX_BOOK_LEVELS_PER_SIDE
        if excess > 0:
            worst = nsmallest(excess, side) if bids else nlargest(excess, side)
            for price in worst:
                del side[price]

    def apply(self, event: dict) -> str:
        if self.update_id is None:
            return "invalid"
        first, last = int(event["U"]), int(event["u"])
        if first <= 0 or last < first:
            return "invalid"
        if last <= self.update_id:
            return "old"
        if first > self.update_id + 1:
            return "gap"
        for entries, side in ((event.get("b", []), self.bids), (event.get("a", []), self.asks)):
            for price, quantity in entries:
                tick = to_price_tick(price, self.tick_size)
                qty = float(quantity)
                if qty < 0 or not math.isfinite(qty):
                    return "invalid"
                if qty == 0:
                    side.pop(tick, None)
                else:
                    side[tick] = qty
            self._trim(side, bids=(side is self.bids))
        self.update_id = last
        return "applied"

    def top(self) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
        if len(self.bids) < STORED_BOOK_DEPTH or len(self.asks) < STORED_BOOK_DEPTH:
            raise ValueError("incomplete top-100 book")
        bids = nlargest(STORED_BOOK_DEPTH, self.bids.items(), key=lambda x: x[0])
        asks = nsmallest(STORED_BOOK_DEPTH, self.asks.items(), key=lambda x: x[0])
        bids.sort(reverse=True)
        asks.sort()
        if bids[0][0] >= asks[0][0]:
            raise ValueError("crossed book")
        return bids, asks
