
from __future__ import annotations

import struct

from config import STORED_BOOK_DEPTH

LOB_RECORD = struct.Struct(
    "<qqq" + f"{STORED_BOOK_DEPTH}i{STORED_BOOK_DEPTH}f"
    + f"{STORED_BOOK_DEPTH}i{STORED_BOOK_DEPTH}f"
)
TRADE_RECORD = struct.Struct("<qqqifB")

LOB_SCHEMA = {
    "event_ts_ms": "int64",
    "recv_ts_ns": "int64",
    "update_id": "int64",
    "bid_px": f"int32[{STORED_BOOK_DEPTH}]",
    "bid_qty": f"float32[{STORED_BOOK_DEPTH}]",
    "ask_px": f"int32[{STORED_BOOK_DEPTH}]",
    "ask_qty": f"float32[{STORED_BOOK_DEPTH}]",
    "record_size_bytes": LOB_RECORD.size,
}
TRADE_SCHEMA = {
    "event_ts_ms": "int64",
    "trade_ts_ms": "int64",
    "trade_id": "int64",
    "price_tick": "int32",
    "quantity": "float32",
    "buyer_is_maker": "uint8",
    "record_size_bytes": TRADE_RECORD.size,
}


def encode_lob(event_ts_ms: int, recv_ts_ns: int, update_id: int,
               bids: list[tuple[int, float]], asks: list[tuple[int, float]]) -> bytes:
    if len(bids) != STORED_BOOK_DEPTH or len(asks) != STORED_BOOK_DEPTH:
        raise ValueError("LOB record requires exactly 100 bid and ask levels")
    bid_px = [p for p, _ in bids]
    bid_qty = [q for _, q in bids]
    ask_px = [p for p, _ in asks]
    ask_qty = [q for _, q in asks]
    return LOB_RECORD.pack(event_ts_ms, recv_ts_ns, update_id,
                           *bid_px, *bid_qty, *ask_px, *ask_qty)


def encode_trade(event_ts_ms: int, trade_ts_ms: int, trade_id: int,
                 price_tick: int, quantity: float, buyer_is_maker: bool) -> bytes:
    return TRADE_RECORD.pack(event_ts_ms, trade_ts_ms, trade_id, price_tick,
                             quantity, int(buyer_is_maker))
