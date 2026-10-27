                      

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from binary_formats import LOB_RECORD, TRADE_RECORD


def price(tick: int, tick_size: Decimal) -> str:
    return format(Decimal(tick) * tick_size, "f")


def inspect_lob(path: Path, tick_size: Decimal, limit: int) -> list[dict]:
    records = []
    with path.open("rb") as handle:
        for _ in range(limit):
            data = handle.read(LOB_RECORD.size)
            if not data:
                break
            if len(data) != LOB_RECORD.size:
                raise ValueError(f"Truncated record in {path}")
            value = LOB_RECORD.unpack(data)
            event_ts, recv_ns, update_id = value[:3]
            bid_ticks = value[3:103]
            bid_qty = value[103:203]
            ask_ticks = value[203:303]
            ask_qty = value[303:403]
            bids = [(price(t, tick_size), q) for t, q in zip(bid_ticks[:5], bid_qty[:5])]
            asks = [(price(t, tick_size), q) for t, q in zip(ask_ticks[:5], ask_qty[:5])]
            records.append({
                "event_ts_ms": event_ts,
                "recv_ts_ns": recv_ns,
                "update_id": update_id,
                "best_bid": bids[0][0],
                "best_ask": asks[0][0],
                "spread_for_inspection_only": format(Decimal(asks[0][0]) - Decimal(bids[0][0]), "f"),
                "first_5_bids": bids,
                "first_5_asks": asks,
            })
    return records


def inspect_trades(path: Path, tick_size: Decimal, limit: int) -> list[dict]:
    records = []
    with path.open("rb") as handle:
        for _ in range(limit):
            data = handle.read(TRADE_RECORD.size)
            if not data:
                break
            if len(data) != TRADE_RECORD.size:
                raise ValueError(f"Truncated record in {path}")
            event_ts, trade_ts, trade_id, tick, quantity, buyer_is_maker = TRADE_RECORD.unpack(data)
            records.append({
                "event_ts_ms": event_ts,
                "trade_ts_ms": trade_ts,
                "trade_id": trade_id,
                "price": price(tick, tick_size),
                "quantity": quantity,
                "buyer_is_maker": buyer_is_maker,
            })
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("symbol")
    parser.add_argument("--records", type=int, default=5)
    args = parser.parse_args()
    run_dir = args.run_directory.resolve(strict=True)
    symbol = args.symbol.upper()
    metadata = json.loads((run_dir / "metadata.json").read_text())
    try:
        tick_size = Decimal(metadata["symbols"][symbol]["tick_size"])
    except KeyError:
        parser.error(f"{symbol} is not present in run metadata")
    print(f"LOB samples: {symbol}")
    print(json.dumps(inspect_lob(run_dir / f"{symbol}_lob.bin", tick_size, args.records), indent=2))
    print(f"Trade samples: {symbol}")
    print(json.dumps(inspect_trades(run_dir / f"{symbol}_trades.bin", tick_size, args.records), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
