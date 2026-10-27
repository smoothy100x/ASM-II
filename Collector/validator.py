                      

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

from binary_formats import LOB_RECORD, TRADE_RECORD

MIN_TS_MS = 1_262_304_000_000              
MAX_TS_MS = 4_102_444_800_000              
MAX_RECV_NS = 4_102_444_800_000_000_000


def _count(counter: Counter, name: str, amount: int = 1) -> None:
    counter[name] += amount


def validate_lob(path: Path, tick_size: Decimal) -> dict:
    failures: Counter = Counter()
    records = 0
    previous_event = previous_recv = previous_update = None
    if not path.exists():
        return {"records": 0, "file_size_bytes": 0, "failures": {"file_missing": 1}}
    size = path.stat().st_size
    if size == 0:
        _count(failures, "empty_file")
    if size % LOB_RECORD.size:
        _count(failures, "file_size_not_divisible_by_record_size")
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(LOB_RECORD.size)
            if not chunk:
                break
            if len(chunk) != LOB_RECORD.size:
                _count(failures, "truncated_final_record")
                break
            values = LOB_RECORD.unpack(chunk)
            event_ts, recv_ns, update_id = values[:3]
            offset = 3
            bid_px = values[offset:offset + 100]
            offset += 100
            bid_qty = values[offset:offset + 100]
            offset += 100
            ask_px = values[offset:offset + 100]
            offset += 100
            ask_qty = values[offset:offset + 100]
            records += 1
            if not MIN_TS_MS <= event_ts <= MAX_TS_MS:
                _count(failures, "invalid_event_timestamp")
            if not 1_500_000_000_000_000_000 <= recv_ns <= MAX_RECV_NS:
                _count(failures, "invalid_receive_timestamp")
            if previous_event is not None and event_ts < previous_event:
                _count(failures, "event_timestamp_decreased")
            if previous_recv is not None and recv_ns < previous_recv:
                _count(failures, "receive_timestamp_decreased")
            if update_id <= 0 or (previous_update is not None and update_id <= previous_update):
                _count(failures, "invalid_or_nonincreasing_update_id")
            previous_event, previous_recv, previous_update = event_ts, recv_ns, update_id
            for qty in (*bid_qty, *ask_qty):
                if math.isnan(qty):
                    _count(failures, "nan_quantity")
                elif math.isinf(qty):
                    _count(failures, "infinite_quantity")
                elif qty < 0:
                    _count(failures, "negative_quantity")
            for ticks in (bid_px, ask_px):
                for tick in ticks:
                    if tick <= 0:
                        _count(failures, "nonpositive_populated_price")
                    else:
                        reconstructed = Decimal(tick) * tick_size
                        if reconstructed / tick_size != Decimal(tick):
                            _count(failures, "price_tick_reconstruction_error")
            if any(bid_px[i] <= bid_px[i + 1] for i in range(99)):
                _count(failures, "bid_prices_not_strictly_descending")
            if any(ask_px[i] >= ask_px[i + 1] for i in range(99)):
                _count(failures, "ask_prices_not_strictly_ascending")
            if bid_px[0] >= ask_px[0]:
                _count(failures, "crossed_or_locked_book")
            if len(bid_px) != 100 or len(ask_px) != 100:
                _count(failures, "not_100_levels_per_side")
    return {"records": records, "file_size_bytes": size,
            "record_size_bytes": LOB_RECORD.size,
            "failures": dict(sorted(failures.items()))}


def validate_trades(path: Path) -> dict:
    failures: Counter = Counter()
    records = 0
    if not path.exists():
        return {"records": 0, "file_size_bytes": 0, "failures": {"file_missing": 1}}
    size = path.stat().st_size
    if size == 0:
        _count(failures, "empty_file")
    if size % TRADE_RECORD.size:
        _count(failures, "file_size_not_divisible_by_record_size")
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(TRADE_RECORD.size)
            if not chunk:
                break
            if len(chunk) != TRADE_RECORD.size:
                _count(failures, "truncated_final_record")
                break
            event_ts, trade_ts, trade_id, price_tick, quantity, buyer_is_maker = TRADE_RECORD.unpack(chunk)
            records += 1
            if not MIN_TS_MS <= event_ts <= MAX_TS_MS:
                _count(failures, "invalid_event_timestamp")
            if not MIN_TS_MS <= trade_ts <= MAX_TS_MS:
                _count(failures, "invalid_trade_timestamp")
            if trade_id <= 0:
                _count(failures, "invalid_trade_id")
            if price_tick <= 0:
                _count(failures, "nonpositive_price_tick")
            if math.isnan(quantity):
                _count(failures, "nan_quantity")
            elif math.isinf(quantity):
                _count(failures, "infinite_quantity")
            elif quantity <= 0:
                _count(failures, "nonpositive_quantity")
            if buyer_is_maker not in (0, 1):
                _count(failures, "invalid_buyer_is_maker")
    return {"records": records, "file_size_bytes": size,
            "record_size_bytes": TRADE_RECORD.size,
            "failures": dict(sorted(failures.items()))}


def validate_run(run_dir: Path) -> dict:
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        raise ValueError(f"Missing metadata.json in {run_dir}")
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("byte_order") != "little-endian":
        raise ValueError("Unsupported or missing metadata byte order")
    if metadata.get("lob_record", {}).get("record_size_bytes") != LOB_RECORD.size:
        raise ValueError("metadata.json LOB record size does not match this validator")
    if metadata.get("trade_record", {}).get("record_size_bytes") != TRADE_RECORD.size:
        raise ValueError("metadata.json trade record size does not match this validator")
    results = {}
    for symbol, details in metadata["symbols"].items():
        tick_size = Decimal(details["tick_size"])
        results[symbol] = {
            "lob": validate_lob(run_dir / f"{symbol}_lob.bin", tick_size),
            "trades": validate_trades(run_dir / f"{symbol}_trades.bin"),
        }
    passed = all(not item[stream]["failures"]
                 for item in results.values() for stream in ("lob", "trades"))
    report = {
        "run_directory": str(run_dir.resolve()),
        "validation_time_utc": __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc).isoformat(),
        "lob_record_size_bytes": LOB_RECORD.size,
        "trade_record_size_bytes": TRADE_RECORD.size,
        "passed": passed,
        "symbols": results,
    }
    output = run_dir / "validation_report.json"
    temp = output.with_suffix(".json.tmp")
    temp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temp.replace(output)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    args = parser.parse_args()
    try:
        report = validate_run(args.run_directory.resolve(strict=True))
    except Exception as exc:
        print(f"VALIDATION ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
