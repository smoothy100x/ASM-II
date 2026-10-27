import csv
import json
import math
import re
import struct
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path


EPSILON = 1e-10
LEVEL_PATTERN = re.compile(r"^(bid|ask)_(px|qty)_(\d+)$", re.IGNORECASE)
DATA_DIR = Path(__file__).resolve().parent / "Collector" / "data"
OUTPUT_DIR = Path(__file__).resolve().parent / "features_output"
THREADS = 8


def discover_levels(fieldnames):
    levels = {"bid": {}, "ask": {}}
    for name in fieldnames:
        match = LEVEL_PATTERN.match(name)
        if match:
            side, kind, level = match.groups()
            levels[side.lower()].setdefault(int(level), {})[kind.lower()] = name
    indices = sorted(set(levels["bid"]) & set(levels["ask"]))
    if not indices:
        raise ValueError("No paired bid and ask depth columns were found")
    for side in ("bid", "ask"):
        for index in indices:
            if set(levels[side].get(index, {})) != {"px", "qty"}:
                raise ValueError(f"Missing price or quantity column for {side} level {index}")
    return indices, levels


def add_columns(columns, names):
    columns.extend(names)


def feature_columns(indices):
    x = len(indices)
    columns = []
    for i in indices:
        columns.extend((f"spread_{i}", f"mid_price_{i}", f"relative_spread_{i}", f"cross_spread_{i}"))
    columns.extend(("ask_range", "bid_range"))
    for i in indices[:-1]:
        columns.extend((f"ask_gap_{i}_{i + 1}", f"bid_gap_{i}_{i + 1}"))
    columns.extend(("mean_ask_price", "mean_bid_price", "mean_ask_volume", "mean_bid_volume"))
    columns.extend(("std_ask_price", "std_bid_price", "std_ask_volume", "std_bid_volume"))
    columns.extend(("accumulated_price_difference", "accumulated_volume_difference", "normalized_volume_difference"))
    for name in ("ask_price", "bid_price", "ask_volume", "bid_volume"):
        columns.extend(f"delta_{name}_{i}" for i in indices)
    for name in ("ask_price", "bid_price", "ask_volume", "bid_volume"):
        columns.extend(f"velocity_{name}_{i}" for i in indices)
    columns.extend(("total_bid_depth", "total_ask_depth", "obi"))
    columns.extend(f"level_imbalance_{i}" for i in indices)
    columns.extend(("microprice", "weighted_bid_price", "weighted_ask_price"))
    columns.extend(f"bid_liquidity_concentration_{i}" for i in indices)
    columns.extend(f"ask_liquidity_concentration_{i}" for i in indices)
    columns.append("depth_ratio")
    columns.extend(("bid_contribution", "ask_contribution", "ofi"))
    for k in (5, 10, 20, 50, 100):
        if k <= x:
            columns.extend((f"total_bid_depth_{k}", f"total_ask_depth_{k}", f"obi_{k}", f"weighted_bid_price_{k}", f"weighted_ask_price_{k}", f"depth_ratio_{k}"))
    return columns


def mean(values):
    return sum(values) / len(values)


def population_std(values, avg):
    return math.sqrt(sum((value - avg) ** 2 for value in values) / len(values))


def calculate_features(row, indices, levels, previous):
    asks = [float(row[levels["ask"][i]["px"]]) for i in indices]
    ask_qty = [float(row[levels["ask"][i]["qty"]]) for i in indices]
    bids = [float(row[levels["bid"][i]["px"]]) for i in indices]
    bid_qty = [float(row[levels["bid"][i]["qty"]]) for i in indices]
    result = {}
    for pos, i in enumerate(indices):
        spread = asks[pos] - bids[pos]
        mid = (asks[pos] + bids[pos]) / 2
        result[f"spread_{i}"] = spread
        result[f"mid_price_{i}"] = mid
        result[f"relative_spread_{i}"] = spread / (mid + EPSILON)
        result[f"cross_spread_{i}"] = spread
    result["ask_range"] = asks[-1] - asks[0]
    result["bid_range"] = bids[0] - bids[-1]
    for pos, i in enumerate(indices[:-1]):
        result[f"ask_gap_{i}_{i + 1}"] = asks[pos + 1] - asks[pos]
        result[f"bid_gap_{i}_{i + 1}"] = bids[pos] - bids[pos + 1]
    mean_ask_price = mean(asks)
    mean_bid_price = mean(bids)
    mean_ask_volume = mean(ask_qty)
    mean_bid_volume = mean(bid_qty)
    result.update({
        "mean_ask_price": mean_ask_price,
        "mean_bid_price": mean_bid_price,
        "mean_ask_volume": mean_ask_volume,
        "mean_bid_volume": mean_bid_volume,
        "std_ask_price": population_std(asks, mean_ask_price),
        "std_bid_price": population_std(bids, mean_bid_price),
        "std_ask_volume": population_std(ask_qty, mean_ask_volume),
        "std_bid_volume": population_std(bid_qty, mean_bid_volume),
        "accumulated_price_difference": sum(a - b for a, b in zip(asks, bids)),
        "accumulated_volume_difference": sum(a - b for a, b in zip(ask_qty, bid_qty)),
        "normalized_volume_difference": sum(b - a for a, b in zip(ask_qty, bid_qty)) / (sum(b + a for a, b in zip(ask_qty, bid_qty)) + EPSILON),
    })
    ask_price_delta = []
    bid_price_delta = []
    ask_volume_delta = []
    bid_volume_delta = []
    for pos, i in enumerate(indices):
        if previous is None:
            values = ("", "", "", "")
        else:
            old_asks, old_ask_qty, old_bids, old_bid_qty, dt = previous
            values = (asks[pos] - old_asks[pos], bids[pos] - old_bids[pos], ask_qty[pos] - old_ask_qty[pos], bid_qty[pos] - old_bid_qty[pos])
            if dt is None or dt <= 0:
                values = ("", "", "", "")
        da, db, dva, dvb = values
        result[f"delta_ask_price_{i}"] = da
        result[f"delta_bid_price_{i}"] = db
        result[f"delta_ask_volume_{i}"] = dva
        result[f"delta_bid_volume_{i}"] = dvb
        ask_price_delta.append(da)
        bid_price_delta.append(db)
        ask_volume_delta.append(dva)
        bid_volume_delta.append(dvb)
    dt = previous[4] if previous is not None else None
    for name, deltas in (("ask_price", ask_price_delta), ("bid_price", bid_price_delta), ("ask_volume", ask_volume_delta), ("bid_volume", bid_volume_delta)):
        for i, delta in zip(indices, deltas):
            result[f"velocity_{name}_{i}"] = delta / dt if delta != "" and dt and dt > 0 else ""
    total_bid = sum(bid_qty)
    total_ask = sum(ask_qty)
    result["total_bid_depth"] = total_bid
    result["total_ask_depth"] = total_ask
    result["obi"] = (total_bid - total_ask) / (total_bid + total_ask + EPSILON)
    for i, bq, aq in zip(indices, bid_qty, ask_qty):
        result[f"level_imbalance_{i}"] = (bq - aq) / (bq + aq + EPSILON)
    result["microprice"] = (asks[0] * bid_qty[0] + bids[0] * ask_qty[0]) / (bid_qty[0] + ask_qty[0] + EPSILON)
    result["weighted_bid_price"] = sum(p * q for p, q in zip(bids, bid_qty)) / (total_bid + EPSILON)
    result["weighted_ask_price"] = sum(p * q for p, q in zip(asks, ask_qty)) / (total_ask + EPSILON)
    cumulative_bid = 0.0
    cumulative_ask = 0.0
    for i, bq, aq in zip(indices, bid_qty, ask_qty):
        cumulative_bid += bq
        cumulative_ask += aq
        result[f"bid_liquidity_concentration_{i}"] = cumulative_bid / (total_bid + EPSILON)
        result[f"ask_liquidity_concentration_{i}"] = cumulative_ask / (total_ask + EPSILON)
    result["depth_ratio"] = total_bid / (total_ask + EPSILON)
    bid_contribution = ""
    ask_contribution = ""
    if previous is not None and previous[4] is not None and previous[4] > 0:
        old_asks, old_ask_qty, old_bids, old_bid_qty, _ = previous
        bid_contribution = (bid_qty[0] if bids[0] >= old_bids[0] else 0.0) - (old_bid_qty[0] if bids[0] <= old_bids[0] else 0.0)
        ask_contribution = -(ask_qty[0] if asks[0] <= old_asks[0] else 0.0) + (old_ask_qty[0] if asks[0] >= old_asks[0] else 0.0)
    result["bid_contribution"] = bid_contribution
    result["ask_contribution"] = ask_contribution
    result["ofi"] = bid_contribution + ask_contribution if bid_contribution != "" else ""
    for k in (5, 10, 20, 50, 100):
        if k <= len(indices):
            bdepth = sum(bid_qty[:k])
            adepth = sum(ask_qty[:k])
            result[f"total_bid_depth_{k}"] = bdepth
            result[f"total_ask_depth_{k}"] = adepth
            result[f"obi_{k}"] = (bdepth - adepth) / (bdepth + adepth + EPSILON)
            result[f"weighted_bid_price_{k}"] = sum(p * q for p, q in zip(bids[:k], bid_qty[:k])) / (bdepth + EPSILON)
            result[f"weighted_ask_price_{k}"] = sum(p * q for p, q in zip(asks[:k], ask_qty[:k])) / (adepth + EPSILON)
            result[f"depth_ratio_{k}"] = bdepth / (adepth + EPSILON)
    return result, (asks, ask_qty, bids, bid_qty)


def make_raw_columns(depth):
    columns = ["symbol", "event_ts_ms", "recv_ts_ns", "update_id"]
    for side in ("bid", "ask"):
        for index in range(1, depth + 1):
            columns.extend((f"{side}_px_{index:03d}", f"{side}_qty_{index:03d}"))
    return columns


def process_file(source):
    run_dir = source.parent
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    symbol = source.name.removesuffix("_lob.bin").upper()
    symbol_metadata = metadata["symbols"][symbol]
    tick_size = Decimal(symbol_metadata["tick_size"])
    depth = int(symbol_metadata["stored_lob_depth_per_side"])
    record = struct.Struct("<qqq" + f"{depth}i{depth}f{depth}i{depth}f")
    if record.size != int(metadata["lob_record"]["record_size_bytes"]):
        raise ValueError(f"Record-size metadata mismatch for {source}")
    file_size = source.stat().st_size
    if file_size % record.size:
        raise ValueError(f"Truncated LOB record in {source}")
    indices = list(range(1, depth + 1))
    fieldnames = make_raw_columns(depth)
    level_map = {"bid": {}, "ask": {}}
    for side in ("bid", "ask"):
        for index in indices:
            level_map[side][index] = {"px": f"{side}_px_{index:03d}", "qty": f"{side}_qty_{index:03d}"}
    derived_columns = feature_columns(indices)
    destination_dir = OUTPUT_DIR / run_dir.relative_to(DATA_DIR)
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / f"{symbol}_lob_features.csv"
    with source.open("rb") as input_file, destination.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames + derived_columns, extrasaction="ignore")
        writer.writeheader()
        previous = None
        previous_event_ts = None
        rows = 0
        while True:
            data = input_file.read(record.size)
            if not data:
                break
            values = record.unpack(data)
            event_ts, recv_ts, update_id = values[:3]
            bid_ticks = values[3:3 + depth]
            bid_qty = values[3 + depth:3 + 2 * depth]
            ask_ticks = values[3 + 2 * depth:3 + 3 * depth]
            ask_qty = values[3 + 3 * depth:3 + 4 * depth]
            row = {"symbol": symbol, "event_ts_ms": event_ts, "recv_ts_ns": recv_ts, "update_id": update_id}
            for side, ticks, quantities in (("bid", bid_ticks, bid_qty), ("ask", ask_ticks, ask_qty)):
                for index, tick, quantity in zip(indices, ticks, quantities):
                    row[f"{side}_px_{index:03d}"] = format(Decimal(tick) * tick_size, "f")
                    row[f"{side}_qty_{index:03d}"] = quantity
            dt = (event_ts - previous_event_ts) / 1000 if previous_event_ts is not None else None
            state = None if previous is None else (*previous, dt)
            features, current = calculate_features(row, indices, level_map, state)
            row.update({key: str(value) if isinstance(value, float) and math.isfinite(value) else value for key, value in features.items()})
            writer.writerow(row)
            previous = current
            previous_event_ts = event_ts
            rows += 1
    return source.name, str(destination), rows, depth


def main():
    output_dir = OUTPUT_DIR.expanduser().resolve()
    threads = THREADS
    if not 1 <= threads <= 8:
        raise ValueError("THREADS must be between 1 and 8")
    if not DATA_DIR.is_dir():
        raise FileNotFoundError(f"LOB data directory does not exist: {DATA_DIR}")
    sources = sorted(DATA_DIR.rglob("*_lob.bin"))
    if not sources:
        raise FileNotFoundError(f"No *_lob.bin files found in {DATA_DIR}")
    output_dir.mkdir(parents=True, exist_ok=True)
    for source in sources:
        symbol = source.name.removesuffix("_lob.bin").upper()
        target = output_dir / source.parent.relative_to(DATA_DIR) / f"{symbol}_lob_features.csv"
        if target.exists():
            raise FileExistsError(f"Output already exists: {target}")
    with ThreadPoolExecutor(max_workers=min(threads, len(sources))) as executor:
        futures = [executor.submit(process_file, source) for source in sources]
        for future in as_completed(futures):
            name, destination, rows, depth = future.result()
            print(f"{name}: {rows} rows, {depth} levels per side -> {destination}")


if __name__ == "__main__":
    main()
