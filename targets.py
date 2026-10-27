import csv
import struct
from array import array
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "Collector" / "data"
OUTPUT_DIR = ROOT_DIR / "targets_output"
ALPHA = 0.002
HORIZONS = (
    1, 2, 3, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000,
    10000, 20000, 50000, 100000, 200000, 500000
)
THREADS = 8
LOB_DEPTH = 100
LOB_RECORD = struct.Struct("<qqq100i100f100i100f")
LABEL_COLUMNS = tuple(f"target_k_{horizon}" for horizon in HORIZONS)
INVALID_LABEL = -128


def load_mid_prices(source):
    if source.stat().st_size % LOB_RECORD.size:
        raise ValueError(f"{source} has a truncated LOB record")

    mids = array("d")
    with source.open("rb") as file:
        while record := file.read(LOB_RECORD.size):
            values = LOB_RECORD.unpack(record)
            best_bid_tick = values[3]
            best_ask_tick = values[3 + 2 * LOB_DEPTH]
            if best_bid_tick <= 0 or best_ask_tick <= 0:
                raise ValueError(f"{source} contains an invalid best price")
            mids.append((best_bid_tick + best_ask_tick) / 2.0)
    return np.frombuffer(mids, dtype=np.float64)


def build_labels(mid_prices):
    count = mid_prices.size
    prefix = np.empty(count + 1, dtype=np.float64)
    prefix[0] = 0.0
    np.cumsum(mid_prices, dtype=np.float64, out=prefix[1:])
    labels = np.full((len(HORIZONS), count), INVALID_LABEL, dtype=np.int8)

    for column, horizon in enumerate(HORIZONS):
        valid_count = count - horizon
        if valid_count <= 0:
            continue
        indices = np.arange(valid_count)
        future_mean = (
            prefix[indices + horizon + 1] - prefix[indices + 1]
        ) / horizon
        movement = (future_mean - mid_prices[:valid_count]) / mid_prices[:valid_count]
        labels[column, :valid_count] = np.where(
            movement >= ALPHA,
            1,
            np.where(movement <= -ALPHA, -1, 0)
        ).astype(np.int8)

    return labels


def process_file(source):
    mid_prices = load_mid_prices(source)
    labels = build_labels(mid_prices)
    destination_dir = OUTPUT_DIR / source.parent.relative_to(DATA_DIR)
    destination_dir.mkdir(parents=True, exist_ok=True)
    symbol = source.name.removesuffix("_lob.bin").upper()
    destination = destination_dir / f"{symbol}_targets.csv"

    if destination.exists():
        raise FileExistsError(f"Output already exists: {destination}")

    with destination.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(("row_index", *LABEL_COLUMNS))
        for row_index, row in enumerate(labels.T):
            writer.writerow((
                row_index,
                *("" if value == INVALID_LABEL else int(value) for value in row)
            ))

    return source.name, destination, mid_prices.size


def main():
    if not 1 <= THREADS <= 8:
        raise ValueError("THREADS must be between 1 and 8")
    if not DATA_DIR.is_dir():
        raise FileNotFoundError(f"LOB data directory does not exist: {DATA_DIR}")

    sources = sorted(DATA_DIR.rglob("*_lob.bin"))
    if not sources:
        raise FileNotFoundError(f"No *_lob.bin files found in {DATA_DIR}")

    with ThreadPoolExecutor(max_workers=min(THREADS, len(sources))) as executor:
        futures = [executor.submit(process_file, source) for source in sources]
        for future in as_completed(futures):
            name, destination, row_count = future.result()
            print(f"{name}: {row_count} rows -> {destination}")


if __name__ == "__main__":
    main()
