                      

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from binance_api import (BinanceAPIError, get_depth_snapshot, get_symbol_info)
from binary_formats import LOB_RECORD, TRADE_RECORD, LOB_SCHEMA, TRADE_SCHEMA
from binary_writer import BufferedBinaryWriter
from config import (COLLECTOR_VERSION, DEFAULT_PILOT_DURATION_SECONDS,
                    DEFAULT_SYMBOLS, LOB_BUFFER_RECORDS,
                    MINIMUM_RUNTIME_FREE_BYTES, MINIMUM_START_FREE_BYTES,
                    RECONNECT_DELAYS_SECONDS, REST_RETRY_DELAYS_SECONDS, REST_SNAPSHOT_DEPTH,
                    SPOT_STREAM_URL, STATUS_INTERVAL_SECONDS,
                    STORED_BOOK_DEPTH, SYNC_BUFFER_EVENTS,
                    TRADE_BUFFER_RECORDS)
from orderbook import LocalOrderBook, to_price_tick
from stats import process_cpu_ticks, process_rss_bytes, system_available_bytes


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str:
    value = value or utc_now()
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def write_json(path: Path, data: dict) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    temp.replace(path)


def mount_for_path(path: Path) -> tuple[str, str]:
    candidates: list[tuple[int, str, str]] = []
    try:
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            left, right = line.split(" - ", 1)
            fields, fs_fields = left.split(), right.split()
            mount = fields[4].replace("\\040", " ").replace("\\011", "\t")
            source = fs_fields[1]
            mount_path = Path(mount)
            if path == mount_path or mount_path in path.parents:
                candidates.append((len(mount), mount, source))
    except (OSError, ValueError, IndexError):
        pass
    if not candidates:
        return "unknown", "unknown"
    _, mount, source = max(candidates)
    return mount, source


def disk_preflight(output_arg: str) -> tuple[Path, int, int, str, str]:
    raw = Path(output_arg).expanduser()
    try:
        path = raw.resolve(strict=True)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Output directory does not exist: {raw}") from exc
    if not path.is_dir():
        raise RuntimeError(f"Output path is not a directory: {path}")
    if not os.access(path, os.W_OK | os.X_OK):
        raise RuntimeError(f"Output directory is not writable: {path}")
    usage = __import__("shutil").disk_usage(path)
    mount, source = mount_for_path(path)
    device = path.stat().st_dev
    print(f"Output directory: {path}", flush=True)
    print(f"Filesystem source: {source}; mount point: {mount}; device id: {device}", flush=True)
    print(f"Free disk space: {usage.free / 1024**3:.2f} GiB", flush=True)
    if usage.free < MINIMUM_START_FREE_BYTES:
        raise RuntimeError("Refusing to start: less than 10 GiB free at output path")
    return path, usage.free, device, mount, source


def check_clock() -> dict:
    try:
        result = subprocess.run(["timedatectl", "status"], check=False,
                                text=True, capture_output=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"Could not check server clock with timedatectl: {exc}") from exc
    details = (result.stdout + result.stderr).strip()
    print("VPS clock status:", flush=True)
    print(details, flush=True)
    if result.returncode != 0:
        raise RuntimeError("timedatectl status failed; refusing to collect")
    normalized = details.lower()
    if "system clock synchronized: yes" not in normalized:
        raise RuntimeError("Server clock is not synchronized; refusing to collect")
    return {"command": "timedatectl status", "output": details}


def unique_run_dir(output_dir: Path) -> Path:
    stem = "pilot_" + utc_now().strftime("%Y%m%d_%H%M%S")
    candidate = output_dir / stem
    suffix = 1
    while candidate.exists():
        candidate = output_dir / f"{stem}_{suffix:02d}"
        suffix += 1
    candidate.mkdir(mode=0o750)
    return candidate


def git_commit() -> str | None:
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], check=True,
                                text=True, capture_output=True, timeout=2)
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def make_logger(run_dir: Path) -> logging.Logger:
    logger = logging.getLogger("Collector")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s",
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    formatter.converter = time.gmtime
    file_handler = logging.FileHandler(run_dir / "collection.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


class SymbolState:
    def __init__(self, symbol: str, info: dict, run_dir: Path, logger: logging.Logger):
        self.symbol = symbol
        self.info = info
        self.logger = logger
        self.book = LocalOrderBook(symbol, info["tick_size"])
        self.depth_buffer: deque[tuple[dict, int]] = deque()
        self.snapshot_task: asyncio.Task | None = None
        self.syncing = True
        self.resync_count = 0
        self.gap_count = 0
        self.reconnect_rebuild_count = 0
        self.crossed_book_count = 0
        self.incomplete_book_count = 0
        self.old_depth_count = 0
        self.buffer_overflow_count = 0
        self.exception_count = 0
        self.snapshot_count = 0
        self.snapshot_requests = 0
        self.last_depth_mono: float | None = None
        self.last_depth_recv_ns: int | None = None
        self.lob_writer = BufferedBinaryWriter(run_dir / f"{symbol}_lob.bin",
                                               LOB_RECORD.size, LOB_BUFFER_RECORDS)
        self.trade_writer = BufferedBinaryWriter(run_dir / f"{symbol}_trades.bin",
                                                 TRADE_RECORD.size, TRADE_BUFFER_RECORDS)

    def start_snapshot(self, reason: str, resync: bool = True) -> None:
        if resync:
            self.resync_count += 1
        self.syncing = True
        self.book = LocalOrderBook(self.symbol, self.info["tick_size"])
        self.depth_buffer.clear()
        self._launch_snapshot(reason)

    def retry_snapshot_preserving_buffer(self, reason: str) -> None:
        self.syncing = True
        self.book = LocalOrderBook(self.symbol, self.info["tick_size"])
        self._launch_snapshot(reason)

    def _launch_snapshot(self, reason: str) -> None:
        if self.snapshot_task is not None and not self.snapshot_task.done():
            self.snapshot_task.cancel()
        self.logger.info("REST snapshot request for %s (%s)", self.symbol, reason)
        self.snapshot_task = asyncio.create_task(self._fetch_snapshot())

    async def _fetch_snapshot(self) -> dict:
        for attempt in range(1, len(REST_RETRY_DELAYS_SECONDS) + 2):
            try:
                self.snapshot_requests += 1
                return await asyncio.to_thread(get_depth_snapshot, self.symbol)
            except BinanceAPIError as exc:
                self.logger.warning("REST snapshot failed for %s (attempt %d): %s",
                                    self.symbol, attempt, exc)
                if attempt > len(REST_RETRY_DELAYS_SECONDS):
                    raise
                await asyncio.sleep(REST_RETRY_DELAYS_SECONDS[attempt - 1])
        raise AssertionError("unreachable")

    def reset_for_reconnect(self) -> None:
        self.resync_count += 1
        self.reconnect_rebuild_count += 1
        self.syncing = True
        self.book = LocalOrderBook(self.symbol, self.info["tick_size"])
        self.depth_buffer.clear()
        if self.snapshot_task is not None and not self.snapshot_task.done():
            self.snapshot_task.cancel()
        self.snapshot_task = None

    def buffer_depth(self, event: dict, recv_ns: int, mono: float) -> None:
        self.last_depth_recv_ns = recv_ns
        self.last_depth_mono = mono
        if len(self.depth_buffer) >= SYNC_BUFFER_EVENTS:
            self.buffer_overflow_count += 1
            self.logger.error("Synchronization buffer limit reached for %s; restarting buffer", self.symbol)
            self.depth_buffer.clear()
            self.book = LocalOrderBook(self.symbol, self.info["tick_size"])
            self.syncing = True
                                                                                  
                                                                                   
        self.depth_buffer.append((event, recv_ns))


class Collector:
    def __init__(self, args: argparse.Namespace, output_dir: Path,
                 start_free: int, start_device: int, mount_point: str,
                 clock: dict, infos: dict[str, dict]):
        self.args = args
        self.output_dir = output_dir
        self.start_free = start_free
        self.start_device = start_device
        self.mount_point = mount_point
        self.clock = clock
        self.infos = infos
        self.run_dir = unique_run_dir(output_dir)
        if self.run_dir.stat().st_dev != start_device:
            self.run_dir.rmdir()
            raise RuntimeError("Output filesystem changed while preparing the run")
        self.logger = make_logger(self.run_dir)
        self.states = {symbol: SymbolState(symbol, infos[symbol], self.run_dir, self.logger)
                       for symbol in args.symbols}
        self.start_wall = utc_now()
        self.start_mono = time.monotonic()
        self.deadline = self.start_mono + args.duration
        self.stop_event = asyncio.Event()
        self.stop_reason = "duration reached"
        self.reconnect_count = 0
        self.peak_rss = process_rss_bytes()
        self.cpu_start = process_cpu_ticks()
        self.cpu_previous_ticks = self.cpu_start
        self.last_status = self.start_mono
        self.summary_file = (self.run_dir / "summary.json").open("x", encoding="utf-8")
        self.metadata = self._metadata()
        write_json(self.run_dir / "metadata.json", self.metadata)
        self.logger.info("Collector starting; output=%s", self.run_dir)
        self.logger.info("Required Spot symbols=%s", ",".join(args.symbols))

    def _metadata(self) -> dict:
        symbols = {}
        for symbol, info in self.infos.items():
            symbols[symbol] = {
                "symbol": symbol,
                "base_asset": info["base_asset"],
                "quote_asset": info["quote_asset"],
                "tick_size": info["tick_size"],
                "step_size": info["step_size"],
                "current_price_at_preflight": info["current_price"],
                "current_price_tick_at_preflight": info["current_price_tick"],
                "checked_max_price": info["max_price"],
                "checked_max_price_tick": info["max_price_tick_checked"],
                "rest_snapshot_depth": REST_SNAPSHOT_DEPTH,
                "stored_lob_depth_per_side": STORED_BOOK_DEPTH,
                "websocket_depth_frequency": "100ms",
            }
        return {
            "project": "Binance Spot Limit Order Book Data Collection",
            "exchange": "Binance",
            "market_type": "spot",
            "collection_mode": "pilot" if self.args.duration == DEFAULT_PILOT_DURATION_SECONDS else "custom",
            "requested_duration_seconds": self.args.duration,
            "start_time_utc": iso_utc(self.start_wall),
            "server_hostname": socket.gethostname(),
            "python_version": sys.version,
            "collector_version": COLLECTOR_VERSION,
            "git_commit": git_commit(),
            "output_directory": str(self.run_dir),
            "filesystem_mount_point": self.mount_point,
            "clock_check": self.clock,
            "byte_order": "little-endian",
            "lob_record": LOB_SCHEMA,
            "trade_record": TRADE_SCHEMA,
            "symbols": symbols,
        }

    async def _start_snapshots(self, reason: str) -> None:
        for state in self.states.values():
            state.start_snapshot(reason, resync=False)

    async def _pump_snapshots(self) -> None:
        for state in self.states.values():
            task = state.snapshot_task
            if task is None or not task.done():
                continue
            state.snapshot_task = None
            try:
                snapshot = task.result()
                state.snapshot_count += 1
                state.book.load_snapshot(snapshot)
                try:
                    state.book.top()
                except ValueError as exc:
                    if "crossed" in str(exc):
                        state.crossed_book_count += 1
                    else:
                        state.incomplete_book_count += 1
                    state.start_snapshot("invalid snapshot book", resync=True)
                    continue
                last_id = state.book.update_id
                assert last_id is not None
                buffered = list(state.depth_buffer)
                                                                               
                                                       
                if buffered and last_id < int(buffered[0][0]["U"]):
                    self.logger.warning("Snapshot too old for %s; retrying alignment", state.symbol)
                    state.retry_snapshot_preserving_buffer("snapshot behind first buffered event")
                    continue
                                                                                
                                                                               
                events = []
                for event, recv_ns in buffered:
                    if int(event["u"]) <= last_id:
                        state.old_depth_count += 1
                    else:
                        events.append((event, recv_ns))
                failed = False
                for event, recv_ns in events:
                    result = state.book.apply(event)
                    if result == "old":
                        state.old_depth_count += 1
                    elif result == "gap":
                        state.gap_count += 1
                        failed = True
                        break
                    elif result != "applied":
                        failed = True
                        break
                    try:
                        bids, asks = state.book.top()
                    except ValueError as exc:
                        if "crossed" in str(exc):
                            state.crossed_book_count += 1
                        else:
                            state.incomplete_book_count += 1
                        failed = True
                        break
                    state.lob_writer.append(
                        LOB_RECORD.pack(int(event["E"]), recv_ns, int(event["u"]),
                                        *[p for p, _ in bids], *[q for _, q in bids],
                                        *[p for p, _ in asks], *[q for _, q in asks]),
                        int(event["E"]))
                if failed:
                    state.start_snapshot("buffer alignment failed", resync=True)
                    continue
                state.depth_buffer.clear()
                state.syncing = False
                self.logger.info("Symbol synchronized: %s update_id=%d buffered=%d",
                                 state.symbol, state.book.update_id, len(events))
            except Exception as exc:
                state.exception_count += 1
                self.logger.exception("Snapshot synchronization failed for %s: %s", state.symbol, exc)
                state.start_snapshot("snapshot or book validation exception", resync=True)

    def _process_trade(self, state: SymbolState, event: dict) -> None:
        event_ts = int(event["E"])
        trade_ts = int(event["T"])
        trade_id = int(event["t"])
        price_tick = to_price_tick(str(event["p"]), state.info["tick_size"])
        quantity = float(event["q"])
        if quantity <= 0 or not __import__("math").isfinite(quantity):
            raise ValueError("trade quantity must be finite and positive")
        state.trade_writer.append(
            TRADE_RECORD.pack(event_ts, trade_ts, trade_id, price_tick, quantity,
                              int(bool(event["m"]))), event_ts)

    def _process_depth(self, state: SymbolState, event: dict, recv_ns: int,
                       mono: float) -> None:
        state.last_depth_recv_ns = recv_ns
        state.last_depth_mono = mono
        if state.syncing or not state.book.live:
            state.buffer_depth(event, recv_ns, mono)
            return
        result = state.book.apply(event)
        if result == "old":
            state.old_depth_count += 1
            return
        if result == "gap":
            state.gap_count += 1
            self.logger.error("Update-ID gap for %s: U=%s local=%s", state.symbol,
                              event.get("U"), state.book.update_id)
            state.start_snapshot("update-ID gap", resync=True)
            state.buffer_depth(event, recv_ns, mono)
            return
        if result != "applied":
            state.exception_count += 1
            state.start_snapshot("invalid depth event", resync=True)
            state.buffer_depth(event, recv_ns, mono)
            return
        try:
            bids, asks = state.book.top()
        except ValueError as exc:
            if "crossed" in str(exc):
                state.crossed_book_count += 1
                self.logger.error("Crossed book detected for %s", state.symbol)
            else:
                state.incomplete_book_count += 1
                self.logger.error("Incomplete top-100 book for %s", state.symbol)
            state.start_snapshot(str(exc), resync=True)
            return
        record = LOB_RECORD.pack(int(event["E"]), recv_ns, int(event["u"]),
                                 *[p for p, _ in bids], *[q for _, q in bids],
                                 *[p for p, _ in asks], *[q for _, q in asks])
        state.lob_writer.append(record, int(event["E"]))

    def _dispatch(self, wrapper: dict, recv_ns: int, mono: float) -> None:
        event = wrapper.get("data", wrapper)
        symbol = str(event.get("s", "")).upper()
        state = self.states.get(symbol)
        if state is None:
            return
        try:
            kind = event.get("e")
            if kind == "trade":
                self._process_trade(state, event)
            elif kind == "depthUpdate":
                self._process_depth(state, event, recv_ns, mono)
        except Exception as exc:
            state.exception_count += 1
            self.logger.exception("Message processing failed for %s: %s", symbol, exc)
            if isinstance(exc, OSError):
                self.stop_reason = f"file I/O error: {exc}"
                self.stop_event.set()
                return
            if event.get("e") == "depthUpdate":
                state.start_snapshot("depth processing exception", resync=True)
                state.buffer_depth(event, recv_ns, mono)

    async def _connect_and_collect(self, url: str) -> None:
        from websockets.asyncio.client import connect

        async with connect(url, ping_interval=20, ping_timeout=20,
                           close_timeout=5, max_size=2 * 1024 * 1024,
                           max_queue=16, write_limit=32 * 1024,
                           compression=None) as websocket:
            self.logger.info("WebSocket connection established")
            await self._start_snapshots("new WebSocket connection")
            while not self.stop_event.is_set() and time.monotonic() < self.deadline:
                timeout = min(1.0, max(0.05, self.deadline - time.monotonic()))
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    raw = None
                if raw is not None:
                    recv_ns = time.time_ns()
                    mono = time.monotonic()
                    self._dispatch(json.loads(raw), recv_ns, mono)
                    await self._pump_snapshots()
                now = time.monotonic()
                if now - self.last_status >= self.args.status_interval:
                    self._status()
                if now >= self.deadline:
                    self.stop_reason = "duration reached"
                    self.stop_event.set()

    def _status(self) -> None:
        now = time.monotonic()
        rss = process_rss_bytes()
        self.peak_rss = max(self.peak_rss, rss)
        available = system_available_bytes()
        try:
            for state in self.states.values():
                state.lob_writer.flush()
                state.trade_writer.flush()
        except OSError as exc:
            state.exception_count += 1
            self.stop_reason = f"file I/O error: {exc}"
            self.logger.exception("Binary writer flush failed; stopping safely")
            self.stop_event.set()
        try:
            dev_now = self.output_dir.stat().st_dev
            disk = (__import__("shutil").disk_usage(self.output_dir).free
                    if dev_now == self.start_device else None)
        except OSError:
            dev_now, disk = None, None
        if dev_now != self.start_device:
            self.stop_reason = "output filesystem changed or mount disappeared"
            self.logger.error("Output filesystem device changed; stopping safely")
            self.stop_event.set()
        elif disk is None or disk < MINIMUM_RUNTIME_FREE_BYTES:
            self.stop_reason = "free disk space fell below 10 GiB"
            self.logger.error("Free disk space is below 10 GiB; stopping safely")
            self.stop_event.set()
        lob_total = sum(s.lob_writer.current_size for s in self.states.values())
        trade_total = sum(s.trade_writer.current_size for s in self.states.values())
        elapsed = now - self.start_mono
        cpu_now = process_cpu_ticks()
        cpu_percent = None
        if cpu_now is not None and self.cpu_previous_ticks is not None:
            ticks_per_second = os.sysconf("SC_CLK_TCK")
            cpu_percent = max(0, cpu_now - self.cpu_previous_ticks) / ticks_per_second / max(now - self.last_status, 0.001) * 100
        self.cpu_previous_ticks = cpu_now
        cpu_text = f"{cpu_percent:.1f}%" if cpu_percent is not None else "n/a"
        disk_text = f"{disk/1024**3:.1f}GiB" if disk is not None else "unknown"
        print(f"STATUS elapsed={elapsed:.0f}s rss={rss/1024**2:.1f}MiB "
              f"available_ram={(available or 0)/1024**2:.1f}MiB cpu={cpu_text} "
              f"free_disk={disk_text} written={(lob_total+trade_total)/1024**2:.1f}MiB "
              f"reconnects={self.reconnect_count}", flush=True)
        for state in self.states.values():
            age = (now - state.last_depth_mono) if state.last_depth_mono else None
            print(f"  {state.symbol} lob={state.lob_writer.records_written} "
                  f"trades={state.trade_writer.records_written} update_id={state.book.update_id} "
                  f"lob_bytes={state.lob_writer.current_size} trade_bytes={state.trade_writer.current_size} "
                  f"resync={state.resync_count} gaps={state.gap_count} crossed={state.crossed_book_count} "
                  f"last_depth_age={age:.2f}s" if age is not None else
                  f"  {state.symbol} lob={state.lob_writer.records_written} "
                  f"trades={state.trade_writer.records_written} update_id={state.book.update_id} "
                  f"lob_bytes={state.lob_writer.current_size} trade_bytes={state.trade_writer.current_size} "
                  f"resync={state.resync_count} gaps={state.gap_count} crossed={state.crossed_book_count} "
                  "last_depth_age=n/a", flush=True)
        self.last_status = now
        self.peak_rss = max(self.peak_rss, process_rss_bytes())

    def _summary(self, end_wall: datetime) -> dict:
        try:
            end_free = (__import__("shutil").disk_usage(self.output_dir).free
                        if self.output_dir.stat().st_dev == self.start_device else None)
        except OSError:
            end_free = None
        runtime = max(time.monotonic() - self.start_mono, 0.001)
        per_symbol = {}
        lob_total = trade_total = 0
        for symbol, state in self.states.items():
            for writer in (state.lob_writer, state.trade_writer):
                try:
                    writer.close()
                except OSError:
                    state.exception_count += 1
                    self.logger.exception("Failed to close binary file %s", writer.path)
            try:
                lob_size = state.lob_writer.path.stat().st_size
            except OSError:
                lob_size = state.lob_writer.current_size
            try:
                trade_size = state.trade_writer.path.stat().st_size
            except OSError:
                trade_size = state.trade_writer.current_size
            lob_total += lob_size
            trade_total += trade_size
            per_symbol[symbol] = {
                "lob_records_written": state.lob_writer.records_written,
                "trade_records_written": state.trade_writer.records_written,
                "first_lob_timestamp_ms": state.lob_writer.first_event_ts_ms,
                "last_lob_timestamp_ms": state.lob_writer.last_event_ts_ms,
                "first_trade_timestamp_ms": state.trade_writer.first_event_ts_ms,
                "last_trade_timestamp_ms": state.trade_writer.last_event_ts_ms,
                "lob_file_size_bytes": lob_size,
                "trade_file_size_bytes": trade_size,
                "rest_snapshots_fetched": state.snapshot_count,
                "rest_snapshot_requests": state.snapshot_requests,
                "resyncs": state.resync_count,
                "update_id_gaps": state.gap_count,
                "reconnect_related_rebuilds": state.reconnect_rebuild_count,
                "crossed_books": state.crossed_book_count,
                "incomplete_book_incidents": state.incomplete_book_count,
                "discarded_old_depth_events": state.old_depth_count,
                "sync_buffer_overflows": state.buffer_overflow_count,
                "exceptions": state.exception_count,
            }
        total = lob_total + trade_total
        collection_runtime = (min(runtime, self.args.duration)
                              if self.stop_reason == "duration reached" else runtime)
        hourly = total * 3600 / max(collection_runtime, 0.001)
        return {
            "start_time_utc": iso_utc(self.start_wall),
            "end_time_utc": iso_utc(end_wall),
            "actual_runtime_seconds": runtime,
            "requested_collection_runtime_seconds": self.args.duration,
            "measured_collection_runtime_seconds": collection_runtime,
            "stop_reason": self.stop_reason,
            "peak_rss_bytes": self.peak_rss,
            "final_rss_bytes": process_rss_bytes(),
            "cpu_ticks": {"start": self.cpu_start, "end": process_cpu_ticks()},
            "total_bytes_written": total,
            "lob_bytes_written": lob_total,
            "trade_bytes_written": trade_total,
            "free_disk_bytes_at_start": self.start_free,
            "free_disk_bytes_at_end": end_free,
            "average_gb_generated_per_hour": hourly / 1e9,
            "projected_gb_per_day": hourly * 24 / 1e9,
            "projected_gb_for_7_days": hourly * 24 * 7 / 1e9,
            "projection_note": "Linear estimate from actual bytes written divided by pilot runtime.",
            "websocket_reconnect_count": self.reconnect_count,
            "total_lob_records": sum(s.lob_writer.records_written for s in self.states.values()),
            "total_trade_records": sum(s.trade_writer.records_written for s in self.states.values()),
            "symbols": per_symbol,
        }

    def request_stop(self, sig: signal.Signals) -> None:
        self.stop_reason = f"signal {sig.name}"
        self.logger.info("Received %s; beginning graceful shutdown", sig.name)
        self.stop_event.set()

    async def run(self) -> Path:
        streams = []
        for symbol in self.args.symbols:
            lower = symbol.lower()
            streams.extend((lower + "@depth@100ms", lower + "@trade"))
        url = SPOT_STREAM_URL + "/".join(streams)
        retry = 0
        try:
            while not self.stop_event.is_set() and time.monotonic() < self.deadline:
                try:
                    await self._connect_and_collect(url)
                    if self.stop_event.is_set():
                        break
                    raise RuntimeError("WebSocket ended unexpectedly")
                except asyncio.CancelledError:
                    self.stop_reason = "cancelled"
                    raise
                except Exception as exc:
                    self.reconnect_count += 1
                    retry = min(retry, len(RECONNECT_DELAYS_SECONDS) - 1)
                    delay = RECONNECT_DELAYS_SECONDS[retry]
                    retry += 1
                    self.logger.exception("WebSocket disconnect/error; reconnect in %ss: %s", delay, exc)
                    for state in self.states.values():
                        state.reset_for_reconnect()
                    await asyncio.sleep(min(delay, max(0, self.deadline - time.monotonic())))
            self.stop_reason = self.stop_reason or "duration reached"
        except asyncio.CancelledError:
            raise
        finally:
            for state in self.states.values():
                if state.snapshot_task and not state.snapshot_task.done():
                    state.snapshot_task.cancel()
            tasks = [s.snapshot_task for s in self.states.values() if s.snapshot_task]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.peak_rss = max(self.peak_rss, process_rss_bytes())
            end_wall = utc_now()
            summary = self._summary(end_wall)
            self.summary_file.seek(0)
            self.summary_file.truncate()
            json.dump(summary, self.summary_file, indent=2, sort_keys=True)
            self.summary_file.write("\n")
            self.summary_file.flush()
            self.logger.info("Graceful shutdown; reason=%s", self.stop_reason)
            self.logger.info("Collector finished; total bytes=%d", summary["total_bytes_written"])
            self.summary_file.close()
            for handler in list(self.logger.handlers):
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)
        return self.run_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True,
                        help="existing writable directory on the intended storage filesystem")
    parser.add_argument("--duration", type=float, default=DEFAULT_PILOT_DURATION_SECONDS,
                        help="collection duration in seconds (default: 3600)")
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS),
                        help="Binance Spot symbols; default is the five project markets")
    parser.add_argument("--status-interval", type=float, default=STATUS_INTERVAL_SECONDS)
    return parser.parse_args()


async def get_symbol_info_with_retry(symbol: str) -> dict:
    for attempt in range(1, len(REST_RETRY_DELAYS_SECONDS) + 2):
        try:
            return await asyncio.to_thread(get_symbol_info, symbol)
        except BinanceAPIError as exc:
            print(f"Binance symbol metadata request failed for {symbol} "
                  f"(attempt {attempt}): {exc}", file=sys.stderr, flush=True)
            if attempt > len(REST_RETRY_DELAYS_SECONDS):
                raise
            await asyncio.sleep(REST_RETRY_DELAYS_SECONDS[attempt - 1])
    raise AssertionError("unreachable")


async def async_main() -> Path:
    args = parse_args()
    args.symbols = [s.upper() for s in args.symbols]
    if args.duration <= 0 or args.status_interval <= 0:
        raise RuntimeError("duration and status interval must be positive")
    unknown = sorted(set(args.symbols) - set(DEFAULT_SYMBOLS))
    if unknown:
        raise RuntimeError(f"Symbols are outside the configured project market set: {unknown}")
    if len(set(args.symbols)) != len(args.symbols):
        raise RuntimeError("Duplicate symbols are not allowed")
    output_dir, start_free, start_device, mount, _source = disk_preflight(args.output_dir)
    clock = check_clock()
    print("Querying Binance Spot symbol filters and current prices concurrently...", flush=True)
    info_rows = await asyncio.gather(*(get_symbol_info_with_retry(symbol) for symbol in args.symbols))
    infos = {item["symbol"]: item for item in info_rows}
    current_free = __import__("shutil").disk_usage(output_dir).free
    if output_dir.stat().st_dev != start_device:
        raise RuntimeError("Output filesystem changed before collection could start")
    if current_free < MINIMUM_START_FREE_BYTES:
        raise RuntimeError("Refusing to start: output filesystem now has less than 10 GiB free")
        print(f"{symbol}: tickSize={infos[symbol]['tick_size']} "
              f"stepSize={infos[symbol]['step_size']} "
              f"currentTick={infos[symbol]['current_price_tick']} "
              f"checkedMaxTick={infos[symbol]['max_price_tick_checked']}", flush=True)
    collector = Collector(args, output_dir, start_free, start_device, mount, clock, infos)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, collector.request_stop, sig)
        except NotImplementedError:
            pass
    run_dir = await collector.run()
    print(f"Run directory: {run_dir}", flush=True)
    return run_dir


def main() -> int:
    try:
        asyncio.run(async_main())
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
