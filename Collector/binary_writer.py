
from __future__ import annotations

from pathlib import Path


class BufferedBinaryWriter:
    def __init__(self, path: Path, record_size: int, capacity_records: int):
        self.path = path
        self.record_size = record_size
        self.capacity_records = capacity_records
        self.file = path.open("xb", buffering=1024 * 1024)
        self.buffer = bytearray()
        self.records_written = 0
        self.first_event_ts_ms: int | None = None
        self.last_event_ts_ms: int | None = None
        self.file_bytes = 0

    def append(self, record: bytes, event_ts_ms: int) -> None:
        if len(record) != self.record_size:
            raise ValueError("record size mismatch")
        self.buffer.extend(record)
        self.records_written += 1
        self.first_event_ts_ms = event_ts_ms if self.first_event_ts_ms is None else self.first_event_ts_ms
        self.last_event_ts_ms = event_ts_ms
        if len(self.buffer) >= self.record_size * self.capacity_records:
            self.flush()

    def flush(self) -> None:
        if self.buffer:
            self.file.write(self.buffer)
            self.file_bytes += len(self.buffer)
            self.buffer.clear()
        self.file.flush()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self.file.close()

    @property
    def current_size(self) -> int:
        return self.file_bytes + len(self.buffer)
