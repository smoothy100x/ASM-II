
from __future__ import annotations

from pathlib import Path


def process_rss_bytes() -> int:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def system_available_bytes() -> int | None:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def process_cpu_ticks() -> int | None:
    try:
        stat = Path("/proc/self/stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
                                                                            
        return int(fields[11]) + int(fields[12])
    except (OSError, ValueError, IndexError):
        return None
