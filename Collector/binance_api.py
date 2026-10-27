
from __future__ import annotations

import json
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import (EXCHANGE_INFO_URL, REST_DEPTH_URL, REST_RETRY_DELAYS_SECONDS,
                    REST_TIMEOUT_SECONDS, REST_TICKER_URL, REST_SNAPSHOT_DEPTH)


class BinanceAPIError(RuntimeError):
    pass


def _get_json(url: str) -> dict:
    request = Request(url, headers={"User-Agent": "academic-spot-lob-collector/1.0"})
    try:
        with urlopen(request, timeout=REST_TIMEOUT_SECONDS) as response:
            return json.loads(response.read())
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise BinanceAPIError(f"GET {url} failed: {exc}") from exc


def get_symbol_info(symbol: str) -> dict:
    query = urlencode({"symbol": symbol})
    payload = _get_json(f"{EXCHANGE_INFO_URL}?{query}")
    entries = payload.get("symbols", [])
    if len(entries) != 1 or entries[0].get("status") != "TRADING":
        raise BinanceAPIError(f"{symbol} is not a trading Spot symbol")
    item = entries[0]
    if item.get("isSpotTradingAllowed") is False:
        raise BinanceAPIError(f"{symbol} does not allow Spot trading")
    filters = {f["filterType"]: f for f in item.get("filters", [])}
    price_filter = filters.get("PRICE_FILTER")
    lot_filter = filters.get("LOT_SIZE")
    if not price_filter or not lot_filter:
        raise BinanceAPIError(f"Missing PRICE_FILTER or LOT_SIZE for {symbol}")
    tick_size = Decimal(price_filter["tickSize"])
    if tick_size <= 0:
        raise BinanceAPIError(f"Invalid tick size for {symbol}")
    max_price = Decimal(price_filter.get("maxPrice", "0"))
    ticker = _get_json(f"{REST_TICKER_URL}?{urlencode({'symbol': symbol})}")
    current_price = Decimal(ticker["price"])
    max_tick = (max_price / tick_size) if max_price else (current_price / tick_size)
    if max_tick != max_tick.to_integral_value() or max_tick > Decimal(2**31 - 1):
        raise BinanceAPIError(f"{symbol} price ticks do not safely fit signed int32")
    current_tick = current_price / tick_size
    if current_tick != current_tick.to_integral_value() or not (0 < current_tick <= 2**31 - 1):
        raise BinanceAPIError(f"{symbol} current price tick does not fit signed int32")
    return {
        "symbol": symbol,
        "base_asset": item["baseAsset"],
        "quote_asset": item["quoteAsset"],
        "tick_size": str(tick_size),
        "step_size": lot_filter["stepSize"],
        "current_price": str(current_price),
        "current_price_tick": int(current_tick),
        "max_price": str(max_price),
        "max_price_tick_checked": int(max_tick),
    }


def get_depth_snapshot(symbol: str) -> dict:
    query = urlencode({"symbol": symbol, "limit": REST_SNAPSHOT_DEPTH})
    return _get_json(f"{REST_DEPTH_URL}?{query}")
