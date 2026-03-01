import json
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import redis

from config import BUFFER_SIZE, REDIS_ADDR, REDIS_STREAM, ROLLING_WINDOWS
from db import connect as db_connect, ensure_tables, insert_features, insert_price
from features import latest_feature_row

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("feature_engineering")

# Minimum candles needed before we can compute features for the largest window.
MIN_BUFFER = max(ROLLING_WINDOWS) + 2


def _parse_kline(raw: dict) -> dict:
    """Turn the JSON blob produced by the Go ingestion service into a flat dict."""
    return {
        "symbol": raw["s"],
        "time": datetime.fromtimestamp(raw["t"] / 1000, tz=timezone.utc),
        "open": float(raw["o"]),
        "high": float(raw["h"]),
        "low": float(raw["l"]),
        "close": float(raw["c"]),
        "volume": float(raw["v"]),
    }


def _buffer_to_df(buf: deque[dict]) -> pd.DataFrame:
    """Convert the ring-buffer of candle dicts into a DatetimeIndex DataFrame."""
    df = pd.DataFrame(list(buf))
    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)
    return df


def main() -> None:
    # ── connections ───────────────────────────────────────────────────
    host, port = REDIS_ADDR.split(":")
    rdb = redis.Redis(host=host, port=int(port), decode_responses=True)
    logger.info("Connected to Redis at %s", REDIS_ADDR)

    pg = db_connect()
    ensure_tables(pg)
    logger.info("TimescaleDB ready")

    # Per-symbol ring-buffer of recent candles (dicts).
    buffers: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=BUFFER_SIZE))

    last_id = "0"

    # ── consume loop ──────────────────────────────────────────────────
    while True:
        response: list[Any] = rdb.xread(  # type: ignore[assignment]
            {REDIS_STREAM: last_id}, block=0, count=10,
        )
        if not response:
            continue

        for _stream, messages in response:
            for msg_id, msg in messages:
                last_id = msg_id

                kline_raw = json.loads(msg["kline"])
                candle = _parse_kline(kline_raw)
                symbol = candle["symbol"]
                ts = candle["time"]

                # 1. Persist raw OHLCV
                insert_price(
                    pg, ts, symbol,
                    candle["open"], candle["high"],
                    candle["low"], candle["close"],
                    candle["volume"],
                )

                # 2. Append to in-memory buffer
                buffers[symbol].append(candle)

                # 3. Compute & persist features once we have enough history
                if len(buffers[symbol]) >= MIN_BUFFER:
                    df = _buffer_to_df(buffers[symbol])
                    feat_row = latest_feature_row(df)
                    insert_features(pg, ts, symbol, feat_row)
                    logger.info(
                        "%s  %s  close=%.2f  rv5=%.6s  rv60=%.6s",
                        symbol, ts.isoformat(),
                        candle["close"],
                        feat_row.get("realized_vol_5"),
                        feat_row.get("realized_vol_60"),
                    )
                else:
                    logger.info(
                        "%s  buffering %d/%d",
                        symbol, len(buffers[symbol]), MIN_BUFFER,
                    )


if __name__ == "__main__":
    main()
