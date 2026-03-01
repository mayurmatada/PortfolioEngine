import os

# Redis
REDIS_ADDR = os.environ.get("REDIS_ADDR", "localhost:6379")
REDIS_STREAM = "kline_stream"

# TimescaleDB
TIMESCALEDB_DSN = os.environ.get(
    "TIMESCALEDB_DSN",
    "postgresql://postgres:postgres@localhost:5432/portfolio",
)

# Symbols
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# Rolling windows (minutes) used for feature computation
ROLLING_WINDOWS = [5, 15, 30, 60]

# How many candles to keep in the in-memory buffer per symbol.
# Must be >= max(ROLLING_WINDOWS) + 1 at minimum.
BUFFER_SIZE = 120
