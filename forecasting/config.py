import os

# TimescaleDB connection string
TIMESCALEDB_DSN = os.environ.get("TIMESCALEDB_DSN", "postgresql://postgres:postgres@timescaledb:5432/portfolio")

# Symbols to forecast
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# Rolling window for training (days)
ROLLING_WINDOW_DAYS = 60

# Retrain interval (hours)
RETRAIN_INTERVAL_HOURS = 24

# Inference polling interval (seconds)
INFERENCE_POLL_SECONDS = 60

# When no models are available yet (or training fails), retry training quickly
# instead of waiting the full retrain interval.
TRAINING_EMPTY_RETRY_SECONDS = int(os.environ.get("TRAINING_EMPTY_RETRY_SECONDS", "60"))

# When some symbols are still missing models, keep retrying faster than daily retrain
# so late-arriving feature data can be picked up quickly.
TRAINING_PARTIAL_RETRY_SECONDS = int(os.environ.get("TRAINING_PARTIAL_RETRY_SECONDS", "300"))

# Forward window for realized volatility label (minutes)
FORWARD_WINDOW = 5
