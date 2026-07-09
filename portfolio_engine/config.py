import os


def _parse_symbols(raw: str) -> list[str]:
    parts = [p.strip().upper() for p in raw.split(",")]
    return [p for p in parts if p]


TIMESCALEDB_DSN = os.environ.get(
    "TIMESCALEDB_DSN",
    "postgresql://postgres:postgres@timescaledb:5432/portfolio",
)
REDIS_ADDR = os.environ.get("REDIS_ADDR", "redis:6379")

SYMBOLS = _parse_symbols(os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"))

REBALANCE_CHECK_SECONDS = int(os.environ.get("REBALANCE_CHECK_SECONDS", "30"))
REBALANCE_HOUR_UTC = int(os.environ.get("REBALANCE_HOUR_UTC", "0"))
REBALANCE_MINUTE_UTC = int(os.environ.get("REBALANCE_MINUTE_UTC", "0"))

COVARIANCE_LOOKBACK_DAYS = int(os.environ.get("COVARIANCE_LOOKBACK_DAYS", "7"))
RETURN_LOOKBACK_MINUTES = int(os.environ.get("RETURN_LOOKBACK_MINUTES", "120"))

DIAGONAL_SHRINKAGE_LAMBDA = float(os.environ.get("DIAGONAL_SHRINKAGE_LAMBDA", "1e-6"))
WEIGHT_CAP = float(os.environ.get("WEIGHT_CAP", "0.6"))
TURNOVER_FEE_RATE = float(os.environ.get("TURNOVER_FEE_RATE", "0.001"))

MIN_PRICE_ROWS = int(os.environ.get("MIN_PRICE_ROWS", "500"))
MAX_OPT_ITERS = int(os.environ.get("MAX_OPT_ITERS", "120"))
OPT_STEP_SIZE = float(os.environ.get("OPT_STEP_SIZE", "0.05"))

PORTFOLIO_STREAM = os.environ.get("PORTFOLIO_STREAM", "portfolio_state_stream")
PORTFOLIO_LATEST_KEY = os.environ.get("PORTFOLIO_LATEST_KEY", "portfolio:latest")
