import logging
import time
from datetime import datetime
import psycopg
from config import TIMESCALEDB_DSN

logger = logging.getLogger(__name__)

_MAX_RETRIES = 10
_RETRY_INTERVAL = 3  # seconds


def connect():
    """Return a new psycopg connection (autocommit), retrying on transient failures."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            conn = psycopg.connect(TIMESCALEDB_DSN, autocommit=True)
            return conn
        except psycopg.OperationalError as exc:
            if attempt == _MAX_RETRIES:
                raise
            logger.warning(
                "TimescaleDB not ready (attempt %d/%d): %s — retrying in %ds",
                attempt, _MAX_RETRIES, exc, _RETRY_INTERVAL,
            )
            time.sleep(_RETRY_INTERVAL)


def ensure_tables(conn) -> None:
    """Idempotently create the prices and features hypertables."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                time    TIMESTAMPTZ      NOT NULL,
                symbol  TEXT             NOT NULL,
                open    DOUBLE PRECISION,
                high    DOUBLE PRECISION,
                low     DOUBLE PRECISION,
                close   DOUBLE PRECISION,
                volume  DOUBLE PRECISION
            );
        """)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_name = 'prices'
            );
        """)
        if not cur.fetchone()[0]:
            cur.execute("SELECT create_hypertable('prices', 'time', if_not_exists => TRUE);")

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_symbol_time
            ON prices (symbol, time DESC);
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS features (
                time              TIMESTAMPTZ      NOT NULL,
                symbol            TEXT             NOT NULL,
                log_return        DOUBLE PRECISION,
                squared_return    DOUBLE PRECISION,
                volume_roc        DOUBLE PRECISION,
                hl_range          DOUBLE PRECISION,
                ret_mean_5        DOUBLE PRECISION,
                ret_std_5         DOUBLE PRECISION,
                realized_vol_5    DOUBLE PRECISION,
                parkinson_vol_5   DOUBLE PRECISION,
                ret_skew_5        DOUBLE PRECISION,
                ret_kurt_5        DOUBLE PRECISION,
                volume_mean_5     DOUBLE PRECISION,
                ret_mean_15       DOUBLE PRECISION,
                ret_std_15        DOUBLE PRECISION,
                realized_vol_15   DOUBLE PRECISION,
                parkinson_vol_15  DOUBLE PRECISION,
                ret_skew_15       DOUBLE PRECISION,
                ret_kurt_15       DOUBLE PRECISION,
                volume_mean_15    DOUBLE PRECISION,
                ret_mean_30       DOUBLE PRECISION,
                ret_std_30        DOUBLE PRECISION,
                realized_vol_30   DOUBLE PRECISION,
                parkinson_vol_30  DOUBLE PRECISION,
                ret_skew_30       DOUBLE PRECISION,
                ret_kurt_30       DOUBLE PRECISION,
                volume_mean_30    DOUBLE PRECISION,
                ret_mean_60       DOUBLE PRECISION,
                ret_std_60        DOUBLE PRECISION,
                realized_vol_60   DOUBLE PRECISION,
                parkinson_vol_60  DOUBLE PRECISION,
                ret_skew_60       DOUBLE PRECISION,
                ret_kurt_60       DOUBLE PRECISION,
                volume_mean_60    DOUBLE PRECISION
            );
        """)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_name = 'features'
            );
        """)
        if not cur.fetchone()[0]:
            cur.execute("SELECT create_hypertable('features', 'time', if_not_exists => TRUE);")

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_features_symbol_time
            ON features (symbol, time DESC);
        """)

    logger.info("TimescaleDB tables ensured.")


def insert_price(conn, ts: datetime, symbol: str, o: float, h: float, l: float, c: float, v: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO prices (time, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (ts, symbol, o, h, l, c, v),
        )


_FEATURE_COLS = [
    "log_return", "squared_return", "volume_roc", "hl_range",
    "ret_mean_5", "ret_std_5", "realized_vol_5", "parkinson_vol_5",
    "ret_skew_5", "ret_kurt_5", "volume_mean_5",
    "ret_mean_15", "ret_std_15", "realized_vol_15", "parkinson_vol_15",
    "ret_skew_15", "ret_kurt_15", "volume_mean_15",
    "ret_mean_30", "ret_std_30", "realized_vol_30", "parkinson_vol_30",
    "ret_skew_30", "ret_kurt_30", "volume_mean_30",
    "ret_mean_60", "ret_std_60", "realized_vol_60", "parkinson_vol_60",
    "ret_skew_60", "ret_kurt_60", "volume_mean_60",
]


def insert_features(conn, ts: datetime, symbol: str, feat_row: dict) -> None:
    """Insert one feature row for (time, symbol)."""
    cols = ["time", "symbol"] + _FEATURE_COLS
    placeholders = ", ".join(["%s"] * len(cols))
    values = [ts, symbol] + [feat_row.get(c) for c in _FEATURE_COLS]

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO features ({', '.join(cols)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;",
            values,
        )
