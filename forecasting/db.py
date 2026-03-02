import logging
import time
from datetime import datetime

import pandas as pd
import psycopg
from config import TIMESCALEDB_DSN

logger = logging.getLogger("forecasting.db")


def ensure_backfill_status_table(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS backfill_status (
                symbol TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                completed_at TIMESTAMPTZ NOT NULL
            );
        ''')
        conn.commit()


def mark_backfill_done(conn, symbol):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO backfill_status (symbol, status, completed_at)
            VALUES (%s, 'done', NOW())
            ON CONFLICT (symbol) DO UPDATE SET status='done', completed_at=NOW();
        ''', (symbol,))
        conn.commit()


def wait_for_backfill(conn, symbols, poll_interval=10):
    expected = set(symbols)
    while True:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM backfill_status WHERE status='done';")
            done = {row[0] for row in cur.fetchall()}
        missing = sorted(expected - done)
        if not missing:
            logger.info("Backfill complete for all symbols: %s", ",".join(sorted(done)))
            break
        logger.info(
            "Waiting for backfill: done=%d/%d missing=%s",
            len(done),
            len(expected),
            ",".join(missing),
        )
        time.sleep(poll_interval)


def wait_for_features(conn, symbols, min_rows=100, poll_interval=15):
    """Block until every symbol has at least *min_rows* in the features table."""
    expected = set(symbols)
    while True:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, COUNT(*) AS cnt FROM features WHERE symbol = ANY(%s) GROUP BY symbol;",
                (list(expected),),
            )
            counts = {row[0]: row[1] for row in cur.fetchall()}
        ready = {s for s, c in counts.items() if c >= min_rows}
        missing = sorted(expected - ready)
        if not missing:
            logger.info(
                "Features ready for all symbols: %s (min %d rows each)",
                ",".join(sorted(ready)),
                min_rows,
            )
            break
        logger.info(
            "Waiting for features: ready=%d/%d missing=%s counts=%s",
            len(ready),
            len(expected),
            ",".join(missing),
            ",".join(f"{s}={counts.get(s, 0)}" for s in sorted(expected)),
        )
        time.sleep(poll_interval)


_MAX_RETRIES = 10
_RETRY_INTERVAL = 3  # seconds

FEATURE_COLS = [
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


def connect():
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


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS forecasts (
                time TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                garch_vol DOUBLE PRECISION,
                xgb_vol DOUBLE PRECISION,
                realized_vol DOUBLE PRECISION,
                PRIMARY KEY (symbol, time)
            );
        """)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_name = 'forecasts'
            );
        """)
        if not cur.fetchone()[0]:
            cur.execute("SELECT create_hypertable('forecasts', 'time', if_not_exists => TRUE);")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_forecasts_symbol_time
            ON forecasts (symbol, time DESC);
        """)
    logger.info("Forecasts table ensured.")


def fetch_training_data(conn, symbol, window_days):
    feature_select = ", ".join([f"f.{col}" for col in FEATURE_COLS])
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT f.time, f.symbol, {feature_select}, p.close
            FROM features f
            JOIN prices p ON f.symbol = p.symbol AND f.time = p.time
            WHERE f.symbol = %s
              AND f.time >= NOW() - (%s * INTERVAL '1 day')
            ORDER BY f.time ASC
        """, (symbol, window_days))
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df


def fetch_unforecasted_features(conn, symbol, limit=5000):
    feature_select = ", ".join([f"f.{col}" for col in FEATURE_COLS])
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT f.time, f.symbol, {feature_select}
            FROM features f
            LEFT JOIN forecasts fc ON f.symbol = fc.symbol AND f.time = fc.time
            WHERE f.symbol = %s AND fc.time IS NULL
            ORDER BY f.time ASC
            LIMIT %s
        """, (symbol, limit))
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df


def insert_forecasts(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO forecasts (time, symbol, garch_vol, xgb_vol, realized_vol)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol, time) DO NOTHING;
            """,
            rows,
        )
    logger.info(f"Inserted {len(rows)} forecast rows.")


def fetch_forecasts_missing_realized(conn, symbol, forward_window):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT time
            FROM forecasts
            WHERE symbol = %s
              AND realized_vol IS NULL
              AND time <= NOW() - (%s * INTERVAL '1 minute')
            ORDER BY time ASC
            """,
            (symbol, forward_window),
        )
        rows = cur.fetchall()
    return [row[0] for row in rows]


def fetch_prices_since(conn, symbol, start_time: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT time, close
            FROM prices
            WHERE symbol = %s
              AND time >= %s
            ORDER BY time ASC
            """,
            (symbol, start_time),
        )
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df


def update_realized_vol(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            UPDATE forecasts
            SET realized_vol = %s
            WHERE time = %s AND symbol = %s
            """,
            rows,
        )
    logger.info("Updated realized_vol for %d forecast rows.", len(rows))
