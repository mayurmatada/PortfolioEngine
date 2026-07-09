import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import psycopg

from config import TIMESCALEDB_DSN

logger = logging.getLogger("portfolio.db")

_MAX_RETRIES = 10
_RETRY_INTERVAL = 3


def connect():
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return psycopg.connect(TIMESCALEDB_DSN, autocommit=True)
        except psycopg.OperationalError as exc:
            if attempt == _MAX_RETRIES:
                raise
            logger.warning(
                "TimescaleDB not ready (attempt %d/%d): %s — retrying in %ds",
                attempt,
                _MAX_RETRIES,
                exc,
                _RETRY_INTERVAL,
            )
            time.sleep(_RETRY_INTERVAL)


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_state (
                time TIMESTAMPTZ NOT NULL,
                weights JSONB NOT NULL,
                portfolio_vol DOUBLE PRECISION,
                sharpe_proxy DOUBLE PRECISION,
                equity DOUBLE PRECISION,
                transaction_cost DOUBLE PRECISION,
                turnover DOUBLE PRECISION,
                period_pnl DOUBLE PRECISION,
                cumulative_pnl DOUBLE PRECISION,
                PRIMARY KEY (time)
            );
            """
        )
        cur.execute("ALTER TABLE portfolio_state ADD COLUMN IF NOT EXISTS period_pnl DOUBLE PRECISION;")
        cur.execute("ALTER TABLE portfolio_state ADD COLUMN IF NOT EXISTS cumulative_pnl DOUBLE PRECISION;")
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_name = 'portfolio_state'
            );
            """
        )
        if not cur.fetchone()[0]:
            cur.execute("SELECT create_hypertable('portfolio_state', 'time', if_not_exists => TRUE);")

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_portfolio_state_time_desc
            ON portfolio_state (time DESC);
            """
        )


def wait_for_backfill(conn, symbols, poll_interval=10):
    expected = set(symbols)
    while True:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM backfill_status WHERE status='done';")
            done = {row[0] for row in cur.fetchall()}
        missing = sorted(expected - done)
        if not missing:
            logger.info("Backfill ready for all symbols: %s", ",".join(sorted(done)))
            return
        logger.info("Waiting for backfill done=%d/%d missing=%s", len(done), len(expected), ",".join(missing))
        time.sleep(poll_interval)


def wait_for_prices(conn, symbols, min_rows, poll_interval=10):
    expected = set(symbols)
    while True:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, COUNT(*) FROM prices WHERE symbol = ANY(%s) GROUP BY symbol;",
                (list(expected),),
            )
            counts = {row[0]: row[1] for row in cur.fetchall()}
        ready = {s for s, c in counts.items() if c >= min_rows}
        missing = sorted(expected - ready)
        if not missing:
            logger.info("Price history ready symbols=%s min_rows=%d", ",".join(sorted(ready)), min_rows)
            return
        logger.info(
            "Waiting for prices ready=%d/%d missing=%s counts=%s",
            len(ready),
            len(expected),
            ",".join(missing),
            ",".join(f"{s}={counts.get(s, 0)}" for s in sorted(expected)),
        )
        time.sleep(poll_interval)


def wait_for_forecasts(conn, symbols, poll_interval=15):
    expected = set(symbols)
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, COUNT(*)
                FROM forecasts
                WHERE symbol = ANY(%s)
                  AND (garch_vol IS NOT NULL OR xgb_vol IS NOT NULL)
                GROUP BY symbol;
                """,
                (list(expected),),
            )
            counts = {row[0]: row[1] for row in cur.fetchall()}
        ready = {s for s, c in counts.items() if c > 0}
        missing = sorted(expected - ready)
        if not missing:
            logger.info("Forecasts ready symbols=%s", ",".join(sorted(ready)))
            return
        logger.info(
            "Waiting for forecasts ready=%d/%d missing=%s",
            len(ready),
            len(expected),
            ",".join(missing),
        )
        time.sleep(poll_interval)


def fetch_latest_portfolio_state(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT time, weights, equity, period_pnl, cumulative_pnl
            FROM portfolio_state
            ORDER BY time DESC
            LIMIT 1;
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "time": row[0],
        "weights": row[1],
        "equity": float(row[2]) if row[2] is not None else 1.0,
        "period_pnl": float(row[3]) if row[3] is not None else 0.0,
        "cumulative_pnl": float(row[4]) if row[4] is not None else 0.0,
    }


def fetch_latest_prices(conn, symbols, as_of: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol, time, close
            FROM prices
            WHERE symbol = ANY(%s)
              AND time <= %s
            ORDER BY symbol, time DESC;
            """,
            (list(symbols), as_of),
        )
        rows = cur.fetchall()
    return {row[0]: {"time": row[1], "close": float(row[2])} for row in rows}


def fetch_return_history(conn, symbols, lookback_days: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT time, symbol, close
            FROM prices
            WHERE symbol = ANY(%s)
              AND time >= NOW() - (%s * INTERVAL '1 day')
            ORDER BY time ASC;
            """,
            (list(symbols), int(lookback_days)),
        )
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    if df.empty:
        return df
    df["close"] = df["close"].astype(float)
    return df


def fetch_short_return_history(conn, symbols, lookback_minutes: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT time, symbol, close
            FROM prices
            WHERE symbol = ANY(%s)
              AND time >= NOW() - (%s * INTERVAL '1 minute')
            ORDER BY time ASC;
            """,
            (list(symbols), int(lookback_minutes)),
        )
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    if df.empty:
        return df
    df["close"] = df["close"].astype(float)
    return df


def fetch_latest_forecasts(conn, symbols, as_of: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol,
                time,
                garch_vol,
                xgb_vol
            FROM forecasts
            WHERE symbol = ANY(%s)
              AND time <= %s
              AND (garch_vol IS NOT NULL OR xgb_vol IS NOT NULL)
            ORDER BY symbol, time DESC;
            """,
            (list(symbols), as_of),
        )
        rows = cur.fetchall()

    out = {}
    for row in rows:
        symbol = row[0]
        garch = float(row[2]) if row[2] is not None else None
        xgb = float(row[3]) if row[3] is not None else None
        out[symbol] = {
            "time": row[1],
            "garch_vol": garch,
            "xgb_vol": xgb,
        }
    return out


def insert_portfolio_state(
    conn,
    ts: datetime,
    weights_json,
    portfolio_vol,
    sharpe_proxy,
    equity,
    transaction_cost,
    turnover,
    period_pnl=None,
    cumulative_pnl=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO portfolio_state (
                time,
                weights,
                portfolio_vol,
                sharpe_proxy,
                equity,
                transaction_cost,
                turnover,
                period_pnl,
                cumulative_pnl
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time) DO UPDATE SET
                weights = EXCLUDED.weights,
                portfolio_vol = EXCLUDED.portfolio_vol,
                sharpe_proxy = EXCLUDED.sharpe_proxy,
                equity = EXCLUDED.equity,
                transaction_cost = EXCLUDED.transaction_cost,
                turnover = EXCLUDED.turnover,
                period_pnl = EXCLUDED.period_pnl,
                cumulative_pnl = EXCLUDED.cumulative_pnl;
            """,
            (
                ts,
                weights_json,
                portfolio_vol,
                sharpe_proxy,
                equity,
                transaction_cost,
                turnover,
                period_pnl,
                cumulative_pnl,
            ),
        )


def compute_period_asset_returns(conn, symbols, from_time: datetime, to_time: datetime):
    from_prices = fetch_latest_prices(conn, symbols, from_time)
    to_prices = fetch_latest_prices(conn, symbols, to_time)

    asset_returns = {}
    for symbol in symbols:
        src = from_prices.get(symbol)
        dst = to_prices.get(symbol)
        if not src or not dst or src["close"] <= 0:
            asset_returns[symbol] = 0.0
            continue
        asset_returns[symbol] = (dst["close"] / src["close"]) - 1.0
    return asset_returns


def compute_portfolio_pnl(
    prev_weights: dict,
    asset_returns: dict,
    prev_equity: float,
    new_weights: Optional[dict] = None,
    transaction_cost_rate: float = 0.001,
    prev_cumulative_pnl: float = 0.0,
    half_l1_turnover: bool = False,
):
    prev_equity = float(prev_equity) if prev_equity is not None else 1.0
    prev_weights = prev_weights or {}
    asset_returns = asset_returns or {}
    new_weights = new_weights or prev_weights

    symbols = set(prev_weights) | set(asset_returns) | set(new_weights)

    gross_return = sum(float(prev_weights.get(symbol, 0.0)) * float(asset_returns.get(symbol, 0.0)) for symbol in symbols)
    l1_turnover = sum(
        abs(float(new_weights.get(symbol, 0.0)) - float(prev_weights.get(symbol, 0.0))) for symbol in symbols
    )
    turnover = 0.5 * l1_turnover if half_l1_turnover else l1_turnover

    transaction_cost = float(transaction_cost_rate) * turnover
    net_return = gross_return - transaction_cost
    equity = prev_equity * (1.0 + net_return)
    period_pnl = equity - prev_equity
    cumulative_pnl = float(prev_cumulative_pnl) + period_pnl

    return {
        "gross_return": float(gross_return),
        "net_return": float(net_return),
        "turnover": float(turnover),
        "transaction_cost": float(transaction_cost),
        "equity": float(equity),
        "period_pnl": float(period_pnl),
        "cumulative_pnl": float(cumulative_pnl),
    }
