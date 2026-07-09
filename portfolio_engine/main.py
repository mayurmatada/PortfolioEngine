import json
import logging
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import redis

from allocator import (
    choose_target_vols,
    estimate_covariance,
    estimate_mu,
    inverse_vol_weights,
    optimize_sharpe_proxy,
    portfolio_volatility,
    sharpe_proxy,
)
from config import (
    COVARIANCE_LOOKBACK_DAYS,
    DIAGONAL_SHRINKAGE_LAMBDA,
    MAX_OPT_ITERS,
    MIN_PRICE_ROWS,
    OPT_STEP_SIZE,
    PORTFOLIO_LATEST_KEY,
    PORTFOLIO_STREAM,
    REBALANCE_CHECK_SECONDS,
    REBALANCE_HOUR_UTC,
    REBALANCE_MINUTE_UTC,
    REDIS_ADDR,
    RETURN_LOOKBACK_MINUTES,
    SYMBOLS,
    TURNOVER_FEE_RATE,
    WEIGHT_CAP,
)
from db import (
    compute_portfolio_pnl,
    compute_period_asset_returns,
    connect,
    ensure_tables,
    fetch_latest_forecasts,
    fetch_latest_portfolio_state,
    fetch_return_history,
    fetch_short_return_history,
    insert_portfolio_state,
    wait_for_backfill,
    wait_for_forecasts,
    wait_for_prices,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
logger = logging.getLogger("portfolio.main")


def _next_rebalance_from(now_utc: datetime) -> datetime:
    target = now_utc.replace(hour=REBALANCE_HOUR_UTC, minute=REBALANCE_MINUTE_UTC, second=0, microsecond=0)
    if now_utc >= target:
        target = target + timedelta(days=1)
    return target


def _publish(redis_client, payload: dict):
    stream_payload = {
        "time": payload["time"],
        "weights": json.dumps(payload["weights"], sort_keys=True),
        "portfolio_vol": str(payload["portfolio_vol"]),
        "sharpe_proxy": str(payload["sharpe_proxy"]),
        "equity": str(payload["equity"]),
        "transaction_cost": str(payload["transaction_cost"]),
        "turnover": str(payload["turnover"]),
        "period_pnl": str(payload["period_pnl"]),
        "cumulative_pnl": str(payload["cumulative_pnl"]),
    }
    redis_client.xadd(PORTFOLIO_STREAM, stream_payload)
    redis_client.set(PORTFOLIO_LATEST_KEY, json.dumps(payload, sort_keys=True))


def _run_rebalance(conn, redis_client):
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    cov_prices = fetch_return_history(conn, SYMBOLS, COVARIANCE_LOOKBACK_DAYS)
    short_prices = fetch_short_return_history(conn, SYMBOLS, RETURN_LOOKBACK_MINUTES)
    forecasts = fetch_latest_forecasts(conn, SYMBOLS, now_utc)

    cov = estimate_covariance(cov_prices, SYMBOLS, DIAGONAL_SHRINKAGE_LAMBDA)
    mu = estimate_mu(short_prices, SYMBOLS)
    model_vols = choose_target_vols(forecasts, SYMBOLS)

    weights_opt = optimize_sharpe_proxy(mu, cov, WEIGHT_CAP, MAX_OPT_ITERS, OPT_STEP_SIZE)
    if np.any(np.isnan(weights_opt)) or np.any(weights_opt < -1e-12):
        weights_opt = np.full(len(SYMBOLS), 1.0 / len(SYMBOLS))

    weights_iv = inverse_vol_weights(model_vols, WEIGHT_CAP)
    if np.any(model_vols <= 0):
        weights = weights_opt
    else:
        weights = 0.5 * weights_opt + 0.5 * weights_iv
        weights = weights / weights.sum()

    state = fetch_latest_portfolio_state(conn)
    if state:
        prev_time = state["time"]
        prev_weights_dict = state["weights"]
        prev_equity = float(state["equity"])
        prev_cumulative_pnl = float(state.get("cumulative_pnl", 0.0))
    else:
        prev_time = now_utc - timedelta(days=1)
        prev_weights_dict = {s: (1.0 / len(SYMBOLS)) for s in SYMBOLS}
        prev_equity = 1.0
        prev_cumulative_pnl = 0.0

    prev_weights = np.array([float(prev_weights_dict.get(s, 0.0)) for s in SYMBOLS], dtype=float)
    weights_dict = {s: float(w) for s, w in zip(SYMBOLS, weights)}

    period_returns = compute_period_asset_returns(conn, SYMBOLS, prev_time, now_utc)
    pnl = compute_portfolio_pnl(
        prev_weights=prev_weights_dict,
        asset_returns=period_returns,
        prev_equity=prev_equity,
        new_weights=weights_dict,
        transaction_cost_rate=TURNOVER_FEE_RATE,
        prev_cumulative_pnl=prev_cumulative_pnl,
        half_l1_turnover=False,
    )
    turnover = float(pnl["turnover"])
    fee = float(pnl["transaction_cost"])
    equity = max(0.0, float(pnl["equity"]))
    period_pnl = float(equity - prev_equity)
    cumulative_pnl = float(prev_cumulative_pnl + period_pnl)

    p_vol = portfolio_volatility(weights, cov)
    p_sharpe = sharpe_proxy(weights, mu, cov)

    insert_portfolio_state(
        conn,
        now_utc,
        weights_dict,
        p_vol,
        p_sharpe,
        equity,
        fee,
        turnover,
        period_pnl,
        cumulative_pnl,
    )

    payload = {
        "time": now_utc.isoformat(),
        "weights": weights_dict,
        "portfolio_vol": p_vol,
        "sharpe_proxy": p_sharpe,
        "equity": equity,
        "transaction_cost": fee,
        "turnover": turnover,
        "period_pnl": period_pnl,
        "cumulative_pnl": cumulative_pnl,
    }
    _publish(redis_client, payload)
    logger.info(
        "REBALANCE_COMPLETED time=%s equity=%.6f period_pnl=%.6f cumulative_pnl=%.6f fee=%.6f turnover=%.6f vol=%.6f sharpe_proxy=%.6f",
        now_utc.isoformat(),
        equity,
        period_pnl,
        cumulative_pnl,
        fee,
        turnover,
        p_vol,
        p_sharpe,
    )


def main():
    logger.info("PORTFOLIO_ENGINE_START symbols=%s", ",".join(SYMBOLS))
    conn = connect()
    ensure_tables(conn)

    host, port = REDIS_ADDR.split(":")
    redis_client = redis.Redis(host=host, port=int(port), decode_responses=True)

    wait_for_backfill(conn, SYMBOLS)
    wait_for_prices(conn, SYMBOLS, min_rows=MIN_PRICE_ROWS)
    wait_for_forecasts(conn, SYMBOLS)

    next_rebalance_at = _next_rebalance_from(datetime.now(timezone.utc))
    logger.info("REBALANCE_SCHEDULE next_at=%s", next_rebalance_at.isoformat())

    while True:
        now_utc = datetime.now(timezone.utc)
        if now_utc >= next_rebalance_at:
            try:
                _run_rebalance(conn, redis_client)
            except Exception as exc:
                logger.exception("REBALANCE_FAILED error=%s", exc)
            next_rebalance_at = _next_rebalance_from(datetime.now(timezone.utc))
            logger.info("REBALANCE_SCHEDULE next_at=%s", next_rebalance_at.isoformat())

        sleep_s = max(1, min(REBALANCE_CHECK_SECONDS, int((next_rebalance_at - now_utc).total_seconds())))
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
