import logging
import time
from datetime import timedelta

import pandas as pd

from db import (
    fetch_forecasts_missing_realized,
    fetch_prices_since,
    fetch_unforecasted_features,
    insert_forecasts,
    update_realized_vol,
    write_rolling_model_metrics,
)
from config import SYMBOLS, FORWARD_WINDOW, ROLLING_LOSS_POINTS
from labels import compute_forward_realized_vol

logger = logging.getLogger("forecasting.inference")


def run_inference(conn, models):
    for symbol in SYMBOLS:
        model_bundle = models.get(symbol)
        if not model_bundle:
            logger.warning("No models available for %s; skipping inference", symbol)
            continue

        loop_start = time.perf_counter()
        df = fetch_unforecasted_features(conn, symbol)
        if df.empty:
            _backfill_realized_vol(conn, symbol)
            _update_rolling_losses(conn, symbol)
            continue

        garch = model_bundle["garch"]
        xgb = model_bundle["xgb"]
        feature_cols = model_bundle["feature_cols"]

        X = df[feature_cols].copy()
        valid_mask = X.notna().all(axis=1)

        if not valid_mask.any():
            logger.warning("No valid feature rows for %s in this inference batch", symbol)
            _backfill_realized_vol(conn, symbol)
            _update_rolling_losses(conn, symbol)
            continue

        valid_df = df.loc[valid_mask].copy()
        X_valid = X.loc[valid_mask]

        garch_vol = None
        try:
            garch_vol = float(garch.predict(1))
        except Exception as exc:
            logger.warning("GARCH failed for %s: %s", symbol, exc)

        xgb_preds = [None] * len(valid_df)
        try:
            xgb_raw = xgb.predict(X_valid)
            xgb_preds = [float(v) for v in xgb_raw]
        except Exception as exc:
            logger.warning("XGBoost failed for %s; fallback to GARCH only: %s", symbol, exc)

        preds = []
        for (_, row), xgb_vol in zip(valid_df.iterrows(), xgb_preds):
            preds.append((row["time"], symbol, garch_vol, xgb_vol, None))

        insert_forecasts(conn, preds)

        latency_ms = (time.perf_counter() - loop_start) * 1000
        logger.info("inference symbol=%s rows=%d latency_ms=%.1f", symbol, len(preds), latency_ms)

        _backfill_realized_vol(conn, symbol)
        _update_rolling_losses(conn, symbol)


def _backfill_realized_vol(conn, symbol):
    missing_times = fetch_forecasts_missing_realized(conn, symbol, FORWARD_WINDOW)
    if not missing_times:
        return

    start_time = missing_times[0] - timedelta(minutes=1)
    prices_df = fetch_prices_since(conn, symbol, start_time)
    if prices_df.empty:
        return

    prices_df.sort_values("time", inplace=True)
    rv = compute_forward_realized_vol(prices_df["close"], window=FORWARD_WINDOW)
    rv_by_time = pd.Series(rv.values, index=prices_df["time"]).to_dict()

    updates = []
    for ts in missing_times:
        value = rv_by_time.get(ts)
        if value is None or pd.isna(value):
            continue
        updates.append((float(value), ts, symbol))

    update_realized_vol(conn, updates)


def _update_rolling_losses(conn, symbol):
    write_rolling_model_metrics(conn, symbol=symbol, window_points=ROLLING_LOSS_POINTS)
