import logging
import time

import numpy as np

from labels import compute_forward_realized_vol
from models.garch_model import GARCHModel
from models.xgb_model import XGBVolModel
from config import SYMBOLS, ROLLING_WINDOW_DAYS, FORWARD_WINDOW
from db import FEATURE_COLS, fetch_training_data

logger = logging.getLogger("forecasting.train")


def _mse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean((y_true - y_pred) ** 2))


def _qlike(y_true_vol: np.ndarray, y_pred_vol: np.ndarray, eps: float = 1e-12) -> float:
    y_true_var = np.maximum(y_true_vol ** 2, eps)
    y_pred_var = np.maximum(y_pred_vol ** 2, eps)
    return float(np.mean(np.log(y_pred_var) + (y_true_var / y_pred_var)))


def train_models(conn):
    models = {}
    total_symbols = len(SYMBOLS)
    attempted = 0
    skipped_no_data = 0
    skipped_not_enough = 0
    failed_symbols = 0
    garch_only_symbols = 0

    for symbol in SYMBOLS:
        attempted += 1
        logger.info("TRAIN_SYMBOL_START symbol=%s", symbol)
        start = time.perf_counter()

        try:
            df = fetch_training_data(conn, symbol, ROLLING_WINDOW_DAYS)
            if df.empty:
                skipped_no_data += 1
                logger.warning("TRAIN_SYMBOL_SKIPPED symbol=%s reason=no_training_data", symbol)
                continue

            df["target_realized_vol"] = compute_forward_realized_vol(df["close"], window=FORWARD_WINDOW)
            df = df.dropna(subset=["target_realized_vol", "log_return"] + FEATURE_COLS)

            if len(df) < 100:
                skipped_not_enough += 1
                logger.warning("TRAIN_SYMBOL_SKIPPED symbol=%s reason=insufficient_rows rows=%d", symbol, len(df))
                continue

            garch = GARCHModel()
            garch.fit(df["log_return"].dropna().to_numpy())

            X = df[FEATURE_COLS]
            y = df["target_realized_vol"].to_numpy(dtype=float)
            garch_const = garch.predict(steps=1)
            garch_pred = np.full_like(y, garch_const, dtype=float)
            garch_mse = _mse(y, garch_pred)
            garch_qlike = _qlike(y, garch_pred)

            xgb = None
            xgb_mse = None
            xgb_qlike = None
            model_mode = "garch_only"
            try:
                xgb = XGBVolModel()
                xgb.fit(X, y)
                xgb_pred = xgb.predict(X)
                xgb_mse = _mse(y, xgb_pred)
                xgb_qlike = _qlike(y, xgb_pred)
                model_mode = "garch_xgb"
            except Exception as exc:
                garch_only_symbols += 1
                logger.warning("TRAIN_SYMBOL_XGB_UNAVAILABLE symbol=%s reason=%s", symbol, exc)

            elapsed_ms = (time.perf_counter() - start) * 1000

            models[symbol] = {
                "garch": garch,
                "xgb": xgb,
                "feature_cols": FEATURE_COLS,
                "metrics": {
                    "xgb_mse": xgb_mse,
                    "xgb_qlike": xgb_qlike,
                    "garch_mse": garch_mse,
                    "garch_qlike": garch_qlike,
                    "train_ms": elapsed_ms,
                    "samples": len(df),
                    "mode": model_mode,
                },
            }

            logger.info(
                "trained symbol=%s mode=%s samples=%d train_ms=%.1f xgb_mse=%s xgb_qlike=%s garch_mse=%.6f garch_qlike=%.6f",
                symbol,
                model_mode,
                len(df),
                elapsed_ms,
                f"{xgb_mse:.6f}" if xgb_mse is not None else "NA",
                f"{xgb_qlike:.6f}" if xgb_qlike is not None else "NA",
                garch_mse,
                garch_qlike,
            )
        except Exception as exc:
            failed_symbols += 1
            logger.exception("TRAIN_SYMBOL_FAILED symbol=%s error=%s", symbol, exc)
            continue

    logger.info(
        "TRAINING_SUMMARY total_symbols=%d attempted=%d trained=%d skipped_no_data=%d skipped_insufficient=%d garch_only=%d failed=%d",
        total_symbols,
        attempted,
        len(models),
        skipped_no_data,
        skipped_not_enough,
        garch_only_symbols,
        failed_symbols,
    )

    return models
