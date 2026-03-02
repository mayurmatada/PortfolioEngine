import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from db import connect, ensure_tables, ensure_backfill_status_table, wait_for_backfill, wait_for_features
from train import train_models
from inference import run_inference
from config import (
    RETRAIN_INTERVAL_HOURS,
    INFERENCE_POLL_SECONDS,
    SYMBOLS,
    TRAINING_EMPTY_RETRY_SECONDS,
    TRAINING_PARTIAL_RETRY_SECONDS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
logger = logging.getLogger("forecasting.main")


def training_loop(models, lock):
    conn = connect()
    cycle = 0
    while True:
        cycle += 1
        started_at = time.perf_counter()
        next_run_at = datetime.now(timezone.utc) + timedelta(hours=RETRAIN_INTERVAL_HOURS)
        should_fast_retry = False
        retry_reason = None
        try:
            logger.info("TRAINING_STARTED cycle=%d", cycle)
            new_models = train_models(conn)
            with lock:
                had_models = bool(models)

            if new_models:
                with lock:
                    models.update(new_models)
                    loaded_symbols = set(models.keys())
                elapsed_s = time.perf_counter() - started_at
                missing_symbols = [s for s in SYMBOLS if s not in loaded_symbols]
                logger.info(
                    "TRAINING_COMPLETED cycle=%d symbols=%d elapsed_s=%.2f next_run_at=%s",
                    cycle,
                    len(new_models),
                    elapsed_s,
                    next_run_at.isoformat(),
                )
                if missing_symbols:
                    should_fast_retry = True
                    retry_reason = "missing_symbols"
                    logger.warning(
                        "TRAINING_PARTIAL cycle=%d loaded=%d/%d missing=%s",
                        cycle,
                        len(loaded_symbols),
                        len(SYMBOLS),
                        ",".join(missing_symbols),
                    )
            else:
                elapsed_s = time.perf_counter() - started_at
                logger.warning(
                    "TRAINING_EMPTY cycle=%d elapsed_s=%.2f retaining_previous_models=true next_run_at=%s",
                    cycle,
                    elapsed_s,
                    next_run_at.isoformat(),
                )
                if not had_models:
                    should_fast_retry = True
                    retry_reason = "no_models_available"
                else:
                    with lock:
                        loaded_symbols = set(models.keys())
                    missing_symbols = [s for s in SYMBOLS if s not in loaded_symbols]
                    if missing_symbols:
                        should_fast_retry = True
                        retry_reason = "missing_symbols"
        except Exception as exc:
            logger.exception("Training loop failed: %s", exc)
            with lock:
                loaded_symbols = set(models.keys())
                should_fast_retry = not loaded_symbols or len(loaded_symbols) < len(SYMBOLS)
                retry_reason = "exception"

        if should_fast_retry:
            if retry_reason == "missing_symbols":
                sleep_s = max(1.0, float(TRAINING_PARTIAL_RETRY_SECONDS))
                logger.warning(
                    "TRAINING_FAST_RETRY cycle=%d reason=missing_symbols retry_in_s=%.1f",
                    cycle,
                    sleep_s,
                )
            else:
                sleep_s = max(1.0, float(TRAINING_EMPTY_RETRY_SECONDS))
                logger.warning(
                    "TRAINING_FAST_RETRY cycle=%d reason=%s retry_in_s=%.1f",
                    cycle,
                    retry_reason or "no_models_available",
                    sleep_s,
                )
        else:
            sleep_s = max(1.0, RETRAIN_INTERVAL_HOURS * 3600 - (time.perf_counter() - started_at))
        logger.info("TRAINING_SLEEP cycle=%d sleep_s=%.1f", cycle, sleep_s)
        time.sleep(sleep_s)


def inference_loop(models, lock):
    conn = connect()
    while True:
        try:
            with lock:
                snapshot = dict(models)

            if not snapshot:
                logger.warning("INFERENCE_WAITING no_trained_models=true")
                time.sleep(INFERENCE_POLL_SECONDS)
                continue

            logger.info("INFERENCE_STARTED symbols=%d", len(snapshot))
            run_inference(conn, snapshot)
            logger.info("INFERENCE_COMPLETED sleep_s=%d", INFERENCE_POLL_SECONDS)
        except Exception as exc:
            logger.exception("Inference loop failed: %s", exc)
        time.sleep(INFERENCE_POLL_SECONDS)


def main():
    logger.info("FORECASTING_SERVICE_START")
    conn = connect()
    ensure_tables(conn)
    ensure_backfill_status_table(conn)
    logger.info("BACKFILL_WAIT_START symbols=%s", ",".join(SYMBOLS))
    wait_for_backfill(conn, SYMBOLS)
    logger.info("BACKFILL_WAIT_COMPLETE symbols=%s", ",".join(SYMBOLS))

    logger.info("FEATURES_WAIT_START symbols=%s min_rows=100", ",".join(SYMBOLS))
    wait_for_features(conn, SYMBOLS, min_rows=100)
    logger.info("FEATURES_WAIT_COMPLETE symbols=%s", ",".join(SYMBOLS))

    models = {}
    lock = threading.Lock()

    logger.info("INITIAL_TRAINING_START")
    initial_models = train_models(conn)
    if initial_models:
        models.update(initial_models)
        logger.info("INITIAL_TRAINING_COMPLETE symbols=%d", len(initial_models))
    else:
        logger.warning("INITIAL_TRAINING_EMPTY no_models_built=true")

    t1 = threading.Thread(target=training_loop, args=(models, lock), daemon=True)
    t2 = threading.Thread(target=inference_loop, args=(models, lock), daemon=True)
    t1.start()
    t2.start()

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
