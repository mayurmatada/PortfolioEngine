import numpy as np
import pandas as pd

from config import ROLLING_WINDOWS


# ── primitives ────────────────────────────────────────────────────────────────

def log_returns(close: pd.Series) -> pd.Series:
    """r_t = log(P_t / P_{t-1})"""
    return pd.Series(np.log(close / close.shift(1)))


def realized_vol(returns: pd.Series, window: int) -> pd.Series:
    """RV = sqrt( rolling sum of squared returns )."""
    return (returns ** 2).rolling(window=window).sum().apply(np.sqrt)


def parkinson_vol(high: pd.Series, low: pd.Series, window: int) -> pd.Series:
    """Parkinson estimator using high-low range."""
    log_hl_sq = pd.Series((np.log(high / low)) ** 2)
    return np.sqrt(log_hl_sq.rolling(window=window).mean() / (4 * np.log(2)))


# ── public API ────────────────────────────────────────────────────────────────

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute all features from an OHLCV DataFrame (DatetimeIndex).

    Returns a DataFrame aligned to the same index.  Only the *last* row is
    guaranteed to have all rolling windows populated (caller should use that).
    """
    feats = pd.DataFrame(index=df.index)

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    lr = log_returns(close)
    feats["log_return"] = lr
    feats["squared_return"] = lr ** 2
    feats["volume_roc"] = volume.pct_change()
    feats["hl_range"] = (high - low) / close

    for w in ROLLING_WINDOWS:
        feats[f"ret_mean_{w}"] = lr.rolling(window=w).mean()
        feats[f"ret_std_{w}"] = lr.rolling(window=w).std()
        feats[f"realized_vol_{w}"] = realized_vol(lr, w)
        feats[f"parkinson_vol_{w}"] = parkinson_vol(high, low, w)
        feats[f"ret_skew_{w}"] = lr.rolling(window=w).skew()
        feats[f"ret_kurt_{w}"] = lr.rolling(window=w).kurt()
        feats[f"volume_mean_{w}"] = volume.rolling(window=w).mean()

    return feats


def latest_feature_row(df: pd.DataFrame) -> dict:
    """Convenience: compute features and return the last row as a dict.

    NaN values are converted to None so psycopg stores SQL NULL.
    """
    feats = compute_features(df)
    row = feats.iloc[-1].to_dict()
    return {k: (None if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)) for k, v in row.items()}
