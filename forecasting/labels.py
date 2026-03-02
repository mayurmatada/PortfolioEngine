import numpy as np
import pandas as pd


def compute_forward_realized_vol(close: pd.Series, window: int = 5) -> pd.Series:
    """
    Compute 5-min forward realized volatility for each timestamp.
    Returns a pd.Series aligned to close, with NaN for rows where not enough forward data exists.
    """
    logret = np.log(close / close.shift(1))
    squared = np.asarray(logret ** 2)

    result = np.full(len(squared), np.nan, dtype=np.float64)
    for i in range(len(squared) - window):
        forward_sq = squared[i + 1:i + 1 + window]
        if np.isnan(forward_sq).any():
            continue
        result[i] = float(np.sqrt(np.sum(forward_sq)))

    return pd.Series(result, index=close.index, dtype="float64")
