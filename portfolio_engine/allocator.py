import math

import numpy as np
import pandas as pd


def _project_weights(weights: np.ndarray, cap: float, max_iter: int = 200) -> np.ndarray:
    n = len(weights)
    lower = np.zeros(n)
    upper = np.full(n, cap)
    v = np.clip(weights.astype(float), lower, upper)

    if np.isclose(v.sum(), 1.0):
        return v

    for _ in range(max_iter):
        diff = 1.0 - v.sum()
        if abs(diff) < 1e-10:
            break

        free = (v > lower + 1e-12) & (v < upper - 1e-12)
        count = int(np.sum(free))
        if count == 0:
            break
        v[free] += diff / count
        v = np.clip(v, lower, upper)

    total = v.sum()
    if total <= 0:
        return np.full(n, 1.0 / n)
    return v / total


def _to_return_matrix(price_df: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame(columns=symbols)

    pivot = price_df.pivot(index="time", columns="symbol", values="close").sort_index()
    pivot = pivot.reindex(columns=symbols)
    returns = pd.DataFrame(np.log(pivot / pivot.shift(1)), index=pivot.index, columns=pivot.columns)
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna(how="all")
    returns = returns.dropna(axis=1, how="all")
    return returns


def estimate_mu(short_price_df: pd.DataFrame, symbols: list[str]) -> np.ndarray:
    returns = _to_return_matrix(short_price_df, symbols)
    if returns.empty:
        return np.zeros(len(symbols), dtype=float)
    mu = returns.mean(axis=0)
    return np.array([float(mu.get(s, 0.0)) for s in symbols], dtype=float)


def estimate_covariance(price_df: pd.DataFrame, symbols: list[str], shrinkage_lambda: float) -> np.ndarray:
    returns = _to_return_matrix(price_df, symbols)
    if returns.empty or len(returns) < 2:
        return np.eye(len(symbols), dtype=float) * max(shrinkage_lambda, 1e-6)

    returns = returns.fillna(0.0)
    cov_df = returns.cov()
    cov = np.zeros((len(symbols), len(symbols)), dtype=float)
    for i, si in enumerate(symbols):
        for j, sj in enumerate(symbols):
            cov[i, j] = float(cov_df.get(si, pd.Series()).get(sj, 0.0))

    cov += np.eye(len(symbols), dtype=float) * max(shrinkage_lambda, 1e-12)
    return cov


def inverse_vol_weights(vols: np.ndarray, cap: float) -> np.ndarray:
    safe = np.where(vols > 1e-12, vols, 1e-12)
    inv = 1.0 / safe
    raw = inv / inv.sum() if inv.sum() > 0 else np.full_like(inv, 1.0 / len(inv))
    return _project_weights(raw, cap)


def optimize_sharpe_proxy(
    mu: np.ndarray,
    cov: np.ndarray,
    cap: float,
    max_iters: int,
    step_size: float,
) -> np.ndarray:
    n = len(mu)
    if n == 0:
        return np.array([])

    weights = np.full(n, 1.0 / n, dtype=float)

    for _ in range(max_iters):
        cov_w = cov @ weights
        variance = float(weights @ cov_w)
        variance = max(variance, 1e-16)
        sigma = math.sqrt(variance)
        numerator = float(mu @ weights)

        grad = (mu / sigma) - ((numerator / (sigma ** 3)) * cov_w)
        candidate = weights + step_size * grad
        weights = _project_weights(candidate, cap)

    return weights


def portfolio_volatility(weights: np.ndarray, cov: np.ndarray) -> float:
    var = float(weights @ (cov @ weights))
    return float(math.sqrt(max(var, 0.0)))


def sharpe_proxy(weights: np.ndarray, mu: np.ndarray, cov: np.ndarray) -> float:
    vol = portfolio_volatility(weights, cov)
    if vol <= 1e-12:
        return 0.0
    return float((mu @ weights) / vol)


def choose_target_vols(forecasts: dict, symbols: list[str]) -> np.ndarray:
    vols = []
    for symbol in symbols:
        row = forecasts.get(symbol) or {}
        garch = row.get("garch_vol")
        xgb = row.get("xgb_vol")

        if garch is not None and xgb is not None:
            vols.append(float((garch + xgb) / 2.0))
        elif xgb is not None:
            vols.append(float(xgb))
        elif garch is not None:
            vols.append(float(garch))
        else:
            vols.append(0.0)
    return np.array(vols, dtype=float)
