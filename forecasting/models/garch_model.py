import logging
import warnings
from typing import Literal

from arch import arch_model
import numpy as np


logger = logging.getLogger("forecasting.garch")


class GARCHModel:
    def __init__(self):
        self.model = None
        self.fitted = None
        self.scale = 1.0
        self.fallback_vol = None

    def fit(self, returns: np.ndarray):
        if returns is None or len(returns) < 20:
            raise ValueError("Not enough return observations to fit GARCH")

        clean = np.asarray(returns, dtype=float)
        clean = clean[np.isfinite(clean)]
        if len(clean) < 20:
            raise ValueError("Not enough finite return observations to fit GARCH")

        clean = clean - np.mean(clean)
        sample_std = float(np.std(clean, ddof=1)) if len(clean) > 1 else 0.0
        if not np.isfinite(sample_std) or sample_std <= 1e-12:
            self.model = None
            self.fitted = None
            self.scale = 1.0
            self.fallback_vol = 0.0
            logger.warning("GARCH fallback activated: near-zero return variance")
            return

        self.scale = 1e4 if sample_std < 1e-4 else 1.0
        scaled = clean * self.scale

        attempts: tuple[Literal["normal"], Literal["t"]] = ("normal", "t")
        last_flag = None
        last_error = None
        for dist in attempts:
            try:
                model = arch_model(
                    scaled,
                    mean='Zero',
                    vol='GARCH',
                    p=1,
                    q=1,
                    dist=dist,
                    rescale=True,
                )
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    fitted = model.fit(disp='off', show_warning=False)

                flag = int(getattr(fitted, "convergence_flag", 0))
                last_flag = flag
                if flag == 0:
                    self.model = model
                    self.fitted = fitted
                    self.fallback_vol = None
                    return

                logger.warning("GARCH non-converged with dist=%s convergence_flag=%d", dist, flag)
            except Exception as exc:
                last_error = exc
                logger.warning("GARCH fit failed with dist=%s error=%s", dist, exc)

        self.model = None
        self.fitted = None
        self.fallback_vol = sample_std
        logger.warning(
            "GARCH fallback activated: using sample std volatility fallback_vol=%.8f convergence_flag=%s error=%s",
            self.fallback_vol,
            str(last_flag),
            str(last_error) if last_error is not None else "none",
        )

    def predict(self, steps=1):
        if self.fitted is None:
            if self.fallback_vol is not None:
                return float(max(self.fallback_vol, 0.0))
            raise RuntimeError("GARCH model not fitted")

        raw_variance = self.fitted.forecast(horizon=steps).variance.iloc[-1, -1]
        variance = float(np.real(np.asarray(raw_variance).item()))
        if variance < 0:
            variance = 0.0
        unscaled_variance = variance / (self.scale ** 2)
        return float(np.sqrt(max(unscaled_variance, 0.0)))
