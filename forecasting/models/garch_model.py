from arch import arch_model
import numpy as np


class GARCHModel:
    def __init__(self):
        self.model = None
        self.fitted = None

    def fit(self, returns: np.ndarray):
        if returns is None or len(returns) < 20:
            raise ValueError("Not enough return observations to fit GARCH")
        self.model = arch_model(returns, vol='GARCH', p=1, q=1, rescale=False)
        self.fitted = self.model.fit(disp='off')

    def predict(self, steps=1):
        if self.fitted is None:
            raise RuntimeError("GARCH model not fitted")
        raw_variance = self.fitted.forecast(horizon=steps).variance.iloc[-1, -1]
        variance = float(np.real(np.asarray(raw_variance).item()))
        if variance < 0:
            variance = 0.0
        return float(np.sqrt(variance))
