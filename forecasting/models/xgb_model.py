import xgboost as xgb


class XGBVolModel:
    def __init__(self):
        self.model = xgb.XGBRegressor(n_estimators=200, max_depth=4, learning_rate=0.05, objective='reg:squarederror')
        self.fitted = False

    def fit(self, X, y):
        self.model.fit(X, y)
        self.fitted = True

    def predict(self, X):
        if not self.fitted:
            raise RuntimeError("XGBoost model not fitted")
        return self.model.predict(X)
