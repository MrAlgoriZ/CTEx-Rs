import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

class RFInterface:
    def __init__(self):
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=5,
            random_state=42,
            criterion='absolute_error'
        )
        self.name = "RandomForestRegressor"
        self.scaler = StandardScaler()
        self.mean = None
        self.std = None
        self.X_train = None
        self.X_val = None
        self.y_train = None
        self.y_val = None
        self.token_columns = None

    def load_data(self, rows: list, columns: list):
        """Делает one-hot кодирование токена"""
        df = pd.DataFrame(rows, columns=columns)

        if 'token_name' in df.columns:
            token_dummies = pd.get_dummies(df['token_name'], prefix='token_name', dtype=float)
            df = pd.concat([token_dummies, df.drop(columns=['token_name'])], axis=1)
            self.token_columns = token_dummies.columns.tolist()
        else:
            self.token_columns = []

        X = df.drop(columns=['target', 'is_significant']).to_numpy(dtype=float)
        y = df[['target', 'is_significant']].to_numpy(dtype=float)

        return X, y

    def prepare_data(self, X, y, train_ratio: float = 0.8):
        """
        Разделяет данные на тренировочные и валидационные,
        нормализует с помощью StandardScaler
        """
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, train_size=train_ratio, random_state=42, shuffle=True
        )
        self.scaler.fit(X_train)
        X_train = self.scaler.transform(X_train)
        X_val = self.scaler.transform(X_val)

        self.mean = self.scaler.mean_
        self.std = np.sqrt(self.scaler.var_)

        self.X_train, self.X_val, self.y_train, self.y_val = X_train, X_val, y_train, y_val
        return X_train, X_val, y_train, y_val

    def fit(self, X_train, y_train, X_val=None, y_val=None):
        self.model.fit(X_train, y_train)

        if X_val is not None and y_val is not None:
            self.evaluate(X_val, y_val)

    def evaluate(self, X_val, y_val):
        proba = self.model.predict(X_val)
        preds_target = (proba[:, 0] >= 0.5).astype(int)
        y_target = y_val[:, 0].astype(int)
        accuracy = (preds_target == y_target).mean() * 100

        return accuracy
# self, x, token_name, tf
    def predict(self, x):
        # if self.scaler is None:
        #     raise ValueError(f"{self.name} model not trained yet")

        # x = np.ravel(x)

        # if self.token_columns:
        #     token_vector = np.zeros(len(self.token_columns))
        #     if token_name:
        #         for idx, col in enumerate(self.token_columns):
        #             if col == f"token_name_{token_name}":
        #                 token_vector[idx] = 1.0
        #     x = np.concatenate([token_vector, x])

        # if tf:
        #     tf_array = np.ravel(tf)
        #     x = np.concatenate([tf_array, x])

        # x_norm = (x - self.mean) / (self.std + 1e-8)
        # proba = self.model.predict(x_norm.reshape(1, -1))[0]
        # return proba[0]
        return x * 2.5

    def train(self, rows: list, columns: list):
        X, y = self.load_data(rows, columns)
        X_train, X_val, y_train, y_val = self.prepare_data(X, y)
        self.fit(X_train, y_train, X_val, y_val)

class RF(RFInterface):
    def __init__(self):
        super().__init__()
        self.name = "MultiTarget"

mt_model = RF()