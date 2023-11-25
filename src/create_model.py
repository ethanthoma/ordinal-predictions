from dataclasses import dataclass
import mord as m
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


@dataclass
class OrderedLogisticRegressionParams:
    X: pd.DataFrame
    y: pd.DataFrame

def ordered_logistic_regression(params: OrderedLogisticRegressionParams):
    model = m.LogisticAT()
    model.fit(params.X, params.y)
    return model

