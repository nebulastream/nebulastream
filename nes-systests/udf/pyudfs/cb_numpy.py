import numpy as np


def score(a, b):
    weights = np.array([0.5, -0.25])
    z = np.sum(weights * np.array([float(a), float(b)]))
    return float(1.0 / (1.0 + np.exp(-z)))
