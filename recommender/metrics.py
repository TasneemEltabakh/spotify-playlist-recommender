import numpy as np


def precision_at_k(recommended, relevant, k):
    recommended_k = recommended[:k]
    if len(recommended_k) == 0:
        return 0.0
    return len(set(recommended_k) & set(relevant)) / float(k)
