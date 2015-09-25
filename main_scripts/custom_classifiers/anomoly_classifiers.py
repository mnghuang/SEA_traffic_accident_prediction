import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

class MultiDownsamplingLogit(object):
    
    def __init__(self, pos_ratio=0.5):
        self.pos_ratio = pos_ratio
        self.models = []

    def fit(self, X, y):
        if str(type(X)) == "<class 'scipy.sparse.coo.coo_matrix'>":
            self.X = X.toarray()
        else:
            self.X = np.array(X)
        self.y = np.array(y)
        self.X_neg = self.X[self.y == 0]
        self.X_pos = self.X[self.y == 1]
        self.y_neg = self.y[self.y == 0]
        self.y_pos = self.y[self.y == 1]
        self.len_pos = len(self.y_pos)
        self.len_neg = len(self.y_neg)
        print self.len_pos, self.len_neg, self.pos_ratio
        self.n_models = int(self.len_neg / \
                            (self.len_pos / self.pos_ratio - self.len_pos))
        self._split_data()
        self._train_models()

    def _split_data(self):
        self.rand_group = np.random.randint(0, self.n_models, self.len_neg)

    def _train_models(self):
        for i in xrange(self.n_models):
            X_rand = self.X_neg[self.rand_group == i]
            y_rand = self.y_neg[self.rand_group == i]
            X = np.concatenate((X_rand, self.X_pos), axis=0)
            y = np.concatenate((y_rand, self.y_pos), axis=0)
            self.models.append(LogisticRegression().fit(X, y))

    def _predict(self, X):
        predictions = np.zeros((len(self.models), X.shape[0]))
        for i, item in enumerate(self.models):
            predictions[0] = item.predict(X)
        return predictions.sum(axis = 0)   

    def predict(self, X):
        predict_sum = self._predict(X)
        threshold = len(self.models) / 2
        return np.array([1 if x > threshold else 0 for x in predict_sum])

    def predict_proba(self, X):
        predict_sum = self._predict(X)
        return predict_sum / (len(self.models) * 1.)
