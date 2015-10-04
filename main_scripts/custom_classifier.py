import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler

class KmeansWeightedLogit(object):
    
    def __init__(self, k=8, categorical_features=[0, 2, 6, 10, 15, 17]):
        self.k = k
        self.cat_feats = categorical_features

    def fit(self, X, y):
        if str(type(X)) == "<class 'scipy.sparse.coo.coo_matrix'>":
            self.X = X.toarray()
        else:
            self.X = np.array(X)
        self.y = y
        self.encoder = OneHotEncoder(categorical_features=self.cat_feats)
        self.scaler = StandardScaler()
        self.X_encode = self.encoder.fit_transform(self.X)
        self.X_scale = self.scaler.fit_transform(self.X_encode.toarray())
        kcluster = KMeans(n_clusters=self.k)
        clusters = kcluster.fit_predict(self.X_scale)
        self._build_logits(clusters, self.X_scale, self.y)
        self._build_weights(clusters)
        return self

    def _build_logits(self, clusters, X, y):
        self.models = []
        for i in xrange(self.k):
            temp_X = X[clusters == i]
            temp_y = y[clusters == i]
            logit = LogisticRegression(class_weight='auto').fit(temp_X, temp_y)
            self.models.append(logit)

    def _build_weights(self, clusters):
        sizes = []
        for i in xrange(self.k):
            sizes.append(len(self.y[clusters == i]))
        inverse_sizes = 1. / np.array(sizes)
        weights = 1. * np.array(inverse_sizes) / sum(inverse_sizes)
        self.weights = weights.reshape((self.k, 1))

    def predict(self, X):
        X_encode = self.encoder.transform(X)
        X_scale = self.scaler.transform(X_encode.toarray())
        pred= []
        for i in xrange(self.k):
            pred.append(self.models[i].predict(X_scale))
        agg_pred = np.array(pred).sum(axis=0)
        return np.array([1 if i else 0 for i in agg_pred > self.k / 2.])

    def predict_proba(self, X):
        X_encode = self.encoder.transform(X)
        X_scale = self.scaler.transform(X_encode.toarray())
        pred= []
        for i in xrange(self.k):
            pred.append(self.models[i].predict_proba(X_scale))
        new_pred = None
        for i, w in enumerate(self.weights):
            if new_pred is None:
                new_pred = w * pred[i]
            else:
                new_pred = new_pred + w * pred[i]
        return new_pred
