import pandas as pd
import numpy as np
import psycopg2
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from wunderground_api import wunderground_api as wa


class TrafficAccidentDataTransformer(object):

    def __init__(self, conn, weather_dict):
        self.conn = conn
        self.weather = weather_dict
        self.query = 'select * from {0};'

    def get_mapping_ids(self, tables=['zone_beat_id', 'condition_id',
                                         'winddir_id']):
        self.mappings = dict()
        for table in tables:
            self.mappings[table] = pd.read_sql_query(self.query.format(table),
                                                    con=self.conn)

    def get_sport_schedules(self, tables=['mariner_plays', 'seahawk_plays']):
        self.sports = dict()
        for table in tables:
            df = pd.read_sql_query(self.query.format(table), con=self.conn)
            self.sports[table] = set(df['event_date'])

    def transform(self, district_table='zone_beat_id'):
        for i in self.mappings[district_table]['id']:
            pass


class TrafficAccidentModel(object):

    def __init__(self, X, y, cat_features=[0, 1, 2], method='logit'):
        self.X = X
        self.y = y
        self.cat_features = cat_features
        self.method = method
        if self.method == 'logit':
            self._build_logit_model()
    
    def _build_logit_model(self):
        self.n = 1
        self._set_encoder(1)
        mat = self.encoder.fit_transform(self.X)
        self.model = LogisticRegression(class_weight='auto').fit(mat, y)

    def _build_mutli_downsample_model(self):
        pass

    def _build_isolation_tree(self):
        pass

    def predict(self, X):
        if self.n == 1
            self._predict_single(X)
        else:
            self._predict_multiple(X)

    def _predict_single(self, X):
        mat = self.encoder.transform(X)
        return self.model.predict(mat)

    def _predict_multiple(self, X):
        pass

    def _set_encoder(self, n=1):
        if n == 1:
            self.encoder = OneHotEncoder(categorical_features=cat_features)
        else:
            self.encoder = 'foobar'


class TrafficAccidentPipeline(object):

    def __init__(self, dbname='traffic_accident', user='minghuang', 
                 host='localhost'):
        self.conn = psycopg2.connect(dbname=dbname, user=user, host=host)

    def load_model_data(self, table='compiled_data', label_name='label'):
        query = 'select * from {0};'.format(table)
        self.df = pd.read_sql_query(query, con=self.conn)
        self.label_name = 'label'

    def load_live_data(self):
        api = wa.WundergroundAPI()
        weather = api.get_current_condition()

    def build_model(self, clear_memory=True):
        df = self.df.copy()
        y = df.pop(self.label_name).values
        X = df.values
        if clear_memory():
            self.df = None
        self.model = TrafficAccidentModel(X, y)

    def close(self):
        self.conn.close()
