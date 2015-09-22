import pandas as pd
import numpy as np
import psycopg2
import datetime
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from wunderground_api import wunderground_api as wa
from collections import defaultdict


class TrafficAccidentDataTransformer(object):

    def __init__(self, conn, weather_dict, condition_col='conditions',
                                           winddir_col='winddir'):
        self.conn = conn
        self.condition_col = condition_col
        self.winddir_col = winddir_col
        self.weather = weather_dict
        self.query = 'select * from {0};'
        self.time = datetime.datetime.now()
        self.data = None

    def get_mapping_ids(self, zone_table='zone_beat_id',
                              condition_table='condition_id',
                              winddir_table='winddir_id'):
        self.mappings = dict()
        self.zone_table = zone_table
        self.condition_table = condition_table
        self.winddir_table = winddir_table
        tables = [zone_table, condition_table, winddir_table]
        for table in tables:
            self.mappings[table] = pd.read_sql_query(self.query.format(table),
                                                     con=self.conn)

    def get_sport_schedules(self, mariner_table='mariner_plays',
                                  seahawk_table='seahawk_plays'):
        self.sports = dict()
        self.mariner_table = mariner_table
        self.seahawk_table = seahawk_table
        tables = [mariner_table, seahawk_table]
        for table in tables:
            df = pd.read_sql_query(self.query.format(table), con=self.conn)
            self.sports[table] = set(df['event_date'])

    def transform(self):
        self.data = dict()
        zones = list(self.mappings[self.zone_table]['id'].values)
        self.data[self.zone_table] = zones
        rows = len(self.data[self.zone_table])
        self._add_hour(rows)
        self._add_mappings(self.condition_col, self.condition_table, rows)
        self._add_mappings(self.winddir_col, self.winddir_table, rows)
        self._add_sports(self.mariner_table, rows)
        self._add_sports(self.seahawk_table, rows)
        self._add_weather(rows)

    def get_data(self):
        zones = list(self.mappings[self.zone_table]['category'].values)
        return pd.DataFrame(self.data).sort(axis=1), zones

    def _add_hour(self, number):
        self.data['hour'] = [self.time.hour] * number
        
    def _add_mappings(self, col, table, number):
        df = self.mappings[table]
        data = [df[df['category'] == self.weather[col]]['id'].values[0]]
        self.data[table] = data * number

    def _add_sports(self, table, number):
        df = self.sports[table]
        current_date = self.time.date()
        self.data[table] = [int(current_date in df)] * number

    def _add_weather(self, number):
        for k, v in self.weather.iteritems():
            if k != self.condition_col and k != self.winddir_col:
                if v == 'NA':
                    value = 0
                else:
                    value = v
                self.data[k] = [float(str(value).replace('%', ''))] * number


class TrafficAccidentModel(object):

    def __init__(self, X, y, cat_features=[0, 12, 14], method='logit'):
        self.X = X
        self.y = y
        self.cat_features = cat_features
        self.method = method
        if self.method == 'logit':
            self._build_logit_model()
    
    def _build_logit_model(self):
        self.encoder = OneHotEncoder(categorical_features=self.cat_features)
        mat = self.encoder.fit_transform(self.X)
        self.model = LogisticRegression(class_weight='auto').fit(mat, self.y)

    def _build_mutli_downsample_model(self):
        pass

    def _build_isolation_tree(self):
        pass

    def predict(self, X):
        mat = self.encoder.transform(X)
        return self.model.predict(mat)

    def predict_proba(self, X):
        mat = self.encoder.transform(X)
        return self.model.predict_proba(mat)


class TrafficAccidentPipeline(object):

    def __init__(self, dbname='traffic_accident', user='minghuang', 
                 host='localhost'):
        self.conn = psycopg2.connect(dbname=dbname, user=user, host=host)

    def load_model_data(self, table='compiled_data', label_name='label'):
        query = 'select * from {0};'.format(table)
        self.df = pd.read_sql_query(query, con=self.conn)
        self.label_name = label_name

    def get_live_data(self):
        api = wa.WundergroundAPI()
        weather = api.get_current_condition()
        tadt = TrafficAccidentDataTransformer(self.conn, weather)
        tadt.get_mapping_ids()
        tadt.get_sport_schedules()
        tadt.transform()
        return tadt.get_data()

    def build_model(self, clear_memory=True):
        df = self.df.copy()
        self.y = df.pop(self.label_name).values
        self.X = df.sort(axis=1).values
        if clear_memory:
            self.df = None
        self.model = TrafficAccidentModel(self.X, self.y)

    def get_predictions(self, X):
        return self.model.predict(X)

    def get_probabilities(self, X):
        return self.model.predict_proba(X)

    def get_current_result(self, predict_type='probabilities'):
        df, districts = self.get_live_data()
        if predict_type == 'probabilities':
            result = list(self.get_probabilities(df))
        elif predict_type == 'booleans':
            result = list(self.get_predictions(df))
        return districts, [x[1] for x in result]

    def close(self):
        self.conn.close()
