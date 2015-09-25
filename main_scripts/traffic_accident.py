import pandas as pd
import numpy as np
import psycopg2
import datetime
import cPickle as pickle
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from collections import defaultdict
import imp
import os

CWD = os.path.dirname(os.path.abspath(__file__))
WA = imp.load_source('WundergroundAPI',
                     CWD + '/wunderground_api/wunderground_api.py')
CHP = imp.load_source('PlotSeattleChoropleth',
                      CWD + '/plot_seattle_choropleth.py')


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
        self._add_time(rows)
        self._add_mappings(self.condition_col, self.condition_table, rows)
        self._add_mappings(self.winddir_col, self.winddir_table, rows)
        self._add_sports(self.mariner_table, rows)
        self._add_sports(self.seahawk_table, rows)
        self._add_weather(rows)

    def get_data(self):
        zones = list(self.mappings[self.zone_table]['category'].values)
        return pd.DataFrame(self.data).sort(axis=1), zones

    def _add_time(self, number):
        self.data['hour'] = [self.time.hour] * number
        dow = self.time.weekday()
        if dow == 6:
            dow = 0
        else:
            dow += 1
        self.data['dow'] = [dow] * number
        self.data['month'] = [self.time.month] * number
        
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

    def __init__(self, X, y, cat_features=[0, 2, 5, 8, 14, 16],
                 method='logit'):
        self.X = X
        self.y = y
        self.cat_features = cat_features
        self.method = method
        if self.method == 'logit':
            self._build_logit_model()
        elif self.method == 'random_forest':
            self._build_random_forest()
    
    def _build_logit_model(self):
        self.encoder = OneHotEncoder(categorical_features=self.cat_features)
        mat = self.encoder.fit_transform(self.X)
        self.model = LogisticRegression(class_weight='auto').fit(mat, self.y)
        #self._create_bins(mat)

    def _build_mutli_downsample_model(self):
        pass

    def _build_random_forest(self):
        #X_train, X_test, y_train, y_test = train_test_split(self.X, self.y,
        #                                                    test_size=0.1,
        #                                                    random_state=2345)
        self.model = RandomForestClassifier(class_weight='auto',
                                            n_estimators=100)
        self.model.fit(self.X, self.y)
        #self.model.fit(X_train, y_train)
        #self._create_bins(X_test)

    def _create_bins(self, X, bins=3):
        proba = pd.Series([x[1] for x in self.model.predict_proba(self.X)])
        frac = 1. * max(proba) / bins
        #bins = pd.cut(proba, bins=bins).unique()
        #bins = [bin.strip('(').strip(']').split(',') for bin in bins]
        self.bins = [0.] + [i * frac for i in xrange(1, bins)] + [1.]

    def get_bins(self):
        return self.bins

    def predict(self, X):
        if self.method == 'logit':
            mat = self.encoder.transform(X)
        else:
            mat = X
        return self.model.predict(mat)

    def predict_proba(self, X):
        if self.method == 'logit':
            mat = self.encoder.transform(X)
        else:
            mat = X
        return self.model.predict_proba(mat)


class TrafficAccidentPipeline(object):

    def __init__(self, dbname='traffic_accident', user='minghuang', 
                 host='localhost'):
        self.conn = psycopg2.connect(dbname=dbname, user=user, host=host)
        self.pickle_path = CWD + '/website/data/model.pkl'

    def load_model_data(self, table='compiled_data', label_name='label',
                        limit=None):
        if limit is None:
            limit = ''
        else:
            limit = 'limit {0}'.format(limit)
        query = 'select * from {0} {1};'.format(table, limit)
        self.df = pd.read_sql_query(query, con=self.conn)
        self.label_name = label_name

    def get_model_data(self):
        return self.df

    def get_live_data(self):
        api = WA.WundergroundAPI()
        self.weather = api.get_current_condition()
        tadt = TrafficAccidentDataTransformer(self.conn, self.weather)
        tadt.get_mapping_ids()
        tadt.get_sport_schedules()
        tadt.transform()
        return tadt.get_data()

    def build_model(self, pickle_path=CWD + '/website/data/model.pkl'):
        df = self.df.copy()
        self.y = df.pop(self.label_name).values
        self.X = df.sort(axis=1).values
        self.model = TrafficAccidentModel(self.X, self.y, 
                                          method='random_forest')
        if pickle_path is not None:
            with open(pickle_path, 'w') as f:
                pickle.dump(self.model, f)

    def load_model(self, pickle_path=CWD+'/website/data/model.pkl'):
        with open(pickle_path) as f:
            self.model = pickle.load(f)

    def get_predictions(self, X):
        return self.model.predict(X)

    def get_probabilities(self, X):
        return self.model.predict_proba(X)

    def get_current_result(self, predict_type='proba'):
        df, districts = self.get_live_data()
        if predict_type == 'proba':
            result = list(self.get_probabilities(df))
        elif predict_type == 'booleans':
            result = list(self.get_predictions(df))
        return districts, [x[1] for x in result]

    def plot_current_result(self, title_name=None, predict_type='proba',
                            save_path=CWD+'/website/static/image.png'):
        beats, values = self.get_current_result(predict_type)
        #bins = self.model.get_bins()
        bins = [0., 0.1, 0.3, 0.5]
        bin_labels = ['Mostly Safe', 'Low Risk', 'Medium Risk', 'High Risk']
        df = pd.DataFrame({'zone_beat': beats, 'values': values})
        psc = CHP.PlotSeattleChoropleth(CWD+'/shapefiles/Precincts/WGS84/geo_yutr-ryap-1',
                                        'Name')
        psc.fit_data(df)
        #title_name = 'Traffic Accident Probabilities in Seattle Precincts'
        psc.plot_map(title_name=title_name, bins=bins, bin_labels=bin_labels,
                     save_path=save_path)
        #self.psc = psc

    def close(self):
        self.conn.close()
