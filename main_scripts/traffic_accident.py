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
WA = imp.load_source('WundergroundAPI', CWD + '/wunderground_api.py')
CHP = imp.load_source('PlotSeattleChoropleth', CWD + '/seattle_choropleth.py')
KL = imp.load_source('KmeansWeightedLogit', CWD + '/custom_classifier.py')
IMGPATH = CWD + '/website/static/image.png'
SHAPEFILE_PATH = CWD + '/../data/shapefiles/Precincts/WGS84/geo_yutr-ryap-1'

class TrafficAccidentDataTransformer(object):

    def __init__(self, mappings, sports, mapping_tables, sport_tables,
                 condition_col='conditions', winddir_col='winddir'):
        self.mappings = mappings
        self.sports = sports
        self.zone_table = mapping_tables['zone']
        self.condition_table = mapping_tables['condition']
        self.winddir_table = mapping_tables['winddir']
        self.mariner_table = sport_tables['mariner']
        self.seahawk_table = sport_tables['seahawk']
        self.condition_col = condition_col
        self.winddir_col = winddir_col
        self.time = datetime.datetime.now()
        self.data = None

    def fit(self, weather_dict):
        self.weather = weather_dict

    def transform(self):
        self.data = dict()
        zones = self.mappings[self.zone_table]['id'].values
        intersections = self.mappings[self.zone_table]['intersections'].values
        self.data[self.zone_table] = zones
        self.data['intersections'] = intersections
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
            if k in ['gustspeed', 'heatindex', 'precip']:
                col_name = 'have_{0}'.format(k)
                self.data[col_name] = self._check_exist_binary(v)
            elif k != self.condition_col and k != self.winddir_col:
                if v == 'NA':
                    value = 0
                else:
                    value = v
                self.data[k] = [float(str(value).replace('%', ''))] * number

    def _check_exist_binary(self, value):
        if value == 'NA' or value == '-':
            return 0
        else:
            return 1


class TrafficAccidentModel(object):

    def __init__(self, X, y, k=8, cat_features=[0, 2, 6, 10, 15, 17],
                 method='logit'):
        self.X = X
        self.y = y
        self.k = k
        self.cat_features = cat_features
        self.method = method
        if self.method == 'logit':
            self._build_logit_model()
        elif self.method == 'random_forest':
            self._build_random_forest()
        elif self.method == 'kmeans_logit':
            self._build_kmeans_logit()
    
    def _build_logit_model(self):
        self.encoder = OneHotEncoder(categorical_features=self.cat_features)
        mat = self.encoder.fit_transform(self.X)
        self.model = LogisticRegression(class_weight='auto').fit(mat, self.y)

    def _build_kmeans_logit(self):
        self.model = KL.KmeansWeightedLogit(k=self.k).fit(self.X, self.y)

    def _build_random_forest(self):
        self.model = RandomForestClassifier(class_weight='auto',
                                            n_estimators=100)
        self.model.fit(self.X, self.y)

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

    def build_transformer(self, mapping_tables={'zone': 'zone_beat_id',
                                                'condition': 'condition_id',
                                                'winddir': 'winddir_id'},
                                sport_tables={'mariner': 'mariner_plays',
                                              'seahawk': 'seahawk_plays'}):
        query = 'select * from {0};'
        self.mapping_tables = mapping_tables
        self.sport_tables = sport_tables
        mappings = self._get_mappings(query, mapping_tables)
        sports = self._get_sports(query, sport_tables)
        self.tadt = TrafficAccidentDataTransformer(mappings, sports,
                                                   mapping_tables, sport_tables)
        self._create_pickle(self.tadt, CWD+'/website/data/transformer.pkl')

    def load_transformer(self):
        self.tadt = self._load_pickle(CWD+'/website/data/transformer.pkl')

    def _get_mappings(self, query, mapping_tables):
        mappings = dict()
        for table in mapping_tables.values():
            mappings[table] = pd.read_sql_query(query.format(table),
                                                con=self.conn)
        return mappings

    def _get_sports(self, query, sport_tables):
        sports = dict()
        for table in sport_tables.values():
            df = pd.read_sql_query(query.format(table), con=self.conn)
            sports[table] = set(df['event_date'])
        return sports

    def load_model_data(self, table='model_data', label_name='label',
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
        self.update_time = str(datetime.datetime.now())
        self.tadt.fit(self.weather)
        self.tadt.transform()
        return self.tadt.get_data()

    def build_model(self, pickle_path=CWD+'/website/data/model.pkl'):
        df = self.df.copy()
        self.y = df.pop(self.label_name).values
        self.X = df.sort(axis=1).values
        self.model = TrafficAccidentModel(self.X, self.y,
                                          method='kmeans_logit')
        self._create_pickle(self.model, pickle_path)

    def load_model(self, pickle_path=CWD+'/website/data/model.pkl'):
        self.model = self._load_pickle(pickle_path)

    def _create_pickle(self, object_name, pickle_path):
        if pickle_path is not None:
            with open(pickle_path, 'w') as f:
                pickle.dump(object_name, f)

    def _load_pickle(self, pickle_path):
        with open(pickle_path) as f:
            return pickle.load(f)

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
                            save_path=IMGPATH):
        beats, values = self.get_current_result(predict_type)
        #bins = [0., 0.1, 0.3, 0.5]
        #bin_labels = ['Mostly Safe', 'Low Risk', 'Medium Risk', 'High Risk']
        df = pd.DataFrame({'zone_beat': beats, 'values': values})
        psc = CHP.PlotSeattleChoropleth(SHAPEFILE_PATH, 'Name')
        psc.fit_data(df)
        #psc.plot_map(title_name=title_name, bins=bins, bin_labels=bin_labels,
        #             save_path=save_path)
        psc.plot_map(title_name=title_name, save_path=save_path)

    def close(self):
        self.conn.close()
