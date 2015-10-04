from flask import Flask, render_template
import cPickle as pickle
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import datetime
import sys
import imp
import os

CWD = os.path.dirname(os.path.abspath(__file__))
TAP = imp.load_source('TrafficAccidentPipeline', CWD+'/../traffic_accident.py')
WEATHER = None
UPDATE = None

app = Flask(__name__)

def update_result():
    global WEATHER, UPDATE
    pipe.plot_current_result(save_path=CWD+'/static/image.png')
    WEATHER = pipe.weather
    UPDATE = 'Last updated on {0}.'.format(pipe.update_time)

def dict_to_html(d):
    data = dict()
    for k, v in d.iteritems():
        data[k] = [v]
    return pd.DataFrame(data).to_html()

def build_pickle_model():
    pipe = TAP.TrafficAccidentPipeline()
    pipe.load_model_data(limit=1500000)
    pipe.build_model()
    pipe.build_transformer()
    return pipe

def load_pickle_model():
    pipe = TAP.TrafficAccidentPipeline()
    pipe.load_model()
    pipe.load_transformer()
    return pipe

def get_pickle_model(command_arg=None):
    if command_arg == 'rebuild':
        pipe = build_pickle_model()
    else:
        pipe = load_pickle_model()
    return pipe

# home page
@app.route('/')
def index():
    return render_template('index.html', update=UPDATE, weather=WEATHER)

@app.route('/about/')
def about():
    return render_template('about.html')


if __name__ == '__main__':
    if len(sys.argv) == 2:
        pipe = get_pickle_model(sys.argv[1])
    else:
        pipe = get_pickle_model()
    update_result()
    apsched = BackgroundScheduler()
    apsched.start()
    apsched.add_job('__main__:update_result', 'interval', minutes=60)
    app.run(host='0.0.0.0', port=8080, debug=True)
