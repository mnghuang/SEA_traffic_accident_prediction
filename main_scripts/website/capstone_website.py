from flask import Flask, render_template
import cPickle as pickle
from apscheduler.schedulers.background import BackgroundScheduler
import sys
import imp
import os

CWD = os.path.dirname(os.path.abspath(__file__))
TAP = imp.load_source('TrafficAccidentPipeline', CWD+'/../traffic_accident.py')


app = Flask(__name__)

# home page
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/about/')
def about():
    return render_template('about.html')

@app.before_first_request
def initialize():
    apsched = BackgroundScheduler(timezone='PST')
    apsched.start()
    apsched.add_job(pipe.plot_current_result(save_path='static/image.png'), 
                    'interval', minutes=60)

def build_pickle_model():
    pipe = TAP.TrafficAccidentPipeline()
    pipe.load_model_data()
    pipe.build_model()
    return pipe

def load_pickle_model():
    pipe = TAP.TrafficAccidentPipeline()
    pipe.load_model()
    return pipe

def get_pickle_model(command_arg=None):
    if command_arg == 'rebuild':
        pipe = build_pickle_model()
    else:
        pipe = load_pickle_model()
    return pipe

if __name__ == '__main__':
    if len(sys.argv) == 2:
        pipe = get_pickle_model(sys.argv[1])
    else:
        pipe = get_pickle_model()
    app.run(host='0.0.0.0', port=8080, debug=True)
