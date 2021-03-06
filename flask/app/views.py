"""
Flask app.

Reads t-digest percentile data from redis and displays in
graphical form using Highcharts.

To start flask app, go to Flask directory and enter:

sudo -E python tornadoapp.py
"""

from flask import render_template
from app import app
import redis
import ast

# connect to redis server
redis_server = 'localhost'
redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

@app.route('/')
@app.route('/index')
def index():
    # collect percentile data from redis
    string_data = redis_db.get("current_digest")
    data = ast.literal_eval(string_data)

    return render_template("real_time_digest.html", digest=data)

@app.route('/compare')
def compare():
    # static page that compares pre-computed t-digest information
    return render_template("compare_digest.html")

if __name__ == "__main__":
    app.run(debug = True, host='localhost', port=8080, passthrough_errors=True)
