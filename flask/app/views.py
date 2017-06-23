import os
from flask import render_template, jsonify, request
from app import app
import redis
import json
# from cassandra.cluster import Cluster

# setting up connections to cassandra
# cluster = Cluster([os.environ["CASSANDRA_IP"]])
# session = cluster.connect('playground')

# connect to redis server
redis_server = 'localhost'
redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

@app.route('/')
@app.route('/index') # the template that gets rendered needs to have this same name
# def index():

    # pull sample data from redis server
    # test data is in form of a t-digest

    # sample_list = redis_db.zrange("my.sorted.set", 0, -1, withscores=True)
    # total_string = " ".join(str(x) for x in sample_list)

    # write to a local csv for data visualization

    # do simple hello world

    # extract data from redis
def index():
    # user = { 'nickname': 'Miguel' } # fake userq
    # num_list = [1,2,3,4]
    # read in list of values from redis
    # my_dict_list = [json.loads(x) for x in redis_db.lrange('redis_t_digest_second_example',0,-1)]
    # remove numbers from the json list
    # centroid_locations = [dicty["centroid"] for dicty in my_dict_list]
    # for elem in centroid_locations:
    #     print elem
    # render the page with template
    # return render_template("index.html")
    return render_template("base_bootstrap_template.html")

@app.route('/new_index')
def new_index():
    # can do some query here to pass into the plotting function
    # with render_template call
    return render_template("new_index.html")


if __name__ == "__main__":
    app.run(debug = True, host='localhost', port=8080, passthrough_errors=True)

"""
   user = { 'nickname': 'Miguel' } # fake user
   array = [1,2,3,4]
   return render_template("index.html", title = 'Home', user = user, array=array)
"""



"""
@app.route('/api/<email>/<date>')
def get_email(email, date):
    stmt = "SELECT * FROM email WHERE id=%s and date=%s"
    response = session.execute(stmt, parameters=[email, date])
        response_list = []
        for val in response:
             response_list.append(val)
        jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
    print jsonresponse
        return jsonify(emails=jsonresponse)

@app.route('/email')
def email():
    return render_template("email.html")

@app.route("/email", methods=['POST'])
def email_post():
    emailid = request.form["emailid"]
    date = request.form["date"]
    stmt = "SELECT * FROM email WHERE id=%s and date=%s"
    response = session.execute(stmt, parameters=[emailid, date])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
    print jsonresponse
    return render_template("emailop.html", output=jsonresponse)
"""
