import os
from flask import render_template, jsonify, request
from app import app
from cassandra.cluster import Cluster

# setting up connections to cassandra
# cluster = Cluster([os.environ["CASSANDRA_IP"]])
# session = cluster.connect('playground')

# connect to redis server
redis_server = 'localhost'
redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

@app.route('/')
@app.route('/index')
def index():

    # pull sample data from redis server
    return "Hello, world!"

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
