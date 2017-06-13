"""
some small functions for experimenting - this is copied over from a
jupyter notebook and isn't mean to be run as a .py file

also a good conversation piece for code review
"""

import time
from random import randint

producer_timings = {}
consumer_timings = {}

# experiment with python-kafka
from kafka import KafkaProducer
from kafka import KafkaConsumer

# make a producer
producer = KafkaProducer()
topic = 'python-kafka-topic'
# send message to topic
producer.send(topic, "SENT FROM IPYTHON")
producer.send(topic, "SECOND SENT FROM IPYTHON")

# can find the servers in terminal
bootstrap_servers = 'localhost:2181'

# create consumer object
# use the consumer_timeout_ms option to avoid having cell hang if it isn't collecting any more
consumer = KafkaConsumer(
        # bootstrap_servers=bootstrap_servers,
        auto_offset_reset = 'earliest', # start at earliest topic
        group_id = None, # do no offest commit,
        consumer_timeout_ms=1000
    )

# set consumer to consume topic
consumer.subscribe([topic])

# function to send ~100 records similar to NASA data to kafka broker

def send_simple_logs(msg_count = 100, topic="default_topic"):
    """ Make simple log producer - for now don't worry about good-looking logs
    """
    # create producer
    producer = KafkaProducer()
    # send msg_count messages to topic
    for i in xrange(msg_count):
        producer.send(topic, "SENT MESSAGE {} FROM IPYTHON".format(i))



######################################################################
# Functions related to producing NASA logs
######################################################################


def send_nasa_logs(msg_count=100, topic="default_topic"):
    """ Send logs in the form of the NASA log dataset

    Log format:

    129.94.144.152 - - [01/Jul/1995:00:00:17 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0

    Want to produce random IP address from list of IPs (so that there will be many duplicates) and
    web address requests from random list of requests.

    """

    log = produce_log()

def produce_log():
    # produce random log file to be sent to kafka stream
    # this functions calls functions below and combines their outputs to make a log

    # the user info is blanked out as "-"

    ip = produce_ip()
    date_time = produce_date_time()
    request = produce_request()
    response = produce_response()
    bytes = produce_bytes()

    log = " ".join([ip, "-", "-", date_time, request, response, bytes])

    return log

def produce_ip():
    # produced from random ip producer - each of 4 parts of ip produced randomly
    # each of four sections of ip is random number from 0 to 255
    return ".".join([str(randint(0,255)) for _ in range(4)])

def produce_date_time():
    # use some kind of poisson process here to produce times with realistic distribution
    # for now just make them come every second
    pass

def produce_request():
    # drawn from random list of requests - request is for specific subpage of website
    return """"GET /images/ksclogo-medium.gif HTTP/1.0""""

def produce_response():
    # return a positive response for now
    return "200"

def produce_bytes(request, http_response):
    # uses dictionary to look up size of returned object based on request and response
    return "47122"
