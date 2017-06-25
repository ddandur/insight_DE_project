from kafka import KafkaProducer
import time
import threading
# import logging
import json
import random

from numpy.random import lognormal, uniform


class Producer(threading.Thread):

    # daemon threads are summarily killed as soon as main program ends
    daemon = True

    def run(topic):
        topic = "NASA-logs" # change this later - these monitoring logs are mainly about latencies
        kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')

        # devices that latencies come from
        devices = ['type1', 'type2', 'type3', 'type4']

        # send messages
        for i in xrange(1000000):
            # latencies should be draw from lognormal
            # for testing use one number and then uniform
            message = json.dumps({'device': random.choice(devices), 'latency': 2})
            # message = """129.94.144.152 - - [01/Jul/1995:00:00:17 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0 {}""".format(i)
            kafka_producer.send(topic, message)
            time.sleep(.1)
            print message + '\n' + 120*'=' + '\n'

if __name__ == "__main__":
    # logging - might turn this on later
    """
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    """
    producer = Producer() # thread instance
    producer.start() # calls run method
    time.sleep(500) # how many seconds to let producer thread run, since
                   # daemon producer thread is killed once main program ends
