"""
Kafka producer file.

Produces a simple stream of simulated latency data.
Latency values follow a standard lognormal distribution.

In a more realistic setting the latency values would be
computed from a join operation on some sort of ID,
e.g. a join that gives ad impression time and click-through
time for a given user, or a join that gives time between
a request to and response from a server.

The T-Digest data structure can of course summarize any
1-D distribution of scalar values - latencies are only
used as an example.
"""

from kafka import KafkaProducer
import json
import random
import threading
import time
from numpy.random import lognormal

###############################
# Kafka producer parameters
###############################

topic = 'Latency'
bootstrap_servers = 'localhost:9092'
run_time = 500 # run time in seconds
msgs_per_second = 50 # message per second

###############################
###############################

# compute pause_time between messages and total_num_msgs
pause_time = 1/float(msgs_per_second)
num_messages = run_time*msgs_per_second


class Producer(threading.Thread):

    daemon = True # daemon thread killed after run_time seconds

    def __init__(self, topic, bootstrap_servers, num_messages, pause_time):
        threading.Thread.__init__(self)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.num_messages = num_messages
        self.pause_time = pause_time

    def run(self):

        kafka_producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        devices = ['type1', 'type2', 'type3', 'type4'] # device types

        # send latency messages with device types as keys
        for i in xrange(self.num_messages):
            device = random.choice(devices)
            message = json.dumps({'message_num': i,
                                  'device': device,
                                  'latency': lognormal()})
            kafka_producer.send(self.topic, key=device, value=message)
            time.sleep(self.pause_time) # rest time between messages

            # print message every second to terminal
            if i % int(round(1/float(pause_time))) == 0:
                print message + '\n' + 120*'=' + '\n'


if __name__ == "__main__":

    print "Stream run time:", round(run_time/60.,1), "minutes"
    print "Messages per second:", msgs_per_second
    print "Total number of messages:", num_messages
    print "Pause time:", pause_time

    producer = Producer(topic, bootstrap_servers, num_messages, pause_time)
    producer.start() # calls run method
    time.sleep(run_time)
