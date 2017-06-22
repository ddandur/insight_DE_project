from kafka import KafkaProducer
import time
import threading
# import logging
from numpy.random import lognormal


class Producer(threading.Thread):

    # daemon threads are summarily killed as soon as main program ends
    daemon = True

    def run(topic):
        topic = "NASA-logs" # change this later - these monitoring logs are mainly about latencies
        kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
        for i in xrange(1000000):
		message = """129.94.144.152 - - [01/Jul/1995:00:00:17 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0 {}""".format(i)
		kafka_producer.send(topic, message)
		time.sleep(.1)
		print message + '\n' + 80*'=' + '\n' 
		
            # create message with random latency drawn from lognormal(0,1) distribution
            #  message = { 
            # 			"id": "{}".format(i), 
            # 			"latency": "{}".format(lognormal(1))
            #           }
       	#  message = """129.94.144.152 - - [01/Jul/1995:00:00:17 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0 {}""".format(i)
          #   kafka_producer.send(topic, message)
           #  time.sleep(.1)
           #  print message + '\n' + 80*'=' + '\n'    	


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
