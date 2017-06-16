from kafka import KafkaProducer
import time
import threading
# import logging

class Producer(threading.Thread):

    # daemon threads are summarily killed as soon as main program ends
    daemon = True

    def run(topic):
        topic = "NASA-logs"
        kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
        for i in xrange(1000000):
            # for now message is just one NASA log with an extra integer
            # column appended at end of row
            message = """129.94.144.152 - - [01/Jul/1995:00:00:17 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0 {}""".format(i)
            kafka_producer.send(topic, message)
            time.sleep(.1) # what does this do?
            print message + '\n' + 80*'=' + '\n'


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
