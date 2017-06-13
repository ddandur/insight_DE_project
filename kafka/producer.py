from kafka import KafkaProducer
import time
import threading
# import logging

class Producer(threading.Thread):

    # using syntax for threading
    daemon = True

    def run(topic):
        topic = "NASA-logs"
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        for i in xrange(1000000):
            # for now message is just one NASA log with an extra integer
            # column appended at end of row
            message = """129.94.144.152 - - [01/Jul/1995:00:00:17 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0 {}""".format(i)
            producer.send(topic, message)
            time.sleep(.2)
            print message + '\n' + 80*'=' + '\n'


if __name__ == "__main__":
    # logging - might turn this on later
    """
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    """
    producer = Producer()
    producer.start()
    # time.sleep(10) # might adjust this later
