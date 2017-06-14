from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# initialize stream
sc = SparkContext(appName="NASA-logs")
ssc = StreamingContext(sc, 1)
# ssc.checkpoint("checkpoint") - leave out checkpointing for now

# set up connection with kafka
brokers = "ec2-52-42-160-109.us-west-2.compute.amazonaws.com" # master dns
topic = 'NASA-logs'

kafka_stream = KafkaUtils.createDirectStream(ssc,
                                             [topic],
                                             {"metadata.broker.list": brokers})
