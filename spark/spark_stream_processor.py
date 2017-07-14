"""
Spark stream processor.

Takes in latency scalar values from the Kafka 'Latency' topic,
computes t-digest summary of data from microbatch interval,
and records percentiles derived from this summary into Redis.
The summary is updated at the microbatch interval in Redis, allowing
for a real-time dashboard display.

The t-digest Python library used is based on the version here:
https://github.com/CamDavidsonPilon/tdigest. Some changes were
made to this library to allow for more flexible merging and more
control over compression.

Several t-digests can be computed in parallel in Spark, with
intent of either merging the digests at end of Spark job or storing them
separately. This example draws from a Kafka topic with a single partition,
but a natural extension is to use a Kafka topic with multiple partition
(e.g. with a partition split based on device type). The t-digests can then
be computed in parallel in Spark and then either merged in Spark or stored
separately for later querying.


To start stream from command line, go to directory that contains this
file (name of file is at end of command) and enter:

sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 cleaned_spark_stream.py
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis
import json
# tdigest import under "sc.addPyFile" command below


##########################################
# Spark stream parameters
##########################################

microbatch_size = 5 # 5 seconds

# Kafka
brokers = "ec2-34-210-183-208.us-west-2.compute.amazonaws.com:9092" # master
topic = 'Latency'

# Redis
redis_server = 'localhost'

##########################################
##########################################


##########################################
# Helper functions
##########################################

def compute_tdigest(values):
    """ Returns list of percentiles and their values as computed from t-digest.

    rtype: [[int, float], [int, float], ...]

    For example,

    [[0, 0.01], [5, 0.13], [10, 1.39], ..., [95, 5.74]]
    """
    ####################################################
    # create t-digest tdig from values data
    ####################################################
    if not values:
        return []
    tdig = TDigest()
    all_values = [x for x in values]
    tdig.batch_update(all_values)

    ####################################################
    # compute list of percentiles using tdig
    ####################################################
    all_percentiles = []
    for perc in range(0, 100, 5):
        all_percentiles.append([perc, tdig.percentile(perc)])

    return all_percentiles

def write_to_redis(rdd):
    """ Write t-digest rdd percentile list to Redis.
    """

    redis_table = redis.StrictRedis(redis_server, port=6379, db=0)
    # collect rdd back to master node as python iterable
    list_of_digests = rdd.collect()
    # write to redis
    redis_table.set("current_digest", list_of_digests)

##########################################
# Debugging functions
##########################################

def print_partitions(rdd):
    """ Print out how many partitions in RDD

    Example usage: digests.foreachRDD(print_partitions)
    """
    print rdd.getNumPartitions()

def print_RDD_contents(rdd):
    """ Print out contents of RDD

    Example usage: digests.foreachRDD(print_RDD_contents)
    """
    for x in rdd.collect():
        print x

##########################################
# Spark job
##########################################

# set Spark context
sc = SparkContext(appName="Latency")
sc.setLogLevel("WARN")

sc.addPyFile("../tdigest/tdigest_altered.py") # import custom tdigest class
from tdigest_altered import TDigest

ssc = StreamingContext(sc, microbatch_size)

# create D-Stream from Kafka topic
kafka_stream = KafkaUtils.createDirectStream(ssc,
                                             [topic],
                                             {"metadata.broker.list": brokers})

# extract latency data (combined across devices)
# json schema: {u'device': u'type2', u'latency': 2.487, u'message_num': 189}
latencies = kafka_stream.map(lambda row: row[1])\
                        .map(json.loads)\
                        .map(lambda x: x["latency"])

# compute tdigest of each partition and write to redis
digests = latencies.mapPartitions(compute_tdigest)
digests.pprint()
digests.foreachRDD(write_to_redis)

# start stream
ssc.start()
ssc.awaitTermination()
