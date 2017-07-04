from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis
import json
from tdigest import TDigest

# set up function to transfer stream to redis database - take from spark streaming documentation
# this function can be further optimized by using a pool - see spark documentation

def sendPartition(itera):
    redis_server = 'localhost'
    # redis_server = 'ec2-34-210-183-208.us-west-2.compute.amazonaws.com' # master
    redis_table = redis.StrictRedis(redis_server, port=6379, db=0)
    for record in itera:
        redis_table.set(record, record)

def write_to_redis(rdd):
    # write rdd from dstream to redis
    redis_server = 'localhost'
    redis_table = redis.StrictRedis(redis_server, port=6379, db=0)

    # collect rdd back to master node as python iterable
    # this appears to be a single digest list of tuples 
    # need to write to database under a single key
    list_of_digests = rdd.collect()
    print "Type of list_of_digests: ", type(list_of_digests)
    print list_of_digests

    # add some more lines to actually write to redis
    redis_table.set("current_digest", list_of_digests)

# initialize stream
sc = SparkContext(appName="NASA-logs") # can tune this to cluster later
sc.setLogLevel("WARN") # make less logs to stdout
ssc = StreamingContext(sc, 5) # 5 second microbatch duration for Dstream
# ssc.checkpoint("checkpoint") - leave out checkpointing for now

# set up connection with kafka
brokers = "ec2-34-210-183-208.us-west-2.compute.amazonaws.com:9092" # master
topic = 'NASA-logs'

kafka_stream = KafkaUtils.createDirectStream(ssc,
                                             [topic],
                                             {"metadata.broker.list": brokers})
##########################################
# HELPER FUNCTIONS
##########################################

# compute sum  across partition
def do_sum(values):
    summ = 0
    for item in values:
        summ += item
    return [summ]

# print out how many partitions per RDD
def print_partitions(rdd):
    print rdd.getNumPartitions()

# print out contents of RDD

def print_RDD_contents(rdd):
    for x in rdd.collect():
        print x

# returns list of tuples of form (location, weight)
# that represent a t-digest - this is for convenient
# storage in redis (will need to be loaded into tdigest data
# structure again when called from redis)
def compute_tdigest(values):
    if not values: 
        return []
    tdig = TDigest()
    all_values = [x for x in values]
    tdig.batch_update(all_values)
    tdig_tuples_list = [(item[0], item[1].count) for item in tdig.C.items()]
    return tdig_tuples_list

# simple transformation on rdd to extract only text
just_words = kafka_stream.map(lambda row: row[1])
# stream records into redis database

#################################################
# TRY DELETING THIS LINE - MAYBE TOO MANY REDIS CONNECTIONS?
################################################

# just_words.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

# convert words from json into dictionary object in stream of RDDs
# schema for this dictionary is
# {u'device': u'type2', u'latency': 2, u'message_num': 189}
dict_data = just_words.map(json.loads)

# latencies are list of integers
latencies = dict_data.map(lambda x: x["latency"])


#####################################################
# TRY KEY-VALUE PAIRING
#####################################################

# create key-value partition (using tuples) for splitting - key is device type
device_keyed_rdd = dict_data.map(lambda x: (x["device"], x))
# split this new rdd on the device key
device_keyed_rdd = device_keyed_rdd.partitionBy(4)
# print how many partitions in this rdd
device_keyed_rdd.foreachRDD(print_partitions)
# try mapping partitions with pairRDD device_keyed_rdd
partitioned_digests = device_keyed_rdd.mapPartitions(compute_tdigest, preservesPartitioning=True)
# there is an error when pprint() is called on partitioned_digests
# try collecting and printing out the output

####################################################
# THIS LINE CAUSES PROBLEM
####################################################
# partitioned_digests.foreachRDD(write_to_redis)
####################################################

# try the following after testing above function
# UPDATE: this is currently failing, not sure why
# For now, check partition computation accuracy by
# storing in redis and querying redis itself

# partitioned_digests.foreachRDD(print_RDD_contents)

# print out current format of device_keyed_rdd
#  device_keyed_rdd.pprint()

# try printing out t-digests by partition
# keyed_digests = device_keyed_rdd.mapPartitions(compute_keyed_tdigest)
# keyed_digests.pprint()

# compute tdigest of each partition using mapPartitions
digests = latencies.mapPartitions(compute_tdigest)

# convert digest

digests.pprint()

# print out how many partitions per rdd
digests.foreachRDD(print_partitions)
# print contents of digests
digests.foreachRDD(print_RDD_contents)

##########################################################
# TRY WRITING TO REDIS WITH JUST REGULAR DIGEST DSTREAM
##########################################################
digests.foreachRDD(write_to_redis)




# UPDATE: doing command below causes an error in spark - seems to be problem with
# sending distributed partitions to redis
# OPTIONS: either do no partition (just to get pipeline) or
# try doing a collect() before writing

# partitioned_digests.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

# redis_table = redis.StrictRedis(host='localhost', port=6379, db=0)
# redis_table.set(just_words, just_words)

# start stream
ssc.start()
ssc.awaitTermination()

# running command: sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 spark_stream_processor.py

