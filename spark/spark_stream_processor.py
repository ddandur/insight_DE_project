from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis

# set up function to transfer stream to redis database - take from spark streaming documentation
# this function can be further optimized by using a pool - see spark documentation

def sendPartition(iter):
    redis_table = redis.StrictRedis('localhost', port=6379, db=0)
    for record in iter:
        redis_table.set(record, record)


# initialize stream
sc = SparkContext(appName="NASA-logs") # can tune this to cluster later
ssc = StreamingContext(sc, 1) # 1 second microbatch duration for Dstream
# ssc.checkpoint("checkpoint") - leave out checkpointing for now

# set up connection with kafka
brokers = "ec2-52-42-160-109.us-west-2.compute.amazonaws.com:9092" # master dns
topic = 'NASA-logs'

kafka_stream = KafkaUtils.createDirectStream(ssc,
                                             [topic],
                                             {"metadata.broker.list": brokers})

# simple transformation on rdd to extract only text
just_words = kafka_stream.map(lambda row: row[1])

# pick out words in each log and print them
# words = kafka_stream.flatMap(lambda line: line.split(" "))
                     # .map(lambda word: (word, 1)) \
                     # .reduceByKey(lambda a, b: a+b)
just_words.pprint()

# stream records into redis database
just_words.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

# redis_table = redis.StrictRedis(host='localhost', port=6379, db=0)
# redis_table.set(just_words, just_words)

# start stream
ssc.start()
ssc.awaitTermination()

# running command: sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 spark_stream_processor.py
