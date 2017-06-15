from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

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

# pick out words in each log and print them
# words = kafka_stream.flatMap(lambda line: line.split(" "))
                     # .map(lambda word: (word, 1)) \
                     # .reduceByKey(lambda a, b: a+b)
kafka_stream.pprint()

# start stream
ssc.start()
ssc.awaitTermination()
