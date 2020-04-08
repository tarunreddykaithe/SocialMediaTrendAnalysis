import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import json

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: PopularHashtags.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PopularHashTags")
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint("PopularHashTags-Checkpoint")

    zkQuorum, topic = sys.argv[1:]
    twitterKafkkaStream = KafkaUtils.createStream(ssc, zkQuorum, "Popular-Hashtags", {topic: 1}, {"auto.offset.reset": "largest"})
    data = twitterKafkkaStream.map(lambda x : x[1])
    tweets= json.loads([data])

    map(lambda json_object: (json_object["id"]["created_at"], json_object["text"]))
    lines = kvs.map(lambda x: x[1])

    #iterate through json 
    #loggin 
    #n debug
    lines.pprint()
    #Sample word count program to check tweets are read from kafka
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()