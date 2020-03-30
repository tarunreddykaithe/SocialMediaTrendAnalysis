import json
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: KafkaSparkStreaming.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="SocialMediaTrendAnalysis")
    ssc = StreamingContext(sc, 20)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "Twitter-streaming", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    print("normal line")
    lines.pprint()
    print("json")
    jsons=lines.map(lambda x:json.load(x))
    jsons.pprint()
    print("id_str")
    jsons.map(lambda x: x["id_str"]).foreach(print)
    print(jsons.id_str)


    #Sample word count program to check tweets are read from kafka
    #counts = lines.flatMap(lambda line: line.split(" ")) \
     #             .map(lambda word: (word, 1)) \
      #            .reduceByKey(lambda a, b: a+b)
    #counts.pprint()

    ssc.start()
    ssc.awaitTermination()