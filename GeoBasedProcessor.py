import json
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import pysolr
from datetime import datetime

class BlankDict(dict):
  def __missing__(self, key):
    return ''

def send2solr(data):
    tweet=json.loads(data)
    try:
        created_at=str(datetime.strptime(str(tweet["created_at"].decode('utf-8')), "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%dT%H:%M:%SZ"))
        index = [{
            "created_at": created_at,
            "id": tweet["id_str"],
            "text": tweet["text"],
            "user_name": tweet["user"]["screen_name"],
            "longitude":(tweet["place"]["bounding_box"]["coordinates"][0][0][0]+tweet["place"]["bounding_box"]["coordinates"][0][2][0])/2, 
            "latitude":(tweet["place"]["bounding_box"]["coordinates"][0][0][1]+tweet["place"]["bounding_box"]["coordinates"][0][1][1])/2,
            "city":tweet["place"]["full_name"],
            "country_code":tweet["place"]["country_code"],
            "country":tweet["place"]["country"]

        }]
        solr = pysolr.Solr('http://192.168.36.131:8886/solr/geoBasedTweets')
        solr.add(index, commit=True)
        solr.commit()
        #print(index)
        return index
    except Exception as e:
        pass
        print(e)
        return tweet

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: GeoBasedProcessor.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="GeoBasedTweets")
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint("GeoBasedTweets-Checkpoint")

    zkQuorum, topic = sys.argv[1:]
    twitterStream = KafkaUtils.createStream(ssc, zkQuorum, "GeoBasedTweets", {topic: 1}, {"auto.offset.reset": "largest"})
    docs = twitterStream.map(lambda x: send2solr(x[1])).count()
    docs.pprint()

    ssc.start()
    ssc.awaitTermination()
