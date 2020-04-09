import json
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import pysolr

class BlankDict(dict):
  def __missing__(self, key):
    return ''

def send2solr(data):
    tweet=json.loads(data)
    try:
        if tweet["coordinates"]:
            lat=tweet["coordinates"]["coordinates"][1]
            lng=tweet["coordinates"]["coordinates"][0]
        elif tweet["geo"]:
            lat=tweet["geo"]["coordinates"][0]
            lng=tweet["geo"]["coordinates"][1]
        elif tweet["place"]:
            lng=(tweet["place"]["bounding_box"]["coordinates"][0][0][0]+tweet["place"]["bounding_box"]["coordinates"][0][2][0])/2 
            lat=(tweet["place"]["bounding_box"]["coordinates"][0][0][1]+tweet["place"]["bounding_box"]["coordinates"][0][1][1])/2 
        else:
            lat,lng=None
    
        if tweet["place"]:
            city=tweet["place"]["full_name"]
            country_code=tweet["place"]["country_code"]
            #country=tweet["place"]["country"]
        else:
            city,country_code=None
            #country=None

        index = [{
            "created_at": tweet["created_at"],
            "id": tweet["id_str"],
            "text": tweet["text"],
            "user_name": tweet["user"]["screen_name"],
            "latitude":lat,
            "longitude":lng,
            "city":city,
            "country_code":country_code#,
            #"country":country

        }]
        solr = pysolr.Solr('http://192.168.36.131:8886/solr/geoTweets')
        solr.add(index, commit=True)
        solr.commit()
        #print(index)
        
    except:
        #continue
    return index

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: GeoBasedProcessor.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="GeoTweets")
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint("GeoTweets-Checkpoint")

    zkQuorum, topic = sys.argv[1:]
    twitterStream = KafkaUtils.createStream(ssc, zkQuorum, "GeoTweets", {topic: 1}, {"auto.offset.reset": "largest"})
    docs = twitterStream.map(lambda x: send2solr(x[1])).count()
    docs.pprint()

    ssc.start()
    ssc.awaitTermination()