from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaClient
from kafka import SimpleProducer
import json,configparser,pysolr

class GeoTweetListener(StreamListener):

  def on_data(self, data):
    tweet=json.loads(data)
    try:
      if tweet["place"]:
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
        producer.send_messages('geoBasedTweets', data.encode("utf-8"))
        print(data)
    except KeyError as msg:
      print(msg)

    return True

  def on_error(self, status):
    print(status)
    return True

if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini')
  kafka_client = KafkaClient("sandbox-hdp.hortonworks.com:6667")  
  producer = SimpleProducer(kafka_client)
  l = GeoTweetListener()
  auth = OAuthHandler(config['TwitterAPI']['key'], config['TwitterAPI']['secret'])
  auth.set_access_token(config['TwitterAPI']['token'], config['TwitterAPI']['token_secret'])
  stream = Stream(auth, l)
  stream.filter(track=['#corona','#coronavirus','#covid','#StayAtHome','#stayhome', '#CoronaLockdown', '#covid19','#covid2019'],filter_level=None,languages=["en"])