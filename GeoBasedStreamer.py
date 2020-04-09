from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaClient
from kafka import SimpleProducer
import json,configparser

class GeoTweetListener(StreamListener):

  def on_data(self, data):
    tweet=json.loads(data)
    if(tweet["place"]!=None || tweet["geo"]!=None || tweet["coordinates"]!=None ):
      producer.send_messages('GeoBasedTweets', data.encode("utf-8"))
    return True

  def on_error(self, status):
    print(status)
    return True

if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini')
  kafka_client = KafkaClient("sandbox-hdp.hortonworks.com:6667")  
  producer = SimpleProducer(kafka_client)
  l = StdOutListener()
  auth = OAuthHandler(config['TwitterAPI']['key'], config['TwitterAPI']['secret'])
  auth.set_access_token(config['TwitterAPI']['token'], config['TwitterAPI']['token_secret'])
  stream = Stream(auth, l)
  stream.filter(track=['#corona','#coronavirus','#covid','#stayhome', '#CoronaLockdown', '#covid19','#covid2019'],locations=[176.6229246568,-71.1165113448,-146.8145324279,84.3931531877], languages=["en"])