from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaClient
from kafka import SimpleProducer

import json
import configparser

class TrendingHashtagsListener(StreamListener):

  def on_data(self, data):
    data=json.loads(data)
    tweet=data["text"]
    producer.send_messages('TrendingHashtags', data.encode("utf-8"))
    #print(data)
    return True

  def on_error(self, status):
    print(status)
    return True

if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read('config.ini')
  kafka_client = KafkaClient("sandbox-hdp.hortonworks.com:6667")  
  producer = SimpleProducer(kafka_client)
  l = TrendingHashtagsListener()
  auth = OAuthHandler(config['TwitterAPI']['key'], config['TwitterAPI']['secret'])
  auth.set_access_token(config['TwitterAPI']['token'], config['TwitterAPI']['token_secret'])
  stream = Stream(auth, l)
  stream.filter(locations=[-125.009764,25.244696,-67.265623,49.267805], languages=["en"])