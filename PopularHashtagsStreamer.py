from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaClient
from kafka import SimpleProducer

consumer_key = "p9Z4umwON3CCgAKnNXYpZ2zKv"
consumer_secret = "PT26HyZos0fIrDS0O4LpJL9GQ43DS4IMPHun9gxhW7mBYxkJFD"
access_token = "987735747447816192-PxcHJaEen69RHPAehlUyLejXgYCttiT"
access_token_secret = "AKtVclTXEJlsZNUWUq2ePjrxfjnAKf1lk88ud6inwHOn8"

class StdOutListener(StreamListener):

  def on_data(self, data):
    producer.send_messages('PopularHashtags', data.encode("utf-8"))
    print(data)
    return True

  def on_error(self, status):
    print(status)
    return True

if __name__ == '__main__':
  kafka_client = KafkaClient("sandbox-hdp.hortonworks.com:6667")  
  producer = SimpleProducer(kafka_client)
  l = StdOutListener()
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  stream = Stream(auth, l)
  stream.filter(locations=[-124.64,32.44,-114.5,41.96], languages=["en"])

