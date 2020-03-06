from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

""" from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
"""
# client = SimpleClient("localhost:9092")
# producer = SimpleProducer(client)
consumer_key = "p9Z4umwON3CCgAKnNXYpZ2zKv"
consumer_secret = "PT26HyZos0fIrDS0O4LpJL9GQ43DS4IMPHun9gxhW7mBYxkJFD"
access_token = "987735747447816192-PxcHJaEen69RHPAehlUyLejXgYCttiT"
access_token_secret = "AKtVclTXEJlsZNUWUq2ePjrxfjnAKf1lk88ud6inwHOn8"

class StdOutListener(StreamListener):

  def on_data(self, data):
    # producer.send_messages('movies', str(data))
    print(data)
    return True

  def on_error(self, status):
    print(status)

if __name__ == '__main__':
  l = StdOutListener()
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  stream = Stream(auth, l)
  stream.filter(track=['#corona'],languages=["en"])

