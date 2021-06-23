# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 12:10:01 2021

@author: hp
"""

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer

ACCESS_TOKEN = '1226574939119136769-Fdpz42OY4GrxOW7JzOFw3DLkF1WMdXv'
ACCESS_SECRET = '2DlTX9RZkZ1YarSISmVfNnmVpboDkRjvrWQNTNBlmF8gw'
CONSUMER_KEY = 'mVTsmXLwbVztsmIqJmZ52vWAW'
CONSUMER_SECRET = 'ToiUNpdHUpr7q0ueRZ0CBvCkKmUBC9C9NjlzBcQ41b0oYx1Uon'

kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
class StdOutListener(StreamListener):
    def on_data(self, data):
        kafka_producer.send("quickstart-events", data.encode('utf-8')).get(timeout=10)
        print (data)
        return True
    def on_error(self, status):
        print (status)



l = StdOutListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
stream = Stream(auth, l)
stream.filter(track=["corona virus"])
"""# Code pour l’analyse en continu des tweets avec Spark Streaming
from pyspark.streaming.kafka import KafkaUtils
# Configuration de Spark Streaming toutes les 5 secondes 
sc = SparkContext(conf=conf)
stream_nbseconds=5
ssc = StreamingContext(sc, stream_nbseconds)
# Connexion au topic Kafka dédié au streaming des tweets
topic="tweets01"
kafkaStream = KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list": 
"kafka01:9092,kafka02:9092"})
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
# Analyse en continu des hashtags dans les tweets 
words = parsed.flatMap(lambda line: line.replace('"',' ').replace("'",' ').replace
("(",' ').replace(")",' ').replace("\\",' ').replace(".",' ').split())
hashtags=words.filter(lambda w: re.findall(r'\B#\w*[a-zA-Z]+\w*',w)).map(lambda x: 
(x, 1))
# fenêtre d
"""