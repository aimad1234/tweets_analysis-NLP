# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 12:00:13 2021

@author: hp
"""

from tweepy.streaming import StreamListener, OAuthHandler, Stream
from kafka import KafkaProducer
# Encodage utf-8 des tweets 
producer = KafkaProducer(
 value_serializer=lambda m: dumps(m).encode('utf-8'), 
 bootstrap_servers=['localhost:9092:','localhost:9092'])
# Envoi des données de streaming sur le topic kafka « tweets01 »
class StdOutListener(StreamListener):
 def on_data(self, data):
 producer.send('tweets01', value=data)
 date = datetime.now() 
 print(date,"New Tweet ! (Len:",len(data),")")
 return True
# Démarrage du Streaming avec Tweepy et Kafka
listener = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
twitter_stream = Stream(auth, listener)
twitter_stream.filter(follow=list_follow,track=list_track,languages = ['en','fr'])