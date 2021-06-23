# -*- coding: utf-8 -*-
"""
Created on Mon Jun 21 16:49:09 2021

@author: Aimad
"""
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

access_token = "1226574939119136769-bd5aBC0QhzE7Qp2GOWULD8CGAyeisd"
access_token_secret =  "8yt5u3d2JMPd8ZgBTy9MoA34I8GYnumQP3b9EZ7sExQtx"
api_key =  "vvl6tAUpTVYG2p9XrrY3pGdS3"
api_secret =  "kv3K5vj9DyTiYB7ELZfgxANrZOZIxlZ29KwKDok4rBo2WHkCyI"
class StdOutListener(StreamListener):
    def on_data(self, data):
        json_ = json.loads(data) 
        producer.send("quickstart-events", json_["text"].encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

producer = KafkaProducer(bootstrap_servers='192.168.1.8:9092')
l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["trump"],languages = ['fr'])


print("mehdi")