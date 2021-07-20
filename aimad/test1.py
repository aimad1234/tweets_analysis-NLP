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

access_token = ""
access_token_secret =  ""
api_key =  ""
api_secret =  ""

class StdOutListener(StreamListener):
    def on_data(self, data):
        json_ = json.loads(data) 
        producer.send("quickstart-events", json_["text"].encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)


producer = KafkaProducer(bootstrap_servers='192.168.1.20:9092')



l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["trump"],languages = ['fr'])


