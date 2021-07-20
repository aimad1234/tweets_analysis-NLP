# -*- coding: utf-8 -*-
"""
Created on Mon Jun 21 16:49:09 2021

"""
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

# --------------------------- set your crendentiels from twitter developper account ---------------------
access_token = ""
access_token_secret =  ""
api_key =  ""
api_secret =  ""
#--------------- get tweets from twitter and save into kafka in a topic calles quickstart-events ------

class StdOutListener(StreamListener):
    def on_data(self, data):
        #--------------------- you have to create your topic using kafka command line -----------------------
        producer.send("quickstart-events",data.encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

# ------------------- change server name, search to your server on kafka-server window ---------------------
producer = KafkaProducer(bootstrap_servers='LAPTOP-GL13JIPM')



l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
# -------------------- change your topic of search here i choose covdi19 and vaccine  in english ----------------
stream.filter(track=["covid19","vaccine"],languages = ['en']) 






