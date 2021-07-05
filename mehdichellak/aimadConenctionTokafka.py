# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 16:07:54 2021

@author: ASUS
"""


from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
import json
from pyspark.sql import SQLContext

conf = SparkConf().setMaster("local").setAppName("AnalyseTweets")
sc = SparkContext.getOrCreate(conf=conf)
batch_interval = 2
ssc = StreamingContext.getActiveOrCreate(sc, batch_interval)
ssc.checkpoint("twittercheckpt")

twitterKafkaStream = KafkaUtils. \
         createDirectStream(ssc, ["quickstart-events"], {"metadata.broker.list": "10.189.77.68:9092"})

parsed = twitterKafkaStream.map(lambda v: json.loads(v[1]))
text_counts = parsed.map(lambda tweet: (tweet['text'],1)).reduceByKey(lambda x,y: x + y)

text_counts.pprint()

ssc.start()
ssc.awaitTermination()
