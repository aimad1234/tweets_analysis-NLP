# -*- coding: utf-8 -*-
"""
Created on Sat Jul  3 16:05:52 2021

@author: ASUS
"""


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.streaming.kafka import KafkaUtils

conf = SparkConf().setMaster("local").setAppName("AnalyseTweets")
sc = SparkContext.getOrCreate(conf=conf)

batch_interval = 2
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("twittercheckpt")

twitterKafkaStream = KafkaUtils. \
         createDirectStream(ssc, ["quickstart-events"], {"metadata.broker.list": "10.189.77.68:9092"})

import json

tweets = twitterKafkaStream. \
        map(lambda key, value: json.loads(value)). \
        map(lambda json_object: (json_object["user"]["screen_name"], json_object["text"]))

from textblob import TextBlob
def analyze_sentiment(text):
    testimonial = TextBlob(text)
    return testimonial.sentiment.polarity

def get_sentiment_tuple(sent):
    neutral_threshold = 0.05
    if sent >= neutral_threshold:       # positive
        return (0, 0, 1)
    elif sent > -neutral_threshold:     # neutral
        return (0, 1, 0)
    else:                               # negative
        return (1, 0, 0)
    
tweets_sentiment_analysed = tweets.map(lambda user, text: (user, text, get_sentiment_tuple(analyze_sentiment(text))))

from pyspark import StorageLevel
tweets_sentiment_analysed.persist(StorageLevel.MEMORY_AND_DISK)

hashtags = tweets_sentiment_analysed. \
        map(lambda user, text, sent: ((user, sent), text)). \
        flatMapValues(lambda text: text.split(" ")). \
        filter(lambda kp, w: len(w) > 1 and w[0] == '#'). \
        map(lambda user, sent, hash: (hash, (1, sent)))

hashtags_count_acc_sent = hashtags. \
     reduceByKeyAndWindow(lambda c1, s1, c2, s2: (c1+c2, (s1[0]+s2[0], s1[1]+s2[1], s1[2]+s2[2])), None,15*60, 6)

sorted_hashtags = hashtags_count_acc_sent.map(lambda hash, count, sent: (count, (hash, sent))). \
     transform(lambda rdd: rdd.sortByKey(False)). \
     map(lambda count, hash, sent: (hash, (count, sent)))

sorted_hashtags.pprint(10)
ssc.start()
ssc.awaitTermination()
     
        
