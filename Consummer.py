# -*- coding: utf-8 -*-
"""
Created on Mon Jul  5 14:44:19 2021

@author: aimad
"""
#------------
import time
import inflect
import re
import string
import nltk

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
from nltk.corpus import stopwords # used for preprocessing
from nltk.stem import WordNetLemmatizer # used for preprocessing
from nltk.stem import PorterStemmer
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize

#------------
from pyspark import SparkContext
from operator import add
from kafka import KafkaConsumer
import json
from textblob import TextBlob
from elasticsearch import Elasticsearch
es = Elasticsearch()
topic_name = 'quickstart-events'
consumer = KafkaConsumer(
    topic_name,
     bootstrap_servers=['LAPTOP-GL13JIPM'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     auto_commit_interval_ms =  5000,
     fetch_max_bytes = 128,
     max_poll_records = 100,
     )

#-------------------Traitement du data -----------------------

def remove_urls(text):
    new_text = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^A-Za-z# \t])|(\w+:\/\/\S+)"," ",text).split())
    return new_text

# make all text lowercase
def text_lowercase(text):
    return text.lower()

# make all text Uppercase

def text_uppercase(text):
    return text.upper()


#----------supprimer les nombres existent dans le text

def remove_numbers(text):
    result = re.sub(r'\d+', '', text)
    return result


# --------------- supprimer la ponctuation existent dans le text

def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)


# ------transformer en une liste de mots
def tokenize(text):
    text = word_tokenize(text)
    return text


#-------- supprimer les mots non significative---------

stop_words = set(stopwords.words('english'))
def remove_stopwords(text):
    text = [i for i in text if not i in stop_words]
    return text


# ---------------trouver les origines des mots
lemmatizer = WordNetLemmatizer()
def lemmatize(text):
    text = [lemmatizer.lemmatize(token) for token in text]
    return text

# -------------remplacer tous les nombre en lettre

def numbers_to_char(text):
    wordtoken = nltk.word_tokenize(text)
    p = inflect.engine()
    new_Text = []
    for word in wordtoken:
        if word.isdigit():
            newword = p.number_to_words(word)
            new_Text.append(newword)
        else:
            new_Text.append(word)
    return new_Text


#--------------------------Fin--------------------------------
def analyze_sentiment(text):
    testimonial = TextBlob(text)
    return testimonial.sentiment.polarity
def get_sentiment_tuple(sent):
    neutral_threshold = 0.05
    if sent >= neutral_threshold:       # positive
        return (0, 0, 1),"positive"
    elif sent > -neutral_threshold:     # neutral
        return (0, 1, 0),"neutral"
    else:                               # negative
        return (1, 0, 0),"negative"
    
    
counter=0 
#--------------------------------------------------------
def topHashtag(text):
    return text.split(" ")
def getNouns(txt):
    blob = TextBlob(txt)
    print(blob.noun_phrases)
for msg in consumer:
        #print(str(1));
        time.sleep(2)
      
        data= text_lowercase(msg.value)
        
        #------heshtags
        dict_data=json.loads(data)
        hashtags = re.findall("#[a-zA-Z0-9_]{1,50}", dict_data["text"])
       
        #---sentimens
        tweets=TextBlob(dict_data["text"])
        sentiments=analyze_sentiment(dict_data["text"])
        #----------les meilleurs et les pirs tweets 
        
        frequencetweets= text_lowercase(dict_data["text"])
        frequencetweets=remove_urls(frequencetweets)
        #--------mot plus frequents
        filtreData= text_lowercase(dict_data["text"])
        filtredData=remove_urls(filtreData)
        filtredData=remove_punctuation(filtredData)
        filtredData=tokenize(filtredData)
        filtredData=remove_stopwords(filtredData)
        FrequencesMots=lemmatize(filtredData)
  
        print(FrequencesMots)

        
        analysentimens,feedback=get_sentiment_tuple(sentiments)
       
        
        es.index(index="tweet_index" ,
                    doc_type="test_doc",
                    body={
                            "author": dict_data["user"]["screen_name"],
                            "date": dict_data["created_at"],
                            "message": dict_data["text"], 
                            "sentimentsPer":sentiments,
                            "feedback":feedback,
                            "hashtag":hashtags,
                            "FrequencMots":FrequencesMots,
                            "FrequenceTweets":frequencetweets,
                    }
                )
        #time.sleep(5)
        #print(str(tweets))
        print('\n')