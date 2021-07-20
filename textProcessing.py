# -*- coding: utf-8 -*-
"""
Created on Tue Jul  6 18:35:43 2021

@author: ASUS
"""

import nltk
from textblob import TextBlob

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
from nltk.corpus import stopwords # used for preprocessing
from nltk.stem import WordNetLemmatizer # used for preprocessing
from nltk.stem import PorterStemmer
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize

txt = """Natural language processing (NLP) is a field of computer science, artificial intelligence, and computational linguistics concerned with the inter
actions between computers and human (natural) languages."""
blob = TextBlob(txt)
print(blob.noun_phrases)
