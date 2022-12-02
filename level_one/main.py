from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

from textblob import TextBlob
import sys
import tweepy
import pandas as pd
import numpy as np
import os
import nltk
import pycountry
import re
import string
import nltk
#nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from langdetect import detect
from nltk.stem import SnowballStemmer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer

consumerKey = 'coxUYVWE6dCg6awNjWnC9ly1V'
consumerSecret = 'uZGGAyooH3wjf0IOl5DjGDAxRtGnUSGl1WkjSZizV0juhkV2zz'
accessToken = '1241042661865213953-h3as33t6XLZJAmUCE00Js1GTIHMmhN'
accessTokenSecret = 'EHR29otaFj505NC10mJlidQiOwRasvKwyBHTm87QDlOQV'
auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessTokenSecret)
api = tweepy.API(auth)

def percentage(part,whole):
	return 100 * float(part)/float(whole)
#keyword = input('Please enter keyword or hashtag to search: ')
#noOfTweet = int(input ('Please enter how many tweets to analyze: '))
keyword='covid'
noOfTweet=100
tweets = tweepy.Cursor(api.search_tweets, q=keyword).items(noOfTweet)

bootstrap_servers = ['10.176.67.102:9092']
topicName = 'level_one'
#tweets.drop_duplicates(inplace = True)
for tweet in tweets:
	#producer = KafkaProducer(bootstrap_servers = bootstrap_servers, retries = 5,value_serializer=lambda m: json.dumps(m).encode('ascii'),api_version=(0,10,1))
	producer = KafkaProducer(bootstrap_servers = bootstrap_servers,api_version=(0,10,1))
	ack = producer.send(topicName, key = str(tweet.id).encode('utf-8'), value = bytes(tweet.text).decode('utf-8', 'ignore'))#str(tweet.text).encode('utf-8'))
	metadata = ack.get()
	print(metadata.topic)
	print(metadata.partition)
