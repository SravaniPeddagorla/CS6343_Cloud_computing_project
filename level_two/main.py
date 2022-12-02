import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import string
import re
import pandas as pd
bootstrap_servers = ['10.176.67.102:9092']
ConsumerTopicName = 'level_one'
ProducerTopicName = 'level_two'

consumer = KafkaConsumer (ConsumerTopicName, group_id = 'group1',bootstrap_servers = bootstrap_servers, auto_offset_reset = 'earliest')

stop_words = set(stopwords.words('english'))
def clean_tweet(sentence):
    print(sentence)
    te = sentence
    te = re.sub('RT @\w+: '," ",str(te))
    #te = re.sub("(@[A-Za-z0-9]+)|([0-9A-Za-z \t])|(\w+:\/\/S+)"," ",te)
    t=re.split(r"\s+|\\",te)
    filtered_t = []
    
    for i in range(1,len(t)):
        if(t[i].lower() not in stop_words):
            filtered_t.append(re.sub('[^A-Za-z0-9]+', '', t[i].lower()))
    return (filtered_t)

for message in consumer:
    temp_tweet = clean_tweet(message.value)
    filtered_tweet = ' '.join(str(t) for t in temp_tweet)
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers, api_version=(0,10,1))
    ack = producer.send(ProducerTopicName, key = message.key, value = str(filtered_tweet).encode('utf-8'))
    metadata = ack.get()
    print(metadata.topic)
    print(metadata.partition)
