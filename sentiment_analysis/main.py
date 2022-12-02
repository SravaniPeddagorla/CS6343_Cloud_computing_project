from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import nltk
nltk.download('vader_lexicon')
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer

bootstrap_servers = ['10.176.67.102:9092']
ConsumerTopicName = 'level_two'
ProducerTopicName = 'level_three'
consumer = KafkaConsumer (ConsumerTopicName, group_id = None, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'earliest', api_version=(0,10,1))

def sentiment_analysis(sentence):
    #analysis = TextBlob(sentence)
    pos = [SentimentIntensityAnalyzer().polarity_scores(i)["pos"] for i in sentence.split(" ")]
    neg = [SentimentIntensityAnalyzer().polarity_scores(i)["neg"] for i in sentence.split(" ")]
    neu = [SentimentIntensityAnalyzer().polarity_scores(i)["neu"] for i in sentence.split(" ")]
    
    pos_val = sum(pos)
    neg_val = sum(neg)
    neu_val = sum(neu)
    #polarity += analysis.sentiment.polarity
    print(neg_val, neu_val, pos_val) 
    if neg_val > pos_val and neg_val > neu_val:
        return 'Negative'
    elif pos_val > neg_val and pos_val > neu_val:
        return 'Positive'
    else:
        return 'Neutral'

for message in consumer:
    sentiment = sentiment_analysis(str(message.value))
    print(sentiment)
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers,api_version=(0,10,1))
    ack = producer.send(ProducerTopicName, key = message.key, value=str(sentiment).encode('utf-8'))
    metadata = ack.get()
    print(metadata.topic)
    print(metadata.partition)
