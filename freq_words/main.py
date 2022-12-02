from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import collections

bootstrap_servers = ['10.176.67.102:9092']
ConsumerTopicName = 'level_two'
ProducerTopicName = 'level_five'
consumer = KafkaConsumer (ConsumerTopicName, group_id = None, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'earliest', api_version=(0,10,1))

freq_words = collections.Counter()
count = 0
for message in consumer:
    freq_words = freq_words + collections.Counter(str(message.value).split(" "))
    #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
    if count != 10:
        count = count + 1
        continue
    count = 0
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers,api_version=(0,10,1))
    ack = producer.send(ProducerTopicName, key = message.key, value = json.dumps(freq_words.most_common(10)).encode('utf-8'))
    metadata = ack.get()
    print(metadata.topic)
    print(metadata.partition)
