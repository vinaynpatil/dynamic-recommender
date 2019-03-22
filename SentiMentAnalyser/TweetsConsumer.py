from kafka import KafkaConsumer
import json
from SentimentsAnalyser import sentimentsAnalyser 


class TweetsConsumer:
  def consumerMessage(self):
    consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'])
    #consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')),auto_offset_reset='earliest', enable_auto_commit=False)
    for message in consumer:
      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
      sentimentsAnalyser().analyseSentiments(tweet_text)
      '''
      print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
      '''

TweetsConsumer().consumerMessage()