from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

class Producer:
	def __init__(self):
		try:
			self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda m: json.dumps(m).encode('ascii'),retries=5)
		except Exception as ex:
			print('Exception while connecting Kafka')
			
	def produceMessage(self,msg,topic):
		print ("producing",msg)
		self.producer.send(topic, msg).add_callback(self.on_send_success).add_errback(self.on_send_error)
		self.producer.flush()



	def on_send_success(self,record_metadata):
		print(record_metadata.topic)
		print(record_metadata.partition)
		print(record_metadata.offset)

	def on_send_error(self,excp):
		print('I am an errback', exc_info=excp)



 
